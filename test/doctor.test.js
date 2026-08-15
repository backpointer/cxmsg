import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  doctorExitCode,
  doctorOverall,
  renderDoctorText,
  runDoctor,
} from "../src/doctor.js";
import {
  diagnosticCheck,
  inspectAppServer,
  inspectBridges,
  inspectConversationState,
  inspectJobs,
  inspectMessageBodies,
  inspectNodeDirectory,
  inspectRouteState,
  inspectState,
} from "../src/inspectors.js";
import { directConversationId } from "../src/conversations.js";
import { CLAUDE_BRIDGE_IMPLEMENTATION_REVISION } from "../src/claude-bridge.js";
import { failedProbe } from "../src/socket-probe.js";
import { rebuildDeliveryLedgerRecords } from "../src/delivery-ledger.js";

const THREAD_ID = "019ff02a-ee3b-7072-8a79-e5ffd491529d";
const JOB_ID = "6ddaa4e0-fa31-454e-a37e-a37f8807f0e7";

function pass(id, scope = "test") {
  return diagnosticCheck({
    id,
    scope,
    status: "pass",
    summary: "fixture passed",
    verification: "fixture",
  });
}

async function writeJson(target, value) {
  await fs.writeFile(target, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
}

function ledgerBatch({
  messageId,
  deliveryId,
  batchId,
  targetThreadId = THREAD_ID,
  createdAt = "2026-08-14T00:00:00.000Z",
  wakePolicy = "immediate",
  triggerId = null,
}) {
  const body = "private body stays absent";
  const route =
    wakePolicy !== "immediate"
      ? {
          schema_version: 1,
          project_id: "hermes",
          target_role: "auditor",
          logical_message_id: messageId,
          payload_type: "coordination",
          ...(wakePolicy === "after-turn" ? { trigger_turn_id: triggerId } : {}),
          ...(wakePolicy === "after-job" ? { trigger_job_id: triggerId } : {}),
          wake_policy: wakePolicy,
          expiry: "2026-08-14T01:00:00.000Z",
        }
      : null;
  return {
    schemaVersion: 1,
    recordType: "ledger-batch",
    batchId,
    committedAt: createdAt,
    logicalMessage: {
      messageId,
      from: "coordinator",
      body: {
        messageId,
        bytes: Buffer.byteLength(body, "utf8"),
        sha256: createHash("sha256").update(body).digest("hex"),
        contentRef: wakePolicy !== "immediate" ? `cxmsg-message:${messageId}` : null,
      },
      route,
      routeFingerprint: createHash("sha256")
        .update(JSON.stringify(route))
        .digest("hex"),
      createdAt,
    },
    deliveries: [
      {
        deliveryId,
        target: "worker",
        targetThreadId,
        admissionState: "admitted",
        admissionReason: "legacy-unbound",
        wakePolicy,
        state: wakePolicy === "immediate" ? "created" : "scheduled",
        createdAt,
        updatedAt: createdAt,
      },
    ],
  };
}

async function stateSnapshot(root) {
  const snapshot = [];
  async function walk(directory, relative = "") {
    const entries = await fs.readdir(directory, { withFileTypes: true });
    for (const entry of entries.sort((left, right) => left.name.localeCompare(right.name))) {
      const childRelative = path.join(relative, entry.name);
      const child = path.join(directory, entry.name);
      const metadata = await fs.lstat(child);
      snapshot.push({
        path: childRelative,
        mode: metadata.mode,
        size: metadata.size,
        mtimeMs: metadata.mtimeMs,
        link: entry.isSymbolicLink() ? await fs.readlink(child) : null,
        contents: entry.isFile() ? await fs.readFile(child, "utf8") : null,
      });
      if (entry.isDirectory()) await walk(child, childRelative);
    }
  }
  await walk(root);
  return snapshot;
}

function passiveAdapters() {
  return {
    inspectRuntime: () => [pass("runtime.fixture", "runtime")],
    inspectAppServer: async () => [pass("app-server.fixture", "app-server")],
    inspectBridges: async () => [pass("bridges.fixture", "bridges")],
    inspectRelay: async () => [pass("relay.fixture", "relay")],
    inspectRegisteredThreads: async () => [pass("sessions.fixture", "sessions")],
  };
}

test("healthy Doctor fixtures are redacted and mutate zero state files", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-"));
  try {
    await fs.chmod(root, 0o700);
    await fs.mkdir(path.join(root, "sessions"), { mode: 0o700 });
    await fs.mkdir(path.join(root, "jobs"), { mode: 0o700 });
    await writeJson(path.join(root, "sessions", "worker.json"), {
      name: "worker",
      threadId: THREAD_ID,
      cwd: "/private/project-that-must-not-be-rendered",
      allowedClaudeRequesters: [],
    });
    await writeJson(path.join(root, "jobs", `${JOB_ID}.json`), {
      version: 1,
      jobId: JOB_ID,
      kind: "delegation",
      status: "completed",
      task: "private task body",
      result: "private result body",
    });
    const before = await stateSnapshot(root);
    const report = await runDoctor({
      stateDir: root,
      pidPath: path.join(root, "app-server.pid"),
      socketPath: path.join(root, "app-server.sock"),
      relayRecordPath: path.join(root, "host-relay.json"),
      adapters: passiveAdapters(),
    });
    const after = await stateSnapshot(root);
    const rendered = JSON.stringify(report);

    assert.equal(report.schemaVersion, 1);
    assert.equal(report.overall, "healthy");
    assert.equal(doctorExitCode(report), 0);
    assert.deepEqual(after, before);
    assert.doesNotMatch(rendered, /private task body|private result body|project-that-must-not-be-rendered/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector validates scheduled Job trigger references without dispatch", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-trigger-"));
  try {
    await fs.chmod(root, 0o700);
    const segments = path.join(root, "delivery-ledger", "segments");
    const quarantine = path.join(root, "delivery-ledger", "quarantine");
    const jobs = path.join(root, "jobs");
    await fs.mkdir(segments, { recursive: true, mode: 0o700 });
    await fs.mkdir(quarantine, { mode: 0o700 });
    await fs.mkdir(jobs, { mode: 0o700 });
    const messageId = "7edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const jobId = "8edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const batch = ledgerBatch({
      messageId,
      deliveryId: "9edaa4e0-fa31-454e-a37e-a37f8807f0e7",
      batchId: "aedaa4e0-fa31-454e-a37e-a37f8807f0e7",
      wakePolicy: "after-job",
      triggerId: jobId,
    });
    await fs.writeFile(
      path.join(segments, "segment-00000001.jsonl"),
      `${JSON.stringify(batch)}\n`,
      { mode: 0o600 },
    );
    await writeJson(path.join(jobs, `${jobId}.json`), {
      version: 1,
      jobId,
      status: "running",
    });
    const inspect = () =>
      inspectRouteState({
        stateDir: root,
        sessions: [{ name: "worker", threadId: THREAD_ID }],
      }).find((check) => check.id === `schedules.trigger.job.${messageId.slice(0, 8)}`);
    assert.equal(inspect().status, "pass");
    await fs.rm(path.join(jobs, `${jobId}.json`));
    assert.equal(inspect().errorCode, "ETRIGGERJOBMISSING");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Conversation Inspector detects missing sources and fan-out plans without exposing content", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-conversations-"));
  try {
    await fs.chmod(root, 0o700);
    const directDir = path.join(root, "conversations", "direct");
    const groupDir = path.join(root, "conversations", "group");
    const planDir = path.join(root, "team-casts", "plans");
    const selectionDir = path.join(root, "team-casts", "selections");
    const ledgerSegments = path.join(root, "delivery-ledger", "segments");
    const ledgerQuarantine = path.join(root, "delivery-ledger", "quarantine");
    for (const directory of [
      directDir,
      groupDir,
      planDir,
      selectionDir,
      ledgerSegments,
      ledgerQuarantine,
    ]) {
      await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    }

    const first = "codex:12345678-1234-4234-8234-123456789abc";
    const second = "codex:22345678-1234-4234-8234-123456789abc";
    const third = "claude:32345678-1234-4234-8234-123456789abc";
    const members = [first, second].sort();
    const conversationId = directConversationId(members[0], members[1]);
    const directMessageId = "42345678-1234-4234-8234-123456789abc";
    await writeJson(path.join(directDir, `${conversationId}.json`), {
      version: 1,
      kind: "direct",
      conversationId,
      members,
      currentMembers: members,
      nextSequence: 2,
      messages: [{
        version: 1,
        conversationId,
        sequence: 1,
        logicalMessageId: directMessageId,
        senderNodeKey: first,
        recipientNodeKey: second,
        sourceKind: "delivery-ledger",
        replyToMessageId: null,
        parentConversationId: null,
        crossConversationReply: false,
        recordedAt: "2026-08-15T00:01:00.000Z",
      }],
      migrations: [],
      createdAt: "2026-08-15T00:00:00.000Z",
      updatedAt: "2026-08-15T00:01:00.000Z",
    });

    const groupConversationId = "52345678-1234-4234-8234-123456789abc";
    const groupMessageId = "62345678-1234-4234-8234-123456789abc";
    const groupMembers = [first, second, third].sort();
    await writeJson(path.join(groupDir, `${groupConversationId}.json`), {
      version: 1,
      kind: "group",
      conversationId: groupConversationId,
      label: "doctor-group",
      projectId: "72345678-1234-4234-8234-123456789abc",
      membershipVersion: 1,
      membershipSnapshots: [{
        version: 1,
        members: groupMembers,
        createdAt: "2026-08-15T00:00:00.000Z",
      }],
      nextSequence: 2,
      messages: [{
        version: 1,
        conversationId: groupConversationId,
        sequence: 1,
        logicalMessageId: groupMessageId,
        senderNodeKey: first,
        membershipVersion: 1,
        recipientNodeKeys: groupMembers.filter((member) => member !== first),
        replyToMessageId: null,
        hopCount: 0,
        expiry: "2026-08-15T01:01:00.000Z",
        bodyBytes: 27,
        bodySha256: createHash("sha256").update("private group message body").digest("hex"),
        recordedAt: "2026-08-15T00:01:00.000Z",
      }],
      createdAt: "2026-08-15T00:00:00.000Z",
      updatedAt: "2026-08-15T00:01:00.000Z",
    });

    const planId = "82345678-1234-4234-8234-123456789abc";
    const missingPlanId = "92345678-1234-4234-8234-123456789abc";
    const selectionId = "a2345678-1234-4234-8234-123456789abc";
    const projectId = "b2345678-1234-4234-8234-123456789abc";
    const recipientNodeKeys = [second, third].sort();
    const recipientSetSha256 = createHash("sha256")
      .update(JSON.stringify(recipientNodeKeys))
      .digest("hex");
    await writeJson(path.join(planDir, `${planId}.json`), {
      version: 1,
      planId,
      senderNodeKey: first,
      projectId,
      selector: {
        kind: "conversation",
        conversationId: groupConversationId,
        conversationKind: "group",
        membershipVersion: 1,
      },
      recipientNodeKeys,
      recipientSetSha256,
      estimatedWakeTurns: 2,
      createdAt: "2026-08-15T00:00:00.000Z",
      expiresAt: "2026-08-15T00:15:00.000Z",
    });
    await writeJson(path.join(selectionDir, `${selectionId}.json`), {
      version: 1,
      selectionId,
      planId: missingPlanId,
      senderNodeKey: first,
      projectId,
      wakePolicy: "wake-all",
      recipientNodeKeys,
      recipientSetSha256,
      estimatedWakeTurns: 2,
      createdAt: "2026-08-15T00:01:00.000Z",
      expiresAt: "2026-08-15T00:15:00.000Z",
    });

    const before = await stateSnapshot(root);
    const checks = inspectConversationState({ stateDir: root });
    const after = await stateSnapshot(root);
    assert.deepEqual(after, before);
    assert.ok(checks.some((check) => check.errorCode === "ECONVERSATIONSOURCE"));
    assert.ok(checks.some((check) => check.errorCode === "EGROUPFANOUT"));
    assert.ok(
      checks.some((check) => check.errorCode === "ETEAMCASTSELECTIONPLAN"),
    );
    assert.doesNotMatch(
      JSON.stringify(checks),
      /private group message body|[0-9a-f]{64}/,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("deep App Server inspection preserves sandbox-denied UDS evidence", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-socket-"));
  const socket = path.join(root, "app-server.sock");
  const pidPath = path.join(root, "app-server.pid");
  const server = net.createServer();
  try {
    await fs.chmod(root, 0o700);
    await fs.writeFile(pidPath, `${process.pid}\n`, { mode: 0o600 });
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(socket, resolve);
    });
    await fs.chmod(socket, 0o600);
    const checks = await inspectAppServer({
      pidPath,
      socketPath: socket,
      deep: true,
      processStateFn: () => "unverified",
      processIdentityFn: () => ({ state: "unavailable", command: null }),
      probe: async () => failedProbe(Object.assign(new Error("denied"), { code: "EPERM" })),
    });
    const connection = checks.find((check) => check.id === "app-server.socket.connect");
    assert.equal(connection.status, "unknown");
    assert.equal(connection.verification, "sandbox-denied");
    assert.equal(connection.errorCode, "EPERM");
    assert.doesNotMatch(connection.summary, /stopped/i);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job Inspector distinguishes missing and unverified workers", () => {
  const base = {
    version: 1,
    kind: "delegation",
    status: "running",
    createdAt: "2026-08-14T00:00:00.000Z",
  };
  const missing = inspectJobs([
    { ...base, jobId: JOB_ID, workerPid: null },
  ], { now: Date.parse("2026-08-14T00:01:00.000Z") });
  assert.equal(missing[0].status, "fail");
  assert.equal(missing[0].errorCode, "EWORKERMISSING");

  const unverified = inspectJobs([
    { ...base, jobId: JOB_ID, workerPid: 42 },
  ], { processStateFn: () => "unverified" });
  assert.equal(unverified[0].status, "unknown");
  assert.equal(unverified[0].errorCode, "EPERM");

  const scheduled = inspectJobs([
    {
      ...base,
      jobId: "52345678-1234-4234-8234-123456789abc",
      status: "scheduled",
      workerPid: null,
      schedule: {
        expiresAt: "2026-08-14T00:02:00.000Z",
        claim: null,
      },
    },
  ], { now: Date.parse("2026-08-14T00:01:00.000Z") });
  assert.equal(scheduled[0].status, "pass");
  assert.equal(scheduled[0].errorCode, undefined);

  const overdueClaude = inspectJobs(
    [
      {
        version: 1,
        kind: "claude-delivery",
        jobId: "62345678-1234-4234-8234-123456789abc",
        status: "acknowledged",
        delivery: {
          completionDeadlineAt: "2026-08-14T00:00:30.000Z",
        },
      },
    ],
    { now: Date.parse("2026-08-14T00:01:00.000Z") },
  );
  assert.equal(overdueClaude[0].status, "warn");
  assert.equal(overdueClaude[0].errorCode, "ECOMPLETIONOVERDUE");

  const missingClaudeDeadline = inspectJobs([
    {
      version: 1,
      kind: "claude-delivery",
      jobId: "72345678-1234-4234-8234-123456789abc",
      status: "acknowledged",
      delivery: {},
    },
  ]);
  assert.equal(missingClaudeDeadline[0].status, "fail");
  assert.equal(missingClaudeDeadline[0].errorCode, "ECOMPLETIONDEADLINE");
});

test("Message Body Store Inspector reports private metadata, quarantine, and quota", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-bodies-"));
  try {
    await fs.chmod(root, 0o700);
    const store = path.join(root, "message-bodies");
    const segments = path.join(store, "segments");
    const quarantine = path.join(store, "quarantine");
    await fs.mkdir(segments, { recursive: true, mode: 0o700 });
    await fs.mkdir(quarantine, { mode: 0o700 });
    await fs.writeFile(path.join(segments, "segment-00000001.jsonl"), "1234567890", {
      mode: 0o600,
    });
    await fs.writeFile(
      path.join(
        quarantine,
        "segment-00000002.partial-12345678-1234-4234-8234-123456789abc.jsonl",
      ),
      "12345678901234567890",
      { mode: 0o600 },
    );

    const checks = inspectMessageBodies({
      stateDir: root,
      quotaBytes: 25,
      segmentBytes: 100,
    });
    assert.equal(
      checks.find((check) => check.id === "message-bodies.directory").status,
      "pass",
    );
    assert.equal(
      checks.find((check) => check.id === "message-bodies.quarantine.count")
        .errorCode,
      "EMESSAGEBODYPARTIAL",
    );
    assert.equal(
      checks.find((check) => check.id === "message-bodies.quota.usage")
        .errorCode,
      "EMESSAGEBODYQUOTA",
    );

    await fs.chmod(path.join(segments, "segment-00000001.jsonl"), 0o644);
    const insecure = inspectMessageBodies({ stateDir: root });
    assert.ok(
      insecure.some(
        (check) =>
          check.scope === "message-bodies" &&
          check.status === "fail" &&
          check.verification === "broad-mode",
      ),
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector checks binding identity and redacts quarantined bodies", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-routes-"));
  try {
    await fs.chmod(root, 0o700);
    for (const directory of ["route-bindings", "route-deliveries", "quarantine"]) {
      await fs.mkdir(path.join(root, directory), { mode: 0o700 });
    }
    await writeJson(path.join(root, "route-bindings", "worker.json"), {
      version: 1,
      sessionName: "worker",
      threadId: THREAD_ID,
      projectId: "hermes",
      role: "auditor",
    });
    const messageId = "7ddaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const message = "private quarantined body";
    const digest = createHash("sha256").update(message).digest("hex");
    await writeJson(path.join(root, "route-deliveries", `${messageId}.json`), {
      version: 1,
      logicalMessageId: messageId,
      from: "coordinator",
      target: "worker",
      targetThreadId: THREAD_ID,
      messageSha256: digest,
      routeFingerprint: createHash("sha256")
        .update(JSON.stringify(null))
        .digest("hex"),
      route: null,
      admissionState: "admitted",
      admissionReason: "legacy-unbound",
      status: "unknown",
    });
    await writeJson(path.join(root, "quarantine", `${messageId}.json`), {
      version: 1,
      logicalMessageId: messageId,
      from: "coordinator",
      target: "worker",
      reason: "project_mismatch",
      message,
      messageBytes: Buffer.byteLength(message, "utf8"),
      messageSha256: digest,
    });

    const checks = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
    });
    assert.equal(
      checks.find((check) => check.id === "route-bindings.session.worker").status,
      "pass",
    );
    assert.equal(
      checks.find((check) => check.id === "quarantine.records.count").status,
      "warn",
    );
    assert.ok(
      checks.some(
        (check) =>
          check.errorCode === "EROUTEUNCONFIRMED" && check.status === "warn",
      ),
    );
    assert.doesNotMatch(JSON.stringify(checks), /private quarantined body/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector rebuilds Delivery Ledger evidence without exposing bodies", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-ledger-"));
  try {
    await fs.chmod(root, 0o700);
    const segments = path.join(root, "delivery-ledger", "segments");
    const quarantine = path.join(root, "delivery-ledger", "quarantine");
    await fs.mkdir(segments, { recursive: true, mode: 0o700 });
    await fs.mkdir(quarantine, { mode: 0o700 });
    const messageId = "8ddaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const deliveryId = "9ddaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const batch = ledgerBatch({
      messageId,
      deliveryId,
      batchId: "addaa4e0-fa31-454e-a37e-a37f8807f0e7",
    });
    const attempt = {
      schemaVersion: 1,
      recordType: "delivery-attempt",
      messageId,
      deliveryId,
      attemptId: "bddaa4e0-fa31-454e-a37e-a37f8807f0e7",
      transport: "codex-app-server",
      startedAt: "2026-08-14T00:00:01.000Z",
    };
    const segment = path.join(segments, "segment-00000001.jsonl");
    await fs.writeFile(
      segment,
      `${JSON.stringify(batch)}\n${JSON.stringify(attempt)}\n`,
      { mode: 0o600 },
    );
    const checks = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      now: Date.parse("2026-08-14T00:00:10.000Z"),
    });
    assert.equal(
      checks.find((check) => check.id === "delivery-ledger.records.schema").status,
      "pass",
    );
    assert.equal(
      checks.find((check) => check.id === `delivery-ledger.target.${messageId.slice(0, 8)}`).status,
      "pass",
    );
    assert.equal(
      checks.some((check) => check.errorCode === "ELEDGERATTEMPTSTALE"),
      false,
    );
    assert.doesNotMatch(JSON.stringify(checks), /private body stays absent/);

    const index = path.join(root, "delivery-ledger", "index");
    await fs.mkdir(index, { mode: 0o700 });
    const projection = rebuildDeliveryLedgerRecords([batch, attempt]).get(messageId);
    await writeJson(path.join(index, `${messageId}.json`), {
      version: 1,
      messageId,
      projection,
      projectionSha256: createHash("sha256")
        .update(JSON.stringify(projection))
        .digest("hex"),
    });
    const segmentMetadata = await fs.stat(segment);
    const manifest = [
      {
        directory: "segments",
        name: "segment-00000001.jsonl",
        size: segmentMetadata.size,
        mtimeMs: segmentMetadata.mtimeMs,
      },
    ];
    await writeJson(path.join(index, "checkpoint.json"), {
      version: 1,
      manifest,
      manifestSha256: createHash("sha256")
        .update(JSON.stringify(manifest))
        .digest("hex"),
      messageCount: 1,
      rebuiltAt: "2026-08-14T00:00:10.000Z",
    });
    const indexed = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      now: Date.parse("2026-08-14T00:00:10.000Z"),
    });
    assert.equal(
      indexed.find((check) => check.id === "delivery-ledger.index.consistency").status,
      "pass",
    );

    const staleProjection = structuredClone(projection);
    staleProjection.delivery.state = "unknown";
    await writeJson(path.join(index, `${messageId}.json`), {
      version: 1,
      messageId,
      projection: staleProjection,
      projectionSha256: createHash("sha256")
        .update(JSON.stringify(staleProjection))
        .digest("hex"),
    });
    const inconsistentIndex = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      now: Date.parse("2026-08-14T00:00:10.000Z"),
    });
    assert.equal(
      inconsistentIndex.find((check) => check.id === "delivery-ledger.index.consistency")
        .errorCode,
      "ELEDGERINDEXSTALE",
    );
    await writeJson(path.join(index, `${messageId}.json`), {
      version: 1,
      messageId,
      projection,
      projectionSha256: createHash("sha256")
        .update(JSON.stringify(projection))
        .digest("hex"),
    });

    const stale = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      now: Date.parse("2026-08-14T00:00:31.000Z"),
    });
    assert.equal(
      stale.find((check) => check.errorCode === "ELEDGERATTEMPTSTALE").status,
      "warn",
    );

    const segmentBytes = (await fs.stat(segment)).size;
    const exhausted = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      ledgerQuotaBytes: segmentBytes,
    });
    assert.equal(
      exhausted.find((check) => check.errorCode === "ELEDGERQUOTA").status,
      "fail",
    );

    await fs.appendFile(
      segment,
      `${JSON.stringify({
        schemaVersion: 1,
        recordType: "delivery-evidence",
        messageId,
        deliveryId,
        attemptId: attempt.attemptId,
        state: "unknown",
        evidenceKind: "reconciliation",
        turnId: null,
        transportResult: null,
        errorCode: "EACCEPTANCEUNVERIFIED",
        observedAt: "2026-08-14T00:00:31.000Z",
      })}\n`,
    );
    const reconciled = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      now: Date.parse("2026-08-14T00:01:00.000Z"),
    });
    assert.equal(
      reconciled.some((check) => check.errorCode === "ELEDGERATTEMPTSTALE"),
      false,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector reports queued work with a missing scheduler and expired claim", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-scheduler-"));
  try {
    await fs.chmod(root, 0o700);
    const segments = path.join(root, "delivery-ledger", "segments");
    const quarantine = path.join(root, "delivery-ledger", "quarantine");
    const successors = path.join(root, "directory", "successors");
    await fs.mkdir(segments, { recursive: true, mode: 0o700 });
    await fs.mkdir(quarantine, { mode: 0o700 });
    await fs.mkdir(successors, { recursive: true, mode: 0o700 });
    const messageId = "1edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const deliveryId = "2edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const workerId = "3edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const batch = ledgerBatch({
      messageId,
      deliveryId,
      batchId: "4edaa4e0-fa31-454e-a37e-a37f8807f0e7",
      wakePolicy: "when-idle",
    });
    const claim = {
      schemaVersion: 1,
      recordType: "delivery-claim",
      action: "acquired",
      messageId,
      deliveryId,
      claimId: "5edaa4e0-fa31-454e-a37e-a37f8807f0e7",
      workerId,
      claimedAt: "2026-08-14T00:00:01.000Z",
      leaseUntil: "2026-08-14T00:00:31.000Z",
    };
    await fs.writeFile(
      path.join(segments, "segment-00000001.jsonl"),
      `${JSON.stringify(batch)}\n${JSON.stringify(claim)}\n`,
      { mode: 0o600 },
    );
    const successorThreadId = "6edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    await writeJson(path.join(successors, `codex--${successorThreadId}.json`), {
      version: 1,
      predecessorNodeKey: `codex:${THREAD_ID}`,
      successorNodeKey: `codex:${successorThreadId}`,
      projectId: "7edaa4e0-fa31-454e-a37e-a37f8807f0e7",
      linkedAt: "2026-08-14T00:00:20.000Z",
    });
    const checks = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      now: Date.parse("2026-08-14T00:01:00.000Z"),
    });
    assert.equal(
      checks.find((check) => check.errorCode === "ESCHEDULECLAIMEXPIRED").status,
      "warn",
    );
    assert.equal(
      checks.find((check) => check.errorCode === "ESCHEDULERDOWN").status,
      "warn",
    );
    assert.equal(
      checks.find((check) => check.errorCode === "ETARGETPREDECESSOR").status,
      "warn",
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector distinguishes healthy, stalled, and legacy scheduler records", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-heartbeat-"));
  try {
    await fs.chmod(root, 0o700);
    const schedulerPath = path.join(root, "scheduler.json");
    const workerId = "6edaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const base = {
      version: 2,
      pid: process.pid,
      workerId,
      startedAt: "2026-08-14T00:00:00.000Z",
      heartbeatAt: "2026-08-14T00:00:19.000Z",
      lastPassAt: "2026-08-14T00:00:19.000Z",
      lastErrorCode: null,
      lastOutcomeCount: 0,
    };
    await writeJson(schedulerPath, base);
    const inspect = () =>
      inspectRouteState({
        stateDir: root,
        now: Date.parse("2026-08-14T00:00:20.000Z"),
        processStateFn: () => "alive",
      }).find((check) => check.id === "schedules.worker.process");

    assert.equal(inspect().status, "pass");
    await writeJson(schedulerPath, {
      ...base,
      heartbeatAt: "2026-08-14T00:00:00.000Z",
    });
    assert.equal(inspect().errorCode, "ESCHEDULERSTALLED");
    await writeJson(schedulerPath, {
      version: 1,
      pid: process.pid,
      workerId,
      startedAt: base.startedAt,
    });
    assert.equal(inspect().errorCode, "ESCHEDULERLEGACY");
    await writeJson(schedulerPath, base);
    const intentPath = path.join(root, "scheduler.intent.json");
    await writeJson(intentPath, {
      version: 1,
      desiredState: "running",
      changedAt: "2026-08-14T00:00:10.000Z",
    });
    const inspectMissing = () =>
      inspectRouteState({
        stateDir: root,
        now: Date.parse("2026-08-14T00:00:20.000Z"),
        processStateFn: () => "missing",
      }).find((check) => check.id === "schedules.worker.process");
    assert.equal(inspectMissing().errorCode, "ESCHEDULERCRASHED");
    await writeJson(intentPath, {
      version: 1,
      desiredState: "stopped",
      changedAt: "2026-08-14T00:00:11.000Z",
    });
    assert.equal(inspectMissing().errorCode, "ESCHEDULERSTOPPED");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector fails closed on malformed Turn Lifecycle state", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-lifecycle-"));
  try {
    await fs.chmod(root, 0o700);
    await writeJson(path.join(root, "turn-lifecycle.json"), {
      version: 1,
      observationSequence: 1,
      connection: null,
      threads: { "not-a-thread-id": { status: "idle" } },
    });
    const check = inspectRouteState({ stateDir: root }).find(
      (candidate) => candidate.id === "schedules.lifecycle.schema",
    );
    assert.equal(check.status, "fail");
    assert.equal(check.errorCode, "ETURNLIFECYCLESCHEMA");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Route Inspector fails on duplicate storage identity and complete quarantined corruption", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-ledger-conflict-"));
  try {
    await fs.chmod(root, 0o700);
    const segments = path.join(root, "delivery-ledger", "segments");
    const ledgerQuarantine = path.join(root, "delivery-ledger", "quarantine");
    const legacy = path.join(root, "route-deliveries");
    await fs.mkdir(segments, { recursive: true, mode: 0o700 });
    await fs.mkdir(ledgerQuarantine, { mode: 0o700 });
    await fs.mkdir(legacy, { mode: 0o700 });
    const messageId = "cddaa4e0-fa31-454e-a37e-a37f8807f0e7";
    const batch = ledgerBatch({
      messageId,
      deliveryId: "dddaa4e0-fa31-454e-a37e-a37f8807f0e7",
      batchId: "eddaa4e0-fa31-454e-a37e-a37f8807f0e7",
    });
    await fs.writeFile(
      path.join(segments, "segment-00000001.jsonl"),
      `${JSON.stringify(batch)}\n`,
      { mode: 0o600 },
    );
    await writeJson(path.join(legacy, `${messageId}.json`), {
      version: 1,
      logicalMessageId: messageId,
      from: "coordinator",
      target: "worker",
      targetThreadId: THREAD_ID,
      messageSha256: batch.logicalMessage.body.sha256,
      routeFingerprint: batch.logicalMessage.routeFingerprint,
      route: null,
      admissionState: "admitted",
      status: "turn_started",
    });
    await fs.writeFile(
      path.join(
        ledgerQuarantine,
        "segment-00000002.partial-fddaa4e0-fa31-454e-a37e-a37f8807f0e7.jsonl",
      ),
      "{}\n",
      { mode: 0o600 },
    );

    const checks = inspectRouteState({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
    });
    assert.equal(
      checks.find((check) => check.errorCode === "ELEDGERDUPLICATEIDENTITY")
        .status,
      "fail",
    );
    const corrupted = checks.find(
      (check) => check.errorCode === "ELEDGERSCHEMA",
    );
    assert.equal(corrupted.status, "fail");
    assert.match(corrupted.summary, /segment-00000002.*line 1/);
    assert.doesNotMatch(JSON.stringify(checks), /private body stays absent/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Node Directory Inspector validates private identity references without paths", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-directory-"));
  try {
    await fs.chmod(root, 0o700);
    const projects = path.join(root, "directory", "projects");
    const nodes = path.join(root, "directory", "nodes");
    const tombstones = path.join(root, "directory", "tombstones", "nodes");
    const successors = path.join(root, "directory", "successors");
    const executionThreads = path.join(root, "directory", "execution-threads");
    const clusters = path.join(root, "directory", "clusters");
    const clusterMemberships = path.join(root, "directory", "cluster-memberships");
    await fs.mkdir(projects, { recursive: true, mode: 0o700 });
    await fs.mkdir(nodes, { mode: 0o700 });
    await fs.mkdir(tombstones, { recursive: true, mode: 0o700 });
    await fs.mkdir(successors, { mode: 0o700 });
    await fs.mkdir(executionThreads, { mode: 0o700 });
    await fs.mkdir(clusters, { mode: 0o700 });
    await fs.mkdir(clusterMemberships, { mode: 0o700 });
    const projectId = "a2345678-1234-4234-8234-123456789abc";
    const predecessorId = "b2345678-1234-4234-8234-123456789abc";
    const executionThreadId = "c2345678-1234-4234-8234-123456789abc";
    const executionJobId = "d2345678-1234-4234-8234-123456789abc";
    const clusterId = "e2345678-1234-4234-8234-123456789abc";
    await writeJson(path.join(projects, `${projectId}.json`), {
      version: 1,
      projectId,
      routingId: "hermes",
      discovery: {
        kind: "canonical-root",
        key: "/private/project-that-must-not-be-rendered",
      },
      rootAliases: [
        {
          path: "/private/project-that-must-not-be-rendered",
          firstSeenAt: "2026-08-14T00:00:00.000Z",
          lastSeenAt: "2026-08-14T00:00:00.000Z",
        },
      ],
    });
    await writeJson(path.join(nodes, `codex--${THREAD_ID}.json`), {
      version: 1,
      nodeKey: `codex:${THREAD_ID}`,
      runtimeKind: "codex",
      nativeId: THREAD_ID,
      projectId,
      aliases: [{ value: "worker" }],
      selectedEndpoints: {
        "codex-app-server": {
          transport: "codex-app-server",
          endpointId: `app-server:${THREAD_ID}`,
          nodeKey: `codex:${THREAD_ID}`,
          generation: 1,
          status: "reachable",
          address: "uds:/private/endpoint-that-must-not-be-rendered.sock",
          observedAt: "2026-08-14T00:00:00.000Z",
        },
      },
      endpointHistory: [
        {
          transport: "codex-app-server",
          endpointId: `app-server:${THREAD_ID}`,
          nodeKey: `codex:${THREAD_ID}`,
          generation: 1,
          status: "reachable",
          decision: "selected",
          address: "uds:/private/endpoint-that-must-not-be-rendered.sock",
          firstObservedAt: "2026-08-14T00:00:00.000Z",
          lastObservedAt: "2026-08-14T00:00:00.000Z",
          observationCount: 1,
        },
      ],
    });
    await writeJson(path.join(tombstones, `claude--${predecessorId}.json`), {
      version: 1,
      nodeKey: `claude:${predecessorId}`,
      runtimeKind: "claude",
      nativeId: predecessorId,
      projectId,
      lastSafeLabel: "retired-auditor",
      removedAt: "2026-08-14T00:01:00.000Z",
      reason: "session-removed",
    });
    await writeJson(path.join(successors, `codex--${THREAD_ID}.json`), {
      version: 1,
      predecessorNodeKey: `claude:${predecessorId}`,
      successorNodeKey: `codex:${THREAD_ID}`,
      projectId,
      linkedAt: "2026-08-14T00:02:00.000Z",
    });
    await writeJson(path.join(executionThreads, `${executionThreadId}.json`), {
      version: 1,
      kind: "execution-thread",
      threadId: executionThreadId,
      jobId: executionJobId,
      sourceThreadId: THREAD_ID,
      sourceNodeKey: `codex:${THREAD_ID}`,
      projectId,
      creationMode: "fork",
      classifiedAt: "2026-08-14T00:03:00.000Z",
    });
    await writeJson(path.join(clusters, `${clusterId}.json`), {
      version: 1,
      clusterId,
      routingId: "release-reviewers",
      membershipVersion: 3,
      members: [`claude:${predecessorId}`, `codex:${THREAD_ID}`],
      createdAt: "2026-08-14T00:04:00.000Z",
      updatedAt: "2026-08-14T00:06:00.000Z",
    });
    await writeJson(
      path.join(clusterMemberships, `${clusterId}--0000000001.json`),
      {
        version: 1,
        clusterId,
        membershipVersion: 1,
        members: [],
        changeKind: "created",
        createdAt: "2026-08-14T00:04:00.000Z",
      },
    );
    await writeJson(
      path.join(clusterMemberships, `${clusterId}--0000000002.json`),
      {
        version: 1,
        clusterId,
        membershipVersion: 2,
        members: [`codex:${THREAD_ID}`],
        changeKind: "member-added",
        changedNodeKey: `codex:${THREAD_ID}`,
        createdAt: "2026-08-14T00:05:00.000Z",
      },
    );
    await writeJson(
      path.join(clusterMemberships, `${clusterId}--0000000003.json`),
      {
        version: 1,
        clusterId,
        membershipVersion: 3,
        members: [`claude:${predecessorId}`, `codex:${THREAD_ID}`],
        changeKind: "member-added",
        changedNodeKey: `claude:${predecessorId}`,
        createdAt: "2026-08-14T00:06:00.000Z",
      },
    );

    const checks = inspectNodeDirectory({
      stateDir: root,
      sessions: [{ name: "worker", threadId: THREAD_ID }],
      jobs: [
        {
          version: 1,
          jobId: executionJobId,
          kind: "delegation",
          execution: "fork",
          targetThreadId: THREAD_ID,
          threadId: executionThreadId,
          executionThreadId,
          status: "completed",
        },
      ],
    });
    assert.equal(
      checks.find((check) => check.id.startsWith("directory-projects.identity"))
        .status,
      "pass",
    );
    assert.equal(
      checks.find((check) => check.id.startsWith("directory-nodes.project"))
        .status,
      "pass",
    );
    assert.equal(
      checks.find((check) =>
        check.id.startsWith("directory-nodes.endpoint-history"),
      ).status,
      "pass",
    );
    assert.equal(
      checks.find((check) =>
        check.id.startsWith("directory-node-tombstones.lifecycle"),
      ).status,
      "pass",
    );
    assert.equal(
      checks.find((check) => check.id.startsWith("directory-successors.reference"))
        .status,
      "pass",
    );
    assert.equal(
      checks.find((check) => check.id === "directory-successors.graph.acyclic")
        .status,
      "pass",
    );
    assert.ok(
      checks
        .filter((check) => check.scope === "directory-execution-threads")
        .every((check) => !["fail", "unknown"].includes(check.status)),
    );
    assert.equal(
      checks.find((check) =>
        check.id.startsWith("directory-cluster-memberships.history"),
      ).status,
      "pass",
    );
    assert.doesNotMatch(
      JSON.stringify(checks),
      /project-that-must-not-be-rendered|endpoint-that-must-not-be-rendered/,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Node Directory Inspector reports Cluster lifecycle and membership gaps without repair", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-clusters-"));
  try {
    await fs.chmod(root, 0o700);
    const clusters = path.join(root, "directory", "clusters");
    const memberships = path.join(root, "directory", "cluster-memberships");
    const tombstones = path.join(root, "directory", "tombstones", "clusters");
    const projects = path.join(root, "directory", "projects");
    const nodes = path.join(root, "directory", "nodes");
    for (const directory of [clusters, memberships, tombstones, projects, nodes]) {
      await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    }
    const clusterId = "f2345678-1234-4234-8234-123456789abc";
    const orphanId = "a3345678-1234-4234-8234-123456789abc";
    const recoverableId = "b4345678-1234-4234-8234-123456789abc";
    const projectId = "c5345678-1234-4234-8234-123456789abc";
    const nodeId = "d6345678-1234-4234-8234-123456789abc";
    await writeJson(path.join(projects, `${projectId}.json`), {
      version: 1,
      projectId,
      routingId: "recovery-project",
      discovery: { kind: "canonical-root", key: "/private/recovery-project" },
      rootAliases: [{ path: "/private/recovery-project" }],
    });
    await writeJson(path.join(nodes, `codex--${nodeId}.json`), {
      version: 1,
      nodeKey: `codex:${nodeId}`,
      runtimeKind: "codex",
      nativeId: nodeId,
      projectId,
      aliases: [{ value: "recovery-node" }],
      selectedEndpoints: {},
      endpointHistory: [],
    });
    await writeJson(path.join(clusters, `${clusterId}.json`), {
      version: 1,
      clusterId,
      routingId: "conflicted-cluster",
      membershipVersion: 2,
      members: [],
      createdAt: "2026-08-14T00:00:00.000Z",
      updatedAt: "2026-08-14T00:01:00.000Z",
    });
    await writeJson(path.join(tombstones, `${clusterId}.json`), {
      version: 1,
      clusterId,
      routingId: "conflicted-cluster",
      lastMembershipVersion: 1,
      removedAt: "2026-08-14T00:02:00.000Z",
      reason: "interrupted",
    });
    await writeJson(
      path.join(memberships, `${clusterId}--0000000001.json`),
      {
        version: 1,
        clusterId,
        membershipVersion: 1,
        members: [],
        changeKind: "created",
        createdAt: "2026-08-14T00:00:00.000Z",
      },
    );
    await writeJson(
      path.join(memberships, `${orphanId}--0000000001.json`),
      {
        version: 1,
        clusterId: orphanId,
        membershipVersion: 1,
        members: [],
        changeKind: "created",
        createdAt: "2026-08-14T00:00:00.000Z",
      },
    );
    await writeJson(path.join(clusters, `${recoverableId}.json`), {
      version: 1,
      clusterId: recoverableId,
      routingId: "recoverable-cluster",
      membershipVersion: 1,
      members: [],
      createdAt: "2026-08-14T00:00:00.000Z",
      updatedAt: "2026-08-14T00:00:00.000Z",
    });
    await writeJson(
      path.join(memberships, `${recoverableId}--0000000001.json`),
      {
        version: 1,
        clusterId: recoverableId,
        membershipVersion: 1,
        members: [],
        changeKind: "created",
        createdAt: "2026-08-14T00:00:00.000Z",
      },
    );
    await writeJson(
      path.join(memberships, `${recoverableId}--0000000002.json`),
      {
        version: 1,
        clusterId: recoverableId,
        membershipVersion: 2,
        members: [`codex:${nodeId}`],
        changeKind: "member-added",
        changedNodeKey: `codex:${nodeId}`,
        createdAt: "2026-08-14T00:01:00.000Z",
      },
    );

    const checks = inspectNodeDirectory({
      stateDir: root,
      sessions: [{ name: "recovery-node", threadId: nodeId }],
    });
    assert.ok(
      checks.some(
        (check) =>
          check.errorCode === "ECLUSTERLIFECYCLE" && check.status === "fail",
      ),
    );
    assert.ok(
      checks.some(
        (check) =>
          check.errorCode === "ECLUSTERMEMBERSHIPREDO" &&
          check.status === "warn" &&
          check.required === false,
      ),
    );
    assert.ok(
      checks.some(
        (check) =>
          check.errorCode === "ECLUSTERMEMBERSHIP" && check.status === "fail",
      ),
    );
    assert.ok(
      checks.some(
        (check) =>
          check.errorCode === "ECLUSTERMEMBERSHIPORPHAN" &&
          check.status === "fail",
      ),
    );
    assert.equal(
      existsSync(path.join(clusters, `${clusterId}.json`)),
      true,
    );
    assert.equal(
      existsSync(path.join(tombstones, `${clusterId}.json`)),
      true,
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Node Directory Inspector reports lifecycle conflicts and successor cycles without repair", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-lifecycle-"));
  try {
    await fs.chmod(root, 0o700);
    const projects = path.join(root, "directory", "projects");
    const nodes = path.join(root, "directory", "nodes");
    const tombstones = path.join(root, "directory", "tombstones", "nodes");
    const successors = path.join(root, "directory", "successors");
    const executionThreads = path.join(root, "directory", "execution-threads");
    for (const directory of [
      projects,
      nodes,
      tombstones,
      successors,
      executionThreads,
    ]) {
      await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    }
    const projectId = "c2345678-1234-4234-8234-123456789abc";
    const firstId = "d2345678-1234-4234-8234-123456789abc";
    const secondId = "e2345678-1234-4234-8234-123456789abc";
    await writeJson(path.join(projects, `${projectId}.json`), {
      version: 1,
      projectId,
      routingId: "orchestra",
      discovery: { kind: "canonical-root", key: "/private/redacted-project" },
      rootAliases: [{ path: "/private/redacted-project" }],
    });
    for (const nativeId of [firstId, secondId]) {
      await writeJson(path.join(nodes, `codex--${nativeId}.json`), {
        version: 1,
        nodeKey: `codex:${nativeId}`,
        runtimeKind: "codex",
        nativeId,
        projectId,
        aliases: [{ value: "worker" }],
        selectedEndpoints: {},
      });
    }
    await writeJson(path.join(nodes, `codex--${secondId}.json`), {
      version: 1,
      nodeKey: `codex:${secondId}`,
      runtimeKind: "codex",
      nativeId: secondId,
      projectId,
      aliases: [{ value: "worker" }],
      selectedEndpoints: {
        "codex-app-server": {
          transport: "codex-app-server",
          endpointId: `selected:${secondId}`,
          nodeKey: `codex:${secondId}`,
          generation: 2,
          status: "reachable",
          observedAt: "2026-08-14T00:02:00.000Z",
        },
      },
      endpointHistory: [
        {
          transport: "codex-app-server",
          endpointId: `older:${secondId}`,
          nodeKey: `codex:${secondId}`,
          generation: 1,
          status: "stale",
          decision: "older-rejected",
          firstObservedAt: "2026-08-14T00:01:00.000Z",
          lastObservedAt: "2026-08-14T00:01:00.000Z",
          observationCount: 1,
        },
      ],
    });
    await writeJson(path.join(tombstones, `codex--${firstId}.json`), {
      version: 1,
      nodeKey: `codex:${firstId}`,
      runtimeKind: "codex",
      nativeId: firstId,
      projectId,
      lastSafeLabel: "retired-worker",
      removedAt: "2026-08-14T00:01:00.000Z",
      reason: "interrupted-transition",
    });
    await writeJson(path.join(successors, `codex--${firstId}.json`), {
      version: 1,
      predecessorNodeKey: `codex:${secondId}`,
      successorNodeKey: `codex:${firstId}`,
      projectId,
      linkedAt: "2026-08-14T00:02:00.000Z",
    });
    await writeJson(path.join(successors, `codex--${secondId}.json`), {
      version: 1,
      predecessorNodeKey: `codex:${firstId}`,
      successorNodeKey: `codex:${secondId}`,
      projectId,
      linkedAt: "2026-08-14T00:03:00.000Z",
    });
    const executionJobId = "f2345678-1234-4234-8234-123456789abc";
    await writeJson(path.join(executionThreads, `${firstId}.json`), {
      version: 1,
      kind: "execution-thread",
      threadId: firstId,
      jobId: executionJobId,
      sourceThreadId: secondId,
      sourceNodeKey: `codex:${secondId}`,
      projectId,
      creationMode: "legacy-observed",
      classifiedAt: "2026-08-14T00:04:00.000Z",
    });

    const before = await stateSnapshot(root);
    const checks = inspectNodeDirectory({
      stateDir: root,
      sessions: [],
      jobs: [
        {
          jobId: executionJobId,
          kind: "delegation",
          execution: "fork",
          targetThreadId: secondId,
          threadId: firstId,
          status: "completed",
        },
      ],
    });
    const after = await stateSnapshot(root);
    assert.deepEqual(after, before);
    assert.ok(checks.some((check) => check.errorCode === "ENODELIFECYCLE"));
    assert.ok(checks.some((check) => check.errorCode === "ESUCCESSORCYCLE"));
    assert.ok(
      checks.some((check) => check.errorCode === "EEXECUTIONNODECOLLISION"),
    );
    assert.ok(checks.some((check) => check.errorCode === "EENDPOINTHISTORY"));
    assert.doesNotMatch(JSON.stringify(checks), /redacted-project/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Node Directory Inspector validates Project move chains without exposing paths", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-project-move-"));
  try {
    await fs.chmod(root, 0o700);
    const projects = path.join(root, "directory", "projects");
    const transitions = path.join(root, "directory", "project-transitions");
    await fs.mkdir(projects, { recursive: true, mode: 0o700 });
    await fs.mkdir(transitions, { recursive: true, mode: 0o700 });
    const projectId = "12345678-2234-4234-8234-123456789abc";
    const transitionId = "22345678-2234-4234-8234-123456789abc";
    const beforePath = "/private/old-project-fixture";
    const afterPath = "/private/new-project-fixture";
    const projectPath = path.join(projects, `${projectId}.json`);
    await writeJson(projectPath, {
      version: 1,
      projectId,
      routingId: "hermes",
      discovery: { kind: "canonical-root", key: afterPath },
      rootAliases: [{ path: beforePath }, { path: afterPath }],
    });
    await writeJson(
      path.join(transitions, `${projectId}--${transitionId}.json`),
      {
        version: 1,
        transitionId,
        kind: "move",
        projectId,
        fromDiscovery: { kind: "canonical-root", key: beforePath },
        toDiscovery: { kind: "canonical-root", key: afterPath },
        toRoot: afterPath,
        createdAt: "2026-08-14T00:00:00.000Z",
      },
    );
    const inspect = () =>
      inspectNodeDirectory({ stateDir: root }).find((check) =>
        check.id.startsWith("directory-project-transitions.chain"),
      );
    assert.equal(inspect().status, "pass");
    assert.doesNotMatch(JSON.stringify(inspect()), /old-project|new-project/);
    await writeJson(projectPath, {
      version: 1,
      projectId,
      routingId: "hermes",
      discovery: { kind: "canonical-root", key: beforePath },
      rootAliases: [{ path: beforePath }, { path: afterPath }],
    });
    assert.equal(inspect().errorCode, "EPROJECTMOVEINCOMPLETE");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Bridge Inspector distinguishes current, unknown, and stale implementations", async () => {
  const base = {
    version: 1,
    targetThreadId: THREAD_ID,
    pid: 42,
    socketPath: "/missing/bridge.sock",
    startedAt: 1,
  };
  const bridges = [
    { ...base, target: "legacy" },
    {
      ...base,
      target: "stale",
      implementationRevision: CLAUDE_BRIDGE_IMPLEMENTATION_REVISION + 1,
    },
    {
      ...base,
      target: "current",
      implementationRevision: CLAUDE_BRIDGE_IMPLEMENTATION_REVISION,
    },
  ];
  const sessions = bridges.map((bridge) => ({
    name: bridge.target,
    threadId: bridge.targetThreadId,
  }));
  const checks = await inspectBridges(bridges, sessions, {
    processStateFn: () => "alive",
    processIdentityFn: () => ({ state: "matched", command: "bridge" }),
  });

  const implementation = (target) =>
    checks.find((check) => check.id === `bridges.${target}.implementation`);
  assert.equal(implementation("legacy").status, "warn");
  assert.equal(implementation("legacy").errorCode, "EBRIDGEVERSIONUNKNOWN");
  assert.equal(implementation("stale").status, "warn");
  assert.equal(implementation("stale").errorCode, "EBRIDGESTALECODE");
  assert.equal(implementation("current").status, "pass");
});

test("State Inspector rejects symlink records without reading their target", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-link-"));
  const outside = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-doctor-secret-"));
  try {
    await fs.chmod(root, 0o700);
    await fs.mkdir(path.join(root, "sessions"), { mode: 0o700 });
    const secret = path.join(outside, "secret.json");
    await writeJson(secret, {
      name: "evil",
      threadId: THREAD_ID,
      cwd: "/secret/cwd-that-must-not-be-read",
    });
    await fs.symlink(secret, path.join(root, "sessions", "evil.json"));

    const inspected = inspectState({ stateDir: root });
    const rendered = JSON.stringify(inspected.checks);
    assert.ok(inspected.checks.some((check) => check.verification === "symlink"));
    assert.doesNotMatch(rendered, /cwd-that-must-not-be-read|cxmsg-doctor-secret/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
    await fs.rm(outside, { recursive: true, force: true });
  }
});

test("Doctor report policy and renderer have stable statuses and exit codes", () => {
  const healthy = { overall: doctorOverall([pass("a")]), checks: [pass("a")], deep: false };
  assert.equal(healthy.overall, "healthy");
  assert.equal(doctorExitCode(healthy), 0);

  const unknown = diagnosticCheck({ id: "b", scope: "test", status: "unknown", summary: "unverified", errorCode: "EPERM" });
  const degraded = { overall: doctorOverall([unknown]), checks: [unknown], deep: true };
  assert.equal(degraded.overall, "degraded");
  assert.equal(doctorExitCode(degraded), 1);
  assert.match(renderDoctorText(degraded), /^cxmsg doctor: degraded/m);

  const failed = diagnosticCheck({ id: "c", scope: "test", status: "fail", summary: "failed" });
  assert.equal(doctorOverall([failed]), "unhealthy");
});

test("Doctor CLI reserves exit code 2 for invalid invocation", () => {
  const result = spawnSync(
    process.execPath,
    [path.resolve("bin/cxmsg.js"), "doctor", "--unknown"],
    { encoding: "utf8" },
  );
  assert.equal(result.status, 2);
  assert.match(result.stderr, /unknown doctor option/);
});
