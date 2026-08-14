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
  inspectJobs,
  inspectMessageBodies,
  inspectNodeDirectory,
  inspectRouteState,
  inspectState,
} from "../src/inspectors.js";
import { CLAUDE_BRIDGE_IMPLEMENTATION_REVISION } from "../src/claude-bridge.js";
import { failedProbe } from "../src/socket-probe.js";

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
    assert.doesNotMatch(JSON.stringify(checks), /private quarantined body/);
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
    for (const directory of [clusters, memberships, tombstones]) {
      await fs.mkdir(directory, { recursive: true, mode: 0o700 });
    }
    const clusterId = "f2345678-1234-4234-8234-123456789abc";
    const orphanId = "a3345678-1234-4234-8234-123456789abc";
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

    const checks = inspectNodeDirectory({ stateDir: root });
    assert.ok(
      checks.some(
        (check) =>
          check.errorCode === "ECLUSTERLIFECYCLE" && check.status === "fail",
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
