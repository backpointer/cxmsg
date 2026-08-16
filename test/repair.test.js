import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  unlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { inspectRepairState } from "../src/inspectors.js";

function writeJson(filename, value) {
  writeFileSync(filename, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
}

function runCxmsg(root, args) {
  return spawnSync(process.execPath, [path.resolve("bin/cxmsg.js"), ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: root },
  });
}

function runCxmsgAsync(root, args) {
  const child = spawn(process.execPath, [path.resolve("bin/cxmsg.js"), ...args], {
    env: { ...process.env, CXMSG_STATE_DIR: root },
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  return {
    child,
    completed: new Promise((resolve, reject) => {
      child.once("error", reject);
      child.once("close", (status) => resolve({ status, stdout, stderr }));
    }),
  };
}

async function waitFor(predicate, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = predicate();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("timed out waiting for Repair test state");
}

function clusterRedoFixture(root) {
  const projectId = "12345678-2234-4234-8234-123456789abc";
  const nodeId = "22345678-2234-4234-8234-123456789abc";
  const clusterId = "32345678-2234-4234-8234-123456789abc";
  const nodeKey = `codex:${nodeId}`;
  const projects = path.join(root, "directory", "projects");
  const nodes = path.join(root, "directory", "nodes");
  const clusters = path.join(root, "directory", "clusters");
  const memberships = path.join(root, "directory", "cluster-memberships");
  for (const directory of [projects, nodes, clusters, memberships]) {
    mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  writeJson(path.join(projects, `${projectId}.json`), {
    version: 1,
    projectId,
    routingId: "repair-project",
    discovery: { kind: "canonical-root", key: "/private/repair-project" },
    rootAliases: [{ path: "/private/repair-project" }],
  });
  writeJson(path.join(nodes, `codex--${nodeId}.json`), {
    version: 1,
    nodeKey,
    runtimeKind: "codex",
    nativeId: nodeId,
    projectId,
    aliases: [{ value: "repair-node" }],
    selectedEndpoints: {},
    endpointHistory: [],
  });
  const createdAt = "2026-08-15T00:00:00.000Z";
  const changedAt = "2026-08-15T00:01:00.000Z";
  writeJson(path.join(clusters, `${clusterId}.json`), {
    version: 1,
    clusterId,
    routingId: "repair-cluster",
    membershipVersion: 1,
    members: [],
    createdAt,
    updatedAt: createdAt,
  });
  writeJson(path.join(memberships, `${clusterId}--0000000001.json`), {
    version: 1,
    clusterId,
    membershipVersion: 1,
    members: [],
    changeKind: "created",
    createdAt,
  });
  const nextMembershipPath = path.join(
    memberships,
    `${clusterId}--0000000002.json`,
  );
  writeJson(nextMembershipPath, {
    version: 1,
    clusterId,
    membershipVersion: 2,
    members: [nodeKey],
    changeKind: "member-added",
    changedNodeKey: nodeKey,
    createdAt: changedAt,
  });
  return {
    clusterId,
    clusterPath: path.join(clusters, `${clusterId}.json`),
    nextMembershipPath,
    findingId: `directory-cluster-memberships.history.${clusterId.slice(0, 8)}`,
  };
}

function staleIndexFixture(root) {
  const segments = path.join(root, "delivery-ledger", "segments");
  const quarantine = path.join(root, "delivery-ledger", "quarantine");
  const index = path.join(root, "delivery-ledger", "index");
  for (const directory of [segments, quarantine, index]) {
    mkdirSync(directory, { recursive: true, mode: 0o700 });
  }
  const messageId = "42345678-2234-4234-8234-123456789abc";
  const deliveryId = "52345678-2234-4234-8234-123456789abc";
  const body = "private ledger body";
  const createdAt = "2026-08-15T00:00:00.000Z";
  const route = null;
  const batch = {
    schemaVersion: 1,
    recordType: "ledger-batch",
    batchId: "62345678-2234-4234-8234-123456789abc",
    committedAt: createdAt,
    logicalMessage: {
      messageId,
      from: "repair-sender",
      body: {
        messageId,
        bytes: Buffer.byteLength(body, "utf8"),
        sha256: createHash("sha256").update(body).digest("hex"),
        contentRef: null,
      },
      route,
      routeFingerprint: createHash("sha256")
        .update(JSON.stringify(route))
        .digest("hex"),
      createdAt,
    },
    deliveries: [{
      deliveryId,
      target: "repair-target",
      targetThreadId: "72345678-2234-4234-8234-123456789abc",
      admissionState: "admitted",
      admissionReason: "legacy-unbound",
      wakePolicy: "immediate",
      state: "created",
      createdAt,
      updatedAt: createdAt,
    }],
  };
  writeFileSync(
    path.join(segments, "segment-00000001.jsonl"),
    `${JSON.stringify(batch)}\n`,
    { mode: 0o600 },
  );
  const checkpointPath = path.join(index, "checkpoint.json");
  writeJson(checkpointPath, { version: 1, stale: true });
  return {
    checkpointPath,
    findingId: "delivery-ledger.index.consistency",
  };
}

function staleInboundPolicyArtifactFixture(root) {
  const policies = path.join(root, "inbound-policies");
  mkdirSync(policies, { recursive: true, mode: 0o700 });
  const targetNodeKey = "codex:93345678-2234-4234-8234-123456789abc";
  const prefix = createHash("sha256").update(targetNodeKey).digest("hex");
  const artifactName =
    `${prefix}.json.a3345678-2234-4234-8234-123456789abc.tmp`;
  const artifactPath = path.join(policies, artifactName);
  const contents = '{"partial":true}\n';
  writeFileSync(artifactPath, contents, { mode: 0o600 });
  const stale = new Date(Date.now() - 60_000);
  utimesSync(artifactPath, stale, stale);
  return {
    artifactName,
    artifactPath,
    contents,
    findingId: "inbound-policies.entries",
  };
}

test("Repair plan is read-only and exact apply emits a recoverable receipt", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-cluster-"));
  try {
    const fixture = clusterRedoFixture(root);
    const before = readFileSync(fixture.clusterPath, "utf8");
    const planResult = runCxmsg(root, [
      "repair",
      "plan",
      fixture.findingId,
      "--json",
    ]);
    assert.equal(planResult.status, 0, planResult.stderr);
    const plan = JSON.parse(planResult.stdout);
    assert.equal(plan.repairKind, "cluster-membership-redo");
    assert.match(plan.planDigest, /^[0-9a-f]{64}$/);
    assert.equal(readFileSync(fixture.clusterPath, "utf8"), before);
    assert.equal(existsSync(path.join(root, "repairs")), false);

    const originalNext = readFileSync(fixture.nextMembershipPath, "utf8");
    const changedNext = JSON.parse(originalNext);
    changedNext.createdAt = "2026-08-15T00:02:00.000Z";
    writeJson(fixture.nextMembershipPath, changedNext);
    const stale = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(stale.status, 1);
    assert.match(stale.stderr, /plan changed/);
    assert.equal(existsSync(path.join(root, "repairs")), false);
    writeFileSync(fixture.nextMembershipPath, originalNext, { mode: 0o600 });

    const rejected = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      "0".repeat(64),
      "--json",
    ]);
    assert.equal(rejected.status, 1);
    assert.match(rejected.stderr, /plan changed/);
    assert.equal(existsSync(path.join(root, "repairs")), false);

    const applied = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(applied.status, 0, applied.stderr);
    const receipt = JSON.parse(applied.stdout);
    assert.equal(receipt.status, "completed");
    assert.equal(receipt.repairKind, "cluster-membership-redo");
    const cluster = JSON.parse(readFileSync(fixture.clusterPath, "utf8"));
    assert.equal(cluster.membershipVersion, 2);
    assert.equal(cluster.members.length, 1);

    const transaction = path.join(
      root,
      "repairs",
      "transactions",
      receipt.transactionId,
    );
    const receiptFile = path.join(
      root,
      "repairs",
      "receipts",
      `${receipt.transactionId}.json`,
    );
    assert.ok(existsSync(path.join(transaction, "cluster-head.json")));
    assert.ok(existsSync(path.join(transaction, "next-membership.json")));
    assert.equal(statSync(receiptFile).mode & 0o077, 0);
    assert.doesNotMatch(readFileSync(receiptFile, "utf8"), /private\/repair-project/);
    const doctor = inspectRepairState({ stateDir: root });
    assert.equal(
      doctor.find((check) =>
        check.id === `repairs.transaction.${receipt.transactionId.slice(0, 8)}.consistency`
      )?.status,
      "pass",
    );

    const replay = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(replay.status, 1);
    assert.match(replay.stderr, /not a current Cluster membership redo/);
    assert.equal(
      readdirSync(path.join(root, "repairs", "receipts")).length,
      1,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair Doctor reports a durable nonterminal transaction without mutating it", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-incomplete-"));
  try {
    const transactionId = "82345678-2234-4234-8234-123456789abc";
    const transaction = path.join(root, "repairs", "transactions", transactionId);
    const receipts = path.join(root, "repairs", "receipts");
    mkdirSync(transaction, { recursive: true, mode: 0o700 });
    mkdirSync(path.join(transaction, "index"), { mode: 0o700 });
    mkdirSync(receipts, { recursive: true, mode: 0o700 });
    const planBase = {
      schemaVersion: 1,
      findingId: "delivery-ledger.index.consistency",
      errorCode: "ELEDGERINDEXSTALE",
      repairKind: "delivery-ledger-index-rebuild",
      mutationCategory: "rebuildable-delivery-index",
      target: { kind: "delivery-ledger-index" },
      evidenceSha256: "1".repeat(64),
      recoverability: "ledger-truth-and-owner-private-index-backup",
      automatic: false,
    };
    const plan = {
      ...planBase,
      planDigest: createHash("sha256")
        .update(JSON.stringify(planBase))
        .digest("hex"),
    };
    writeJson(path.join(transaction, "manifest.json"), {
      schemaVersion: 1,
      transactionId,
      phase: "mutation-started",
      plan,
      backup: { kind: "delivery-ledger-index", files: [] },
      startedAt: "2026-08-16T00:00:00.000Z",
      updatedAt: "2026-08-16T00:01:00.000Z",
    });
    const before = readFileSync(path.join(transaction, "manifest.json"), "utf8");
    const checks = inspectRepairState({ stateDir: root });
    assert.ok(checks.some((check) => check.errorCode === "EREPAIRINCOMPLETE"));
    assert.equal(
      readFileSync(path.join(transaction, "manifest.json"), "utf8"),
      before,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair apply rebuilds only a stale cache and backs up its prior generation", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-index-"));
  try {
    const fixture = staleIndexFixture(root);
    const staleCheckpoint = readFileSync(fixture.checkpointPath, "utf8");
    const planResult = runCxmsg(root, [
      "repair",
      "plan",
      fixture.findingId,
      "--json",
    ]);
    assert.equal(planResult.status, 0, planResult.stderr);
    const plan = JSON.parse(planResult.stdout);
    assert.equal(plan.repairKind, "delivery-ledger-index-rebuild");
    assert.equal(readFileSync(fixture.checkpointPath, "utf8"), staleCheckpoint);
    assert.equal(existsSync(path.join(root, "repairs")), false);

    const applied = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(applied.status, 0, applied.stderr);
    const receipt = JSON.parse(applied.stdout);
    assert.equal(receipt.status, "completed");
    assert.equal(receipt.result.messageCount, 1);
    const checkpoint = JSON.parse(readFileSync(fixture.checkpointPath, "utf8"));
    assert.equal(checkpoint.version, 1);
    assert.equal(checkpoint.messageCount, 1);
    assert.match(checkpoint.manifestSha256, /^[0-9a-f]{64}$/);
    const backup = path.join(
      root,
      "repairs",
      "transactions",
      receipt.transactionId,
      "index",
      "checkpoint.json",
    );
    assert.equal(readFileSync(backup, "utf8"), staleCheckpoint);

    const replay = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(replay.status, 1);
    assert.match(replay.stderr, /not a current stale Delivery Ledger index/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair removes only exact stale Inbound Policy artifacts after durable backup", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-policy-"));
  try {
    const fixture = staleInboundPolicyArtifactFixture(root);
    const planResult = runCxmsg(root, [
      "repair",
      "plan",
      fixture.findingId,
      "--json",
    ]);
    assert.equal(planResult.status, 0, planResult.stderr);
    const plan = JSON.parse(planResult.stdout);
    assert.equal(plan.repairKind, "inbound-policy-stale-artifact-purge");
    assert.equal(plan.artifactCount, 1);
    assert.equal(readFileSync(fixture.artifactPath, "utf8"), fixture.contents);
    assert.equal(existsSync(path.join(root, "repairs")), false);

    writeFileSync(fixture.artifactPath, '{"partial":"changed"}\n', {
      mode: 0o600,
    });
    const stale = new Date(Date.now() - 60_000);
    utimesSync(fixture.artifactPath, stale, stale);
    const rejected = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(rejected.status, 1);
    assert.match(rejected.stderr, /plan changed/);
    assert.equal(existsSync(path.join(root, "repairs")), false);

    writeFileSync(fixture.artifactPath, fixture.contents, { mode: 0o600 });
    utimesSync(fixture.artifactPath, stale, stale);
    const currentPlan = JSON.parse(
      runCxmsg(root, ["repair", "plan", fixture.findingId, "--json"]).stdout,
    );
    const applied = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      currentPlan.planDigest,
      "--json",
    ]);
    assert.equal(applied.status, 0, applied.stderr);
    const receipt = JSON.parse(applied.stdout);
    assert.equal(receipt.status, "completed");
    assert.equal(receipt.result.removed, 1);
    assert.equal(existsSync(fixture.artifactPath), false);
    const backup = path.join(
      root,
      "repairs",
      "transactions",
      receipt.transactionId,
      "inbound-policy-artifacts",
      fixture.artifactName,
    );
    assert.equal(readFileSync(backup, "utf8"), fixture.contents);
    assert.equal(
      inspectRepairState({ stateDir: root })
        .find((check) =>
          check.id ===
            `repairs.transaction.${receipt.transactionId.slice(0, 8)}.consistency`
        )?.status,
      "pass",
    );

    const replay = runCxmsg(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      currentPlan.planDigest,
      "--json",
    ]);
    assert.equal(replay.status, 1);
    assert.match(replay.stderr, /not a current stale Inbound Policy artifact/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair refuses to hide an unexpected Inbound Policy entry behind stale cleanup", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-policy-mixed-"));
  try {
    const fixture = staleInboundPolicyArtifactFixture(root);
    writeFileSync(
      path.join(root, "inbound-policies", "unexpected-entry"),
      "unrecognized\n",
      { mode: 0o600 },
    );
    const plan = runCxmsg(root, [
      "repair",
      "plan",
      fixture.findingId,
      "--json",
    ]);
    assert.equal(plan.status, 1);
    assert.match(plan.stderr, /requires only recognized stale artifacts/);
    assert.equal(existsSync(fixture.artifactPath), true);
    assert.equal(existsSync(path.join(root, "repairs")), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair preserves a post-lock stale error when failure recording also fails", async () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-race-"));
  let running = null;
  try {
    const fixture = clusterRedoFixture(root);
    const planResult = runCxmsg(root, [
      "repair",
      "plan",
      fixture.findingId,
      "--json",
    ]);
    assert.equal(planResult.status, 0, planResult.stderr);
    const plan = JSON.parse(planResult.stdout);
    const clusterLock = path.join(root, "directory", "clusters.lock");
    writeJson(clusterLock, {
      version: 1,
      pid: process.pid,
      token: "repair-race-test-owner",
      createdAt: Date.now(),
    });

    running = runCxmsgAsync(root, [
      "repair",
      "apply",
      fixture.findingId,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    const transactionId = await waitFor(() => {
      const transactions = path.join(root, "repairs", "transactions");
      if (!existsSync(transactions)) return null;
      return readdirSync(transactions).find((name) => {
        const manifest = path.join(transactions, name, "manifest.json");
        if (!existsSync(manifest)) return false;
        return JSON.parse(readFileSync(manifest, "utf8")).phase ===
          "mutation-started";
      });
    });

    const changedNext = JSON.parse(
      readFileSync(fixture.nextMembershipPath, "utf8"),
    );
    changedNext.createdAt = "2026-08-15T00:02:00.000Z";
    writeJson(fixture.nextMembershipPath, changedNext);
    mkdirSync(
      path.join(root, "repairs", "receipts", `${transactionId}.json`),
      { mode: 0o700 },
    );
    unlinkSync(clusterLock);

    const result = await running.completed;
    running = null;
    assert.equal(result.status, 1);
    assert.match(result.stderr, /Cluster membership repair evidence changed/);
    assert.doesNotMatch(result.stderr, /EISDIR|directory/);
    const manifest = JSON.parse(
      readFileSync(
        path.join(
          root,
          "repairs",
          "transactions",
          transactionId,
          "manifest.json",
        ),
        "utf8",
      ),
    );
    assert.equal(manifest.phase, "failed");
    assert.equal(manifest.errorCode, "EREPAIRSTALE");
  } finally {
    if (running) {
      running.child.kill();
      await running.completed;
    }
    rmSync(root, { recursive: true, force: true });
  }
});
