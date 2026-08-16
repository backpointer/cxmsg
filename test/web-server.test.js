import assert from "node:assert/strict";
import test from "node:test";
import {
  buildWebSnapshot,
  liveAttachment,
  publicJob,
  startWebServer,
} from "../src/web-server.js";
import { buildTopology, edgeTone, focusProject } from "../web/topology.js";

test("web job snapshots omit task, result, error, and capability material", () => {
  const job = publicJob({
    jobId: "12345678-1234-4123-8123-123456789abc",
    kind: "claude-request",
    from: "claude-reviewer",
    target: "worker",
    status: "completed",
    permissions: ":read-only",
    targetThreadId: "target-thread",
    threadId: "execution-thread",
    turnId: "turn-1",
    task: "private task body",
    result: "private result body",
    error: "private error body",
    source: {
      name: "claude-reviewer",
      sessionId: "87654321-4321-4321-8321-123456789abc",
      address: "uds:/private/socket",
      grantToken: "capability-secret",
    },
    reply: { status: "delivered", error: "private reply error" },
    createdAt: "2026-08-12T00:00:00.000Z",
    updatedAt: "2026-08-12T00:01:00.000Z",
    completedAt: "2026-08-12T00:01:00.000Z",
  });

  assert.equal(job.hasResult, true);
  assert.equal(job.hasError, true);
  assert.equal(job.replyStatus, "delivered");
  assert.equal(job.sourceName, "claude-reviewer");
  assert.equal(job.sourceSessionId, "87654321-4321-4321-8321-123456789abc");
  assert.equal("task" in job, false);
  assert.equal("result" in job, false);
  assert.equal("error" in job, false);
  assert.equal("source" in job, false);
  assert.equal("reply" in job, false);
  assert.doesNotMatch(JSON.stringify(job), /private|capability-secret|uds:/);
});

test("web snapshot redacts Claude addresses, grant hints, and App Server errors", async () => {
  const secretToken = "92345678-1234-4123-8123-123456789abc";
  const snapshot = await buildWebSnapshot({
    connect: async (callback) =>
      callback({
        request: async (method) => {
          if (method === "thread/read") throw new Error("private App Server error");
          if (method === "permissionProfile/list") return { data: [] };
          assert.fail(`unexpected request: ${method}`);
        },
      }),
    sessions: () => [
      {
        name: "worker",
        threadId: "thread-1",
        cwd: "/project",
        allowedClaudeRequesters: [
          {
            sessionId: "87654321-4321-4321-8321-123456789abc",
            name: "reviewer",
            address: "uds:/private/grant.sock",
            permissions: ":read-only",
            token: secretToken,
            grantedAt: "2026-08-12T00:00:00.000Z",
          },
        ],
      },
    ],
    claudePeers: async () => [
      {
        pid: 123,
        name: "reviewer",
        sessionId: "87654321-4321-4321-8321-123456789abc",
        cwd: "/project",
        address: "uds:/private/peer.sock",
        socketPath: "/private/peer.sock",
      },
    ],
    jobs: () => [],
    deliveries: async () => [{
      logicalMessage: { messageId: "11345678-1234-4123-8123-123456789abc" },
      delivery: {
        inboundPolicy: {
          decision: "deny",
          reason: "sender_denied",
          targetNodeKey: "codex:21345678-1234-4123-8123-123456789abc",
          senderIdentityState: "verified",
          senderNodeKey: "claude:31345678-1234-4123-8123-123456789abc",
          senderProjectId: "41345678-1234-4123-8123-123456789abc",
          policyRevision: 2,
          policySha256: "d".repeat(64),
          ruleId: "51345678-1234-4123-8123-123456789abc",
          selectorKind: "sender-node",
          failClosed: false,
        },
      },
    }],
    inboundPolicies: () => [{ rules: [{}] }],
    appServerProbe: async () => ({ state: "healthy", errorCode: null }),
  });

  const encoded = JSON.stringify(snapshot);
  assert.doesNotMatch(encoded, /uds:|private App Server error/);
  assert.doesNotMatch(encoded, new RegExp(secretToken.slice(0, 8)));
  assert.equal(snapshot.codexSessions[0].hasError, true);
  assert.equal("address" in snapshot.codexSessions[0].claudeGrants[0], false);
  assert.equal("tokenHint" in snapshot.codexSessions[0].claudeGrants[0], false);
  assert.deepEqual(snapshot.inboundPolicy, {
    status: "available",
    configuredTargetCount: 1,
    configuredRuleCount: 1,
    evaluatedRecipientCount: 1,
    deniedRecipientCount: 1,
    continuedRecipientCount: 0,
    invalidEvidenceCount: 0,
    failClosedDenialCount: 0,
    countsByReason: { sender_denied: 1 },
  });
  assert.doesNotMatch(encoded, /21345678|31345678|41345678|51345678|policySha256|ruleId/);
});

test("web snapshot contains a bounded unavailable policy projection on read failure", async () => {
  const snapshot = await buildWebSnapshot({
    connect: async (callback) => callback({ request: async () => ({ data: [] }) }),
    sessions: () => [],
    claudePeers: async () => [],
    jobs: () => [],
    deliveries: async () => {
      throw new Error("private ledger path and token");
    },
    inboundPolicies: () => assert.fail("policy records are not read after ledger failure"),
    appServerProbe: async () => ({ state: "missing", errorCode: "ENOENT" }),
  });
  assert.equal(snapshot.inboundPolicy.status, "unavailable");
  assert.doesNotMatch(JSON.stringify(snapshot), /private ledger path|token/);
});

test("web attachment inspection is read-only and rejects stale identities", () => {
  const attachment = {
    version: 1,
    name: "worker",
    threadId: "thread-1",
    childPid: 123,
    parentPid: 122,
    cwd: "/project",
    startedAt: "2026-08-12T00:00:00.000Z",
  };
  let reads = 0;
  const read = () => {
    reads += 1;
    return attachment;
  };
  assert.equal(
    liveAttachment("worker", {
      read,
      state: () => "missing",
      identity: () => assert.fail("missing processes have no identity probe"),
    }),
    null,
  );
  assert.equal(
    liveAttachment("worker", {
      read,
      state: () => "alive",
      identity: () => ({ state: "matched", command: "unrelated process" }),
    }),
    null,
  );
  assert.equal(
    liveAttachment("worker", {
      read,
      state: () => "unverified",
      identity: () => ({ state: "unavailable", command: null }),
    }),
    attachment,
  );
  assert.equal(reads, 3);
});

test("loopback web server separates dashboard, orchestration, and snapshot routes", async () => {
  const snapshot = {
    generatedAt: "2026-08-12T00:00:00.000Z",
    server: { running: true, pid: 42, transport: "unix", socketPresent: true },
    codexSessions: [],
    claudeSessions: [],
    jobs: [],
  };
  let snapshotCalls = 0;
  const web = await startWebServer({
    port: 0,
    snapshot: async () => {
      snapshotCalls += 1;
      return snapshot;
    },
  });
  try {
    const dashboard = await fetch(`${web.origin}/dashboard`);
    assert.equal(dashboard.status, 200);
    assert.match(await dashboard.text(), /Sessions and authority/);
    assert.match(dashboard.headers.get("content-security-policy"), /default-src 'self'/);

    const orchestration = await fetch(`${web.origin}/orchestration`);
    assert.equal(orchestration.status, 200);
    assert.match(await orchestration.text(), /Follow work/);

    const response = await fetch(`${web.origin}/api/snapshot`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), snapshot);
    const cached = await Promise.all([
      fetch(`${web.origin}/api/snapshot`),
      fetch(`${web.origin}/api/snapshot`),
    ]);
    assert.deepEqual(await cached[0].json(), snapshot);
    assert.deepEqual(await cached[1].json(), snapshot);
    assert.equal(snapshotCalls, 1);

    const rejected = await fetch(`${web.origin}/api/snapshot`, { method: "POST" });
    assert.equal(rejected.status, 405);
  } finally {
    await new Promise((resolve, reject) =>
      web.server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

test("topology groups sessions by project and aggregates correlation edges", () => {
  const topology = buildTopology({
    codexSessions: [
      { name: "coordinator", status: "idle", cwd: "/projects/control" },
      { name: "worker", status: "active", cwd: "/projects/stock" },
    ],
    claudeSessions: [
      {
        name: "stock-reviewer",
        sessionId: "87654321-4321-4321-8321-123456789abc",
        status: "idle",
        cwd: "/projects/stock",
      },
    ],
    jobs: [
      {
        from: "coordinator",
        target: "worker",
        status: "completed",
        updatedAt: "2026-08-12T00:01:00.000Z",
      },
      {
        from: "coordinator",
        target: "worker",
        status: "running",
        updatedAt: "2026-08-12T00:02:00.000Z",
      },
      {
        from: "claude-stock-reviewer",
        sourceName: "stock-reviewer",
        sourceSessionId: "87654321-4321-4321-8321-123456789abc",
        target: "worker",
        status: "completed",
        updatedAt: "2026-08-12T00:03:00.000Z",
      },
    ],
  });

  assert.deepEqual(topology.projects.map((project) => project.label), ["control", "stock"]);
  assert.equal(topology.projects.find((project) => project.label === "stock").nodes.length, 2);
  assert.equal(topology.edges.length, 2);
  const crossProject = topology.edges.find((edge) => edge.source.label === "coordinator");
  assert.equal(crossProject.count, 2);
  assert.notEqual(crossProject.source.projectKey, crossProject.target.projectKey);
  assert.equal(edgeTone(crossProject), "running");
  const sameProject = topology.edges.find((edge) => edge.source.label === "stock-reviewer");
  assert.equal(sameProject.source.projectKey, sameProject.target.projectKey);
  assert.equal(edgeTone(sameProject), "completed");

  const stock = focusProject(topology, "/projects/stock");
  assert.equal(stock.project.label, "stock");
  assert.equal(stock.localEdges.length, 1);
  assert.equal(stock.crossEdges.length, 1);
});
