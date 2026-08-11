import assert from "node:assert/strict";
import test from "node:test";
import {
  delegatedTaskInput,
  deliverDelegatedTask,
  deliverPeerMessage,
  displaySessionName,
  peerMessageInput,
  peerThreads,
  finalTurnResult,
  resolveTarget,
  storedSessionName,
  validateSessionName,
} from "../src/messaging.js";

test("session names are validated and stored in a private namespace", () => {
  assert.equal(storedSessionName("worker-1"), "cxmsg:worker-1");
  assert.equal(displaySessionName("cxmsg:worker-1"), "worker-1");
  assert.equal(displaySessionName("ordinary thread"), null);
  assert.throws(() => validateSessionName("bad name"));
});

test("only cxmsg threads are discoverable as peers", () => {
  const threads = [
    { id: "a", name: "cxmsg:alpha" },
    { id: "b", name: "unrelated" },
  ];
  assert.deepEqual(peerThreads(threads).map((thread) => thread.peerName), [
    "alpha",
  ]);
  assert.equal(resolveTarget(threads, "alpha").id, "a");
  assert.throws(() => resolveTarget(threads, "missing"));
});

test("peer input is explicitly untrusted", () => {
  const result = peerMessageInput({
    from: "alpha",
    message: "tests passed",
    messageId: "message-1",
  });
  assert.equal(result.additionalContext["cxmsg:message-1"].kind, "untrusted");
  assert.match(result.additionalContext["cxmsg:message-1"].value, /tests passed/);
  assert.match(result.input[0].text, /not user consent/);
});

test("idle delivery starts a non-escalating turn", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return { turn: { id: "turn-new" } };
    },
  };
  const result = await deliverPeerMessage(
    client,
    {
      id: "thread-beta",
      name: "cxmsg:beta",
      status: { type: "idle" },
      turns: [],
    },
    { from: "alpha", message: "ready", messageId: "message-2" },
  );
  assert.equal(result.delivery, "started");
  assert.equal(calls[0].method, "turn/start");
  assert.equal(calls[0].params.approvalPolicy, "never");
  assert.equal(calls[0].params.additionalContext["cxmsg:message-2"].kind, "untrusted");
});

test("active delivery steers the current turn", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return { turnId: "turn-active" };
    },
  };
  const result = await deliverPeerMessage(
    client,
    {
      id: "thread-beta",
      name: "cxmsg:beta",
      status: { type: "active", activeFlags: [] },
      turns: [{ id: "turn-active", status: "inProgress" }],
    },
    { from: "alpha", message: "schema changed", messageId: "message-3" },
  );
  assert.equal(result.delivery, "steered");
  assert.equal(calls[0].method, "turn/steer");
  assert.equal(calls[0].params.expectedTurnId, "turn-active");
});

test("unloaded threads are resumed before delivery", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/resume") {
        return {
          thread: {
            id: "thread-beta",
            name: "cxmsg:beta",
            status: { type: "idle" },
            turns: [],
          },
        };
      }
      return { turn: { id: "turn-new" } };
    },
  };
  await deliverPeerMessage(
    client,
    {
      id: "thread-beta",
      name: "cxmsg:beta",
      status: { type: "notLoaded" },
      turns: [],
    },
    { from: "alpha", message: "wake up", messageId: "message-4" },
  );
  assert.deepEqual(calls.map((call) => call.method), [
    "thread/resume",
    "turn/start",
  ]);
});

test("delegated tasks are direct correlated user turns without approval override", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return { turn: { id: "turn-delegated" } };
    },
  };
  const result = await deliverDelegatedTask(
    client,
    {
      id: "thread-worker",
      status: { type: "idle" },
      turns: [],
    },
    {
      from: "coordinator",
      target: "worker",
      task: "run tests",
      jobId: "12345678-1234-1234-1234-123456789abc",
    },
  );
  assert.equal(result.turnId, "turn-delegated");
  assert.equal(calls[0].method, "turn/start");
  assert.equal(calls[0].params.clientUserMessageId, result.jobId);
  assert.equal(calls[0].params.approvalPolicy, undefined);
  assert.equal(calls[0].params.sandboxPolicy, undefined);
  assert.match(calls[0].params.input[0].text, /run tests/);
});

test("delegation can select an explicit named permission profile", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return { turn: { id: "turn-read-only" } };
    },
  };
  await deliverDelegatedTask(
    client,
    { id: "thread-worker", status: { type: "idle" }, turns: [] },
    {
      from: "coordinator",
      target: "worker",
      task: "inspect only",
      jobId: "12345678-1234-1234-1234-123456789abd",
      permissions: ":read-only",
    },
  );
  assert.equal(calls[0].params.permissions, ":read-only");
  assert.equal(calls[0].params.sandboxPolicy, undefined);
});

test("delegation refuses to merge into an active turn", async () => {
  const client = { request: async () => assert.fail("should not request") };
  await assert.rejects(
    deliverDelegatedTask(
      client,
      {
        id: "thread-worker",
        status: { type: "active" },
        turns: [{ id: "turn-active", status: "inProgress" }],
      },
      {
        from: "coordinator",
        target: "worker",
        task: "new task",
      },
    ),
    /active turn/,
  );
});

test("delegated task envelope and result extraction preserve correlation", () => {
  const delegated = delegatedTaskInput({
    from: "coordinator",
    target: "worker",
    task: "summarize",
    jobId: "12345678-1234-1234-1234-123456789abe",
  });
  assert.match(delegated.input[0].text, /12345678-1234-1234-1234-123456789abe/);
  assert.equal(
    finalTurnResult({
      items: [
        { type: "agentMessage", phase: "commentary", text: "working" },
        { type: "agentMessage", phase: "final_answer", text: "done" },
      ],
    }),
    "done",
  );
});
