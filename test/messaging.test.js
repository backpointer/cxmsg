import assert from "node:assert/strict";
import { createHash } from "node:crypto";
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
  splitUtf8,
  storedSessionName,
  truncateUtf8,
  validateMessage,
  validateStoredMessage,
  validateSessionName,
} from "../src/messaging.js";

test("session names are validated and stored in a private namespace", () => {
  assert.equal(storedSessionName("worker-1"), "cxmsg:worker-1");
  assert.equal(displaySessionName("cxmsg:worker-1"), "worker-1");
  assert.equal(displaySessionName("ordinary thread"), null);
  assert.equal(validateSessionName(`a${"b".repeat(63)}`).length, 64);
  assert.throws(() => validateSessionName("bad name"));
  assert.throws(() => validateSessionName(`a${"b".repeat(64)}`));
  assert.throws(() => validateSessionName("_worker"));
});

test("message byte limits and UTF-8 truncation preserve character boundaries", () => {
  assert.equal(validateMessage("a".repeat(16 * 1024)).length, 16 * 1024);
  assert.throws(() => validateMessage("a".repeat(16 * 1024 + 1)));
  assert.equal(validateStoredMessage("a".repeat(256 * 1024)).length, 256 * 1024);
  assert.throws(() => validateStoredMessage("a".repeat(256 * 1024 + 1)));
  const truncated = truncateUtf8("가".repeat(10), 10);
  assert.equal(Buffer.byteLength(truncated, "utf8"), 9);
  assert.doesNotMatch(truncated, /\uFFFD/);
  const parts = splitUtf8("가".repeat(10), 10);
  assert.deepEqual(parts.map((part) => Buffer.byteLength(part, "utf8")), [9, 9, 9, 3]);
  assert.equal(parts.join(""), "가".repeat(10));
  assert.throws(() => splitUtf8("가", 2), /too small/);
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
  const route = {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: "11345678-1234-4234-8234-123456789abc",
    payload_type: "coordination",
    wake_policy: "immediate",
  };
  const result = peerMessageInput({
    from: "alpha",
    message: "tests passed",
    messageId: "message-1",
    route,
  });
  assert.equal(result.additionalContext["cxmsg:message-1"].kind, "untrusted");
  assert.match(result.additionalContext["cxmsg:message-1"].value, /tests passed/);
  assert.deepEqual(
    JSON.parse(result.additionalContext["cxmsg:message-1"].value).route,
    route,
  );
  assert.match(result.input[0].text, /not user consent/);
});

test("large peer input is split into ordered verifiable context fragments", () => {
  const message = `heading\n${"가나다라마바사".repeat(500)}\ntrailer`;
  const result = peerMessageInput({
    from: "alpha",
    message,
    messageId: "message-large",
  });
  const values = Object.values(result.additionalContext).map(({ value }) =>
    JSON.parse(value),
  );

  assert.ok(values.length > 1);
  assert.deepEqual(
    values.map((value) => value.fragment.index),
    values.map((_, index) => index + 1),
  );
  assert.ok(values.every((value) => value.fragment.total === values.length));
  assert.ok(
    values.every(
      (value) => Buffer.byteLength(value.message, "utf8") <= 2 * 1024,
    ),
  );
  assert.equal(values.map((value) => value.message).join(""), message);
  assert.ok(values.every((value) => value.fragment.messageBytes === Buffer.byteLength(message, "utf8")));
  assert.ok(values.every((value) => value.fragment.messageSha256 === values[0].fragment.messageSha256));
  assert.match(result.input[0].text, new RegExp(`${values.length} ordered cxmsg fragments`));
});

test("stored peer input carries a bounded preview and verifiable opaque reference", () => {
  const messageId = "62345678-1234-4234-8234-123456789abc";
  const message = "a".repeat(20 * 1024);
  const bodySha256 = createHash("sha256").update(message).digest("hex");
  const result = peerMessageInput({
    from: "alpha",
    message,
    messageId,
    bodyReference: {
      messageId,
      contentRef: `cxmsg-message:${messageId}`,
      bodyBytes: Buffer.byteLength(message, "utf8"),
      bodySha256,
    },
  });
  const [context] = Object.values(result.additionalContext).map(({ value }) =>
    JSON.parse(value),
  );

  assert.equal(Object.keys(result.additionalContext).length, 1);
  assert.equal(context.body.contentRef, `cxmsg-message:${messageId}`);
  assert.equal(context.body.bytes, 20 * 1024);
  assert.equal(context.body.sha256, bodySha256);
  assert.equal(Buffer.byteLength(context.message, "utf8"), 2 * 1024);
  assert.doesNotMatch(JSON.stringify(context), new RegExp(`a{${16 * 1024}}`));
  assert.match(result.input[0].text, /cxmsg message show/);
  assert.throws(
    () => peerMessageInput({ from: "alpha", message, messageId }),
    /matching stored body reference/,
  );
});

test("large delivery persists the body before starting a turn", async () => {
  const calls = [];
  const stored = [];
  const messageId = "72345678-1234-4234-8234-123456789abc";
  const message = "body ".repeat(4_000);
  const bodySha256 = createHash("sha256").update(message).digest("hex");
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return { turn: { id: "turn-large" } };
    },
  };
  const result = await deliverPeerMessage(
    client,
    { id: "thread-beta", name: "cxmsg:beta", status: { type: "idle" }, turns: [] },
    { from: "alpha", message, messageId },
    {
      async storeBody(value) {
        stored.push(value);
        return {
          messageId,
          contentRef: `cxmsg-message:${messageId}`,
          bodyBytes: Buffer.byteLength(message, "utf8"),
          bodySha256,
        };
      },
    },
  );

  assert.deepEqual(stored, [{ messageId, body: message }]);
  assert.equal(calls[0].method, "turn/start");
  assert.equal(result.messageId, messageId);
  const context = JSON.parse(
    calls[0].params.additionalContext[`cxmsg:${messageId}`].value,
  );
  assert.equal(context.body.contentRef, `cxmsg-message:${messageId}`);
  assert.ok(Buffer.byteLength(context.message, "utf8") < Buffer.byteLength(message, "utf8"));
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

test("delegation can select an interactive approval policy", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return { turn: { id: "turn-relay" } };
    },
  };
  await deliverDelegatedTask(
    client,
    { id: "thread-worker", status: { type: "idle" }, turns: [] },
    {
      from: "coordinator",
      target: "worker",
      task: "inspect",
      jobId: "12345678-1234-1234-1234-123456789abf",
      approvalPolicy: "on-request",
    },
  );
  assert.equal(calls[0].params.approvalPolicy, "on-request");
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
