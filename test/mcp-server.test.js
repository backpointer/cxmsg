import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import test from "node:test";
import {
  callCxmsgMcpTool,
  handleMcpRequest,
  runMcpStdio,
} from "../src/mcp-server.js";

const SESSION_ID = "87654321-4321-4321-4321-cba987654321";
const DELIVERY_ID = "12345678-1234-1234-1234-123456789abc";

function peer(overrides = {}) {
  return {
    name: "reviewer",
    sessionId: SESSION_ID,
    address: "uds:/tmp/cc-socks/12345.sock",
    socketPath: "/tmp/cc-socks/12345.sock",
    cwd: "/project",
    status: "idle",
    sessionStatus: "idle",
    verification: "socket",
    errorCode: null,
    ...overrides,
  };
}

test("MCP exposes bounded host-side list, send, status, and wait tools", async () => {
  const initialized = await handleMcpRequest({
    method: "initialize",
    params: { protocolVersion: "2025-06-18" },
  });
  assert.equal(initialized.serverInfo.name, "cxmsg");
  assert.match(initialized.instructions, /not user authority/);

  const listed = await handleMcpRequest({ method: "tools/list" });
  assert.deepEqual(
    listed.tools.map((tool) => tool.name),
    [
      "cxmsg_peers_list",
      "cxmsg_send_peer",
      "cxmsg_delivery_status",
      "cxmsg_wait_delivery",
    ],
  );

  const sent = [];
  const dependencies = {
    peers: async () => [peer()],
    bridgeState: async () => ({
      running: true,
      record: { socketPath: "/tmp/cc-socks/99999.sock" },
    }),
    session: () => ({ threadId: "source-thread" }),
    createDelivery: ({ from, message, peer: target }) => ({
      jobId: DELIVERY_ID,
      kind: "claude-delivery",
      from,
      target: target.name,
      task: message,
      claudeTarget: { name: target.name },
      delivery: { attempt: 0, maxAttempts: 4 },
    }),
    sendDelivery: async (_bridge, _source, job) => {
      sent.push(job);
      return {
        ...job,
        status: "transport_delivered",
        delivery: {
          ...job.delivery,
          attempt: 1,
          transportStatus: "delivered",
        },
      };
    },
  };
  const delivery = await callCxmsgMcpTool(
    "cxmsg_send_peer",
    {
      from_session: "coordinator",
      target_session: SESSION_ID,
      message: "diagnostic coordination only",
    },
    dependencies,
  );
  assert.equal(delivery.deliveryId, DELIVERY_ID);
  assert.equal(delivery.status, "transport_delivered");
  assert.equal(delivery.destinationAttempted, true);
  assert.equal(sent.length, 1);

  const status = await callCxmsgMcpTool(
    "cxmsg_delivery_status",
    { delivery_id: DELIVERY_ID, detail: "full" },
    {
      jobReader: () => ({
        ...sent[0],
        status: "completed",
        delivery: {
          attempt: 1,
          maxAttempts: 4,
          ackProtocolVersion: 2,
          targetSessionStatusAtAttempt: "idle",
          targetPeerProtocolAtAttempt: 1,
          transportStatus: "delivered",
          acceptedAt: "2026-08-16T00:00:00.000Z",
          completionDeadlineAt: "2026-08-16T00:15:00.000Z",
          nativeReceipts: [
            { messageId: DELIVERY_ID, status: "delivered" },
          ],
        },
        replyEvidence: { status: "correlated", messageId: DELIVERY_ID },
      }),
    },
  );
  assert.equal(status.status, "completed");
  assert.equal(status.ackProtocolVersion, 2);
  assert.equal(status.targetSessionStatusAtAttempt, "idle");
  assert.equal(status.targetPeerProtocolAtAttempt, 1);
  assert.equal(status.acceptedAt, "2026-08-16T00:00:00.000Z");
  assert.equal(
    status.completionDeadlineAt,
    "2026-08-16T00:15:00.000Z",
  );
  assert.equal(status.nativeReceipts[0].status, "delivered");
  assert.equal(status.replyEvidence.status, "correlated");

  const legacyTimeout = await callCxmsgMcpTool(
    "cxmsg_delivery_status",
    { delivery_id: DELIVERY_ID, detail: "full" },
    {
      jobReader: () => ({
        ...sent[0],
        status: "ack_timeout",
        error: "legacy timeout wording",
        delivery: {
          ...sent[0].delivery,
          attempt: 1,
          transportStatus: "delivered",
          errorCode: null,
        },
      }),
    },
  );
  assert.equal(legacyTimeout.ackProtocolVersion, null);
  assert.equal(legacyTimeout.errorCode, "EACKTIMEOUT");
  assert.match(legacyTimeout.error, /target work may still be in progress/);
});

test("MCP defaults to compact redacted projections and accepts retained content refs", async () => {
  const listed = await callCxmsgMcpTool(
    "cxmsg_peers_list",
    {},
    { peers: async () => [peer()] },
  );
  assert.deepEqual(listed, {
    peers: [
      {
        name: "reviewer",
        status: "idle",
        verification: "socket",
      },
    ],
  });
  assert.equal(JSON.stringify(listed).includes("/tmp/"), false);
  assert.equal(JSON.stringify(listed).includes("/project"), false);

  let storedMessage = null;
  const delivery = await callCxmsgMcpTool(
    "cxmsg_send_peer",
    {
      from_session: "coordinator",
      target_session: SESSION_ID,
      content_ref: "cxmsg-message:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    },
    {
      peers: async () => [peer()],
      bridgeState: async () => ({ running: true, record: {} }),
      session: () => ({ threadId: "source-thread" }),
      messageReader: () => "retained coordination text",
      createDelivery: ({ message, peer: target }) => {
        storedMessage = message;
        return {
          jobId: DELIVERY_ID,
          kind: "claude-delivery",
          from: "coordinator",
          target: target.name,
          claudeTarget: { name: target.name },
          delivery: { attempt: 0, maxAttempts: 4 },
        };
      },
      sendDelivery: async (_bridge, _source, job) => ({
        ...job,
        status: "transport_delivered",
        delivery: { attempt: 1, transportStatus: "delivered" },
      }),
    },
  );
  assert.equal(storedMessage, "retained coordination text");
  assert.deepEqual(delivery, {
    deliveryId: DELIVERY_ID,
    from: "coordinator",
    target: "reviewer",
    status: "transport_delivered",
    terminal: false,
    destinationAttempted: true,
    attempt: 1,
    transportStatus: "delivered",
  });
});

test("MCP waits once for a terminal delivery instead of model-driven polling", async () => {
  let reads = 0;
  const result = await callCxmsgMcpTool(
    "cxmsg_wait_delivery",
    { delivery_id: DELIVERY_ID, timeout_seconds: 1 },
    {
      jobReader: () => ({
        jobId: DELIVERY_ID,
        kind: "claude-delivery",
        from: "coordinator",
        target: "reviewer",
        status: reads++ === 0 ? "transport_delivered" : "completed",
        delivery: { attempt: 1, transportStatus: "delivered" },
      }),
      sleep: async () => {},
    },
  );
  assert.equal(reads, 2);
  assert.equal(result.status, "completed");
  assert.equal(result.terminal, true);
});

test("MCP wait is bounded and send requires exactly one body source", async () => {
  let timestamp = 0;
  const waiting = await callCxmsgMcpTool(
    "cxmsg_wait_delivery",
    { delivery_id: DELIVERY_ID, timeout_seconds: 1 },
    {
      jobReader: () => ({
        jobId: DELIVERY_ID,
        kind: "claude-delivery",
        from: "coordinator",
        target: "reviewer",
        status: "transport_delivered",
        delivery: { attempt: 1, transportStatus: "delivered" },
      }),
      now: () => timestamp,
      sleep: async (milliseconds) => {
        timestamp += milliseconds;
      },
    },
  );
  assert.equal(waiting.waitTimedOut, true);
  assert.equal(waiting.terminal, false);

  const baseArgs = {
    from_session: "coordinator",
    target_session: "reviewer",
  };
  await assert.rejects(
    callCxmsgMcpTool("cxmsg_send_peer", baseArgs),
    /exactly one/,
  );
  await assert.rejects(
    callCxmsgMcpTool("cxmsg_send_peer", {
      ...baseArgs,
      message: "inline",
      content_ref: "cxmsg-message:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    }),
    /exactly one/,
  );
  await assert.rejects(
    callCxmsgMcpTool(
      "cxmsg_wait_delivery",
      { delivery_id: DELIVERY_ID, timeout_seconds: 31 },
      { jobReader: () => assert.fail("invalid wait must not read a Job") },
    ),
    /1 to 30/,
  );
});

test("MCP rejects unreachable and ambiguous targets without sending", async () => {
  const base = {
    bridgeState: async () => ({ running: true, record: {} }),
    session: () => ({ threadId: "source-thread" }),
    createDelivery: () => assert.fail("delivery must not be created"),
    sendDelivery: () => assert.fail("delivery must not be attempted"),
  };
  await assert.rejects(
    callCxmsgMcpTool(
      "cxmsg_send_peer",
      {
        from_session: "coordinator",
        target_session: "reviewer",
        message: "hello",
      },
      { ...base, peers: async () => [peer({ status: "unreachable" })] },
    ),
    /unreachable/,
  );
  await assert.rejects(
    callCxmsgMcpTool(
      "cxmsg_send_peer",
      {
        from_session: "coordinator",
        target_session: "reviewer",
        message: "hello",
      },
      {
        ...base,
        peers: async () => [peer(), peer({ sessionId: "another-session" })],
      },
    ),
    /multiple live/,
  );
  await assert.rejects(
    callCxmsgMcpTool(
      "cxmsg_send_peer",
      {
        from_session: "coordinator",
        target_session: "reviewer",
        message: "a".repeat(16 * 1024 + 1),
      },
      { ...base, peers: async () => [peer()] },
    ),
    /message exceeds 16384 bytes/,
  );
  await assert.rejects(
    callCxmsgMcpTool(
      "cxmsg_send_peer",
      {
        from_session: "_coordinator",
        target_session: "reviewer",
        message: "hello",
      },
      { ...base, peers: async () => [peer()] },
    ),
    /session name/,
  );
});

test("MCP stdio emits JSON-RPC responses without protocol noise", async () => {
  const input = new PassThrough();
  const output = new PassThrough();
  let response = "";
  output.on("data", (chunk) => {
    response += chunk.toString("utf8");
  });
  const lines = runMcpStdio({ input, output });
  input.end(
    `${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} })}\n`,
  );
  await new Promise((resolve) => lines.once("close", resolve));
  await new Promise((resolve) => setImmediate(resolve));
  const parsed = JSON.parse(response.trim());
  assert.equal(parsed.id, 1);
  assert.equal(parsed.result.tools.length, 4);
});

test("MCP tool content is a compact summary while structured content remains usable", async () => {
  const response = await handleMcpRequest(
    {
      method: "tools/call",
      params: { name: "cxmsg_peers_list", arguments: {} },
    },
    { peers: async () => [peer()] },
  );
  assert.deepEqual(response.content, [{ type: "text", text: "1 Claude peers" }]);
  assert.equal(response.structuredContent.peers[0].name, "reviewer");
  assert.equal(response.structuredContent.peers[0].address, undefined);
});
