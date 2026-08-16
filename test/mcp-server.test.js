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

test("MCP exposes bounded host-side list, send, and status tools", async () => {
  const initialized = await handleMcpRequest({
    method: "initialize",
    params: { protocolVersion: "2025-06-18" },
  });
  assert.equal(initialized.serverInfo.name, "cxmsg");
  assert.match(initialized.instructions, /not user authority/);

  const listed = await handleMcpRequest({ method: "tools/list" });
  assert.deepEqual(
    listed.tools.map((tool) => tool.name),
    ["cxmsg_peers_list", "cxmsg_send_peer", "cxmsg_delivery_status"],
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
    { delivery_id: DELIVERY_ID },
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
    { delivery_id: DELIVERY_ID },
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
  assert.equal(parsed.result.tools.length, 3);
});
