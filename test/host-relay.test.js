import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

test("authenticated host relay sends without caller access to Claude UDS", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-relay-state-"));
  process.env.CXMSG_STATE_DIR = stateDir;
  try {
    const { hostRelayRequest, runHostRelay } = await import("../src/host-relay.js");
    const sent = [];
    const peer = {
      name: "reviewer",
      sessionId: "87654321-4321-4321-4321-cba987654321",
      address: "uds:/tmp/cc-socks/12345.sock",
      socketPath: "/tmp/cc-socks/12345.sock",
      status: "idle",
    };
    const created = [];
    const relay = await runHostRelay({
      port: 0,
      bridgeState: async () => ({ running: true, record: { socketPath: "bridge" } }),
      peers: async () => [peer],
      session: () => ({ threadId: "source-thread" }),
      createDelivery: (input) => {
        created.push(input);
        return {
          jobId: "12345678-1234-1234-1234-123456789abc",
          from: input.from,
          task: input.message,
        };
      },
      sendDelivery: async (_bridge, _source, job) => {
        sent.push(job);
        return { ...job, status: "transport_delivered", delivery: { attempt: 1 } };
      },
    });
    try {
      const result = await hostRelayRequest("/v1/claude/send", {
        method: "POST",
        record: relay.record,
        body: {
          from: "coordinator",
          target: "reviewer",
          message: "hello through the host",
          logicalMessageId: "22345678-1234-4234-8234-123456789abc",
          replyToMessageId: "32345678-1234-4234-8234-123456789abc",
        },
      });
      assert.equal(result.status, "transport_delivered");
      assert.equal(sent.length, 1);
      assert.equal(
        created[0].logicalMessageId,
        "22345678-1234-4234-8234-123456789abc",
      );
      assert.equal(
        created[0].replyToMessageId,
        "32345678-1234-4234-8234-123456789abc",
      );

      await assert.rejects(
        hostRelayRequest("/v1/claude/send", {
          method: "POST",
          record: relay.record,
          body: {
            from: "coordinator",
            target: "reviewer",
            message: "a".repeat(16 * 1024 + 1),
          },
        }),
        /message exceeds 16384 bytes/,
      );
      assert.equal(sent.length, 1);

      await assert.rejects(
        hostRelayRequest("/health", {
          record: { ...relay.record, token: "wrong-token" },
        }),
        /unauthorized/,
      );

      await assert.rejects(
        hostRelayRequest("/health", {
          record: { ...relay.record, port: 1 },
        }),
        (error) => {
          assert.notEqual(error.message, "fetch failed");
          assert.ok(error.code);
          return true;
        },
      );
    } finally {
      await new Promise((resolve) => relay.server.close(resolve));
    }
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
  }
});
