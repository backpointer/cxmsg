import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import net from "node:net";
import path from "node:path";
import test from "node:test";
import {
  evaluateClaudeBridgeRecord,
  probeClaudeBridge,
} from "../src/claude-bridge.js";
import { CLAUDE_SOCKETS_DIR } from "../src/claude-messaging.js";

test("bridge identity handshake supports EPERM status and rejects mismatches", async () => {
  const pid = Number(`${process.pid}${String(Date.now()).slice(-5)}`);
  const socketPath = path.join(CLAUDE_SOCKETS_DIR, `${pid}.sock`);
  const record = {
    target: "worker",
    targetThreadId: "thread-1",
    pid,
    socketPath,
    startedAt: 1234,
  };
  const server = net.createServer((socket) => {
    const chunks = [];
    socket.on("data", (chunk) => chunks.push(chunk));
    socket.on("end", () => {
      const request = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      socket.end(
        `${JSON.stringify({
          cxmsgHealth: 1,
          nonce: request.nonce,
          target: record.target,
          targetThreadId: record.targetThreadId,
          pid: record.pid,
          startedAt: record.startedAt,
        })}\n`,
      );
    });
  });
  await fs.mkdir(CLAUDE_SOCKETS_DIR, { recursive: true, mode: 0o700 });
  await fs.unlink(socketPath).catch(() => {});
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  await fs.chmod(socketPath, 0o600);
  try {
    assert.equal((await probeClaudeBridge(record)).state, "healthy");
    const state = await evaluateClaudeBridgeRecord(record, {
      processStateFn: () => "unverified",
    });
    assert.equal(state.running, true);
    assert.equal(state.safeToSignal, false);
    assert.equal(state.safeToRemove, false);
    assert.equal(
      (await probeClaudeBridge({ ...record, targetThreadId: "wrong-thread" })).state,
      "invalid",
    );
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.unlink(socketPath).catch(() => {});
  }
});
