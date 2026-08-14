import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test, { after } from "node:test";
import {
  evaluateClaudeBridgeRecord,
  probeClaudeBridge,
} from "../src/claude-bridge.js";
import { claudeSocketsDir } from "../src/claude-messaging.js";

const TEST_CLAUDE_SOCKETS_DIR = await fs.mkdtemp(
  path.join(os.tmpdir(), "cxmsg-bridge-sockets-"),
);
process.env.CXMSG_CLAUDE_SOCKETS_DIR = TEST_CLAUDE_SOCKETS_DIR;
after(async () => {
  delete process.env.CXMSG_CLAUDE_SOCKETS_DIR;
  await fs.rm(TEST_CLAUDE_SOCKETS_DIR, { recursive: true, force: true });
});

test("bridge identity handshake supports EPERM status and rejects mismatches", async () => {
  const pid = Number(`${process.pid}${String(Date.now()).slice(-5)}`);
  const socketsDir = claudeSocketsDir();
  const socketPath = path.join(socketsDir, `${pid}.sock`);
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
  await fs.mkdir(socketsDir, { recursive: true, mode: 0o700 });
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
