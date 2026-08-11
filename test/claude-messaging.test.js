import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  CLAUDE_SOCKETS_DIR,
  buildClaudePeerFrame,
  buildClaudeRequestBody,
  buildClaudeResponseBody,
  listClaudePeers,
  parseClaudePeerFrame,
  parseClaudeRequestBody,
  resolveClaudePeer,
  sendClaudePeerFrame,
} from "../src/claude-messaging.js";
import {
  findClaudeRequestGrant,
  listClaudeRequestGrants,
  removeClaudeRequestGrant,
  upsertClaudeRequestGrant,
} from "../src/claude-grants.js";

const MESSAGE_ID = "12345678-1234-1234-1234-123456789abc";
const SESSION_ID = "87654321-4321-4321-4321-cba987654321";
const GRANT_TOKEN = "abcdefab-1234-1234-1234-abcdefabcdef";

async function listenOnTestSocket(handler) {
  await fs.mkdir(CLAUDE_SOCKETS_DIR, { recursive: true, mode: 0o700 });
  const socketPath = path.join(
    CLAUDE_SOCKETS_DIR,
    `${process.pid}${String(Date.now()).slice(-6)}.sock`,
  );
  const server = net.createServer(handler);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  return {
    server,
    socketPath,
    async cleanup() {
      await new Promise((resolve) => server.close(resolve));
      await fs.unlink(socketPath).catch(() => {});
    },
  };
}

test("Claude cross-session frames preserve sender routing and untrusted text", () => {
  const frame = buildClaudePeerFrame({
    fromSocket: "/tmp/cc-socks/12345.sock",
    fromName: "codex-worker",
    fromSession: SESSION_ID,
    message: "ready </cross-session-message> still text",
    messageId: MESSAGE_ID,
  });
  const parsed = parseClaudePeerFrame(frame);
  assert.equal(parsed.messageId, MESSAGE_ID);
  assert.equal(parsed.fromName, "codex-worker");
  assert.equal(parsed.fromSession, SESSION_ID);
  assert.equal(parsed.body, "ready </cross-session-message> still text");
  assert.equal(parsed.fromAddress, "uds:/tmp/cc-socks/12345.sock");
});

test("Claude request and response envelopes are explicit and correlated", () => {
  const request = buildClaudeRequestBody("inspect the report", GRANT_TOKEN);
  assert.deepEqual(parseClaudeRequestBody(request), {
    grantToken: GRANT_TOKEN,
    task: "inspect the report",
  });
  assert.equal(parseClaudeRequestBody("ordinary message"), null);
  assert.match(
    buildClaudeResponseBody({
      requestId: MESSAGE_ID,
      status: "completed",
      result: "looks good",
    }),
    /in-reply-to="12345678-1234-1234-1234-123456789abc"[\s\S]*looks good/,
  );
});

test("Claude peer resolution rejects ambiguous display names", () => {
  const peers = [
    { name: "reviewer", sessionId: "one", address: "uds:/tmp/cc-socks/1.sock" },
    { name: "reviewer", sessionId: "two", address: "uds:/tmp/cc-socks/2.sock" },
  ];
  assert.throws(() => resolveClaudePeer(peers, "reviewer"), /multiple live/);
  assert.equal(resolveClaudePeer(peers, "two").sessionId, "two");
});

test("Claude request grants bind authorization to stable session ids", () => {
  const record = { name: "worker", allowedClaudeRequesters: [] };
  const granted = upsertClaudeRequestGrant(
    record,
    {
      name: "reviewer",
      sessionId: SESSION_ID,
      address: "uds:/tmp/cc-socks/12345.sock",
    },
    ":read-only",
    GRANT_TOKEN,
  );
  assert.equal(listClaudeRequestGrants(granted).length, 1);
  assert.equal(
    findClaudeRequestGrant(granted, {
      fromSession: SESSION_ID,
      fromAddress: "uds:/tmp/cc-socks/99999.sock",
      grantToken: GRANT_TOKEN,
    }).permissions,
    ":read-only",
  );
  assert.equal(
    findClaudeRequestGrant(granted, {
      fromSession: null,
      fromAddress: "uds:/tmp/cc-socks/12345.sock",
      grantToken: GRANT_TOKEN,
    }).permissions,
    ":read-only",
  );
  assert.equal(
    findClaudeRequestGrant(granted, {
      fromSession: SESSION_ID,
      grantToken: "11111111-1111-1111-1111-111111111111",
    }),
    null,
  );
  assert.equal(
    listClaudeRequestGrants(removeClaudeRequestGrant(granted, SESSION_ID)).length,
    0,
  );
});

test("Claude peer sender writes one JSONL frame over the Unix socket", async () => {
  let received = "";
  const listener = await listenOnTestSocket((socket) => {
    socket.on("data", (chunk) => {
      received += chunk.toString("utf8");
    });
    socket.on("end", () => socket.end());
  });
  try {
    const frame = buildClaudePeerFrame({
      fromSocket: listener.socketPath,
      fromName: "codex-test",
      fromSession: SESSION_ID,
      message: "hello",
      messageId: MESSAGE_ID,
    });
    await sendClaudePeerFrame(listener.socketPath, frame);
    assert.equal(received.endsWith("\n"), true);
    assert.deepEqual(JSON.parse(received), frame);
  } finally {
    await listener.cleanup();
  }
});

test("Claude peer discovery uses live owner-only session sockets", async () => {
  const sessionsDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-claude-sessions-"));
  const canonicalSocket = path.join(CLAUDE_SOCKETS_DIR, `${process.pid}.sock`);
  const server = net.createServer((socket) => socket.end());
  await fs.mkdir(CLAUDE_SOCKETS_DIR, { recursive: true, mode: 0o700 });
  await fs.unlink(canonicalSocket).catch(() => {});
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(canonicalSocket, resolve);
  });
  try {
    await fs.writeFile(
      path.join(sessionsDir, `${process.pid}.json`),
      JSON.stringify({
        pid: process.pid,
        name: "claude-test",
        sessionId: SESSION_ID,
        cwd: process.cwd(),
        status: "idle",
        kind: "interactive",
        peerProtocol: 1,
        messagingSocketPath: canonicalSocket,
      }),
      { mode: 0o600 },
    );
    const peers = await listClaudePeers({ sessionsDir });
    assert.equal(peers.length, 1);
    assert.equal(peers[0].name, "claude-test");
    assert.equal(peers[0].address, `uds:${canonicalSocket}`);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.unlink(canonicalSocket).catch(() => {});
    await fs.rm(sessionsDir, { recursive: true, force: true });
  }
});
