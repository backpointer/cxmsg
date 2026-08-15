import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test, { after } from "node:test";
import {
  buildClaudePeerFrame,
  buildClaudeRequestBody,
  buildClaudeResponseBody,
  claudeSocketsDir,
  listClaudePeers,
  parseClaudePeerFrame,
  parseClaudeRequestBody,
  redactClaudeRequestCapabilities,
  resolveClaudePeer,
  sendClaudePeerFrame,
} from "../src/claude-messaging.js";
import { handleClaudeRequestOrMessage } from "../src/claude-bridge.js";
import {
  findClaudeRequestGrant,
  listClaudeRequestGrants,
  publicClaudeRequestGrant,
  removeClaudeRequestGrant,
  upsertClaudeRequestGrant,
} from "../src/claude-grants.js";
import { failedProbe } from "../src/socket-probe.js";

const MESSAGE_ID = "12345678-1234-1234-1234-123456789abc";
const SESSION_ID = "87654321-4321-4321-4321-cba987654321";
const GRANT_TOKEN = "abcdefab-1234-1234-1234-abcdefabcdef";
const TEST_CLAUDE_SOCKETS_DIR = await fs.mkdtemp(
  path.join(os.tmpdir(), "cxmsg-claude-sockets-"),
);
process.env.CXMSG_CLAUDE_SOCKETS_DIR = TEST_CLAUDE_SOCKETS_DIR;
after(async () => {
  delete process.env.CXMSG_CLAUDE_SOCKETS_DIR;
  await fs.rm(TEST_CLAUDE_SOCKETS_DIR, { recursive: true, force: true });
});

async function listenOnTestSocket(handler) {
  const socketsDir = claudeSocketsDir();
  await fs.mkdir(socketsDir, { recursive: true, mode: 0o700 });
  const socketPath = path.join(
    socketsDir,
    `${process.pid}${String(Date.now()).slice(-6)}.sock`,
  );
  const server = net.createServer(handler);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  await fs.chmod(socketPath, 0o600);
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

test("Claude cross-session frames reject malformed stable sender identity", () => {
  const frame = buildClaudePeerFrame({
    fromSocket: "/tmp/cc-socks/12345.sock",
    fromName: "reviewer",
    fromSession: SESSION_ID,
    message: "hello",
  });
  frame.message.content = frame.message.content.replace(
    `from-session="${SESSION_ID}"`,
    'from-session="not-a-session"',
  );
  assert.throws(() => parseClaudePeerFrame(frame), /sender session is invalid/);
});

test("Claude request and response envelopes are explicit and correlated", () => {
  const request = buildClaudeRequestBody("inspect the report", GRANT_TOKEN);
  assert.deepEqual(parseClaudeRequestBody(request), {
    grantToken: GRANT_TOKEN,
    task: "inspect the report",
  });
  assert.equal(parseClaudeRequestBody("ordinary message"), null);
  const malformed = `<cxmsg-request grant = '${GRANT_TOKEN}'>\nreview\n</cxmsg-request>`;
  assert.match(redactClaudeRequestCapabilities(malformed), /grant="\[redacted\]"/);
  assert.doesNotMatch(redactClaudeRequestCapabilities(malformed), new RegExp(GRANT_TOKEN));
  assert.equal(
    redactClaudeRequestCapabilities(`ordinary grant="${GRANT_TOKEN}"`),
    `ordinary grant="${GRANT_TOKEN}"`,
  );
  assert.match(
    buildClaudeResponseBody({
      requestId: MESSAGE_ID,
      status: "completed",
      result: "looks good",
    }),
    /in-reply-to="12345678-1234-1234-1234-123456789abc"[\s\S]*looks good/,
  );
});

test("unauthorized Claude requests are sanitized and delivered as untrusted messages", async () => {
  const record = upsertClaudeRequestGrant(
    { name: "worker", threadId: "thread-worker", allowedClaudeRequesters: [] },
    {
      name: "reviewer",
      sessionId: SESSION_ID,
      address: "uds:/tmp/cc-socks/12345.sock",
    },
    ":read-only",
    "never",
    GRANT_TOKEN,
  );
  const deliveries = [];
  let created = 0;
  let scheduled = 0;
  const dependencies = {
    readRecord: () => record,
    createRequest: async (input) => {
      created += 1;
      return { jobId: input.parsed.messageId };
    },
    scheduleRequest: () => {
      scheduled += 1;
    },
    deliverMessage: async (_target, parsed) => {
      deliveries.push(parsed.body);
      return { delivery: "started" };
    },
  };
  const base = {
    fromName: "reviewer",
    fromSession: SESSION_ID,
    fromAddress: "uds:/tmp/cc-socks/12345.sock",
    messageId: MESSAGE_ID,
  };

  const unauthorized = await handleClaudeRequestOrMessage(
    "worker",
    { ...base, body: buildClaudeRequestBody("inspect", "22222222-2222-2222-2222-222222222222") },
    dependencies,
  );
  assert.equal(unauthorized.kind, "message");
  assert.match(deliveries[0], /grant="\[redacted\]"/);
  assert.doesNotMatch(deliveries[0], /22222222/);

  const wrongSession = await handleClaudeRequestOrMessage(
    "worker",
    {
      ...base,
      fromSession: "77654321-4321-4321-4321-cba987654321",
      body: buildClaudeRequestBody("inspect", GRANT_TOKEN),
    },
    dependencies,
  );
  assert.equal(wrongSession.kind, "message");
  assert.doesNotMatch(deliveries[1], new RegExp(GRANT_TOKEN));
  assert.equal(created, 0);

  const authorized = await handleClaudeRequestOrMessage(
    "worker",
    { ...base, body: buildClaudeRequestBody("inspect", GRANT_TOKEN) },
    dependencies,
  );
  assert.equal(authorized.kind, "request");
  assert.equal(created, 1);
  assert.equal(scheduled, 1);
  assert.equal(deliveries.length, 2);
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
  assert.equal(listClaudeRequestGrants(granted)[0].approval, "never");
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
      fromSession: null,
      fromAddress: "uds:/tmp/cc-socks/99999.sock",
      grantToken: GRANT_TOKEN,
    }),
    null,
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

test("regranting a Claude session rotates and redacts its capability token", () => {
  const peer = {
    name: "reviewer",
    sessionId: SESSION_ID,
    address: "uds:/tmp/cc-socks/12345.sock",
  };
  const firstToken = "11111111-1111-1111-1111-111111111111";
  const secondToken = "22222222-2222-2222-2222-222222222222";
  const first = upsertClaudeRequestGrant(
    { name: "worker", allowedClaudeRequesters: [] },
    peer,
    ":read-only",
    "never",
    firstToken,
  );
  const rotated = upsertClaudeRequestGrant(
    first,
    { ...peer, address: "uds:/tmp/cc-socks/54321.sock" },
    ":workspace",
    "relay",
    secondToken,
  );
  assert.equal(listClaudeRequestGrants(rotated).length, 1);
  assert.equal(
    findClaudeRequestGrant(rotated, {
      fromSession: SESSION_ID,
      grantToken: firstToken,
    }),
    null,
  );
  const active = findClaudeRequestGrant(rotated, {
    fromSession: SESSION_ID,
    grantToken: secondToken,
  });
  assert.equal(active.permissions, ":workspace");
  assert.equal(active.approval, "relay");
  const publicGrant = publicClaudeRequestGrant(active);
  assert.equal(publicGrant.token, undefined);
  assert.equal(publicGrant.tokenHint, "22222222…");
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
  const socketsDir = claudeSocketsDir();
  const canonicalSocket = path.join(socketsDir, `${process.pid}.sock`);
  const server = net.createServer((socket) => {
    socket.resume();
    socket.on("end", () => socket.end());
  });
  await fs.mkdir(socketsDir, { recursive: true, mode: 0o700 });
  await fs.unlink(canonicalSocket).catch(() => {});
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(canonicalSocket, resolve);
  });
  await fs.chmod(canonicalSocket, 0o600);
  try {
    await fs.writeFile(
      path.join(sessionsDir, `${process.pid}.json`),
      JSON.stringify({
        pid: process.pid,
        name: "claude-test",
        sessionId: SESSION_ID,
        cwd: process.cwd(),
        status: "idle",
        startedAt: 123456,
        kind: "interactive",
        peerProtocol: 1,
        messagingSocketPath: canonicalSocket,
      }),
      { mode: 0o600 },
    );
    const peers = await listClaudePeers({ sessionsDir });
    assert.equal(peers.length, 1);
    assert.equal(peers[0].name, "claude-test");
    assert.equal(peers[0].startedAt, 123456);
    assert.equal(peers[0].address, `uds:${canonicalSocket}`);

    const restrictedPeers = await listClaudePeers({
      sessionsDir,
      processStateFn: () => "unverified",
    });
    assert.equal(restrictedPeers.length, 1);
    await sendClaudePeerFrame(
      restrictedPeers[0].socketPath,
      buildClaudePeerFrame({
        fromSocket: canonicalSocket,
        fromName: "codex-test",
        fromSession: SESSION_ID,
        message: "EPERM does not block socket delivery",
        messageId: MESSAGE_ID,
      }),
    );

    await fs.chmod(canonicalSocket, 0o666);
    assert.equal(
      (
        await listClaudePeers({
          sessionsDir,
          processStateFn: () => "unverified",
        })
      ).length,
      0,
    );
    await fs.chmod(canonicalSocket, 0o600);

    const sandboxDenied = await listClaudePeers({
      sessionsDir,
      processStateFn: () => "unverified",
      probeSocket: async () =>
        failedProbe(Object.assign(new Error("connect EPERM"), { code: "EPERM" })),
    });
    assert.equal(sandboxDenied.length, 1);
    assert.equal(sandboxDenied[0].status, "unreachable");
    assert.equal(sandboxDenied[0].verification, "sandbox-denied");
    assert.equal(sandboxDenied[0].errorCode, "EPERM");
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.unlink(canonicalSocket).catch(() => {});
    await fs.rm(sessionsDir, { recursive: true, force: true });
  }
});
