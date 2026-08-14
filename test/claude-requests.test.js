import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const REQUEST_ID = "22345678-1234-1234-1234-123456789abc";
const SESSION_ID = "97654321-4321-4321-4321-cba987654321";

test("authorized Claude requests run in a fork and return one correlated reply", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-request-state-"));
  process.env.CXMSG_STATE_DIR = stateDir;
  const socketDir = await fs.mkdtemp(path.join("/tmp", "cxmsg-request-sockets-"));
  process.env.CXMSG_CLAUDE_SOCKETS_DIR = socketDir;
  const socketPath = path.join(
    socketDir,
    `${process.pid}${String(Date.now()).slice(-6)}.sock`,
  );
  await fs.mkdir(socketDir, { recursive: true, mode: 0o700 });
  await fs.unlink(socketPath).catch(() => {});
  let received = "";
  const server = net.createServer((socket) => {
    socket.on("data", (chunk) => {
      received += chunk.toString("utf8");
    });
    socket.on("end", () => socket.end());
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  await fs.chmod(socketPath, 0o600);

  try {
    const { createClaudeRequestJob, processClaudeRequest } = await import(
      "../src/claude-requests.js"
    );
    const targetRecord = {
      name: "worker",
      threadId: "source-thread",
      cwd: process.cwd(),
    };
    const job = await createClaudeRequestJob({
      target: "worker",
      targetRecord,
      parsed: {
        messageId: REQUEST_ID,
        fromName: "reviewer",
        fromSession: SESSION_ID,
        fromAddress: `uds:${socketPath}`,
      },
      grant: { permissions: ":read-only" },
      task: "inspect the report",
    });
    const calls = [];
    const client = {
      async request(method, params) {
        calls.push({ method, params });
        if (method === "permissionProfile/list") {
          return { data: [{ id: ":read-only", allowed: true }] };
        }
        if (method === "thread/read" && params.threadId === "source-thread") {
          return {
            thread: {
              id: "source-thread",
              status: { type: "idle" },
              turns: [],
            },
          };
        }
        if (method === "thread/fork") {
          return {
            thread: {
              id: "execution-thread",
              status: { type: "idle" },
              turns: [],
            },
          };
        }
        if (method === "turn/start") return { turn: { id: "request-turn" } };
        if (
          method === "thread/turns/list" &&
          params.threadId === "execution-thread"
        ) {
          return {
            data: [
              {
                id: "request-turn",
                status: "completed",
                items: [
                  {
                    type: "agentMessage",
                    phase: "final_answer",
                    text: "report accepted",
                  },
                ],
              },
            ],
            nextCursor: null,
          };
        }
        assert.fail(`unexpected App Server request: ${method}`);
      },
    };

    const result = await processClaudeRequest({
      bridgeRecord: {
        socketPath: path.join(socketDir, "55555.sock"),
      },
      targetRecord,
      job,
      timeoutMs: 2_000,
      connect: (callback) => callback(client),
    });

    assert.equal(result.status, "completed");
    assert.equal(result.result, "report accepted");
    assert.equal(result.reply.status, "delivered");
    assert.equal(
      calls.find((call) => call.method === "thread/fork").params.permissions,
      ":read-only",
    );
    assert.equal(
      calls.find((call) => call.method === "thread/fork").params.includeTurns,
      false,
    );
    assert.equal(
      calls.find((call) => call.method === "thread/read").params.includeTurns,
      false,
    );
    assert.equal(
      calls.find((call) => call.method === "thread/turns/list").params.itemsView,
      "summary",
    );
    const frame = JSON.parse(received.trim());
    assert.match(frame.message.content, new RegExp(`in-reply-to="${REQUEST_ID}"`));
    assert.match(frame.message.content, /report accepted/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.unlink(socketPath).catch(() => {});
    await fs.rm(stateDir, { recursive: true, force: true });
    await fs.rm(socketDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
    delete process.env.CXMSG_CLAUDE_SOCKETS_DIR;
  }
});
