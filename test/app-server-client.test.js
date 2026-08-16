import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  AppServerClient,
  AppServerError,
  appServerVersion,
  classifyAppServerNegativeAcceptance,
  validateAppServerSocket,
} from "../src/app-server-client.js";

class FakeTransport extends EventEmitter {
  constructor() {
    super();
    this.upgraded = false;
    this.sent = [];
  }

  async connect() {
    this.upgraded = true;
  }

  sendText(value) {
    const message = JSON.parse(value);
    this.sent.push(message);
    if (message.method === "initialize") {
      queueMicrotask(() =>
        this.emit(
          "message",
          JSON.stringify({
            id: message.id,
            result: { userAgent: "codex_cli_rs/0.147.0 (test)" },
          }),
        ),
      );
    } else if (message.method === "turn/steer") {
      queueMicrotask(() =>
        this.emit(
          "message",
          JSON.stringify({
            id: message.id,
            error: { code: -32600, message: "no active turn to steer" },
          }),
        ),
      );
    }
  }

  close() {
    this.upgraded = false;
    this.emit("close");
  }
}

test("App Server client answers server-initiated approval requests", async () => {
  const transport = new FakeTransport();
  const client = new AppServerClient({
    transportFactory: () => transport,
    onServerRequest: async (request) => {
      assert.equal(request.method, "item/commandExecution/requestApproval");
      return { decision: "accept" };
    },
  });
  await client.connect();
  transport.emit(
    "message",
    JSON.stringify({
      id: 99,
      method: "item/commandExecution/requestApproval",
      params: { itemId: "command-1" },
    }),
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(
    transport.sent.find((message) => message.id === 99),
    { id: 99, result: { decision: "accept" } },
  );
  await client.close();
});

test("App Server client exposes notifications without confusing them with responses", async () => {
  const transport = new FakeTransport();
  const notifications = [];
  const client = new AppServerClient({
    transportFactory: () => transport,
    onNotification: async (message) => notifications.push(message),
  });
  await client.connect();
  transport.emit(
    "message",
    JSON.stringify({
      method: "thread/status/changed",
      params: {
        threadId: "11345678-1234-4234-8234-123456789abc",
        status: { type: "idle" },
      },
    }),
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(notifications.length, 1);
  assert.equal(notifications[0].method, "thread/status/changed");
  await client.close();
});

test("negative acceptance is recognized only for pinned App Server contracts", () => {
  const mismatch = new AppServerError("turn/steer failed", {
    code: -32600,
    message:
      "expected active turn id `11345678-1234-4234-8234-123456789abc` but found `21345678-1234-4234-8234-123456789abc`",
  });
  mismatch.appServerUserAgent = "codex_cli_rs/0.147.0 (macOS; arm64)";
  assert.equal(appServerVersion(mismatch.appServerUserAgent), "0.147.0");
  assert.equal(appServerVersion("malformed user agent"), null);
  assert.deepEqual(classifyAppServerNegativeAcceptance(mismatch), {
    reason: "expected_turn_mismatch",
    errorCode: "EEXPECTEDTURNMISMATCH",
    contract: "codex-app-server/0.147.0",
  });

  const unsupported = new AppServerError("turn/steer failed", mismatch.details);
  unsupported.appServerUserAgent = "codex_cli_rs/0.148.0 (macOS; arm64)";
  assert.equal(classifyAppServerNegativeAcceptance(unsupported), null);
  assert.equal(
    classifyAppServerNegativeAcceptance(
      new AppServerError("app-server request timed out: turn/steer"),
    ),
    null,
  );
});

test("request errors retain the initialized App Server contract", async () => {
  const client = new AppServerClient({ transportFactory: () => new FakeTransport() });
  await client.connect();
  assert.equal(client.initializeResult.userAgent, "codex_cli_rs/0.147.0 (test)");
  await assert.rejects(
    client.request("turn/steer", {}),
    (error) => {
      assert.equal(error.appServerUserAgent, "codex_cli_rs/0.147.0 (test)");
      assert.equal(
        classifyAppServerNegativeAcceptance(error)?.reason,
        "no_active_turn",
      );
      return true;
    },
  );
  await client.close();
});

test("App Server socket validation requires a private owner-controlled path", async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-app-socket-"));
  const socketPath = path.join(directory, "app-server.sock");
  const server = net.createServer();
  await fs.chmod(directory, 0o700);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  try {
    await fs.chmod(socketPath, 0o600);
    assert.equal(validateAppServerSocket(socketPath).isSocket(), true);
    await fs.chmod(socketPath, 0o666);
    assert.throws(() => validateAppServerSocket(socketPath), /too broad/);
    await fs.chmod(socketPath, 0o600);
    await fs.chmod(directory, 0o755);
    assert.throws(() => validateAppServerSocket(socketPath), /parent permissions/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    await fs.rm(directory, { recursive: true, force: true });
  }
});
