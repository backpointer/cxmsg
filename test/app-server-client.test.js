import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  AppServerClient,
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
        this.emit("message", JSON.stringify({ id: message.id, result: {} })),
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
