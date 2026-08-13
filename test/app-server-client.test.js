import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import { AppServerClient } from "../src/app-server-client.js";

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
