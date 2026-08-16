import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { UnixWebSocket } from "../src/unix-websocket.js";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

async function listen(socketPath, onSocket) {
  const server = net.createServer(onSocket);
  const sockets = new Set();
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  server.destroyConnections = () => {
    for (const socket of sockets) socket.destroy();
  };
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  return server;
}

function upgrade(socket, onData = () => {}, onUpgrade = () => {}) {
  let buffer = Buffer.alloc(0);
  let upgraded = false;
  socket.on("data", (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    if (!upgraded) {
      const boundary = buffer.indexOf("\r\n\r\n");
      if (boundary === -1) return;
      const request = buffer.subarray(0, boundary).toString("utf8");
      const key = request.match(/^Sec-WebSocket-Key:\s*(.+)$/im)?.[1]?.trim();
      const accept = createHash("sha1").update(key + GUID).digest("base64");
      buffer = buffer.subarray(boundary + 4);
      upgraded = true;
      socket.write(
        "HTTP/1.1 101 Switching Protocols\r\n" +
          "Upgrade: websocket\r\n" +
          "Connection: Upgrade\r\n" +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
        onUpgrade,
      );
    }
    if (buffer.length) {
      const current = buffer;
      buffer = Buffer.alloc(0);
      onData(current);
    }
  });
}

function serverFrame(payload, { opcode, final }) {
  const body = Buffer.from(payload, "utf8");
  return Buffer.concat([
    Buffer.from([(final ? 0x80 : 0) | opcode, body.length]),
    body,
  ]);
}

test("UnixWebSocket times out upgrades, flushes close, and bounds frames", { timeout: 2_000 }, async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-ws-"));
  const timeoutPath = path.join(directory, "timeout.sock");
  const closePath = path.join(directory, "close.sock");
  const boundedPath = path.join(directory, "bounded.sock");
  const servers = [];
  try {
    servers.push(await listen(timeoutPath, () => {}));
    const timed = new UnixWebSocket(timeoutPath, { connectTimeoutMs: 50 });
    await assert.rejects(timed.connect(), (error) => error.code === "ETIMEDOUT");

    let observeClose;
    const closeObserved = new Promise((resolve) => {
      observeClose = resolve;
    });
    servers.push(
      await listen(closePath, (socket) => {
        upgrade(socket, (frame) => {
          observeClose(frame[0] & 0x0f);
          socket.end();
        });
      }),
    );
    const closing = new UnixWebSocket(closePath, { closeTimeoutMs: 500 });
    await closing.connect();
    await closing.close();
    assert.equal(await closeObserved, 0x8);

    let boundedSocket;
    let markBoundedReady;
    const boundedReady = new Promise((resolve) => {
      markBoundedReady = resolve;
    });
    servers.push(
      await listen(boundedPath, (socket) => {
        boundedSocket = socket;
        upgrade(socket);
        socket.once("data", () => markBoundedReady());
      }),
    );
    const bounded = new UnixWebSocket(boundedPath, { maxBufferBytes: 256 });
    const boundedError = new Promise((resolve) => bounded.once("error", resolve));
    await bounded.connect();
    await boundedReady;
    const header = Buffer.alloc(4);
    header[0] = 0x81;
    header[1] = 126;
    header.writeUInt16BE(257, 2);
    boundedSocket.write(header);
    const frameError = await boundedError;
    assert.equal(frameError.code, "EAPPWSFRAME");
    assert.equal(frameError.observedBytes, 257);
    assert.equal(frameError.limitBytes, 256);
  } finally {
    for (const server of servers) {
      server.destroyConnections();
      await new Promise((resolve) => server.close(resolve));
    }
    await fs.rm(directory, { recursive: true, force: true });
  }
});

test("UnixWebSocket masks client frames and assembles split server fragments", { timeout: 2_000 }, async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-ws-frames-"));
  const socketPath = path.join(directory, "frames.sock");
  let observedClientFrame;
  const clientFrame = new Promise((resolve) => {
    observedClientFrame = resolve;
  });
  const server = await listen(socketPath, (socket) => {
    upgrade(
      socket,
      (frame) => observedClientFrame(frame),
      () => {
        const first = serverFrame("hel", { opcode: 0x1, final: false });
        const second = serverFrame("lo", { opcode: 0x0, final: true });
        socket.write(first.subarray(0, 1));
        socket.write(first.subarray(1));
        socket.write(second);
      },
    );
  });
  const client = new UnixWebSocket(socketPath, { maxBufferBytes: 256 });
  try {
    assert.throws(
      () => client.sendText("before-connect"),
      (error) => error?.code === "EAPPWSNOTCONNECTED",
    );
    const message = new Promise((resolve) => client.once("message", resolve));
    await client.connect();
    assert.throws(
      () => client.sendText("x".repeat(257)),
      (error) =>
        error?.code === "EAPPWSOUTBOUND" &&
        error.observedBytes === 257 &&
        error.limitBytes === 256,
    );
    client.sendText("masked");
    assert.equal(await message, "hello");
    const frame = await clientFrame;
    assert.equal(frame[0] & 0x0f, 0x1);
    assert.equal(Boolean(frame[1] & 0x80), true);
    const length = frame[1] & 0x7f;
    const mask = frame.subarray(2, 6);
    const decoded = Buffer.from(frame.subarray(6, 6 + length));
    for (let index = 0; index < decoded.length; index += 1) {
      decoded[index] ^= mask[index % 4];
    }
    assert.equal(decoded.toString("utf8"), "masked");
  } finally {
    await client.close();
    server.destroyConnections();
    await new Promise((resolve) => server.close(resolve));
    await fs.rm(directory, { recursive: true, force: true });
  }
});
