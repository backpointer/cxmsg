import { lstatSync } from "node:fs";
import { socketPath as defaultSocketPath } from "./runtime.js";
import { UnixWebSocket } from "./unix-websocket.js";

const DEFAULT_TIMEOUT_MS = 10_000;

export class AppServerError extends Error {
  constructor(message, details = undefined) {
    super(message);
    this.name = "AppServerError";
    this.details = details;
  }
}

export class AppServerClient {
  constructor({
    socketPath = defaultSocketPath(),
    timeoutMs = DEFAULT_TIMEOUT_MS,
    onServerRequest = null,
    transportFactory = null,
  } = {}) {
    this.socketPath = socketPath;
    this.timeoutMs = timeoutMs;
    this.transport = null;
    this.nextId = 1;
    this.pending = new Map();
    this.closed = false;
    this.onServerRequest = onServerRequest;
    this.transportFactory = transportFactory;
  }

  async connect() {
    if (this.transport) return;

    this.transport = this.transportFactory
      ? this.transportFactory(this.socketPath)
      : new UnixWebSocket(this.socketPath);
    this.transport.on("message", (message) => this.#handleMessage(message));
    this.transport.on("error", (error) => this.#failAll(error));
    this.transport.once("close", () => {
      if (this.closed) return;
      this.#failAll(
        new AppServerError("Codex app-server Unix socket closed unexpectedly"),
      );
    });
    await this.transport.connect();

    await this.request("initialize", {
      clientInfo: {
        name: "cxmsg",
        title: "Codex Session Messaging",
        version: "0.6.0",
      },
      capabilities: {
        experimentalApi: true,
      },
    });
    this.notify("initialized");
  }

  request(method, params = {}) {
    if (!this.transport?.upgraded) {
      return Promise.reject(
        new AppServerError("app-server client is not connected"),
      );
    }

    const id = this.nextId++;
    const payload = { method, id, params };

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new AppServerError(`app-server request timed out: ${method}`));
      }, this.timeoutMs);

      this.pending.set(id, { method, resolve, reject, timer });
      this.transport.sendText(JSON.stringify(payload));
    });
  }

  notify(method, params = undefined) {
    if (!this.transport?.upgraded) {
      throw new AppServerError("app-server client is not connected");
    }
    const payload = params === undefined ? { method } : { method, params };
    this.transport.sendText(JSON.stringify(payload));
  }

  respond(id, result) {
    if (!this.transport?.upgraded) {
      throw new AppServerError("app-server client is not connected");
    }
    this.transport.sendText(JSON.stringify({ id, result }));
  }

  respondError(id, error) {
    if (!this.transport?.upgraded) return;
    this.transport.sendText(
      JSON.stringify({
        id,
        error: {
          code: -32000,
          message: error?.message || String(error || "server request failed"),
        },
      }),
    );
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    this.transport?.close();
    this.#failAll(new AppServerError("app-server client closed"));
  }

  #handleMessage(rawMessage) {
    let message;
    try {
      message = JSON.parse(rawMessage);
    } catch {
      return;
    }

    if (message.id !== undefined && message.method) {
      if (!this.onServerRequest) {
        this.respondError(
          message.id,
          new AppServerError(`unsupported server request: ${message.method}`),
        );
        return;
      }
      Promise.resolve(this.onServerRequest(message))
        .then((result) => this.respond(message.id, result))
        .catch((error) => this.respondError(message.id, error));
      return;
    }

    if (message.id === undefined) return;

    const pending = this.pending.get(message.id);
    if (!pending) return;

    clearTimeout(pending.timer);
    this.pending.delete(message.id);

    if (message.error) {
      pending.reject(
        new AppServerError(
          `${pending.method} failed: ${message.error.message || "unknown error"}`,
          message.error,
        ),
      );
      return;
    }
    pending.resolve(message.result);
  }

  #failAll(error) {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }
}

export async function withAppServer(callback, options = {}) {
  const client = new AppServerClient(options);
  try {
    await client.connect();
    return await callback(client);
  } finally {
    await client.close();
  }
}

export async function probeAppServerSocket(
  socketPath = defaultSocketPath(),
  { timeoutMs = 1_000 } = {},
) {
  let metadata;
  try {
    metadata = lstatSync(socketPath);
  } catch {
    return false;
  }
  if (!metadata.isSocket()) return false;
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    return false;
  }
  if ((metadata.mode & 0o077) !== 0) return false;

  const client = new AppServerClient({ socketPath, timeoutMs });
  try {
    await client.connect();
    return true;
  } catch {
    return false;
  } finally {
    await client.close();
  }
}
