import { lstatSync } from "node:fs";
import path from "node:path";
import { CXMSG_VERSION } from "./version.js";
import { socketPath as defaultSocketPath } from "./runtime.js";
import { failedProbe, healthyProbe } from "./socket-probe.js";
import { UnixWebSocket } from "./unix-websocket.js";

const DEFAULT_TIMEOUT_MS = 10_000;
const NEGATIVE_ACCEPTANCE_CONTRACTS = new Set(["0.147.0"]);
const APP_SERVER_VERSION_PATTERN = /^[^/\s]+\/(\d+\.\d+\.\d+)(?:[-+\s]|$)/;

export function validateAppServerSocket(socketPath) {
  const metadata = lstatSync(socketPath);
  if (!metadata.isSocket()) throw new Error("not a Unix socket");
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error("Unix socket is owned by another user");
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error("Unix socket permissions are too broad");
  }
  const parent = lstatSync(path.dirname(socketPath));
  if (!parent.isDirectory()) throw new Error("Unix socket parent is not a directory");
  if (typeof process.getuid === "function" && parent.uid !== process.getuid()) {
    throw new Error("Unix socket parent is owned by another user");
  }
  if ((parent.mode & 0o077) !== 0) {
    throw new Error("Unix socket parent permissions are too broad");
  }
  return metadata;
}

export class AppServerError extends Error {
  constructor(message, details = undefined) {
    super(message);
    this.name = "AppServerError";
    this.details = details;
  }
}

export function appServerVersion(userAgent) {
  return APP_SERVER_VERSION_PATTERN.exec(userAgent || "")?.[1] || null;
}

export function classifyAppServerNegativeAcceptance(error) {
  if (!(error instanceof AppServerError) || error.details?.code !== -32600) {
    return null;
  }
  const version = appServerVersion(error.appServerUserAgent);
  if (!NEGATIVE_ACCEPTANCE_CONTRACTS.has(version)) return null;
  const message = error.details?.message || "";
  let reason = null;
  let errorCode = null;
  if (message === "no active turn to steer") {
    reason = "no_active_turn";
    errorCode = "ENOACTIVETURN";
  } else if (
    /^expected active turn id `[0-9a-f-]{36}` but found `[0-9a-f-]{36}`$/i.test(message)
  ) {
    reason = "expected_turn_mismatch";
    errorCode = "EEXPECTEDTURNMISMATCH";
  } else if (message === "cannot steer a review turn") {
    reason = "non_steerable_review";
    errorCode = "ENONSTEERABLEREVIEW";
  } else if (message === "cannot steer a compact turn") {
    reason = "non_steerable_compact";
    errorCode = "ENONSTEERABLECOMPACT";
  }
  if (!reason) return null;
  return {
    reason,
    errorCode,
    contract: `codex-app-server/${version}`,
  };
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
    this.initializeResult = null;
  }

  async connect() {
    if (this.transport) return;

    if (!this.transportFactory) validateAppServerSocket(this.socketPath);

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

    this.initializeResult = await this.request("initialize", {
      clientInfo: {
        name: "cxmsg",
        title: "Codex Session Messaging",
        version: CXMSG_VERSION,
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
    await this.transport?.close();
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
      const error = new AppServerError(
        `${pending.method} failed: ${message.error.message || "unknown error"}`,
        message.error,
      );
      error.appServerUserAgent = this.initializeResult?.userAgent || null;
      pending.reject(error);
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
  try {
    validateAppServerSocket(socketPath);
  } catch (error) {
    return failedProbe(error);
  }

  const client = new AppServerClient({ socketPath, timeoutMs });
  let probeTimer;
  try {
    await Promise.race([
      client.connect(),
      new Promise((_, reject) =>
        (probeTimer = setTimeout(
          () =>
            reject(
              Object.assign(new Error("app-server Unix socket probe timed out"), {
                code: "ETIMEDOUT",
              }),
            ),
          timeoutMs,
        )),
      ),
    ]);
    return healthyProbe();
  } catch (error) {
    return failedProbe(error);
  } finally {
    clearTimeout(probeTimer);
    await client.close();
  }
}
