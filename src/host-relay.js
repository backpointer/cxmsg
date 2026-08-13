import { randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import http from "node:http";
import path from "node:path";
import { claudeBridgeState } from "./claude-bridge.js";
import {
  createClaudeDeliveryJob,
  sendClaudeDeliveryJob,
} from "./claude-delivery.js";
import { listClaudePeers, resolveClaudePeer } from "./claude-messaging.js";
import { readJob } from "./jobs.js";
import { validateMessage, validateSessionName } from "./messaging.js";
import { readSessionRecord } from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const HOST_RELAY_RECORD_PATH = path.join(CXMSG_STATE_DIR, "host-relay.json");
export const HOST_RELAY_LOG_PATH = path.join(CXMSG_STATE_DIR, "host-relay.log");
const MAX_BODY_BYTES = 32 * 1024;

function atomicWrite(destination, value) {
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, destination);
}

export function readHostRelayRecord() {
  try {
    const record = JSON.parse(readFileSync(HOST_RELAY_RECORD_PATH, "utf8"));
    if (
      record?.version !== 1 ||
      !Number.isSafeInteger(record.pid) ||
      !Number.isSafeInteger(record.port) ||
      record.port < 1 ||
      record.port > 65_535 ||
      typeof record.token !== "string" ||
      typeof record.startedAt !== "number"
    ) {
      return null;
    }
    return record;
  } catch {
    return null;
  }
}

function json(response, status, value) {
  const body = `${JSON.stringify(value)}\n`;
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    "cache-control": "no-store",
  });
  response.end(body);
}

async function readJson(request) {
  const chunks = [];
  let bytes = 0;
  for await (const chunk of request) {
    bytes += chunk.length;
    if (bytes > MAX_BODY_BYTES) throw new Error("relay request is too large");
    chunks.push(chunk);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
}

export async function runHostRelay({
  port = 4174,
  bridgeState = claudeBridgeState,
  peers = listClaudePeers,
  session = readSessionRecord,
  createDelivery = createClaudeDeliveryJob,
  sendDelivery = sendClaudeDeliveryJob,
} = {}) {
  mkdirSync(CXMSG_STATE_DIR, { recursive: true, mode: 0o700 });
  chmodSync(CXMSG_STATE_DIR, 0o700);
  const record = {
    version: 1,
    pid: process.pid,
    port,
    token: randomUUID(),
    startedAt: Date.now(),
  };
  const server = http.createServer(async (request, response) => {
    if (request.headers.authorization !== `Bearer ${record.token}`) {
      json(response, 401, { error: "unauthorized" });
      return;
    }
    try {
      if (request.method === "GET" && request.url === "/health") {
        json(response, 200, {
          status: "running",
          pid: record.pid,
          port: record.port,
          startedAt: record.startedAt,
        });
        return;
      }
      if (request.method === "POST" && request.url === "/v1/claude/send") {
        const body = await readJson(request);
        const from = validateSessionName(body.from);
        const message = validateMessage(body.message);
        const sourceRecord = session(from);
        if (!sourceRecord) throw new Error(`unknown Codex session: ${from}`);
        const bridge = await bridgeState(from);
        if (!bridge.running) throw new Error(`Claude bridge for ${from} is ${bridge.status}`);
        const peer = resolveClaudePeer(await peers(), body.target);
        if (peer.status === "unreachable") {
          throw new Error(`Claude target is unreachable: ${peer.name}`);
        }
        let job = body.jobId ? readJob(body.jobId) : null;
        if (job) {
          if (job.kind !== "claude-delivery" || job.from !== from) {
            throw new Error(`invalid Claude delivery job: ${body.jobId}`);
          }
        } else {
          job = createDelivery({
            from,
            sourceRecord,
            peer,
            message,
          });
        }
        job = await sendDelivery(bridge.record, sourceRecord, job);
        json(response, 200, {
          jobId: job.jobId,
          status: job.status,
          attempt: job.delivery?.attempt || 0,
        });
        return;
      }
      json(response, 404, { error: "not found" });
    } catch (error) {
      json(response, 400, { error: error.message, errorCode: error.code || null });
    }
  });

  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    const saved = readHostRelayRecord();
    if (saved?.pid === process.pid && existsSync(HOST_RELAY_RECORD_PATH)) {
      unlinkSync(HOST_RELAY_RECORD_PATH);
    }
  };
  process.once("SIGINT", () => server.close(() => process.exit(0)));
  process.once("SIGTERM", () => server.close(() => process.exit(0)));
  process.once("exit", cleanup);

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      record.port = server.address().port;
      atomicWrite(HOST_RELAY_RECORD_PATH, record);
      resolve();
    });
  });
  return { server, record };
}

export async function hostRelayRequest(
  route,
  { method = "GET", body = null, record = readHostRelayRecord(), timeoutMs = 2_000 } = {},
) {
  if (!record) throw new Error("cxmsg host relay is not registered");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`http://127.0.0.1:${record.port}${route}`, {
      method,
      headers: {
        authorization: `Bearer ${record.token}`,
        ...(body ? { "content-type": "application/json" } : {}),
      },
      body: body ? JSON.stringify(body) : null,
      signal: controller.signal,
    });
    const value = await response.json();
    if (!response.ok) {
      throw Object.assign(
        new Error(value.error || `host relay HTTP ${response.status}`),
        { code: value.errorCode || null },
      );
    }
    return value;
  } catch (error) {
    if (error.name === "AbortError") {
      throw Object.assign(new Error("cxmsg host relay timed out"), { code: "ETIMEDOUT" });
    }
    const code = error.code || error.cause?.code || "ERELAYUNREACHABLE";
    const message = error.cause?.message || error.message;
    throw Object.assign(
      new Error(code ? `${message} (${code})` : message),
      { code },
    );
  } finally {
    clearTimeout(timer);
  }
}
