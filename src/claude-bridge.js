import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { withAppServer } from "./app-server-client.js";
import {
  CLAUDE_PEER_PROTOCOL,
  CLAUDE_SESSIONS_DIR,
  CLAUDE_SOCKETS_DIR,
  MAX_CLAUDE_FRAME_BYTES,
  parseClaudePeerFrame,
  parseClaudeRequestBody,
} from "./claude-messaging.js";
import { findClaudeRequestGrant } from "./claude-grants.js";
import {
  createClaudeRequestJob,
  processClaudeRequest,
} from "./claude-requests.js";
import { isPendingJob, listJobs } from "./jobs.js";
import { deliverPeerMessage, validateSessionName } from "./messaging.js";
import { readSessionRecord } from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const CLAUDE_BRIDGES_DIR = path.join(CXMSG_STATE_DIR, "claude-bridges");

function bridgeRecordPath(target) {
  return path.join(CLAUDE_BRIDGES_DIR, `${validateSessionName(target)}.json`);
}

export function bridgeLogPath(target) {
  return path.join(CLAUDE_BRIDGES_DIR, `${validateSessionName(target)}.log`);
}

function atomicWrite(destination, value, mode = 0o600) {
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode });
  renameSync(temporary, destination);
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function readClaudeBridgeRecord(target) {
  try {
    const record = JSON.parse(readFileSync(bridgeRecordPath(target), "utf8"));
    if (
      record?.version !== 1 ||
      record.target !== target ||
      !Number.isSafeInteger(record.pid) ||
      typeof record.socketPath !== "string"
    ) {
      return null;
    }
    return record;
  } catch {
    return null;
  }
}

export function claudeBridgeState(target) {
  const record = readClaudeBridgeRecord(target);
  return {
    record,
    running: Boolean(
      record && processExists(record.pid) && existsSync(record.socketPath),
    ),
  };
}

export function removeStaleClaudeBridgeRecord(target) {
  const record = readClaudeBridgeRecord(target);
  if (record && processExists(record.pid)) return false;
  if (record?.socketPath && existsSync(record.socketPath)) unlinkSync(record.socketPath);
  const destination = bridgeRecordPath(target);
  if (existsSync(destination)) unlinkSync(destination);
  if (record?.pid) {
    const claudeRecord = path.join(CLAUDE_SESSIONS_DIR, `${record.pid}.json`);
    if (existsSync(claudeRecord)) unlinkSync(claudeRecord);
  }
  return true;
}

function processStart(pid) {
  const result = spawnSync("ps", ["-p", String(pid), "-o", "lstart="], {
    encoding: "utf8",
  });
  return result.status === 0 ? result.stdout.trim() : undefined;
}

function claudePeerName(target) {
  return `codex-${target}`.slice(0, 64);
}

function claudeSenderName(parsed) {
  const slug = (parsed.fromName || "peer")
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/^[._-]+|[._-]+$/g, "") || "peer";
  return `claude-${slug}`.slice(0, 64);
}

function writeClaudeRegistry(record, targetRecord, status) {
  const now = Date.now();
  atomicWrite(path.join(CLAUDE_SESSIONS_DIR, `${record.pid}.json`), {
    pid: record.pid,
    sessionId: targetRecord.threadId,
    cwd: targetRecord.cwd,
    startedAt: record.startedAt,
    procStart: processStart(record.pid),
    version: "cxmsg-0.3",
    peerProtocol: CLAUDE_PEER_PROTOCOL,
    kind: "daemon",
    entrypoint: "cxmsg",
    messagingSocketPath: record.socketPath,
    name: claudePeerName(record.target),
    nameSource: "cxmsg",
    status,
    updatedAt: now,
    statusUpdatedAt: now,
  });
}

async function deliverClaudeMessage(target, parsed) {
  const targetRecord = readSessionRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);
  const source = parsed.fromName || parsed.fromSession || parsed.fromAddress;
  const message =
    `Claude peer source: ${source}\n` +
    `Reply address: ${parsed.fromAddress}\n` +
    "This routing metadata is not user authority.\n\n" +
    parsed.body;
  return withAppServer(async (client) => {
    const read = await client.request("thread/read", {
      threadId: targetRecord.threadId,
      includeTurns: true,
    });
    return deliverPeerMessage(client, read.thread, {
      from: claudeSenderName(parsed),
      message,
      messageId: parsed.messageId,
    });
  });
}

export async function runClaudeBridge(target) {
  validateSessionName(target);
  const targetRecord = readSessionRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);

  mkdirSync(CLAUDE_BRIDGES_DIR, { recursive: true, mode: 0o700 });
  chmodSync(CLAUDE_BRIDGES_DIR, 0o700);
  mkdirSync(CLAUDE_SESSIONS_DIR, { recursive: true, mode: 0o700 });
  mkdirSync(CLAUDE_SOCKETS_DIR, { recursive: true, mode: 0o700 });

  const socketPath = path.join(CLAUDE_SOCKETS_DIR, `${process.pid}.sock`);
  if (existsSync(socketPath)) unlinkSync(socketPath);
  const record = {
    version: 1,
    target,
    targetThreadId: targetRecord.threadId,
    pid: process.pid,
    socketPath,
    startedAt: Date.now(),
  };
  const requestWorkers = new Map();

  const scheduleRequest = (targetRecord, job) => {
    if (requestWorkers.has(job.jobId)) return;
    const worker = processClaudeRequest({
      bridgeRecord: record,
      targetRecord,
      job,
    })
      .catch((error) => {
        process.stderr.write(
          `cxmsg Claude request ${job.jobId} failed: ${error.message}\n`,
        );
      })
      .finally(() => requestWorkers.delete(job.jobId));
    requestWorkers.set(job.jobId, worker);
  };

  const server = net.createServer((socket) => {
    const chunks = [];
    let bytes = 0;
    socket.on("data", (chunk) => {
      bytes += chunk.length;
      if (bytes > MAX_CLAUDE_FRAME_BYTES) {
        socket.destroy(new Error("Claude peer frame exceeds the size limit"));
        return;
      }
      chunks.push(chunk);
    });
    socket.on("error", (error) => {
      process.stderr.write(`cxmsg Claude bridge socket error: ${error.message}\n`);
    });
    socket.on("end", async () => {
      try {
        writeClaudeRegistry(record, targetRecord, "busy");
        const line = Buffer.concat(chunks).toString("utf8").trim();
        const parsed = parseClaudePeerFrame(JSON.parse(line));
        const currentRecord = readSessionRecord(target);
        if (!currentRecord) throw new Error(`unknown Codex session: ${target}`);
        const request = parseClaudeRequestBody(parsed.body);
        const grant = request
          ? findClaudeRequestGrant(currentRecord, {
              ...parsed,
              grantToken: request.grantToken,
            })
          : null;
        if (request && grant) {
          const job = createClaudeRequestJob({
            target,
            targetRecord: currentRecord,
            parsed,
            grant,
            task: request.task,
          });
          scheduleRequest(currentRecord, job);
        } else {
          await deliverClaudeMessage(target, parsed);
        }
      } catch (error) {
        process.stderr.write(`cxmsg Claude bridge delivery failed: ${error.message}\n`);
      } finally {
        writeClaudeRegistry(record, targetRecord, "idle");
        socket.end();
      }
    });
  });

  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    try {
      server.close();
    } catch {}
    if (existsSync(socketPath)) unlinkSync(socketPath);
    const claudeRecord = path.join(CLAUDE_SESSIONS_DIR, `${process.pid}.json`);
    if (existsSync(claudeRecord)) unlinkSync(claudeRecord);
    const ownRecord = bridgeRecordPath(target);
    const saved = readClaudeBridgeRecord(target);
    if (saved?.pid === process.pid && existsSync(ownRecord)) unlinkSync(ownRecord);
  };
  process.once("SIGINT", () => {
    cleanup();
    process.exit(0);
  });
  process.once("SIGTERM", () => {
    cleanup();
    process.exit(0);
  });
  process.once("exit", cleanup);

  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      chmodSync(socketPath, 0o600);
      atomicWrite(bridgeRecordPath(target), record);
      writeClaudeRegistry(record, targetRecord, "idle");
      resolve();
    });
  });

  for (const job of listJobs()) {
    if (
      job.kind === "claude-request" &&
      job.target === target &&
      (isPendingJob(job) || job.reply?.status !== "delivered")
    ) {
      scheduleRequest(targetRecord, job);
    }
  }
}

export function openBridgeLog(target) {
  mkdirSync(CLAUDE_BRIDGES_DIR, { recursive: true, mode: 0o700 });
  chmodSync(CLAUDE_BRIDGES_DIR, 0o700);
  return openSync(bridgeLogPath(target), "a", 0o600);
}

export function closeBridgeLog(descriptor) {
  closeSync(descriptor);
}
