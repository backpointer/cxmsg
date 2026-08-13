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
  validateClaudeSocketPath,
} from "./claude-messaging.js";
import { findClaudeRequestGrant } from "./claude-grants.js";
import {
  parseClaudeDeliveryAck,
  recordClaudeDeliveryAck,
  refreshClaudeDelivery,
  sendClaudeDeliveryJob,
} from "./claude-delivery.js";
import {
  createClaudeRequestJob,
  processClaudeRequest,
} from "./claude-requests.js";
import { isPendingJob, listJobs, mutateJob, readJob } from "./jobs.js";
import { deliverPeerMessage, validateSessionName } from "./messaging.js";
import { readSessionRecord } from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";
import { processState, serviceEvidence } from "./process-state.js";
import {
  failedProbe,
  healthyProbe,
  timeoutProbe,
} from "./socket-probe.js";

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

export function readClaudeBridgeRecord(target) {
  try {
    const record = JSON.parse(readFileSync(bridgeRecordPath(target), "utf8"));
    if (
      record?.version !== 1 ||
      record.target !== target ||
      typeof record.targetThreadId !== "string" ||
      !Number.isSafeInteger(record.pid) ||
      typeof record.socketPath !== "string" ||
      !Number.isSafeInteger(record.startedAt)
    ) {
      return null;
    }
    return record;
  } catch {
    return null;
  }
}

export async function probeClaudeBridge(record, timeoutMs = 500) {
  if (!record?.socketPath) return failedProbe(new Error("missing bridge socket path"));
  if (path.basename(record.socketPath) !== `${record.pid}.sock`) {
    return failedProbe(
      Object.assign(new Error("bridge socket path does not match its PID"), {
        code: "EIDENTITY",
      }),
    );
  }
  let resolved;
  try {
    resolved = await validateClaudeSocketPath(record.socketPath);
  } catch (error) {
    return failedProbe(error);
  }
  const nonce = randomUUID();
  return new Promise((resolve) => {
    const socket = net.createConnection({ path: resolved });
    const chunks = [];
    let settled = false;
    const finish = (probe) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(probe);
    };
    socket.on("data", (chunk) => chunks.push(chunk));
    socket.once("error", (error) => finish(failedProbe(error)));
    socket.once("end", () => {
      try {
        const response = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        const matched =
          response?.cxmsgHealth === 1 &&
            response.nonce === nonce &&
            response.target === record.target &&
            response.targetThreadId === record.targetThreadId &&
            response.pid === record.pid &&
            response.startedAt === record.startedAt;
        finish(
          matched
            ? healthyProbe()
            : failedProbe(
                Object.assign(new Error("bridge identity handshake mismatch"), {
                  code: "EIDENTITY",
                }),
              ),
        );
      } catch (error) {
        finish(failedProbe(error));
      }
    });
    socket.setTimeout(timeoutMs, () => finish(timeoutProbe()));
    socket.once("connect", () => {
      socket.end(`${JSON.stringify({ cxmsgHealth: 1, nonce, target: record.target })}\n`);
    });
  });
}

export async function evaluateClaudeBridgeRecord(
  record,
  { processStateFn = processState, probeBridge = probeClaudeBridge } = {},
) {
  const process = record ? processStateFn(record.pid) : "missing";
  const socketProbe = record
    ? await probeBridge(record)
    : failedProbe(Object.assign(new Error("bridge record is missing"), { code: "ENOENT" }));
  const normalizedProbe =
    typeof socketProbe === "boolean"
      ? socketProbe
        ? healthyProbe()
        : failedProbe(new Error("bridge probe failed"))
      : socketProbe;
  const socketPresent = Boolean(record?.socketPath && existsSync(record.socketPath));
  return {
    record,
    process,
    socketProbe: normalizedProbe,
    socketPresent,
    socketHealthy: normalizedProbe.state === "healthy",
    ...serviceEvidence({
      process,
      identity: "unavailable",
      socketProbe: normalizedProbe,
      socketPresent,
    }),
  };
}

export async function claudeBridgeState(target, options = {}) {
  return evaluateClaudeBridgeRecord(readClaudeBridgeRecord(target), options);
}

export async function removeStaleClaudeBridgeRecord(target, options = {}) {
  const record = readClaudeBridgeRecord(target);
  if (record) {
    const state = await claudeBridgeState(target, options);
    if (!state.safeToRemove) return false;
  }
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

function claudeDeliveryWakeBody(delivery) {
  const detail = delivery.result || delivery.error || "No result detail was provided.";
  const bounded = Buffer.from(detail, "utf8")
    .subarray(0, 12 * 1024)
    .toString("utf8");
  return (
    `Claude delivery ${delivery.jobId} reached terminal status ${delivery.status}.\n\n` +
    bounded
  );
}

export async function handleClaudeDeliveryAck(
  target,
  parsed,
  ack,
  {
    recordAck = recordClaudeDeliveryAck,
    deliverMessage = deliverClaudeMessage,
    mutate = mutateJob,
  } = {},
) {
  const delivery = await recordAck(parsed, ack);
  if (!delivery) return null;
  if (
    (!delivery.ackTransitioned && delivery.wake?.status !== "failed") ||
    !["completed", "failed"].includes(delivery.status)
  ) {
    return delivery;
  }

  try {
    const wake = await deliverMessage(target, {
      ...parsed,
      messageId: delivery.jobId,
      body: claudeDeliveryWakeBody(delivery),
    });
    return mutate(delivery.jobId, (current) => ({
      ...current,
      wake: {
        ...current.wake,
        status: "delivered",
        attemptedAt: new Date().toISOString(),
        deliveredAt: new Date().toISOString(),
        delivery: wake.delivery,
        turnId: wake.turnId,
        error: null,
      },
    }));
  } catch (error) {
    await mutate(delivery.jobId, (current) => ({
      ...current,
      wake: {
        ...current.wake,
        status: "failed",
        attemptedAt: new Date().toISOString(),
        error: error.message,
      },
    }));
    throw error;
  }
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
  const deliveryTimers = new Map();

  const scheduleDelivery = (job) => {
    if (deliveryTimers.has(job.jobId) || job.status !== "retry_scheduled") return;
    const delayMs = Math.max(0, Date.parse(job.delivery?.nextAttemptAt) - Date.now());
    const timer = setTimeout(async () => {
      deliveryTimers.delete(job.jobId);
      try {
        const current = readJob(job.jobId);
        if (!current || current.status !== "retry_scheduled") return;
        const sourceRecord = readSessionRecord(target);
        if (!sourceRecord) throw new Error(`unknown Codex session: ${target}`);
        await sendClaudeDeliveryJob(record, sourceRecord, current);
      } catch (error) {
        process.stderr.write(
          `cxmsg Claude delivery ${job.jobId} retry failed: ${error.message}\n`,
        );
      }
    }, delayMs);
    deliveryTimers.set(job.jobId, timer);
  };

  const scanDeliveries = () => {
    for (const job of listJobs()) {
      if (job.kind !== "claude-delivery" || job.from !== target) continue;
      const current = refreshClaudeDelivery(job);
      if (current.status === "retry_scheduled") scheduleDelivery(current);
    }
  };
  const deliveryMonitor = setInterval(scanDeliveries, 5_000);
  deliveryMonitor.unref();

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
        const line = Buffer.concat(chunks).toString("utf8").trim();
        const frame = JSON.parse(line);
        if (
          frame?.cxmsgHealth === 1 &&
          typeof frame.nonce === "string" &&
          frame.target === target
        ) {
          socket.end(
            `${JSON.stringify({
              cxmsgHealth: 1,
              nonce: frame.nonce,
              target,
              targetThreadId: record.targetThreadId,
              pid: record.pid,
              startedAt: record.startedAt,
            })}\n`,
          );
          return;
        }
        writeClaudeRegistry(record, targetRecord, "busy");
        const parsed = parseClaudePeerFrame(frame);
        const deliveryAck = parseClaudeDeliveryAck(parsed.body);
        if (deliveryAck) {
          const delivery = await handleClaudeDeliveryAck(
            target,
            parsed,
            deliveryAck,
          );
          if (!delivery) throw new Error(`unknown Claude delivery: ${deliveryAck.jobId}`);
          scheduleDelivery(delivery);
          return;
        }
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
        if (!socket.writableEnded) {
          writeClaudeRegistry(record, targetRecord, "idle");
          socket.end();
        }
      }
    });
  });

  let cleaned = false;
  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    for (const timer of deliveryTimers.values()) clearTimeout(timer);
    deliveryTimers.clear();
    clearInterval(deliveryMonitor);
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

  scanDeliveries();
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
