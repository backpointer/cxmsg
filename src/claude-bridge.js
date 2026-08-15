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
  buildClaudePeerStatusFrame,
  claudeSocketsDir,
  MAX_CLAUDE_FRAME_BYTES,
  parseClaudePeerFrame,
  parseClaudePeerStatusFrame,
  parseClaudeRequestBody,
  redactClaudeRequestCapabilities,
  sendClaudePeerFrame,
  validateClaudeSocketPath,
} from "./claude-messaging.js";
import { findClaudeRequestGrant } from "./claude-grants.js";
import {
  parseClaudeDeliveryAck,
  recordClaudeDeliveryAck,
  recordClaudeDeliveryReply,
  recordClaudeNativeDeliveryReceipt,
  refreshClaudeDelivery,
  sendClaudeDeliveryJob,
} from "./claude-delivery.js";
import {
  createClaudeRequestJob,
  processClaudeRequest,
} from "./claude-requests.js";
import { isPendingJob, listJobs, mutateJob, readJob } from "./jobs.js";
import { writeCoordinationEvent } from "./observability.js";
import {
  deliverPeerMessage,
  truncateUtf8,
  validateSessionName,
} from "./messaging.js";
import {
  readSessionRecord,
  sessionAllowsAppServerResume,
} from "./registry.js";
import {
  parseTypedPeerEnvelope,
  routePeerMessage,
} from "./route-admission.js";
import { CXMSG_STATE_DIR } from "./runtime.js";
import { processState, serviceEvidence } from "./process-state.js";
import { readThreadMetadata } from "./thread-activity.js";
import { CXMSG_VERSION } from "./version.js";
import {
  failedProbe,
  healthyProbe,
  timeoutProbe,
} from "./socket-probe.js";

export const CLAUDE_BRIDGES_DIR = path.join(CXMSG_STATE_DIR, "claude-bridges");
export const CLAUDE_BRIDGE_IMPLEMENTATION_REVISION = 23;

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
      !Number.isSafeInteger(record.startedAt) ||
      (record.cxmsgVersion !== undefined &&
        typeof record.cxmsgVersion !== "string") ||
      (record.implementationRevision !== undefined &&
        (!Number.isSafeInteger(record.implementationRevision) ||
          record.implementationRevision < 1))
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
            response.startedAt === record.startedAt &&
            response.cxmsgVersion === record.cxmsgVersion &&
            response.implementationRevision === record.implementationRevision;
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

function writeClaudeRegistry(record, targetRecord, handlerStatus) {
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
    status: "idle",
    statusSource: "bridge-availability",
    handlerStatus,
    updatedAt: now,
    statusUpdatedAt: now,
  });
}

export async function deliverClaudeMessage(
  target,
  parsed,
  {
    bypassAdmission = false,
    readRecord = readSessionRecord,
    withServer = withAppServer,
    readThread = readThreadMetadata,
    deliver = deliverPeerMessage,
  } = {},
) {
  const targetRecord = readRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);
  const typed = bypassAdmission ? null : parseTypedPeerEnvelope(parsed.body);
  const stableClaudeNode = parsed.fromSession
    ? { runtimeKind: "claude", nativeId: parsed.fromSession }
    : null;
  const message =
    (!bypassAdmission && !stableClaudeNode
      ? `Claude reply address: ${parsed.fromAddress}\n`
      : "") + (typed?.message || parsed.body);
  const dispatch = async ({ logicalMessageId, route, replyHandle }) =>
    withServer(async (client) => {
      const thread = await readThread(client, targetRecord.threadId);
      return deliver(client, thread, {
        from: claudeSenderName(parsed),
        message,
        messageId: logicalMessageId,
        replyHandle,
        route,
      }, {
        allowResume: sessionAllowsAppServerResume(targetRecord),
      });
    });
  if (bypassAdmission) {
    return dispatch({ logicalMessageId: parsed.messageId, route: null });
  }
  const outcome = await routePeerMessage(
    {
      from: claudeSenderName(parsed),
      target,
      message,
      route: typed?.route || null,
      logicalMessageId: typed?.logicalMessageId || parsed.messageId,
      ...(stableClaudeNode ? { senderNode: stableClaudeNode } : {}),
    },
    dispatch,
  );
  if (outcome.admissionState === "quarantined") {
    return {
      delivery: "quarantined",
      messageId: outcome.logicalMessageId,
      turnId: null,
      admission: outcome,
    };
  }
  if (outcome.deduplicated) {
    return {
      delivery: "deduplicated",
      messageId: outcome.logicalMessageId,
      turnId: outcome.turnId || null,
      admission: outcome,
    };
  }
  return outcome.result;
}

function claudeDeliveryWakeBody(delivery) {
  const detail = delivery.result || delivery.error || "No result detail was provided.";
  const bounded = truncateUtf8(detail, 12 * 1024);
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
    log = writeCoordinationEvent,
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

  await log({
    kind: "claude-delivery",
    phase: "wake-attempt",
    correlationId: delivery.jobId,
    target,
    attempt: delivery.delivery?.attempt,
    outcome: "attempted",
    late: delivery.ack?.late,
  });
  try {
    const wake = await deliverMessage(
      target,
      {
        ...parsed,
        messageId: delivery.jobId,
        body: claudeDeliveryWakeBody(delivery),
      },
      { bypassAdmission: true },
    );
    const reconciled = await mutate(delivery.jobId, (current) => ({
      ...current,
      wake: {
        ...current.wake,
        status: "delivered",
        attemptedAt: new Date().toISOString(),
        deliveredAt: new Date().toISOString(),
        delivery: wake.delivery,
        turnId: wake.turnId,
        errorCode: null,
        error: null,
      },
    }));
    await log({
      kind: "claude-delivery",
      phase: "wake",
      correlationId: reconciled.jobId,
      target,
      attempt: reconciled.delivery?.attempt,
      outcome: "delivered",
      late: reconciled.ack?.late,
    });
    return reconciled;
  } catch (error) {
    await mutate(delivery.jobId, (current) => ({
      ...current,
      wake: {
        ...current.wake,
        status: "failed",
        attemptedAt: new Date().toISOString(),
        errorCode: error?.code || "wake_error",
        error: error.message,
      },
    }));
    await log({
      kind: "claude-delivery",
      phase: "wake",
      correlationId: delivery.jobId,
      target,
      attempt: delivery.delivery?.attempt,
      outcome: "failed",
      errorCode: error?.code || "wake_error",
      late: delivery.ack?.late,
    });
    throw error;
  }
}

export async function handleClaudeRequestOrMessage(
  target,
  parsed,
  {
    readRecord = readSessionRecord,
    findGrant = findClaudeRequestGrant,
    createRequest = createClaudeRequestJob,
    scheduleRequest,
    deliverMessage = deliverClaudeMessage,
  } = {},
) {
  const currentRecord = readRecord(target);
  if (!currentRecord) throw new Error(`unknown Codex session: ${target}`);
  const request = parseClaudeRequestBody(parsed.body);
  const grant = request
    ? findGrant(currentRecord, {
        ...parsed,
        grantToken: request.grantToken,
      })
    : null;
  if (request && grant) {
    if (typeof scheduleRequest !== "function") {
      throw new Error("authorized Claude request requires a scheduler");
    }
    const job = await createRequest({
      target,
      targetRecord: currentRecord,
      parsed,
      grant,
      task: request.task,
    });
    scheduleRequest(currentRecord, job);
    return { kind: "request", job };
  }
  const delivery = await deliverMessage(target, {
    ...parsed,
    body: redactClaudeRequestCapabilities(parsed.body),
  });
  return { kind: "message", delivery };
}

export async function returnClaudePeerStatus(
  target,
  parsed,
  status,
  { send = sendClaudePeerFrame, log = writeCoordinationEvent } = {},
) {
  try {
    await send(
      parsed.fromSocket,
      buildClaudePeerStatusFrame({ messageId: parsed.messageId, status }),
    );
    await log({
      kind: "claude-native-peer",
      phase: "status-returned",
      correlationId: parsed.messageId,
      target,
      outcome: status,
    });
    return true;
  } catch (error) {
    await log({
      kind: "claude-native-peer",
      phase: "status-return",
      correlationId: parsed.messageId,
      target,
      outcome: "failed",
      errorCode: error?.code || "status_return_error",
    });
    return false;
  }
}

export async function runClaudeBridge(target) {
  validateSessionName(target);
  const targetRecord = readSessionRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);

  mkdirSync(CLAUDE_BRIDGES_DIR, { recursive: true, mode: 0o700 });
  chmodSync(CLAUDE_BRIDGES_DIR, 0o700);
  mkdirSync(CLAUDE_SESSIONS_DIR, { recursive: true, mode: 0o700 });
  const socketsDir = claudeSocketsDir();
  mkdirSync(socketsDir, { recursive: true, mode: 0o700 });

  const socketPath = path.join(socketsDir, `${process.pid}.sock`);
  if (existsSync(socketPath)) unlinkSync(socketPath);
  const record = {
    version: 1,
    cxmsgVersion: CXMSG_VERSION,
    implementationRevision: CLAUDE_BRIDGE_IMPLEMENTATION_REVISION,
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

  const scanDeliveries = async () => {
    for (const job of listJobs()) {
      if (job.kind !== "claude-delivery" || job.from !== target) continue;
      const current = await refreshClaudeDelivery(job);
      if (current.status === "retry_scheduled") scheduleDelivery(current);
    }
  };
  const deliveryMonitor = setInterval(() => {
    scanDeliveries().catch((error) => {
      process.stderr.write(`cxmsg Claude delivery scan failed: ${error.message}\n`);
    });
  }, 5_000);
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
      let parsed = null;
      try {
        const line = Buffer.concat(chunks).toString("utf8").trim();
        if (!line) return;
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
              cxmsgVersion: record.cxmsgVersion,
              implementationRevision: record.implementationRevision,
            })}\n`,
          );
          return;
        }
        writeClaudeRegistry(record, targetRecord, "handling");
        const nativeReceipt = parseClaudePeerStatusFrame(frame);
        if (nativeReceipt) {
          await recordClaudeNativeDeliveryReceipt(nativeReceipt);
          return;
        }
        parsed = parseClaudePeerFrame(frame);
        const deliveryAck = parseClaudeDeliveryAck(parsed.body);
        if (deliveryAck) {
          const delivery = await handleClaudeDeliveryAck(
            target,
            parsed,
            deliveryAck,
          );
          if (!delivery) throw new Error(`unknown Claude delivery: ${deliveryAck.jobId}`);
          scheduleDelivery(delivery);
          await returnClaudePeerStatus(target, parsed, "delivered");
          return;
        }
        if (parsed.replyToMessageId) {
          try {
            await recordClaudeDeliveryReply(parsed, parsed.replyToMessageId);
          } catch (error) {
            await writeCoordinationEvent({
              kind: "claude-delivery",
              phase: "reply-correlation",
              correlationId: parsed.replyToMessageId,
              target,
              outcome: "rejected",
              errorCode: error?.code || "reply_correlation_error",
            });
          }
        }
        const handled = await handleClaudeRequestOrMessage(target, parsed, {
          scheduleRequest,
        });
        const denied =
          handled.kind === "message" &&
          handled.delivery?.delivery === "quarantined";
        await returnClaudePeerStatus(
          target,
          parsed,
          denied ? "denied" : "delivered",
        );
      } catch (error) {
        if (parsed) await returnClaudePeerStatus(target, parsed, "denied");
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
    const onListenError = (error) => reject(error);
    server.once("error", onListenError);
    server.listen(socketPath, () => {
      server.off("error", onListenError);
      chmodSync(socketPath, 0o600);
      atomicWrite(bridgeRecordPath(target), record);
      writeClaudeRegistry(record, targetRecord, "idle");
      resolve();
    });
  });
  server.on("error", (error) => {
    process.stderr.write(`cxmsg Claude bridge listener failed: ${error.message}\n`);
    cleanup();
    process.exitCode = 1;
  });

  try {
    await scanDeliveries();
    for (const job of listJobs()) {
      if (
        job.kind === "claude-request" &&
        job.target === target &&
        (isPendingJob(job) || job.reply?.status !== "delivered")
      ) {
        scheduleRequest(targetRecord, job);
      }
    }
  } catch (error) {
    cleanup();
    throw error;
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
