import { randomUUID, createHash } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { AppServerClient } from "./app-server-client.js";
import {
  appendDeliveryEvidence,
  beginScheduledDelivery,
  claimScheduledDelivery,
  listDeliveryLedgerIndexed,
  releaseScheduledDeliveryClaim,
} from "./delivery-ledger.js";
import {
  SCHEDULER_HEARTBEAT_MS,
  SCHEDULER_CLAIM_LEASE_MS,
  SCHEDULER_POLL_MS,
} from "./delivery-policy.js";
import {
  MAX_MESSAGE_READ_BYTES,
  readMessageBody,
} from "./message-bodies.js";
import {
  failJobIfWorkerExited,
  isPendingJob,
  readJob,
} from "./jobs.js";
import {
  deliverPeerMessageWhenIdle,
  TargetBusyError,
} from "./messaging.js";
import {
  readSessionRecord,
  sessionAllowsAppServerResume,
} from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";
import {
  findThreadTurn,
  isTerminalTurnStatus,
  readThreadMetadata,
} from "./thread-activity.js";
import { writeCoordinationEvent } from "./observability.js";

export const SCHEDULER_RECORD_PATH = path.join(CXMSG_STATE_DIR, "scheduler.json");
export const SCHEDULER_LOG_PATH = path.join(CXMSG_STATE_DIR, "scheduler.log");
export const SCHEDULER_LIFECYCLE_LOCK_PATH = path.join(
  CXMSG_STATE_DIR,
  "scheduler.lifecycle.lock",
);
export {
  SCHEDULER_CLAIM_LEASE_MS,
  SCHEDULER_HEARTBEAT_MS,
  SCHEDULER_POLL_MS,
};

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function atomicWrite(destination, value) {
  mkdirSync(path.dirname(destination), { recursive: true, mode: 0o700 });
  chmodSync(path.dirname(destination), 0o700);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, destination);
}

export function readSchedulerRecord() {
  try {
    const record = JSON.parse(readFileSync(SCHEDULER_RECORD_PATH, "utf8"));
    if (
      ![1, 2].includes(record?.version) ||
      !Number.isSafeInteger(record.pid) ||
      record.pid < 2 ||
      !UUID_PATTERN.test(record.workerId || "") ||
      !Number.isFinite(Date.parse(record.startedAt || "")) ||
      (record.version === 2 && !Number.isFinite(Date.parse(record.heartbeatAt || "")))
    ) {
      return null;
    }
    return record;
  } catch {
    return null;
  }
}

function updateSchedulerHeartbeat(identity, patch = {}) {
  const current = readSchedulerRecord();
  if (
    current?.pid !== identity.pid ||
    current.workerId !== identity.workerId ||
    current.version !== 2
  ) {
    return false;
  }
  atomicWrite(SCHEDULER_RECORD_PATH, {
    ...current,
    ...patch,
    heartbeatAt: new Date().toISOString(),
  });
  return true;
}

function boundedErrorCode(error) {
  return typeof error?.code === "string" && /^[A-Z0-9_]{1,32}$/.test(error.code)
    ? error.code
    : "ESCHEDULERUNKNOWN";
}

function wholeStoredBody(contentRef) {
  if (!contentRef) throw new Error("scheduled Delivery has no retained body reference");
  const parts = [];
  let offset = 0;
  let info = null;
  do {
    info = readMessageBody(contentRef, { offset, limit: MAX_MESSAGE_READ_BYTES });
    parts.push(info.text);
    offset = info.nextOffset;
  } while (!info.complete);
  return parts.join("");
}

export async function scheduledTriggerReadiness(
  record,
  client,
  {
    findTurn = findThreadTurn,
    job = readJob,
    pendingJob = isPendingJob,
    refreshPendingJob = failJobIfWorkerExited,
  } = {},
) {
  const route = record.logicalMessage.route;
  if (record.delivery.wakePolicy === "when-idle") return { state: "eligible" };
  if (record.delivery.wakePolicy === "after-turn") {
    let turn;
    try {
      turn = await findTurn(
        client,
        record.delivery.targetThreadId,
        route?.trigger_turn_id,
      );
    } catch {
      return { state: "blocked", errorCode: "ETRIGGERUNAVAILABLE" };
    }
    if (!turn) return { state: "blocked", errorCode: "ETRIGGERNOTFOUND" };
    if (turn.id !== route?.trigger_turn_id) {
      return { state: "blocked", errorCode: "ETRIGGERMISMATCH" };
    }
    if (turn.status === "inProgress") return { state: "waiting-trigger" };
    return isTerminalTurnStatus(turn.status)
      ? { state: "eligible" }
      : { state: "blocked", errorCode: "ETRIGGERSTATUS" };
  }
  if (record.delivery.wakePolicy === "after-job") {
    let current;
    try {
      current = job(route?.trigger_job_id);
      if (current && pendingJob(current)) {
        current = await refreshPendingJob(current);
      }
    } catch {
      return { state: "blocked", errorCode: "ETRIGGERUNAVAILABLE" };
    }
    if (!current) return { state: "blocked", errorCode: "ETRIGGERNOTFOUND" };
    if (current.jobId !== route?.trigger_job_id) {
      return { state: "blocked", errorCode: "ETRIGGERMISMATCH" };
    }
    if (pendingJob(current)) return { state: "waiting-trigger" };
    return typeof current.status === "string" && current.status !== "unknown"
      ? { state: "eligible" }
      : { state: "blocked", errorCode: "ETRIGGERSTATUS" };
  }
  return { state: "blocked", errorCode: "ETRIGGERPOLICY" };
}

async function eligibleScheduledDeliveries(
  client,
  now,
  triggerReadiness = scheduledTriggerReadiness,
) {
  const firstByTarget = new Map();
  const expired = [];
  for (const record of await listDeliveryLedgerIndexed()) {
    const delivery = record.delivery;
    if (
      delivery.admissionState !== "admitted" ||
      !["when-idle", "after-turn", "after-job"].includes(delivery.wakePolicy) ||
      delivery.state !== "scheduled" ||
      delivery.attempts.length > 0
    ) {
      continue;
    }
    const expiry = Date.parse(record.logicalMessage.route?.expiry || "");
    if (Number.isFinite(expiry) && expiry <= now) {
      expired.push(record);
      continue;
    }
    const readiness = await triggerReadiness(record, client);
    if (readiness.state !== "eligible") continue;
    const lane = delivery.targetThreadId || delivery.target;
    if (!firstByTarget.has(lane)) firstByTarget.set(lane, record);
  }
  return [...expired, ...firstByTarget.values()].filter((record) => {
    const claim = record.delivery.claim;
    return !claim || Date.parse(claim.leaseUntil) <= now;
  });
}

async function markUnknown(messageId, error, now) {
  return appendDeliveryEvidence(messageId, {
    attemptId: null,
    state: "unknown",
    evidenceKind: "scheduler",
    errorCode: boundedErrorCode(error),
    observedAt: new Date(now).toISOString(),
  });
}

export async function dispatchScheduledDelivery(
  record,
  client,
  workerId,
  {
    now = () => Date.now(),
    session = readSessionRecord,
    readThread = readThreadMetadata,
    deliver = deliverPeerMessageWhenIdle,
    triggerReadiness = scheduledTriggerReadiness,
    log = writeCoordinationEvent,
  } = {},
) {
  const messageId = record.logicalMessage.messageId;
  const delivery = record.delivery;
  const expiry = record.logicalMessage.route?.expiry
    ? Date.parse(record.logicalMessage.route.expiry)
    : null;
  if (expiry !== null && expiry <= now()) {
    await appendDeliveryEvidence(messageId, {
      attemptId: null,
      state: "expired",
      evidenceKind: "scheduler",
      errorCode: "EDELIVERYEXPIRED",
      observedAt: new Date(now()).toISOString(),
    });
    await log({
      kind: "scheduled-delivery",
      phase: "expiry",
      correlationId: messageId,
      target: delivery.target,
      outcome: "expired",
      errorCode: "EDELIVERYEXPIRED",
    });
    return { state: "expired", messageId };
  }

  const readiness = await triggerReadiness(record, client);
  if (readiness.state !== "eligible") {
    return { ...readiness, messageId };
  }

  const target = session(delivery.target);
  if (!target || target.threadId !== delivery.targetThreadId) {
    const error = new Error("scheduled Delivery target identity is unavailable");
    error.code = "ETARGETIDENTITY";
    await markUnknown(messageId, error, now());
    await log({
      kind: "scheduled-delivery",
      phase: "target",
      correlationId: messageId,
      target: delivery.target,
      outcome: "unknown",
      errorCode: error.code,
    });
    return { state: "unknown", messageId, errorCode: error.code };
  }
  const observed = await readThread(client, target.threadId);
  if (observed.status?.type === "active") return { state: "busy", messageId };
  const allowResume = sessionAllowsAppServerResume(target);
  if (observed.status?.type === "notLoaded" && !allowResume) {
    return {
      state: "blocked",
      messageId,
      errorCode: "EEXTERNALWRITERUNVERIFIED",
    };
  }

  const claimResult = await claimScheduledDelivery(messageId, {
    workerId,
    leaseMs: SCHEDULER_CLAIM_LEASE_MS,
    now: new Date(now()).toISOString(),
  });
  if (!claimResult.acquired) return { state: "claimed", messageId };
  await log({
    kind: "scheduled-delivery",
    phase: "claim",
    correlationId: messageId,
    target: delivery.target,
    outcome: "acquired",
    errorCode: null,
  });

  let attempt = null;
  try {
    const currentTarget = session(delivery.target);
    if (!currentTarget || currentTarget.threadId !== delivery.targetThreadId) {
      const error = new Error("scheduled Delivery target identity changed after claim");
      error.code = "ETARGETIDENTITY";
      throw error;
    }
    const currentReadiness = await triggerReadiness(record, client);
    if (currentReadiness.state !== "eligible") {
      await releaseScheduledDeliveryClaim(messageId, {
        claimId: claimResult.claim.claimId,
        workerId,
        reason: "dispatch_unavailable",
        now: new Date(now()).toISOString(),
      });
      await log({
        kind: "scheduled-delivery",
        phase: "claim-release",
        correlationId: messageId,
        target: delivery.target,
        outcome: currentReadiness.state,
        errorCode: currentReadiness.errorCode || null,
      });
      return { ...currentReadiness, messageId };
    }
    const current = await readThread(client, currentTarget.threadId);
    if (current.status?.type === "active") throw new TargetBusyError();

    const message = wholeStoredBody(record.logicalMessage.body.contentRef);
    if (
      Buffer.byteLength(message, "utf8") !== record.logicalMessage.body.bytes ||
      createHash("sha256").update(message).digest("hex") !== record.logicalMessage.body.sha256
    ) {
      const error = new Error("scheduled Delivery body does not match Ledger evidence");
      error.code = "EBODYINTEGRITY";
      throw error;
    }
    const result = await deliver(
      client,
      current,
      {
        from: record.logicalMessage.from,
        message,
        messageId,
        replyTo: record.logicalMessage.replyToMessageId || null,
        route: record.logicalMessage.route,
      },
      {
        allowResume,
        beforeStart: async () => {
          attempt = await beginScheduledDelivery(messageId, {
            claimId: claimResult.claim.claimId,
            workerId,
            now: new Date(now()).toISOString(),
          });
        },
      },
    );
    await appendDeliveryEvidence(messageId, {
      attemptId: attempt.attemptId,
      state: "turn_started",
      evidenceKind: "dispatch-result",
      transportResult: result.delivery,
      turnId: result.turnId,
      observedAt: new Date(now()).toISOString(),
    });
    await log({
      kind: "scheduled-delivery",
      phase: "dispatch",
      correlationId: messageId,
      target: delivery.target,
      outcome: "turn_started",
      errorCode: null,
    });
    return { state: "turn_started", messageId, turnId: result.turnId };
  } catch (error) {
    if ((error instanceof TargetBusyError || error?.code === "ETARGETBUSY") && !attempt) {
      await releaseScheduledDeliveryClaim(messageId, {
        claimId: claimResult.claim.claimId,
        workerId,
        reason: "target_busy",
        now: new Date(now()).toISOString(),
      });
      await log({
        kind: "scheduled-delivery",
        phase: "claim-release",
        correlationId: messageId,
        target: delivery.target,
        outcome: "target_busy",
        errorCode: "ETARGETBUSY",
      });
      return { state: "busy", messageId };
    }
    await appendDeliveryEvidence(messageId, {
      attemptId: attempt?.attemptId || null,
      state: "unknown",
      evidenceKind: attempt ? "dispatch-result" : "scheduler",
      errorCode: boundedErrorCode(error),
      observedAt: new Date(now()).toISOString(),
    });
    await log({
      kind: "scheduled-delivery",
      phase: "dispatch",
      correlationId: messageId,
      target: delivery.target,
      outcome: "unknown",
      errorCode: boundedErrorCode(error),
    });
    return { state: "unknown", messageId, errorCode: boundedErrorCode(error) };
  }
}

export async function runSchedulerPass(
  client,
  workerId,
  {
    now = () => Date.now(),
    dispatch = dispatchScheduledDelivery,
    triggerReadiness = scheduledTriggerReadiness,
  } = {},
) {
  const outcomes = [];
  for (const record of await eligibleScheduledDeliveries(
    client,
    now(),
    triggerReadiness,
  )) {
    outcomes.push(
      await dispatch(record, client, workerId, { now, triggerReadiness }),
    );
  }
  return outcomes;
}

export async function runSchedulerWorker({
  Client = AppServerClient,
  pollMs = SCHEDULER_POLL_MS,
  once = false,
} = {}) {
  const workerId = randomUUID();
  const record = {
    version: 2,
    pid: process.pid,
    workerId,
    startedAt: new Date().toISOString(),
    heartbeatAt: new Date().toISOString(),
    lastPassAt: null,
    lastErrorCode: null,
    lastOutcomeCount: 0,
  };
  atomicWrite(SCHEDULER_RECORD_PATH, record);
  let lastHeartbeatAt = Date.now();
  let stopping = false;
  const stop = () => {
    stopping = true;
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    do {
      const client = new Client();
      try {
        await client.connect();
        do {
          const outcomes = await runSchedulerPass(client, workerId);
          const heartbeatDue = Date.now() - lastHeartbeatAt >= SCHEDULER_HEARTBEAT_MS;
          if (heartbeatDue || outcomes.length > 0) {
            updateSchedulerHeartbeat(record, {
              lastPassAt: new Date().toISOString(),
              lastErrorCode: null,
              lastOutcomeCount: outcomes.length,
            });
            lastHeartbeatAt = Date.now();
          }
          if (!once && !stopping) await delay(pollMs);
        } while (!once && !stopping);
      } catch (error) {
        updateSchedulerHeartbeat(record, {
          lastPassAt: new Date().toISOString(),
          lastErrorCode: boundedErrorCode(error),
          lastOutcomeCount: 0,
        });
        lastHeartbeatAt = Date.now();
        if (once) throw error;
      } finally {
        await client.close();
      }
      if (!once && !stopping) await delay(pollMs);
    } while (!once && !stopping);
  } finally {
    const saved = readSchedulerRecord();
    if (
      saved?.pid === process.pid &&
      saved.workerId === workerId &&
      existsSync(SCHEDULER_RECORD_PATH)
    ) {
      unlinkSync(SCHEDULER_RECORD_PATH);
    }
  }
  return record;
}
