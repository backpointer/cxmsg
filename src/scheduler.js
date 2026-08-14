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
  listDeliveryLedger,
  releaseScheduledDeliveryClaim,
} from "./delivery-ledger.js";
import {
  SCHEDULER_CLAIM_LEASE_MS,
  SCHEDULER_POLL_MS,
} from "./delivery-policy.js";
import {
  MAX_MESSAGE_READ_BYTES,
  readMessageBody,
} from "./message-bodies.js";
import {
  deliverPeerMessageWhenIdle,
  TargetBusyError,
} from "./messaging.js";
import { readSessionRecord } from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";
import { readThreadMetadata } from "./thread-activity.js";

export const SCHEDULER_RECORD_PATH = path.join(CXMSG_STATE_DIR, "scheduler.json");
export const SCHEDULER_LOG_PATH = path.join(CXMSG_STATE_DIR, "scheduler.log");
export const SCHEDULER_LIFECYCLE_LOCK_PATH = path.join(
  CXMSG_STATE_DIR,
  "scheduler.lifecycle.lock",
);
export { SCHEDULER_CLAIM_LEASE_MS, SCHEDULER_POLL_MS };

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
      record?.version !== 1 ||
      !Number.isSafeInteger(record.pid) ||
      record.pid < 2 ||
      !UUID_PATTERN.test(record.workerId || "") ||
      !Number.isFinite(Date.parse(record.startedAt || ""))
    ) {
      return null;
    }
    return record;
  } catch {
    return null;
  }
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

function eligibleScheduledDeliveries(now) {
  const firstByTarget = new Map();
  for (const record of listDeliveryLedger()) {
    const delivery = record.delivery;
    if (
      delivery.admissionState !== "admitted" ||
      delivery.wakePolicy !== "when-idle" ||
      delivery.state !== "scheduled" ||
      delivery.attempts.length > 0
    ) {
      continue;
    }
    const lane = delivery.targetThreadId || delivery.target;
    if (!firstByTarget.has(lane)) firstByTarget.set(lane, record);
  }
  return [...firstByTarget.values()].filter((record) => {
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
    return { state: "expired", messageId };
  }

  const target = session(delivery.target);
  if (!target || target.threadId !== delivery.targetThreadId) {
    const error = new Error("scheduled Delivery target identity is unavailable");
    error.code = "ETARGETIDENTITY";
    await markUnknown(messageId, error, now());
    return { state: "unknown", messageId, errorCode: error.code };
  }
  const observed = await readThread(client, target.threadId);
  if (observed.status?.type === "active") return { state: "busy", messageId };

  const claimResult = await claimScheduledDelivery(messageId, {
    workerId,
    leaseMs: SCHEDULER_CLAIM_LEASE_MS,
    now: new Date(now()).toISOString(),
  });
  if (!claimResult.acquired) return { state: "claimed", messageId };

  let attempt = null;
  try {
    const currentTarget = session(delivery.target);
    if (!currentTarget || currentTarget.threadId !== delivery.targetThreadId) {
      const error = new Error("scheduled Delivery target identity changed after claim");
      error.code = "ETARGETIDENTITY";
      throw error;
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
        route: record.logicalMessage.route,
      },
      {
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
    return { state: "turn_started", messageId, turnId: result.turnId };
  } catch (error) {
    if ((error instanceof TargetBusyError || error?.code === "ETARGETBUSY") && !attempt) {
      await releaseScheduledDeliveryClaim(messageId, {
        claimId: claimResult.claim.claimId,
        workerId,
        reason: "target_busy",
        now: new Date(now()).toISOString(),
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
    return { state: "unknown", messageId, errorCode: boundedErrorCode(error) };
  }
}

export async function runSchedulerPass(
  client,
  workerId,
  { now = () => Date.now(), dispatch = dispatchScheduledDelivery } = {},
) {
  const outcomes = [];
  for (const record of eligibleScheduledDeliveries(now())) {
    outcomes.push(await dispatch(record, client, workerId, { now }));
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
    version: 1,
    pid: process.pid,
    workerId,
    startedAt: new Date().toISOString(),
  };
  atomicWrite(SCHEDULER_RECORD_PATH, record);
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
          await runSchedulerPass(client, workerId);
          if (!once && !stopping) await delay(pollMs);
        } while (!once && !stopping);
      } catch (error) {
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
