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
import { AppServerClient, appServerVersion } from "./app-server-client.js";
import {
  appendDeliveryEvidence,
  beginScheduledDelivery,
  claimScheduledDelivery,
  listDeliveryLedgerIndexed,
  releaseScheduledDeliveryClaim,
  renewScheduledDeliveryClaim,
} from "./delivery-ledger.js";
import {
  SCHEDULER_HEARTBEAT_MS,
  SCHEDULER_CLAIM_LEASE_MS,
  SCHEDULER_POLL_MS,
} from "./delivery-policy.js";
import { readWholeMessageBody } from "./message-bodies.js";
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
  listRecentTurns,
  readThreadMetadata,
} from "./thread-activity.js";
import { writeCoordinationEvent } from "./observability.js";
import { findNodeSuccessors } from "./node-directory.js";
import {
  beginTurnLifecycleConnection,
  endTurnLifecycleConnection,
  observeTurnLifecycleCatchUp,
  observeTurnLifecycleNotification,
} from "./turn-lifecycle.js";

export const SCHEDULER_RECORD_PATH = path.join(CXMSG_STATE_DIR, "scheduler.json");
export const SCHEDULER_LOG_PATH = path.join(CXMSG_STATE_DIR, "scheduler.log");
export const SCHEDULER_INTENT_PATH = path.join(
  CXMSG_STATE_DIR,
  "scheduler.intent.json",
);
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

export function readSchedulerIntent() {
  try {
    const record = JSON.parse(readFileSync(SCHEDULER_INTENT_PATH, "utf8"));
    if (
      record?.version !== 1 ||
      !["running", "stopped"].includes(record.desiredState) ||
      !Number.isFinite(Date.parse(record.changedAt || ""))
    ) {
      return null;
    }
    return record;
  } catch {
    return null;
  }
}

export function writeSchedulerIntent(desiredState, { now } = {}) {
  if (!["running", "stopped"].includes(desiredState)) {
    throw new Error("invalid Scheduler desired state");
  }
  const changedAt = now || new Date().toISOString();
  if (!Number.isFinite(Date.parse(changedAt))) {
    throw new Error("invalid Scheduler intent timestamp");
  }
  const record = { version: 1, desiredState, changedAt };
  atomicWrite(SCHEDULER_INTENT_PATH, record);
  return record;
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
  const candidates = (await listDeliveryLedgerIndexed())
    .filter((record) => {
      const delivery = record.delivery;
      return (
        delivery.admissionState === "admitted" &&
        ["when-idle", "after-turn", "after-job"].includes(delivery.wakePolicy) &&
        delivery.state === "scheduled" &&
        delivery.attempts.length === 0
      );
    })
    .sort((left, right) => {
      const byCreation = left.delivery.createdAt.localeCompare(right.delivery.createdAt);
      return byCreation || left.logicalMessage.messageId.localeCompare(right.logicalMessage.messageId);
    });
  for (const record of candidates) {
    const delivery = record.delivery;
    const lane = delivery.targetThreadId || delivery.target;
    if (!firstByTarget.has(lane)) firstByTarget.set(lane, record);
  }
  const ready = [];
  for (const record of firstByTarget.values()) {
    const expiry = Date.parse(record.logicalMessage.route?.expiry || "");
    if (Number.isFinite(expiry) && expiry <= now) {
      ready.push(record);
      continue;
    }
    if ((await triggerReadiness(record, client)).state === "eligible") {
      ready.push(record);
    }
  }
  return ready.filter((record) => {
    const claim = record.delivery.claim;
    return !claim || Date.parse(claim.leaseUntil) <= now;
  });
}

export async function reconcileTurnLifecycle(
  client,
  {
    readThread = readThreadMetadata,
    listTurns = listRecentTurns,
    observe = observeTurnLifecycleCatchUp,
    limit = 256,
  } = {},
) {
  const targets = new Set();
  for (const record of await listDeliveryLedgerIndexed()) {
    if (
      record.delivery.admissionState === "admitted" &&
      record.delivery.state === "scheduled" &&
      record.delivery.attempts.length === 0 &&
      record.delivery.targetThreadId
    ) {
      targets.add(record.delivery.targetThreadId);
      if (targets.size >= limit) break;
    }
  }
  const outcomes = [];
  for (const threadId of targets) {
    try {
      const thread = await readThread(client, threadId);
      let page = { data: [], nextCursor: null };
      let errorCode = null;
      try {
        page = await listTurns(client, threadId, {
          limit: 8,
          itemsView: "notLoaded",
        });
      } catch (error) {
        errorCode = boundedErrorCode(error);
      }
      observe(thread, page);
      outcomes.push({
        threadId,
        state: errorCode ? "metadata-only" : "observed",
        ...(errorCode ? { errorCode } : {}),
      });
    } catch (error) {
      outcomes.push({
        threadId,
        state: "unavailable",
        errorCode: boundedErrorCode(error),
      });
    }
  }
  return outcomes;
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

export function scheduledTargetIdentity(record, { successors = findNodeSuccessors } = {}) {
  const threadId = record?.delivery?.targetThreadId;
  if (!UUID_PATTERN.test(threadId || "")) return { state: "legacy" };
  const nodeKey = `codex:${threadId.toLowerCase()}`;
  const replacements = successors(nodeKey);
  if (replacements.length === 0) return { state: "current", nodeKey };
  return {
    state: "predecessor",
    nodeKey,
    successorNodeKeys: replacements.map((relation) => relation.successorNodeKey).sort(),
  };
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
    renewClaim = renewScheduledDeliveryClaim,
    targetIdentity = scheduledTargetIdentity,
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

  let identity;
  try {
    identity = targetIdentity(record);
  } catch {
    identity = { state: "unavailable" };
  }
  if (identity.state === "unavailable") {
    await log({
      kind: "scheduled-delivery",
      phase: "target-identity",
      correlationId: messageId,
      target: delivery.target,
      outcome: "blocked",
      errorCode: "ESUCCESSORUNAVAILABLE",
    });
    return { state: "blocked", messageId, errorCode: "ESUCCESSORUNAVAILABLE" };
  }
  if (identity.state === "predecessor") {
    await log({
      kind: "scheduled-delivery",
      phase: "target-identity",
      correlationId: messageId,
      target: delivery.target,
      outcome: "blocked",
      errorCode: "ETARGETPREDECESSOR",
    });
    return { state: "blocked", messageId, errorCode: "ETARGETPREDECESSOR" };
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
    let currentIdentity;
    try {
      currentIdentity = targetIdentity(record);
    } catch {
      currentIdentity = { state: "unavailable" };
    }
    if (["predecessor", "unavailable"].includes(currentIdentity.state)) {
      const identityErrorCode =
        currentIdentity.state === "predecessor"
          ? "ETARGETPREDECESSOR"
          : "ESUCCESSORUNAVAILABLE";
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
        outcome: currentIdentity.state,
        errorCode: identityErrorCode,
      });
      return { state: "blocked", messageId, errorCode: identityErrorCode };
    }
    const current = await readThread(client, currentTarget.threadId);
    if (current.status?.type === "active") throw new TargetBusyError();

    if (!record.logicalMessage.body.contentRef) {
      throw new Error("scheduled Delivery has no retained body reference");
    }
    const message = readWholeMessageBody(record.logicalMessage.body.contentRef);
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
        replyHandle: delivery.replyHandle || null,
        legacyReplyMessageId:
          !delivery.replyHandle &&
          record.logicalMessage.senderThreadId &&
          delivery.targetThreadId
            ? messageId
            : null,
        route: record.logicalMessage.route,
      },
      {
        allowResume,
        beforeStart: async () => {
          await renewClaim(messageId, {
            claimId: claimResult.claim.claimId,
            workerId,
            leaseMs: SCHEDULER_CLAIM_LEASE_MS,
            now: new Date(now()).toISOString(),
          });
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
    if (!attempt && error?.code === "ECLAIMLOST") {
      await log({
        kind: "scheduled-delivery",
        phase: "claim-loss",
        correlationId: messageId,
        target: delivery.target,
        outcome: "stopped",
        errorCode: "ECLAIMLOST",
      });
      return { state: "claim_lost", messageId, errorCode: "ECLAIMLOST" };
    }
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

export function createSchedulerWakeSignal() {
  let pending = false;
  let waiter = null;
  return {
    wake() {
      if (waiter) {
        const current = waiter;
        waiter = null;
        current();
      } else {
        pending = true;
      }
    },
    wait(timeoutMs) {
      if (pending) {
        pending = false;
        return Promise.resolve("event");
      }
      return new Promise((resolve) => {
        const timer = setTimeout(() => {
          if (waiter === onWake) waiter = null;
          resolve("poll");
        }, timeoutMs);
        const onWake = () => {
          clearTimeout(timer);
          resolve("event");
        };
        waiter = onWake;
      });
    },
  };
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
  const wakeSignal = createSchedulerWakeSignal();
  const stop = () => {
    stopping = true;
    wakeSignal.wake();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    do {
      let connectionEpoch = null;
      const client = new Client({
        onNotification: (message) => {
          try {
            const observation = observeTurnLifecycleNotification(message);
            if (observation) wakeSignal.wake();
          } catch (error) {
            updateSchedulerHeartbeat(record, {
              lastErrorCode: boundedErrorCode(error),
            });
          }
        },
        onDisconnect: () => wakeSignal.wake(),
      });
      try {
        await client.connect();
        connectionEpoch = beginTurnLifecycleConnection({
          appServerVersion: appServerVersion(client.initializeResult?.userAgent),
        }).epoch;
        await reconcileTurnLifecycle(client);
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
          if (!once && !stopping) await wakeSignal.wait(pollMs);
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
        try {
          if (connectionEpoch) endTurnLifecycleConnection(connectionEpoch);
        } catch (error) {
          updateSchedulerHeartbeat(record, {
            lastErrorCode: boundedErrorCode(error),
          });
        } finally {
          await client.close();
        }
      }
      if (!once && !stopping) await wakeSignal.wait(pollMs);
    } while (!once && !stopping);
  } finally {
    const saved = readSchedulerRecord();
    if (
      (once || stopping) &&
      saved?.pid === process.pid &&
      saved.workerId === workerId &&
      existsSync(SCHEDULER_RECORD_PATH)
    ) {
      unlinkSync(SCHEDULER_RECORD_PATH);
    }
  }
  return record;
}
