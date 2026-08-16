import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import {
  MAX_MESSAGE_BYTES,
  validateMessage,
  validateSessionName,
} from "./messaging.js";
import { appServerFailureEvidence } from "./app-server-client.js";
import { processState } from "./process-state.js";
import { MAX_STORED_MESSAGE_BYTES } from "./message-bodies.js";
import {
  assertRetentionReadable,
  assertRetentionReadableNoCreate,
  withRetentionWriter,
} from "./retention-barrier.js";
import { CXMSG_STATE_DIR } from "./runtime.js";
import {
  findFinalTurnResult,
  findThreadTurn,
} from "./thread-activity.js";

const JOBS_DIR = path.join(CXMSG_STATE_DIR, "jobs");
const WORKER_REGISTRATION_GRACE_MS = 10_000;

export const JOB_OBSERVATION_NOTIFICATION_OPT_OUT = Object.freeze([
  "item/completed",
  "turn/completed",
]);

function ensureJobsDirectory() {
  mkdirSync(JOBS_DIR, { recursive: true, mode: 0o700 });
  chmodSync(JOBS_DIR, 0o700);
}

function validateJobId(jobId) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(jobId || "")) {
    throw new Error("job-id must be a UUID");
  }
  return jobId;
}

function jobPath(jobId) {
  return path.join(JOBS_DIR, `${validateJobId(jobId)}.json`);
}

function jobLockPath(jobId) {
  return path.join(JOBS_DIR, `${validateJobId(jobId)}.lock`);
}

function delegationTaskFields(jobId, task, taskBody) {
  if (!taskBody) {
    return { task: validateMessage(task), taskBody: null };
  }
  if (task !== null && task !== undefined) {
    throw new Error("stored Delegation task must not be duplicated in the Job");
  }
  if (
    taskBody.messageId !== jobId ||
    taskBody.contentRef !== `cxmsg-message:${jobId}` ||
    !Number.isSafeInteger(taskBody.bodyBytes) ||
    taskBody.bodyBytes <= MAX_MESSAGE_BYTES ||
    taskBody.bodyBytes > MAX_STORED_MESSAGE_BYTES ||
    !/^[0-9a-f]{64}$/.test(taskBody.bodySha256 || "")
  ) {
    throw new Error("stored Delegation task reference is invalid");
  }
  return {
    task: null,
    taskBody: {
      messageId: taskBody.messageId,
      contentRef: taskBody.contentRef,
      bodyBytes: taskBody.bodyBytes,
      bodySha256: taskBody.bodySha256,
    },
  };
}

export function newJobId() {
  return randomUUID();
}

export function readJob(jobId) {
  assertRetentionReadable();
  try {
    const job = JSON.parse(readFileSync(jobPath(jobId), "utf8"));
    if (job?.version !== 1 || job.jobId !== jobId) return null;
    return job;
  } catch {
    return null;
  }
}

export function listJobs() {
  assertRetentionReadable();
  if (!existsSync(JOBS_DIR)) return [];
  return readdirSync(JOBS_DIR)
    .filter((filename) => /^[0-9a-f-]+\.json$/i.test(filename))
    .map((filename) => readJob(filename.slice(0, -5)))
    .filter(Boolean);
}

export function listJobsReadOnly() {
  assertRetentionReadableNoCreate();
  if (!existsSync(JOBS_DIR)) return [];
  return readdirSync(JOBS_DIR)
    .filter((filename) => /^[0-9a-f-]+\.json$/i.test(filename))
    .map((filename) => {
      try {
        const job = JSON.parse(
          readFileSync(path.join(JOBS_DIR, filename), "utf8"),
        );
        return job?.version === 1 && `${job.jobId}.json` === filename
          ? job
          : null;
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

export function writeJob(job) {
  return withRetentionWriter(() => {
    validateJobId(job.jobId);
    ensureJobsDirectory();
    const destination = jobPath(job.jobId);
    const temporary = `${destination}.${randomUUID()}.tmp`;
    writeFileSync(temporary, `${JSON.stringify(job, null, 2)}\n`, {
      mode: 0o600,
    });
    renameSync(temporary, destination);
    return job;
  });
}

function buildJob({
  jobId = newJobId(),
  from,
  target,
  targetThreadId = null,
  threadId,
  task,
  taskBody = null,
  permissions = null,
  kind = "delegation",
  source = null,
  reply = null,
  execution = "fork",
  approval = "never",
  mirror = "none",
  approvalTimeoutSeconds = 600,
  schedule = null,
}) {
  validateJobId(jobId);
  const normalizedTask = kind === "delegation"
    ? delegationTaskFields(jobId, task, taskBody)
    : { task, taskBody: null };
  return {
    version: 1,
    jobId,
    from,
    target,
    targetThreadId: targetThreadId || threadId,
    threadId,
    executionThreadId: null,
    turnId: null,
    ...normalizedTask,
    permissions,
    kind,
    source,
    reply,
    execution,
    approval,
    mirror,
    approvalTimeoutSeconds,
    approvals: [],
    workerPid: null,
    mirrorDelivery: null,
    resultObservation: null,
    ...(kind === "delegation"
      ? {
          turnStartAttemptedAt: null,
          modelTurnStarted: false,
          failureStage: null,
          rerouteGuidance: null,
        }
      : {}),
    schedule,
    status: schedule ? "scheduled" : "dispatching",
    result: null,
    error: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    completedAt: null,
  };
}

function scheduledDelegationFingerprint(spec) {
  return createHash("sha256")
    .update(
      JSON.stringify({
        from: spec.from,
        target: spec.target,
        targetThreadId: spec.targetThreadId,
        task: spec.task,
        taskBody: spec.taskBody,
        permissions: spec.permissions || null,
        execution: spec.execution || "fork",
        approval: spec.approval || "never",
        mirror: spec.mirror || "none",
        approvalTimeoutSeconds: spec.approvalTimeoutSeconds || 600,
        wakePolicy: "when-idle",
        expiresAt: spec.expiresAt,
        targetNodeKey: spec.targetNodeKey,
        targetProjectId: spec.targetProjectId,
      }),
    )
    .digest("hex");
}

export async function createScheduledDelegationJob(spec) {
  const jobId = spec.jobId || newJobId();
  validateJobId(jobId);
  const targetThreadId = validateJobId(spec.targetThreadId).toLowerCase();
  const targetProjectId = validateJobId(spec.targetProjectId).toLowerCase();
  const expiresAt = new Date(spec.expiresAt || "").toISOString();
  const delay = Date.parse(expiresAt) - Date.now();
  const normalized = {
    ...spec,
    from: validateSessionName(spec.from),
    target: validateSessionName(spec.target),
    targetThreadId,
    targetProjectId,
    ...delegationTaskFields(jobId, spec.task, spec.taskBody),
    expiresAt,
  };
  if (
    normalized.targetNodeKey !== `codex:${targetThreadId}` ||
    !Number.isFinite(delay) ||
    delay <= 0 ||
    delay > 7 * 24 * 60 * 60 * 1_000
  ) {
    throw new Error("invalid scheduled Delegation identity or expiry");
  }
  const fingerprint = scheduledDelegationFingerprint(normalized);
  return withJobLock(jobId, async () => {
    const existing = readJob(jobId);
    if (existing) {
      if (
        existing.kind === "delegation" &&
        existing.schedule?.enqueueFingerprint === fingerprint
      ) {
        return { job: existing, created: false };
      }
      throw new Error(`scheduled Delegation idempotency conflict: ${jobId}`);
    }
    const now = new Date().toISOString();
    const job = buildJob({
      ...normalized,
      jobId,
      threadId: targetThreadId,
      schedule: {
        version: 1,
        wakePolicy: "when-idle",
        expiresAt,
        targetNodeKey: normalized.targetNodeKey,
        targetProjectId,
        enqueueFingerprint: fingerprint,
        claim: null,
        attemptCount: 0,
        queuedAt: now,
        dispatchAttemptedAt: null,
        lastReleaseReason: null,
      },
    });
    return { job: writeJob(job), created: true };
  });
}

export async function claimScheduledDelegation(
  jobId,
  { workerId, leaseMs, now = new Date().toISOString() },
) {
  validateJobId(workerId);
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1) {
    throw new Error("scheduled Delegation lease must be a positive integer");
  }
  return withJobLock(jobId, async () => {
    const current = readJob(jobId);
    if (!current) throw new Error(`unknown job: ${jobId}`);
    if (current.status !== "scheduled" || !current.schedule) {
      return { acquired: false, job: current };
    }
    const observedAt = Date.parse(now);
    if (!Number.isFinite(observedAt)) throw new Error("invalid claim timestamp");
    if (Date.parse(current.schedule.expiresAt) <= observedAt) {
      const expired = writeJob({
        ...current,
        status: "expired",
        failureCode: "delegation_expired",
        error: "scheduled Delegation expired before execution",
        completedAt: now,
        updatedAt: now,
      });
      return { acquired: false, expired: true, job: expired };
    }
    const activeClaim = current.schedule.claim;
    if (activeClaim && Date.parse(activeClaim.leaseUntil) > observedAt) {
      return { acquired: false, job: current };
    }
    const claim = {
      claimId: randomUUID(),
      workerId,
      claimedAt: now,
      leaseUntil: new Date(observedAt + leaseMs).toISOString(),
    };
    const claimed = writeJob({
      ...current,
      schedule: { ...current.schedule, claim },
      updatedAt: now,
    });
    return { acquired: true, claim, job: claimed };
  });
}

export async function releaseScheduledDelegationClaim(
  jobId,
  { claimId, workerId, reason, now = new Date().toISOString() },
) {
  validateJobId(claimId);
  validateJobId(workerId);
  return withJobLock(jobId, async () => {
    const current = readJob(jobId);
    if (!current) throw new Error(`unknown job: ${jobId}`);
    const claim = current.schedule?.claim;
    if (
      current.status !== "scheduled" ||
      !claim ||
      claim.claimId !== claimId ||
      claim.workerId !== workerId
    ) {
      return { released: false, job: current };
    }
    const released = writeJob({
      ...current,
      schedule: {
        ...current.schedule,
        claim: null,
        lastReleaseReason: reason,
      },
      updatedAt: now,
    });
    return { released: true, job: released };
  });
}

export async function activateScheduledDelegation(
  jobId,
  { claimId, workerId, workerPid = process.pid, now = new Date().toISOString() },
) {
  validateJobId(claimId);
  validateJobId(workerId);
  if (!Number.isSafeInteger(workerPid) || workerPid < 1) {
    throw new Error("scheduled Delegation worker pid is invalid");
  }
  return withJobLock(jobId, async () => {
    const current = readJob(jobId);
    if (!current) throw new Error(`unknown job: ${jobId}`);
    const claim = current.schedule?.claim;
    if (
      current.status !== "scheduled" ||
      !claim ||
      claim.claimId !== claimId ||
      claim.workerId !== workerId ||
      Date.parse(claim.leaseUntil) <= Date.parse(now)
    ) {
      return { activated: false, job: current };
    }
    const activated = writeJob({
      ...current,
      status: "queued",
      workerPid,
      workerStartedAt: now,
      schedule: {
        ...current.schedule,
        claim: null,
        attemptCount: current.schedule.attemptCount + 1,
        dispatchAttemptedAt: now,
      },
      updatedAt: now,
    });
    return { activated: true, job: activated };
  });
}

export function createJob(spec) {
  return withRetentionWriter(() => {
    const job = buildJob(spec);
    if (readJob(job.jobId)) throw new Error(`job already exists: ${job.jobId}`);
    return writeJob(job);
  });
}

export async function createJobOnce(spec, initialize = (job) => job) {
  const jobId = spec.jobId || newJobId();
  if (typeof initialize !== "function") {
    throw new Error("job initializer must be a function");
  }
  return withJobLock(jobId, async () => {
    const existing = readJob(jobId);
    if (existing) return { job: existing, created: false };
    const candidate = initialize(buildJob({ ...spec, jobId }));
    if (
      !candidate ||
      candidate.version !== 1 ||
      candidate.jobId !== jobId
    ) {
      throw new Error("job initializer changed immutable identity");
    }
    return { job: writeJob(candidate), created: true };
  });
}

export function isPendingJob(job) {
  return ["dispatching", "scheduled", "queued", "running", "awaiting_approval"].includes(
    job?.status,
  );
}

export async function withJobLock(jobId, callback, timeoutMs = 10_000) {
  validateJobId(jobId);
  return withRetentionWriter(() => {
    ensureJobsDirectory();
    return withFileLock(jobLockPath(jobId), callback, { timeoutMs });
  });
}

export async function mutateJob(jobId, mutate) {
  return withJobLock(jobId, async () => {
    const current = readJob(jobId);
    if (!current) throw new Error(`unknown job: ${jobId}`);
    return writeJob({
      ...mutate(current),
      updatedAt: new Date().toISOString(),
    });
  });
}

export async function updateJob(job, changes) {
  return mutateJob(job.jobId, (current) => ({
    ...current,
    ...changes,
  }));
}

export function activeJobsForTarget(target) {
  return listJobs().filter((job) => job.target === target && isPendingJob(job));
}

export async function failJobIfWorkerExited(
  job,
  {
    processStateFn = processState,
    now = Date.now(),
    registrationGraceMs = WORKER_REGISTRATION_GRACE_MS,
  } = {},
) {
  if (job?.kind !== "delegation" || !isPendingJob(job)) {
    return job;
  }
  if (job.status === "scheduled") return job;

  if (!Number.isSafeInteger(job.workerPid)) {
    const createdAt = Date.parse(job.createdAt);
    if (!Number.isFinite(createdAt) || now - createdAt < registrationGraceMs) {
      return job;
    }
    return mutateJob(job.jobId, (current) => {
      if (
        current.kind !== "delegation" ||
        !isPendingJob(current) ||
        Number.isSafeInteger(current.workerPid)
      ) {
        return current;
      }
      return {
        ...current,
        status: "failed",
        failureCode: "worker_missing",
        error: "delegation worker was not registered before the startup deadline",
        completedAt: new Date().toISOString(),
      };
    });
  }

  if (processStateFn(job.workerPid) !== "missing") return job;

  return mutateJob(job.jobId, (current) => {
    if (
      current.kind !== "delegation" ||
      !isPendingJob(current) ||
      current.workerPid !== job.workerPid
    ) {
      return current;
    }
    return {
      ...current,
      status: "failed",
      failureCode: "worker_exited",
      error: "delegation worker exited before job completion",
      completedAt: new Date().toISOString(),
    };
  });
}

export async function refreshJob(client, job) {
  if (!job.turnId) return job;
  const turn = await findThreadTurn(client, job.threadId, job.turnId, {
    itemsView: "notLoaded",
  });
  if (!turn) {
    const startedAt = Date.parse(job.turnStartedAt || job.updatedAt || job.createdAt);
    if (Number.isFinite(startedAt) && Date.now() - startedAt < 10_000) {
      return job;
    }
    return await updateJob(job, {
      status: "unknown",
      error: `turn not found: ${job.turnId}`,
      completedAt: job.completedAt || new Date().toISOString(),
    });
  }

  const status =
    turn.status === "inProgress"
      ? "running"
      : turn.status === "completed"
        ? "completed"
        : turn.status;
  const startedAt = Date.parse(job.turnStartedAt || job.updatedAt || job.createdAt);
  if (
    status !== "running" &&
    status !== "completed" &&
    Number.isFinite(startedAt) &&
    Date.now() - startedAt < 10_000
  ) {
    return job;
  }
  const terminal = status !== "running";
  if (!terminal && job.status === "running") return readJob(job.jobId) || job;
  let result = null;
  let resultObservation = null;
  if (status === "completed") {
    const observedAt = new Date().toISOString();
    try {
      const final = await findFinalTurnResult(client, job.threadId, job.turnId);
      result = final.result;
      resultObservation = final.state === "available"
        ? { status: "available", source: "thread-items", observedAt }
        : {
            status: "missing",
            source: "thread-items",
            observedAt,
            errorCode:
              final.state === "incomplete"
                ? "ERESULTWINDOW"
                : "ERESULTNOTFOUND",
          };
    } catch (error) {
      resultObservation = {
        status: "failed",
        source: "thread-items",
        observedAt,
        ...appServerFailureEvidence(error, "ERESULTOBSERVATION"),
      };
    }
  }
  return updateJob(job, {
    status,
    result,
    resultObservation,
    error: turn.error?.message || turn.error || null,
    completedAt: terminal ? job.completedAt || new Date().toISOString() : null,
  });
}
