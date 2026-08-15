import { randomUUID } from "node:crypto";
import {
  chmodSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import { finalTurnResult } from "./messaging.js";
import { processState } from "./process-state.js";
import { withRetentionWriter } from "./retention-barrier.js";
import { CXMSG_STATE_DIR } from "./runtime.js";
import { findThreadTurn } from "./thread-activity.js";

const JOBS_DIR = path.join(CXMSG_STATE_DIR, "jobs");
const WORKER_REGISTRATION_GRACE_MS = 10_000;

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

export function newJobId() {
  return randomUUID();
}

export function readJob(jobId) {
  try {
    const job = JSON.parse(readFileSync(jobPath(jobId), "utf8"));
    if (job?.version !== 1 || job.jobId !== jobId) return null;
    return job;
  } catch {
    return null;
  }
}

export function listJobs() {
  ensureJobsDirectory();
  return readdirSync(JOBS_DIR)
    .filter((filename) => /^[0-9a-f-]+\.json$/i.test(filename))
    .map((filename) => readJob(filename.slice(0, -5)))
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
  permissions = null,
  kind = "delegation",
  source = null,
  reply = null,
  execution = "fork",
  approval = "never",
  mirror = "none",
  approvalTimeoutSeconds = 600,
}) {
  validateJobId(jobId);
  return {
    version: 1,
    jobId,
    from,
    target,
    targetThreadId: targetThreadId || threadId,
    threadId,
    executionThreadId: null,
    turnId: null,
    task,
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
    status: "dispatching",
    result: null,
    error: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    completedAt: null,
  };
}

export function createJob(spec) {
  const job = buildJob(spec);
  if (readJob(job.jobId)) throw new Error(`job already exists: ${job.jobId}`);
  return writeJob(job);
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
  return ["dispatching", "queued", "running", "awaiting_approval"].includes(
    job?.status,
  );
}

export async function withJobLock(jobId, callback, timeoutMs = 10_000) {
  validateJobId(jobId);
  ensureJobsDirectory();
  return withFileLock(jobLockPath(jobId), callback, { timeoutMs });
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
  const turn = await findThreadTurn(client, job.threadId, job.turnId);
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
  return updateJob(job, {
    status,
    result: terminal ? finalTurnResult(turn) : null,
    error: turn.error?.message || turn.error || null,
    completedAt: terminal ? job.completedAt || new Date().toISOString() : null,
  });
}
