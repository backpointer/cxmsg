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
import { CXMSG_STATE_DIR } from "./runtime.js";

const JOBS_DIR = path.join(CXMSG_STATE_DIR, "jobs");

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
  validateJobId(job.jobId);
  ensureJobsDirectory();
  const destination = jobPath(job.jobId);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(job, null, 2)}\n`, {
    mode: 0o600,
  });
  renameSync(temporary, destination);
  return job;
}

export function createJob({
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
  if (readJob(jobId)) throw new Error(`job already exists: ${jobId}`);
  return writeJob({
    version: 1,
    jobId,
    from,
    target,
    targetThreadId: targetThreadId || threadId,
    threadId,
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
  { processStateFn = processState } = {},
) {
  if (
    job?.kind !== "delegation" ||
    !isPendingJob(job) ||
    !Number.isSafeInteger(job.workerPid) ||
    processStateFn(job.workerPid) !== "missing"
  ) {
    return job;
  }
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
  const read = await client.request("thread/read", {
    threadId: job.threadId,
    includeTurns: true,
  });
  const turn = read.thread.turns.find((candidate) => candidate.id === job.turnId);
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
