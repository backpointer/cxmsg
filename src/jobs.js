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
import { finalTurnResult } from "./messaging.js";
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
  targetThreadId = threadId,
  threadId,
  task,
  permissions = null,
  kind = "delegation",
  source = null,
  reply = null,
}) {
  if (readJob(jobId)) throw new Error(`job already exists: ${jobId}`);
  return writeJob({
    version: 1,
    jobId,
    from,
    target,
    targetThreadId,
    threadId,
    turnId: null,
    task,
    permissions,
    kind,
    source,
    reply,
    status: "dispatching",
    result: null,
    error: null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    completedAt: null,
  });
}

export function isPendingJob(job) {
  return ["dispatching", "queued", "running"].includes(job?.status);
}

export function updateJob(job, changes) {
  return writeJob({
    ...job,
    ...changes,
    updatedAt: new Date().toISOString(),
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
    return updateJob(job, {
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
  const terminal = status !== "running";
  if (!terminal && job.status === "running") return job;
  return updateJob(job, {
    status,
    result: terminal ? finalTurnResult(turn) : null,
    error: turn.error?.message || turn.error || null,
    completedAt: terminal ? job.completedAt || new Date().toISOString() : null,
  });
}
