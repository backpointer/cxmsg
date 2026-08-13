import { setTimeout as delay } from "node:timers/promises";
import { createApprovalHandler } from "./approvals.js";
import { AppServerClient } from "./app-server-client.js";
import {
  isPendingJob,
  mutateJob,
  readJob,
  refreshJob,
  updateJob,
} from "./jobs.js";
import { deliverDelegatedTask, deliverPeerMessage } from "./messaging.js";
import { readSessionRecord } from "./registry.js";

export const EXECUTION_MODES = new Set(["fork", "inline"]);
export const APPROVAL_MODES = new Set(["never", "relay", "auto"]);
export const MIRROR_MODES = new Set(["none", "summary", "full"]);

async function availablePermissionProfiles(client, cwd) {
  const result = await client.request("permissionProfile/list", { cwd });
  return result.data || [];
}

async function validatePermissionProfile(client, record, permissions) {
  if (!permissions) return;
  const profiles = await availablePermissionProfiles(client, record.cwd);
  const selected = profiles.find((profile) => profile.id === permissions);
  if (!selected) throw new Error(`unknown permission profile: ${permissions}`);
  if (!selected.allowed) throw new Error(`permission profile is blocked: ${permissions}`);
}

async function executionThread(client, sourceThread, record, job) {
  if (job.execution === "inline") return sourceThread;
  const forkParams = {
    threadId: sourceThread.id,
    approvalPolicy: job.approval === "never" ? "never" : "on-request",
    deferGoalContinuation: true,
  };
  if (job.permissions) forkParams.permissions = job.permissions;
  try {
    const forked = await client.request("thread/fork", forkParams);
    return forked.thread;
  } catch (error) {
    if (!/no rollout found|thread not loaded/i.test(error.message)) throw error;
    const startParams = {
      cwd: record.cwd,
      serviceName: "cxmsg-delegate",
      approvalPolicy: job.approval === "never" ? "never" : "on-request",
    };
    if (job.permissions) startParams.permissions = job.permissions;
    const started = await client.request("thread/start", startParams);
    return started.thread;
  }
}

async function waitForTerminal(client, jobId) {
  while (true) {
    let current = readJob(jobId);
    if (!current || !isPendingJob(current)) return current;
    if (current.status === "running") {
      current = await refreshJob(client, current);
      if (!isPendingJob(current)) return current;
    }
    await delay(250);
  }
}

function mirrorMessage(job) {
  const header =
    `Delegated cxmsg job ${job.jobId} from ${job.from} finished with status ${job.status}. ` +
    "This is a synchronization record from an isolated execution fork.";
  if (job.mirror === "full") {
    return `${header}\n\nTask:\n${job.task}\n\nResult:\n${job.result || job.error || "No result."}`;
  }
  const result = job.result || job.error || "No result.";
  const bounded = Buffer.byteLength(result, "utf8") > 2_048
    ? `${Buffer.from(result, "utf8").subarray(0, 2_048).toString("utf8")}\n[truncated]`
    : result;
  return `${header}\n\nResult summary:\n${bounded}`;
}

async function mirrorResult(client, job) {
  if (job.execution !== "fork" || job.mirror === "none") return job;
  try {
    const targetRecord = readSessionRecord(job.target);
    if (!targetRecord) throw new Error(`unknown Codex session: ${job.target}`);
    const read = await client.request("thread/read", {
      threadId: targetRecord.threadId,
      includeTurns: true,
    });
    if (read.thread.status?.type === "active") {
      throw new Error("target session is active; refusing to steer a mirror into unrelated work");
    }
    const delivery = await deliverPeerMessage(client, read.thread, {
      from: job.from,
      message: mirrorMessage(job),
    });
    return updateJob(job, {
      mirrorDelivery: {
        status: "delivered",
        turnId: delivery.turnId,
        deliveredAt: new Date().toISOString(),
        error: null,
      },
    });
  } catch (error) {
    return updateJob(job, {
      mirrorDelivery: {
        status: "failed",
        turnId: null,
        deliveredAt: new Date().toISOString(),
        error: error.message,
      },
    });
  }
}

export async function runDelegationWorker(jobId, { Client = AppServerClient } = {}) {
  let job = readJob(jobId);
  if (!job) throw new Error(`unknown job: ${jobId}`);
  const record = readSessionRecord(job.target);
  if (!record) throw new Error(`unknown Codex session: ${job.target}`);

  job = await mutateJob(jobId, (current) => ({
    ...current,
    status: "queued",
    workerPid: process.pid,
    workerStartedAt: new Date().toISOString(),
  }));

  const client = new Client({ onServerRequest: createApprovalHandler(jobId) });
  try {
    await client.connect();
    await validatePermissionProfile(client, record, job.permissions);
    const read = await client.request("thread/read", {
      threadId: record.threadId,
      includeTurns: true,
    });
    if (read.thread.status?.type === "active") {
      throw new Error("target session already has an active turn");
    }
    const targetThread = await executionThread(client, read.thread, record, job);
    const delivery = await deliverDelegatedTask(client, targetThread, {
      from: job.from,
      target: job.target,
      task: job.task,
      jobId: job.jobId,
      permissions: job.permissions,
      approvalPolicy: job.approval === "never" ? "never" : "on-request",
    });
    job = await mutateJob(jobId, (current) => ({
      ...current,
      status: "running",
      threadId: delivery.threadId,
      turnId: delivery.turnId,
      turnStartedAt: new Date().toISOString(),
    }));
    job = await waitForTerminal(client, jobId);
    if (job) await mirrorResult(client, job);
  } catch (error) {
    job = readJob(jobId) || job;
    if (isPendingJob(job)) {
      updateJob(job, {
        status: "failed",
        error: error.message,
        completedAt: new Date().toISOString(),
      });
    }
    throw error;
  } finally {
    await client.close();
  }
  return readJob(jobId);
}
