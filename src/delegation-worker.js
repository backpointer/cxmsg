import { setTimeout as delay } from "node:timers/promises";
import { createApprovalHandler } from "./approvals.js";
import {
  AppServerClient,
  appServerFailureEvidence,
} from "./app-server-client.js";
import {
  APPROVAL_MODES,
  EXECUTION_MODES,
  MIRROR_MODES,
  validateDelegationAuthority,
} from "./delegation-authority.js";
import {
  activateScheduledDelegation,
  isPendingJob,
  mutateJob,
  readJob,
  refreshJob,
  updateJob,
} from "./jobs.js";
import {
  deliverDelegatedTask,
  deliverPeerMessage,
  truncateUtf8,
} from "./messaging.js";
import { classifyExecutionThread } from "./node-directory.js";
import {
  readSessionRecord,
  sessionAllowsAppServerResume,
} from "./registry.js";
import { readThreadMetadata } from "./thread-activity.js";

export { APPROVAL_MODES, EXECUTION_MODES, MIRROR_MODES };

async function executionThread(client, record, job) {
  if (job.execution === "inline") {
    const sourceThread = await readThreadMetadata(client, record.threadId);
    if (sourceThread.status?.type === "active") {
      throw new Error("target session already has an active turn");
    }
    return { thread: sourceThread, creationMode: null };
  }
  const forkParams = {
    threadId: record.threadId,
    approvalPolicy: job.approval === "never" ? "never" : "on-request",
    deferGoalContinuation: true,
    includeTurns: false,
  };
  if (job.permissions) forkParams.permissions = job.permissions;
  try {
    const forked = await client.request("thread/fork", forkParams);
    return { thread: forked.thread, creationMode: "fork" };
  } catch (error) {
    if (!/no rollout found|thread not loaded/i.test(error.message)) throw error;
    const startParams = {
      cwd: record.cwd,
      serviceName: "cxmsg-delegate",
      approvalPolicy: job.approval === "never" ? "never" : "on-request",
    };
    if (job.permissions) startParams.permissions = job.permissions;
    const started = await client.request("thread/start", startParams);
    return { thread: started.thread, creationMode: "start-fallback" };
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
    ? `${truncateUtf8(result, 2_048)}\n[truncated]`
    : result;
  return `${header}\n\nResult summary:\n${bounded}`;
}

async function mirrorResult(client, job) {
  if (job.execution !== "fork" || job.mirror === "none") return job;
  try {
    const targetRecord = readSessionRecord(job.target);
    if (!targetRecord) throw new Error(`unknown Codex session: ${job.target}`);
    const thread = await readThreadMetadata(client, targetRecord.threadId);
    if (thread.status?.type === "active") {
      throw new Error("target session is active; refusing to steer a mirror into unrelated work");
    }
    const delivery = await deliverPeerMessage(client, thread, {
      from: job.from,
      message: mirrorMessage(job),
    }, {
      allowResume: sessionAllowsAppServerResume(targetRecord),
    });
    return await updateJob(job, {
      mirrorDelivery: {
        status: "delivered",
        turnId: delivery.turnId,
        deliveredAt: new Date().toISOString(),
        errorCode: null,
        error: null,
      },
    });
  } catch (error) {
    const failureEvidence = appServerFailureEvidence(error, "EPEERDELIVERY");
    return await updateJob(job, {
      mirrorDelivery: {
        status: "failed",
        turnId: null,
        deliveredAt: new Date().toISOString(),
        errorCode: failureEvidence.errorCode,
        error: error.message,
      },
    });
  }
}

export async function runDelegationWorker(
  jobId,
  { Client = AppServerClient, scheduleClaim = null } = {},
) {
  let job = readJob(jobId);
  if (!job) throw new Error(`unknown job: ${jobId}`);
  if (scheduleClaim) {
    const activated = await activateScheduledDelegation(jobId, {
      claimId: scheduleClaim.claimId,
      workerId: scheduleClaim.workerId,
      workerPid: process.pid,
    });
    if (!activated.activated) {
      const error = new Error("scheduled Delegation claim is no longer active");
      error.code = "ECLAIMLOST";
      throw error;
    }
    job = activated.job;
  } else {
    if (job.schedule) {
      const error = new Error("scheduled Delegation requires an active Scheduler claim");
      error.code = "ECLAIMREQUIRED";
      throw error;
    }
    job = await mutateJob(jobId, (current) => ({
      ...current,
      status: "queued",
      workerPid: process.pid,
      workerStartedAt: new Date().toISOString(),
    }));
  }

  const client = new Client({ onServerRequest: createApprovalHandler(jobId) });
  try {
    await client.connect();
    const { record } = await validateDelegationAuthority(job, client);
    const execution = await executionThread(client, record, job);
    if (execution.creationMode) {
      await classifyExecutionThread({
        threadId: execution.thread.id,
        jobId: job.jobId,
        sourceThreadId: record.threadId,
        creationMode: execution.creationMode,
      });
      job = await mutateJob(jobId, (current) => ({
        ...current,
        threadId: execution.thread.id,
        executionThreadId: execution.thread.id,
      }));
    }
    const delivery = await deliverDelegatedTask(client, execution.thread, {
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
      executionThreadId:
        current.execution === "fork" ? delivery.threadId : null,
      turnId: delivery.turnId,
      turnStartedAt: new Date().toISOString(),
    }));
    job = await waitForTerminal(client, jobId);
    if (job) await mirrorResult(client, job);
  } catch (error) {
    job = readJob(jobId) || job;
    if (isPendingJob(job)) {
      const failureEvidence = appServerFailureEvidence(
        error,
        "EDELEGATIONWORKER",
      );
      await updateJob(job, {
        status: "failed",
        failureCode: failureEvidence.errorCode,
        failureEvidence,
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
