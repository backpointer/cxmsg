import { createHash } from "node:crypto";
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
  JOB_OBSERVATION_NOTIFICATION_OPT_OUT,
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
import { readWholeMessageBody } from "./message-bodies.js";
import {
  readSessionRecord,
  sessionAllowsAppServerResume,
} from "./registry.js";
import { readThreadMetadata } from "./thread-activity.js";

export { APPROVAL_MODES, EXECUTION_MODES, MIRROR_MODES };

function resolvedDelegationTask(job) {
  if (!job.taskBody) return { task: job.task, taskBody: null };
  let task;
  try {
    task = readWholeMessageBody(job.taskBody.contentRef);
  } catch {
    const error = new Error("stored Delegation task body is unavailable");
    error.code = "EDELEGATIONTASKBODY";
    throw error;
  }
  const bodyBytes = Buffer.byteLength(task, "utf8");
  const bodySha256 = createHash("sha256").update(task).digest("hex");
  if (
    job.task !== null ||
    job.taskBody.messageId !== job.jobId ||
    job.taskBody.contentRef !== `cxmsg-message:${job.jobId}` ||
    job.taskBody.bodyBytes !== bodyBytes ||
    job.taskBody.bodySha256 !== bodySha256
  ) {
    const error = new Error("stored Delegation task reference does not match its body");
    error.code = "EDELEGATIONTASKBODY";
    throw error;
  }
  return { task, taskBody: job.taskBody };
}

function delegationFailureGuidance({ errorCode, failureStage, modelTurnStarted }) {
  if (modelTurnStarted === null) {
    return "Turn acceptance is unverified; do not retry or reroute automatically.";
  }
  if (errorCode === "EAPPWSNOTCONNECTED") {
    return "No model turn started; verify cxmsg server connectivity before using a new Job ID.";
  }
  if (
    errorCode === "EAPPWSFRAME" &&
    failureStage === "execution-thread" &&
    modelTurnStarted === false
  ) {
    return "The source thread could not be forked within the frame bound; retry only by explicit operator choice with --execution fresh and a new Job ID.";
  }
  return null;
}

async function executionThread(client, record, job) {
  if (job.execution === "inline") {
    const sourceThread = await readThreadMetadata(client, record.threadId);
    if (sourceThread.status?.type === "active") {
      throw new Error("target session already has an active turn");
    }
    return { thread: sourceThread, creationMode: null };
  }
  if (job.execution === "fresh") {
    const startParams = {
      cwd: record.cwd,
      serviceName: "cxmsg-delegate-fresh",
      approvalPolicy: job.approval === "never" ? "never" : "on-request",
    };
    if (job.permissions) startParams.permissions = job.permissions;
    const started = await client.request("thread/start", startParams);
    return { thread: started.thread, creationMode: "explicit-fresh" };
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
    "This is a synchronization record from an isolated execution thread.";
  if (job.mirror === "full") {
    const task = job.taskBody
      ? `Stored task: ${job.taskBody.contentRef} (${job.taskBody.bodyBytes} bytes)`
      : `Task:\n${job.task}`;
    return `${header}\n\n${task}\n\nResult:\n${job.result || job.error || "No result."}`;
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
  let failureStage = "worker-initialization";
  let turnStartAttempted = false;
  let modelTurnStarted = false;
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

  const client = new Client({
    onServerRequest: createApprovalHandler(jobId),
    optOutNotificationMethods: JOB_OBSERVATION_NOTIFICATION_OPT_OUT,
  });
  try {
    failureStage = "transport-connect";
    await client.connect();
    failureStage = "authority-validation";
    const { record } = await validateDelegationAuthority(job, client);
    failureStage = "execution-thread";
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
    failureStage = "task-body";
    const delegatedTask = resolvedDelegationTask(job);
    failureStage = "turn-preflight";
    const delivery = await deliverDelegatedTask(client, execution.thread, {
      from: job.from,
      target: job.target,
      ...delegatedTask,
      jobId: job.jobId,
      permissions: job.permissions,
      approvalPolicy: job.approval === "never" ? "never" : "on-request",
    }, {
      beforeStart: async () => {
        failureStage = "turn-start-evidence";
        job = await mutateJob(jobId, (current) => ({
          ...current,
          turnStartAttemptedAt:
            current.turnStartAttemptedAt || new Date().toISOString(),
          modelTurnStarted: null,
        }));
        turnStartAttempted = true;
        failureStage = "turn-start";
      },
    });
    modelTurnStarted = true;
    job = await mutateJob(jobId, (current) => ({
      ...current,
      status: "running",
      threadId: delivery.threadId,
      executionThreadId:
        current.execution === "inline" ? null : delivery.threadId,
      turnId: delivery.turnId,
      turnStartedAt: new Date().toISOString(),
      modelTurnStarted: true,
      failureStage: null,
    }));
    failureStage = "turn-observation";
    job = await waitForTerminal(client, jobId);
    if (job) await mirrorResult(client, job);
  } catch (error) {
    job = readJob(jobId) || job;
    if (isPendingJob(job)) {
      const failureEvidence = appServerFailureEvidence(
        error,
        "EDELEGATIONWORKER",
      );
      const failedBeforeModelTurn =
        !turnStartAttempted ||
        failureEvidence.errorCode === "EAPPWSNOTCONNECTED" ||
        failureEvidence.errorCode === "EAPPWSOUTBOUND";
      const modelTurnEvidence = modelTurnStarted
        ? true
        : failedBeforeModelTurn
          ? false
          : turnStartAttempted || job.turnStartAttemptedAt
            ? null
            : false;
      await updateJob(job, {
        status: "failed",
        failureCode: failureEvidence.errorCode,
        failureEvidence,
        failureStage,
        modelTurnStarted: modelTurnEvidence,
        rerouteGuidance: delegationFailureGuidance({
          errorCode: failureEvidence.errorCode,
          failureStage,
          modelTurnStarted: modelTurnEvidence,
        }),
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
