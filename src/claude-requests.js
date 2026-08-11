import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { withAppServer } from "./app-server-client.js";
import {
  buildClaudePeerFrame,
  buildClaudeResponseBody,
  sendClaudePeerFrame,
} from "./claude-messaging.js";
import {
  createJob,
  isPendingJob,
  readJob,
  refreshJob,
  updateJob,
} from "./jobs.js";
import { activeTurnId, deliverDelegatedTask } from "./messaging.js";

export const CLAUDE_REQUEST_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_REPLY_TEXT_BYTES = 24 * 1024;

function senderName(parsed) {
  const slug = (parsed.fromName || "peer")
    .replace(/[^A-Za-z0-9._-]+/g, "-")
    .replace(/^[._-]+|[._-]+$/g, "") || "peer";
  return `claude-${slug}`.slice(0, 64);
}

function boundedReplyText(value) {
  const text = value || "Codex completed without a final response.";
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length <= MAX_REPLY_TEXT_BYTES) return text;
  return `${bytes.subarray(0, MAX_REPLY_TEXT_BYTES).toString("utf8")}\n\n[truncated by cxmsg]`;
}

function terminalFailure(job, message) {
  return updateJob(job, {
    status: "failed",
    error: message,
    completedAt: job.completedAt || new Date().toISOString(),
  });
}

async function validatePermissionProfile(client, record, permissions) {
  const result = await client.request("permissionProfile/list", {
    cwd: record.cwd,
  });
  const profile = (result.data || []).find(
    (candidate) => candidate.id === permissions,
  );
  if (!profile) throw new Error(`unknown permission profile: ${permissions}`);
  if (!profile.allowed) {
    throw new Error(`permission profile is blocked: ${permissions}`);
  }
}

async function waitForSourceThread(client, job, deadline) {
  while (true) {
    const read = await client.request("thread/read", {
      threadId: job.targetThreadId,
      includeTurns: true,
    });
    if (!activeTurnId(read.thread)) return read.thread;
    if (Date.now() >= deadline) {
      throw new Error("timed out waiting for the target Codex session to become idle");
    }
    await delay(250);
  }
}

async function forkExecutionThread(client, sourceThread, record, permissions) {
  const forkParams = {
    threadId: sourceThread.id,
    approvalPolicy: "never",
    deferGoalContinuation: true,
    permissions,
  };
  try {
    const forked = await client.request("thread/fork", forkParams);
    return forked.thread;
  } catch (error) {
    if (!/no rollout found|thread not loaded/i.test(error.message)) throw error;
    const started = await client.request("thread/start", {
      cwd: record.cwd,
      serviceName: "cxmsg-claude-request",
      approvalPolicy: "never",
      permissions,
    });
    return started.thread;
  }
}

async function startClaudeRequest(client, job, targetRecord, deadline) {
  await validatePermissionProfile(client, targetRecord, job.permissions);
  const sourceThread = await waitForSourceThread(client, job, deadline);
  const executionThread = await forkExecutionThread(
    client,
    sourceThread,
    targetRecord,
    job.permissions,
  );
  const delivery = await deliverDelegatedTask(client, executionThread, {
    from: job.from,
    target: job.target,
    task: job.task,
    jobId: job.jobId,
    permissions: job.permissions,
  });
  return updateJob(job, {
    status: "running",
    threadId: delivery.threadId,
    turnId: delivery.turnId,
    turnStartedAt: new Date().toISOString(),
  });
}

async function waitForCompletion(client, job, deadline) {
  let current = job;
  while (current.status === "running") {
    current = await refreshJob(client, current);
    if (current.status !== "running") return current;
    if (Date.now() >= deadline) {
      return terminalFailure(current, "timed out waiting for the Codex result");
    }
    await delay(250);
  }
  return current;
}

export function createClaudeRequestJob({ target, targetRecord, parsed, grant, task }) {
  const existing = readJob(parsed.messageId);
  if (existing) {
    if (existing.kind !== "claude-request") {
      throw new Error(`request id collides with an existing job: ${parsed.messageId}`);
    }
    return existing;
  }
  const created = createJob({
    jobId: parsed.messageId,
    from: senderName(parsed),
    target,
    targetThreadId: targetRecord.threadId,
    threadId: null,
    task,
    permissions: grant.permissions,
    kind: "claude-request",
    source: {
      sessionId: parsed.fromSession,
      name: parsed.fromName,
      address: parsed.fromAddress,
    },
    reply: {
      status: "pending",
      messageId: null,
      error: null,
      attemptedAt: null,
    },
  });
  return updateJob(created, { status: "queued" });
}

export async function replyToClaudeRequest(bridgeRecord, targetRecord, job) {
  if (job.reply?.status === "delivered") return job;
  const responseStatus = job.status === "completed" ? "completed" : "failed";
  const response = buildClaudeResponseBody({
    requestId: job.jobId,
    status: responseStatus,
    result: responseStatus === "completed" ? boundedReplyText(job.result) : null,
    error:
      responseStatus === "failed"
        ? boundedReplyText(job.error || `Codex request ended with status ${job.status}`)
        : null,
  });
  const frame = buildClaudePeerFrame({
    fromSocket: bridgeRecord.socketPath,
    fromName: `codex-${job.target}`,
    fromSession: targetRecord.threadId,
    message: response,
    messageId: randomUUID(),
  });
  try {
    const socketPath = job.source?.address?.startsWith("uds:")
      ? job.source.address.slice(4)
      : "";
    const delivery = await sendClaudePeerFrame(socketPath, frame);
    return updateJob(job, {
      reply: {
        status: "delivered",
        messageId: delivery.messageId,
        error: null,
        attemptedAt: new Date().toISOString(),
      },
    });
  } catch (error) {
    return updateJob(job, {
      reply: {
        status: "failed",
        messageId: frame.msg_id,
        error: error.message,
        attemptedAt: new Date().toISOString(),
      },
    });
  }
}

export async function processClaudeRequest({
  bridgeRecord,
  targetRecord,
  job,
  timeoutMs = CLAUDE_REQUEST_TIMEOUT_MS,
  connect = withAppServer,
}) {
  if (job.reply?.status === "delivered") return job;
  let current = job;
  const deadline = Date.now() + timeoutMs;
  try {
    current = await connect(async (client) => {
      let active = current;
      if (!active.turnId && isPendingJob(active)) {
        active = await startClaudeRequest(client, active, targetRecord, deadline);
      }
      return waitForCompletion(client, active, deadline);
    });
  } catch (error) {
    current = readJob(job.jobId) || current;
    if (isPendingJob(current)) current = terminalFailure(current, error.message);
  }
  if (isPendingJob(current)) {
    current = terminalFailure(current, "Claude request stopped before completion");
  }
  return replyToClaudeRequest(bridgeRecord, targetRecord, current);
}
