import { createHash, randomUUID } from "node:crypto";
import {
  buildClaudePeerFrame,
  listClaudePeers,
  resolveClaudePeer,
  sendClaudePeerFrame,
} from "./claude-messaging.js";
import { createJob, mutateJob, readJob, updateJob } from "./jobs.js";

const ACK_PATTERN =
  /^<cxmsg-ack in-reply-to="([0-9a-f-]{36})" status="(accepted|completed|retryable_error|failed)"(?: code="([^"]{1,32})")?(?: retry-after="(\d{1,5})")?>\n([\s\S]*)\n<\/cxmsg-ack>$/i;

function deliveryBody(job) {
  return (
    `<cxmsg-delivery id="${job.jobId}">\n` +
    `${job.task}\n` +
    "</cxmsg-delivery>\n\n" +
    "After processing this message, reply to its from address with exactly one correlated status envelope:\n" +
    `<cxmsg-ack in-reply-to="${job.jobId}" status="completed">\n` +
    "brief result\n" +
    "</cxmsg-ack>\n" +
    "Use status=retryable_error with code=429 or code=529 and retry-after=<seconds> for a transient API overload. " +
    "Use the delivery id to avoid performing the same task twice after a retry."
  );
}

export function parseClaudeDeliveryAck(body) {
  if (typeof body !== "string") return null;
  const match = body.match(ACK_PATTERN);
  if (!match) return null;
  return {
    jobId: match[1],
    status: match[2].toLowerCase(),
    code: match[3] || null,
    retryAfterSeconds: match[4] ? Number(match[4]) : null,
    detail: match[5],
  };
}

function identityEvidence(value) {
  if (!value) return "missing";
  const digest = createHash("sha256").update(value).digest("hex").slice(0, 10);
  return `present:${digest}`;
}

function ackSourceEvidence(job, parsed) {
  return {
    expectedSession: identityEvidence(job.claudeTarget?.sessionId),
    actualSession: identityEvidence(parsed.fromSession),
    expectedAddress: identityEvidence(job.claudeTarget?.address),
    actualAddress: identityEvidence(parsed.fromAddress),
  };
}

function ackSourceMatches(job, parsed) {
  const expectedSession = job.claudeTarget?.sessionId;
  const expectedAddress = job.claudeTarget?.address;
  const addressMatches = Boolean(
    expectedAddress && parsed.fromAddress === expectedAddress,
  );
  if (!parsed.fromSession) return addressMatches;
  return Boolean(
    expectedSession &&
      parsed.fromSession === expectedSession &&
      addressMatches,
  );
}

export function createClaudeDeliveryJob({
  from,
  sourceRecord,
  peer,
  message,
  maxAttempts = 4,
  ackTimeoutSeconds = 120,
}) {
  const created = createJob({
    from,
    target: peer.name,
    targetThreadId: sourceRecord.threadId,
    threadId: null,
    task: message,
    kind: "claude-delivery",
  });
  return updateJob(created, {
    status: "queued",
    claudeTarget: {
      name: peer.name,
      sessionId: peer.sessionId,
      address: peer.address,
    },
    delivery: {
      attempt: 0,
      maxAttempts,
      messageIds: [],
      transportStatus: "pending",
      attemptedAt: null,
      deliveredAt: null,
      ackDeadlineAt: null,
      nextAttemptAt: null,
      errorCode: null,
    },
    ack: null,
    ackTimeoutSeconds,
  });
}

function resolveStoredTarget(peers, job) {
  const candidates = [
    job.claudeTarget?.sessionId,
    job.claudeTarget?.address,
    job.claudeTarget?.name,
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      const peer = resolveClaudePeer(peers, candidate);
      if (peer.status !== "unreachable") return peer;
    } catch {}
  }
  throw new Error(`Claude target is not reachable: ${job.claudeTarget?.name || job.target}`);
}

export async function sendClaudeDeliveryJob(
  bridgeRecord,
  sourceRecord,
  job,
  { peers = listClaudePeers, send = sendClaudePeerFrame } = {},
) {
  const current = readJob(job.jobId) || job;
  if ((current.delivery?.attempt || 0) >= (current.delivery?.maxAttempts || 4)) {
    return updateJob(current, {
      status: "failed",
      error: "Claude delivery exhausted its retry budget",
      completedAt: new Date().toISOString(),
    });
  }
  const peer = resolveStoredTarget(await peers(), current);
  const messageId = randomUUID();
  const attemptedAt = new Date().toISOString();
  const attempt = (current.delivery?.attempt || 0) + 1;
  const frame = buildClaudePeerFrame({
    fromSocket: bridgeRecord.socketPath,
    fromName: `codex-${current.from}`,
    fromSession: sourceRecord.threadId,
    message: deliveryBody(current),
    messageId,
  });
  try {
    await send(peer.socketPath, frame);
    const ackDeadlineAt = new Date(
      Date.now() + (current.ackTimeoutSeconds || 120) * 1_000,
    ).toISOString();
    return updateJob(current, {
      status: "transport_delivered",
      error: null,
      delivery: {
        ...current.delivery,
        attempt,
        messageIds: [...(current.delivery?.messageIds || []), messageId],
        transportStatus: "delivered",
        attemptedAt,
        deliveredAt: new Date().toISOString(),
        ackDeadlineAt,
        nextAttemptAt: null,
        errorCode: null,
      },
    });
  } catch (error) {
    return updateJob(current, {
      status: error?.code === "EPERM" ? "unreachable" : "transport_error",
      error: error.message,
      delivery: {
        ...current.delivery,
        attempt,
        messageIds: [...(current.delivery?.messageIds || []), messageId],
        transportStatus: "failed",
        attemptedAt,
        nextAttemptAt: null,
        errorCode: error?.code || null,
      },
    });
  }
}

export async function recordClaudeDeliveryAck(parsed, ack) {
  const job = readJob(ack.jobId);
  if (!job || job.kind !== "claude-delivery") return null;
  const now = new Date().toISOString();
  let ackTransitioned = false;
  let rejection = null;
  const updated = await mutateJob(job.jobId, (current) => {
    if (!ackSourceMatches(current, parsed)) {
      const source = ackSourceEvidence(current, parsed);
      rejection = Object.assign(
        new Error(
          `Claude delivery ACK source mismatch: ${ack.jobId} ` +
            `expected-session=${source.expectedSession} actual-session=${source.actualSession} ` +
            `expected-address=${source.expectedAddress} actual-address=${source.actualAddress}`,
        ),
        { code: "EACKSOURCE" },
      );
      return {
        ...current,
        status: "ack_rejected",
        error: "Claude delivery ACK source mismatch",
        completedAt: now,
        ack: {
          status: "rejected",
          code: "source_mismatch",
          detail: null,
          receivedAt: now,
          source,
        },
        delivery: {
          ...current.delivery,
          errorCode: "source_mismatch",
        },
      };
    }
    if (["completed", "failed"].includes(current.status)) {
      if (current.ack?.status === ack.status) return current;
      rejection = new Error(
        `Claude delivery is already ${current.status}: ${ack.jobId}`,
      );
      return current;
    }
    ackTransitioned = true;
    const retryable = ack.status === "retryable_error";
    const exhausted =
      (current.delivery?.attempt || 0) >= (current.delivery?.maxAttempts || 4);
    const retryAfterSeconds = Math.min(
      900,
      Math.max(1, ack.retryAfterSeconds || 30 * 2 ** Math.max(0, (current.delivery?.attempt || 1) - 1)),
    );
    const nextAttemptAt =
      retryable && !exhausted
        ? new Date(Date.now() + retryAfterSeconds * 1_000).toISOString()
        : null;
    return {
      ...current,
      status:
        retryable && !exhausted
          ? "retry_scheduled"
          : retryable && exhausted
            ? "failed"
          : ack.status === "accepted"
            ? "acknowledged"
            : ack.status,
      result: ack.status === "completed" ? ack.detail : current.result,
      error: retryable || ack.status === "failed" ? ack.detail : null,
      completedAt:
        ack.status === "completed" || ack.status === "failed" || exhausted
          ? now
          : null,
      ack: {
        status: ack.status,
        code: ack.code,
        detail: ack.detail,
        receivedAt: now,
        fromSession: parsed.fromSession,
      },
      delivery: {
        ...current.delivery,
        nextAttemptAt,
        errorCode: ack.code,
      },
      wake:
        ack.status === "completed" || ack.status === "failed" || exhausted
          ? {
              status: "pending",
              clientUserMessageId: ack.jobId,
              attemptedAt: null,
              deliveredAt: null,
              error: null,
            }
          : current.wake || null,
    };
  });
  if (rejection) throw rejection;
  return { ...updated, ackTransitioned };
}

export function refreshClaudeDelivery(job) {
  if (
    job?.kind === "claude-delivery" &&
    job.status === "transport_delivered" &&
    Date.parse(job.delivery?.ackDeadlineAt) <= Date.now()
  ) {
    return updateJob(job, {
      status: "ack_timeout",
      error: "Claude did not acknowledge the delivery before its deadline",
    });
  }
  return job;
}
