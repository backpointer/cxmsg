import { createHash, randomUUID } from "node:crypto";
import {
  buildClaudePeerFrame,
  listClaudePeers,
  resolveClaudePeer,
  sendClaudePeerFrame,
} from "./claude-messaging.js";
import {
  createJob,
  createJobOnce,
  listJobs,
  mutateJob,
  readJob,
  updateJob,
} from "./jobs.js";
import { writeCoordinationEvent } from "./observability.js";
import {
  recordDirectMessageIfKnown,
  refreshDirectConversationSummary,
} from "./conversations.js";

const ACK_PATTERN =
  /^<cxmsg-ack in-reply-to="([0-9a-f-]{36})" status="(accepted|completed|retryable_error|failed)"(?: code="([^"]{1,32})")?(?: retry-after="(\d{1,5})")?>\n([\s\S]*)\n<\/cxmsg-ack>$/i;
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
export const DEFAULT_CLAUDE_ACK_TIMEOUT_SECONDS = 120;
export const DEFAULT_CLAUDE_COMPLETION_TIMEOUT_SECONDS = 15 * 60;
const MAX_CLAUDE_TIMEOUT_SECONDS = 24 * 60 * 60;

function boundedTimeoutSeconds(label, value) {
  if (
    !Number.isSafeInteger(value) ||
    value < 1 ||
    value > MAX_CLAUDE_TIMEOUT_SECONDS
  ) {
    throw new Error(`${label} must be an integer from 1 to 86400 seconds`);
  }
  return value;
}

function deliveryBody(job) {
  return (
    `<cxmsg-delivery id="${job.jobId}">\n` +
    `${job.task}\n` +
    "</cxmsg-delivery>\n\n" +
    "After receiving this message, you may first reply with one correlated status=accepted envelope. " +
    "If you do, later reply with exactly one correlated status=completed or status=failed envelope. " +
    "Otherwise reply with exactly one terminal envelope after processing:\n" +
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

function deliverySourceMatches(job, parsed) {
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

export async function recordClaudeDeliveryReply(
  parsed,
  jobId,
  { log = writeCoordinationEvent } = {},
) {
  const job = readJob(jobId);
  if (!job || job.kind !== "claude-delivery") return null;
  let transitioned = false;
  let conflict = null;
  let rejection = null;
  let late = false;
  const now = new Date().toISOString();
  const updated = await mutateJob(job.jobId, (current) => {
    late = ["ack_timeout", "completion_timeout"].includes(current.status);
    if (!deliverySourceMatches(current, parsed)) {
      rejection = Object.assign(
        new Error(`Claude delivery reply source mismatch: ${jobId}`),
        { code: "EREPLYSOURCE" },
      );
      return current;
    }
    if (current.replyEvidence?.messageId === parsed.messageId) return current;
    if (current.replyEvidence) {
      conflict = Object.assign(
        new Error(`Claude delivery already has correlated reply evidence: ${jobId}`),
        { code: "EREPLYCONFLICT" },
      );
      return current;
    }
    transitioned = true;
    return {
      ...current,
      replyEvidence: {
        status: "correlated",
        messageId: parsed.messageId,
        receivedAt: now,
        late,
      },
    };
  });
  if (rejection) {
    await log({
      kind: "claude-delivery",
      phase: "reply-source-validation",
      correlationId: job.jobId,
      target: job.target,
      attempt: job.delivery?.attempt,
      outcome: "rejected",
      errorCode: "source_mismatch",
      late,
    });
    throw rejection;
  }
  if (conflict) throw conflict;
  if (transitioned) {
    await log({
      kind: "claude-delivery",
      phase: "reply-correlated",
      correlationId: updated.jobId,
      target: updated.target,
      attempt: updated.delivery?.attempt,
      outcome: updated.status,
      late,
    });
  }
  return { ...updated, replyTransitioned: transitioned };
}

export async function recordClaudeNativeDeliveryReceipt(
  receipt,
  { log = writeCoordinationEvent } = {},
) {
  const matches = listJobs().filter(
    (job) =>
      job.kind === "claude-delivery" &&
      job.delivery?.messageIds?.includes(receipt.messageId),
  );
  if (matches.length === 0) {
    await log({
      kind: "claude-delivery",
      phase: "native-receipt-unmatched",
      correlationId: receipt.messageId,
      outcome: "ignored",
      errorCode: "unknown_message",
    });
    return null;
  }
  if (matches.length > 1) {
    throw Object.assign(
      new Error("Claude native receipt matches multiple deliveries"),
      { code: "ENATIVERECEIPTAMBIGUOUS" },
    );
  }
  const job = matches[0];
  const terminal = new Set(["denied", "expired", "delivered"]);
  let transitioned = false;
  let conflict = null;
  const now = new Date().toISOString();
  const updated = await mutateJob(job.jobId, (current) => {
    const receipts = [...(current.delivery?.nativeReceipts || [])];
    const index = receipts.findIndex(
      (candidate) => candidate.messageId === receipt.messageId,
    );
    const existing = index === -1 ? null : receipts[index];
    if (existing?.status === receipt.status) return current;
    if (existing && terminal.has(existing.status)) {
      conflict = Object.assign(
        new Error("Claude native receipt terminal status conflict"),
        { code: "ENATIVERECEIPTCONFLICT" },
      );
      return current;
    }
    const evidence = {
      status: receipt.status,
      messageId: receipt.messageId,
      receivedAt: now,
      late: ["ack_timeout", "completion_timeout"].includes(current.status),
    };
    if (index === -1) receipts.push(evidence);
    else receipts[index] = evidence;
    transitioned = true;
    return {
      ...current,
      delivery: {
        ...current.delivery,
        nativeReceipts: receipts,
      },
    };
  });
  if (conflict) throw conflict;
  if (transitioned) {
    await log({
      kind: "claude-delivery",
      phase: "native-receipt",
      correlationId: updated.jobId,
      target: updated.target,
      attempt: updated.delivery?.attempt,
      outcome: `native_${receipt.status}`,
      late: ["ack_timeout", "completion_timeout"].includes(updated.status),
    });
  }
  return { ...updated, nativeReceiptTransitioned: transitioned };
}

export async function createClaudeDeliveryJob({
  from,
  sourceRecord,
  peer,
  message,
  maxAttempts = 4,
  ackTimeoutSeconds = DEFAULT_CLAUDE_ACK_TIMEOUT_SECONDS,
  completionTimeoutSeconds = DEFAULT_CLAUDE_COMPLETION_TIMEOUT_SECONDS,
  logicalMessageId = null,
  replyToMessageId = null,
}) {
  boundedTimeoutSeconds("ACK timeout", ackTimeoutSeconds);
  boundedTimeoutSeconds("completion timeout", completionTimeoutSeconds);
  if (replyToMessageId !== null && logicalMessageId === null) {
    throw new Error("Claude peer reply correlation requires a Logical Message ID");
  }
  if (
    logicalMessageId !== null &&
    (!UUID_PATTERN.test(logicalMessageId) ||
      (replyToMessageId !== null &&
        (!UUID_PATTERN.test(replyToMessageId) ||
          logicalMessageId === replyToMessageId)))
  ) {
    throw new Error("Claude peer reply correlation is invalid");
  }
  const jobId = logicalMessageId || randomUUID();
  const conversation = await recordDirectMessageIfKnown({
    logicalMessageId: jobId,
    senderNodeKey: `codex:${sourceRecord.threadId}`,
    recipientNodeKey: `claude:${peer.sessionId}`,
    replyToMessageId,
    sourceKind: "claude-job",
  });
  const spec = {
    jobId,
    from,
    target: peer.name,
    targetThreadId: sourceRecord.threadId,
    threadId: null,
    task: message,
    kind: "claude-delivery",
  };
  const initial = (created) => ({
    ...created,
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
      acceptedAt: null,
      completionDeadlineAt: null,
      nextAttemptAt: null,
      errorCode: null,
    },
    ack: null,
    correlation:
      replyToMessageId === null
        ? null
        : {
            kind: "peer-reply",
            logicalMessageId: jobId,
            replyToMessageId,
          },
    conversation: conversation
      ? {
          conversationId: conversation.conversation.conversationId,
          sequence: conversation.message.sequence,
        }
      : null,
    ackTimeoutSeconds,
    completionTimeoutSeconds,
  });
  let job;
  if (logicalMessageId) {
    const once = await createJobOnce(spec, initial);
    job = once.job;
    if (!once.created) {
      if (
        job.kind !== "claude-delivery" ||
        job.from !== from ||
        job.task !== message ||
        job.claudeTarget?.sessionId !== peer.sessionId ||
        (job.ackTimeoutSeconds || DEFAULT_CLAUDE_ACK_TIMEOUT_SECONDS) !==
          ackTimeoutSeconds ||
        (job.completionTimeoutSeconds ||
          DEFAULT_CLAUDE_COMPLETION_TIMEOUT_SECONDS) !==
          completionTimeoutSeconds ||
        (replyToMessageId === null && job.correlation !== null) ||
        (replyToMessageId !== null &&
          (job.correlation?.kind !== "peer-reply" ||
            job.correlation.logicalMessageId !== logicalMessageId ||
            job.correlation.replyToMessageId !== replyToMessageId)) ||
        (conversation &&
          (job.conversation?.conversationId !==
            conversation.conversation.conversationId ||
            job.conversation?.sequence !== conversation.message.sequence))
      ) {
        throw new Error(`Claude peer reply idempotency conflict: ${logicalMessageId}`);
      }
      await writeCoordinationEvent({
        kind: "claude-delivery",
        phase: "deduplication",
        correlationId: job.jobId,
        target: job.target,
        attempt: job.delivery?.attempt || 0,
        outcome: job.status,
      });
      if (conversation) {
        await refreshDirectConversationSummary(
          conversation.conversation.conversationId,
        );
      }
      return { ...job, deduplicated: true };
    }
  } else {
    const created = createJob(spec);
    job = await updateJob(created, initial(created));
  }
  if (conversation) {
    const refreshed = await refreshDirectConversationSummary(
      conversation.conversation.conversationId,
    );
    if (!refreshed.refreshed) {
      throw new Error("Direct Conversation summary source is unavailable after Job commit");
    }
  }
  await writeCoordinationEvent({
    kind: "claude-delivery",
    phase: "created",
    correlationId: job.jobId,
    target: job.target,
    attempt: 0,
    outcome: job.status,
  });
  return job;
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
  {
    peers = listClaudePeers,
    send = sendClaudePeerFrame,
    log = writeCoordinationEvent,
  } = {},
) {
  const current = readJob(job.jobId) || job;
  if ((current.delivery?.attempt || 0) >= (current.delivery?.maxAttempts || 4)) {
    return await updateJob(current, {
      status: "failed",
      error: "Claude delivery exhausted its retry budget",
      completedAt: new Date().toISOString(),
    });
  }
  const messageId = randomUUID();
  const attemptedAt = new Date().toISOString();
  const attempt = (current.delivery?.attempt || 0) + 1;
  await log({
    kind: "claude-delivery",
    phase: "transport-attempt",
    correlationId: current.jobId,
    target: current.target,
    attempt,
    outcome: "attempted",
  });
  try {
    const peer = resolveStoredTarget(await peers(), current);
    const sending =
      current.claudeTarget?.sessionId &&
      (current.claudeTarget.address !== peer.address ||
        current.claudeTarget.name !== peer.name)
        ? await updateJob(current, {
            claudeTarget: {
              name: peer.name,
              sessionId: current.claudeTarget.sessionId,
              address: peer.address,
            },
          })
        : current;
    const frame = buildClaudePeerFrame({
      fromSocket: bridgeRecord.socketPath,
      fromName: `codex-${sending.from}`,
      fromSession: sourceRecord.threadId,
      message: deliveryBody(sending),
      messageId,
    });
    await send(peer.socketPath, frame);
    const ackDeadlineAt = new Date(
      Date.now() +
        (current.ackTimeoutSeconds || DEFAULT_CLAUDE_ACK_TIMEOUT_SECONDS) * 1_000,
    ).toISOString();
    const delivered = await updateJob(sending, {
      status: "transport_delivered",
      error: null,
      delivery: {
        ...sending.delivery,
        attempt,
        messageIds: [...(sending.delivery?.messageIds || []), messageId],
        transportStatus: "delivered",
        attemptedAt,
        deliveredAt: new Date().toISOString(),
        ackDeadlineAt,
        nextAttemptAt: null,
        errorCode: null,
      },
    });
    await log({
      kind: "claude-delivery",
      phase: "transport",
      correlationId: delivered.jobId,
      target: delivered.target,
      attempt,
      outcome: "delivered",
    });
    return delivered;
  } catch (error) {
    const failed = await updateJob(current, {
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
    await log({
      kind: "claude-delivery",
      phase: "transport",
      correlationId: failed.jobId,
      target: failed.target,
      attempt,
      outcome: "failed",
      errorCode: error?.code || "transport_error",
    });
    return failed;
  }
}

export async function recordClaudeDeliveryAck(
  parsed,
  ack,
  { log = writeCoordinationEvent } = {},
) {
  const job = readJob(ack.jobId);
  if (!job || job.kind !== "claude-delivery") return null;
  await log({
    kind: "claude-delivery",
    phase: "ack-ingress",
    correlationId: job.jobId,
    target: job.target,
    attempt: job.delivery?.attempt,
    outcome: ack.status,
    errorCode: ack.code,
  });
  const now = new Date().toISOString();
  let ackTransitioned = false;
  let rejection = null;
  let rejectionPhase = "ack-source-validation";
  let rejectionOutcome = "rejected";
  let late = false;
  const updated = await mutateJob(job.jobId, (current) => {
    if (!deliverySourceMatches(current, parsed)) {
      late = ["ack_timeout", "completion_timeout"].includes(current.status);
      const source = ackSourceEvidence(current, parsed);
      rejection = Object.assign(
        new Error(
          `Claude delivery ACK source mismatch: ${ack.jobId} ` +
            `expected-session=${source.expectedSession} actual-session=${source.actualSession} ` +
            `expected-address=${source.expectedAddress} actual-address=${source.actualAddress}`,
        ),
        { code: "EACKSOURCE" },
      );
      if (late) return current;
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
      rejectionPhase = "ack-transition";
      rejectionOutcome = current.status;
      rejection = Object.assign(
        new Error(`Claude delivery is already ${current.status}: ${ack.jobId}`),
        { code: "EACKTRANSITION" },
      );
      return current;
    }
    if (
      ack.status === "accepted" &&
      ["acknowledged", "completion_timeout"].includes(current.status) &&
      current.ack?.status === "accepted"
    ) {
      return current;
    }
    if (
      current.delivery?.acceptedAt &&
      ack.status === "retryable_error"
    ) {
      rejectionPhase = "ack-transition";
      rejectionOutcome = current.status;
      rejection = Object.assign(
        new Error("An accepted Claude delivery requires a terminal ACK"),
        { code: "EACKTRANSITION" },
      );
      return current;
    }
    ackTransitioned = true;
    late = ["ack_timeout", "completion_timeout"].includes(current.status);
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
    const accepted = ack.status === "accepted";
    const completionDeadlineAt = accepted
      ? new Date(
          Date.now() +
            (current.completionTimeoutSeconds ||
              DEFAULT_CLAUDE_COMPLETION_TIMEOUT_SECONDS) *
              1_000,
        ).toISOString()
      : current.delivery?.completionDeadlineAt || null;
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
        late,
      },
      delivery: {
        ...current.delivery,
        acceptedAt: accepted
          ? current.delivery?.acceptedAt || now
          : current.delivery?.acceptedAt || null,
        completionDeadlineAt,
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
  if (rejection) {
    await log({
      kind: "claude-delivery",
      phase: rejectionPhase,
      correlationId: job.jobId,
      target: job.target,
      attempt: job.delivery?.attempt,
      outcome: rejectionOutcome,
      errorCode: rejection.code || "ack_rejected",
      late,
    });
    throw rejection;
  }
  await log({
    kind: "claude-delivery",
    phase: "ack-persisted",
    correlationId: updated.jobId,
    target: updated.target,
    attempt: updated.delivery?.attempt,
    outcome: updated.status,
    errorCode: updated.delivery?.errorCode,
    late,
  });
  return { ...updated, ackTransitioned };
}

export async function refreshClaudeDelivery(
  job,
  { log = writeCoordinationEvent, now = Date.now() } = {},
) {
  if (job?.kind !== "claude-delivery") return job;
  const candidate =
    (job.status === "transport_delivered" &&
      Date.parse(job.delivery?.ackDeadlineAt) <= now) ||
    (job.status === "acknowledged" &&
      Date.parse(job.delivery?.completionDeadlineAt) <= now);
  if (!candidate) return job;

  let transition = null;
  const timedOut = await mutateJob(job.jobId, (current) => {
    if (
      current.status === "transport_delivered" &&
      Date.parse(current.delivery?.ackDeadlineAt) <= now
    ) {
      transition = {
        status: "ack_timeout",
        phase: "ack-deadline",
        error: "Claude did not acknowledge the delivery before its deadline",
      };
    } else if (
      current.status === "acknowledged" &&
      Date.parse(current.delivery?.completionDeadlineAt) <= now
    ) {
      transition = {
        status: "completion_timeout",
        phase: "completion-deadline",
        error: "Claude did not complete the accepted delivery before its deadline",
      };
    }
    if (!transition) return current;
    return {
      ...current,
      status: transition.status,
      error: transition.error,
    };
  });
  if (transition) {
    await log({
      kind: "claude-delivery",
      phase: transition.phase,
      correlationId: timedOut.jobId,
      target: timedOut.target,
      attempt: timedOut.delivery?.attempt,
      outcome: "timeout",
    });
  }
  return timedOut;
}
