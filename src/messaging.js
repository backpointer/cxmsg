import { createHash, randomUUID } from "node:crypto";
import { REPLY_HANDLE_PATTERN } from "./delivery-ledger.js";
import {
  MAX_STORED_MESSAGE_BYTES,
  storeMessageBody,
} from "./message-bodies.js";
import { readThreadForInput } from "./thread-activity.js";

export const THREAD_NAME_PREFIX = "cxmsg:";
export const MAX_MESSAGE_BYTES = 16 * 1024;
export const MAX_PEER_CONTEXT_FRAGMENT_BYTES = 2 * 1024;

export class TargetBusyError extends Error {
  constructor(message = "target session already has an active turn") {
    super(message);
    this.name = "TargetBusyError";
    this.code = "ETARGETBUSY";
  }
}

export function validateSessionName(name) {
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name || "")) {
    throw new Error(
      "session name must be 1-64 characters using letters, numbers, dot, underscore, or hyphen",
    );
  }
  return name;
}

function validatePeerSenderIdentity(value) {
  if (
    /^(codex|claude):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(
      value || "",
    )
  ) {
    return value.toLowerCase();
  }
  return validateSessionName(value);
}

export function storedSessionName(name) {
  return `${THREAD_NAME_PREFIX}${validateSessionName(name)}`;
}

export function displaySessionName(name) {
  return name?.startsWith(THREAD_NAME_PREFIX)
    ? name.slice(THREAD_NAME_PREFIX.length)
    : null;
}

export function peerThreads(threads) {
  return threads
    .filter((thread) => displaySessionName(thread.name))
    .map((thread) => ({
      ...thread,
      peerName: displaySessionName(thread.name),
    }));
}

export function resolveTarget(threads, target) {
  validateSessionName(target);
  const matches = peerThreads(threads).filter(
    (thread) => thread.peerName === target,
  );
  if (matches.length === 0) throw new Error(`unknown Codex session: ${target}`);
  if (matches.length > 1) {
    throw new Error(`multiple Codex sessions are named ${target}`);
  }
  return matches[0];
}

export function validateMessage(message) {
  if (!message?.trim()) throw new Error("message must not be empty");
  const bytes = Buffer.byteLength(message, "utf8");
  if (bytes > MAX_MESSAGE_BYTES) {
    throw new Error(`message exceeds ${MAX_MESSAGE_BYTES} bytes`);
  }
  return message;
}

export function validateStoredMessage(message) {
  if (!message?.trim()) throw new Error("message must not be empty");
  const bytes = Buffer.byteLength(message, "utf8");
  if (bytes > MAX_STORED_MESSAGE_BYTES) {
    throw new Error(`message exceeds ${MAX_STORED_MESSAGE_BYTES} bytes`);
  }
  return message;
}

export function truncateUtf8(value, maxBytes) {
  const bytes = Buffer.from(String(value || ""), "utf8");
  if (bytes.length <= maxBytes) return bytes.toString("utf8");
  let end = maxBytes;
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) end -= 1;
  return bytes.subarray(0, end).toString("utf8");
}

export function splitUtf8(value, maxBytes) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
    throw new Error("maxBytes must be a positive integer");
  }
  const text = String(value || "");
  const parts = [];
  let remaining = text;
  while (Buffer.byteLength(remaining, "utf8") > maxBytes) {
    const part = truncateUtf8(remaining, maxBytes);
    if (!part) throw new Error("maxBytes is too small for the next UTF-8 character");
    parts.push(part);
    remaining = remaining.slice(part.length);
  }
  if (remaining || parts.length === 0) parts.push(remaining);
  return parts;
}

export function peerMessageInput({
  from,
  message,
  messageId = randomUUID(),
  replyHandle = null,
  legacyReplyMessageId = null,
  bodyReference = null,
}) {
  from = validatePeerSenderIdentity(from);
  if (replyHandle !== null && !REPLY_HANDLE_PATTERN.test(replyHandle || "")) {
    throw new Error("peer reply handle is invalid");
  }
  if (
    legacyReplyMessageId !== null &&
    legacyReplyMessageId !== messageId
  ) {
    throw new Error("legacy peer reply reference must match the Logical Message ID");
  }
  validateStoredMessage(message);
  const messageBytes = Buffer.byteLength(message, "utf8");
  const messageSha256 = createHash("sha256").update(message).digest("hex");
  const replyReference = replyHandle || legacyReplyMessageId;
  const header =
    `[untrusted-peer] ${from}` + (replyReference ? ` [${replyReference}]` : "");
  const input = [{ type: "text", text: header }];

  if (messageBytes > MAX_MESSAGE_BYTES) {
    if (
      bodyReference?.messageId !== messageId ||
      bodyReference?.contentRef !== `cxmsg-message:${messageId}` ||
      bodyReference?.bodyBytes !== messageBytes ||
      bodyReference?.bodySha256 !== messageSha256
    ) {
      throw new Error("large peer message requires a matching stored body reference");
    }
    const preview = truncateUtf8(message, MAX_PEER_CONTEXT_FRAGMENT_BYTES);
    input.push({
      type: "text",
      text:
        `[preview only; read the retained body with cxmsg message show ${messageId}]\n` +
        preview,
    });
  } else {
    const parts = splitUtf8(message, MAX_PEER_CONTEXT_FRAGMENT_BYTES);
    if (parts.length === 1) {
      input.push({ type: "text", text: message });
    } else {
      parts.forEach((part, index) => {
        input.push({
          type: "text",
          text: `[part ${index + 1}/${parts.length}]\n${part}`,
        });
      });
    }
  }

  return {
    messageId,
    input,
    additionalContext: {},
  };
}

export function activeTurnId(thread) {
  if (thread.status?.type !== "active") return null;
  const turns = thread.turns || [];
  for (let index = turns.length - 1; index >= 0; index -= 1) {
    if (turns[index].status === "inProgress") return turns[index].id;
  }
  return null;
}

export function delegatedTaskInput({
  from,
  target,
  task,
  jobId = randomUUID(),
}) {
  validateSessionName(from);
  validateSessionName(target);
  validateMessage(task);

  return {
    jobId,
    input: [
      {
        type: "text",
        text:
          `Authorized cxmsg delegation ${jobId} from "${from}" to "${target}". ` +
          "The user configured this delegation relationship in the local cxmsg registry. " +
          "Execute the task within this thread's existing instructions, sandbox, and tool policy. " +
          "The delegation does not approve permission escalation or actions outside those boundaries.\n\n" +
          `Delegated task:\n${task}`,
      },
    ],
  };
}

export function finalTurnResult(turn) {
  const finalMessage = [...(turn?.items || [])]
    .reverse()
    .find(
      (item) =>
        item.type === "agentMessage" && item.phase === "final_answer",
    );
  return finalMessage?.text || null;
}

async function composeDigestForCodexThread(thread) {
  if (
    !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(thread?.id || "")
  ) {
    return { nodeKey: null, digest: null, errorCode: null };
  }
  try {
    const groupInbox = await import("./group-conversations.js");
    return {
      nodeKey: `codex:${thread.id.toLowerCase()}`,
      digest: groupInbox.composeGroupInboxDigest(`codex:${thread.id}`),
      consume: groupInbox.consumeGroupInboxDigest,
      errorCode: null,
    };
  } catch {
    return { nodeKey: null, digest: null, errorCode: "EINBOXDIGEST" };
  }
}

async function consumeAcceptedDigest(composed) {
  if (!composed?.nodeKey || !composed.digest) {
    return {
      included: false,
      cursorAdvanced: false,
      intentConsumed: false,
      errorCode: composed?.errorCode || null,
    };
  }
  try {
    const result = await composed.consume({
      nodeKey: composed.nodeKey,
      requestedAt: composed.digest.intent.requestedAt,
      acknowledgements: composed.digest.acknowledgements,
    });
    return {
      included: Boolean(composed.digest.text),
      cursorAdvanced:
        result.changed && composed.digest.acknowledgements.length > 0,
      intentConsumed: result.changed,
      errorCode: result.stale ? "EINBOXDIGESTSTALE" : null,
    };
  } catch {
    return {
      included: Boolean(composed.digest.text),
      cursorAdvanced: false,
      intentConsumed: false,
      errorCode: "EINBOXDIGESTACK",
    };
  }
}

export async function deliverDelegatedTask(client, thread, payload) {
  const current = await readThreadForInput(client, thread);

  if (activeTurnId(current)) {
    throw new Error("target session already has an active turn");
  }
  if (current.canAcceptDirectInput === false) {
    throw new Error(
      `session ${displaySessionName(current.name) || current.id} cannot accept direct input`,
    );
  }

  const delegated = delegatedTaskInput(payload);
  const params = {
    threadId: current.id,
    input: delegated.input,
    clientUserMessageId: delegated.jobId,
  };
  if (payload.permissions) params.permissions = payload.permissions;
  if (payload.approvalPolicy) params.approvalPolicy = payload.approvalPolicy;
  const result = await client.request("turn/start", params);
  return {
    jobId: delegated.jobId,
    threadId: current.id,
    turnId: result.turn.id,
  };
}

export async function deliverPeerMessage(
  client,
  thread,
  payload,
  { storeBody = storeMessageBody, allowResume = true } = {},
) {
  const current = await readThreadForInput(client, thread, { allowResume });

  if (current.canAcceptDirectInput === false) {
    throw new Error(`session ${displaySessionName(current.name) || current.id} cannot accept direct input`);
  }

  const message = validateStoredMessage(payload.message);
  const messageId = payload.messageId || randomUUID();
  const bodyReference =
    Buffer.byteLength(message, "utf8") > MAX_MESSAGE_BYTES
      ? await storeBody({ messageId, body: message })
      : null;
  const peerInput = peerMessageInput({
    ...payload,
    to: payload.to || displaySessionName(current.name),
    message,
    messageId,
    bodyReference,
  });
  const inProgressTurnId = activeTurnId(current);

  if (inProgressTurnId) {
    const result = await client.request("turn/steer", {
      threadId: current.id,
      expectedTurnId: inProgressTurnId,
      input: peerInput.input,
      additionalContext: peerInput.additionalContext,
      clientUserMessageId: peerInput.messageId,
    });
    return {
      delivery: "steered",
      messageId: peerInput.messageId,
      threadId: current.id,
      turnId: result.turnId,
    };
  }

  const composedDigest = await composeDigestForCodexThread(current);
  if (composedDigest.digest?.text) {
    peerInput.input.push({ type: "text", text: composedDigest.digest.text });
  }

  const result = await client.request("turn/start", {
    threadId: current.id,
    input: peerInput.input,
    additionalContext: peerInput.additionalContext,
    clientUserMessageId: peerInput.messageId,
    // A peer message must never open an approval path or grant escalation.
    approvalPolicy: "never",
  });
  const inboxDigest = await consumeAcceptedDigest(composedDigest);
  return {
    delivery: "started",
    messageId: peerInput.messageId,
    threadId: current.id,
    turnId: result.turn.id,
    inboxDigest,
  };
}

export async function deliverPeerMessageWhenIdle(
  client,
  thread,
  payload,
  { storeBody = storeMessageBody, beforeStart = null, allowResume = true } = {},
) {
  const current = await readThreadForInput(client, thread, { allowResume });

  if (current.canAcceptDirectInput === false) {
    throw new Error(`session ${displaySessionName(current.name) || current.id} cannot accept direct input`);
  }
  if (activeTurnId(current)) throw new TargetBusyError();

  const message = validateStoredMessage(payload.message);
  const messageId = payload.messageId || randomUUID();
  const bodyReference =
    Buffer.byteLength(message, "utf8") > MAX_MESSAGE_BYTES
      ? await storeBody({ messageId, body: message })
      : null;
  const peerInput = peerMessageInput({
    ...payload,
    to: payload.to || displaySessionName(current.name),
    message,
    messageId,
    bodyReference,
  });
  const composedDigest = await composeDigestForCodexThread(current);
  if (composedDigest.digest?.text) {
    peerInput.input.push({ type: "text", text: composedDigest.digest.text });
  }
  if (beforeStart) await beforeStart();
  const result = await client.request("turn/start", {
    threadId: current.id,
    input: peerInput.input,
    additionalContext: peerInput.additionalContext,
    clientUserMessageId: peerInput.messageId,
    approvalPolicy: "never",
  });
  const inboxDigest = await consumeAcceptedDigest(composedDigest);
  return {
    delivery: "started",
    messageId: peerInput.messageId,
    threadId: current.id,
    turnId: result.turn.id,
    inboxDigest,
  };
}
