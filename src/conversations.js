import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import { readDeliveryLedger } from "./delivery-ledger.js";
import { readJob } from "./jobs.js";
import {
  readNode,
  readNodeTombstone,
  readSuccessor,
} from "./node-directory.js";
import { withRetentionWriter } from "./retention-barrier.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const CONVERSATIONS_DIR = path.join(CXMSG_STATE_DIR, "conversations");
export const DIRECT_CONVERSATIONS_DIR = path.join(CONVERSATIONS_DIR, "direct");
const CONVERSATIONS_LOCK_PATH = path.join(CONVERSATIONS_DIR, "direct.lock");
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const NODE_KEY_PATTERN = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i;
const CONVERSATION_LIMIT = 2_048;
const MESSAGE_LIMIT = 4_096;
const MIGRATION_LIMIT = 32;
const RECORD_MAX_BYTES = 4 * 1024 * 1024;
const CONVERSATION_FIELDS = new Set([
  "version",
  "kind",
  "conversationId",
  "members",
  "currentMembers",
  "nextSequence",
  "messages",
  "migrations",
  "createdAt",
  "updatedAt",
]);
const MESSAGE_FIELDS = new Set([
  "version",
  "conversationId",
  "sequence",
  "logicalMessageId",
  "senderNodeKey",
  "recipientNodeKey",
  "sourceKind",
  "replyToMessageId",
  "parentConversationId",
  "crossConversationReply",
  "recordedAt",
]);
const MIGRATION_FIELDS = new Set([
  "version",
  "predecessorNodeKey",
  "successorNodeKey",
  "migratedAt",
]);

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value.toLowerCase();
}

function normalizeNodeKey(value) {
  const match = NODE_KEY_PATTERN.exec(value || "");
  if (!match) throw new Error("Conversation member must be a stable Node key");
  return `${match[1].toLowerCase()}:${match[2].toLowerCase()}`;
}

function sortedMembers(first, second) {
  const members = [normalizeNodeKey(first), normalizeNodeKey(second)].sort();
  if (members[0] === members[1]) {
    throw new Error("Direct Conversation requires two distinct Nodes");
  }
  return members;
}

export function directConversationId(first, second) {
  const members = sortedMembers(first, second);
  const bytes = Buffer.from(
    createHash("sha256")
      .update(`cxmsg-direct-v1\0${members[0]}\0${members[1]}`)
      .digest()
      .subarray(0, 16),
  );
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function ensureDirectory() {
  for (const directory of [CONVERSATIONS_DIR, DIRECT_CONVERSATIONS_DIR]) {
    if (!existsSync(directory)) mkdirSync(directory, { mode: 0o700 });
    const metadata = lstatSync(directory);
    if (
      !metadata.isDirectory() ||
      metadata.isSymbolicLink() ||
      metadata.uid !== process.getuid()
    ) {
      throw new Error("Conversation storage must be an owner-controlled directory");
    }
    chmodSync(directory, 0o700);
  }
}

function directoryIsPrivate(directory) {
  try {
    const metadata = lstatSync(directory);
    return Boolean(
      metadata.isDirectory() &&
        !metadata.isSymbolicLink() &&
        metadata.uid === process.getuid() &&
        (metadata.mode & 0o077) === 0,
    );
  } catch {
    return false;
  }
}

function conversationPath(conversationId) {
  return path.join(
    DIRECT_CONVERSATIONS_DIR,
    `${validateUuid("conversation id", conversationId)}.json`,
  );
}

function validMessage(message, conversationId, sequence) {
  return Boolean(
    message?.version === 1 &&
      message.conversationId === conversationId &&
      message.sequence === sequence &&
      UUID_PATTERN.test(message.logicalMessageId || "") &&
      NODE_KEY_PATTERN.test(message.senderNodeKey || "") &&
      NODE_KEY_PATTERN.test(message.recipientNodeKey || "") &&
      message.senderNodeKey !== message.recipientNodeKey &&
      ["delivery-ledger", "claude-job"].includes(message.sourceKind) &&
      (message.replyToMessageId === null ||
        UUID_PATTERN.test(message.replyToMessageId || "")) &&
      (message.parentConversationId === null ||
        UUID_PATTERN.test(message.parentConversationId || "")) &&
      typeof message.crossConversationReply === "boolean" &&
      Number.isFinite(Date.parse(message.recordedAt || "")) &&
      Object.keys(message).every((field) => MESSAGE_FIELDS.has(field))
  );
}

function validConversation(record) {
  if (
    record?.version !== 1 ||
    record.kind !== "direct" ||
    !UUID_PATTERN.test(record.conversationId || "") ||
    !Array.isArray(record.members) ||
    !Array.isArray(record.currentMembers) ||
    record.members.length !== 2 ||
    record.currentMembers.length !== 2 ||
    JSON.stringify([...record.members].sort()) !== JSON.stringify(record.members) ||
    JSON.stringify([...record.currentMembers].sort()) !== JSON.stringify(record.currentMembers) ||
    record.members.some((member) => !NODE_KEY_PATTERN.test(member)) ||
    record.currentMembers.some((member) => !NODE_KEY_PATTERN.test(member)) ||
    record.members[0] === record.members[1] ||
    record.currentMembers[0] === record.currentMembers[1] ||
    directConversationId(record.members[0], record.members[1]) !==
      record.conversationId ||
    !Array.isArray(record.messages) ||
    record.messages.length > MESSAGE_LIMIT ||
    !Array.isArray(record.migrations) ||
    record.migrations.length > MIGRATION_LIMIT ||
    record.nextSequence !== record.messages.length + 1 ||
    !Number.isFinite(Date.parse(record.createdAt || "")) ||
    !Number.isFinite(Date.parse(record.updatedAt || "")) ||
    !Object.keys(record).every((field) => CONVERSATION_FIELDS.has(field))
  ) {
    return false;
  }
  const messageIds = new Set();
  for (let index = 0; index < record.messages.length; index += 1) {
    const message = record.messages[index];
    if (!validMessage(message, record.conversationId, index + 1)) return false;
    if (messageIds.has(message.logicalMessageId)) return false;
    messageIds.add(message.logicalMessageId);
  }
  let projectedMembers = [...record.members];
  for (const migration of record.migrations) {
    if (
      migration?.version !== 1 ||
      !NODE_KEY_PATTERN.test(migration.predecessorNodeKey || "") ||
      !NODE_KEY_PATTERN.test(migration.successorNodeKey || "") ||
      !Number.isFinite(Date.parse(migration.migratedAt || "")) ||
      !Object.keys(migration).every((field) => MIGRATION_FIELDS.has(field)) ||
      !projectedMembers.includes(migration.predecessorNodeKey) ||
      projectedMembers.includes(migration.successorNodeKey)
    ) {
      return false;
    }
    projectedMembers = projectedMembers
      .map((member) =>
        member === migration.predecessorNodeKey
          ? migration.successorNodeKey
          : member,
      )
      .sort();
  }
  return JSON.stringify(projectedMembers) === JSON.stringify(record.currentMembers);
}

function secureRead(filename) {
  try {
    const metadata = lstatSync(filename);
    if (
      !metadata.isFile() ||
      metadata.isSymbolicLink() ||
      metadata.uid !== process.getuid() ||
      (metadata.mode & 0o077) !== 0
    ) {
      return null;
    }
    if (metadata.size > RECORD_MAX_BYTES) return null;
    const record = JSON.parse(readFileSync(filename, "utf8"));
    return validConversation(record) ? record : null;
  } catch {
    return null;
  }
}

function writeConversation(record) {
  if (!validConversation(record)) throw new Error("invalid Direct Conversation record");
  ensureDirectory();
  const destination = conversationPath(record.conversationId);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  const serialized = `${JSON.stringify(record, null, 2)}\n`;
  if (Buffer.byteLength(serialized, "utf8") > RECORD_MAX_BYTES) {
    throw new Error("Direct Conversation record reached its bounded storage limit");
  }
  const fd = openSync(
    temporary,
    constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW,
    0o600,
  );
  try {
    writeFileSync(fd, serialized);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(temporary, destination);
  const directoryFd = openSync(DIRECT_CONVERSATIONS_DIR, constants.O_RDONLY);
  try {
    fsyncSync(directoryFd);
  } finally {
    closeSync(directoryFd);
  }
  return record;
}

export function readDirectConversation(conversationId) {
  if (!directoryIsPrivate(CONVERSATIONS_DIR) || !directoryIsPrivate(DIRECT_CONVERSATIONS_DIR)) {
    return null;
  }
  return secureRead(conversationPath(conversationId));
}

function listDirectConversationsStrict() {
  if (!existsSync(DIRECT_CONVERSATIONS_DIR)) return [];
  if (!directoryIsPrivate(CONVERSATIONS_DIR) || !directoryIsPrivate(DIRECT_CONVERSATIONS_DIR)) {
    throw new Error("Conversation storage must be an owner-controlled directory");
  }
  const names = readdirSync(DIRECT_CONVERSATIONS_DIR).sort();
  if (names.length > CONVERSATION_LIMIT) {
    throw new Error("Direct Conversation count exceeds its bounded limit");
  }
  const records = [];
  for (const name of names) {
    if (!/^[0-9a-f-]{36}\.json$/i.test(name)) {
      throw new Error("Direct Conversation Directory contains an unexpected record");
    }
    const record = secureRead(path.join(DIRECT_CONVERSATIONS_DIR, name));
    if (!record || `${record.conversationId}.json` !== name.toLowerCase()) {
      throw new Error(`Direct Conversation failed validation: ${name}`);
    }
    records.push(record);
  }
  return records;
}

export function listDirectConversations() {
  return listDirectConversationsStrict();
}

function parseNodeKey(value) {
  const normalized = normalizeNodeKey(value);
  const [runtimeKind, nativeId] = normalized.split(":");
  return { nodeKey: normalized, runtimeKind, nativeId };
}

function liveNode(value) {
  const parsed = parseNodeKey(value);
  const node = readNode(parsed.runtimeKind, parsed.nativeId);
  return node?.nodeKey === parsed.nodeKey ? node : null;
}

function knownNode(value) {
  const parsed = parseNodeKey(value);
  return (
    readNode(parsed.runtimeKind, parsed.nativeId) ||
    readNodeTombstone(parsed.runtimeKind, parsed.nativeId)
  );
}

function findCurrentConversation(records, members) {
  const matches = records.filter(
    (record) => JSON.stringify(record.currentMembers) === JSON.stringify(members),
  );
  if (matches.length > 1) {
    throw new Error("Direct Conversation membership is ambiguous");
  }
  return matches[0] || null;
}

function ensureDirectConversationLocked(first, second, records) {
  const members = sortedMembers(first, second);
  if (!liveNode(members[0]) || !liveNode(members[1])) {
    throw new Error("Direct Conversation creation requires two live Nodes");
  }
  const current = findCurrentConversation(records, members);
  if (current) return { conversation: current, created: false };
  if (records.length >= CONVERSATION_LIMIT) {
    throw new Error("Direct Conversation count reached its bounded limit");
  }
  const conversationId = directConversationId(members[0], members[1]);
  const collision = records.find((record) => record.conversationId === conversationId);
  if (collision) throw new Error("Direct Conversation identity collision");
  const now = new Date().toISOString();
  const conversation = writeConversation({
    version: 1,
    kind: "direct",
    conversationId,
    members,
    currentMembers: members,
    nextSequence: 1,
    messages: [],
    migrations: [],
    createdAt: now,
    updatedAt: now,
  });
  return { conversation, created: true };
}

export async function ensureDirectConversation(first, second) {
  return withRetentionWriter(async () => {
    ensureDirectory();
    return withFileLock(CONVERSATIONS_LOCK_PATH, async () =>
      ensureDirectConversationLocked(first, second, listDirectConversationsStrict()),
    );
  });
}

export async function recordDirectMessage({
  logicalMessageId,
  senderNodeKey,
  recipientNodeKey,
  replyToMessageId = null,
  sourceKind,
  recordedAt = new Date().toISOString(),
}) {
  logicalMessageId = validateUuid("logical message id", logicalMessageId);
  const pair = sortedMembers(senderNodeKey, recipientNodeKey);
  senderNodeKey = normalizeNodeKey(senderNodeKey);
  recipientNodeKey = normalizeNodeKey(recipientNodeKey);
  if (replyToMessageId !== null) {
    replyToMessageId = validateUuid("reply-to message id", replyToMessageId);
  }
  if (!["delivery-ledger", "claude-job"].includes(sourceKind)) {
    throw new Error("Direct Conversation message source is invalid");
  }
  if (!Number.isFinite(Date.parse(recordedAt))) {
    throw new Error("Direct Conversation message timestamp is invalid");
  }
  return withRetentionWriter(async () => {
    ensureDirectory();
    return withFileLock(CONVERSATIONS_LOCK_PATH, async () => {
      let records = listDirectConversationsStrict();
      const existing = records
        .flatMap((record) => record.messages)
        .find((message) => message.logicalMessageId === logicalMessageId);
      if (existing) {
        if (
          existing.senderNodeKey !== senderNodeKey ||
          existing.recipientNodeKey !== recipientNodeKey ||
          existing.replyToMessageId !== replyToMessageId ||
          existing.sourceKind !== sourceKind
        ) {
          throw new Error(`Direct Conversation message idempotency conflict: ${logicalMessageId}`);
        }
        const conversation = records.find(
          (record) => record.conversationId === existing.conversationId,
        );
        return { conversation, message: existing, created: false };
      }
      const ensured = ensureDirectConversationLocked(pair[0], pair[1], records);
      const conversation = ensured.conversation;
      records = ensured.created ? [...records, conversation] : records;
      if (conversation.messages.length >= MESSAGE_LIMIT) {
        throw new Error("Direct Conversation message history reached its bounded limit");
      }
      const parent = replyToMessageId
        ? records
            .flatMap((record) => record.messages)
            .find((message) => message.logicalMessageId === replyToMessageId) || null
        : null;
      const message = {
        version: 1,
        conversationId: conversation.conversationId,
        sequence: conversation.nextSequence,
        logicalMessageId,
        senderNodeKey,
        recipientNodeKey,
        sourceKind,
        replyToMessageId,
        parentConversationId: parent?.conversationId || null,
        crossConversationReply: Boolean(
          parent && parent.conversationId !== conversation.conversationId,
        ),
        recordedAt,
      };
      const updated = writeConversation({
        ...conversation,
        nextSequence: conversation.nextSequence + 1,
        messages: [...conversation.messages, message],
        updatedAt: recordedAt,
      });
      return { conversation: updated, message, created: true };
    });
  });
}

export async function recordDirectMessageIfKnown(spec) {
  try {
    if (!liveNode(spec.senderNodeKey) || !liveNode(spec.recipientNodeKey)) return null;
    return await recordDirectMessage(spec);
  } catch (error) {
    if (/stable Node key|live Nodes/.test(error.message)) return null;
    throw error;
  }
}

export async function migrateDirectConversationMember({
  conversationId,
  predecessorNodeKey,
  successorNodeKey,
}) {
  conversationId = validateUuid("conversation id", conversationId);
  predecessorNodeKey = normalizeNodeKey(predecessorNodeKey);
  successorNodeKey = normalizeNodeKey(successorNodeKey);
  return withRetentionWriter(async () => {
    ensureDirectory();
    return withFileLock(CONVERSATIONS_LOCK_PATH, async () => {
      const records = listDirectConversationsStrict();
      const conversation = records.find(
        (record) => record.conversationId === conversationId,
      );
      if (!conversation) throw new Error(`unknown Direct Conversation: ${conversationId}`);
      const existing = conversation.migrations.find(
        (migration) =>
          migration.predecessorNodeKey === predecessorNodeKey &&
          migration.successorNodeKey === successorNodeKey,
      );
      if (existing && conversation.currentMembers.includes(successorNodeKey)) {
        return { conversation, migrated: false };
      }
      if (!conversation.currentMembers.includes(predecessorNodeKey)) {
        throw new Error("Conversation predecessor is not a current member");
      }
      if (!knownNode(predecessorNodeKey) || !liveNode(successorNodeKey)) {
        throw new Error("Conversation migration requires known predecessor and live successor");
      }
      const relation = readSuccessor(successorNodeKey);
      if (relation?.predecessorNodeKey !== predecessorNodeKey) {
        throw new Error("Conversation migration requires the exact successor relation");
      }
      const currentMembers = conversation.currentMembers
        .map((member) => (member === predecessorNodeKey ? successorNodeKey : member))
        .sort();
      const collision = findCurrentConversation(
        records.filter((record) => record.conversationId !== conversationId),
        currentMembers,
      );
      if (collision) {
        throw new Error("successor pair already has another Direct Conversation");
      }
      if (conversation.migrations.length >= MIGRATION_LIMIT) {
        throw new Error("Direct Conversation migration history reached its bounded limit");
      }
      const migratedAt = new Date().toISOString();
      const updated = writeConversation({
        ...conversation,
        currentMembers,
        migrations: [
          ...conversation.migrations,
          {
            version: 1,
            predecessorNodeKey,
            successorNodeKey,
            migratedAt,
          },
        ],
        updatedAt: migratedAt,
      });
      return { conversation: updated, migrated: true };
    });
  });
}

function memberState(nodeKey) {
  const parsed = parseNodeKey(nodeKey);
  if (readNode(parsed.runtimeKind, parsed.nativeId)) return "live";
  if (readNodeTombstone(parsed.runtimeKind, parsed.nativeId)) return "tombstoned";
  return "missing";
}

export function publicDirectConversation(record) {
  if (!validConversation(record)) throw new Error("invalid Direct Conversation record");
  return {
    version: record.version,
    kind: record.kind,
    conversationId: record.conversationId,
    members: record.members.map((nodeKey) => ({ nodeKey, state: memberState(nodeKey) })),
    currentMembers: record.currentMembers.map((nodeKey) => ({
      nodeKey,
      state: memberState(nodeKey),
    })),
    messageCount: record.messages.length,
    migrationCount: record.migrations.length,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}

export function directConversationHistory(
  conversationId,
  { limit = 50, beforeSequence = Number.MAX_SAFE_INTEGER } = {},
) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200) {
    throw new Error("Conversation history limit must be between 1 and 200");
  }
  if (!Number.isSafeInteger(beforeSequence) || beforeSequence < 1) {
    throw new Error("Conversation history cursor must be a positive integer");
  }
  const conversation = readDirectConversation(conversationId);
  if (!conversation) throw new Error(`unknown Direct Conversation: ${conversationId}`);
  return conversation.messages
    .filter((message) => message.sequence < beforeSequence)
    .slice(-limit)
    .map((message) => {
      const source =
        message.sourceKind === "delivery-ledger"
          ? readDeliveryLedger(message.logicalMessageId)
          : readJob(message.logicalMessageId);
      return {
        sequence: message.sequence,
        logicalMessageId: message.logicalMessageId,
        senderNodeKey: message.senderNodeKey,
        recipientNodeKey: message.recipientNodeKey,
        sourceKind: message.sourceKind,
        replyToMessageId: message.replyToMessageId,
        parentConversationId: message.parentConversationId,
        crossConversationReply: message.crossConversationReply,
        status:
          message.sourceKind === "delivery-ledger"
            ? source?.delivery?.state || "missing-source"
            : source?.status || "missing-source",
        recordedAt: message.recordedAt,
      };
    });
}

export function listConversationMessageIds() {
  return listDirectConversationsStrict().flatMap((record) =>
    record.messages.map((message) => message.logicalMessageId),
  );
}
