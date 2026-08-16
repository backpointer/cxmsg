import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { requireNoFollowFlag } from "./file-safety.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const CONVERSATION_SUMMARIES_DIR = path.join(
  CXMSG_STATE_DIR,
  "conversations",
  "summaries",
);

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const NODE_KEY_PATTERN = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i;
const LABEL_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SUMMARY_LIMIT = 2_560;
const RECORD_MAX_BYTES = 32 * 1024;
const SUMMARY_FIELDS = new Set([
  "version",
  "kind",
  "conversationId",
  "currentMembers",
  "label",
  "lastActivityAt",
  "lastMessageId",
  "lastSenderNodeKey",
  "lastSourceKind",
  "lastSequence",
  "messageCount",
  "conversationUpdatedAt",
  "recordEvidence",
]);

function ownerUid() {
  if (typeof process.getuid !== "function") {
    throw new Error("Conversation summary storage requires owner identity support");
  }
  return process.getuid();
}

function ensureDirectory() {
  if (!existsSync(CONVERSATION_SUMMARIES_DIR)) {
    mkdirSync(CONVERSATION_SUMMARIES_DIR, { mode: 0o700, recursive: true });
  }
  const metadata = lstatSync(CONVERSATION_SUMMARIES_DIR);
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    metadata.uid !== ownerUid() ||
    (metadata.mode & 0o077) !== 0
  ) {
    throw new Error("Conversation summary storage must be owner-controlled");
  }
  chmodSync(CONVERSATION_SUMMARIES_DIR, 0o700);
}

function privateDirectory() {
  try {
    const metadata = lstatSync(CONVERSATION_SUMMARIES_DIR);
    return Boolean(
      metadata.isDirectory() &&
        !metadata.isSymbolicLink() &&
        metadata.uid === ownerUid() &&
        (metadata.mode & 0o077) === 0,
    );
  } catch {
    return false;
  }
}

function filenameFor(summary) {
  return `${summary.kind}--${summary.conversationId.toLowerCase()}.json`;
}

function summaryPath(kind, conversationId) {
  if (!["direct", "group"].includes(kind)) {
    throw new Error("Conversation summary kind is invalid");
  }
  if (!UUID_PATTERN.test(conversationId || "")) {
    throw new Error("Conversation summary id must be a UUID");
  }
  return path.join(
    CONVERSATION_SUMMARIES_DIR,
    `${kind}--${conversationId.toLowerCase()}.json`,
  );
}

function conversationPath(kind, conversationId) {
  if (!["direct", "group"].includes(kind)) {
    throw new Error("Conversation summary kind is invalid");
  }
  if (!UUID_PATTERN.test(conversationId || "")) {
    throw new Error("Conversation summary id must be a UUID");
  }
  return path.join(
    CXMSG_STATE_DIR,
    "conversations",
    kind,
    `${conversationId.toLowerCase()}.json`,
  );
}

function metadataEvidence(metadata) {
  return createHash("sha256")
    .update(
      `${metadata.dev}:${metadata.ino}:${metadata.size}:${metadata.mtimeMs}`,
    )
    .digest("hex");
}

export function conversationRecordEvidence(kind, conversationId) {
  try {
    const metadata = lstatSync(conversationPath(kind, conversationId));
    if (
      !metadata.isFile() ||
      metadata.isSymbolicLink() ||
      metadata.uid !== ownerUid() ||
      metadata.nlink !== 1 ||
      (metadata.mode & 0o077) !== 0 ||
      metadata.size > 4 * 1024 * 1024
    ) {
      return null;
    }
    return metadataEvidence(metadata);
  } catch {
    return null;
  }
}

export function conversationSummaryMatchesRecord(summary) {
  return Boolean(
    validConversationSummary(summary) &&
      conversationRecordEvidence(summary.kind, summary.conversationId) ===
        summary.recordEvidence,
  );
}

export function validConversationSummary(summary) {
  if (
    summary?.version !== 1 ||
    !["direct", "group"].includes(summary.kind) ||
    !UUID_PATTERN.test(summary.conversationId || "") ||
    !Array.isArray(summary.currentMembers) ||
    summary.currentMembers.some((member) => !NODE_KEY_PATTERN.test(member)) ||
    JSON.stringify([...summary.currentMembers].sort()) !==
      JSON.stringify(summary.currentMembers) ||
    new Set(summary.currentMembers).size !== summary.currentMembers.length ||
    (summary.kind === "direct"
      ? summary.currentMembers.length !== 2 || summary.label !== null
      : summary.currentMembers.length < 3 ||
        summary.currentMembers.length > 65 ||
        !LABEL_PATTERN.test(summary.label || "")) ||
    !Number.isSafeInteger(summary.messageCount) ||
    summary.messageCount < 0 ||
    summary.messageCount > 4_096 ||
    !Number.isFinite(Date.parse(summary.conversationUpdatedAt || "")) ||
    !/^[0-9a-f]{64}$/.test(summary.recordEvidence || "") ||
    !Object.keys(summary).every((field) => SUMMARY_FIELDS.has(field))
  ) {
    return false;
  }
  if (summary.lastActivityAt === null) {
    return Boolean(
      summary.messageCount === 0 &&
        summary.lastMessageId === null &&
        summary.lastSenderNodeKey === null &&
        summary.lastSourceKind === null &&
        summary.lastSequence === 0
    );
  }
  return Boolean(
    Number.isFinite(Date.parse(summary.lastActivityAt || "")) &&
      new Date(summary.lastActivityAt).toISOString() === summary.lastActivityAt &&
      UUID_PATTERN.test(summary.lastMessageId || "") &&
      NODE_KEY_PATTERN.test(summary.lastSenderNodeKey || "") &&
      (summary.kind === "group"
        ? summary.lastSourceKind === "delivery-ledger"
        : ["delivery-ledger", "claude-job"].includes(summary.lastSourceKind)) &&
      Number.isSafeInteger(summary.lastSequence) &&
      summary.lastSequence >= 1 &&
      summary.lastSequence <= summary.messageCount
  );
}

function secureRead(filename) {
  let descriptor;
  try {
    descriptor = openSync(filename, constants.O_RDONLY | requireNoFollowFlag());
    const metadata = fstatSync(descriptor);
    if (
      !metadata.isFile() ||
      metadata.uid !== ownerUid() ||
      metadata.nlink !== 1 ||
      (metadata.mode & 0o077) !== 0 ||
      metadata.size > RECORD_MAX_BYTES
    ) {
      throw new Error("Conversation summary record is not private");
    }
    const record = JSON.parse(readFileSync(descriptor, "utf8"));
    return validConversationSummary(record) ? record : null;
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
  }
}

export function writeConversationSummary(summary) {
  const recordEvidence = conversationRecordEvidence(
    summary?.kind,
    summary?.conversationId,
  );
  const stored = { ...summary, recordEvidence };
  if (!validConversationSummary(stored)) {
    throw new Error("invalid Conversation summary record");
  }
  ensureDirectory();
  const filename = summaryPath(stored.kind, stored.conversationId);
  const temporary = `${filename}.${randomUUID()}.tmp`;
  let descriptor;
  try {
    descriptor = openSync(
      temporary,
      constants.O_CREAT |
        constants.O_EXCL |
        constants.O_WRONLY |
        requireNoFollowFlag(),
      0o600,
    );
    writeFileSync(descriptor, `${JSON.stringify(stored, null, 2)}\n`);
    fsyncSync(descriptor);
  } catch (error) {
    try {
      unlinkSync(temporary);
    } catch {}
    throw error;
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
  }
  try {
    renameSync(temporary, filename);
  } catch (error) {
    try {
      unlinkSync(temporary);
    } catch {}
    throw error;
  }
  const directoryFd = openSync(CONVERSATION_SUMMARIES_DIR, constants.O_RDONLY);
  try {
    fsyncSync(directoryFd);
  } finally {
    closeSync(directoryFd);
  }
  return structuredClone(stored);
}

export function scanConversationSummaries() {
  const result = {
    records: [],
    diagnostics: {
      invalidRecords: 0,
      staleArtifacts: 0,
      unexpectedEntries: 0,
    },
  };
  if (!existsSync(CONVERSATION_SUMMARIES_DIR)) return result;
  if (!privateDirectory()) {
    throw new Error("Conversation summary storage must be owner-controlled");
  }
  const names = readdirSync(CONVERSATION_SUMMARIES_DIR).sort();
  if (names.length > 4_096) {
    throw new Error("Conversation summary storage exceeds its bounded entry limit");
  }
  if (names.filter((name) => name.endsWith(".json")).length > SUMMARY_LIMIT) {
    throw new Error("Conversation summary count exceeds its bounded limit");
  }
  for (const name of names) {
    if (/^(direct|group)--[0-9a-f-]{36}\.json\.[0-9a-f-]{36}\.tmp$/i.test(name)) {
      result.diagnostics.staleArtifacts += 1;
      continue;
    }
    if (!/^(direct|group)--[0-9a-f-]{36}\.json$/i.test(name)) {
      result.diagnostics.unexpectedEntries += 1;
      continue;
    }
    try {
      const record = secureRead(path.join(CONVERSATION_SUMMARIES_DIR, name));
      if (!record || filenameFor(record) !== name.toLowerCase()) {
        result.diagnostics.invalidRecords += 1;
        continue;
      }
      result.records.push(record);
    } catch {
      result.diagnostics.invalidRecords += 1;
    }
  }
  return result;
}

export function listConversationSummaries() {
  const scanned = scanConversationSummaries();
  return Array.isArray(scanned) ? scanned : scanned.records;
}
