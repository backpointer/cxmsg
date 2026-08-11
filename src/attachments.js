import { randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { validateSessionName } from "./messaging.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const ATTACHMENTS_DIR = path.join(CXMSG_STATE_DIR, "attachments");

function ensureAttachmentsDir() {
  mkdirSync(ATTACHMENTS_DIR, { recursive: true, mode: 0o700 });
  chmodSync(ATTACHMENTS_DIR, 0o700);
}

function attachmentPath(name) {
  return path.join(ATTACHMENTS_DIR, `${validateSessionName(name)}.json`);
}

function validAttachmentRecord(record) {
  return Boolean(
    record &&
      record.version === 1 &&
      typeof record.name === "string" &&
      typeof record.threadId === "string" &&
      Number.isSafeInteger(record.childPid) &&
      record.childPid > 1 &&
      Number.isSafeInteger(record.parentPid) &&
      record.parentPid > 1 &&
      typeof record.cwd === "string" &&
      typeof record.startedAt === "string",
  );
}

export function readAttachmentRecord(name) {
  try {
    const record = JSON.parse(readFileSync(attachmentPath(name), "utf8"));
    return validAttachmentRecord(record) ? record : null;
  } catch {
    return null;
  }
}

export function listAttachmentRecords() {
  ensureAttachmentsDir();
  return readdirSync(ATTACHMENTS_DIR)
    .filter((filename) => filename.endsWith(".json"))
    .map((filename) => {
      try {
        const record = JSON.parse(
          readFileSync(path.join(ATTACHMENTS_DIR, filename), "utf8"),
        );
        return validAttachmentRecord(record) ? record : null;
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

export function writeAttachmentRecord(record) {
  validateSessionName(record.name);
  if (!validAttachmentRecord(record)) {
    throw new Error("invalid cxmsg attachment record");
  }
  ensureAttachmentsDir();
  const destination = attachmentPath(record.name);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(record, null, 2)}\n`, {
    mode: 0o600,
  });
  renameSync(temporary, destination);
}

export function removeAttachmentRecord(name, expectedChildPid = null) {
  const target = attachmentPath(name);
  if (!existsSync(target)) return false;
  if (expectedChildPid !== null) {
    const current = readAttachmentRecord(name);
    if (!current || current.childPid !== expectedChildPid) return false;
  }
  unlinkSync(target);
  return true;
}

export function markAttachmentDetachRequested(name, expectedChildPid) {
  const current = readAttachmentRecord(name);
  if (!current || current.childPid !== expectedChildPid) return null;
  const updated = {
    ...current,
    detachRequestedAt: new Date().toISOString(),
  };
  writeAttachmentRecord(updated);
  return updated;
}

export function attachmentCommandMatches(record, command) {
  if (!validAttachmentRecord(record) || typeof command !== "string") {
    return false;
  }
  return (
    command.includes("codex") &&
    command.includes("--remote") &&
    command.includes(record.threadId)
  );
}

export function sessionPresentation(sessionRecord, liveRecord = null) {
  if (liveRecord) return "foreground";
  if (sessionRecord && (sessionRecord.managedByCxmsgAt || !sessionRecord.adopted)) {
    return "background";
  }
  return "stored-or-external";
}
