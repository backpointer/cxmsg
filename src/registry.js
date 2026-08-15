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
import { withFileLock } from "./file-lock.js";
import { validateSessionName } from "./messaging.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

const SESSIONS_DIR = path.join(CXMSG_STATE_DIR, "sessions");
const THREAD_ID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function ensureRegistry() {
  mkdirSync(SESSIONS_DIR, { recursive: true, mode: 0o700 });
  chmodSync(SESSIONS_DIR, 0o700);
}

function recordPath(name) {
  return path.join(SESSIONS_DIR, `${validateSessionName(name)}.json`);
}

function lockPath(name) {
  return path.join(SESSIONS_DIR, `${validateSessionName(name)}.lock`);
}

function threadLockPath(threadId) {
  if (!THREAD_ID_PATTERN.test(threadId || "")) {
    throw new Error("thread-id must be a UUID");
  }
  return path.join(SESSIONS_DIR, `thread-${threadId.toLowerCase()}.lock`);
}

function validRecord(record) {
  return (
    record &&
    typeof record.name === "string" &&
    typeof record.threadId === "string" &&
    typeof record.cwd === "string"
  );
}

export function readSessionRecord(name) {
  try {
    const record = JSON.parse(readFileSync(recordPath(name), "utf8"));
    return validRecord(record) ? record : null;
  } catch {
    return null;
  }
}

export function listSessionRecords() {
  if (!existsSync(SESSIONS_DIR)) return [];
  return readdirSync(SESSIONS_DIR)
    .filter((filename) => filename.endsWith(".json"))
    .map((filename) => {
      try {
        const record = JSON.parse(
          readFileSync(path.join(SESSIONS_DIR, filename), "utf8"),
        );
        return validRecord(record) ? record : null;
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

export function sessionRecordsForThread(threadId) {
  if (!THREAD_ID_PATTERN.test(threadId || "")) {
    throw new Error("thread-id must be a UUID");
  }
  const normalized = threadId.toLowerCase();
  return listSessionRecords()
    .filter((record) => record.threadId.toLowerCase() === normalized)
    .sort((left, right) => left.name.localeCompare(right.name));
}

export function sessionAllowsAppServerResume(record) {
  return Boolean(
    record &&
      (record.adopted !== true || typeof record.managedByCxmsgAt === "string"),
  );
}

export function writeSessionRecord(record) {
  validateSessionName(record.name);
  if (!validRecord(record)) throw new Error("invalid cxmsg session record");
  ensureRegistry();
  const destination = recordPath(record.name);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(record, null, 2)}\n`, {
    mode: 0o600,
  });
  renameSync(temporary, destination);
}

export function removeSessionRecord(name) {
  const target = recordPath(name);
  if (existsSync(target)) unlinkSync(target);
}

export async function withSessionLock(name, callback, timeoutMs = 10_000) {
  ensureRegistry();
  return withFileLock(lockPath(name), callback, { timeoutMs });
}

export async function withSessionLocks(names, callback, timeoutMs = 10_000) {
  const ordered = [
    ...new Set(names.map((name) => validateSessionName(name))),
  ].sort();
  const acquire = (index) =>
    index === ordered.length
      ? callback()
      : withSessionLock(ordered[index], () => acquire(index + 1), timeoutMs);
  return acquire(0);
}

export async function withThreadRegistrationLock(
  threadId,
  callback,
  timeoutMs = 10_000,
) {
  ensureRegistry();
  return withFileLock(threadLockPath(threadId), callback, { timeoutMs });
}
