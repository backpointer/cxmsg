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
  ensureRegistry();
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
