import {
  appendFileSync,
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  renameSync,
  statSync,
  unlinkSync,
} from "node:fs";
import { withFileLock } from "./file-lock.js";
import {
  CXMSG_STATE_DIR,
  EVENT_LOG_ARCHIVES,
  EVENT_LOG_LOCK_PATH,
  EVENT_LOG_MAX_BYTES,
  EVENT_LOG_PATH,
} from "./runtime.js";

const SAFE_VALUE_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;
export { EVENT_LOG_ARCHIVES, EVENT_LOG_MAX_BYTES } from "./runtime.js";

function safeValue(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = String(value);
  return SAFE_VALUE_PATTERN.test(text) ? text : "redacted";
}

export function coordinationEvent(fields) {
  const event = {
    protocol: "cxmsg-event/1",
    timestamp: new Date().toISOString(),
    kind: safeValue(fields.kind),
    phase: safeValue(fields.phase),
    correlationId: safeValue(fields.correlationId),
    target: safeValue(fields.target),
    attempt: Number.isSafeInteger(fields.attempt) ? fields.attempt : null,
    outcome: safeValue(fields.outcome),
    errorCode: safeValue(fields.errorCode),
    returnErrorCode: safeValue(fields.returnErrorCode),
    denialOrigin: safeValue(fields.denialOrigin),
    late: fields.late === true,
  };
  return Object.fromEntries(
    Object.entries(event).filter(([, value]) => value !== null),
  );
}

function archivedEventLogPath(index) {
  return `${EVENT_LOG_PATH}.${index}`;
}

function validateOwnedStateDirectory() {
  const metadata = lstatSync(CXMSG_STATE_DIR);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error("cxmsg state path is not a real directory");
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error("cxmsg state directory is owned by another user");
  }
}

function validateOwnedEventFile(target) {
  if (!existsSync(target)) return;
  const metadata = lstatSync(target);
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid())
  ) {
    throw new Error("coordination event segment failed identity validation");
  }
}

function rotateEventLog(lineBytes, maxBytes, archives) {
  validateOwnedEventFile(EVENT_LOG_PATH);
  for (let index = 1; index <= archives; index += 1) {
    validateOwnedEventFile(archivedEventLogPath(index));
  }
  let currentBytes = 0;
  try {
    currentBytes = statSync(EVENT_LOG_PATH).size;
  } catch {}
  if (currentBytes + lineBytes <= maxBytes) return;

  if (archives < 1) {
    if (existsSync(EVENT_LOG_PATH)) unlinkSync(EVENT_LOG_PATH);
    return;
  }
  const oldest = archivedEventLogPath(archives);
  if (existsSync(oldest)) unlinkSync(oldest);
  for (let index = archives - 1; index >= 1; index -= 1) {
    const source = archivedEventLogPath(index);
    if (existsSync(source)) renameSync(source, archivedEventLogPath(index + 1));
  }
  if (existsSync(EVENT_LOG_PATH)) renameSync(EVENT_LOG_PATH, archivedEventLogPath(1));
}

export async function writeCoordinationEvent(
  fields,
  {
    maxBytes = EVENT_LOG_MAX_BYTES,
    archives = EVENT_LOG_ARCHIVES,
    lockTimeoutMs = 250,
  } = {},
) {
  try {
    mkdirSync(CXMSG_STATE_DIR, { recursive: true, mode: 0o700 });
    validateOwnedStateDirectory();
    chmodSync(CXMSG_STATE_DIR, 0o700);
    const event = coordinationEvent(fields);
    const line = `${JSON.stringify(event)}\n`;
    await withFileLock(EVENT_LOG_LOCK_PATH, async () => {
      rotateEventLog(Buffer.byteLength(line), maxBytes, archives);
      appendFileSync(EVENT_LOG_PATH, line, {
        encoding: "utf8",
        mode: 0o600,
      });
      chmodSync(EVENT_LOG_PATH, 0o600);
      for (let index = 1; index <= archives; index += 1) {
        const archived = archivedEventLogPath(index);
        if (existsSync(archived)) chmodSync(archived, 0o600);
      }
    }, {
      timeoutMs: lockTimeoutMs,
      leaseMs: 5_000,
    });
    return event;
  } catch {
    return null;
  }
}
