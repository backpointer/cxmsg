import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fchmodSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  statSync,
  writeSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const MAX_STORED_MESSAGE_BYTES = 256 * 1024;
export const DEFAULT_MESSAGE_READ_BYTES = 16 * 1024;
export const MAX_MESSAGE_READ_BYTES = 64 * 1024;
export const MESSAGE_BODY_SEGMENT_BYTES = 8 * 1024 * 1024;
export const MESSAGE_BODY_STORE_QUOTA_BYTES = 64 * 1024 * 1024;
export const MESSAGE_BODY_MAX_SCAN_BYTES = 256 * 1024 * 1024;

export const MESSAGE_BODIES_DIR = path.join(CXMSG_STATE_DIR, "message-bodies");
export const MESSAGE_BODY_SEGMENTS_DIR = path.join(MESSAGE_BODIES_DIR, "segments");
export const MESSAGE_BODY_QUARANTINE_DIR = path.join(
  MESSAGE_BODIES_DIR,
  "quarantine",
);
const MESSAGE_BODY_LOCK_PATH = path.join(MESSAGE_BODIES_DIR, "append.lock");
const SEGMENT_PATTERN = /^segment-(\d{8})(?:\.partial-[0-9a-f-]+)?\.jsonl$/i;

function validateMessageId(messageId) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(messageId || "")) {
    throw new Error("message-id must be a UUID");
  }
  return messageId;
}

function ensurePrivateDirectory(directory) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const metadata = lstatSync(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error(`message body path is not a directory: ${path.basename(directory)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`message body directory is owned by another user: ${path.basename(directory)}`);
  }
  chmodSync(directory, 0o700);
}

function ensureStore() {
  ensurePrivateDirectory(CXMSG_STATE_DIR);
  ensurePrivateDirectory(MESSAGE_BODIES_DIR);
  ensurePrivateDirectory(MESSAGE_BODY_SEGMENTS_DIR);
  ensurePrivateDirectory(MESSAGE_BODY_QUARANTINE_DIR);
}

function assertPrivateRegularFile(filename) {
  const metadata = lstatSync(filename);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`message body segment is not a regular file: ${path.basename(filename)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`message body segment is owned by another user: ${path.basename(filename)}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`message body segment permissions are too broad: ${path.basename(filename)}`);
  }
  return metadata;
}

function segmentPaths(directory) {
  return readdirSync(directory)
    .filter((filename) => SEGMENT_PATTERN.test(filename))
    .sort()
    .map((filename) => path.join(directory, filename));
}

function allSegmentPaths() {
  return [
    ...segmentPaths(MESSAGE_BODY_SEGMENTS_DIR),
    ...segmentPaths(MESSAGE_BODY_QUARANTINE_DIR),
  ];
}

function validRecord(record) {
  return Boolean(
    record &&
      record.schemaVersion === 1 &&
      record.recordType === "message-body" &&
      typeof record.messageId === "string" &&
      typeof record.createdAt === "string" &&
      Number.isSafeInteger(record.bodyBytes) &&
      record.bodyBytes > 0 &&
      record.bodyBytes <= MAX_STORED_MESSAGE_BYTES &&
      /^[0-9a-f]{64}$/.test(record.bodySha256 || "") &&
      typeof record.bodyBase64 === "string",
  );
}

function readSegmentRecords(filename) {
  assertPrivateRegularFile(filename);
  const raw = readFileSync(filename, "utf8");
  const complete = raw.endsWith("\n");
  const lines = raw.split("\n");
  if (!complete) lines.pop();
  else if (lines.at(-1) === "") lines.pop();
  return lines.map((line) => {
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      throw new Error(`malformed message body segment: ${path.basename(filename)}`);
    }
    if (!validRecord(record)) {
      throw new Error(`invalid message body record: ${path.basename(filename)}`);
    }
    return record;
  });
}

function listRecords(maxScanBytes = MESSAGE_BODY_MAX_SCAN_BYTES) {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const filenames = allSegmentPaths();
      const totalBytes = currentStoreBytes(filenames);
      if (totalBytes > maxScanBytes) {
        throw new Error("message body store exceeds the bounded scan limit");
      }
      return filenames.flatMap(readSegmentRecords);
    } catch (error) {
      if (error?.code !== "ENOENT" || attempt > 0) throw error;
    }
  }
  throw new Error("message body store could not be scanned");
}

function bodyReference(record) {
  return {
    contentRef: `cxmsg-message:${record.messageId}`,
    messageId: record.messageId,
    bodyBytes: record.bodyBytes,
    bodySha256: record.bodySha256,
    createdAt: record.createdAt,
  };
}

function currentStoreBytes(filenames = allSegmentPaths()) {
  return filenames.reduce(
    (total, filename) => total + assertPrivateRegularFile(filename).size,
    0,
  );
}

function nextSegmentPath() {
  const highest = allSegmentPaths().reduce((current, filename) => {
    const match = path.basename(filename).match(SEGMENT_PATTERN);
    return Math.max(current, Number.parseInt(match?.[1] || "0", 10));
  }, 0);
  return path.join(
    MESSAGE_BODY_SEGMENTS_DIR,
    `segment-${String(highest + 1).padStart(8, "0")}.jsonl`,
  );
}

function quarantinePartialSegment(filename) {
  const raw = readFileSync(filename);
  if (raw.length === 0 || raw.at(-1) === 0x0a) return false;
  const match = path.basename(filename).match(SEGMENT_PATTERN);
  const number = match?.[1] || "00000000";
  const destination = path.join(
    MESSAGE_BODY_QUARANTINE_DIR,
    `segment-${number}.partial-${randomUUID()}.jsonl`,
  );
  renameSync(filename, destination);
  return true;
}

function appendRecord(filename, encoded) {
  if (existsSync(filename)) assertPrivateRegularFile(filename);
  const flags =
    constants.O_WRONLY |
    constants.O_CREAT |
    constants.O_APPEND |
    (constants.O_NOFOLLOW || 0);
  const descriptor = openSync(filename, flags, 0o600);
  try {
    const metadata = fstatSync(descriptor);
    if (!metadata.isFile()) throw new Error("message body segment is not a regular file");
    if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
      throw new Error("message body segment is owned by another user");
    }
    fchmodSync(descriptor, 0o600);
    const bytes = Buffer.from(encoded, "utf8");
    let offset = 0;
    while (offset < bytes.length) {
      offset += writeSync(descriptor, bytes, offset, bytes.length - offset);
    }
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

export function parseMessageContentRef(value) {
  const messageId = String(value || "").startsWith("cxmsg-message:")
    ? String(value).slice("cxmsg-message:".length)
    : String(value || "");
  return validateMessageId(messageId);
}

export async function storeMessageBody(
  { messageId, body },
  {
    quotaBytes = MESSAGE_BODY_STORE_QUOTA_BYTES,
    segmentBytes = MESSAGE_BODY_SEGMENT_BYTES,
  } = {},
) {
  validateMessageId(messageId);
  if (typeof body !== "string" || !body.trim()) {
    throw new Error("message body must not be empty");
  }
  const bodyBytes = Buffer.byteLength(body, "utf8");
  if (bodyBytes > MAX_STORED_MESSAGE_BYTES) {
    throw new Error(`message exceeds stored body limit of ${MAX_STORED_MESSAGE_BYTES} bytes`);
  }
  if (!Number.isSafeInteger(quotaBytes) || quotaBytes < MAX_STORED_MESSAGE_BYTES) {
    throw new Error("message body quota is invalid");
  }
  if (!Number.isSafeInteger(segmentBytes) || segmentBytes < MAX_STORED_MESSAGE_BYTES * 2) {
    throw new Error("message body segment size is invalid");
  }

  ensureStore();
  return withFileLock(MESSAGE_BODY_LOCK_PATH, async () => {
    const bodySha256 = createHash("sha256").update(body).digest("hex");
    const existing = listRecords().find(
      (record) => record.messageId === messageId,
    );
    if (existing) {
      if (existing.bodyBytes !== bodyBytes || existing.bodySha256 !== bodySha256) {
        throw new Error(`message body idempotency conflict: ${messageId}`);
      }
      return bodyReference(existing);
    }

    const record = {
      schemaVersion: 1,
      recordType: "message-body",
      messageId,
      createdAt: new Date().toISOString(),
      bodyBytes,
      bodySha256,
      bodyBase64: Buffer.from(body, "utf8").toString("base64"),
    };
    const encoded = `${JSON.stringify(record)}\n`;
    const encodedBytes = Buffer.byteLength(encoded, "utf8");
    if (currentStoreBytes() + encodedBytes > quotaBytes) {
      throw new Error("message body store quota exceeded; no content was deleted");
    }

    const active = segmentPaths(MESSAGE_BODY_SEGMENTS_DIR).at(-1) || null;
    let destination = active;
    if (destination && quarantinePartialSegment(destination)) destination = null;
    if (
      !destination ||
      statSync(destination).size + encodedBytes > segmentBytes
    ) {
      destination = nextSegmentPath();
    }
    appendRecord(destination, encoded);
    return bodyReference(record);
  });
}

function verifiedBody(record) {
  const body = Buffer.from(record.bodyBase64, "base64");
  const digest = createHash("sha256").update(body).digest("hex");
  if (body.length !== record.bodyBytes || digest !== record.bodySha256) {
    throw new Error(`message body integrity check failed: ${record.messageId}`);
  }
  return body;
}

export function messageBodyInfo(reference) {
  const messageId = parseMessageContentRef(reference);
  ensureStore();
  const record = listRecords().find((candidate) => candidate.messageId === messageId);
  if (!record) throw new Error(`unknown message body: ${messageId}`);
  verifiedBody(record);
  return bodyReference(record);
}

export function listMessageBodies() {
  ensureStore();
  return listRecords().map((record) => {
    verifiedBody(record);
    return bodyReference(record);
  });
}

export function readMessageBody(
  reference,
  { offset = 0, limit = DEFAULT_MESSAGE_READ_BYTES } = {},
) {
  const messageId = parseMessageContentRef(reference);
  if (!Number.isSafeInteger(offset) || offset < 0) {
    throw new Error("message body offset must be a non-negative integer");
  }
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_MESSAGE_READ_BYTES) {
    throw new Error(`message body limit must be 1-${MAX_MESSAGE_READ_BYTES} bytes`);
  }
  ensureStore();
  const record = listRecords().find((candidate) => candidate.messageId === messageId);
  if (!record) throw new Error(`unknown message body: ${messageId}`);
  const body = verifiedBody(record);
  let start = Math.min(offset, body.length);
  while (start < body.length && (body[start] & 0xc0) === 0x80) start += 1;
  let end = Math.min(start + limit, body.length);
  while (end < body.length && (body[end] & 0xc0) === 0x80) end += 1;
  return {
    ...bodyReference(record),
    requestedOffset: offset,
    offset: start,
    nextOffset: end,
    complete: end >= body.length,
    text: body.subarray(start, end).toString("utf8"),
  };
}
