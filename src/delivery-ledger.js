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
import {
  MAX_WHEN_IDLE_DELAY_MS,
  SCHEDULED_DELIVERY_PER_TARGET_LIMIT,
} from "./delivery-policy.js";
import { withFileLock } from "./file-lock.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const DELIVERY_LEDGER_SEGMENT_BYTES = 8 * 1024 * 1024;
export const DELIVERY_LEDGER_QUOTA_BYTES = 64 * 1024 * 1024;
export const DELIVERY_LEDGER_MAX_SCAN_BYTES = 256 * 1024 * 1024;
export const DELIVERY_LEDGER_MAX_RECORD_BYTES = 256 * 1024;
export const DELIVERY_LEDGER_EVENT_RESERVE_BYTES = 4 * 1024;
export { SCHEDULED_DELIVERY_PER_TARGET_LIMIT };

export const DELIVERY_LEDGER_DIR = path.join(CXMSG_STATE_DIR, "delivery-ledger");
export const DELIVERY_LEDGER_SEGMENTS_DIR = path.join(
  DELIVERY_LEDGER_DIR,
  "segments",
);
export const DELIVERY_LEDGER_QUARANTINE_DIR = path.join(
  DELIVERY_LEDGER_DIR,
  "quarantine",
);
const DELIVERY_LEDGER_LOCK_PATH = path.join(DELIVERY_LEDGER_DIR, "append.lock");
const SEGMENT_PATTERN = /^segment-(\d{8})(?:\.partial-[0-9a-f-]+)?\.jsonl$/i;
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const CLAIM_RELEASE_REASONS = new Set(["target_busy", "worker_stopping", "dispatch_unavailable"]);

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value;
}

function ensurePrivateDirectory(directory) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const metadata = lstatSync(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error(`Delivery Ledger path is not a directory: ${path.basename(directory)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Delivery Ledger directory is owned by another user: ${path.basename(directory)}`);
  }
  chmodSync(directory, 0o700);
}

function ensureStore() {
  ensurePrivateDirectory(CXMSG_STATE_DIR);
  ensurePrivateDirectory(DELIVERY_LEDGER_DIR);
  ensurePrivateDirectory(DELIVERY_LEDGER_SEGMENTS_DIR);
  ensurePrivateDirectory(DELIVERY_LEDGER_QUARANTINE_DIR);
}

function assertPrivateRegularFile(filename) {
  const metadata = lstatSync(filename);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`Delivery Ledger segment is not a regular file: ${path.basename(filename)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Delivery Ledger segment is owned by another user: ${path.basename(filename)}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`Delivery Ledger segment permissions are too broad: ${path.basename(filename)}`);
  }
  return metadata;
}

function segmentPaths(directory) {
  if (!existsSync(directory)) return [];
  return readdirSync(directory)
    .filter((filename) => SEGMENT_PATTERN.test(filename))
    .sort()
    .map((filename) => path.join(directory, filename));
}

function allSegmentPaths() {
  return [
    ...segmentPaths(DELIVERY_LEDGER_SEGMENTS_DIR),
    ...segmentPaths(DELIVERY_LEDGER_QUARANTINE_DIR),
  ];
}

function validTimestamp(value) {
  return Number.isFinite(Date.parse(value || ""));
}

function validBody(body, messageId) {
  return Boolean(
    body?.messageId === messageId &&
      Number.isSafeInteger(body.bytes) &&
      body.bytes > 0 &&
      SHA256_PATTERN.test(body.sha256 || "") &&
      (body.contentRef === null || body.contentRef === `cxmsg-message:${messageId}`),
  );
}

function validBatch(record) {
  const message = record?.logicalMessage;
  const delivery = record?.deliveries?.[0];
  return Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "ledger-batch" &&
      UUID_PATTERN.test(record.batchId || "") &&
      validTimestamp(record.committedAt) &&
      message?.messageId &&
      UUID_PATTERN.test(message.messageId) &&
      NAME_PATTERN.test(message.from || "") &&
      validBody(message.body, message.messageId) &&
      typeof message.routeFingerprint === "string" &&
      SHA256_PATTERN.test(message.routeFingerprint) &&
      message.routeFingerprint ===
        createHash("sha256").update(JSON.stringify(message.route ?? null)).digest("hex") &&
      validTimestamp(message.createdAt) &&
      Array.isArray(record.deliveries) &&
      record.deliveries.length === 1 &&
      UUID_PATTERN.test(delivery?.deliveryId || "") &&
      NAME_PATTERN.test(delivery?.target || "") &&
      (delivery.targetThreadId === null || UUID_PATTERN.test(delivery.targetThreadId || "")) &&
      ["admitted", "quarantined"].includes(delivery.admissionState) &&
      typeof delivery.admissionReason === "string" &&
      ["immediate", "when-idle"].includes(delivery.wakePolicy) &&
      delivery.state === (delivery.wakePolicy === "immediate" ? "created" : "scheduled") &&
      (delivery.wakePolicy === "immediate" ||
        (message.body.contentRef === `cxmsg-message:${message.messageId}` &&
          message.route?.wake_policy === "when-idle" &&
          validTimestamp(message.route.expiry) &&
          Date.parse(message.route.expiry) > Date.parse(message.createdAt) &&
          Date.parse(message.route.expiry) - Date.parse(message.createdAt) <=
            MAX_WHEN_IDLE_DELAY_MS)) &&
      validTimestamp(delivery.createdAt) &&
      delivery.createdAt === delivery.updatedAt
  );
}

function validAttempt(record) {
  return Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "delivery-attempt" &&
      UUID_PATTERN.test(record.messageId || "") &&
      UUID_PATTERN.test(record.deliveryId || "") &&
      UUID_PATTERN.test(record.attemptId || "") &&
      (record.claimId === undefined ||
        record.claimId === null ||
        UUID_PATTERN.test(record.claimId || "")) &&
      record.transport === "codex-app-server" &&
      validTimestamp(record.startedAt),
  );
}

function validClaim(record) {
  if (
    record?.schemaVersion !== 1 ||
    record.recordType !== "delivery-claim" ||
    !UUID_PATTERN.test(record.messageId || "") ||
    !UUID_PATTERN.test(record.deliveryId || "") ||
    !UUID_PATTERN.test(record.claimId || "") ||
    !UUID_PATTERN.test(record.workerId || "")
  ) {
    return false;
  }
  if (record.action === "acquired") {
    return Boolean(
      validTimestamp(record.claimedAt) &&
        validTimestamp(record.leaseUntil) &&
        Date.parse(record.leaseUntil) > Date.parse(record.claimedAt),
    );
  }
  return Boolean(
    record.action === "released" &&
      CLAIM_RELEASE_REASONS.has(record.reason) &&
      validTimestamp(record.releasedAt),
  );
}

function validEvidence(record) {
  const common = Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "delivery-evidence" &&
      UUID_PATTERN.test(record.messageId || "") &&
      UUID_PATTERN.test(record.deliveryId || "") &&
      (record.attemptId === null || UUID_PATTERN.test(record.attemptId || "")) &&
      ["turn_started", "unknown", "expired"].includes(record.state) &&
      ["dispatch-result", "reconciliation", "scheduler"].includes(record.evidenceKind) &&
      (record.turnId === null || UUID_PATTERN.test(record.turnId || "")) &&
      (record.transportResult === null || typeof record.transportResult === "string") &&
      (record.errorCode === null || /^[A-Z0-9_]{1,32}$/.test(record.errorCode || "")) &&
      validTimestamp(record.observedAt)
  );
  if (!common) return false;
  if (record.evidenceKind === "scheduler") {
    return Boolean(
      ["unknown", "expired"].includes(record.state) &&
        record.attemptId === null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode,
    );
  }
  return ["turn_started", "unknown"].includes(record.state);
}

export function validDeliveryLedgerRecord(record) {
  if (record?.recordType === "ledger-batch") return validBatch(record);
  if (record?.recordType === "delivery-claim") return validClaim(record);
  if (record?.recordType === "delivery-attempt") return validAttempt(record);
  if (record?.recordType === "delivery-evidence") return validEvidence(record);
  return false;
}

function readSegmentRecords(filename) {
  assertPrivateRegularFile(filename);
  const raw = readFileSync(filename, "utf8");
  const complete = raw.endsWith("\n");
  const lines = raw.split("\n");
  if (!complete) lines.pop();
  else if (lines.at(-1) === "") lines.pop();
  return lines.map((line) => {
    if (Buffer.byteLength(line, "utf8") > DELIVERY_LEDGER_MAX_RECORD_BYTES) {
      throw new Error(`Delivery Ledger record is too large: ${path.basename(filename)}`);
    }
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      throw new Error(`malformed Delivery Ledger segment: ${path.basename(filename)}`);
    }
    if (!validDeliveryLedgerRecord(record)) {
      throw new Error(`invalid Delivery Ledger record: ${path.basename(filename)}`);
    }
    return record;
  });
}

function currentStoreBytes(filenames = allSegmentPaths()) {
  return filenames.reduce(
    (total, filename) => total + assertPrivateRegularFile(filename).size,
    0,
  );
}

function readRecords(maxScanBytes = DELIVERY_LEDGER_MAX_SCAN_BYTES) {
  if (!existsSync(DELIVERY_LEDGER_DIR)) return [];
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const filenames = allSegmentPaths();
      if (currentStoreBytes(filenames) > maxScanBytes) {
        throw new Error("Delivery Ledger exceeds the bounded scan limit");
      }
      return filenames.flatMap(readSegmentRecords);
    } catch (error) {
      if (error?.code !== "ENOENT" || attempt > 0) throw error;
    }
  }
  throw new Error("Delivery Ledger could not be scanned");
}

function nextSegmentPath() {
  const highest = allSegmentPaths().reduce((current, filename) => {
    const match = path.basename(filename).match(SEGMENT_PATTERN);
    return Math.max(current, Number.parseInt(match?.[1] || "0", 10));
  }, 0);
  return path.join(
    DELIVERY_LEDGER_SEGMENTS_DIR,
    `segment-${String(highest + 1).padStart(8, "0")}.jsonl`,
  );
}

function quarantinePartialSegment(filename) {
  const raw = readFileSync(filename);
  if (raw.length === 0 || raw.at(-1) === 0x0a) return false;
  const match = path.basename(filename).match(SEGMENT_PATTERN);
  const number = match?.[1] || "00000000";
  renameSync(
    filename,
    path.join(
      DELIVERY_LEDGER_QUARANTINE_DIR,
      `segment-${number}.partial-${randomUUID()}.jsonl`,
    ),
  );
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
    if (!metadata.isFile()) throw new Error("Delivery Ledger segment is not a regular file");
    if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
      throw new Error("Delivery Ledger segment is owned by another user");
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

function appendLedgerRecord(record, { quotaBytes, segmentBytes, reserveBytes = 0 }) {
  const encoded = `${JSON.stringify(record)}\n`;
  const encodedBytes = Buffer.byteLength(encoded, "utf8");
  if (encodedBytes > DELIVERY_LEDGER_MAX_RECORD_BYTES) {
    throw new Error("Delivery Ledger record exceeds the bounded record limit");
  }
  if (
    record.recordType !== "ledger-batch" &&
    encodedBytes > DELIVERY_LEDGER_EVENT_RESERVE_BYTES
  ) {
    throw new Error("Delivery Ledger evidence exceeds its reserved record size");
  }
  if (currentStoreBytes() + encodedBytes + reserveBytes > quotaBytes) {
    throw new Error("Delivery Ledger quota exceeded; no evidence was deleted");
  }
  const active = segmentPaths(DELIVERY_LEDGER_SEGMENTS_DIR).at(-1) || null;
  let destination = active;
  if (destination && quarantinePartialSegment(destination)) destination = null;
  if (!destination || statSync(destination).size + encodedBytes > segmentBytes) {
    destination = nextSegmentPath();
  }
  appendRecord(destination, encoded);
}

function reservedEvidenceBytes(messages) {
  let records = 0;
  for (const message of messages.values()) {
    const delivery = message.delivery;
    if (delivery.admissionState !== "admitted") continue;
    if (delivery.state === "turn_started") continue;
    if (delivery.state === "unknown") {
      records += 1;
      continue;
    }
    records += delivery.attempts.length > 0 ? 2 : 3;
  }
  return records * DELIVERY_LEDGER_EVENT_RESERVE_BYTES;
}

function cloneBatch(record) {
  return {
    schemaVersion: record.schemaVersion,
    batchId: record.batchId,
    committedAt: record.committedAt,
    logicalMessage: structuredClone(record.logicalMessage),
    delivery: {
      ...structuredClone(record.deliveries[0]),
      attempts: [],
      evidence: [],
      claim: null,
      claimCount: 0,
    },
  };
}

export function rebuildDeliveryLedgerRecords(records) {
  const messages = new Map();
  const deliveryIds = new Map();
  const batchIds = new Set();
  for (const record of records) {
    if (record.recordType === "ledger-batch") {
      const messageId = record.logicalMessage.messageId;
      if (
        messages.has(messageId) ||
        deliveryIds.has(record.deliveries[0].deliveryId) ||
        batchIds.has(record.batchId)
      ) {
        throw new Error(`duplicate Delivery Ledger batch identity: ${messageId}`);
      }
      const projected = cloneBatch(record);
      messages.set(messageId, projected);
      deliveryIds.set(projected.delivery.deliveryId, projected);
      batchIds.add(record.batchId);
      continue;
    }
    const projected = messages.get(record.messageId);
    if (!projected || projected.delivery.deliveryId !== record.deliveryId) {
      throw new Error(`orphaned Delivery Ledger record: ${record.messageId}`);
    }
    if (projected.delivery.admissionState !== "admitted") {
      throw new Error(`quarantined Delivery has transport evidence: ${record.messageId}`);
    }
    if (record.recordType === "delivery-claim") {
      if (projected.delivery.wakePolicy !== "when-idle" || projected.delivery.state !== "scheduled") {
        throw new Error(`Delivery claim is not scheduled: ${record.messageId}`);
      }
      if (projected.delivery.attempts.length > 0) {
        throw new Error(`Delivery claim follows a dispatch attempt: ${record.messageId}`);
      }
      if (record.action === "acquired") {
        if (
          projected.delivery.claim &&
          Date.parse(record.claimedAt) < Date.parse(projected.delivery.claim.leaseUntil)
        ) {
          throw new Error(`overlapping Delivery claim: ${record.messageId}`);
        }
        projected.delivery.claim = structuredClone(record);
        projected.delivery.claimCount += 1;
        projected.delivery.updatedAt = record.claimedAt;
        continue;
      }
      if (
        !projected.delivery.claim ||
        projected.delivery.claim.claimId !== record.claimId ||
        projected.delivery.claim.workerId !== record.workerId ||
        Date.parse(record.releasedAt) < Date.parse(projected.delivery.claim.claimedAt)
      ) {
        throw new Error(`Delivery claim release has no owner: ${record.messageId}`);
      }
      projected.delivery.claim = null;
      projected.delivery.updatedAt = record.releasedAt;
      continue;
    }
    if (record.recordType === "delivery-attempt") {
      const immediate =
        projected.delivery.wakePolicy === "immediate" &&
        projected.delivery.state === "created" &&
        (record.claimId === undefined || record.claimId === null);
      const scheduled =
        projected.delivery.wakePolicy === "when-idle" &&
        projected.delivery.state === "scheduled" &&
        projected.delivery.claim?.claimId === record.claimId;
      if ((!immediate && !scheduled) || projected.delivery.attempts.length > 0) {
        throw new Error(`duplicate Delivery attempt: ${record.messageId}`);
      }
      projected.delivery.attempts.push(structuredClone(record));
      projected.delivery.updatedAt = record.startedAt;
      continue;
    }
    if (record.attemptId !== null) {
      const attempt = projected.delivery.attempts.find(
        (candidate) => candidate.attemptId === record.attemptId,
      );
      if (!attempt) throw new Error(`Delivery evidence has no attempt: ${record.messageId}`);
    }
    const current = projected.delivery.state;
    if (
      !(
        (["created", "scheduled"].includes(current) && ["turn_started", "unknown"].includes(record.state)) ||
        (current === "scheduled" && record.state === "expired") ||
        (current === "unknown" && ["turn_started", "unknown"].includes(record.state)) ||
        (current === "turn_started" && record.state === "turn_started") ||
        (current === "expired" && record.state === "expired")
      )
    ) {
      throw new Error(`invalid Delivery evidence transition: ${current}->${record.state}`);
    }
    projected.delivery.state = record.state;
    projected.delivery.turnId = record.turnId;
    projected.delivery.transportResult = record.transportResult;
    projected.delivery.errorCode = record.errorCode;
    projected.delivery.updatedAt = record.observedAt;
    projected.delivery.evidence.push(structuredClone(record));
  }
  return messages;
}

function ledgerState(messageId) {
  validateUuid("logical message id", messageId);
  return rebuildDeliveryLedgerRecords(readRecords()).get(messageId) || null;
}

function validateStorageOptions(quotaBytes, segmentBytes) {
  if (!Number.isSafeInteger(quotaBytes) || quotaBytes < DELIVERY_LEDGER_MAX_RECORD_BYTES) {
    throw new Error("Delivery Ledger quota is invalid");
  }
  if (!Number.isSafeInteger(segmentBytes) || segmentBytes < DELIVERY_LEDGER_MAX_RECORD_BYTES * 2) {
    throw new Error("Delivery Ledger segment size is invalid");
  }
}

export function readDeliveryLedger(messageId) {
  return ledgerState(messageId);
}

export function listDeliveryLedger() {
  return [...rebuildDeliveryLedgerRecords(readRecords()).values()].sort((left, right) =>
    left.committedAt.localeCompare(right.committedAt),
  );
}

export async function commitSingleRecipientDelivery(
  {
    logicalMessage,
    target,
    targetThreadId = null,
    admissionState,
    admissionReason,
    wakePolicy = "immediate",
    now = new Date().toISOString(),
  },
  {
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
    scheduledPerTargetLimit = SCHEDULED_DELIVERY_PER_TARGET_LIMIT,
  } = {},
) {
  validateStorageOptions(quotaBytes, segmentBytes);
  if (
    !Number.isSafeInteger(scheduledPerTargetLimit) ||
    scheduledPerTargetLimit < 1 ||
    scheduledPerTargetLimit > SCHEDULED_DELIVERY_PER_TARGET_LIMIT
  ) {
    throw new Error("scheduled Delivery per-target limit is invalid");
  }
  validateUuid("logical message id", logicalMessage?.messageId);
  if (!NAME_PATTERN.test(logicalMessage?.from || "")) throw new Error("invalid Ledger sender");
  if (!NAME_PATTERN.test(target || "")) throw new Error("invalid Ledger target");
  if (targetThreadId !== null) validateUuid("target thread id", targetThreadId);
  if (!["admitted", "quarantined"].includes(admissionState)) {
    throw new Error("invalid Delivery admission state");
  }
  if (!["immediate", "when-idle"].includes(wakePolicy)) {
    throw new Error("Ledger v1 supports only immediate or when-idle wake");
  }
  if (!validTimestamp(now)) throw new Error("invalid Delivery timestamp");

  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const messages = rebuildDeliveryLedgerRecords(readRecords());
    const existing = messages.get(logicalMessage.messageId);
    if (existing) {
      if (
        JSON.stringify(existing.logicalMessage) !== JSON.stringify(logicalMessage) ||
        existing.delivery.target !== target ||
        existing.delivery.targetThreadId !== targetThreadId ||
        existing.delivery.admissionState !== admissionState ||
        existing.delivery.admissionReason !== admissionReason ||
        existing.delivery.wakePolicy !== wakePolicy
      ) {
        throw new Error(`Delivery Ledger idempotency conflict: ${logicalMessage.messageId}`);
      }
      return { record: existing, created: false };
    }
    if (
      wakePolicy === "when-idle" &&
      [...messages.values()].filter(
        (message) =>
          (message.delivery.targetThreadId || message.delivery.target) ===
            (targetThreadId || target) &&
          message.delivery.wakePolicy === "when-idle" &&
          message.delivery.state === "scheduled" &&
          message.delivery.attempts.length === 0,
      ).length >= scheduledPerTargetLimit
    ) {
      throw new Error(
        `scheduled Delivery queue for ${target} reached ${scheduledPerTargetLimit}`,
      );
    }
    const record = {
      schemaVersion: 1,
      recordType: "ledger-batch",
      batchId: randomUUID(),
      committedAt: now,
      logicalMessage: structuredClone(logicalMessage),
      deliveries: [
        {
          deliveryId: randomUUID(),
          target,
          targetThreadId,
          admissionState,
          admissionReason,
          wakePolicy,
          state: wakePolicy === "immediate" ? "created" : "scheduled",
          createdAt: now,
          updatedAt: now,
        },
      ],
    };
    if (!validBatch(record)) throw new Error("invalid Delivery Ledger batch");
    const projected = cloneBatch(record);
    const after = new Map(messages);
    after.set(logicalMessage.messageId, projected);
    appendLedgerRecord(record, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    return { record: projected, created: true };
  });
}

export async function beginImmediateDelivery(messageId, options = {}) {
  const quotaBytes = options.quotaBytes ?? DELIVERY_LEDGER_QUOTA_BYTES;
  const segmentBytes = options.segmentBytes ?? DELIVERY_LEDGER_SEGMENT_BYTES;
  validateStorageOptions(quotaBytes, segmentBytes);
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const record = ledgerState(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    if (record.delivery.admissionState !== "admitted") {
      throw new Error("quarantined Delivery cannot be dispatched");
    }
    if (
      record.delivery.wakePolicy !== "immediate" ||
      record.delivery.state !== "created" ||
      record.delivery.attempts.length > 0
    ) {
      throw new Error("Delivery already has a dispatch attempt");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-attempt",
      messageId,
      deliveryId: record.delivery.deliveryId,
      attemptId: randomUUID(),
      claimId: null,
      transport: "codex-app-server",
      startedAt: options.now || new Date().toISOString(),
    };
    if (!validAttempt(event)) throw new Error("invalid Delivery attempt");
    const after = rebuildDeliveryLedgerRecords([...readRecords(), event]);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    return structuredClone(event);
  });
}

export async function claimScheduledDelivery(
  messageId,
  {
    workerId,
    leaseMs = 30_000,
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateUuid("scheduler worker id", workerId);
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1_000 || leaseMs > 300_000) {
    throw new Error("Delivery claim lease must be 1000-300000 milliseconds");
  }
  if (!validTimestamp(now)) throw new Error("invalid Delivery claim timestamp");
  validateStorageOptions(quotaBytes, segmentBytes);
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const record = ledgerState(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    if (
      record.delivery.admissionState !== "admitted" ||
      record.delivery.wakePolicy !== "when-idle" ||
      record.delivery.state !== "scheduled" ||
      record.delivery.attempts.length > 0
    ) {
      throw new Error("Delivery is not claimable for when-idle dispatch");
    }
    const claimedAt = Date.parse(now);
    if (record.delivery.claim && Date.parse(record.delivery.claim.leaseUntil) > claimedAt) {
      return { claim: structuredClone(record.delivery.claim), acquired: false };
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-claim",
      action: "acquired",
      messageId,
      deliveryId: record.delivery.deliveryId,
      claimId: randomUUID(),
      workerId,
      claimedAt: now,
      leaseUntil: new Date(claimedAt + leaseMs).toISOString(),
    };
    if (!validClaim(event)) throw new Error("invalid Delivery claim");
    const after = rebuildDeliveryLedgerRecords([...readRecords(), event]);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    return { claim: structuredClone(event), acquired: true };
  });
}

export async function releaseScheduledDeliveryClaim(
  messageId,
  {
    claimId,
    workerId,
    reason,
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateUuid("Delivery claim id", claimId);
  validateUuid("scheduler worker id", workerId);
  if (!CLAIM_RELEASE_REASONS.has(reason)) throw new Error("invalid Delivery claim release reason");
  if (!validTimestamp(now)) throw new Error("invalid Delivery claim release timestamp");
  validateStorageOptions(quotaBytes, segmentBytes);
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const record = ledgerState(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    if (!record.delivery.claim) return { released: false, record };
    if (
      record.delivery.claim.claimId !== claimId ||
      record.delivery.claim.workerId !== workerId
    ) {
      throw new Error("Delivery claim is owned by another scheduler worker");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-claim",
      action: "released",
      messageId,
      deliveryId: record.delivery.deliveryId,
      claimId,
      workerId,
      reason,
      releasedAt: now,
    };
    const after = rebuildDeliveryLedgerRecords([...readRecords(), event]);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    return { released: true, record: after.get(messageId) };
  });
}

export async function beginScheduledDelivery(
  messageId,
  {
    claimId,
    workerId,
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateUuid("Delivery claim id", claimId);
  validateUuid("scheduler worker id", workerId);
  if (!validTimestamp(now)) throw new Error("invalid scheduled Delivery timestamp");
  validateStorageOptions(quotaBytes, segmentBytes);
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const record = ledgerState(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    if (
      record.delivery.admissionState !== "admitted" ||
      record.delivery.wakePolicy !== "when-idle" ||
      record.delivery.state !== "scheduled" ||
      record.delivery.attempts.length > 0 ||
      record.delivery.claim?.claimId !== claimId ||
      record.delivery.claim?.workerId !== workerId ||
      Date.parse(record.delivery.claim.leaseUntil) <= Date.parse(now)
    ) {
      throw new Error("scheduled Delivery claim is missing, expired, or already dispatched");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-attempt",
      messageId,
      deliveryId: record.delivery.deliveryId,
      attemptId: randomUUID(),
      claimId,
      transport: "codex-app-server",
      startedAt: now,
    };
    const after = rebuildDeliveryLedgerRecords([...readRecords(), event]);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    return structuredClone(event);
  });
}

export async function appendDeliveryEvidence(
  messageId,
  {
    attemptId = null,
    state,
    evidenceKind,
    turnId = null,
    transportResult = null,
    errorCode = null,
    observedAt = new Date().toISOString(),
  },
  options = {},
) {
  const quotaBytes = options.quotaBytes ?? DELIVERY_LEDGER_QUOTA_BYTES;
  const segmentBytes = options.segmentBytes ?? DELIVERY_LEDGER_SEGMENT_BYTES;
  validateStorageOptions(quotaBytes, segmentBytes);
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const record = ledgerState(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    const event = {
      schemaVersion: 1,
      recordType: "delivery-evidence",
      messageId,
      deliveryId: record.delivery.deliveryId,
      attemptId,
      state,
      evidenceKind,
      turnId,
      transportResult,
      errorCode,
      observedAt,
    };
    if (!validEvidence(event)) throw new Error("invalid Delivery evidence");
    if (
      record.delivery.state === state &&
      record.delivery.errorCode === errorCode &&
      record.delivery.turnId === turnId &&
      record.delivery.transportResult === transportResult &&
      record.delivery.evidence.at(-1)?.evidenceKind === evidenceKind
    ) {
      return record;
    }
    const after = rebuildDeliveryLedgerRecords([...readRecords(), event]);
    const projected = after.get(messageId);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    return projected;
  });
}
