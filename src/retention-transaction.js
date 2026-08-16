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
import { requireNoFollowFlag } from "./file-safety.js";
import {
  createDeliveryDedupTombstones,
  DELIVERY_LEDGER_MAX_SCAN_BYTES,
  DELIVERY_LEDGER_QUARANTINE_DIR,
  DELIVERY_LEDGER_SEGMENT_BYTES,
  DELIVERY_LEDGER_SEGMENTS_DIR,
  rebuildDeliveryLedgerIndex,
  validDeliveryLedgerRecord,
} from "./delivery-ledger.js";
import {
  MESSAGE_BODY_QUARANTINE_DIR,
  MESSAGE_BODY_MAX_SCAN_BYTES,
  MESSAGE_BODY_SEGMENT_BYTES,
  MESSAGE_BODY_SEGMENTS_DIR,
  validMessageBodyRecord,
} from "./message-bodies.js";
import { buildRetentionPlan } from "./retention.js";
import {
  RETENTION_STATE_DIR,
  withRetentionMutation,
} from "./retention-barrier.js";
import {
  QUARANTINE_DIR,
  readQuarantineSnapshotForRetention,
} from "./route-admission.js";

export const RETENTION_TRANSACTIONS_DIR = path.join(
  RETENTION_STATE_DIR,
  "transactions",
);
export const RETENTION_RECEIPTS_DIR = path.join(RETENTION_STATE_DIR, "receipts");
export const RETENTION_HEAD_PATH = path.join(RETENTION_STATE_DIR, "head.json");

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const OPERATION_KINDS = new Set(["ledger", "bodies", "quarantine"]);

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value;
}

function ensurePrivateDirectory(directory) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const metadata = lstatSync(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error(`Retention transaction path is not a directory: ${path.basename(directory)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Retention transaction directory is owned by another user: ${path.basename(directory)}`);
  }
  chmodSync(directory, 0o700);
}

function assertPrivateDirectory(directory) {
  const metadata = lstatSync(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error(`Retention generation is not a directory: ${path.basename(directory)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Retention generation is owned by another user: ${path.basename(directory)}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`Retention generation permissions are too broad: ${path.basename(directory)}`);
  }
  return metadata;
}

function assertPrivateFile(filename) {
  const metadata = lstatSync(filename);
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.nlink !== 1) {
    throw new Error(`Retention record is not a private regular file: ${path.basename(filename)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Retention record is owned by another user: ${path.basename(filename)}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`Retention record permissions are too broad: ${path.basename(filename)}`);
  }
  return metadata;
}

function fsyncDirectory(directory) {
  const descriptor = openSync(directory, constants.O_RDONLY);
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

function fsyncFile(filename) {
  const descriptor = openSync(
    filename,
    constants.O_RDONLY | requireNoFollowFlag(),
  );
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

function atomicWriteJson(filename, value) {
  ensurePrivateDirectory(path.dirname(filename));
  const temporary = `${filename}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  fsyncFile(temporary);
  renameSync(temporary, filename);
  fsyncDirectory(path.dirname(filename));
  return value;
}

function readPrivateJson(filename, label) {
  assertPrivateFile(filename);
  let value;
  try {
    value = JSON.parse(readFileSync(filename, "utf8"));
  } catch {
    throw new Error(`${label} is malformed`);
  }
  return value;
}

function transactionDirectory(backupId) {
  return path.join(
    RETENTION_TRANSACTIONS_DIR,
    validateUuid("Retention backup id", backupId),
  );
}

function manifestPath(backupId) {
  return path.join(transactionDirectory(backupId), "manifest.json");
}

function receiptPath(backupId) {
  return path.join(
    RETENTION_RECEIPTS_DIR,
    `${validateUuid("Retention backup id", backupId)}.json`,
  );
}

function operationPaths(backupId, kind) {
  if (!OPERATION_KINDS.has(kind)) throw new Error(`invalid Retention operation: ${kind}`);
  const root = transactionDirectory(backupId);
  const active = kind === "ledger"
    ? DELIVERY_LEDGER_SEGMENTS_DIR
    : kind === "bodies"
      ? MESSAGE_BODY_SEGMENTS_DIR
      : QUARANTINE_DIR;
  return {
    active,
    stage: path.join(root, `stage-${kind}`),
    backup: path.join(root, `backup-${kind}`),
  };
}

function directoryGeneration(directory) {
  assertPrivateDirectory(directory);
  const entries = readdirSync(directory).sort();
  const manifest = entries.map((name) => {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
      throw new Error(`Retention generation filename is invalid: ${name}`);
    }
    const filename = path.join(directory, name);
    const metadata = assertPrivateFile(filename);
    const content = readFileSync(filename);
    return { name, size: metadata.size, sha256: sha256(content) };
  });
  return {
    fileCount: manifest.length,
    bytes: manifest.reduce((total, entry) => total + entry.size, 0),
    sha256: sha256(JSON.stringify(manifest)),
  };
}

function ensureEmptyInternalQuarantine(directory, label) {
  ensurePrivateDirectory(directory);
  const names = readdirSync(directory);
  if (names.length > 0) {
    throw new Error(`${label} contains quarantined or partial segments; purge is blocked`);
  }
}

function strictJsonlRecords(directory, validator, label, maxScanBytes) {
  assertPrivateDirectory(directory);
  const records = [];
  let scannedBytes = 0;
  for (const name of readdirSync(directory).sort()) {
    if (!/^segment-\d{8}\.jsonl$/.test(name)) {
      throw new Error(`${label} segment filename is invalid: ${name}`);
    }
    const filename = path.join(directory, name);
    const metadata = assertPrivateFile(filename);
    scannedBytes += metadata.size;
    if (scannedBytes > maxScanBytes) {
      throw new Error(`${label} exceeds the bounded Retention scan limit`);
    }
    const raw = readFileSync(filename, "utf8");
    if (raw && !raw.endsWith("\n")) {
      throw new Error(`${label} has an incomplete segment: ${name}`);
    }
    const lines = raw ? raw.slice(0, -1).split("\n") : [];
    for (const line of lines) {
      let record;
      try {
        record = JSON.parse(line);
      } catch {
        throw new Error(`${label} segment is malformed: ${name}`);
      }
      if (!validator(record)) throw new Error(`${label} record failed validation: ${name}`);
      records.push({ record, line });
    }
  }
  return records;
}

function writeSegmentGeneration(directory, records, segmentBytes) {
  ensurePrivateDirectory(directory);
  let index = 1;
  let lines = [];
  let bytes = 0;
  const flush = () => {
    if (lines.length === 0) return;
    const filename = path.join(
      directory,
      `segment-${String(index).padStart(8, "0")}.jsonl`,
    );
    writeFileSync(filename, `${lines.join("\n")}\n`, { mode: 0o600 });
    fsyncFile(filename);
    index += 1;
    lines = [];
    bytes = 0;
  };
  for (const { line } of records) {
    const lineBytes = Buffer.byteLength(`${line}\n`, "utf8");
    if (lines.length > 0 && bytes + lineBytes > segmentBytes) flush();
    lines.push(line);
    bytes += lineBytes;
  }
  flush();
  fsyncDirectory(directory);
}

function assertExactSelection(records, selected, messageIdFor, label) {
  const present = new Set(records.map(({ record }) => messageIdFor(record)));
  for (const messageId of selected) {
    if (!present.has(messageId)) {
      throw new Error(`${label} selection changed before purge: ${messageId}`);
    }
  }
}

function stageLedger(backupId, selected) {
  ensurePrivateDirectory(DELIVERY_LEDGER_SEGMENTS_DIR);
  ensureEmptyInternalQuarantine(DELIVERY_LEDGER_QUARANTINE_DIR, "Delivery Ledger");
  const records = strictJsonlRecords(
    DELIVERY_LEDGER_SEGMENTS_DIR,
    validDeliveryLedgerRecord,
    "Delivery Ledger",
    DELIVERY_LEDGER_MAX_SCAN_BYTES,
  );
  const messageIdFor = (record) =>
    record.recordType === "ledger-batch"
      ? record.logicalMessage.messageId
      : record.messageId;
  assertExactSelection(records, selected, messageIdFor, "Delivery Ledger");
  const selectedSet = new Set(selected);
  const retained = records.filter(({ record }) => !selectedSet.has(messageIdFor(record)));
  const paths = operationPaths(backupId, "ledger");
  writeSegmentGeneration(paths.stage, retained, DELIVERY_LEDGER_SEGMENT_BYTES);
  return {
    kind: "ledger",
    state: "staged",
    selectedCount: selected.length,
    messageIds: [...selected],
    beforeGeneration: directoryGeneration(paths.active),
    afterGeneration: directoryGeneration(paths.stage),
  };
}

function stageBodies(backupId, selected) {
  ensurePrivateDirectory(MESSAGE_BODY_SEGMENTS_DIR);
  ensureEmptyInternalQuarantine(MESSAGE_BODY_QUARANTINE_DIR, "Message Body Store");
  const records = strictJsonlRecords(
    MESSAGE_BODY_SEGMENTS_DIR,
    validMessageBodyRecord,
    "Message Body Store",
    MESSAGE_BODY_MAX_SCAN_BYTES,
  );
  assertExactSelection(records, selected, (record) => record.messageId, "Message Body");
  const selectedSet = new Set(selected);
  const retained = records.filter(({ record }) => !selectedSet.has(record.messageId));
  const paths = operationPaths(backupId, "bodies");
  writeSegmentGeneration(paths.stage, retained, MESSAGE_BODY_SEGMENT_BYTES);
  return {
    kind: "bodies",
    state: "staged",
    selectedCount: selected.length,
    messageIds: [...selected],
    beforeGeneration: directoryGeneration(paths.active),
    afterGeneration: directoryGeneration(paths.stage),
  };
}

function stageQuarantine(backupId, selected) {
  ensurePrivateDirectory(QUARANTINE_DIR);
  const records = readQuarantineSnapshotForRetention();
  const present = new Set(records.map((record) => record.logicalMessageId));
  for (const messageId of selected) {
    if (!present.has(messageId)) {
      throw new Error(`Quarantine selection changed before purge: ${messageId}`);
    }
  }
  const selectedSet = new Set(selected);
  const paths = operationPaths(backupId, "quarantine");
  ensurePrivateDirectory(paths.stage);
  for (const record of records) {
    if (selectedSet.has(record.logicalMessageId)) continue;
    const filename = path.join(paths.stage, `${record.logicalMessageId}.json`);
    writeFileSync(filename, `${JSON.stringify(record, null, 2)}\n`, { mode: 0o600 });
    fsyncFile(filename);
  }
  fsyncDirectory(paths.stage);
  return {
    kind: "quarantine",
    state: "staged",
    selectedCount: selected.length,
    messageIds: [...selected],
    beforeGeneration: directoryGeneration(paths.active),
    afterGeneration: directoryGeneration(paths.stage),
  };
}

function validGeneration(value) {
  return Boolean(
    Number.isSafeInteger(value?.fileCount) &&
      value.fileCount >= 0 &&
      Number.isSafeInteger(value.bytes) &&
      value.bytes >= 0 &&
      SHA256_PATTERN.test(value.sha256 || ""),
  );
}

function validOperation(operation) {
  return Boolean(
    OPERATION_KINDS.has(operation?.kind) &&
      ["staged", "backed_up", "installed", "restored"].includes(operation.state) &&
      Number.isSafeInteger(operation.selectedCount) &&
      operation.selectedCount > 0 &&
      Array.isArray(operation.messageIds) &&
      operation.messageIds.length === operation.selectedCount &&
      operation.messageIds.every((messageId) => UUID_PATTERN.test(messageId || "")) &&
      new Set(operation.messageIds).size === operation.messageIds.length &&
      validGeneration(operation.beforeGeneration) &&
      validGeneration(operation.afterGeneration),
  );
}

function validManifest(manifest, backupId) {
  return Boolean(
    manifest?.schemaVersion === 1 &&
      manifest.backupId === backupId &&
      UUID_PATTERN.test(backupId || "") &&
      [
        "staging",
        "prepared",
        "tombstoning",
        "swapping",
        "indexing",
        "committed",
        "abandoned",
      ].includes(manifest.status) &&
      SHA256_PATTERN.test(manifest.planDigest || "") &&
      typeof manifest.tombstonesComplete === "boolean" &&
      (manifest.receiptSha256 === null ||
        SHA256_PATTERN.test(manifest.receiptSha256 || "")) &&
      Number.isFinite(Date.parse(manifest.createdAt || "")) &&
      Array.isArray(manifest.operations) &&
      (["staging", "abandoned"].includes(manifest.status) ||
        manifest.operations.length > 0) &&
      manifest.operations.every(validOperation) &&
      new Set(manifest.operations.map((operation) => operation.kind)).size ===
        manifest.operations.length &&
      (manifest.restore === null ||
        (UUID_PATTERN.test(manifest.restore?.restoreId || "") &&
          ["swapping", "indexing", "committed"].includes(manifest.restore?.status))),
  );
}

function writeManifest(manifest) {
  return atomicWriteJson(manifestPath(manifest.backupId), manifest);
}

function readManifest(backupId) {
  const manifest = readPrivateJson(manifestPath(backupId), "Retention transaction manifest");
  if (!validManifest(manifest, backupId)) {
    throw new Error(`Retention transaction manifest failed validation: ${backupId}`);
  }
  return manifest;
}

function generationEquals(actual, expected) {
  return actual.fileCount === expected.fileCount &&
    actual.bytes === expected.bytes &&
    actual.sha256 === expected.sha256;
}

function requireGeneration(directory, expected, label) {
  const actual = directoryGeneration(directory);
  if (!generationEquals(actual, expected)) {
    throw new Error(`${label} generation changed`);
  }
  return actual;
}

function renameGeneration(source, destination) {
  renameSync(source, destination);
  fsyncDirectory(path.dirname(source));
  if (path.dirname(destination) !== path.dirname(source)) {
    fsyncDirectory(path.dirname(destination));
  }
}

function receiptFor(manifest) {
  return {
    schemaVersion: 1,
    outcome: "committed",
    backupId: manifest.backupId,
    planDigest: manifest.planDigest,
    cutoff: manifest.cutoff,
    scope: manifest.scope,
    createdAt: manifest.createdAt,
    committedAt: manifest.committedAt,
    previousHeadBackupId: manifest.previousHeadBackupId,
    categories: Object.fromEntries(
      manifest.operations.map((operation) => [
        operation.kind,
        {
          purged: operation.selectedCount,
          beforeGeneration: operation.beforeGeneration,
          afterGeneration: operation.afterGeneration,
        },
      ]),
    ),
    automaticDeletion: false,
  };
}

function readHead() {
  if (!existsSync(RETENTION_HEAD_PATH)) return null;
  const head = readPrivateJson(RETENTION_HEAD_PATH, "Retention transaction head");
  if (
    head?.schemaVersion !== 1 ||
    (head.backupId !== null && !UUID_PATTERN.test(head.backupId || "")) ||
    !Number.isFinite(Date.parse(head.updatedAt || ""))
  ) {
    throw new Error("Retention transaction head failed validation");
  }
  return head;
}

function writeHead(backupId, generations) {
  atomicWriteJson(RETENTION_HEAD_PATH, {
    schemaVersion: 1,
    backupId,
    generations,
    updatedAt: new Date().toISOString(),
  });
}

async function finishPurge(manifest, fault = null) {
  if (!manifest.tombstonesComplete) {
    const ledgerOperation = manifest.operations.find((operation) => operation.kind === "ledger");
    if (ledgerOperation) {
      const quarantineIds = new Set(
        manifest.operations.find((operation) => operation.kind === "quarantine")?.messageIds || [],
      );
      await createDeliveryDedupTombstones({
        messageIds: ledgerOperation.messageIds,
        backupId: manifest.backupId,
        purgedAt: manifest.createdAt,
        coupledQuarantineMessageIds: ledgerOperation.messageIds.filter((messageId) =>
          quarantineIds.has(messageId),
        ),
      });
    }
    manifest.tombstonesComplete = true;
    writeManifest(manifest);
    if (fault) await fault("after-tombstones", manifest);
  }
  manifest.status = "swapping";
  writeManifest(manifest);
  for (const operation of manifest.operations) {
    const paths = operationPaths(manifest.backupId, operation.kind);
    if (operation.state === "staged") {
      requireGeneration(paths.active, operation.beforeGeneration, `${operation.kind} active`);
      requireGeneration(paths.stage, operation.afterGeneration, `${operation.kind} stage`);
      renameGeneration(paths.active, paths.backup);
      operation.state = "backed_up";
      writeManifest(manifest);
      if (fault) await fault(`after-backup-${operation.kind}`, manifest);
    }
    if (operation.state === "backed_up") {
      requireGeneration(paths.backup, operation.beforeGeneration, `${operation.kind} backup`);
      if (existsSync(paths.active)) {
        throw new Error(`${operation.kind} active generation reappeared during purge`);
      }
      requireGeneration(paths.stage, operation.afterGeneration, `${operation.kind} stage`);
      renameGeneration(paths.stage, paths.active);
      operation.state = "installed";
      writeManifest(manifest);
      if (fault) await fault(`after-install-${operation.kind}`, manifest);
    }
    requireGeneration(paths.backup, operation.beforeGeneration, `${operation.kind} backup`);
    requireGeneration(paths.active, operation.afterGeneration, `${operation.kind} active`);
  }

  manifest.status = "indexing";
  writeManifest(manifest);
  if (manifest.operations.some((operation) => operation.kind === "ledger")) {
    await rebuildDeliveryLedgerIndex();
  }
  if (fault) await fault("after-index", manifest);

  manifest.committedAt ||= new Date().toISOString();
  const receipt = receiptFor(manifest);
  atomicWriteJson(receiptPath(manifest.backupId), receipt);
  manifest.receiptSha256 = sha256(JSON.stringify(receipt));
  writeHead(
    manifest.backupId,
    Object.fromEntries(
      manifest.operations.map((operation) => [operation.kind, operation.afterGeneration]),
    ),
  );
  manifest.status = "committed";
  writeManifest(manifest);
  return receipt;
}

function inferAndNormalizePurgeState(manifest) {
  for (const operation of manifest.operations) {
    const paths = operationPaths(manifest.backupId, operation.kind);
    const active = existsSync(paths.active) ? directoryGeneration(paths.active) : null;
    const stage = existsSync(paths.stage) ? directoryGeneration(paths.stage) : null;
    const backup = existsSync(paths.backup) ? directoryGeneration(paths.backup) : null;
    if (active && generationEquals(active, operation.afterGeneration) &&
        backup && generationEquals(backup, operation.beforeGeneration)) {
      operation.state = "installed";
      continue;
    }
    if (!active && stage && generationEquals(stage, operation.afterGeneration) &&
        backup && generationEquals(backup, operation.beforeGeneration)) {
      operation.state = "backed_up";
      continue;
    }
    if (active && generationEquals(active, operation.beforeGeneration) &&
        stage && generationEquals(stage, operation.afterGeneration) && !backup) {
      operation.state = "staged";
      continue;
    }
    throw new Error(`Retention ${operation.kind} generation cannot be recovered safely`);
  }
  writeManifest(manifest);
  return manifest;
}

function validReceipt(receipt, backupId) {
  return Boolean(
    receipt?.schemaVersion === 1 &&
      receipt.outcome === "committed" &&
      receipt.backupId === backupId &&
      SHA256_PATTERN.test(receipt.planDigest || "") &&
      Number.isFinite(Date.parse(receipt.committedAt || "")) &&
      receipt.automaticDeletion === false,
  );
}

function readReceipt(backupId) {
  const receipt = readPrivateJson(receiptPath(backupId), "Retention receipt");
  if (!validReceipt(receipt, backupId)) {
    throw new Error(`Retention receipt failed validation: ${backupId}`);
  }
  return receipt;
}

function restoreDisplacedPath(manifest, operation) {
  return path.join(
    transactionDirectory(manifest.backupId),
    `restore-${manifest.restore.restoreId}-${operation.kind}`,
  );
}

async function finishRestore(manifest, fault = null) {
  for (const operation of manifest.operations) {
    const paths = operationPaths(manifest.backupId, operation.kind);
    const displaced = restoreDisplacedPath(manifest, operation);
    if (operation.state === "installed") {
      requireGeneration(paths.active, operation.afterGeneration, `${operation.kind} active`);
      requireGeneration(paths.backup, operation.beforeGeneration, `${operation.kind} backup`);
      renameGeneration(paths.active, displaced);
      operation.state = "backed_up";
      writeManifest(manifest);
      if (fault) await fault(`after-restore-backup-${operation.kind}`, manifest);
    }
    if (operation.state === "backed_up") {
      if (existsSync(paths.active)) {
        throw new Error(`${operation.kind} active generation reappeared during restore`);
      }
      requireGeneration(paths.backup, operation.beforeGeneration, `${operation.kind} backup`);
      requireGeneration(displaced, operation.afterGeneration, `${operation.kind} displaced`);
      renameGeneration(paths.backup, paths.active);
      operation.state = "restored";
      writeManifest(manifest);
      if (fault) await fault(`after-restore-install-${operation.kind}`, manifest);
    }
    requireGeneration(paths.active, operation.beforeGeneration, `${operation.kind} restored`);
    requireGeneration(displaced, operation.afterGeneration, `${operation.kind} displaced`);
  }
  manifest.restore.status = "indexing";
  writeManifest(manifest);
  if (manifest.operations.some((operation) => operation.kind === "ledger")) {
    await rebuildDeliveryLedgerIndex();
  }
  manifest.restore.status = "committed";
  manifest.restore.committedAt = new Date().toISOString();
  const receipt = {
    schemaVersion: 1,
    outcome: "restored",
    backupId: manifest.backupId,
    restoreId: manifest.restore.restoreId,
    restoredAt: manifest.restore.committedAt,
    categories: Object.fromEntries(
      manifest.operations.map((operation) => [operation.kind, operation.beforeGeneration]),
    ),
  };
  atomicWriteJson(
    path.join(
      RETENTION_RECEIPTS_DIR,
      `${manifest.backupId}.restore-${manifest.restore.restoreId}.json`,
    ),
    receipt,
  );
  writeHead(
    manifest.previousHeadBackupId,
    Object.fromEntries(
      manifest.operations.map((operation) => [operation.kind, operation.beforeGeneration]),
    ),
  );
  writeManifest(manifest);
  return receipt;
}

function inferAndNormalizeRestoreState(manifest) {
  for (const operation of manifest.operations) {
    const paths = operationPaths(manifest.backupId, operation.kind);
    const displaced = restoreDisplacedPath(manifest, operation);
    const active = existsSync(paths.active) ? directoryGeneration(paths.active) : null;
    const backup = existsSync(paths.backup) ? directoryGeneration(paths.backup) : null;
    const moved = existsSync(displaced) ? directoryGeneration(displaced) : null;
    if (active && generationEquals(active, operation.beforeGeneration) &&
        moved && generationEquals(moved, operation.afterGeneration)) {
      operation.state = "restored";
      continue;
    }
    if (!active && backup && generationEquals(backup, operation.beforeGeneration) &&
        moved && generationEquals(moved, operation.afterGeneration)) {
      operation.state = "backed_up";
      continue;
    }
    if (active && generationEquals(active, operation.afterGeneration) &&
        backup && generationEquals(backup, operation.beforeGeneration) && !moved) {
      operation.state = "installed";
      continue;
    }
    throw new Error(`Retention ${operation.kind} restore generation cannot be recovered safely`);
  }
  writeManifest(manifest);
  return manifest;
}

async function recoverLocked() {
  ensurePrivateDirectory(RETENTION_TRANSACTIONS_DIR);
  ensurePrivateDirectory(RETENTION_RECEIPTS_DIR);
  const recovered = [];
  for (const name of readdirSync(RETENTION_TRANSACTIONS_DIR).sort()) {
    if (!UUID_PATTERN.test(name)) {
      throw new Error(`Retention transaction directory is invalid: ${name}`);
    }
    let manifest = readManifest(name);
    if (manifest.restore && manifest.restore.status !== "committed") {
      manifest = inferAndNormalizeRestoreState(manifest);
      await finishRestore(manifest);
      recovered.push({ backupId: name, outcome: "restored" });
      continue;
    }
    if (["tombstoning", "swapping", "indexing"].includes(manifest.status)) {
      manifest = inferAndNormalizePurgeState(manifest);
      await finishPurge(manifest);
      recovered.push({ backupId: name, outcome: "committed" });
      continue;
    }
    if (["staging", "prepared"].includes(manifest.status)) {
      manifest.status = "abandoned";
      writeManifest(manifest);
      recovered.push({ backupId: name, outcome: "abandoned" });
    }
  }
  return recovered;
}

export async function recoverRetentionTransactions() {
  return withRetentionMutation(recoverLocked);
}

export async function purgeRetention(
  { before, scope = "all", expectedPlanDigest },
  { now = Date.now(), fault = null } = {},
) {
  if (!SHA256_PATTERN.test(expectedPlanDigest || "")) {
    throw new Error("purge requires the exact plan digest");
  }
  return withRetentionMutation(async () => {
    await recoverLocked();
    const plan = await buildRetentionPlan({ before, scope }, { now });
    if (plan.planDigest !== expectedPlanDigest) {
      throw new Error("Retention plan changed; run retention plan again");
    }
    const selected = Object.fromEntries(
      Object.entries(plan.categories).map(([kind, category]) => [
        kind,
        category.eligible.map((candidate) => candidate.messageId).sort(),
      ]),
    );
    if (Object.values(selected).every((messageIds) => messageIds.length === 0)) {
      throw new Error("Retention plan has no eligible records");
    }

    const backupId = randomUUID();
    ensurePrivateDirectory(RETENTION_TRANSACTIONS_DIR);
    ensurePrivateDirectory(RETENTION_RECEIPTS_DIR);
    ensurePrivateDirectory(transactionDirectory(backupId));
    const previousHead = readHead();
    const manifest = {
      schemaVersion: 1,
      backupId,
      status: "staging",
      planDigest: plan.planDigest,
      cutoff: plan.cutoff,
      scope: plan.scope,
      createdAt: new Date(now).toISOString(),
      committedAt: null,
      previousHeadBackupId: previousHead?.backupId || null,
      tombstonesComplete: false,
      receiptSha256: null,
      operations: [],
      restore: null,
    };
    writeManifest(manifest);
    if (selected.ledger.length) {
      manifest.operations.push(stageLedger(backupId, selected.ledger));
      writeManifest(manifest);
    }
    if (selected.bodies.length) {
      manifest.operations.push(stageBodies(backupId, selected.bodies));
      writeManifest(manifest);
    }
    if (selected.quarantine.length) {
      manifest.operations.push(stageQuarantine(backupId, selected.quarantine));
      writeManifest(manifest);
    }
    manifest.status = "prepared";
    writeManifest(manifest);
    if (fault) await fault("prepared", manifest);

    manifest.status = "tombstoning";
    writeManifest(manifest);
    return finishPurge(manifest, fault);
  });
}

export async function restoreRetention(
  { backupId },
  { fault = null } = {},
) {
  validateUuid("Retention backup id", backupId);
  return withRetentionMutation(async () => {
    await recoverLocked();
    const receipt = readReceipt(backupId);
    const head = readHead();
    if (head?.backupId !== backupId) {
      throw new Error("Retention backup is not the current transaction head");
    }
    const manifest = readManifest(backupId);
    if (manifest.receiptSha256 !== sha256(JSON.stringify(receipt))) {
      throw new Error("Retention receipt digest does not match its transaction");
    }
    if (manifest.status !== "committed" || manifest.restore !== null) {
      throw new Error("Retention backup is not restorable");
    }
    for (const operation of manifest.operations) {
      const paths = operationPaths(backupId, operation.kind);
      requireGeneration(paths.active, operation.afterGeneration, `${operation.kind} active`);
      requireGeneration(paths.backup, operation.beforeGeneration, `${operation.kind} backup`);
    }
    manifest.restore = {
      restoreId: randomUUID(),
      status: "swapping",
      startedAt: new Date().toISOString(),
      committedAt: null,
    };
    writeManifest(manifest);
    return finishRestore(manifest, fault);
  });
}
