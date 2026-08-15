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
import { inspectRepairState } from "./inspectors.js";
import {
  assertRepairRestoreCapacity,
  REPAIR_RECEIPTS_DIR,
  REPAIR_STATE_DIR,
  REPAIR_TRANSACTIONS_DIR,
  withRepairMutation,
} from "./repair.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const REPAIR_RETENTION_MIN_AGE_MS = 90 * 24 * 60 * 60 * 1_000;
export const REPAIR_RETENTION_STATE_DIR = path.join(
  CXMSG_STATE_DIR,
  "repair-retention",
);
export const REPAIR_RETENTION_TRANSACTIONS_DIR = path.join(
  REPAIR_RETENTION_STATE_DIR,
  "transactions",
);
export const REPAIR_RETENTION_RECEIPTS_DIR = path.join(
  REPAIR_RETENTION_STATE_DIR,
  "receipts",
);

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const ARCHIVE_QUOTA_BYTES = 1024 * 1024 * 1024;
const ARCHIVE_TRANSACTION_LIMIT = 1_024;

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function timestamp(label, value) {
  const parsed = Date.parse(value || "");
  if (!Number.isFinite(parsed)) throw new Error(`${label} must be an ISO timestamp`);
  return parsed;
}

function ensurePrivateDirectory(directory) {
  if (!existsSync(directory)) mkdirSync(directory, { recursive: true, mode: 0o700 });
  const metadata = lstatSync(directory);
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    throw new Error(`Repair archive directory is not owner-private: ${path.basename(directory)}`);
  }
  chmodSync(directory, 0o700);
}

function assertPrivateFile(filename, maxBytes = 1024 * 1024) {
  const metadata = lstatSync(filename);
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0 ||
    metadata.size > maxBytes
  ) {
    throw new Error(`Repair archive file is not bounded and owner-private: ${path.basename(filename)}`);
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

function atomicWriteJson(filename, value) {
  ensurePrivateDirectory(path.dirname(filename));
  const temporary = `${filename}.${randomUUID()}.tmp`;
  const descriptor = openSync(
    temporary,
    constants.O_CREAT |
      constants.O_EXCL |
      constants.O_WRONLY |
      (constants.O_NOFOLLOW || 0),
    0o600,
  );
  try {
    writeFileSync(descriptor, `${JSON.stringify(value, null, 2)}\n`);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  renameSync(temporary, filename);
  fsyncDirectory(path.dirname(filename));
  return value;
}

function readJson(filename, label) {
  const metadata = assertPrivateFile(filename);
  const contents = readFileSync(filename);
  let value;
  try {
    value = JSON.parse(contents.toString("utf8"));
  } catch {
    throw new Error(`${label} is malformed`);
  }
  return { value, bytes: metadata.size, sha256: sha256(contents) };
}

function privateTreeSummary(directory, depth = 0, prefix = "") {
  if (depth > 6) throw new Error("Repair archive tree exceeds its depth limit");
  ensurePrivateDirectory(directory);
  const files = [];
  for (const name of readdirSync(directory).sort()) {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
      throw new Error("Repair archive tree contains an invalid entry");
    }
    const filename = path.join(directory, name);
    const relative = prefix ? `${prefix}/${name}` : name;
    const metadata = lstatSync(filename);
    if (
      metadata.isSymbolicLink() ||
      (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
      (metadata.mode & 0o077) !== 0
    ) {
      throw new Error("Repair archive tree contains a non-private entry");
    }
    if (metadata.isDirectory()) {
      files.push(...privateTreeSummary(filename, depth + 1, relative).files);
    } else if (metadata.isFile() && metadata.nlink === 1) {
      const contents = readFileSync(filename);
      files.push({ name: relative, bytes: metadata.size, sha256: sha256(contents) });
    } else {
      throw new Error("Repair archive tree contains an unsupported entry");
    }
  }
  return {
    files,
    bytes: files.reduce((total, file) => total + file.bytes, 0),
    sha256: sha256(JSON.stringify(files)),
  };
}

function archiveStateUsage(directory = REPAIR_RETENTION_STATE_DIR, depth = 0) {
  if (!existsSync(directory)) return { bytes: 0, transactionCount: 0 };
  if (depth > 7) throw new Error("Repair archive exceeds its depth limit");
  ensurePrivateDirectory(directory);
  let bytes = 0;
  let transactionCount = 0;
  for (const name of readdirSync(directory).sort()) {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
      throw new Error("Repair archive contains an invalid entry");
    }
    const filename = path.join(directory, name);
    const metadata = lstatSync(filename);
    if (
      metadata.isSymbolicLink() ||
      (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
      (metadata.mode & 0o077) !== 0
    ) {
      throw new Error("Repair archive contains a non-private entry");
    }
    if (metadata.isDirectory()) {
      if (directory === REPAIR_RETENTION_TRANSACTIONS_DIR) transactionCount += 1;
      const nested = archiveStateUsage(filename, depth + 1);
      bytes += nested.bytes;
      transactionCount += nested.transactionCount;
    } else if (metadata.isFile() && metadata.nlink === 1) {
      bytes += metadata.size;
    } else {
      throw new Error("Repair archive contains an unsupported entry");
    }
  }
  return { bytes, transactionCount };
}

function assertArchiveCapacity(additionalBytes, additionalTransactions) {
  const usage = archiveStateUsage();
  if (
    usage.bytes + additionalBytes > ARCHIVE_QUOTA_BYTES ||
    usage.transactionCount + additionalTransactions > ARCHIVE_TRANSACTION_LIMIT
  ) {
    const error = new Error("Repair archive reached its bounded retention limit");
    error.code = "EREPAIRARCHIVEQUOTA";
    throw error;
  }
}

function assertRepairStateConsistent() {
  const failed = inspectRepairState({ stateDir: CXMSG_STATE_DIR }).filter(
    (check) => check.status === "fail",
  );
  if (failed.length > 0) {
    const error = new Error("Repair retention requires consistent Repair state");
    error.code = "EREPAIRRETENTIONSTATE";
    throw error;
  }
}

function selectionDigest(plan) {
  return sha256(JSON.stringify({
    schemaVersion: plan.schemaVersion,
    cutoff: plan.cutoff,
    eligible: plan.category.eligible,
  }));
}

export function buildRepairRetentionPlan(
  { before },
  { now = Date.now() } = {},
) {
  const cutoff = timestamp("Repair retention cutoff", before);
  if (!Number.isFinite(now)) throw new Error("Repair retention clock is invalid");
  if (cutoff > now - REPAIR_RETENTION_MIN_AGE_MS) {
    throw new Error("Repair retention cutoff must preserve at least 90 days");
  }
  const category = {
    eligible: [],
    blocked: [],
    retainedByAge: 0,
    estimatedBytes: 0,
  };
  if (!existsSync(REPAIR_STATE_DIR)) {
    const plan = {
      schemaVersion: 1,
      generatedAt: new Date(now).toISOString(),
      cutoff: new Date(cutoff).toISOString(),
      policy: {
        automaticDeletion: false,
        mutationEnabled: true,
        mutationKind: "recoverable-archive",
        minimumAgeDays: REPAIR_RETENTION_MIN_AGE_MS / (24 * 60 * 60 * 1_000),
        terminalState: "completed",
      },
      category,
    };
    return { ...plan, planDigest: selectionDigest(plan) };
  }
  assertRepairStateConsistent();
  for (const transactionId of readdirSync(REPAIR_TRANSACTIONS_DIR).sort()) {
    if (!UUID_PATTERN.test(transactionId)) {
      throw new Error("Repair retention found an invalid transaction identity");
    }
    const transactionDirectory = path.join(REPAIR_TRANSACTIONS_DIR, transactionId);
    const manifestRecord = readJson(
      path.join(transactionDirectory, "manifest.json"),
      "Repair transaction manifest",
    );
    const manifest = manifestRecord.value;
    if (manifest.phase !== "completed") {
      category.blocked.push({
        transactionId,
        phase: manifest.phase,
        reason: "noncompleted_repair",
      });
      continue;
    }
    const receiptRecord = readJson(
      path.join(REPAIR_RECEIPTS_DIR, `${transactionId}.json`),
      "Repair receipt",
    );
    const receipt = receiptRecord.value;
    const completedAt = timestamp("Repair completion timestamp", receipt.completedAt);
    if (completedAt >= cutoff) {
      category.retainedByAge += 1;
      continue;
    }
    const transaction = privateTreeSummary(transactionDirectory);
    const candidate = {
      transactionId,
      completedAt: new Date(completedAt).toISOString(),
      repairKind: receipt.repairKind,
      planDigest: receipt.planDigest,
      transactionSha256: transaction.sha256,
      receiptSha256: receiptRecord.sha256,
      estimatedBytes: transaction.bytes + receiptRecord.bytes,
    };
    category.eligible.push(candidate);
    category.estimatedBytes += candidate.estimatedBytes;
  }
  const plan = {
    schemaVersion: 1,
    generatedAt: new Date(now).toISOString(),
    cutoff: new Date(cutoff).toISOString(),
    policy: {
      automaticDeletion: false,
      mutationEnabled: true,
      mutationKind: "recoverable-archive",
      minimumAgeDays: REPAIR_RETENTION_MIN_AGE_MS / (24 * 60 * 60 * 1_000),
      terminalState: "completed",
    },
    category,
  };
  return { ...plan, planDigest: selectionDigest(plan) };
}

function archiveTransactionDirectory(archiveId) {
  if (!UUID_PATTERN.test(archiveId || "")) throw new Error("Repair archive id is invalid");
  return path.join(REPAIR_RETENTION_TRANSACTIONS_DIR, archiveId);
}

function archiveManifestPath(archiveId) {
  return path.join(archiveTransactionDirectory(archiveId), "manifest.json");
}

function archiveReceiptPath(archiveId) {
  return path.join(REPAIR_RETENTION_RECEIPTS_DIR, `${archiveId}.json`);
}

function archiveItemPaths(archiveId, transactionId) {
  const item = path.join(
    archiveTransactionDirectory(archiveId),
    "items",
    transactionId,
  );
  return {
    item,
    activeTransaction: path.join(REPAIR_TRANSACTIONS_DIR, transactionId),
    activeReceipt: path.join(REPAIR_RECEIPTS_DIR, `${transactionId}.json`),
    archivedTransaction: path.join(item, "transaction"),
    archivedReceipt: path.join(item, "receipt.json"),
  };
}

function validArchiveManifest(manifest, archiveId) {
  return Boolean(
    manifest?.schemaVersion === 1 &&
      manifest.archiveId === archiveId &&
      ["archiving", "committed", "restoring", "restored"].includes(manifest.status) &&
      SHA256_PATTERN.test(manifest.planDigest || "") &&
      Number.isFinite(Date.parse(manifest.createdAt || "")) &&
      (manifest.status === "archiving" ||
        (Number.isFinite(Date.parse(manifest.committedAt || "")) &&
          SHA256_PATTERN.test(manifest.receiptSha256 || ""))) &&
      Array.isArray(manifest.items) &&
      manifest.items.length > 0 &&
      manifest.items.every((item) =>
        UUID_PATTERN.test(item?.transactionId || "") &&
        ["pending", "transaction-moved", "archived", "transaction-restored", "restored"].includes(item.state) &&
        SHA256_PATTERN.test(item.transactionSha256 || "") &&
        SHA256_PATTERN.test(item.receiptSha256 || "") &&
        Number.isSafeInteger(item.estimatedBytes) &&
        item.estimatedBytes >= 0
      ) &&
      new Set(manifest.items.map((item) => item.transactionId)).size === manifest.items.length &&
      (manifest.restore === null
        ? ["archiving", "committed"].includes(manifest.status)
        : (["restoring", "restored"].includes(manifest.status) &&
          UUID_PATTERN.test(manifest.restore?.restoreId || "") &&
          Number.isFinite(Date.parse(manifest.restore?.startedAt || "")) &&
          (manifest.status === "restoring" ||
            (Number.isFinite(Date.parse(manifest.restore?.completedAt || "")) &&
              SHA256_PATTERN.test(manifest.restore?.receiptSha256 || "")))))
  );
}

function writeArchiveManifest(manifest) {
  return atomicWriteJson(archiveManifestPath(manifest.archiveId), manifest);
}

function readArchiveManifest(archiveId) {
  const manifest = readJson(
    archiveManifestPath(archiveId),
    "Repair archive manifest",
  ).value;
  if (!validArchiveManifest(manifest, archiveId)) {
    throw new Error("Repair archive manifest failed validation");
  }
  return manifest;
}

function validateArchiveContainer(manifest) {
  const root = archiveTransactionDirectory(manifest.archiveId);
  if (
    readdirSync(root).some((name) => !["manifest.json", "items"].includes(name))
  ) {
    throw new Error("Repair archive transaction contains an unexpected entry");
  }
  const itemsDirectory = path.join(root, "items");
  ensurePrivateDirectory(itemsDirectory);
  const expected = new Set(manifest.items.map((item) => item.transactionId));
  if (
    readdirSync(itemsDirectory).some((name) => !expected.has(name))
  ) {
    throw new Error("Repair archive contains an unexpected item");
  }
}

function requireArchiveReceipt(manifest) {
  const record = readJson(
    archiveReceiptPath(manifest.archiveId),
    "Repair archive terminal receipt",
  );
  const receipt = record.value;
  if (
    receipt?.schemaVersion !== 1 ||
    receipt.outcome !== "archived" ||
    receipt.archiveId !== manifest.archiveId ||
    receipt.planDigest !== manifest.planDigest ||
    manifest.receiptSha256 !== sha256(JSON.stringify(receipt))
  ) {
    throw new Error("Repair archive terminal receipt failed validation");
  }
  return receipt;
}

function reusableArchiveReceipt(manifest) {
  const filename = archiveReceiptPath(manifest.archiveId);
  if (!existsSync(filename)) return null;
  const receipt = readJson(filename, "Repair archive terminal receipt").value;
  const estimatedBytes = manifest.items.reduce(
    (total, item) => total + item.estimatedBytes,
    0,
  );
  if (
    receipt?.schemaVersion !== 1 ||
    receipt.outcome !== "archived" ||
    receipt.archiveId !== manifest.archiveId ||
    receipt.planDigest !== manifest.planDigest ||
    receipt.cutoff !== manifest.cutoff ||
    receipt.itemCount !== manifest.items.length ||
    receipt.estimatedBytes !== estimatedBytes ||
    receipt.createdAt !== manifest.createdAt ||
    !Number.isFinite(Date.parse(receipt.committedAt || "")) ||
    receipt.automaticDeletion !== false
  ) {
    throw new Error("Repair archive terminal receipt failed recovery validation");
  }
  return receipt;
}

function requireTransaction(directory, expectedSha256) {
  const summary = privateTreeSummary(directory);
  if (summary.sha256 !== expectedSha256) {
    const error = new Error("Repair archive transaction evidence changed");
    error.code = "EREPAIRRETENTIONSTALE";
    throw error;
  }
  return summary;
}

function requireReceipt(filename, expectedSha256) {
  const receipt = readJson(filename, "Repair archive receipt source");
  if (receipt.sha256 !== expectedSha256) {
    const error = new Error("Repair archive receipt evidence changed");
    error.code = "EREPAIRRETENTIONSTALE";
    throw error;
  }
  return receipt;
}

function classifyItem(archiveId, item) {
  const paths = archiveItemPaths(archiveId, item.transactionId);
  const state = {
    activeTransaction: existsSync(paths.activeTransaction),
    activeReceipt: existsSync(paths.activeReceipt),
    archivedTransaction: existsSync(paths.archivedTransaction),
    archivedReceipt: existsSync(paths.archivedReceipt),
  };
  let classification;
  if (
    state.activeTransaction && state.activeReceipt &&
    !state.archivedTransaction && !state.archivedReceipt
  ) classification = "active";
  else if (
    !state.activeTransaction && state.activeReceipt &&
    state.archivedTransaction && !state.archivedReceipt
  ) classification = "transaction-moved";
  else if (
    !state.activeTransaction && !state.activeReceipt &&
    state.archivedTransaction && state.archivedReceipt
  ) classification = "archived";
  else if (
    state.activeTransaction && !state.activeReceipt &&
    !state.archivedTransaction && state.archivedReceipt
  ) classification = "transaction-restored";
  else {
    throw new Error("Repair archive item cannot be recovered safely");
  }
  const expectedEntries = classification === "active"
    ? []
    : classification === "transaction-moved"
      ? ["transaction"]
      : classification === "archived"
        ? ["receipt.json", "transaction"]
        : ["receipt.json"];
  if (
    existsSync(paths.item) &&
    JSON.stringify(readdirSync(paths.item).sort()) !==
      JSON.stringify(expectedEntries)
  ) {
    throw new Error("Repair archive item contains unexpected evidence");
  }
  if (state.activeTransaction) {
    requireTransaction(paths.activeTransaction, item.transactionSha256);
  }
  if (state.archivedTransaction) {
    requireTransaction(paths.archivedTransaction, item.transactionSha256);
  }
  if (state.activeReceipt) requireReceipt(paths.activeReceipt, item.receiptSha256);
  if (state.archivedReceipt) requireReceipt(paths.archivedReceipt, item.receiptSha256);
  return { classification, paths };
}

function move(source, destination) {
  renameSync(source, destination);
  fsyncDirectory(path.dirname(source));
  if (path.dirname(destination) !== path.dirname(source)) {
    fsyncDirectory(path.dirname(destination));
  }
}

async function finishArchive(manifest, fault = null) {
  for (const item of manifest.items) {
    let { classification, paths } = classifyItem(manifest.archiveId, item);
    if (classification === "active") {
      ensurePrivateDirectory(paths.item);
      move(paths.activeTransaction, paths.archivedTransaction);
      item.state = "transaction-moved";
      writeArchiveManifest(manifest);
      if (fault) await fault("after-transaction-move", manifest, item);
      classification = "transaction-moved";
    }
    if (classification === "transaction-moved") {
      move(paths.activeReceipt, paths.archivedReceipt);
      item.state = "archived";
      writeArchiveManifest(manifest);
      if (fault) await fault("after-receipt-move", manifest, item);
      classification = "archived";
    }
    if (classification !== "archived") {
      throw new Error("Repair archive item is not in an archivable state");
    }
  }
  const existingReceipt = reusableArchiveReceipt(manifest);
  const committedAt = existingReceipt?.committedAt || new Date().toISOString();
  const receipt = existingReceipt || atomicWriteJson(
    archiveReceiptPath(manifest.archiveId),
    {
      schemaVersion: 1,
      outcome: "archived",
      archiveId: manifest.archiveId,
      planDigest: manifest.planDigest,
      cutoff: manifest.cutoff,
      itemCount: manifest.items.length,
      estimatedBytes: manifest.items.reduce(
        (total, item) => total + item.estimatedBytes,
        0,
      ),
      createdAt: manifest.createdAt,
      committedAt,
      automaticDeletion: false,
    },
  );
  if (fault) await fault("after-archive-receipt", manifest);
  manifest.status = "committed";
  manifest.committedAt = committedAt;
  manifest.receiptSha256 = sha256(JSON.stringify(receipt));
  writeArchiveManifest(manifest);
  return receipt;
}

async function finishRestore(manifest, fault = null) {
  for (const item of manifest.items) {
    let { classification, paths } = classifyItem(manifest.archiveId, item);
    if (classification === "archived") {
      move(paths.archivedTransaction, paths.activeTransaction);
      item.state = "transaction-restored";
      writeArchiveManifest(manifest);
      if (fault) await fault("after-transaction-restore", manifest, item);
      classification = "transaction-restored";
    }
    if (classification === "transaction-restored") {
      move(paths.archivedReceipt, paths.activeReceipt);
      item.state = "restored";
      writeArchiveManifest(manifest);
      if (fault) await fault("after-receipt-restore", manifest, item);
      classification = "active";
    }
    if (classification !== "active") {
      throw new Error("Repair archive item is not in a restorable state");
    }
  }
  const restoredAt = new Date().toISOString();
  const receipt = atomicWriteJson(
    path.join(
      REPAIR_RETENTION_RECEIPTS_DIR,
      `${manifest.archiveId}.restore-${manifest.restore.restoreId}.json`,
    ),
    {
      schemaVersion: 1,
      outcome: "restored",
      archiveId: manifest.archiveId,
      restoreId: manifest.restore.restoreId,
      itemCount: manifest.items.length,
      restoredAt,
    },
  );
  manifest.status = "restored";
  manifest.restore.completedAt = restoredAt;
  manifest.restore.receiptSha256 = sha256(JSON.stringify(receipt));
  writeArchiveManifest(manifest);
  return receipt;
}

async function recoverLocked() {
  if (!existsSync(REPAIR_RETENTION_STATE_DIR)) return [];
  ensurePrivateDirectory(REPAIR_RETENTION_TRANSACTIONS_DIR);
  ensurePrivateDirectory(REPAIR_RETENTION_RECEIPTS_DIR);
  const recovered = [];
  for (const archiveId of readdirSync(REPAIR_RETENTION_TRANSACTIONS_DIR).sort()) {
    if (!UUID_PATTERN.test(archiveId)) {
      throw new Error("Repair archive contains an invalid transaction identity");
    }
    const manifest = readArchiveManifest(archiveId);
    validateArchiveContainer(manifest);
    if (manifest.status === "archiving") {
      await finishArchive(manifest);
      recovered.push({ archiveId, outcome: "archived" });
    } else if (manifest.status === "restoring") {
      requireArchiveReceipt(manifest);
      await finishRestore(manifest);
      recovered.push({ archiveId, outcome: "restored" });
    }
  }
  return recovered;
}

export async function recoverRepairRetention() {
  if (!existsSync(REPAIR_RETENTION_STATE_DIR)) return [];
  return withRepairMutation(recoverLocked);
}

export async function archiveRepairRetention(
  { before, expectedPlanDigest },
  { now = Date.now(), fault = null } = {},
) {
  if (!SHA256_PATTERN.test(expectedPlanDigest || "")) {
    throw new Error("Repair archive requires the exact plan digest");
  }
  const preview = buildRepairRetentionPlan({ before }, { now });
  if (preview.planDigest !== expectedPlanDigest) {
    const error = new Error("Repair retention plan changed; generate a new plan");
    error.code = "EREPAIRRETENTIONSTALE";
    throw error;
  }
  return withRepairMutation(async () => {
    await recoverLocked();
    const plan = buildRepairRetentionPlan({ before }, { now });
    if (plan.planDigest !== expectedPlanDigest) {
      const error = new Error("Repair retention plan changed; generate a new plan");
      error.code = "EREPAIRRETENTIONSTALE";
      throw error;
    }
    if (plan.category.eligible.length === 0) {
      throw new Error("Repair retention plan has no eligible transactions");
    }
    assertArchiveCapacity(plan.category.estimatedBytes + 512 * 1024, 1);
    ensurePrivateDirectory(REPAIR_RETENTION_TRANSACTIONS_DIR);
    ensurePrivateDirectory(REPAIR_RETENTION_RECEIPTS_DIR);
    const archiveId = randomUUID();
    const transactionDirectory = archiveTransactionDirectory(archiveId);
    ensurePrivateDirectory(transactionDirectory);
    ensurePrivateDirectory(path.join(transactionDirectory, "items"));
    const manifest = {
      schemaVersion: 1,
      archiveId,
      status: "archiving",
      planDigest: plan.planDigest,
      cutoff: plan.cutoff,
      createdAt: new Date(now).toISOString(),
      committedAt: null,
      receiptSha256: null,
      restore: null,
      items: plan.category.eligible.map((candidate) => ({
        transactionId: candidate.transactionId,
        state: "pending",
        transactionSha256: candidate.transactionSha256,
        receiptSha256: candidate.receiptSha256,
        estimatedBytes: candidate.estimatedBytes,
      })),
    };
    writeArchiveManifest(manifest);
    return finishArchive(manifest, fault);
  });
}

export async function restoreRepairRetention(
  { archiveId },
  { fault = null } = {},
) {
  if (!UUID_PATTERN.test(archiveId || "")) {
    throw new Error("Repair archive id is invalid");
  }
  return withRepairMutation(async () => {
    await recoverLocked();
    const manifest = readArchiveManifest(archiveId);
    if (manifest.status !== "committed" || manifest.restore !== null) {
      throw new Error("Repair archive is not restorable");
    }
    validateArchiveContainer(manifest);
    requireArchiveReceipt(manifest);
    const bytes = manifest.items.reduce(
      (total, item) => total + item.estimatedBytes,
      0,
    );
    assertRepairRestoreCapacity(bytes, manifest.items.length);
    for (const item of manifest.items) {
      const { classification } = classifyItem(archiveId, item);
      if (classification !== "archived") {
        throw new Error("Repair archive restore target is not empty");
      }
    }
    manifest.status = "restoring";
    manifest.restore = {
      restoreId: randomUUID(),
      startedAt: new Date().toISOString(),
      completedAt: null,
      receiptSha256: null,
    };
    writeArchiveManifest(manifest);
    return finishRestore(manifest, fault);
  });
}
