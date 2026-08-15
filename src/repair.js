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
import {
  deliveryLedgerIndexRepairEvidence,
  DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH,
  DELIVERY_LEDGER_INDEX_DIR,
  rebuildDeliveryLedgerIndex,
} from "./delivery-ledger.js";
import { withFileLock } from "./file-lock.js";
import { inspectNodeDirectory, inspectRouteState } from "./inspectors.js";
import {
  clusterMembershipRecoveryEvidence,
  listClusters,
  recoverClusterMembership,
} from "./node-directory.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const REPAIR_STATE_DIR = path.join(CXMSG_STATE_DIR, "repairs");
export const REPAIR_TRANSACTIONS_DIR = path.join(
  REPAIR_STATE_DIR,
  "transactions",
);
export const REPAIR_RECEIPTS_DIR = path.join(REPAIR_STATE_DIR, "receipts");
const REPAIR_LOCK_PATH = path.join(REPAIR_STATE_DIR, "mutation.lock");

const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const CLUSTER_FINDING_PREFIX = "directory-cluster-memberships.history.";
const INDEX_FINDING_ID = "delivery-ledger.index.consistency";
const INDEX_BACKUP_MAX_BYTES = 128 * 1024 * 1024;
const INDEX_BACKUP_MAX_FILES = 4_097;
const REPAIR_STATE_QUOTA_BYTES = 256 * 1024 * 1024;
const REPAIR_TRANSACTION_LIMIT = 1_024;

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
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
    throw new Error(`Repair directory is not owner-private: ${path.basename(directory)}`);
  }
  chmodSync(directory, 0o700);
}

function assertPrivateFile(filename, maxBytes = Number.MAX_SAFE_INTEGER) {
  const metadata = lstatSync(filename);
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0 ||
    metadata.size > maxBytes
  ) {
    throw new Error(`Repair source is not a bounded private file: ${path.basename(filename)}`);
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

function writePrivateFile(filename, contents) {
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
    writeFileSync(descriptor, contents);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  renameSync(temporary, filename);
  fsyncDirectory(path.dirname(filename));
}

function atomicWriteJson(filename, value) {
  writePrivateFile(filename, `${JSON.stringify(value, null, 2)}\n`);
  return value;
}

function repairStateUsage(directory = REPAIR_STATE_DIR, depth = 0) {
  if (!existsSync(directory)) return { bytes: 0, transactionCount: 0 };
  if (depth > 4) throw new Error("Repair state exceeds its directory depth limit");
  ensurePrivateDirectory(directory);
  let bytes = 0;
  let transactionCount = 0;
  for (const name of readdirSync(directory).sort()) {
    if (directory === REPAIR_STATE_DIR && name === path.basename(REPAIR_LOCK_PATH)) {
      continue;
    }
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
      throw new Error("Repair state contains an invalid entry name");
    }
    const target = path.join(directory, name);
    const metadata = lstatSync(target);
    if (
      metadata.isSymbolicLink() ||
      (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
      (metadata.mode & 0o077) !== 0
    ) {
      throw new Error("Repair state contains a non-private entry");
    }
    if (metadata.isDirectory()) {
      if (directory === REPAIR_TRANSACTIONS_DIR) transactionCount += 1;
      const nested = repairStateUsage(target, depth + 1);
      bytes += nested.bytes;
      transactionCount += nested.transactionCount;
    } else if (metadata.isFile() && metadata.nlink === 1) {
      bytes += metadata.size;
    } else {
      throw new Error("Repair state contains an unsupported entry type");
    }
  }
  return { bytes, transactionCount };
}

function assertRepairCapacity(additionalBytes = 0, additionalTransactions = 0) {
  const usage = repairStateUsage();
  if (
    usage.transactionCount + additionalTransactions > REPAIR_TRANSACTION_LIMIT ||
    usage.bytes + additionalBytes > REPAIR_STATE_QUOTA_BYTES
  ) {
    const error = new Error("Repair state reached its bounded retention limit");
    error.code = "EREPAIRQUOTA";
    throw error;
  }
  return usage;
}

function findingById(checks, findingId) {
  const matches = checks.filter((check) => check.id === findingId);
  if (matches.length !== 1) {
    throw new Error(`Repair finding is missing or ambiguous: ${findingId}`);
  }
  return matches[0];
}

function finalizePlan(base) {
  return {
    ...base,
    planDigest: sha256(JSON.stringify(base)),
  };
}

function clusterRepairPlan(findingId) {
  const finding = findingById(
    inspectNodeDirectory({ stateDir: CXMSG_STATE_DIR }),
    findingId,
  );
  if (finding.errorCode !== "ECLUSTERMEMBERSHIPREDO") {
    throw new Error("Repair finding is not a current Cluster membership redo");
  }
  const prefix = findingId.slice(CLUSTER_FINDING_PREFIX.length).toLowerCase();
  if (!/^[0-9a-f]{8}$/.test(prefix)) {
    throw new Error("Cluster Repair finding identity is invalid");
  }
  const candidates = listClusters().filter((record) =>
    record.clusterId.startsWith(prefix),
  );
  if (candidates.length !== 1) {
    throw new Error("Cluster Repair target is missing or ambiguous");
  }
  const target = candidates[0];
  const evidence = clusterMembershipRecoveryEvidence(target.clusterId);
  if (!evidence.next || evidence.followingExists || evidence.tombstoneExists) {
    throw new Error("Cluster Repair evidence is not a single recoverable redo");
  }
  return finalizePlan({
    schemaVersion: 1,
    findingId,
    errorCode: finding.errorCode,
    repairKind: "cluster-membership-redo",
    mutationCategory: "directory-cluster-head",
    target: { kind: "cluster", id: target.clusterId },
    evidenceSha256: evidence.evidenceSha256,
    recoverability: "owner-private-head-backup",
    automatic: false,
  });
}

function indexRepairPlan(findingId) {
  const finding = findingById(
    inspectRouteState({ stateDir: CXMSG_STATE_DIR }),
    findingId,
  );
  if (finding.errorCode !== "ELEDGERINDEXSTALE") {
    throw new Error("Repair finding is not a current stale Delivery Ledger index");
  }
  const evidence = deliveryLedgerIndexRepairEvidence();
  return finalizePlan({
    schemaVersion: 1,
    findingId,
    errorCode: finding.errorCode,
    repairKind: "delivery-ledger-index-rebuild",
    mutationCategory: "rebuildable-delivery-index",
    target: { kind: "delivery-ledger-index" },
    evidenceSha256: sha256(JSON.stringify(evidence)),
    recoverability: "ledger-truth-and-owner-private-index-backup",
    automatic: false,
  });
}

export function buildRepairPlan({ findingId }) {
  if (typeof findingId !== "string" || findingId.length > 192) {
    throw new Error("Repair finding id is invalid");
  }
  if (findingId === INDEX_FINDING_ID) return indexRepairPlan(findingId);
  if (findingId.startsWith(CLUSTER_FINDING_PREFIX)) {
    return clusterRepairPlan(findingId);
  }
  throw new Error("Repair supports only Cluster membership redo and stale Ledger index findings");
}

function backupCluster(transactionDirectory, plan) {
  const evidence = clusterMembershipRecoveryEvidence(plan.target.id);
  if (evidence.evidenceSha256 !== plan.evidenceSha256) {
    const error = new Error("Cluster Repair evidence changed before backup");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  const records = [
    ["cluster-head.json", evidence.current],
    ["next-membership.json", evidence.next],
  ];
  const prepared = records.map(([name, record]) => ({
    name,
    contents: `${JSON.stringify(record, null, 2)}\n`,
  }));
  assertRepairCapacity(
    prepared.reduce(
      (total, record) => total + Buffer.byteLength(record.contents, "utf8"),
      0,
    ) + 512 * 1024,
  );
  const files = prepared.map(({ name, contents }) => {
    writePrivateFile(path.join(transactionDirectory, name), contents);
    return {
      name,
      bytes: Buffer.byteLength(contents, "utf8"),
      sha256: sha256(contents),
    };
  });
  return { kind: "cluster-head", files, evidence };
}

function indexBackupNames() {
  if (!existsSync(DELIVERY_LEDGER_INDEX_DIR)) return [];
  return readdirSync(DELIVERY_LEDGER_INDEX_DIR)
    .filter(
      (name) =>
        name === path.basename(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH) ||
        /^[0-9a-f-]{36}\.json$/i.test(name),
    )
    .sort();
}

function backupIndex(transactionDirectory, plan) {
  const evidence = deliveryLedgerIndexRepairEvidence();
  if (sha256(JSON.stringify(evidence)) !== plan.evidenceSha256) {
    const error = new Error("Delivery Ledger index Repair evidence changed before backup");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  const names = indexBackupNames();
  if (names.length > INDEX_BACKUP_MAX_FILES) {
    throw new Error("Delivery Ledger index backup exceeds its file limit");
  }
  let totalBytes = 0;
  const prepared = names.map((name) => {
    const source = path.join(DELIVERY_LEDGER_INDEX_DIR, name);
    const metadata = assertPrivateFile(source, INDEX_BACKUP_MAX_BYTES);
    totalBytes += metadata.size;
    if (totalBytes > INDEX_BACKUP_MAX_BYTES) {
      throw new Error("Delivery Ledger index backup exceeds its byte limit");
    }
    const contents = readFileSync(source);
    return { name, contents };
  });
  assertRepairCapacity(totalBytes + 512 * 1024);
  const backupDirectory = path.join(transactionDirectory, "index");
  ensurePrivateDirectory(backupDirectory);
  const files = prepared.map(({ name, contents }) => {
    writePrivateFile(path.join(backupDirectory, name), contents);
    return { name, bytes: contents.length, sha256: sha256(contents) };
  });
  return { kind: "delivery-ledger-index", files, evidence };
}

function backupRepair(transactionDirectory, plan) {
  return plan.repairKind === "cluster-membership-redo"
    ? backupCluster(transactionDirectory, plan)
    : backupIndex(transactionDirectory, plan);
}

function boundedErrorCode(error) {
  return /^[A-Z0-9_]{1,32}$/.test(error?.code || "")
    ? error.code
    : "EREPAIRFAILED";
}

function verifyRepair(plan) {
  const checks = plan.repairKind === "cluster-membership-redo"
    ? inspectNodeDirectory({ stateDir: CXMSG_STATE_DIR })
    : inspectRouteState({ stateDir: CXMSG_STATE_DIR });
  const finding = findingById(checks, plan.findingId);
  if (finding.status !== "pass" || finding.errorCode) {
    const error = new Error("Repair did not resolve its exact finding");
    error.code = "EREPAIRVERIFY";
    throw error;
  }
  return { id: finding.id, status: finding.status };
}

async function executeRepair(plan, backup) {
  if (plan.repairKind === "cluster-membership-redo") {
    const result = await recoverClusterMembership(plan.target.id, {
      expectedEvidenceSha256: backup.evidence.evidenceSha256,
    });
    if (!result.recovered) throw new Error("Cluster Repair performed no mutation");
    return {
      recovered: true,
      membershipVersion: result.record.membershipVersion,
    };
  }
  return rebuildDeliveryLedgerIndex({
    expectedLedgerManifestSha256: backup.evidence.ledgerManifestSha256,
    expectedIndexGenerationSha256: backup.evidence.indexGenerationSha256,
  });
}

export async function applyRepair({
  findingId,
  expectedPlanDigest,
}) {
  if (!SHA256_PATTERN.test(expectedPlanDigest || "")) {
    throw new Error("Repair apply requires the exact 64-character plan digest");
  }
  const preview = buildRepairPlan({ findingId });
  if (preview.planDigest !== expectedPlanDigest) {
    const error = new Error("Repair plan changed; generate and confirm a new plan");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  ensurePrivateDirectory(REPAIR_STATE_DIR);
  return withFileLock(REPAIR_LOCK_PATH, async () => {
    const plan = buildRepairPlan({ findingId });
    if (plan.planDigest !== expectedPlanDigest) {
      const error = new Error("Repair plan changed; generate and confirm a new plan");
      error.code = "EREPAIRSTALE";
      throw error;
    }
    ensurePrivateDirectory(REPAIR_TRANSACTIONS_DIR);
    ensurePrivateDirectory(REPAIR_RECEIPTS_DIR);
    assertRepairCapacity(512 * 1024, 1);
    const transactionId = randomUUID();
    const transactionDirectory = path.join(REPAIR_TRANSACTIONS_DIR, transactionId);
    mkdirSync(transactionDirectory, { mode: 0o700 });
    fsyncDirectory(REPAIR_TRANSACTIONS_DIR);
    const manifestPath = path.join(transactionDirectory, "manifest.json");
    const receiptPath = path.join(REPAIR_RECEIPTS_DIR, `${transactionId}.json`);
    const startedAt = new Date().toISOString();
    let manifest = atomicWriteJson(manifestPath, {
      schemaVersion: 1,
      transactionId,
      phase: "initializing",
      plan,
      startedAt,
      updatedAt: startedAt,
    });
    try {
      const backup = backupRepair(transactionDirectory, plan);
      manifest = atomicWriteJson(manifestPath, {
        ...manifest,
        phase: "prepared",
        backup: { kind: backup.kind, files: backup.files },
        updatedAt: new Date().toISOString(),
      });
      manifest = atomicWriteJson(manifestPath, {
        ...manifest,
        phase: "mutation-started",
        updatedAt: new Date().toISOString(),
      });
      const result = await executeRepair(plan, backup);
      manifest = atomicWriteJson(manifestPath, {
        ...manifest,
        phase: "mutated",
        result,
        updatedAt: new Date().toISOString(),
      });
      const verification = verifyRepair(plan);
      const completedAt = new Date().toISOString();
      const receipt = atomicWriteJson(receiptPath, {
        schemaVersion: 1,
        transactionId,
        findingId,
        repairKind: plan.repairKind,
        planDigest: plan.planDigest,
        status: "completed",
        result,
        verification,
        startedAt,
        completedAt,
      });
      atomicWriteJson(manifestPath, {
        ...manifest,
        phase: "completed",
        receiptSha256: sha256(JSON.stringify(receipt)),
        updatedAt: completedAt,
      });
      return receipt;
    } catch (error) {
      const failedAt = new Date().toISOString();
      const code = boundedErrorCode(error);
      atomicWriteJson(receiptPath, {
        schemaVersion: 1,
        transactionId,
        findingId,
        repairKind: plan.repairKind,
        planDigest: plan.planDigest,
        status: "failed",
        errorCode: code,
        startedAt,
        completedAt: failedAt,
      });
      atomicWriteJson(manifestPath, {
        ...manifest,
        phase: "failed",
        errorCode: code,
        updatedAt: failedAt,
      });
      throw error;
    }
  });
}
