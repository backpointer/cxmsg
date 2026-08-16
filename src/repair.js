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
  deliveryLedgerIndexRepairEvidence,
  DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH,
  DELIVERY_LEDGER_INDEX_DIR,
  rebuildDeliveryLedgerIndex,
} from "./delivery-ledger.js";
import { withFileLock } from "./file-lock.js";
import {
  inspectInboundPolicies,
  inspectJobs,
  inspectNodeDirectory,
  inspectRouteState,
} from "./inspectors.js";
import {
  INBOUND_POLICY_STALE_FINDING_ID,
  purgeStaleInboundPolicyArtifacts,
  readStaleInboundPolicyArtifact,
  staleInboundPolicyArtifactEvidence,
} from "./inbound-policy.js";
import {
  clusterMembershipRecoveryEvidence,
  listClusters,
  recoverClusterMembership,
} from "./node-directory.js";
import {
  listJobsReadOnly,
  readJob,
  withJobLock,
  writeJob,
} from "./jobs.js";
import { withRetentionMutation } from "./retention-barrier.js";
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
const LEGACY_JOB_FINDING_ID = "jobs.records.legacy-kind";
const INBOUND_POLICY_ARTIFACT_BACKUP_DIR = "inbound-policy-artifacts";
const LEGACY_JOB_BACKUP_NAME = "legacy-job.json";
const INDEX_BACKUP_MAX_BYTES = 128 * 1024 * 1024;
const LEGACY_JOB_BACKUP_MAX_BYTES = 256 * 1024;
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
      requireNoFollowFlag(),
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

export function assertRepairRestoreCapacity(
  additionalBytes,
  additionalTransactions,
) {
  return assertRepairCapacity(additionalBytes, additionalTransactions);
}

export function withRepairMutation(callback) {
  ensurePrivateDirectory(REPAIR_STATE_DIR);
  return withFileLock(REPAIR_LOCK_PATH, callback);
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

function inboundPolicyArtifactRepairPlan(findingId) {
  const finding = findingById(
    inspectInboundPolicies({ stateDir: CXMSG_STATE_DIR }),
    findingId,
  );
  if (finding.errorCode !== "EINBOUNDPOLICYTRANSIENTSTALE") {
    throw new Error("Repair finding is not a current stale Inbound Policy artifact");
  }
  const evidence = staleInboundPolicyArtifactEvidence();
  if (evidence.artifacts.length === 0 || evidence.unexpectedCount !== 0) {
    throw new Error("Inbound Policy artifact Repair requires only recognized stale artifacts");
  }
  return finalizePlan({
    schemaVersion: 1,
    findingId,
    errorCode: finding.errorCode,
    repairKind: "inbound-policy-stale-artifact-purge",
    mutationCategory: "owner-private-stale-artifact-removal",
    target: { kind: "inbound-policy-artifacts" },
    artifactCount: evidence.artifacts.length,
    evidenceSha256: evidence.evidenceSha256,
    recoverability: "owner-private-artifact-backup",
    automatic: false,
  });
}

const LEGACY_DELEGATION_FIELDS = new Set([
  "version",
  "jobId",
  "from",
  "target",
  "targetThreadId",
  "threadId",
  "task",
  "permissions",
  "turnId",
  "status",
  "result",
  "error",
  "createdAt",
  "updatedAt",
  "completedAt",
]);
const LEGACY_DELEGATION_TERMINAL_STATES = new Set([
  "completed",
  "failed",
  "interrupted",
  "cancelled",
  "unknown",
]);

function assertMigratableLegacyDelegation(job) {
  if (
    job?.version !== 1 ||
    !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(
      job.jobId || "",
    ) ||
    job.kind !== undefined ||
    !LEGACY_DELEGATION_TERMINAL_STATES.has(job.status) ||
    typeof job.from !== "string" ||
    typeof job.target !== "string" ||
    !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(
      job.threadId || "",
    ) ||
    typeof job.task !== "string" ||
    Buffer.byteLength(job.task, "utf8") > 16 * 1024 ||
    !Number.isFinite(Date.parse(job.createdAt || "")) ||
    !Number.isFinite(Date.parse(job.updatedAt || "")) ||
    !Number.isFinite(Date.parse(job.completedAt || "")) ||
    Object.keys(job).some((field) => !LEGACY_DELEGATION_FIELDS.has(field))
  ) {
    const error = new Error(
      "Legacy Job is not an unambiguous terminal Delegation record",
    );
    error.code = "ELEGACYJOBUNSAFE";
    throw error;
  }
  return job;
}

function legacyJobMigrationEvidence() {
  const candidates = listJobsReadOnly()
    .filter((job) => job.kind === undefined)
    .sort((left, right) => left.jobId.localeCompare(right.jobId))
    .map((job) => {
      assertMigratableLegacyDelegation(job);
      return {
        job,
        jobId: job.jobId,
        recordSha256: sha256(JSON.stringify(job)),
      };
    });
  const summary = candidates.map(({ jobId, recordSha256 }) => ({
    jobId,
    recordSha256,
  }));
  return {
    candidates,
    legacyCount: candidates.length,
    evidenceSha256: sha256(JSON.stringify(summary)),
  };
}

function legacyJobRepairPlan(findingId) {
  const finding = findingById(inspectJobs(listJobsReadOnly()), findingId);
  if (finding.errorCode !== "ELEGACYJOB") {
    throw new Error("Repair finding is not a current legacy Job kind warning");
  }
  const evidence = legacyJobMigrationEvidence();
  if (evidence.legacyCount === 0) {
    throw new Error("Legacy Job Repair has no eligible record");
  }
  const target = evidence.candidates[0];
  return finalizePlan({
    schemaVersion: 1,
    findingId,
    errorCode: finding.errorCode,
    repairKind: "legacy-job-kind-migration",
    mutationCategory: "job-schema-migration",
    target: { kind: "job", id: target.jobId },
    legacyCount: evidence.legacyCount,
    targetRecordSha256: target.recordSha256,
    evidenceSha256: evidence.evidenceSha256,
    recoverability: "owner-private-job-backup",
    automatic: false,
  });
}

export function buildRepairPlan({ findingId }) {
  if (typeof findingId !== "string" || findingId.length > 192) {
    throw new Error("Repair finding id is invalid");
  }
  if (findingId === INDEX_FINDING_ID) return indexRepairPlan(findingId);
  if (findingId === LEGACY_JOB_FINDING_ID) {
    return legacyJobRepairPlan(findingId);
  }
  if (findingId === INBOUND_POLICY_STALE_FINDING_ID) {
    return inboundPolicyArtifactRepairPlan(findingId);
  }
  if (findingId.startsWith(CLUSTER_FINDING_PREFIX)) {
    return clusterRepairPlan(findingId);
  }
  throw new Error(
    "Repair supports only Cluster membership redo, stale Ledger index, stale Inbound Policy artifact, and legacy Job kind findings",
  );
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

function backupInboundPolicyArtifacts(transactionDirectory, plan) {
  const evidence = staleInboundPolicyArtifactEvidence();
  if (
    evidence.unexpectedCount !== 0 ||
    evidence.evidenceSha256 !== plan.evidenceSha256 ||
    evidence.artifacts.length !== plan.artifactCount
  ) {
    const error = new Error("Inbound Policy artifact Repair evidence changed before backup");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  const totalBytes = evidence.artifacts.reduce(
    (total, artifact) => total + artifact.bytes,
    0,
  );
  assertRepairCapacity(totalBytes + 512 * 1024);
  const backupDirectory = path.join(
    transactionDirectory,
    INBOUND_POLICY_ARTIFACT_BACKUP_DIR,
  );
  ensurePrivateDirectory(backupDirectory);
  const files = evidence.artifacts.map((artifact) => {
    const contents = readStaleInboundPolicyArtifact(artifact);
    writePrivateFile(path.join(backupDirectory, artifact.name), contents);
    return {
      name: artifact.name,
      bytes: artifact.bytes,
      sha256: artifact.sha256,
    };
  });
  return { kind: "inbound-policy-artifacts", files, evidence };
}

function backupLegacyJob(transactionDirectory, plan) {
  const evidence = legacyJobMigrationEvidence();
  const target = evidence.candidates[0];
  if (
    evidence.legacyCount !== plan.legacyCount ||
    evidence.evidenceSha256 !== plan.evidenceSha256 ||
    target?.jobId !== plan.target.id ||
    target?.recordSha256 !== plan.targetRecordSha256
  ) {
    const error = new Error("Legacy Job Repair evidence changed before backup");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  const contents = `${JSON.stringify(target.job, null, 2)}\n`;
  const bytes = Buffer.byteLength(contents, "utf8");
  if (bytes > LEGACY_JOB_BACKUP_MAX_BYTES) {
    throw new Error("Legacy Job backup exceeds its byte limit");
  }
  assertRepairCapacity(bytes + 512 * 1024);
  writePrivateFile(
    path.join(transactionDirectory, LEGACY_JOB_BACKUP_NAME),
    contents,
  );
  return {
    kind: "legacy-job",
    files: [{
      name: LEGACY_JOB_BACKUP_NAME,
      bytes,
      sha256: sha256(contents),
    }],
    evidence,
  };
}

function backupRepair(transactionDirectory, plan) {
  if (plan.repairKind === "cluster-membership-redo") {
    return backupCluster(transactionDirectory, plan);
  }
  if (plan.repairKind === "delivery-ledger-index-rebuild") {
    return backupIndex(transactionDirectory, plan);
  }
  if (plan.repairKind === "legacy-job-kind-migration") {
    return backupLegacyJob(transactionDirectory, plan);
  }
  return backupInboundPolicyArtifacts(transactionDirectory, plan);
}

function boundedErrorCode(error) {
  return /^[A-Z0-9_]{1,32}$/.test(error?.code || "")
    ? error.code
    : "EREPAIRFAILED";
}

function verifyRepair(plan) {
  if (plan.repairKind === "legacy-job-kind-migration") {
    const target = readJob(plan.target.id);
    if (!target || target.kind !== "delegation") {
      const error = new Error("Legacy Job Repair did not migrate its exact target");
      error.code = "EREPAIRVERIFY";
      throw error;
    }
    const { kind: _kind, ...originalShape } = target;
    if (sha256(JSON.stringify(originalShape)) !== plan.targetRecordSha256) {
      const error = new Error("Legacy Job Repair changed fields beyond kind");
      error.code = "EREPAIRVERIFY";
      throw error;
    }
    const remaining = listJobsReadOnly().filter(
      (job) => job.kind === undefined,
    ).length;
    if (remaining !== plan.legacyCount - 1) {
      const error = new Error("Legacy Job Repair remaining count is inconsistent");
      error.code = "EREPAIRVERIFY";
      throw error;
    }
    return {
      id: plan.findingId,
      status: remaining === 0 ? "pass" : "progress",
      remainingCount: remaining,
    };
  }
  const checks = plan.repairKind === "cluster-membership-redo"
    ? inspectNodeDirectory({ stateDir: CXMSG_STATE_DIR })
    : plan.repairKind === "delivery-ledger-index-rebuild"
      ? inspectRouteState({ stateDir: CXMSG_STATE_DIR })
      : inspectInboundPolicies({ stateDir: CXMSG_STATE_DIR });
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
  if (plan.repairKind === "delivery-ledger-index-rebuild") {
    return rebuildDeliveryLedgerIndex({
      expectedLedgerManifestSha256: backup.evidence.ledgerManifestSha256,
      expectedIndexGenerationSha256: backup.evidence.indexGenerationSha256,
    });
  }
  if (plan.repairKind === "legacy-job-kind-migration") {
    return withRetentionMutation(async () => {
      const evidence = legacyJobMigrationEvidence();
      const target = evidence.candidates[0];
      if (
        evidence.legacyCount !== plan.legacyCount ||
        evidence.evidenceSha256 !== backup.evidence.evidenceSha256 ||
        target?.jobId !== plan.target.id ||
        target?.recordSha256 !== plan.targetRecordSha256
      ) {
        const error = new Error(
          "Legacy Job Repair evidence changed before mutation",
        );
        error.code = "EREPAIRSTALE";
        throw error;
      }
      await withJobLock(plan.target.id, () => {
        const current = readJob(plan.target.id);
        if (
          !current ||
          current.kind !== undefined ||
          sha256(JSON.stringify(current)) !== plan.targetRecordSha256
        ) {
          const error = new Error(
            "Legacy Job Repair target changed before mutation",
          );
          error.code = "EREPAIRSTALE";
          throw error;
        }
        writeJob({ ...current, kind: "delegation" });
      });
      return {
        migrated: true,
        remainingCount: plan.legacyCount - 1,
      };
    });
  }
  return purgeStaleInboundPolicyArtifacts({
    expectedEvidenceSha256: backup.evidence.evidenceSha256,
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
  return withRepairMutation(async () => {
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
      try {
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
      } catch {}
      try {
        atomicWriteJson(manifestPath, {
          ...manifest,
          phase: "failed",
          errorCode: code,
          updatedAt: failedAt,
        });
      } catch {}
      throw error;
    }
  });
}
