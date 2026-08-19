import { createHash, randomUUID } from "node:crypto";
import {
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
import { listConversationMessageIds } from "./conversations.js";
import { listDeliveryLedgerIndexed } from "./delivery-ledger.js";
import { requireNoFollowFlag } from "./file-safety.js";
import { listGroupConversationMessageIds } from "./group-conversations.js";
import { listExecutionThreads } from "./node-directory.js";
import {
  assertRetentionReadableNoCreate,
  withRetentionMutation,
} from "./retention-barrier.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const JOB_RETENTION_MIN_AGE_MS = 7 * 24 * 60 * 60 * 1_000;
export const JOB_RETENTION_STATE_DIR = path.join(CXMSG_STATE_DIR, "job-retention");
export const JOB_RETENTION_TRANSACTIONS_DIR = path.join(
  JOB_RETENTION_STATE_DIR,
  "transactions",
);
export const JOB_RETENTION_RECEIPTS_DIR = path.join(
  JOB_RETENTION_STATE_DIR,
  "receipts",
);

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const TERMINAL_JOB_STATES = new Set(["completed", "failed", "expired", "cancelled"]);
const ARCHIVE_QUOTA_BYTES = 1024 * 1024 * 1024;
const ARCHIVE_TRANSACTION_LIMIT = 1_024;
const JOB_FILE_LIMIT_BYTES = 1024 * 1024;

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function timestamp(label, value) {
  const parsed = Date.parse(value || "");
  if (!Number.isFinite(parsed)) throw new Error(`${label} must be an ISO timestamp`);
  return parsed;
}

function pathsFor(stateDir = CXMSG_STATE_DIR) {
  const state = path.join(stateDir, "job-retention");
  return {
    state,
    activeJobs: path.join(stateDir, "jobs"),
    transactions: path.join(state, "transactions"),
    receipts: path.join(state, "receipts"),
  };
}

function privateDirectory(directory) {
  const metadata = lstatSync(directory);
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    const error = new Error(`Job archive directory is not owner-private: ${path.basename(directory)}`);
    error.code = "EJOBARCHIVEIDENTITY";
    throw error;
  }
  return metadata;
}

function ensurePrivateDirectory(directory) {
  if (!existsSync(directory)) mkdirSync(directory, { recursive: true, mode: 0o700 });
  return privateDirectory(directory);
}

function privateFile(filename, maxBytes = JOB_FILE_LIMIT_BYTES) {
  const metadata = lstatSync(filename);
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0 ||
    metadata.size > maxBytes
  ) {
    const error = new Error(`Job archive file is not bounded and owner-private: ${path.basename(filename)}`);
    error.code = "EJOBARCHIVEIDENTITY";
    throw error;
  }
  return metadata;
}

function syncDirectory(directory) {
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
      requireNoFollowFlag(),
    0o600,
  );
  try {
    writeFileSync(descriptor, `${JSON.stringify(value, null, 2)}\n`);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  renameSync(temporary, filename);
  syncDirectory(path.dirname(filename));
  return value;
}

function readRecord(filename, label) {
  const metadata = privateFile(filename);
  const contents = readFileSync(filename);
  let value;
  try {
    value = JSON.parse(contents.toString("utf8"));
  } catch {
    const error = new Error(`${label} is malformed`);
    error.code = "EJOBARCHIVESCHEMA";
    throw error;
  }
  return { value, bytes: metadata.size, sha256: sha256(contents) };
}

function validJobIdentity(job, jobId) {
  return Boolean(job?.version === 1 && job.jobId === jobId && UUID_PATTERN.test(jobId));
}

function readActiveJobRecords(stateDir = CXMSG_STATE_DIR) {
  const directory = pathsFor(stateDir).activeJobs;
  if (!existsSync(directory)) return [];
  privateDirectory(directory);
  return readdirSync(directory)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .map((name) => {
      const jobId = name.slice(0, -5);
      if (!UUID_PATTERN.test(jobId)) {
        const error = new Error("Job directory contains an invalid record identity");
        error.code = "EJOBARCHIVESCHEMA";
        throw error;
      }
      const record = readRecord(path.join(directory, name), "Job record");
      if (!validJobIdentity(record.value, jobId)) {
        const error = new Error("Job record failed identity validation");
        error.code = "EJOBARCHIVESCHEMA";
        throw error;
      }
      return { job: record.value, bytes: record.bytes, sha256: record.sha256 };
    });
}

function protect(map, jobId, reason) {
  if (!UUID_PATTERN.test(jobId || "")) return;
  const reasons = map.get(jobId) || new Set();
  reasons.add(reason);
  map.set(jobId, reasons);
}

export function jobRetentionReferences({
  jobs = [],
  ledger = [],
  executionThreads = [],
  conversationMessageIds = [],
} = {}) {
  const references = new Map();
  for (const job of jobs) {
    if (
      job.correlation?.kind === "peer-reply" &&
      job.correlation.replyToMessageId !== job.jobId
    ) {
      protect(references, job.correlation.replyToMessageId, "job_reply_correlation");
    }
  }
  for (const record of ledger) {
    protect(
      references,
      record.logicalMessage?.route?.trigger_job_id,
      "delivery_after_job_trigger",
    );
    for (const delivery of record.deliveries || []) {
      protect(references, delivery.schedule?.triggerJobId, "team_after_job_trigger");
    }
  }
  for (const execution of executionThreads) {
    protect(references, execution.jobId, "execution_thread");
  }
  for (const messageId of conversationMessageIds) {
    protect(references, messageId, "conversation_source");
  }
  return references;
}

function selectionDigest(plan) {
  return sha256(JSON.stringify({
    schemaVersion: plan.schemaVersion,
    cutoff: plan.cutoff,
    eligible: plan.category.eligible,
  }));
}

export function planJobRetention(
  { before, records = [], references = new Map() },
  { now = Date.now() } = {},
) {
  const cutoff = timestamp("Job retention cutoff", before);
  if (!Number.isFinite(now)) throw new Error("Job retention clock is invalid");
  if (cutoff > now - JOB_RETENTION_MIN_AGE_MS) {
    throw new Error("Job retention cutoff must preserve at least 7 days");
  }
  const category = {
    eligible: [],
    blocked: [],
    retainedByAge: 0,
    estimatedBytes: 0,
  };
  for (const record of records) {
    const { job, bytes, sha256: recordSha256 } = record;
    if (!validJobIdentity(job, job.jobId)) {
      throw new Error("Job retention received an invalid Job record");
    }
    const completedAt = Date.parse(job.completedAt || "");
    if (Number.isFinite(completedAt) && completedAt >= cutoff) {
      category.retainedByAge += 1;
      continue;
    }
    const reasons = [];
    if (!TERMINAL_JOB_STATES.has(job.status)) reasons.push("nonterminal_or_reconcilable");
    if (!Number.isFinite(completedAt)) reasons.push("missing_terminal_timestamp");
    if (
      job.kind === "claude-request" &&
      job.reply &&
      job.reply.status !== "delivered"
    ) {
      reasons.push("claude_request_reply_unresolved");
    }
    for (const reason of references.get(job.jobId) || []) reasons.push(reason);
    const candidate = {
      jobId: job.jobId,
      kind: job.kind ?? "delegation",
      status: job.status,
      completedAt: Number.isFinite(completedAt)
        ? new Date(completedAt).toISOString()
        : null,
      recordSha256,
      estimatedBytes: bytes,
    };
    if (reasons.length > 0) {
      category.blocked.push({ ...candidate, reasons: [...new Set(reasons)].sort() });
    } else {
      category.eligible.push(candidate);
      category.estimatedBytes += bytes;
    }
  }
  const plan = {
    schemaVersion: 1,
    generatedAt: new Date(now).toISOString(),
    cutoff: new Date(cutoff).toISOString(),
    policy: {
      automaticDeletion: false,
      mutationKind: "recoverable-archive",
      minimumAgeDays: JOB_RETENTION_MIN_AGE_MS / (24 * 60 * 60 * 1_000),
      reconcilableStatesArchived: false,
    },
    category,
  };
  return { ...plan, planDigest: selectionDigest(plan) };
}

export async function buildJobRetentionPlan(
  { before },
  {
    stateDir = CXMSG_STATE_DIR,
    jobReader = null,
    ledgerReader = listDeliveryLedgerIndexed,
    executionReader = listExecutionThreads,
    conversationReader = () => [
      ...listConversationMessageIds(),
      ...listGroupConversationMessageIds(),
    ],
    readable = assertRetentionReadableNoCreate,
    now = Date.now(),
  } = {},
) {
  readable();
  const records = jobReader ? await jobReader() : readActiveJobRecords(stateDir);
  const jobs = records.map((record) => record.job);
  const [ledger, executionThreads, conversationMessageIds] = await Promise.all([
    ledgerReader(),
    executionReader(),
    conversationReader(),
  ]);
  return planJobRetention(
    {
      before,
      records,
      references: jobRetentionReferences({
        jobs,
        ledger,
        executionThreads,
        conversationMessageIds,
      }),
    },
    { now },
  );
}

function transactionDirectory(archiveId, stateDir = CXMSG_STATE_DIR) {
  if (!UUID_PATTERN.test(archiveId || "")) throw new Error("Job archive id is invalid");
  return path.join(pathsFor(stateDir).transactions, archiveId);
}

function manifestPath(archiveId, stateDir = CXMSG_STATE_DIR) {
  return path.join(transactionDirectory(archiveId, stateDir), "manifest.json");
}

function receiptPath(archiveId, stateDir = CXMSG_STATE_DIR) {
  return path.join(pathsFor(stateDir).receipts, `${archiveId}.json`);
}

function itemPaths(archiveId, jobId, stateDir = CXMSG_STATE_DIR) {
  const item = path.join(transactionDirectory(archiveId, stateDir), "items", jobId);
  return {
    item,
    active: path.join(pathsFor(stateDir).activeJobs, `${jobId}.json`),
    archived: path.join(item, "job.json"),
  };
}

function validManifest(manifest, archiveId) {
  const archiveCommitted = Number.isFinite(Date.parse(manifest?.committedAt || "")) &&
    SHA256_PATTERN.test(manifest?.receiptSha256 || "");
  const restoreValid = Boolean(
    manifest?.restore &&
      UUID_PATTERN.test(manifest.restore.restoreId || "") &&
      Number.isFinite(Date.parse(manifest.restore.startedAt || "")) &&
      (manifest.status === "restoring" ||
        (Number.isFinite(Date.parse(manifest.restore.completedAt || "")) &&
          SHA256_PATTERN.test(manifest.restore.receiptSha256 || ""))),
  );
  return Boolean(
    manifest?.schemaVersion === 1 &&
      manifest.archiveId === archiveId &&
      ["archiving", "committed", "restoring", "restored"].includes(manifest.status) &&
      SHA256_PATTERN.test(manifest.planDigest || "") &&
      Number.isFinite(Date.parse(manifest.cutoff || "")) &&
      Number.isFinite(Date.parse(manifest.createdAt || "")) &&
      Array.isArray(manifest.items) &&
      manifest.items.length > 0 &&
      manifest.items.every((item) =>
        UUID_PATTERN.test(item?.jobId || "") &&
        ["pending", "archived", "restored"].includes(item.state) &&
        SHA256_PATTERN.test(item.recordSha256 || "") &&
        Number.isSafeInteger(item.estimatedBytes) &&
        item.estimatedBytes >= 0
      ) &&
      new Set(manifest.items.map((item) => item.jobId)).size === manifest.items.length &&
      (manifest.status === "archiving"
        ? manifest.committedAt === null &&
          manifest.receiptSha256 === null &&
          manifest.restore === null
        : manifest.status === "committed"
          ? archiveCommitted && manifest.restore === null
          : archiveCommitted && restoreValid)
  );
}

function readManifest(archiveId, stateDir = CXMSG_STATE_DIR) {
  const manifest = readRecord(manifestPath(archiveId, stateDir), "Job archive manifest").value;
  if (!validManifest(manifest, archiveId)) {
    const error = new Error("Job archive manifest failed validation");
    error.code = "EJOBARCHIVESCHEMA";
    throw error;
  }
  return manifest;
}

function writeManifest(manifest, stateDir = CXMSG_STATE_DIR) {
  return atomicWriteJson(manifestPath(manifest.archiveId, stateDir), manifest);
}

function requireArchiveReceipt(manifest, stateDir = CXMSG_STATE_DIR) {
  const receipt = readRecord(
    receiptPath(manifest.archiveId, stateDir),
    "Job archive receipt",
  ).value;
  if (
    receipt?.schemaVersion !== 1 ||
    receipt.outcome !== "archived" ||
    receipt.archiveId !== manifest.archiveId ||
    receipt.planDigest !== manifest.planDigest ||
    receipt.itemCount !== manifest.items.length ||
    receipt.committedAt !== manifest.committedAt ||
    manifest.receiptSha256 !== sha256(JSON.stringify(receipt))
  ) {
    const error = new Error("Job archive receipt failed validation");
    error.code = "EJOBARCHIVESCHEMA";
    throw error;
  }
  return receipt;
}

function validateJobFile(filename, item) {
  const record = readRecord(filename, "Archived Job");
  if (
    record.sha256 !== item.recordSha256 ||
    record.bytes !== item.estimatedBytes ||
    !validJobIdentity(record.value, item.jobId)
  ) {
    const error = new Error("Job archive source changed after planning");
    error.code = "EJOBARCHIVESTALE";
    throw error;
  }
  return record.value;
}

function classifyItem(manifest, item, stateDir = CXMSG_STATE_DIR) {
  const paths = itemPaths(manifest.archiveId, item.jobId, stateDir);
  const active = existsSync(paths.active);
  const archived = existsSync(paths.archived);
  if (active === archived) {
    const error = new Error("Job archive item cannot be recovered safely");
    error.code = "EJOBARCHIVEPAIR";
    throw error;
  }
  if (active) validateJobFile(paths.active, item);
  else validateJobFile(paths.archived, item);
  return { classification: active ? "active" : "archived", paths };
}

function move(source, destination) {
  renameSync(source, destination);
  syncDirectory(path.dirname(source));
  if (path.dirname(source) !== path.dirname(destination)) {
    syncDirectory(path.dirname(destination));
  }
}

function archiveUsage(stateDir = CXMSG_STATE_DIR) {
  const roots = pathsFor(stateDir);
  if (!existsSync(roots.state)) return { bytes: 0, transactions: 0 };
  let bytes = 0;
  let transactions = 0;
  const visit = (directory, depth = 0) => {
    if (depth > 5) throw new Error("Job archive exceeds its depth limit");
    ensurePrivateDirectory(directory);
    for (const name of readdirSync(directory).sort()) {
      if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
        throw new Error("Job archive contains an invalid entry");
      }
      const filename = path.join(directory, name);
      const metadata = lstatSync(filename);
      if (metadata.isDirectory() && !metadata.isSymbolicLink()) visit(filename, depth + 1);
      else if (metadata.isFile() && !metadata.isSymbolicLink() && metadata.nlink === 1) {
        privateFile(filename);
        bytes += metadata.size;
      } else throw new Error("Job archive contains an unsupported entry");
    }
  };
  if (existsSync(roots.transactions)) {
    ensurePrivateDirectory(roots.transactions);
    transactions = readdirSync(roots.transactions).filter((name) => UUID_PATTERN.test(name)).length;
  }
  visit(roots.state);
  return { bytes, transactions };
}

function assertCapacity(additionalBytes, stateDir = CXMSG_STATE_DIR) {
  const usage = archiveUsage(stateDir);
  if (
    usage.bytes + additionalBytes > ARCHIVE_QUOTA_BYTES ||
    usage.transactions + 1 > ARCHIVE_TRANSACTION_LIMIT
  ) {
    const error = new Error("Job archive reached its bounded retention limit");
    error.code = "EJOBARCHIVEQUOTA";
    throw error;
  }
}

async function finishArchive(manifest, { stateDir = CXMSG_STATE_DIR, fault = null } = {}) {
  for (const item of manifest.items) {
    const { classification, paths } = classifyItem(manifest, item, stateDir);
    if (classification === "active") {
      ensurePrivateDirectory(paths.item);
      move(paths.active, paths.archived);
      item.state = "archived";
      writeManifest(manifest, stateDir);
      if (fault) await fault("after-job-move", manifest, item);
    }
  }
  const filename = receiptPath(manifest.archiveId, stateDir);
  const estimatedBytes = manifest.items.reduce(
    (sum, item) => sum + item.estimatedBytes,
    0,
  );
  let receipt = null;
  if (existsSync(filename)) {
    receipt = readRecord(filename, "Job archive receipt").value;
    if (
      receipt?.schemaVersion !== 1 ||
      receipt.outcome !== "archived" ||
      receipt.archiveId !== manifest.archiveId ||
      receipt.planDigest !== manifest.planDigest ||
      receipt.cutoff !== manifest.cutoff ||
      receipt.itemCount !== manifest.items.length ||
      receipt.estimatedBytes !== estimatedBytes ||
      !Number.isFinite(Date.parse(receipt.committedAt || "")) ||
      receipt.automaticDeletion !== false
    ) {
      const error = new Error("Job archive receipt failed recovery validation");
      error.code = "EJOBARCHIVESCHEMA";
      throw error;
    }
  } else {
    receipt = atomicWriteJson(filename, {
      schemaVersion: 1,
      outcome: "archived",
      archiveId: manifest.archiveId,
      planDigest: manifest.planDigest,
      cutoff: manifest.cutoff,
      itemCount: manifest.items.length,
      estimatedBytes,
      committedAt: new Date().toISOString(),
      automaticDeletion: false,
    });
  }
  if (fault) await fault("after-archive-receipt", manifest);
  manifest.status = "committed";
  manifest.committedAt = receipt.committedAt;
  manifest.receiptSha256 = sha256(JSON.stringify(receipt));
  writeManifest(manifest, stateDir);
  return receipt;
}

async function finishRestore(manifest, { stateDir = CXMSG_STATE_DIR, fault = null } = {}) {
  for (const item of manifest.items) {
    const { classification, paths } = classifyItem(manifest, item, stateDir);
    if (classification === "archived") {
      move(paths.archived, paths.active);
      item.state = "restored";
      writeManifest(manifest, stateDir);
      if (fault) await fault("after-job-restore", manifest, item);
    }
  }
  const completedAt = new Date().toISOString();
  const receipt = atomicWriteJson(
    path.join(
      pathsFor(stateDir).receipts,
      `${manifest.archiveId}.restore-${manifest.restore.restoreId}.json`,
    ),
    {
      schemaVersion: 1,
      outcome: "restored",
      archiveId: manifest.archiveId,
      restoreId: manifest.restore.restoreId,
      itemCount: manifest.items.length,
      completedAt,
    },
  );
  manifest.status = "restored";
  manifest.restore.completedAt = completedAt;
  manifest.restore.receiptSha256 = sha256(JSON.stringify(receipt));
  writeManifest(manifest, stateDir);
  return receipt;
}

async function recoverLocked({ stateDir = CXMSG_STATE_DIR } = {}) {
  const roots = pathsFor(stateDir);
  if (!existsSync(roots.transactions)) return [];
  privateDirectory(roots.transactions);
  ensurePrivateDirectory(roots.receipts);
  const recovered = [];
  for (const archiveId of readdirSync(roots.transactions).sort()) {
    if (!UUID_PATTERN.test(archiveId)) throw new Error("Job archive id is invalid");
    const manifest = readManifest(archiveId, stateDir);
    if (manifest.status === "archiving") {
      await finishArchive(manifest, { stateDir });
      recovered.push({ archiveId, outcome: "archived" });
    } else if (manifest.status === "restoring") {
      requireArchiveReceipt(manifest, stateDir);
      await finishRestore(manifest, { stateDir });
      recovered.push({ archiveId, outcome: "restored" });
    }
  }
  return recovered;
}

export async function archiveJobs(
  { before, expectedPlanDigest },
  {
    stateDir = CXMSG_STATE_DIR,
    now = Date.now(),
    fault = null,
    adapters = {},
    mutation = withRetentionMutation,
  } = {},
) {
  if (!SHA256_PATTERN.test(expectedPlanDigest || "")) {
    throw new Error("Job archive requires the exact plan digest");
  }
  const build = () => buildJobRetentionPlan(
    { before },
    { stateDir, now, ...adapters },
  );
  const preview = await build();
  if (preview.planDigest !== expectedPlanDigest) {
    const error = new Error("Job retention plan changed; generate a new plan");
    error.code = "EJOBARCHIVESTALE";
    throw error;
  }
  return mutation(async () => {
    await recoverLocked({ stateDir });
    const plan = await build();
    if (plan.planDigest !== expectedPlanDigest) {
      const error = new Error("Job retention plan changed; generate a new plan");
      error.code = "EJOBARCHIVESTALE";
      throw error;
    }
    if (plan.category.eligible.length === 0) {
      throw new Error("Job retention plan has no eligible Jobs");
    }
    assertCapacity(plan.category.estimatedBytes + 256 * 1024, stateDir);
    const roots = pathsFor(stateDir);
    ensurePrivateDirectory(roots.transactions);
    ensurePrivateDirectory(roots.receipts);
    const archiveId = randomUUID();
    const transaction = transactionDirectory(archiveId, stateDir);
    ensurePrivateDirectory(transaction);
    ensurePrivateDirectory(path.join(transaction, "items"));
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
        jobId: candidate.jobId,
        state: "pending",
        recordSha256: candidate.recordSha256,
        estimatedBytes: candidate.estimatedBytes,
      })),
    };
    writeManifest(manifest, stateDir);
    return finishArchive(manifest, { stateDir, fault });
  });
}

export async function restoreJobs(
  { archiveId },
  { stateDir = CXMSG_STATE_DIR, fault = null, mutation = withRetentionMutation } = {},
) {
  return mutation(async () => {
    await recoverLocked({ stateDir });
    const manifest = readManifest(archiveId, stateDir);
    if (manifest.status !== "committed" || manifest.restore !== null) {
      throw new Error("Job archive is not restorable");
    }
    requireArchiveReceipt(manifest, stateDir);
    for (const item of manifest.items) {
      const { classification } = classifyItem(manifest, item, stateDir);
      if (classification !== "archived") {
        throw new Error("Job archive restore target is not empty");
      }
    }
    manifest.status = "restoring";
    manifest.restore = {
      restoreId: randomUUID(),
      startedAt: new Date().toISOString(),
      completedAt: null,
      receiptSha256: null,
    };
    writeManifest(manifest, stateDir);
    return finishRestore(manifest, { stateDir, fault });
  });
}

export async function recoverJobRetention(
  { mutation = withRetentionMutation, ...options } = {},
) {
  return mutation(() => recoverLocked(options));
}

export function listArchivedJobsForRetention(
  { stateDir = CXMSG_STATE_DIR, readable = assertRetentionReadableNoCreate } = {},
) {
  readable();
  const roots = pathsFor(stateDir);
  if (!existsSync(roots.transactions)) return [];
  ensurePrivateDirectory(roots.transactions);
  const jobs = [];
  for (const archiveId of readdirSync(roots.transactions).sort()) {
    if (!UUID_PATTERN.test(archiveId)) throw new Error("Job archive id is invalid");
    const manifest = readManifest(archiveId, stateDir);
    for (const item of manifest.items) {
      const paths = itemPaths(archiveId, item.jobId, stateDir);
      if (existsSync(paths.archived)) jobs.push(validateJobFile(paths.archived, item));
    }
  }
  return jobs;
}

export function inspectJobRetentionState({ stateDir = CXMSG_STATE_DIR } = {}) {
  const roots = pathsFor(stateDir);
  if (!existsSync(roots.state)) return { status: "missing", archives: 0, nonterminal: 0 };
  try {
    privateDirectory(roots.state);
    privateDirectory(roots.transactions);
    privateDirectory(roots.receipts);
    let nonterminal = 0;
    const names = readdirSync(roots.transactions).sort();
    for (const archiveId of names) {
      if (!UUID_PATTERN.test(archiveId)) throw new Error("Job archive id is invalid");
      const manifest = readManifest(archiveId, stateDir);
      if (["committed", "restoring", "restored"].includes(manifest.status)) {
        requireArchiveReceipt(manifest, stateDir);
      }
      if (!["committed", "restored"].includes(manifest.status)) nonterminal += 1;
      for (const item of manifest.items) classifyItem(manifest, item, stateDir);
    }
    return { status: "secure", archives: names.length, nonterminal };
  } catch (error) {
    return {
      status: "invalid",
      archives: 0,
      nonterminal: 0,
      errorCode: /^[A-Z0-9_]{1,32}$/.test(error?.code || "")
        ? error.code
        : "EJOBARCHIVESTATE",
    };
  }
}
