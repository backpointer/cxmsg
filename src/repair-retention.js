import { createHash } from "node:crypto";
import {
  existsSync,
  lstatSync,
  readFileSync,
  readdirSync,
} from "node:fs";
import path from "node:path";
import { inspectRepairState } from "./inspectors.js";
import {
  REPAIR_STATE_DIR,
  REPAIR_RECEIPTS_DIR,
  REPAIR_TRANSACTIONS_DIR,
} from "./repair.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const REPAIR_RETENTION_MIN_AGE_MS = 90 * 24 * 60 * 60 * 1_000;

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function timestamp(label, value) {
  const parsed = Date.parse(value || "");
  if (!Number.isFinite(parsed)) throw new Error(`${label} must be an ISO timestamp`);
  return parsed;
}

function readJson(filename, label) {
  const metadata = lstatSync(filename);
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0 ||
    metadata.size > 1024 * 1024
  ) {
    throw new Error(`${label} is not a bounded owner-private file`);
  }
  const contents = readFileSync(filename);
  let value;
  try {
    value = JSON.parse(contents.toString("utf8"));
  } catch {
    throw new Error(`${label} is malformed`);
  }
  return { value, bytes: metadata.size, sha256: sha256(contents) };
}

function privateTreeBytes(directory, depth = 0) {
  if (depth > 3) throw new Error("Repair retention source exceeds its depth limit");
  const metadata = lstatSync(directory);
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    throw new Error("Repair retention source is not an owner-private directory");
  }
  let bytes = 0;
  for (const name of readdirSync(directory).sort()) {
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) {
      throw new Error("Repair retention source contains an invalid entry");
    }
    const filename = path.join(directory, name);
    const entry = lstatSync(filename);
    if (
      entry.isSymbolicLink() ||
      (typeof process.getuid === "function" && entry.uid !== process.getuid()) ||
      (entry.mode & 0o077) !== 0
    ) {
      throw new Error("Repair retention source contains a non-private entry");
    }
    if (entry.isDirectory()) bytes += privateTreeBytes(filename, depth + 1);
    else if (entry.isFile() && entry.nlink === 1) bytes += entry.size;
    else throw new Error("Repair retention source contains an unsupported entry");
  }
  return bytes;
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
        mutationEnabled: false,
        minimumAgeDays: 90,
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
    const transactionDirectory = path.join(
      REPAIR_TRANSACTIONS_DIR,
      transactionId,
    );
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
    const candidate = {
      transactionId,
      completedAt: new Date(completedAt).toISOString(),
      repairKind: receipt.repairKind,
      planDigest: receipt.planDigest,
      manifestSha256: manifestRecord.sha256,
      receiptSha256: receiptRecord.sha256,
      estimatedBytes:
        privateTreeBytes(transactionDirectory) + receiptRecord.bytes,
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
      mutationEnabled: false,
      minimumAgeDays: 90,
      terminalState: "completed",
    },
    category,
  };
  return { ...plan, planDigest: selectionDigest(plan) };
}
