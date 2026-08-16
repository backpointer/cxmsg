import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { requireNoFollowFlag } from "./file-safety.js";
import { withFileLock } from "./file-lock.js";
import { readNode, readNodeTombstone } from "./node-directory.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const INBOUND_POLICIES_DIR = path.join(
  CXMSG_STATE_DIR,
  "inbound-policies",
);
export const INBOUND_POLICY_LOCK_PATH = path.join(
  CXMSG_STATE_DIR,
  "inbound-policies.lock",
);
export const INBOUND_POLICY_MAX_RECORD_BYTES = 128 * 1024;
export const INBOUND_POLICY_MAX_RECORDS = 1024;
export const INBOUND_POLICY_MAX_RULES = 4096;
export const INBOUND_POLICY_MAX_RULES_PER_TARGET = 256;
export const INBOUND_POLICY_TRANSIENT_GRACE_MS = 30_000;
export const INBOUND_POLICY_FEATURE_ACTIVE = true;
export const INBOUND_POLICY_STALE_FINDING_ID = "inbound-policies.entries";

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const NODE_KEY_PATTERN = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i;
const POLICY_RECORD_NAME_PATTERN = /^[0-9a-f]{64}\.json$/;
const POLICY_TRANSIENT_NAME_PATTERN = /^[0-9a-f]{64}\.json\.[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\.(?:tmp|deleting)$/i;
const SELECTOR_KINDS = new Set([
  "sender-node",
  "sender-project",
  "unknown-sender",
]);
const POLICY_FIELDS = new Set([
  "schemaVersion",
  "recordType",
  "targetNodeKey",
  "revision",
  "rules",
  "createdAt",
  "updatedAt",
]);
const RULE_FIELDS = Object.freeze({
  "sender-node": new Set([
    "ruleId",
    "selectorKind",
    "selectorNodeKey",
    "createdAt",
  ]),
  "sender-project": new Set([
    "ruleId",
    "selectorKind",
    "projectId",
    "createdAt",
  ]),
  "unknown-sender": new Set(["ruleId", "selectorKind", "createdAt"]),
});

function validTimestamp(value) {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

export function normalizeInboundNodeKey(value) {
  const match = NODE_KEY_PATTERN.exec(value || "");
  if (!match) throw new Error("Inbound policy Node key is invalid");
  return `${match[1].toLowerCase()}:${match[2].toLowerCase()}`;
}

export function inboundPolicyFilename(targetNodeKey) {
  const normalized = normalizeInboundNodeKey(targetNodeKey);
  return `${createHash("sha256").update(normalized).digest("hex")}.json`;
}

function policyPath(targetNodeKey) {
  return path.join(INBOUND_POLICIES_DIR, inboundPolicyFilename(targetNodeKey));
}

function canonicalValue(value) {
  if (Array.isArray(value)) return value.map(canonicalValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalValue(value[key])]),
    );
  }
  return value;
}

function policyDigest(record) {
  return createHash("sha256")
    .update(JSON.stringify(canonicalValue(record)))
    .digest("hex");
}

export function classifyInboundPolicyEntry(
  name,
  mtimeMs,
  now = Date.now(),
) {
  if (POLICY_RECORD_NAME_PATTERN.test(name)) return "record";
  if (!POLICY_TRANSIENT_NAME_PATTERN.test(name)) return "unexpected";
  return Number.isFinite(mtimeMs) && now - mtimeMs <= INBOUND_POLICY_TRANSIENT_GRACE_MS
    ? "transient"
    : "stale-transient";
}

function selectorIdentity(rule) {
  if (rule.selectorKind === "sender-node") {
    return `sender-node:${rule.selectorNodeKey}`;
  }
  if (rule.selectorKind === "sender-project") {
    return `sender-project:${rule.projectId}`;
  }
  return "unknown-sender";
}

function validRule(rule) {
  if (
    !rule ||
    !UUID_PATTERN.test(rule.ruleId || "") ||
    !SELECTOR_KINDS.has(rule.selectorKind) ||
    !validTimestamp(rule.createdAt) ||
    !Object.keys(rule).every((field) =>
      RULE_FIELDS[rule.selectorKind].has(field),
    )
  ) {
    return false;
  }
  if (rule.selectorKind === "sender-node") {
    try {
      return rule.selectorNodeKey === normalizeInboundNodeKey(rule.selectorNodeKey);
    } catch {
      return false;
    }
  }
  if (rule.selectorKind === "sender-project") {
    return (
      UUID_PATTERN.test(rule.projectId || "") &&
      rule.projectId === rule.projectId.toLowerCase()
    );
  }
  return true;
}

export function validInboundPolicyRecord(record, filenameStem = null) {
  if (
    !record ||
    record.schemaVersion !== 1 ||
    record.recordType !== "inbound-peer-policy" ||
    !Number.isSafeInteger(record.revision) ||
    record.revision < 1 ||
    !Array.isArray(record.rules) ||
    record.rules.length > INBOUND_POLICY_MAX_RULES_PER_TARGET ||
    !validTimestamp(record.createdAt) ||
    !validTimestamp(record.updatedAt) ||
    Date.parse(record.updatedAt) < Date.parse(record.createdAt) ||
    !Object.keys(record).every((field) => POLICY_FIELDS.has(field))
  ) {
    return false;
  }
  let targetNodeKey;
  try {
    targetNodeKey = normalizeInboundNodeKey(record.targetNodeKey);
  } catch {
    return false;
  }
  if (
    targetNodeKey !== record.targetNodeKey ||
    (filenameStem !== null &&
      `${filenameStem}.json` !== inboundPolicyFilename(targetNodeKey))
  ) {
    return false;
  }
  const ruleIds = new Set();
  const selectors = new Set();
  for (const rule of record.rules) {
    if (!validRule(rule)) return false;
    const selector = selectorIdentity(rule);
    if (ruleIds.has(rule.ruleId) || selectors.has(selector)) return false;
    ruleIds.add(rule.ruleId);
    selectors.add(selector);
  }
  return true;
}

function assertPrivateMetadata(metadata, expectedType) {
  const validType =
    expectedType === "directory" ? metadata.isDirectory() : metadata.isFile();
  if (
    !validType ||
    metadata.isSymbolicLink() ||
    (expectedType === "file" && metadata.nlink !== 1) ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    throw new Error("Inbound policy state is not owner-private");
  }
  return metadata;
}

function privateMetadata(filename, expectedType) {
  return assertPrivateMetadata(lstatSync(filename), expectedType);
}

function readPrivatePolicyFile(filename) {
  const descriptor = openSync(
    filename,
    constants.O_RDONLY | requireNoFollowFlag(),
  );
  try {
    const metadata = assertPrivateMetadata(fstatSync(descriptor), "file");
    if (metadata.size > INBOUND_POLICY_MAX_RECORD_BYTES) {
      const error = new Error("Inbound policy record exceeds its bounded size");
      error.code = "EINBOUNDPOLICYSIZE";
      throw error;
    }
    const bytes = readFileSync(descriptor);
    return { metadata, bytes, contents: bytes.toString("utf8") };
  } finally {
    closeSync(descriptor);
  }
}

function ensurePrivateDirectory(directory) {
  if (!existsSync(directory)) mkdirSync(directory, { recursive: true, mode: 0o700 });
  privateMetadata(directory, "directory");
  chmodSync(directory, 0o700);
}

function ensureStore() {
  ensurePrivateDirectory(CXMSG_STATE_DIR);
  ensurePrivateDirectory(INBOUND_POLICIES_DIR);
}

async function withInboundPolicyLock(callback) {
  if (existsSync(INBOUND_POLICY_LOCK_PATH)) {
    privateMetadata(INBOUND_POLICY_LOCK_PATH, "file");
  }
  return withFileLock(INBOUND_POLICY_LOCK_PATH, async () => {
    privateMetadata(INBOUND_POLICY_LOCK_PATH, "file");
    return callback();
  });
}

function fsyncDirectory(directory) {
  const descriptor = openSync(directory, constants.O_RDONLY);
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

function atomicWritePolicy(filename, record) {
  const contents = `${JSON.stringify(record, null, 2)}\n`;
  if (Buffer.byteLength(contents) > INBOUND_POLICY_MAX_RECORD_BYTES) {
    throw new Error("Inbound policy record exceeds its bounded size");
  }
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
  return structuredClone(record);
}

function removePolicyFile(filename) {
  privateMetadata(filename, "file");
  const deleting = `${filename}.${randomUUID()}.deleting`;
  renameSync(filename, deleting);
  fsyncDirectory(path.dirname(filename));
  unlinkSync(deleting);
  fsyncDirectory(path.dirname(filename));
}

export function inboundPolicyState(targetNodeKey) {
  const normalized = normalizeInboundNodeKey(targetNodeKey);
  const filename = policyPath(normalized);
  let source;
  try {
    source = readPrivatePolicyFile(filename);
  } catch (error) {
    if (error?.code === "ENOENT") {
      return { state: "missing", record: null, errorCode: null };
    }
    return {
      state: "invalid",
      record: null,
      errorCode: error?.code || "EINBOUNDPOLICYMETADATA",
    };
  }
  try {
    const record = JSON.parse(source.contents);
    if (!validInboundPolicyRecord(record, path.basename(filename, ".json"))) {
      return {
        state: "invalid",
        record: null,
        errorCode: "EINBOUNDPOLICYSCHEMA",
      };
    }
    return { state: "valid", record, errorCode: null };
  } catch {
    return {
      state: "invalid",
      record: null,
      errorCode: "EINBOUNDPOLICYJSON",
    };
  }
}

export function listInboundPoliciesStrict() {
  if (!existsSync(INBOUND_POLICIES_DIR)) return [];
  privateMetadata(INBOUND_POLICIES_DIR, "directory");
  const names = readdirSync(INBOUND_POLICIES_DIR).sort();
  const recordNames = [];
  for (const name of names) {
    const filename = path.join(INBOUND_POLICIES_DIR, name);
    const metadata = lstatSync(filename);
    const classification = classifyInboundPolicyEntry(name, metadata.mtimeMs);
    if (["transient", "stale-transient"].includes(classification)) continue;
    if (classification !== "record") {
      throw new Error("Inbound policy directory contains an unexpected entry");
    }
    recordNames.push(name);
  }
  if (recordNames.length > INBOUND_POLICY_MAX_RECORDS) {
    throw new Error("Inbound policy record quota exceeded");
  }
  const records = [];
  for (const name of recordNames) {
    const filename = path.join(INBOUND_POLICIES_DIR, name);
    const { contents } = readPrivatePolicyFile(filename);
    const record = JSON.parse(contents);
    if (!validInboundPolicyRecord(record, name.slice(0, -5))) {
      throw new Error("Inbound policy record failed schema validation");
    }
    records.push(record);
  }
  if (
    records.reduce((total, record) => total + record.rules.length, 0) >
    INBOUND_POLICY_MAX_RULES
  ) {
    throw new Error("Inbound policy global rule quota exceeded");
  }
  return records.map((record) => structuredClone(record));
}

export function staleInboundPolicyArtifactEvidence({ now = Date.now() } = {}) {
  if (!existsSync(INBOUND_POLICIES_DIR)) {
    const evidence = { schemaVersion: 1, artifacts: [] };
    return {
      ...evidence,
      unexpectedCount: 0,
      evidenceSha256: createHash("sha256")
        .update(JSON.stringify(evidence))
        .digest("hex"),
    };
  }
  privateMetadata(INBOUND_POLICIES_DIR, "directory");
  const artifacts = [];
  let unexpectedCount = 0;
  for (const name of readdirSync(INBOUND_POLICIES_DIR).sort()) {
    const filename = path.join(INBOUND_POLICIES_DIR, name);
    const metadata = privateMetadata(filename, "file");
    const classification = classifyInboundPolicyEntry(name, metadata.mtimeMs, now);
    if (classification === "unexpected") {
      unexpectedCount += 1;
      continue;
    }
    if (classification !== "stale-transient") continue;
    const source = readPrivatePolicyFile(filename);
    artifacts.push({
      name,
      bytes: source.bytes.length,
      sha256: createHash("sha256").update(source.bytes).digest("hex"),
    });
    if (artifacts.length > INBOUND_POLICY_MAX_RECORDS) {
      const error = new Error("Inbound policy stale artifact quota exceeded");
      error.code = "EINBOUNDPOLICYARTIFACTQUOTA";
      throw error;
    }
  }
  const evidence = { schemaVersion: 1, artifacts };
  return {
    ...evidence,
    unexpectedCount,
    evidenceSha256: createHash("sha256")
      .update(JSON.stringify(evidence))
      .digest("hex"),
  };
}

export function readStaleInboundPolicyArtifact(artifact, { now = Date.now() } = {}) {
  if (
    !artifact ||
    typeof artifact.name !== "string" ||
    !Number.isSafeInteger(artifact.bytes) ||
    artifact.bytes < 0 ||
    !SHA256_PATTERN.test(artifact.sha256 || "")
  ) {
    throw new Error("Inbound policy stale artifact evidence is invalid");
  }
  const filename = path.join(INBOUND_POLICIES_DIR, artifact.name);
  const metadata = privateMetadata(filename, "file");
  if (classifyInboundPolicyEntry(artifact.name, metadata.mtimeMs, now) !== "stale-transient") {
    const error = new Error("Inbound policy artifact is no longer stale");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  const source = readPrivatePolicyFile(filename);
  const digest = createHash("sha256").update(source.bytes).digest("hex");
  if (source.bytes.length !== artifact.bytes || digest !== artifact.sha256) {
    const error = new Error("Inbound policy artifact changed before backup");
    error.code = "EREPAIRSTALE";
    throw error;
  }
  return source.bytes;
}

export async function purgeStaleInboundPolicyArtifacts({
  expectedEvidenceSha256,
}) {
  if (!SHA256_PATTERN.test(expectedEvidenceSha256 || "")) {
    throw new Error("Inbound policy stale artifact evidence digest is invalid");
  }
  return withInboundPolicyLock(() => {
    const evidence = staleInboundPolicyArtifactEvidence();
    if (
      evidence.unexpectedCount !== 0 ||
      evidence.artifacts.length === 0 ||
      evidence.evidenceSha256 !== expectedEvidenceSha256
    ) {
      const error = new Error("Inbound policy stale artifact evidence changed");
      error.code = "EREPAIRSTALE";
      throw error;
    }
    for (const artifact of evidence.artifacts) {
      readStaleInboundPolicyArtifact(artifact);
    }
    for (const artifact of evidence.artifacts) {
      unlinkSync(path.join(INBOUND_POLICIES_DIR, artifact.name));
      fsyncDirectory(INBOUND_POLICIES_DIR);
    }
    return { removed: evidence.artifacts.length };
  });
}

function normalizeSelector(selectorKind, selectorValue) {
  if (!SELECTOR_KINDS.has(selectorKind)) {
    throw new Error("Inbound policy selector kind is invalid");
  }
  if (selectorKind === "sender-node") {
    return {
      selectorKind,
      selectorNodeKey: normalizeInboundNodeKey(selectorValue),
    };
  }
  if (selectorKind === "sender-project") {
    if (!UUID_PATTERN.test(selectorValue || "")) {
      throw new Error("Inbound policy Project identity is invalid");
    }
    return { selectorKind, projectId: selectorValue.toLowerCase() };
  }
  if (selectorValue !== null && selectorValue !== undefined) {
    throw new Error("unknown-sender policy does not accept a selector value");
  }
  return { selectorKind };
}

function matchingRule(record, selector) {
  const identity = selectorIdentity(selector);
  return record.rules.find((rule) => selectorIdentity(rule) === identity) || null;
}

function stateDigest(state) {
  return state.state === "valid" ? policyDigest(state.record) : state.state;
}

export async function upsertInboundDenyRule({
  targetNodeKey,
  selectorKind,
  selectorValue = null,
  now = new Date().toISOString(),
}) {
  targetNodeKey = normalizeInboundNodeKey(targetNodeKey);
  if (!validTimestamp(now)) throw new Error("Inbound policy timestamp is invalid");
  const selector = normalizeSelector(selectorKind, selectorValue);
  ensureStore();
  return withInboundPolicyLock(async () => {
    const before = inboundPolicyState(targetNodeKey);
    if (before.state === "invalid") {
      throw new Error("Inbound policy target record is invalid");
    }
    const records = listInboundPoliciesStrict();
    const current = before.record;
    const duplicate = current ? matchingRule(current, selector) : null;
    if (duplicate) {
      return {
        policy: structuredClone(current),
        rule: structuredClone(duplicate),
        changed: false,
      };
    }
    if (!current && records.length >= INBOUND_POLICY_MAX_RECORDS) {
      throw new Error("Inbound policy record quota exceeded");
    }
    if (
      records.reduce((total, record) => total + record.rules.length, 0) >=
      INBOUND_POLICY_MAX_RULES
    ) {
      throw new Error("Inbound policy global rule quota exceeded");
    }
    if (current?.rules.length >= INBOUND_POLICY_MAX_RULES_PER_TARGET) {
      throw new Error("Inbound policy target rule quota exceeded");
    }
    const rule = {
      ruleId: randomUUID(),
      ...selector,
      createdAt: now,
    };
    const next = current
      ? {
          ...current,
          revision: current.revision + 1,
          rules: [...current.rules, rule],
          updatedAt: now,
        }
      : {
          schemaVersion: 1,
          recordType: "inbound-peer-policy",
          targetNodeKey,
          revision: 1,
          rules: [rule],
          createdAt: now,
          updatedAt: now,
        };
    if (!validInboundPolicyRecord(next)) {
      throw new Error("Inbound policy mutation produced an invalid record");
    }
    const confirmed = inboundPolicyState(targetNodeKey);
    if (stateDigest(confirmed) !== stateDigest(before)) {
      const error = new Error("Inbound policy changed before replacement");
      error.code = "EINBOUNDPOLICYSTALE";
      throw error;
    }
    const written = atomicWritePolicy(policyPath(targetNodeKey), next);
    return { policy: written, rule: structuredClone(rule), changed: true };
  });
}

export async function removeInboundDenyRule({
  targetNodeKey,
  ruleId,
  now = new Date().toISOString(),
}) {
  targetNodeKey = normalizeInboundNodeKey(targetNodeKey);
  if (!UUID_PATTERN.test(ruleId || "")) throw new Error("Inbound policy rule id is invalid");
  if (!validTimestamp(now)) throw new Error("Inbound policy timestamp is invalid");
  ensureStore();
  return withInboundPolicyLock(async () => {
    const before = inboundPolicyState(targetNodeKey);
    if (before.state !== "valid") {
      throw new Error("Inbound policy target record is missing or invalid");
    }
    listInboundPoliciesStrict();
    const rule = before.record.rules.find((candidate) => candidate.ruleId === ruleId);
    if (!rule) throw new Error("Inbound policy rule is missing");
    const next = {
      ...before.record,
      revision: before.record.revision + 1,
      rules: before.record.rules.filter((candidate) => candidate.ruleId !== ruleId),
      updatedAt: now,
    };
    const confirmed = inboundPolicyState(targetNodeKey);
    if (stateDigest(confirmed) !== stateDigest(before)) {
      const error = new Error("Inbound policy changed before replacement");
      error.code = "EINBOUNDPOLICYSTALE";
      throw error;
    }
    if (next.rules.length === 0) {
      removePolicyFile(policyPath(targetNodeKey));
      return {
        policy: null,
        removedRule: structuredClone(rule),
        deleted: true,
      };
    }
    return {
      policy: atomicWritePolicy(policyPath(targetNodeKey), next),
      removedRule: structuredClone(rule),
      deleted: false,
    };
  });
}

export async function purgeInboundPolicyRecord({
  targetNodeKey,
  confirmSha256,
  now = new Date().toISOString(),
}) {
  targetNodeKey = normalizeInboundNodeKey(targetNodeKey);
  if (!SHA256_PATTERN.test(confirmSha256 || "")) {
    throw new Error("Inbound policy purge requires an exact SHA-256 confirmation");
  }
  if (!validTimestamp(now)) throw new Error("Inbound policy timestamp is invalid");
  ensureStore();
  return withInboundPolicyLock(async () => {
    const filename = policyPath(targetNodeKey);
    const { bytes, contents } = readPrivatePolicyFile(filename);
    try {
      const record = JSON.parse(contents);
      if (validInboundPolicyRecord(record, path.basename(filename, ".json"))) {
        const error = new Error(
          "A valid inbound policy must be changed through rule removal",
        );
        error.code = "EINBOUNDPOLICYVALID";
        throw error;
      }
    } catch (error) {
      if (error?.code === "EINBOUNDPOLICYVALID") throw error;
    }
    const observedSha256 = createHash("sha256").update(bytes).digest("hex");
    if (observedSha256 !== confirmSha256) {
      const error = new Error("Inbound policy purge confirmation is stale");
      error.code = "EINBOUNDPOLICYSTALE";
      throw error;
    }
    removePolicyFile(filename);
    return {
      targetNodeKey,
      recordSha256: observedSha256,
      removedAt: now,
    };
  });
}

function normalizeSenderIdentity(identity) {
  if (!identity || !["verified", "unidentified", "unverifiable"].includes(identity.state)) {
    throw new Error("Inbound sender identity evidence is invalid");
  }
  if (identity.state === "verified") {
    if (!UUID_PATTERN.test(identity.projectId || "")) {
      throw new Error("Verified sender identity requires a Project UUID");
    }
    return {
      state: "verified",
      nodeKey: normalizeInboundNodeKey(identity.nodeKey),
      projectId: identity.projectId.toLowerCase(),
    };
  }
  if (identity.projectId !== undefined && identity.projectId !== null) {
    throw new Error("Unverified sender identity cannot claim a Project");
  }
  if (identity.state === "unidentified" && identity.nodeKey) {
    throw new Error("Unidentified sender evidence cannot claim a Node");
  }
  return {
    state: identity.state,
    nodeKey:
      identity.nodeKey === undefined || identity.nodeKey === null
        ? null
        : normalizeInboundNodeKey(identity.nodeKey),
    projectId: null,
  };
}

export function resolveInboundSenderIdentity(
  senderNodeKey,
  { node = readNode, tombstone = readNodeTombstone } = {},
) {
  if (!senderNodeKey) return { state: "unidentified" };
  const normalized = normalizeInboundNodeKey(senderNodeKey);
  const match = NODE_KEY_PATTERN.exec(normalized);
  const runtimeKind = match[1].toLowerCase();
  const nativeId = match[2].toLowerCase();
  const live = node(runtimeKind, nativeId);
  if (!live || tombstone(runtimeKind, nativeId)) {
    return { state: "unverifiable", nodeKey: normalized };
  }
  return {
    state: "verified",
    nodeKey: live.nodeKey,
    projectId: live.projectId,
  };
}

export function evaluateInboundPolicyRecord({
  targetNodeKey,
  policyState,
  senderIdentity,
}) {
  targetNodeKey = normalizeInboundNodeKey(targetNodeKey);
  const sender = normalizeSenderIdentity(senderIdentity);
  const verifiedSenderNodeKey = sender.state === "verified" ? sender.nodeKey : null;
  const verifiedSenderProjectId =
    sender.state === "verified" ? sender.projectId : null;
  if (!policyState || !["missing", "valid", "invalid"].includes(policyState.state)) {
    throw new Error("Inbound policy state evidence is invalid");
  }
  if (policyState.state === "missing") {
    return {
      decision: "continue",
      reason: "no_policy",
      targetNodeKey,
      senderIdentityState: sender.state,
      senderNodeKey: verifiedSenderNodeKey,
      senderProjectId: verifiedSenderProjectId,
      policyRevision: null,
      policySha256: null,
      ruleId: null,
      selectorKind: null,
      failClosed: false,
    };
  }
  if (
    policyState.state === "invalid" ||
    !validInboundPolicyRecord(policyState.record) ||
    policyState.record.targetNodeKey !== targetNodeKey
  ) {
    return {
      decision: "deny",
      reason: "policy_invalid",
      targetNodeKey,
      senderIdentityState: sender.state,
      senderNodeKey: verifiedSenderNodeKey,
      senderProjectId: verifiedSenderProjectId,
      policyRevision: null,
      policySha256: null,
      ruleId: null,
      selectorKind: null,
      failClosed: true,
    };
  }
  const policy = policyState.record;
  const senderNodeRule =
    sender.state === "verified"
      ? policy.rules.find(
          (rule) =>
            rule.selectorKind === "sender-node" &&
            rule.selectorNodeKey === sender.nodeKey,
        )
      : null;
  const senderProjectRule =
    sender.state === "verified"
      ? policy.rules.find(
          (rule) =>
            rule.selectorKind === "sender-project" &&
            rule.projectId === sender.projectId,
        )
      : null;
  const hasProjectRule = policy.rules.some(
    (rule) => rule.selectorKind === "sender-project",
  );
  const unknownRule = policy.rules.find(
    (rule) => rule.selectorKind === "unknown-sender",
  );
  const matched = senderNodeRule || senderProjectRule || null;
  let decision = "continue";
  let reason = sender.state === "verified" ? "no_match" : `sender_${sender.state}`;
  let rule = matched;
  if (matched) {
    decision = "deny";
    reason = matched.selectorKind === "sender-node" ? "sender_denied" : "project_denied";
  } else if (sender.state === "unverifiable" && hasProjectRule) {
    decision = "deny";
    reason = "identity_unverifiable";
    rule = null;
  } else if (sender.state !== "verified" && unknownRule) {
    decision = "deny";
    reason = sender.state === "unidentified"
      ? "sender_unidentified"
      : "sender_unverifiable";
    rule = unknownRule;
  }
  return {
    decision,
    reason,
    targetNodeKey,
    senderIdentityState: sender.state,
    senderNodeKey: verifiedSenderNodeKey,
    senderProjectId: verifiedSenderProjectId,
    policyRevision: policy.revision,
    policySha256: policyDigest(policy),
    ruleId: rule?.ruleId || null,
    selectorKind: rule?.selectorKind || null,
    failClosed: reason === "identity_unverifiable",
  };
}

export function evaluateInboundPolicy({ targetNodeKey, senderIdentity }) {
  return evaluateInboundPolicyRecord({
    targetNodeKey,
    policyState: inboundPolicyState(targetNodeKey),
    senderIdentity,
  });
}
