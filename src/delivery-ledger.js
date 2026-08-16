import { createHash, randomBytes, randomUUID } from "node:crypto";
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
  unlinkSync,
  writeFileSync,
  writeSync,
} from "node:fs";
import path from "node:path";
import {
  MAX_WHEN_IDLE_DELAY_MS,
  SCHEDULED_DELIVERY_PER_TARGET_LIMIT,
  SCHEDULED_WAKE_POLICIES,
} from "./delivery-policy.js";
import { withFileLock } from "./file-lock.js";
import {
  assertRetentionReadable,
  assertRetentionReadableNoCreate,
  assertRetentionMutation,
  withRetentionWriter,
} from "./retention-barrier.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const DELIVERY_LEDGER_SEGMENT_BYTES = 8 * 1024 * 1024;
export const DELIVERY_LEDGER_QUOTA_BYTES = 64 * 1024 * 1024;
export const DELIVERY_LEDGER_MAX_SCAN_BYTES = 256 * 1024 * 1024;
export const DELIVERY_LEDGER_MAX_RECORD_BYTES = 256 * 1024;
export const DELIVERY_LEDGER_EVENT_RESERVE_BYTES = 4 * 1024;
export const MAX_ORDINARY_DELIVERY_ATTEMPTS = 2;
export const TEAM_CAST_RECIPIENT_LIMIT = 64;
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
export const DELIVERY_LEDGER_INDEX_DIR = path.join(
  DELIVERY_LEDGER_DIR,
  "index",
);
export const DELIVERY_LEDGER_TOMBSTONES_DIR = path.join(
  DELIVERY_LEDGER_DIR,
  "tombstones",
);
export const DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH = path.join(
  DELIVERY_LEDGER_INDEX_DIR,
  "checkpoint.json",
);
const DELIVERY_LEDGER_LOCK_PATH = path.join(DELIVERY_LEDGER_DIR, "append.lock");
const SEGMENT_PATTERN = /^segment-(\d{8})(?:\.partial-[0-9a-f-]+)?\.jsonl$/i;
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const NODE_KEY_PATTERN = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i;
const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const INBOUND_POLICY_REASONS = new Set([
  "policy_invalid",
  "sender_denied",
  "project_denied",
  "identity_unverifiable",
  "sender_unidentified",
  "sender_unverifiable",
]);
const INBOUND_POLICY_CONTINUE_REASONS = new Set([
  "no_policy",
  "no_match",
  "sender_unidentified",
  "sender_unverifiable",
]);
const INBOUND_POLICY_SELECTOR_KINDS = new Set([
  "sender-node",
  "sender-project",
  "unknown-sender",
]);
const INBOUND_POLICY_FIELDS = new Set([
  "decision",
  "reason",
  "targetNodeKey",
  "senderIdentityState",
  "senderNodeKey",
  "senderProjectId",
  "policyRevision",
  "policySha256",
  "ruleId",
  "selectorKind",
  "failClosed",
]);
const REPLY_HANDLE_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
export const REPLY_HANDLE_PATTERN = /^m:[0-9A-HJKMNP-TV-Z]{10}$/;
const CLAIM_RELEASE_REASONS = new Set(["target_busy", "worker_stopping", "dispatch_unavailable"]);
const INDEX_SHARD_PATTERN = /^([0-9a-f-]{36})\.json$/i;
const TOMBSTONE_PATTERN = /^([0-9a-f-]{36})\.json$/i;
const DELIVERY_LEDGER_MAX_INDEX_RECORDS = 4096;

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value;
}

export function createReplyHandle(random = randomBytes) {
  const bytes = random(10);
  if (!Buffer.isBuffer(bytes) || bytes.length < 10) {
    throw new Error("reply handle entropy source returned too few bytes");
  }
  let encoded = "";
  for (let index = 0; index < 10; index += 1) {
    encoded += REPLY_HANDLE_ALPHABET[bytes[index] & 31];
  }
  return `m:${encoded}`;
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
  ensurePrivateDirectory(DELIVERY_LEDGER_INDEX_DIR);
  ensurePrivateDirectory(DELIVERY_LEDGER_TOMBSTONES_DIR);
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

function validInboundPolicyMetadata(evidence, decision, reasons) {
  return Boolean(
    evidence &&
      evidence.decision === decision &&
      reasons.has(evidence.reason) &&
      NODE_KEY_PATTERN.test(evidence.targetNodeKey || "") &&
      ["verified", "unidentified", "unverifiable"].includes(
        evidence.senderIdentityState,
      ) &&
      (evidence.senderNodeKey === null ||
        NODE_KEY_PATTERN.test(evidence.senderNodeKey || "")) &&
      (evidence.senderProjectId === null ||
        UUID_PATTERN.test(evidence.senderProjectId || "")) &&
      (evidence.policyRevision === null ||
        (Number.isSafeInteger(evidence.policyRevision) &&
          evidence.policyRevision >= 1)) &&
      (evidence.policySha256 === null ||
        SHA256_PATTERN.test(evidence.policySha256 || "")) &&
      (evidence.ruleId === null || UUID_PATTERN.test(evidence.ruleId || "")) &&
      (evidence.selectorKind === null ||
        INBOUND_POLICY_SELECTOR_KINDS.has(evidence.selectorKind)) &&
      typeof evidence.failClosed === "boolean" &&
      Object.keys(evidence).every((field) => INBOUND_POLICY_FIELDS.has(field))
  );
}

function validInboundPolicyDenial(evidence) {
  if (!validInboundPolicyMetadata(evidence, "deny", INBOUND_POLICY_REASONS)) {
    return false;
  }
  const verifiedIdentity = Boolean(
    evidence.senderNodeKey && evidence.senderProjectId,
  );
  if ((evidence.senderIdentityState === "verified") !== verifiedIdentity) {
    return false;
  }
  if (evidence.reason === "policy_invalid") {
    return Boolean(
      evidence.policyRevision === null &&
        evidence.policySha256 === null &&
        evidence.ruleId === null &&
        evidence.selectorKind === null &&
        evidence.failClosed,
    );
  }
  if (
    !Number.isSafeInteger(evidence.policyRevision) ||
    !SHA256_PATTERN.test(evidence.policySha256 || "")
  ) {
    return false;
  }
  if (evidence.reason === "identity_unverifiable") {
    return Boolean(
      evidence.senderIdentityState === "unverifiable" &&
      evidence.ruleId === null &&
        evidence.selectorKind === null &&
        evidence.failClosed,
    );
  }
  if (evidence.failClosed || !evidence.ruleId) return false;
  if (evidence.reason === "sender_denied") {
    return (
      evidence.senderIdentityState === "verified" &&
      evidence.selectorKind === "sender-node"
    );
  }
  if (evidence.reason === "project_denied") {
    return (
      evidence.senderIdentityState === "verified" &&
      evidence.selectorKind === "sender-project"
    );
  }
  return Boolean(
    ["sender_unidentified", "sender_unverifiable"].includes(
      evidence.reason,
    ) &&
      evidence.senderIdentityState !== "verified" &&
      evidence.selectorKind === "unknown-sender",
  );
}

function validInboundPolicyContinue(evidence) {
  if (
    !validInboundPolicyMetadata(
      evidence,
      "continue",
      INBOUND_POLICY_CONTINUE_REASONS,
    ) ||
    evidence.failClosed ||
    evidence.ruleId !== null ||
    evidence.selectorKind !== null
  ) {
    return false;
  }
  const verifiedIdentity = Boolean(
    evidence.senderNodeKey && evidence.senderProjectId,
  );
  if ((evidence.senderIdentityState === "verified") !== verifiedIdentity) {
    return false;
  }
  if (evidence.reason === "no_policy") {
    return evidence.policyRevision === null && evidence.policySha256 === null;
  }
  if (
    !Number.isSafeInteger(evidence.policyRevision) ||
    !SHA256_PATTERN.test(evidence.policySha256 || "")
  ) {
    return false;
  }
  if (evidence.reason === "no_match") {
    return evidence.senderIdentityState === "verified";
  }
  return (
    (evidence.reason === "sender_unidentified" &&
      evidence.senderIdentityState === "unidentified") ||
    (evidence.reason === "sender_unverifiable" &&
      evidence.senderIdentityState === "unverifiable")
  );
}

function validSenderNode(message) {
  if (message.senderNodeKey === undefined) return true;
  const match = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i.exec(
    message.senderNodeKey || "",
  );
  if (!match) return false;
  if (match[1].toLowerCase() === "codex") {
    return message.senderThreadId?.toLowerCase() === match[2].toLowerCase();
  }
  return message.senderThreadId === null || message.senderThreadId === undefined;
}

function validSenderIdentityDigest(message) {
  return (
    message.senderIdentitySha256 === undefined ||
    SHA256_PATTERN.test(message.senderIdentitySha256 || "")
  );
}

function validConversationProjection(message) {
  const conversationId = message.conversationId;
  const conversationSequence = message.conversationSequence;
  if (conversationId === undefined && conversationSequence === undefined) return true;
  return Boolean(
    UUID_PATTERN.test(conversationId || "") &&
      Number.isSafeInteger(conversationSequence) &&
      conversationSequence > 0,
  );
}

function validFanoutTarget(delivery) {
  const targetIdentity = NODE_KEY_PATTERN.exec(delivery?.targetNodeKey || "");
  return Boolean(
    targetIdentity &&
      delivery.target === delivery.targetNodeKey &&
      (targetIdentity[1].toLowerCase() === "codex"
        ? delivery.targetThreadId?.toLowerCase() ===
          targetIdentity[2].toLowerCase()
        : delivery.targetThreadId === null),
  );
}

function validFanoutDenial(delivery, message, wakePolicy) {
  return Boolean(
    UUID_PATTERN.test(delivery?.deliveryId || "") &&
      validFanoutTarget(delivery) &&
      delivery.replyHandle === null &&
      delivery.admissionState === "denied" &&
      delivery.admissionReason === "inbound_policy" &&
      delivery.wakePolicy === wakePolicy &&
      delivery.state === "denied" &&
      validTimestamp(delivery.createdAt) &&
      delivery.updatedAt === delivery.createdAt &&
      [null, `cxmsg-message:${message.messageId}`].includes(
        message.body.contentRef,
      ) &&
      validInboundPolicyDenial(delivery.inboundPolicy) &&
      Object.keys(delivery).every((field) =>
        [
          "deliveryId",
          "target",
          "targetNodeKey",
          "targetThreadId",
          "replyHandle",
          "admissionState",
          "admissionReason",
          "inboundPolicy",
          "wakePolicy",
          "state",
          "createdAt",
          "updatedAt",
        ].includes(field),
      ),
  );
}

function validFanoutBodyRetention(deliveries, message) {
  const hasAdmittedRecipient = deliveries.some(
    (delivery) => delivery.admissionState === "admitted",
  );
  return (
    message.body.contentRef ===
    (hasAdmittedRecipient ? `cxmsg-message:${message.messageId}` : null)
  );
}

function validInitialDelivery(delivery, message, { storeOnly = false } = {}) {
  const common = Boolean(
    UUID_PATTERN.test(delivery?.deliveryId || "") &&
      NAME_PATTERN.test(delivery?.target || "") &&
      (delivery.targetThreadId === null ||
        UUID_PATTERN.test(delivery.targetThreadId || "")) &&
      (delivery.replyHandle === undefined ||
        delivery.replyHandle === null ||
        (delivery.admissionState === "admitted" &&
          REPLY_HANDLE_PATTERN.test(delivery.replyHandle))) &&
      ["admitted", "quarantined", "denied"].includes(
        delivery.admissionState,
      ) &&
      typeof delivery.admissionReason === "string" &&
      validTimestamp(delivery.createdAt) &&
      delivery.createdAt === delivery.updatedAt,
  );
  if (!common) return false;
  if (delivery.admissionState === "denied") {
    if (storeOnly) {
      return validFanoutDenial(delivery, message, "store-only");
    }
    return Boolean(
      delivery.replyHandle === null &&
        delivery.state === "denied" &&
        delivery.admissionReason === "inbound_policy" &&
        message.body.contentRef === null &&
        validInboundPolicyDenial(delivery.inboundPolicy),
    );
  }
  if (delivery.inboundPolicy !== undefined) return false;
  if (storeOnly) {
    const targetIdentity = NODE_KEY_PATTERN.exec(delivery.targetNodeKey || "");
    return Boolean(
      targetIdentity &&
      delivery.target === delivery.targetNodeKey &&
        (targetIdentity[1].toLowerCase() === "codex"
          ? delivery.targetThreadId?.toLowerCase() ===
            targetIdentity[2].toLowerCase()
          : delivery.targetThreadId === null) &&
        delivery.replyHandle === null &&
        delivery.admissionState === "admitted" &&
        delivery.admissionReason === "group_membership" &&
        delivery.wakePolicy === "store-only" &&
        delivery.state === "scheduled" &&
        message.body.contentRef === `cxmsg-message:${message.messageId}`,
    );
  }
  return Boolean(
    delivery.targetNodeKey === undefined &&
      ["immediate", ...SCHEDULED_WAKE_POLICIES].includes(delivery.wakePolicy) &&
      delivery.state ===
        (delivery.wakePolicy === "immediate" ? "created" : "scheduled") &&
      (delivery.wakePolicy === "immediate" ||
        ((delivery.admissionState === "quarantined"
          ? message.body.contentRef === null
          : message.body.contentRef === `cxmsg-message:${message.messageId}`) &&
          message.route?.wake_policy === delivery.wakePolicy &&
          validTimestamp(message.route.expiry) &&
          Date.parse(message.route.expiry) > Date.parse(message.createdAt) &&
          Date.parse(message.route.expiry) - Date.parse(message.createdAt) <=
            MAX_WHEN_IDLE_DELAY_MS &&
          (delivery.wakePolicy === "after-turn"
            ? UUID_PATTERN.test(message.route.trigger_turn_id || "") &&
              message.route.trigger_job_id === undefined
            : message.route.trigger_turn_id === undefined) &&
          (delivery.wakePolicy === "after-job"
            ? UUID_PATTERN.test(message.route.trigger_job_id || "") &&
              message.route.trigger_turn_id === undefined
            : message.route.trigger_job_id === undefined))),
  );
}

function validGroupEnvelope(group, message) {
  if (
    group?.version !== 1 ||
    !UUID_PATTERN.test(group.conversationId || "") ||
    !Number.isSafeInteger(group.sequence) ||
    group.sequence < 1 ||
    !Number.isSafeInteger(group.membershipVersion) ||
    group.membershipVersion < 1 ||
    !Array.isArray(group.recipientNodeKeys) ||
    group.recipientNodeKeys.length < 2 ||
    group.recipientNodeKeys.length > 64 ||
    group.recipientNodeKeys.some((nodeKey) => !NODE_KEY_PATTERN.test(nodeKey)) ||
    JSON.stringify([...group.recipientNodeKeys].sort()) !==
      JSON.stringify(group.recipientNodeKeys) ||
    new Set(group.recipientNodeKeys).size !== group.recipientNodeKeys.length ||
    !validTimestamp(group.expiry) ||
    Date.parse(group.expiry) <= Date.parse(message.createdAt) ||
    Date.parse(group.expiry) - Date.parse(message.createdAt) >
      MAX_WHEN_IDLE_DELAY_MS ||
    !Number.isSafeInteger(group.hopCount) ||
    group.hopCount < 0 ||
    group.hopCount > 8 ||
    (group.parentMessageId !== null &&
      (!UUID_PATTERN.test(group.parentMessageId || "") ||
        group.parentMessageId === message.messageId)) ||
    group.parentMessageId !== (message.replyToMessageId || null) ||
    !Object.keys(group).every((field) =>
      [
        "version",
        "conversationId",
        "sequence",
        "membershipVersion",
        "recipientNodeKeys",
        "expiry",
        "hopCount",
        "parentMessageId",
      ].includes(field),
    )
  ) {
    return false;
  }
  return true;
}

function validTeamCastEnvelope(teamCast, message) {
  return Boolean(
    teamCast?.version === 1 &&
      UUID_PATTERN.test(teamCast.planId || "") &&
      UUID_PATTERN.test(teamCast.selectionId || "") &&
      UUID_PATTERN.test(teamCast.projectId || "") &&
      ["mention-wake", "wake-all"].includes(teamCast.wakePolicy) &&
      Array.isArray(teamCast.recipientNodeKeys) &&
      teamCast.recipientNodeKeys.length >= 1 &&
      teamCast.recipientNodeKeys.length <= TEAM_CAST_RECIPIENT_LIMIT &&
      teamCast.recipientNodeKeys.every((nodeKey) =>
        NODE_KEY_PATTERN.test(nodeKey),
      ) &&
      JSON.stringify([...teamCast.recipientNodeKeys].sort()) ===
        JSON.stringify(teamCast.recipientNodeKeys) &&
      new Set(teamCast.recipientNodeKeys).size ===
        teamCast.recipientNodeKeys.length &&
      SHA256_PATTERN.test(teamCast.recipientSetSha256 || "") &&
      teamCast.recipientSetSha256 ===
        createHash("sha256")
          .update(JSON.stringify(teamCast.recipientNodeKeys))
          .digest("hex") &&
      validTimestamp(teamCast.expiresAt) &&
      Date.parse(teamCast.expiresAt) > Date.parse(message.createdAt) &&
      Date.parse(teamCast.expiresAt) - Date.parse(message.createdAt) <=
        15 * 60 * 1_000 &&
      message.route?.kind === "team-cast" &&
      message.route.plan_id === teamCast.planId &&
      message.route.selection_id === teamCast.selectionId &&
      message.route.project_id === teamCast.projectId &&
      message.route.wake_policy === teamCast.wakePolicy &&
      message.route.expiry === teamCast.expiresAt &&
      Object.keys(message.route).every((field) =>
        [
          "schema_version",
          "kind",
          "plan_id",
          "selection_id",
          "project_id",
          "wake_policy",
          "expiry",
        ].includes(field),
      ) &&
      message.route.schema_version === 1 &&
      message.from === message.senderNodeKey &&
      message.replyToMessageId === undefined &&
      message.conversationId === undefined &&
      message.conversationSequence === undefined &&
      Object.keys(message).every((field) =>
        [
          "messageId",
          "from",
          "senderThreadId",
          "senderNodeKey",
          "body",
          "route",
          "routeFingerprint",
          "createdAt",
          "teamCast",
        ].includes(field),
      ) &&
      Object.keys(message.body).every((field) =>
        ["messageId", "bytes", "sha256", "contentRef"].includes(field),
      ) &&
      Object.keys(teamCast).every((field) =>
        [
          "version",
          "planId",
          "selectionId",
          "projectId",
          "wakePolicy",
          "recipientNodeKeys",
          "recipientSetSha256",
          "expiresAt",
        ].includes(field),
      ),
  );
}

function validPreparedTeamDelivery(delivery, message) {
  if (delivery?.admissionState === "denied") {
    return validFanoutDenial(
      delivery,
      message,
      message.teamCast.wakePolicy,
    );
  }
  const targetIdentity = NODE_KEY_PATTERN.exec(delivery?.targetNodeKey || "");
  return Boolean(
    targetIdentity &&
      UUID_PATTERN.test(delivery.deliveryId || "") &&
      delivery.target === delivery.targetNodeKey &&
      (targetIdentity[1].toLowerCase() === "codex"
        ? delivery.targetThreadId?.toLowerCase() ===
          targetIdentity[2].toLowerCase()
        : delivery.targetThreadId === null) &&
      delivery.replyHandle === null &&
      delivery.admissionState === "admitted" &&
      delivery.admissionReason === "team_cast_plan" &&
      delivery.wakePolicy === message.teamCast.wakePolicy &&
      delivery.state === "prepared" &&
      validTimestamp(delivery.createdAt) &&
      delivery.updatedAt === delivery.createdAt &&
      message.body.contentRef === `cxmsg-message:${message.messageId}` &&
      Object.keys(delivery).every((field) =>
        [
          "deliveryId",
          "target",
          "targetNodeKey",
          "targetThreadId",
          "replyHandle",
          "admissionState",
          "admissionReason",
          "wakePolicy",
          "state",
          "createdAt",
          "updatedAt",
        ].includes(field),
      ),
  );
}

function validBatch(record) {
  const message = record?.logicalMessage;
  const deliveries = record?.deliveries;
  const group = message?.group;
  const teamCast = message?.teamCast;
  const common = Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "ledger-batch" &&
      UUID_PATTERN.test(record.batchId || "") &&
      validTimestamp(record.committedAt) &&
      message?.messageId &&
      UUID_PATTERN.test(message.messageId) &&
      NAME_PATTERN.test(message.from || "") &&
      (message.senderThreadId === undefined ||
        message.senderThreadId === null ||
        UUID_PATTERN.test(message.senderThreadId || "")) &&
      validSenderNode(message) &&
      validSenderIdentityDigest(message) &&
      validConversationProjection(message) &&
      (message.replyToMessageId === undefined ||
        (UUID_PATTERN.test(message.replyToMessageId || "") &&
          message.replyToMessageId !== message.messageId)) &&
      validBody(message.body, message.messageId) &&
      typeof message.routeFingerprint === "string" &&
      SHA256_PATTERN.test(message.routeFingerprint) &&
      message.routeFingerprint ===
        createHash("sha256").update(JSON.stringify(message.route ?? null)).digest("hex") &&
      validTimestamp(message.createdAt) &&
      Array.isArray(record.deliveries) &&
      record.deliveries.length >= 1
  );
  if (!common) return false;
  if (group !== undefined && teamCast !== undefined) return false;
  if (group === undefined && teamCast === undefined) {
    return deliveries.length === 1 && validInitialDelivery(deliveries[0], message);
  }
  if (group !== undefined) {
    if (
      !validGroupEnvelope(group, message) ||
      deliveries.length !== group.recipientNodeKeys.length ||
      new Set(deliveries.map((delivery) => delivery.deliveryId)).size !==
        deliveries.length
    ) {
      return false;
    }
    return Boolean(
      validFanoutBodyRetention(deliveries, message) &&
      deliveries.every(
      (delivery, index) =>
        delivery.targetNodeKey === group.recipientNodeKeys[index] &&
        validInitialDelivery(delivery, message, { storeOnly: true }),
      )
    );
  }
  if (
    !validTeamCastEnvelope(teamCast, message) ||
    deliveries.length !== teamCast.recipientNodeKeys.length ||
    new Set(deliveries.map((delivery) => delivery.deliveryId)).size !==
      deliveries.length
  ) {
    return false;
  }
  return Boolean(
    validFanoutBodyRetention(deliveries, message) &&
    deliveries.every(
      (delivery, index) =>
        delivery.targetNodeKey === teamCast.recipientNodeKeys[index] &&
        validPreparedTeamDelivery(delivery, message),
    ),
  );
}

function validAttempt(record) {
  return Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "delivery-attempt" &&
      UUID_PATTERN.test(record.messageId || "") &&
      UUID_PATTERN.test(record.deliveryId || "") &&
      UUID_PATTERN.test(record.attemptId || "") &&
      (record.retryOfAttemptId === undefined ||
        UUID_PATTERN.test(record.retryOfAttemptId || "")) &&
      (record.claimId === undefined ||
        record.claimId === null ||
        UUID_PATTERN.test(record.claimId || "")) &&
      (record.inboundPolicySnapshot === undefined ||
        validInboundPolicyContinue(record.inboundPolicySnapshot)) &&
      ["codex-app-server", "claude-uds"].includes(record.transport) &&
      validTimestamp(record.startedAt),
  );
}

function expectedTeamDeliveryTransport(delivery) {
  const targetIdentity = NODE_KEY_PATTERN.exec(delivery?.targetNodeKey || "");
  if (!targetIdentity) return null;
  if (targetIdentity[1].toLowerCase() === "codex") {
    return "codex-app-server";
  }
  if (targetIdentity[1].toLowerCase() === "claude") {
    return "claude-uds";
  }
  return null;
}

function validTeamSchedule(record) {
  if (
    !(
      record?.schemaVersion === 1 &&
      record.recordType === "team-delivery-schedule" &&
      UUID_PATTERN.test(record.messageId || "") &&
      UUID_PATTERN.test(record.deliveryId || "") &&
      SCHEDULED_WAKE_POLICIES.includes(record.wakePolicy) &&
      validTimestamp(record.scheduledAt)
    )
  ) {
    return false;
  }
  if (record.wakePolicy === "after-turn") {
    return (
      UUID_PATTERN.test(record.triggerTurnId || "") &&
      record.triggerJobId === null
    );
  }
  if (record.wakePolicy === "after-job") {
    return (
      record.triggerTurnId === null &&
      UUID_PATTERN.test(record.triggerJobId || "")
    );
  }
  return record.triggerTurnId == null && record.triggerJobId == null;
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
  if (record.action === "renewed") {
    return Boolean(
      validTimestamp(record.renewedAt) &&
        validTimestamp(record.leaseUntil) &&
        Date.parse(record.leaseUntil) > Date.parse(record.renewedAt),
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
      [
        "turn_started",
        "retryable",
        "failed",
        "unknown",
        "expired",
        "cancelled",
        "policy_denied",
      ].includes(record.state) &&
      [
        "dispatch-result",
        "negative-acceptance",
        "reconciliation",
        "retry-policy",
        "scheduler",
        "inbound-policy",
      ].includes(record.evidenceKind) &&
      (record.turnId === null || UUID_PATTERN.test(record.turnId || "")) &&
      (record.transportResult === null || typeof record.transportResult === "string") &&
      (record.errorCode === null || /^[A-Z0-9_]{1,32}$/.test(record.errorCode || "")) &&
      validTimestamp(record.observedAt)
  );
  if (!common) return false;
  if (record.evidenceKind === "inbound-policy") {
    return Boolean(
      record.state === "policy_denied" &&
        record.attemptId === null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode === "EINBOUNDDENIED" &&
        validInboundPolicyDenial(record.inboundPolicy),
    );
  }
  if (record.inboundPolicy !== undefined) return false;
  if (
    record.evidenceKind !== "negative-acceptance" &&
    record.negativeAcceptanceContract !== undefined
  ) {
    return false;
  }
  if (record.evidenceKind === "scheduler") {
    return Boolean(
      ["unknown", "expired", "cancelled"].includes(record.state) &&
        record.attemptId === null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode,
    );
  }
  if (record.evidenceKind === "retry-policy") {
    return Boolean(
      record.state === "expired" &&
        record.attemptId === null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode === "ERETRYEXPIRED",
    );
  }
  if (record.evidenceKind === "negative-acceptance") {
    return Boolean(
      ["retryable", "failed"].includes(record.state) &&
        record.attemptId !== null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode &&
        /^codex-app-server\/\d+\.\d+\.\d+$/.test(
          record.negativeAcceptanceContract || "",
        ),
    );
  }
  return ["turn_started", "unknown"].includes(record.state);
}

function validGroupEvidence(record) {
  return Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "group-delivery-evidence" &&
      UUID_PATTERN.test(record.messageId || "") &&
      UUID_PATTERN.test(record.deliveryId || "") &&
      ["failed", "expired", "cancelled"].includes(record.state) &&
      /^[A-Z0-9_]{1,32}$/.test(record.errorCode || "") &&
      validTimestamp(record.observedAt),
  );
}

function validTeamEvidence(record) {
  if (
    record?.schemaVersion !== 1 ||
    record.recordType !== "team-delivery-evidence" ||
    !UUID_PATTERN.test(record.messageId || "") ||
    !UUID_PATTERN.test(record.deliveryId || "") ||
    (record.attemptId !== null &&
      !UUID_PATTERN.test(record.attemptId || "")) ||
    ![
      "turn_started",
      "transport_delivered",
      "failed",
      "unknown",
      "expired",
      "policy_denied",
    ].includes(record.state) ||
    !validTimestamp(record.observedAt) ||
    (record.turnId !== null && !UUID_PATTERN.test(record.turnId || "")) ||
    (record.transportResult !== null &&
      (typeof record.transportResult !== "string" ||
        Buffer.byteLength(record.transportResult, "utf8") > 128)) ||
    (record.errorCode !== null &&
      !/^[A-Z0-9_]{1,32}$/.test(record.errorCode || ""))
  ) {
    return false;
  }
  if (record.state === "policy_denied") {
    return Boolean(
      record.attemptId === null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode === "EINBOUNDDENIED" &&
        validInboundPolicyDenial(record.inboundPolicy),
    );
  }
  if (record.inboundPolicy !== undefined) return false;
  if (record.state === "turn_started") {
    return Boolean(
      record.attemptId !== null &&
        record.turnId &&
        record.transportResult &&
        record.errorCode === null,
    );
  }
  if (record.state === "transport_delivered") {
    return Boolean(
      record.attemptId !== null &&
        record.turnId === null &&
        /^claude-job:[0-9a-f-]{36}$/i.test(record.transportResult || "") &&
        record.errorCode === null,
    );
  }
  if (record.state === "expired") {
    return Boolean(
      record.attemptId === null &&
        record.turnId === null &&
        record.transportResult === null &&
        record.errorCode === "EDELIVERYEXPIRED",
    );
  }
  return Boolean(
    record.attemptId !== null &&
    record.turnId === null &&
      record.errorCode &&
      (record.state === "failed"
        ? record.transportResult === null
        : record.transportResult !== null),
  );
}

export function validDeliveryLedgerRecord(record) {
  if (record?.recordType === "ledger-batch") return validBatch(record);
  if (record?.recordType === "delivery-claim") return validClaim(record);
  if (record?.recordType === "delivery-attempt") return validAttempt(record);
  if (record?.recordType === "delivery-evidence") return validEvidence(record);
  if (record?.recordType === "group-delivery-evidence") {
    return validGroupEvidence(record);
  }
  if (record?.recordType === "team-delivery-evidence") {
    return validTeamEvidence(record);
  }
  if (record?.recordType === "team-delivery-schedule") {
    return validTeamSchedule(record);
  }
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

function ledgerManifest() {
  return allSegmentPaths().map((filename) => {
    const metadata = assertPrivateRegularFile(filename);
    return {
      directory:
        path.dirname(filename) === DELIVERY_LEDGER_SEGMENTS_DIR
          ? "segments"
          : "quarantine",
      name: path.basename(filename),
      size: metadata.size,
      mtimeMs: metadata.mtimeMs,
    };
  });
}

function manifestDigest(manifest) {
  return createHash("sha256").update(JSON.stringify(manifest)).digest("hex");
}

function indexGenerationDigest() {
  if (!existsSync(DELIVERY_LEDGER_INDEX_DIR)) {
    return createHash("sha256").update("[]").digest("hex");
  }
  const metadata = lstatSync(DELIVERY_LEDGER_INDEX_DIR);
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    throw new Error("Delivery Ledger index directory is not owner-private");
  }
  const names = [
    ...(existsSync(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH)
      ? [path.basename(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH)]
      : []),
    ...indexShardNames(),
  ].sort();
  const manifest = names.map((name) => {
    const filename = path.join(DELIVERY_LEDGER_INDEX_DIR, name);
    const file = assertPrivateIndexFile(filename);
    return {
      name,
      size: file.size,
      sha256: createHash("sha256").update(readFileSync(filename)).digest("hex"),
    };
  });
  return createHash("sha256").update(JSON.stringify(manifest)).digest("hex");
}

export function deliveryLedgerIndexRepairEvidence() {
  assertRetentionReadableNoCreate();
  const manifest = ledgerManifest();
  return {
    ledgerManifestSha256: manifestDigest(manifest),
    indexGenerationSha256: indexGenerationDigest(),
  };
}

function assertPrivateIndexFile(filename) {
  const metadata = lstatSync(filename);
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.nlink !== 1) {
    throw new Error(`Delivery Ledger index entry is not a private regular file: ${path.basename(filename)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Delivery Ledger index entry is owned by another user: ${path.basename(filename)}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`Delivery Ledger index permissions are too broad: ${path.basename(filename)}`);
  }
  if (metadata.size > DELIVERY_LEDGER_MAX_RECORD_BYTES) {
    throw new Error(`Delivery Ledger index entry is too large: ${path.basename(filename)}`);
  }
  return metadata;
}

function atomicWriteIndex(filename, value) {
  const temporary = `${filename}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value)}\n`, { mode: 0o600 });
  const fileDescriptor = openSync(
    temporary,
    constants.O_RDONLY | (constants.O_NOFOLLOW || 0),
  );
  try {
    fsyncSync(fileDescriptor);
  } finally {
    closeSync(fileDescriptor);
  }
  renameSync(temporary, filename);
  const directoryDescriptor = openSync(path.dirname(filename), constants.O_RDONLY);
  try {
    fsyncSync(directoryDescriptor);
  } finally {
    closeSync(directoryDescriptor);
  }
}

function indexShardPath(messageId) {
  return path.join(DELIVERY_LEDGER_INDEX_DIR, `${validateUuid("logical message id", messageId)}.json`);
}

function tombstonePath(messageId) {
  return path.join(
    DELIVERY_LEDGER_TOMBSTONES_DIR,
    `${validateUuid("logical message id", messageId)}.json`,
  );
}

function validDedupTombstone(record, messageId) {
  return Boolean(
    record?.schemaVersion === 1 &&
      record.recordType === "delivery-dedup-tombstone" &&
      record.messageId === messageId &&
      SHA256_PATTERN.test(record.deliveryFingerprintSha256 || "") &&
      validTimestamp(record.purgedAt) &&
      UUID_PATTERN.test(record.backupId || "")
  );
}

function readDedupTombstone(messageId) {
  const filename = tombstonePath(messageId);
  if (!existsSync(filename)) return null;
  assertPrivateIndexFile(filename);
  let record;
  try {
    record = JSON.parse(readFileSync(filename, "utf8"));
  } catch {
    throw new Error(`Delivery dedup Tombstone is malformed: ${messageId}`);
  }
  if (!validDedupTombstone(record, messageId)) {
    throw new Error(`Delivery dedup Tombstone failed validation: ${messageId}`);
  }
  return record;
}

function writeDedupTombstone(record) {
  const existing = readDedupTombstone(record.messageId);
  if (existing) {
    if (
      existing.deliveryFingerprintSha256 !== record.deliveryFingerprintSha256
    ) {
      throw new Error(`Delivery dedup Tombstone conflict: ${record.messageId}`);
    }
    return existing;
  }
  atomicWriteIndex(tombstonePath(record.messageId), record);
  return record;
}

function indexProjectionDigest(projection) {
  return createHash("sha256").update(JSON.stringify(projection)).digest("hex");
}

function validTeamDeliveryProjection(candidate, messageId, initialWakePolicy) {
  if (
    ![
      "denied",
      "prepared",
      "scheduled",
      "turn_started",
      "transport_delivered",
      "failed",
      "unknown",
      "expired",
      "policy_denied",
    ].includes(candidate.state) ||
    !Array.isArray(candidate.attempts) ||
    candidate.attempts.length > 1 ||
    !Array.isArray(candidate.evidence) ||
    candidate.evidence.length > 1 ||
    !Number.isSafeInteger(candidate.claimCount) ||
    candidate.claimCount < 0 ||
    !validTimestamp(candidate.updatedAt)
  ) {
    return false;
  }
  if (candidate.admissionState === "denied") {
    return Boolean(
      candidate.state === "denied" &&
        candidate.wakePolicy === initialWakePolicy &&
        candidate.attempts.length === 0 &&
        candidate.evidence.length === 0 &&
        candidate.claim === null &&
        candidate.claimCount === 0 &&
        candidate.schedule === null &&
        candidate.updatedAt === candidate.createdAt
    );
  }
  const scheduled = candidate.schedule != null;
  if (
    scheduled !== SCHEDULED_WAKE_POLICIES.includes(candidate.wakePolicy) ||
    (!scheduled && candidate.wakePolicy !== initialWakePolicy) ||
    (scheduled &&
      (!validTeamSchedule(candidate.schedule) ||
        candidate.schedule.messageId !== messageId ||
        candidate.schedule.deliveryId !== candidate.deliveryId ||
        candidate.schedule.wakePolicy !== candidate.wakePolicy ||
        !candidate.targetNodeKey.startsWith("codex:")))
  ) {
    return false;
  }
  if (
    candidate.attempts.some(
      (attempt) =>
        !validAttempt(attempt) ||
        attempt.transport !== expectedTeamDeliveryTransport(candidate) ||
        attempt.messageId !== messageId ||
        attempt.deliveryId !== candidate.deliveryId ||
        attempt.retryOfAttemptId !== undefined ||
        (scheduled
          ? !UUID_PATTERN.test(attempt.claimId || "")
          : attempt.claimId !== null),
    )
  ) {
    return false;
  }
  if (
    candidate.claim !== null &&
    (!scheduled ||
      !validClaim(candidate.claim) ||
      candidate.claim.action !== "acquired" ||
      candidate.claim.messageId !== messageId ||
      candidate.claim.deliveryId !== candidate.deliveryId)
  ) {
    return false;
  }
  if ((!scheduled && candidate.claimCount !== 0) || (candidate.claim && candidate.claimCount < 1)) {
    return false;
  }
  if (
    candidate.evidence.some(
      (evidence) =>
        !validTeamEvidence(evidence) ||
        evidence.messageId !== messageId ||
        evidence.deliveryId !== candidate.deliveryId ||
        (evidence.attemptId === null
          ? (evidence.state === "expired" &&
              (!scheduled || candidate.attempts.length !== 0)) ||
            (evidence.state === "policy_denied" &&
              candidate.attempts.length !== 0) ||
            !["expired", "policy_denied"].includes(evidence.state)
          : !candidate.attempts.some(
              (attempt) => attempt.attemptId === evidence.attemptId,
            )),
    )
  ) {
    return false;
  }
  if (candidate.evidence.length === 1) {
    const evidence = candidate.evidence[0];
    return Boolean(
      candidate.state === evidence.state &&
        candidate.turnId === evidence.turnId &&
        candidate.transportResult === evidence.transportResult &&
        candidate.errorCode === evidence.errorCode &&
        candidate.updatedAt === evidence.observedAt,
    );
  }
  if (candidate.attempts.length === 1) {
    return Boolean(
      candidate.state === (scheduled ? "scheduled" : "prepared") &&
        candidate.updatedAt === candidate.attempts[0].startedAt,
    );
  }
  if (scheduled) {
    return Boolean(
      candidate.state === "scheduled" &&
        Date.parse(candidate.updatedAt) >=
          Date.parse(candidate.schedule.scheduledAt),
    );
  }
  return Boolean(
    candidate.state === "prepared" &&
      candidate.updatedAt === candidate.createdAt,
  );
}

function validIndexProjection(projection, messageId) {
  const delivery = projection?.delivery;
  if (!delivery || projection.logicalMessage?.messageId !== messageId) return false;
  if (projection.teamDeliveries !== undefined) {
    if (
      !Array.isArray(projection.teamDeliveries) ||
      projection.teamDeliveries.length < 1 ||
      JSON.stringify(delivery) !== JSON.stringify(projection.teamDeliveries[0])
    ) {
      return false;
    }
    const baseBatch = {
      schemaVersion: 1,
      recordType: "ledger-batch",
      batchId: projection.batchId,
      committedAt: projection.committedAt,
      logicalMessage: structuredClone(projection.logicalMessage),
      deliveries: projection.teamDeliveries.map((candidate) => {
        const {
          attempts,
          evidence,
          claim,
          claimCount,
          schedule,
          turnId,
          transportResult,
          errorCode,
          ...initial
        } = candidate;
        initial.wakePolicy = projection.logicalMessage.teamCast.wakePolicy;
        initial.state =
          initial.admissionState === "denied" ? "denied" : "prepared";
        initial.updatedAt = initial.createdAt;
        return initial;
      }),
    };
    return Boolean(
      validBatch(baseBatch) &&
        projection.teamDeliveries.every((candidate) =>
          validTeamDeliveryProjection(
            candidate,
            messageId,
            projection.logicalMessage.teamCast.wakePolicy,
          ),
        ),
    );
  }
  if (projection.groupDeliveries !== undefined) {
    if (
      !Array.isArray(projection.groupDeliveries) ||
      projection.groupDeliveries.length < 1 ||
      JSON.stringify(delivery) !== JSON.stringify(projection.groupDeliveries[0])
    ) {
      return false;
    }
    const baseBatch = {
      schemaVersion: 1,
      recordType: "ledger-batch",
      batchId: projection.batchId,
      committedAt: projection.committedAt,
      logicalMessage: structuredClone(projection.logicalMessage),
      deliveries: projection.groupDeliveries.map((candidate) => {
        const {
          attempts,
          evidence,
          claim,
          claimCount,
          turnId,
          transportResult,
          errorCode,
          ...initial
        } = candidate;
        initial.state =
          initial.admissionState === "denied" ? "denied" : "scheduled";
        initial.updatedAt = initial.createdAt;
        return initial;
      }),
    };
    return Boolean(
      validBatch(baseBatch) &&
        projection.groupDeliveries.every(
          (candidate) =>
            ["denied", "scheduled", "failed", "expired", "cancelled"].includes(
              candidate.state,
            ) &&
            candidate.wakePolicy === "store-only" &&
            Array.isArray(candidate.attempts) &&
            candidate.attempts.length === 0 &&
            Array.isArray(candidate.evidence) &&
            candidate.evidence.every(
              (record) =>
                validGroupEvidence(record) &&
                record.messageId === messageId &&
                record.deliveryId === candidate.deliveryId,
            ) &&
            candidate.claim === null &&
            candidate.claimCount === 0 &&
            (candidate.admissionState === "denied"
              ? candidate.state === "denied" &&
                candidate.evidence.length === 0 &&
                candidate.updatedAt === candidate.createdAt
              : candidate.evidence.length === 0
                ? candidate.state === "scheduled" &&
                candidate.updatedAt === candidate.createdAt
              : candidate.evidence.length === 1 &&
                candidate.state === candidate.evidence[0].state &&
                candidate.errorCode === candidate.evidence[0].errorCode &&
                candidate.updatedAt === candidate.evidence[0].observedAt),
        ),
    );
  }
  const initialState =
    delivery.admissionState === "denied"
      ? "denied"
      : delivery.wakePolicy === "immediate"
        ? "created"
        : "scheduled";
  const baseDelivery = {
    ...structuredClone(delivery),
    state: initialState,
    updatedAt: delivery.createdAt,
  };
  const baseBatch = {
    schemaVersion: 1,
    recordType: "ledger-batch",
    batchId: projection.batchId,
    committedAt: projection.committedAt,
    logicalMessage: structuredClone(projection.logicalMessage),
    deliveries: [baseDelivery],
  };
  if (
    !validBatch(baseBatch) ||
    ![
      "created",
      "scheduled",
      "turn_started",
      "retryable",
      "failed",
      "unknown",
      "expired",
      "cancelled",
      "denied",
      "policy_denied",
    ].includes(
      delivery.state,
    ) ||
    !validTimestamp(delivery.updatedAt) ||
    !Number.isSafeInteger(delivery.claimCount) ||
    delivery.claimCount < 0 ||
    !Array.isArray(delivery.attempts) ||
    delivery.attempts.length > MAX_ORDINARY_DELIVERY_ATTEMPTS ||
    !Array.isArray(delivery.evidence)
  ) {
    return false;
  }
  const attempts = new Set();
  for (const [index, attempt] of delivery.attempts.entries()) {
    if (
      !validAttempt(attempt) ||
      attempt.messageId !== messageId ||
      attempt.deliveryId !== delivery.deliveryId ||
      attempts.has(attempt.attemptId) ||
      (index === 0 && attempt.retryOfAttemptId !== undefined) ||
      (index === 1 &&
        attempt.retryOfAttemptId !== delivery.attempts[0].attemptId)
    ) {
      return false;
    }
    attempts.add(attempt.attemptId);
  }
  if (
    delivery.claim !== null &&
    (!validClaim(delivery.claim) ||
      delivery.claim.action !== "acquired" ||
      delivery.claim.messageId !== messageId ||
      delivery.claim.deliveryId !== delivery.deliveryId)
  ) {
    return false;
  }
  if (delivery.claim && delivery.claimCount < 1) return false;
  for (const evidence of delivery.evidence) {
    if (
      !validEvidence(evidence) ||
      evidence.messageId !== messageId ||
      evidence.deliveryId !== delivery.deliveryId ||
      (evidence.attemptId !== null && !attempts.has(evidence.attemptId))
    ) {
      return false;
    }
  }
  if (
    delivery.attempts.length === 2 &&
    !delivery.evidence.some(
      (evidence) =>
        evidence.attemptId === delivery.attempts[0].attemptId &&
        evidence.state === "retryable" &&
        evidence.evidenceKind === "negative-acceptance",
    )
  ) {
    return false;
  }
  const lastEvidence = delivery.evidence.at(-1) || null;
  if (!lastEvidence) return delivery.state === initialState;
  return Boolean(
    delivery.state === lastEvidence.state &&
      (delivery.turnId ?? null) === lastEvidence.turnId &&
      (delivery.transportResult ?? null) === lastEvidence.transportResult &&
      (delivery.errorCode ?? null) === lastEvidence.errorCode,
  );
}

export function validDeliveryLedgerIndexRecord(wrapper, messageId) {
  const projection = wrapper?.projection;
  return Boolean(
    wrapper?.version === 1 &&
      wrapper.messageId === messageId &&
      projection?.logicalMessage?.messageId === messageId &&
      UUID_PATTERN.test(projection.batchId || "") &&
      UUID_PATTERN.test(projection.delivery?.deliveryId || "") &&
      validIndexProjection(projection, messageId) &&
      SHA256_PATTERN.test(wrapper.projectionSha256 || "") &&
      wrapper.projectionSha256 === indexProjectionDigest(projection),
  );
}

function indexShardNames() {
  if (!existsSync(DELIVERY_LEDGER_INDEX_DIR)) return [];
  return readdirSync(DELIVERY_LEDGER_INDEX_DIR)
    .filter((name) => INDEX_SHARD_PATTERN.test(name))
    .sort();
}

function writeIndexShard(projection) {
  const messageId = projection.logicalMessage.messageId;
  atomicWriteIndex(indexShardPath(messageId), {
    version: 1,
    messageId,
    projection: structuredClone(projection),
    projectionSha256: indexProjectionDigest(projection),
  });
}

function writeIndexCheckpoint(messages) {
  const manifest = ledgerManifest();
  atomicWriteIndex(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH, {
    version: 1,
    manifest,
    manifestSha256: manifestDigest(manifest),
    messageCount: messages.size,
    rebuiltAt: new Date().toISOString(),
  });
}

function rebuildDeliveryIndexLocked() {
  const messages = rebuildDeliveryLedgerRecords(readRecords());
  if (messages.size > DELIVERY_LEDGER_MAX_INDEX_RECORDS) {
    throw new Error("Delivery Ledger index exceeds its bounded message count");
  }
  const expected = new Set([...messages.keys()].map((messageId) => `${messageId}.json`));
  for (const name of indexShardNames()) {
    const filename = path.join(DELIVERY_LEDGER_INDEX_DIR, name);
    assertPrivateIndexFile(filename);
    if (!expected.has(name)) unlinkSync(filename);
  }
  for (const projection of messages.values()) writeIndexShard(projection);
  writeIndexCheckpoint(messages);
  return messages;
}

function readDeliveryIndexLocked() {
  if (!existsSync(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH)) {
    return rebuildDeliveryIndexLocked();
  }
  assertPrivateIndexFile(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH);
  let checkpoint;
  try {
    checkpoint = JSON.parse(readFileSync(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH, "utf8"));
  } catch {
    throw new Error("Delivery Ledger index checkpoint is malformed");
  }
  const manifest = ledgerManifest();
  if (
    checkpoint?.version !== 1 ||
    !Array.isArray(checkpoint.manifest) ||
    checkpoint.manifestSha256 !== manifestDigest(checkpoint.manifest) ||
    checkpoint.manifestSha256 !== manifestDigest(manifest) ||
    !Number.isSafeInteger(checkpoint.messageCount) ||
    checkpoint.messageCount < 0 ||
    checkpoint.messageCount > DELIVERY_LEDGER_MAX_INDEX_RECORDS
  ) {
    return rebuildDeliveryIndexLocked();
  }
  const names = indexShardNames();
  if (names.length !== checkpoint.messageCount) return rebuildDeliveryIndexLocked();
  const messages = new Map();
  for (const name of names) {
    const match = name.match(INDEX_SHARD_PATTERN);
    const messageId = match?.[1];
    if (!UUID_PATTERN.test(messageId || "")) {
      throw new Error(`Delivery Ledger index filename is invalid: ${name}`);
    }
    const filename = path.join(DELIVERY_LEDGER_INDEX_DIR, name);
    assertPrivateIndexFile(filename);
    let wrapper;
    try {
      wrapper = JSON.parse(readFileSync(filename, "utf8"));
    } catch {
      throw new Error(`Delivery Ledger index entry is malformed: ${name}`);
    }
    if (!validDeliveryLedgerIndexRecord(wrapper, messageId)) {
      throw new Error(`Delivery Ledger index entry failed validation: ${name}`);
    }
    if (messages.has(messageId)) throw new Error(`duplicate Delivery Ledger index identity: ${messageId}`);
    messages.set(messageId, structuredClone(wrapper.projection));
  }
  return messages;
}

function updateDeliveryIndexLocked(messages, messageId) {
  const projection = messages.get(messageId);
  if (!projection) throw new Error(`Delivery Ledger index update is missing: ${messageId}`);
  writeIndexShard(projection);
  writeIndexCheckpoint(messages);
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
    if (Array.isArray(message.teamDeliveries)) {
      records += message.teamDeliveries.reduce(
        (count, delivery) =>
          count +
          (["denied", "turn_started", "transport_delivered", "failed", "expired", "policy_denied"].includes(
            delivery.state,
          )
            ? 0
            : delivery.state === "unknown"
              ? 1
              : delivery.state === "prepared"
                ? delivery.attempts.length === 0
                  ? 5
                  : 1
                : delivery.attempts.length > 0
                  ? 2
                  : delivery.claim
                    ? 3
                    : 4),
        0,
      );
      continue;
    }
    if (Array.isArray(message.groupDeliveries)) {
      records += message.groupDeliveries.filter(
        (delivery) => delivery.state === "scheduled",
      ).length;
      continue;
    }
    const delivery = message.delivery;
    if (delivery.admissionState !== "admitted") continue;
    if (
      [
        "turn_started",
        "failed",
        "expired",
        "cancelled",
        "policy_denied",
      ].includes(delivery.state)
    ) {
      continue;
    }
    if (delivery.state === "unknown") {
      records += 1;
      continue;
    }
    if (delivery.state === "created") {
      records += delivery.attempts.length === 0 ? 4 : 3;
      continue;
    }
    if (delivery.state === "retryable") {
      records += delivery.attempts.length === 1 ? 2 : 1;
      continue;
    }
    records += delivery.attempts.length > 0 ? 2 : delivery.claim ? 3 : 4;
  }
  return records * DELIVERY_LEDGER_EVENT_RESERVE_BYTES;
}

function cloneBatch(record) {
  const deliveries = record.deliveries.map((delivery) => ({
    ...structuredClone(delivery),
    attempts: [],
    evidence: [],
    claim: null,
    claimCount: 0,
  }));
  const projection = {
    schemaVersion: record.schemaVersion,
    batchId: record.batchId,
    committedAt: record.committedAt,
    logicalMessage: structuredClone(record.logicalMessage),
    delivery: deliveries[0],
  };
  if (record.logicalMessage.teamCast) {
    for (const delivery of deliveries) delivery.schedule = null;
    projection.teamDeliveries = deliveries;
  } else if (deliveries.length > 1) projection.groupDeliveries = deliveries;
  return projection;
}

function applyDeliveryEvent(projected, record) {
  if (Array.isArray(projected?.teamDeliveries)) {
    const delivery = projected.teamDeliveries.find(
      (candidate) => candidate.deliveryId === record.deliveryId,
    );
    const initialWakePolicy = projected.logicalMessage.teamCast.wakePolicy;
    if (
      !delivery ||
      ![initialWakePolicy, ...SCHEDULED_WAKE_POLICIES].includes(
        delivery.wakePolicy,
      )
    ) {
      throw new Error(`invalid Team Cast Delivery event: ${record.messageId}`);
    }
    if (record.recordType === "team-delivery-schedule") {
      if (
        !validTeamSchedule(record) ||
        delivery.wakePolicy !== initialWakePolicy ||
        delivery.state !== "prepared" ||
        delivery.schedule != null ||
        delivery.attempts.length !== 0 ||
        delivery.evidence.length !== 0 ||
        delivery.claim !== null ||
        !delivery.targetNodeKey.startsWith("codex:")
      ) {
        throw new Error(`invalid Team Cast schedule: ${record.messageId}`);
      }
      delivery.wakePolicy = record.wakePolicy;
      delivery.state = "scheduled";
      delivery.schedule = structuredClone(record);
      delivery.updatedAt = record.scheduledAt;
    } else if (record.recordType === "delivery-claim") {
      if (
        !validClaim(record) ||
        !SCHEDULED_WAKE_POLICIES.includes(delivery.wakePolicy) ||
        delivery.state !== "scheduled" ||
        delivery.attempts.length !== 0
      ) {
        throw new Error(`invalid Team Cast claim: ${record.messageId}`);
      }
      if (record.action === "acquired") {
        if (
          delivery.claim &&
          Date.parse(record.claimedAt) < Date.parse(delivery.claim.leaseUntil)
        ) {
          throw new Error(`overlapping Team Cast claim: ${record.messageId}`);
        }
        delivery.claim = structuredClone(record);
        delivery.claimCount += 1;
        delivery.updatedAt = record.claimedAt;
      } else if (record.action === "renewed") {
        const claim = delivery.claim;
        if (
          !claim ||
          claim.claimId !== record.claimId ||
          claim.workerId !== record.workerId ||
          Date.parse(record.renewedAt) <
            Date.parse(claim.renewedAt || claim.claimedAt) ||
          Date.parse(record.renewedAt) >= Date.parse(claim.leaseUntil) ||
          Date.parse(record.leaseUntil) <= Date.parse(claim.leaseUntil)
        ) {
          throw new Error(`Team Cast claim renewal has no live owner: ${record.messageId}`);
        }
        delivery.claim = {
          ...claim,
          leaseUntil: record.leaseUntil,
          renewedAt: record.renewedAt,
          renewalCount: (claim.renewalCount || 0) + 1,
        };
        delivery.updatedAt = record.renewedAt;
      } else {
        if (
          !delivery.claim ||
          delivery.claim.claimId !== record.claimId ||
          delivery.claim.workerId !== record.workerId ||
          Date.parse(record.releasedAt) <
            Date.parse(
              delivery.claim.renewedAt || delivery.claim.claimedAt,
            )
        ) {
          throw new Error(`Team Cast claim release has no owner: ${record.messageId}`);
        }
        delivery.claim = null;
        delivery.updatedAt = record.releasedAt;
      }
    } else if (record.recordType === "delivery-attempt") {
      const direct =
        delivery.wakePolicy === initialWakePolicy &&
        delivery.state === "prepared" &&
        record.claimId === null;
      const scheduled =
        SCHEDULED_WAKE_POLICIES.includes(delivery.wakePolicy) &&
        delivery.state === "scheduled" &&
        delivery.claim?.claimId === record.claimId &&
        Date.parse(record.startedAt) >= Date.parse(delivery.updatedAt);
      if (
        !validAttempt(record) ||
        record.transport !== expectedTeamDeliveryTransport(delivery) ||
        (!direct && !scheduled) ||
        delivery.attempts.length !== 0 ||
        delivery.evidence.length !== 0 ||
        record.retryOfAttemptId !== undefined
      ) {
        throw new Error(
          `duplicate Team Cast Delivery attempt: ${record.messageId}`,
        );
      }
      delivery.attempts.push(structuredClone(record));
      delivery.updatedAt = record.startedAt;
    } else if (record.recordType === "team-delivery-evidence") {
      const attempt = delivery.attempts[0];
      const expiry =
        record.state === "expired" &&
        record.attemptId === null &&
        SCHEDULED_WAKE_POLICIES.includes(delivery.wakePolicy) &&
        delivery.state === "scheduled" &&
        delivery.attempts.length === 0 &&
        delivery.claim === null;
      const policyDenied =
        record.state === "policy_denied" &&
        record.attemptId === null &&
        delivery.attempts.length === 0 &&
        ((delivery.state === "prepared" && delivery.claim === null) ||
          (SCHEDULED_WAKE_POLICIES.includes(delivery.wakePolicy) &&
            delivery.state === "scheduled" &&
            delivery.claim !== null));
      const attempted =
        record.attemptId !== null &&
        ["prepared", "scheduled"].includes(delivery.state) &&
        delivery.attempts.length === 1 &&
        attempt?.attemptId === record.attemptId;
      if (
        !validTeamEvidence(record) ||
        (!expiry && !policyDenied && !attempted) ||
        (record.state === "turn_started" &&
          attempt.transport !== "codex-app-server") ||
        (record.state === "transport_delivered" &&
          attempt.transport !== "claude-uds") ||
        delivery.evidence.length !== 0 ||
        Date.parse(record.observedAt) < Date.parse(delivery.updatedAt)
      ) {
        throw new Error(
          `invalid Team Cast Delivery evidence: ${record.messageId}`,
        );
      }
      delivery.state = record.state;
      delivery.turnId = record.turnId;
      delivery.transportResult = record.transportResult;
      delivery.errorCode = record.errorCode;
      delivery.updatedAt = record.observedAt;
      delivery.evidence.push(structuredClone(record));
      delivery.claim = null;
    } else {
      throw new Error(
        `unsupported Team Cast Delivery event: ${record.messageId}`,
      );
    }
    if (projected.delivery.deliveryId === delivery.deliveryId) {
      projected.delivery = delivery;
    }
    return projected;
  }
  if (Array.isArray(projected?.groupDeliveries)) {
    const delivery = projected.groupDeliveries.find(
      (candidate) => candidate.deliveryId === record.deliveryId,
    );
    if (
      !delivery ||
      !validGroupEvidence(record) ||
      delivery.wakePolicy !== "store-only" ||
      delivery.state !== "scheduled" ||
      delivery.attempts.length !== 0 ||
      delivery.evidence.length !== 0
    ) {
      throw new Error(`invalid Group Delivery evidence: ${record.messageId}`);
    }
    delivery.state = record.state;
    delivery.errorCode = record.errorCode;
    delivery.updatedAt = record.observedAt;
    delivery.evidence.push(structuredClone(record));
    if (projected.delivery.deliveryId === delivery.deliveryId) {
      projected.delivery = delivery;
    }
    return projected;
  }
  if (!projected || projected.delivery.deliveryId !== record.deliveryId) {
    throw new Error(`orphaned Delivery Ledger record: ${record.messageId}`);
  }
  if (projected.delivery.admissionState !== "admitted") {
    throw new Error(`quarantined Delivery has transport evidence: ${record.messageId}`);
  }
  if (record.recordType === "delivery-claim") {
    if (
      !SCHEDULED_WAKE_POLICIES.includes(projected.delivery.wakePolicy) ||
      projected.delivery.state !== "scheduled"
    ) {
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
      return projected;
    }
    if (record.action === "renewed") {
      const claim = projected.delivery.claim;
      if (
        !claim ||
        claim.claimId !== record.claimId ||
        claim.workerId !== record.workerId ||
        Date.parse(record.renewedAt) <
          Date.parse(claim.renewedAt || claim.claimedAt) ||
        Date.parse(record.renewedAt) >= Date.parse(claim.leaseUntil) ||
        Date.parse(record.leaseUntil) <= Date.parse(claim.leaseUntil)
      ) {
        throw new Error(`Delivery claim renewal has no live owner: ${record.messageId}`);
      }
      projected.delivery.claim = {
        ...claim,
        leaseUntil: record.leaseUntil,
        renewedAt: record.renewedAt,
        renewalCount: (claim.renewalCount || 0) + 1,
      };
      projected.delivery.updatedAt = record.renewedAt;
      return projected;
    }
    if (
      !projected.delivery.claim ||
      projected.delivery.claim.claimId !== record.claimId ||
      projected.delivery.claim.workerId !== record.workerId ||
      Date.parse(record.releasedAt) <
        Date.parse(
          projected.delivery.claim.renewedAt ||
            projected.delivery.claim.claimedAt,
        )
    ) {
      throw new Error(`Delivery claim release has no owner: ${record.messageId}`);
    }
    projected.delivery.claim = null;
    projected.delivery.updatedAt = record.releasedAt;
    return projected;
  }
  if (record.recordType === "delivery-attempt") {
    const immediate =
      projected.delivery.wakePolicy === "immediate" &&
      projected.delivery.state === "created" &&
      (record.claimId === undefined || record.claimId === null) &&
      record.retryOfAttemptId === undefined;
    const scheduled =
      SCHEDULED_WAKE_POLICIES.includes(projected.delivery.wakePolicy) &&
      projected.delivery.state === "scheduled" &&
      projected.delivery.claim?.claimId === record.claimId &&
      Date.parse(record.startedAt) >= Date.parse(projected.delivery.updatedAt) &&
      record.retryOfAttemptId === undefined;
    const retry =
      projected.delivery.wakePolicy === "immediate" &&
      projected.delivery.state === "retryable" &&
      projected.delivery.attempts.length === 1 &&
      record.retryOfAttemptId === projected.delivery.attempts[0].attemptId &&
      (record.claimId === undefined || record.claimId === null);
    if (
      record.transport !== "codex-app-server" ||
      (!immediate && !scheduled && !retry) ||
      projected.delivery.attempts.length >= MAX_ORDINARY_DELIVERY_ATTEMPTS
    ) {
      throw new Error(`duplicate Delivery attempt: ${record.messageId}`);
    }
    projected.delivery.attempts.push(structuredClone(record));
    projected.delivery.updatedAt = record.startedAt;
    return projected;
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
      (current === "created" && ["turn_started", "retryable", "unknown"].includes(record.state)) ||
      (current === "scheduled" && ["turn_started", "unknown"].includes(record.state)) ||
      (current === "retryable" &&
        [
          "turn_started",
          "failed",
          "unknown",
          "expired",
          "policy_denied",
        ].includes(record.state)) ||
      (current === "scheduled" &&
        ["expired", "cancelled", "policy_denied"].includes(record.state)) ||
      (current === "unknown" && ["turn_started", "unknown"].includes(record.state)) ||
      (current === "turn_started" && record.state === "turn_started") ||
      (["expired", "cancelled"].includes(current) && record.state === current)
    )
  ) {
    throw new Error(`invalid Delivery evidence transition: ${current}->${record.state}`);
  }
  if (
      (record.state === "retryable" && projected.delivery.attempts.length !== 1) ||
      (record.state === "failed" && projected.delivery.attempts.length !== 2) ||
      (record.state === "policy_denied" &&
        !(
          (current === "retryable" &&
            projected.delivery.attempts.length === 1) ||
          (current === "scheduled" &&
            SCHEDULED_WAKE_POLICIES.includes(projected.delivery.wakePolicy) &&
            projected.delivery.attempts.length === 0 &&
            projected.delivery.claim)
        )) ||
    (record.evidenceKind === "retry-policy" &&
      projected.delivery.attempts.length !== 1)
  ) {
    throw new Error(`Delivery retry evidence has an invalid attempt count: ${record.messageId}`);
  }
  projected.delivery.state = record.state;
  projected.delivery.turnId = record.turnId;
  projected.delivery.transportResult = record.transportResult;
  projected.delivery.errorCode = record.errorCode;
  projected.delivery.updatedAt = record.observedAt;
  projected.delivery.evidence.push(structuredClone(record));
  if (record.state === "policy_denied") projected.delivery.claim = null;
  return projected;
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
        record.deliveries.some((delivery) =>
          deliveryIds.has(delivery.deliveryId),
        ) ||
        batchIds.has(record.batchId)
      ) {
        throw new Error(`duplicate Delivery Ledger batch identity: ${messageId}`);
      }
      const projected = cloneBatch(record);
      messages.set(messageId, projected);
      for (const delivery of record.deliveries) {
        deliveryIds.set(delivery.deliveryId, projected);
      }
      batchIds.add(record.batchId);
      continue;
    }
    applyDeliveryEvent(messages.get(record.messageId), record);
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

function withDeliveryLedgerMutation(callback) {
  return withRetentionWriter(() => {
    ensureStore();
    return withFileLock(DELIVERY_LEDGER_LOCK_PATH, callback);
  });
}

export function readDeliveryLedger(messageId) {
  assertRetentionReadable();
  return ledgerState(messageId);
}

export function listDeliveryLedger() {
  assertRetentionReadable();
  return [...rebuildDeliveryLedgerRecords(readRecords()).values()].sort((left, right) =>
    left.committedAt.localeCompare(right.committedAt),
  );
}

export function listDeliveryLedgerReadOnly() {
  assertRetentionReadableNoCreate();
  return [...rebuildDeliveryLedgerRecords(readRecords()).values()].sort((left, right) =>
    left.committedAt.localeCompare(right.committedAt),
  );
}

export function findDeliveryByReplyHandle({ replyHandle, target, targetThreadId }) {
  if (!REPLY_HANDLE_PATTERN.test(replyHandle || "")) {
    throw new Error("reply handle is invalid");
  }
  if (!NAME_PATTERN.test(target || "")) throw new Error("reply handle target is invalid");
  validateUuid("reply handle target thread id", targetThreadId);
  const matches = listDeliveryLedger().filter(
    (record) =>
      record.delivery.admissionState === "admitted" &&
      record.delivery.target === target &&
      record.delivery.targetThreadId === targetThreadId &&
      record.delivery.replyHandle === replyHandle,
  );
  if (matches.length > 1) {
    throw new Error("reply handle is ambiguous for the current recipient Node");
  }
  return matches[0] ? structuredClone(matches[0]) : null;
}

export async function listDeliveryLedgerIndexed() {
  assertRetentionReadable();
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () =>
    [...readDeliveryIndexLocked().values()]
      .map((record) => structuredClone(record))
      .sort(
        (left, right) =>
          left.committedAt.localeCompare(right.committedAt) ||
          left.logicalMessage.messageId.localeCompare(right.logicalMessage.messageId),
      ),
  );
}

export async function readDeliveryLedgerIndexed(messageId) {
  assertRetentionReadable();
  validateUuid("logical message id", messageId);
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const record = readDeliveryIndexLocked().get(messageId);
    return record ? structuredClone(record) : null;
  });
}

export async function rebuildDeliveryLedgerIndex({
  expectedLedgerManifestSha256 = null,
  expectedIndexGenerationSha256 = null,
} = {}) {
  return withDeliveryLedgerMutation(async () => {
    const currentEvidence = deliveryLedgerIndexRepairEvidence();
    if (
      (expectedLedgerManifestSha256 &&
        currentEvidence.ledgerManifestSha256 !== expectedLedgerManifestSha256) ||
      (expectedIndexGenerationSha256 &&
        currentEvidence.indexGenerationSha256 !== expectedIndexGenerationSha256)
    ) {
      const error = new Error("Delivery Ledger index repair evidence changed");
      error.code = "EREPAIRSTALE";
      throw error;
    }
    const messages = rebuildDeliveryIndexLocked();
    return { messageCount: messages.size, manifestSha256: manifestDigest(ledgerManifest()) };
  });
}

export function listDeliveryDedupTombstones() {
  ensureStore();
  return readdirSync(DELIVERY_LEDGER_TOMBSTONES_DIR)
    .filter((name) => TOMBSTONE_PATTERN.test(name))
    .sort()
    .map((name) => readDedupTombstone(name.slice(0, -5)));
}

export async function createDeliveryDedupTombstones(
  {
    messageIds,
    backupId,
    purgedAt = new Date().toISOString(),
    coupledQuarantineMessageIds = [],
  },
) {
  assertRetentionMutation();
  if (!Array.isArray(messageIds) || messageIds.length < 1) {
    throw new Error("Delivery dedup Tombstones require at least one message id");
  }
  validateUuid("Retention backup id", backupId);
  if (!validTimestamp(purgedAt)) throw new Error("invalid Delivery purge timestamp");
  const unique = [...new Set(messageIds.map((messageId) =>
    validateUuid("logical message id", messageId),
  ))].sort();
  const coupledQuarantine = new Set(
    coupledQuarantineMessageIds.map((messageId) =>
      validateUuid("coupled Quarantine message id", messageId),
    ),
  );
  if ([...coupledQuarantine].some((messageId) => !unique.includes(messageId))) {
    throw new Error("coupled Quarantine Tombstone is outside the purge selection");
  }
  ensureStore();
  return withFileLock(DELIVERY_LEDGER_LOCK_PATH, async () => {
    const messages = readDeliveryIndexLocked();
    const records = unique.map((messageId) => {
      const existingTombstone = readDedupTombstone(messageId);
      const message = messages.get(messageId);
      if (!message && existingTombstone) return existingTombstone;
      if (!message) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
      const deliveries =
        message.teamDeliveries || message.groupDeliveries || [message.delivery];
      const admittedTerminal = deliveries.every(
        (delivery) =>
          delivery.admissionState === "admitted" &&
          ["turn_started", "failed", "expired", "cancelled"].includes(
            delivery.state,
          ),
      );
      const quarantinedCoupled =
        message.delivery.admissionState === "quarantined" &&
        coupledQuarantine.has(messageId) &&
        ["created", "scheduled"].includes(message.delivery.state);
      if (
        (!admittedTerminal && !quarantinedCoupled) ||
        deliveries.some((delivery) => delivery.claim)
      ) {
        throw new Error(`Delivery is not terminal and purgeable: ${messageId}`);
      }
      return {
        schemaVersion: 1,
        recordType: "delivery-dedup-tombstone",
        messageId,
        deliveryFingerprintSha256: createHash("sha256")
          .update(JSON.stringify({
            logicalMessage: message.logicalMessage,
            deliveries: deliveries.map((delivery) => ({
              target: delivery.target,
              targetThreadId: delivery.targetThreadId,
              admissionState: delivery.admissionState,
              admissionReason: delivery.admissionReason,
              wakePolicy: delivery.wakePolicy,
            })),
          }))
          .digest("hex"),
        purgedAt,
        backupId,
      };
    });
    return records.map(writeDedupTombstone);
  });
}

export async function commitSingleRecipientDelivery(
  {
    logicalMessage,
    target,
    targetThreadId = null,
    admissionState,
    admissionReason,
    inboundPolicy = null,
    wakePolicy = "immediate",
    now = new Date().toISOString(),
  },
  {
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
    scheduledPerTargetLimit = SCHEDULED_DELIVERY_PER_TARGET_LIMIT,
    replyHandleFactory = createReplyHandle,
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
  if (!["admitted", "quarantined", "denied"].includes(admissionState)) {
    throw new Error("invalid Delivery admission state");
  }
  if (
    (admissionState === "denied" && !validInboundPolicyDenial(inboundPolicy)) ||
    (admissionState !== "denied" && inboundPolicy !== null)
  ) {
    throw new Error("invalid Inbound Policy denial evidence");
  }
  if (!["immediate", ...SCHEDULED_WAKE_POLICIES].includes(wakePolicy)) {
    throw new Error("Ledger v1 does not support this wake policy");
  }
  if (!validTimestamp(now)) throw new Error("invalid Delivery timestamp");
  if (typeof replyHandleFactory !== "function") {
    throw new Error("reply handle factory must be a function");
  }

  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    if (readDedupTombstone(logicalMessage.messageId)) {
      throw new Error(
        `Delivery Ledger message was permanently purged: ${logicalMessage.messageId}`,
      );
    }
    if (
      logicalMessage.replyToMessageId &&
      readDedupTombstone(logicalMessage.replyToMessageId)
    ) {
      throw new Error(
        `peer reply references a permanently purged message: ${logicalMessage.replyToMessageId}`,
      );
    }
    const existing = messages.get(logicalMessage.messageId);
    if (existing) {
      if (
        JSON.stringify(existing.logicalMessage) !== JSON.stringify(logicalMessage) ||
        existing.delivery.target !== target ||
        existing.delivery.targetThreadId !== targetThreadId ||
        existing.delivery.admissionState !== admissionState ||
        existing.delivery.admissionReason !== admissionReason ||
        JSON.stringify(existing.delivery.inboundPolicy || null) !==
          JSON.stringify(inboundPolicy) ||
        existing.delivery.wakePolicy !== wakePolicy
      ) {
        throw new Error(`Delivery Ledger idempotency conflict: ${logicalMessage.messageId}`);
      }
      return { record: existing, created: false };
    }
    if (
      SCHEDULED_WAKE_POLICIES.includes(wakePolicy) &&
      [...messages.values()].filter(
        (message) =>
          (message.delivery.targetThreadId || message.delivery.target) ===
            (targetThreadId || target) &&
          message.delivery.admissionState === "admitted" &&
          SCHEDULED_WAKE_POLICIES.includes(message.delivery.wakePolicy) &&
          message.delivery.state === "scheduled" &&
          message.delivery.attempts.length === 0,
      ).length >= scheduledPerTargetLimit
    ) {
      throw new Error(
        `scheduled Delivery queue for ${target} reached ${scheduledPerTargetLimit}`,
      );
    }
    let replyHandle = null;
    if (
      admissionState === "admitted" &&
      targetThreadId &&
      (logicalMessage.senderThreadId || logicalMessage.senderNodeKey)
    ) {
      const namespace = targetThreadId || target;
      for (let attempt = 0; attempt < 16; attempt += 1) {
        const candidate = replyHandleFactory();
        if (!REPLY_HANDLE_PATTERN.test(candidate || "")) {
          throw new Error("reply handle factory returned an invalid handle");
        }
        const collision = [...messages.values()].some(
          (message) =>
            (message.delivery.targetThreadId || message.delivery.target) === namespace &&
            message.delivery.replyHandle === candidate,
        );
        if (!collision) {
          replyHandle = candidate;
          break;
        }
      }
      if (!replyHandle) {
        const error = new Error("could not allocate a unique reply handle");
        error.code = "EREPLYHANDLECOLLISION";
        throw error;
      }
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
          replyHandle,
          admissionState,
          admissionReason,
          ...(inboundPolicy
            ? { inboundPolicy: structuredClone(inboundPolicy) }
            : {}),
          wakePolicy,
          state:
            admissionState === "denied"
              ? "denied"
              : wakePolicy === "immediate"
                ? "created"
                : "scheduled",
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
    updateDeliveryIndexLocked(after, logicalMessage.messageId);
    return { record: projected, created: true };
  });
}

export async function commitStoreOnlyGroupDelivery(
  {
    logicalMessage,
    recipients,
    now = new Date().toISOString(),
  },
  {
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateStorageOptions(quotaBytes, segmentBytes);
  validateUuid("logical message id", logicalMessage?.messageId);
  if (!NAME_PATTERN.test(logicalMessage?.from || "")) {
    throw new Error("invalid Group Ledger sender");
  }
  if (!Array.isArray(recipients) || recipients.length < 2 || recipients.length > 64) {
    throw new Error("Group Delivery requires 2-64 fixed recipients");
  }
  if (!validTimestamp(now)) throw new Error("invalid Group Delivery timestamp");
  const normalizedRecipients = recipients.map((recipient) => {
    if (!NODE_KEY_PATTERN.test(recipient?.nodeKey || "")) {
      throw new Error("Group Delivery recipient must be a stable Node key");
    }
    const nodeKey = recipient.nodeKey.toLowerCase();
    const runtimeKind = nodeKey.split(":", 1)[0];
    const nativeId = nodeKey.slice(runtimeKind.length + 1);
    const targetThreadId = recipient.targetThreadId ?? null;
    if (
      (runtimeKind === "codex" && targetThreadId !== nativeId) ||
      (runtimeKind === "claude" && targetThreadId !== null)
    ) {
      throw new Error("Group Delivery recipient runtime identity is inconsistent");
    }
    const inboundPolicy = recipient.inboundPolicy ?? null;
    if (inboundPolicy !== null && !validInboundPolicyDenial(inboundPolicy)) {
      throw new Error("Group Delivery recipient policy evidence is invalid");
    }
    return { nodeKey, targetThreadId, inboundPolicy };
  });
  if (
    JSON.stringify([...normalizedRecipients].sort((left, right) =>
      left.nodeKey.localeCompare(right.nodeKey),
    )) !== JSON.stringify(normalizedRecipients) ||
    new Set(normalizedRecipients.map((recipient) => recipient.nodeKey)).size !==
      normalizedRecipients.length
  ) {
    throw new Error("Group Delivery recipients must be sorted and unique");
  }

  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    if (readDedupTombstone(logicalMessage.messageId)) {
      throw new Error(
        `Delivery Ledger message was permanently purged: ${logicalMessage.messageId}`,
      );
    }
    if (
      logicalMessage.replyToMessageId &&
      readDedupTombstone(logicalMessage.replyToMessageId)
    ) {
      throw new Error(
        `peer reply references a permanently purged message: ${logicalMessage.replyToMessageId}`,
      );
    }
    const existing = messages.get(logicalMessage.messageId);
    if (existing) {
      const existingRecipients = (existing.groupDeliveries || []).map(
        (delivery) => ({
          nodeKey: delivery.targetNodeKey,
          targetThreadId: delivery.targetThreadId,
          inboundPolicy: delivery.inboundPolicy ?? null,
        }),
      );
      if (
        JSON.stringify(existing.logicalMessage) !== JSON.stringify(logicalMessage) ||
        JSON.stringify(existingRecipients) !== JSON.stringify(normalizedRecipients)
      ) {
        throw new Error(
          `Delivery Ledger idempotency conflict: ${logicalMessage.messageId}`,
        );
      }
      return { record: existing, created: false };
    }
    const record = {
      schemaVersion: 1,
      recordType: "ledger-batch",
      batchId: randomUUID(),
      committedAt: now,
      logicalMessage: structuredClone(logicalMessage),
      deliveries: normalizedRecipients.map((recipient) => ({
        deliveryId: randomUUID(),
        target: recipient.nodeKey,
        targetNodeKey: recipient.nodeKey,
        targetThreadId: recipient.targetThreadId,
        replyHandle: null,
        admissionState: recipient.inboundPolicy ? "denied" : "admitted",
        admissionReason: recipient.inboundPolicy
          ? "inbound_policy"
          : "group_membership",
        ...(recipient.inboundPolicy
          ? { inboundPolicy: structuredClone(recipient.inboundPolicy) }
          : {}),
        wakePolicy: "store-only",
        state: recipient.inboundPolicy ? "denied" : "scheduled",
        createdAt: now,
        updatedAt: now,
      })),
    };
    if (!validBatch(record)) throw new Error("invalid Group Delivery Ledger batch");
    const projected = cloneBatch(record);
    const after = new Map(messages);
    after.set(logicalMessage.messageId, projected);
    appendLedgerRecord(record, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, logicalMessage.messageId);
    return { record: projected, created: true };
  });
}

export async function commitPreparedTeamCastDelivery(
  {
    logicalMessage,
    recipients,
    now = new Date().toISOString(),
  },
  {
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateStorageOptions(quotaBytes, segmentBytes);
  validateUuid("logical message id", logicalMessage?.messageId);
  if (!NAME_PATTERN.test(logicalMessage?.from || "")) {
    throw new Error("invalid Team Cast Ledger sender");
  }
  if (
    !Array.isArray(recipients) ||
    recipients.length < 1 ||
    recipients.length > TEAM_CAST_RECIPIENT_LIMIT
  ) {
    throw new Error(
      `Team Cast Delivery requires 1-${TEAM_CAST_RECIPIENT_LIMIT} fixed recipients`,
    );
  }
  if (!validTimestamp(now)) {
    throw new Error("invalid Team Cast Delivery timestamp");
  }
  const normalizedRecipients = recipients.map((recipient) => {
    if (!NODE_KEY_PATTERN.test(recipient?.nodeKey || "")) {
      throw new Error("Team Cast Delivery recipient must be a stable Node key");
    }
    const nodeKey = recipient.nodeKey.toLowerCase();
    const runtimeKind = nodeKey.split(":", 1)[0];
    const nativeId = nodeKey.slice(runtimeKind.length + 1);
    const targetThreadId = recipient.targetThreadId ?? null;
    if (
      (runtimeKind === "codex" && targetThreadId !== nativeId) ||
      (runtimeKind === "claude" && targetThreadId !== null)
    ) {
      throw new Error(
        "Team Cast Delivery recipient runtime identity is inconsistent",
      );
    }
    const inboundPolicy = recipient.inboundPolicy ?? null;
    if (inboundPolicy !== null && !validInboundPolicyDenial(inboundPolicy)) {
      throw new Error("Team Cast recipient policy evidence is invalid");
    }
    return { nodeKey, targetThreadId, inboundPolicy };
  });
  if (
    JSON.stringify(
      [...normalizedRecipients].sort((left, right) =>
        left.nodeKey.localeCompare(right.nodeKey),
      ),
    ) !== JSON.stringify(normalizedRecipients) ||
    new Set(normalizedRecipients.map((recipient) => recipient.nodeKey)).size !==
      normalizedRecipients.length
  ) {
    throw new Error("Team Cast Delivery recipients must be sorted and unique");
  }

  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    if (readDedupTombstone(logicalMessage.messageId)) {
      throw new Error(
        `Delivery Ledger message was permanently purged: ${logicalMessage.messageId}`,
      );
    }
    const existing = messages.get(logicalMessage.messageId);
    if (existing) {
      const existingRecipients = (existing.teamDeliveries || []).map(
        (delivery) => ({
          nodeKey: delivery.targetNodeKey,
          targetThreadId: delivery.targetThreadId,
          inboundPolicy: delivery.inboundPolicy ?? null,
        }),
      );
      if (
        JSON.stringify(existing.logicalMessage) !== JSON.stringify(logicalMessage) ||
        JSON.stringify(existingRecipients) !== JSON.stringify(normalizedRecipients)
      ) {
        throw new Error(
          `Delivery Ledger idempotency conflict: ${logicalMessage.messageId}`,
        );
      }
      return { record: existing, created: false };
    }
    const record = {
      schemaVersion: 1,
      recordType: "ledger-batch",
      batchId: randomUUID(),
      committedAt: now,
      logicalMessage: structuredClone(logicalMessage),
      deliveries: normalizedRecipients.map((recipient) => ({
        deliveryId: randomUUID(),
        target: recipient.nodeKey,
        targetNodeKey: recipient.nodeKey,
        targetThreadId: recipient.targetThreadId,
        replyHandle: null,
        admissionState: recipient.inboundPolicy ? "denied" : "admitted",
        admissionReason: recipient.inboundPolicy
          ? "inbound_policy"
          : "team_cast_plan",
        ...(recipient.inboundPolicy
          ? { inboundPolicy: structuredClone(recipient.inboundPolicy) }
          : {}),
        wakePolicy: logicalMessage.teamCast.wakePolicy,
        state: recipient.inboundPolicy ? "denied" : "prepared",
        createdAt: now,
        updatedAt: now,
      })),
    };
    if (!validBatch(record)) {
      throw new Error("invalid prepared Team Cast Delivery Ledger batch");
    }
    const projected = cloneBatch(record);
    const after = new Map(messages);
    after.set(logicalMessage.messageId, projected);
    appendLedgerRecord(record, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, logicalMessage.messageId);
    return { record: projected, created: true };
  });
}

export async function appendStoreOnlyGroupDeliveryEvidence(
  messageId,
  {
    targetNodeKey,
    state,
    errorCode,
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  },
) {
  messageId = validateUuid("logical message id", messageId);
  if (!NODE_KEY_PATTERN.test(targetNodeKey || "")) {
    throw new Error("Group Delivery evidence target must be a stable Node key");
  }
  targetNodeKey = targetNodeKey.toLowerCase();
  if (!["failed", "expired", "cancelled"].includes(state)) {
    throw new Error("Group Delivery evidence state is invalid");
  }
  if (!/^[A-Z0-9_]{1,32}$/.test(errorCode || "")) {
    throw new Error("Group Delivery evidence error code is invalid");
  }
  if (!validTimestamp(now)) throw new Error("Group Delivery evidence timestamp is invalid");
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    const delivery = record?.groupDeliveries?.find(
      (candidate) => candidate.targetNodeKey === targetNodeKey,
    );
    if (!delivery) throw new Error("unknown Group recipient Delivery");
    if (delivery.state !== "scheduled") {
      if (delivery.state === state && delivery.errorCode === errorCode) {
        return { record: structuredClone(record), created: false };
      }
      throw new Error("Group recipient Delivery already has terminal evidence");
    }
    const event = {
      schemaVersion: 1,
      recordType: "group-delivery-evidence",
      messageId,
      deliveryId: delivery.deliveryId,
      state,
      errorCode,
      observedAt: now,
    };
    if (!validGroupEvidence(event)) throw new Error("invalid Group Delivery evidence");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { record: structuredClone(projected), created: true };
  });
}

export async function beginTeamCastRecipientDelivery(
  messageId,
  targetNodeKey,
  {
    now = new Date().toISOString(),
    transport = "codex-app-server",
    inboundPolicySnapshot = undefined,
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  messageId = validateUuid("logical message id", messageId);
  if (!NODE_KEY_PATTERN.test(targetNodeKey || "")) {
    throw new Error("Team Cast attempt target must be a stable Node key");
  }
  targetNodeKey = targetNodeKey.toLowerCase();
  if (!validTimestamp(now)) {
    throw new Error("Team Cast attempt timestamp is invalid");
  }
  if (!["codex-app-server", "claude-uds"].includes(transport)) {
    throw new Error("Team Cast attempt transport is invalid");
  }
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    const delivery = record?.teamDeliveries?.find(
      (candidate) => candidate.targetNodeKey === targetNodeKey,
    );
    if (!delivery) throw new Error("unknown Team Cast recipient Delivery");
    if (delivery.state !== "prepared") {
      throw new Error("Team Cast recipient Delivery is already terminal");
    }
    if (delivery.attempts.length === 1) {
      return { attempt: structuredClone(delivery.attempts[0]), created: false };
    }
    if (delivery.attempts.length !== 0 || delivery.evidence.length !== 0) {
      throw new Error("Team Cast recipient Delivery state is inconsistent");
    }
    if (transport !== expectedTeamDeliveryTransport(delivery)) {
      throw new Error("Team Cast attempt transport does not match recipient runtime");
    }
    if (Date.parse(record.logicalMessage.teamCast.expiresAt) <= Date.parse(now)) {
      throw new Error("Team Cast recipient Delivery expired before dispatch");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-attempt",
      messageId,
      deliveryId: delivery.deliveryId,
      attemptId: randomUUID(),
      claimId: null,
      ...(inboundPolicySnapshot
        ? { inboundPolicySnapshot: structuredClone(inboundPolicySnapshot) }
        : {}),
      transport,
      startedAt: now,
    };
    if (!validAttempt(event)) throw new Error("invalid Team Cast Delivery attempt");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { attempt: structuredClone(event), created: true };
  });
}

export async function scheduleTeamCastRecipientDelivery(
  messageId,
  targetNodeKey,
  {
    now = new Date().toISOString(),
    wakePolicy = "when-idle",
    triggerTurnId = null,
    triggerJobId = null,
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  messageId = validateUuid("logical message id", messageId);
  if (!NODE_KEY_PATTERN.test(targetNodeKey || "")) {
    throw new Error("Team Cast schedule target must be a stable Node key");
  }
  targetNodeKey = targetNodeKey.toLowerCase();
  if (!SCHEDULED_WAKE_POLICIES.includes(wakePolicy)) {
    throw new Error("Team Cast schedule wake policy is unsupported");
  }
  if (!validTimestamp(now)) {
    throw new Error("Team Cast schedule timestamp is invalid");
  }
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    const delivery = record?.teamDeliveries?.find(
      (candidate) => candidate.targetNodeKey === targetNodeKey,
    );
    if (!delivery) throw new Error("unknown Team Cast recipient Delivery");
    if (delivery.schedule) {
      if (
        delivery.wakePolicy === wakePolicy &&
        delivery.state === "scheduled" &&
        (delivery.schedule.triggerTurnId ?? null) === triggerTurnId &&
        (delivery.schedule.triggerJobId ?? null) === triggerJobId
      ) {
        return { record: structuredClone(record), scheduled: false };
      }
      throw new Error("Team Cast recipient already has another outcome");
    }
    if (!delivery.targetNodeKey.startsWith("codex:")) {
      throw new Error("Team Cast scheduled fallback currently requires Codex");
    }
    if (Date.parse(record.logicalMessage.teamCast.expiresAt) <= Date.parse(now)) {
      throw new Error("Team Cast recipient expired before scheduling");
    }
    const lane = delivery.targetThreadId || delivery.targetNodeKey;
    let queuedForLane = 0;
    for (const message of messages.values()) {
      const candidates = message.teamDeliveries || [message.delivery];
      queuedForLane += candidates.filter(
        (candidate) =>
          SCHEDULED_WAKE_POLICIES.includes(candidate.wakePolicy) &&
          candidate.state === "scheduled" &&
          candidate.attempts.length === 0 &&
          (candidate.targetThreadId || candidate.target) === lane,
      ).length;
    }
    if (queuedForLane >= SCHEDULED_DELIVERY_PER_TARGET_LIMIT) {
      throw new Error(
        `scheduled Delivery queue for ${lane} reached ${SCHEDULED_DELIVERY_PER_TARGET_LIMIT}`,
      );
    }
    const event = {
      schemaVersion: 1,
      recordType: "team-delivery-schedule",
      messageId,
      deliveryId: delivery.deliveryId,
      wakePolicy,
      triggerTurnId,
      triggerJobId,
      scheduledAt: now,
    };
    if (!validTeamSchedule(event)) throw new Error("invalid Team Cast schedule");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { record: structuredClone(projected), scheduled: true };
  });
}

export async function appendTeamCastRecipientEvidence(
  messageId,
  targetNodeKey,
  {
    attemptId,
    state,
    turnId = null,
    transportResult = null,
    errorCode = null,
    inboundPolicy = undefined,
    observedAt = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  },
) {
  messageId = validateUuid("logical message id", messageId);
  if (!NODE_KEY_PATTERN.test(targetNodeKey || "")) {
    throw new Error("Team Cast evidence target must be a stable Node key");
  }
  targetNodeKey = targetNodeKey.toLowerCase();
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    const delivery = record?.teamDeliveries?.find(
      (candidate) => candidate.targetNodeKey === targetNodeKey,
    );
    if (!delivery) throw new Error("unknown Team Cast recipient Delivery");
    const event = {
      schemaVersion: 1,
      recordType: "team-delivery-evidence",
      messageId,
      deliveryId: delivery.deliveryId,
      attemptId,
      state,
      turnId,
      transportResult,
      errorCode,
      ...(inboundPolicy ? { inboundPolicy: structuredClone(inboundPolicy) } : {}),
      observedAt,
    };
    if (!validTeamEvidence(event)) {
      throw new Error("invalid Team Cast Delivery evidence");
    }
    const existing = delivery.evidence[0];
    if (existing) {
      if (JSON.stringify(existing) === JSON.stringify(event)) {
        return { record: structuredClone(record), created: false };
      }
      throw new Error("Team Cast recipient Delivery already has evidence");
    }
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { record: structuredClone(projected), created: true };
  });
}

export async function beginImmediateDelivery(messageId, options = {}) {
  const quotaBytes = options.quotaBytes ?? DELIVERY_LEDGER_QUOTA_BYTES;
  const segmentBytes = options.segmentBytes ?? DELIVERY_LEDGER_SEGMENT_BYTES;
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
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
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return structuredClone(event);
  });
}

export async function beginRetryDelivery(messageId, options = {}) {
  const quotaBytes = options.quotaBytes ?? DELIVERY_LEDGER_QUOTA_BYTES;
  const segmentBytes = options.segmentBytes ?? DELIVERY_LEDGER_SEGMENT_BYTES;
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    if (
      record.delivery.admissionState !== "admitted" ||
      record.delivery.wakePolicy !== "immediate" ||
      record.delivery.state !== "retryable" ||
      record.delivery.attempts.length !== 1
    ) {
      throw new Error("Delivery is not eligible for its one explicit retry");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-attempt",
      messageId,
      deliveryId: record.delivery.deliveryId,
      attemptId: randomUUID(),
      retryOfAttemptId: record.delivery.attempts[0].attemptId,
      claimId: null,
      transport: "codex-app-server",
      startedAt: options.now || new Date().toISOString(),
    };
    if (!validAttempt(event)) throw new Error("invalid Delivery retry attempt");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return structuredClone(event);
  });
}

function scheduledDeliveryForMutation(record, targetNodeKey = null) {
  if (targetNodeKey === null) return record?.delivery || null;
  if (!NODE_KEY_PATTERN.test(targetNodeKey || "")) {
    throw new Error("scheduled Delivery target must be a stable Node key");
  }
  const normalized = targetNodeKey.toLowerCase();
  return (
    record?.teamDeliveries?.find(
      (delivery) => delivery.targetNodeKey === normalized,
    ) || null
  );
}

export async function claimScheduledDelivery(
  messageId,
  {
    workerId,
    targetNodeKey = null,
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
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    const delivery = scheduledDeliveryForMutation(record, targetNodeKey);
    if (
      delivery?.admissionState !== "admitted" ||
      !SCHEDULED_WAKE_POLICIES.includes(delivery.wakePolicy) ||
      delivery.state !== "scheduled" ||
      delivery.attempts.length > 0
    ) {
      throw new Error("Delivery is not claimable for scheduled dispatch");
    }
    const claimedAt = Date.parse(now);
    if (delivery.claim && Date.parse(delivery.claim.leaseUntil) > claimedAt) {
      return { claim: structuredClone(delivery.claim), acquired: false };
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-claim",
      action: "acquired",
      messageId,
      deliveryId: delivery.deliveryId,
      claimId: randomUUID(),
      workerId,
      claimedAt: now,
      leaseUntil: new Date(claimedAt + leaseMs).toISOString(),
    };
    if (!validClaim(event)) throw new Error("invalid Delivery claim");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { claim: structuredClone(event), acquired: true };
  });
}

export async function releaseScheduledDeliveryClaim(
  messageId,
  {
    claimId,
    workerId,
    reason,
    targetNodeKey = null,
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
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    const delivery = scheduledDeliveryForMutation(record, targetNodeKey);
    if (!delivery) throw new Error("unknown scheduled Delivery target");
    if (!delivery.claim) return { released: false, record };
    if (
      delivery.claim.claimId !== claimId ||
      delivery.claim.workerId !== workerId
    ) {
      throw new Error("Delivery claim is owned by another scheduler worker");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-claim",
      action: "released",
      messageId,
      deliveryId: delivery.deliveryId,
      claimId,
      workerId,
      reason,
      releasedAt: now,
    };
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { released: true, record: after.get(messageId) };
  });
}

export async function renewScheduledDeliveryClaim(
  messageId,
  {
    claimId,
    workerId,
    targetNodeKey = null,
    leaseMs = 30_000,
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateUuid("Delivery claim id", claimId);
  validateUuid("scheduler worker id", workerId);
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1_000 || leaseMs > 300_000) {
    throw new Error("Delivery claim lease must be 1000-300000 milliseconds");
  }
  if (!validTimestamp(now)) throw new Error("invalid Delivery claim renewal timestamp");
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    const delivery = scheduledDeliveryForMutation(record, targetNodeKey);
    if (!delivery) throw new Error("unknown scheduled Delivery target");
    const claim = delivery.claim;
    const renewedAt = Date.parse(now);
    if (
      delivery.state !== "scheduled" ||
      delivery.attempts.length > 0 ||
      claim?.claimId !== claimId ||
      claim?.workerId !== workerId ||
      Date.parse(claim.leaseUntil) <= renewedAt
    ) {
      const error = new Error("Delivery claim is missing, expired, or owned by another scheduler worker");
      error.code = "ECLAIMLOST";
      throw error;
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-claim",
      action: "renewed",
      messageId,
      deliveryId: delivery.deliveryId,
      claimId,
      workerId,
      renewedAt: now,
      leaseUntil: new Date(
        Math.max(Date.parse(claim.leaseUntil) + 1, renewedAt + leaseMs),
      ).toISOString(),
    };
    if (!validClaim(event)) throw new Error("invalid Delivery claim renewal");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    const projectedDelivery = scheduledDeliveryForMutation(
      projected,
      targetNodeKey,
    );
    return { claim: structuredClone(projectedDelivery.claim), renewed: true };
  });
}

export async function beginScheduledDelivery(
  messageId,
  {
    claimId,
    workerId,
    targetNodeKey = null,
    inboundPolicySnapshot = undefined,
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateUuid("Delivery claim id", claimId);
  validateUuid("scheduler worker id", workerId);
  if (!validTimestamp(now)) throw new Error("invalid scheduled Delivery timestamp");
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    const delivery = scheduledDeliveryForMutation(record, targetNodeKey);
    if (
      delivery?.admissionState !== "admitted" ||
      !SCHEDULED_WAKE_POLICIES.includes(delivery.wakePolicy) ||
      delivery.state !== "scheduled" ||
      delivery.attempts.length > 0 ||
      delivery.claim?.claimId !== claimId ||
      delivery.claim?.workerId !== workerId ||
      Date.parse(delivery.claim.leaseUntil) <= Date.parse(now)
    ) {
      throw new Error("scheduled Delivery claim is missing, expired, or already dispatched");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-attempt",
      messageId,
      deliveryId: delivery.deliveryId,
      attemptId: randomUUID(),
      claimId,
      ...(inboundPolicySnapshot
        ? { inboundPolicySnapshot: structuredClone(inboundPolicySnapshot) }
        : {}),
      transport: "codex-app-server",
      startedAt: now,
    };
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return structuredClone(event);
  });
}

export async function cancelScheduledDelivery(
  messageId,
  {
    now = new Date().toISOString(),
    quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
    segmentBytes = DELIVERY_LEDGER_SEGMENT_BYTES,
  } = {},
) {
  validateUuid("logical message id", messageId);
  if (!validTimestamp(now)) throw new Error("invalid scheduled Delivery cancellation timestamp");
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    if (record.delivery.state === "cancelled") {
      return { record: structuredClone(record), cancelled: false };
    }
    if (
      record.delivery.admissionState !== "admitted" ||
      !SCHEDULED_WAKE_POLICIES.includes(record.delivery.wakePolicy) ||
      record.delivery.state !== "scheduled" ||
      record.delivery.attempts.length > 0
    ) {
      throw new Error("only an unattempted scheduled Delivery can be cancelled");
    }
    if (
      record.delivery.claim &&
      Date.parse(record.delivery.claim.leaseUntil) > Date.parse(now)
    ) {
      throw new Error("scheduled Delivery has an active claim; retry cancellation after its lease");
    }
    const event = {
      schemaVersion: 1,
      recordType: "delivery-evidence",
      messageId,
      deliveryId: record.delivery.deliveryId,
      attemptId: null,
      state: "cancelled",
      evidenceKind: "scheduler",
      turnId: null,
      transportResult: null,
      errorCode: "EDELIVERYCANCELLED",
      observedAt: now,
    };
    if (!validEvidence(event)) throw new Error("invalid Delivery cancellation evidence");
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return { record: projected, cancelled: true };
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
    negativeAcceptanceContract = undefined,
    inboundPolicy = undefined,
    observedAt = new Date().toISOString(),
  },
  options = {},
) {
  const quotaBytes = options.quotaBytes ?? DELIVERY_LEDGER_QUOTA_BYTES;
  const segmentBytes = options.segmentBytes ?? DELIVERY_LEDGER_SEGMENT_BYTES;
  validateStorageOptions(quotaBytes, segmentBytes);
  return withDeliveryLedgerMutation(async () => {
    const messages = readDeliveryIndexLocked();
    const record = messages.get(messageId);
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
      ...(negativeAcceptanceContract ? { negativeAcceptanceContract } : {}),
      ...(inboundPolicy
        ? { inboundPolicy: structuredClone(inboundPolicy) }
        : {}),
      observedAt,
    };
    if (!validEvidence(event)) throw new Error("invalid Delivery evidence");
    if (
      record.delivery.state === state &&
      record.delivery.errorCode === errorCode &&
      record.delivery.turnId === turnId &&
      record.delivery.transportResult === transportResult &&
      (record.delivery.evidence.at(-1)?.negativeAcceptanceContract || null) ===
        (negativeAcceptanceContract || null) &&
      JSON.stringify(record.delivery.evidence.at(-1)?.inboundPolicy || null) ===
        JSON.stringify(inboundPolicy || null) &&
      record.delivery.evidence.at(-1)?.evidenceKind === evidenceKind
    ) {
      return record;
    }
    const after = new Map(messages);
    const projected = structuredClone(record);
    after.set(messageId, projected);
    applyDeliveryEvent(projected, event);
    appendLedgerRecord(event, {
      quotaBytes,
      segmentBytes,
      reserveBytes: reservedEvidenceBytes(after),
    });
    updateDeliveryIndexLocked(after, messageId);
    return projected;
  });
}
