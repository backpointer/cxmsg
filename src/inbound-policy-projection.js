const DECISIONS = new Set(["continue", "deny"]);
const DENY_REASONS = new Set([
  "policy_invalid",
  "sender_denied",
  "project_denied",
  "identity_unverifiable",
  "sender_unidentified",
  "sender_unverifiable",
]);
const CONTINUE_REASONS = new Set([
  "no_policy",
  "no_match",
  "sender_unidentified",
  "sender_unverifiable",
]);
const IDENTITY_STATES = new Set(["verified", "unidentified", "unverifiable"]);
const SELECTOR_KINDS = new Set([
  "sender-node",
  "sender-project",
  "unknown-sender",
]);
const EVIDENCE_SOURCES = new Set([
  "initial-admission",
  "delivery-attempt",
  "terminal-evidence",
]);
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const NODE_KEY_PATTERN = /^(codex|claude):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;

export const INBOUND_POLICY_TRACE_LIMIT = 16;

function validEvidence(value) {
  if (
    !value ||
    !DECISIONS.has(value.decision) ||
    !IDENTITY_STATES.has(value.senderIdentityState) ||
    (value.selectorKind !== null && !SELECTOR_KINDS.has(value.selectorKind)) ||
    typeof value.failClosed !== "boolean" ||
    !(
      value.policyRevision === null ||
      (Number.isSafeInteger(value.policyRevision) && value.policyRevision >= 1)
    ) ||
    !(value.ruleId === null || UUID_PATTERN.test(value.ruleId || "")) ||
    !NODE_KEY_PATTERN.test(value.targetNodeKey || "") ||
    !(value.senderNodeKey === null || NODE_KEY_PATTERN.test(value.senderNodeKey || "")) ||
    !(value.senderProjectId === null || UUID_PATTERN.test(value.senderProjectId || "")) ||
    !(value.policySha256 === null || SHA256_PATTERN.test(value.policySha256 || ""))
  ) {
    return false;
  }
  const verifiedIdentity = Boolean(value.senderNodeKey && value.senderProjectId);
  if ((value.senderIdentityState === "verified") !== verifiedIdentity) return false;
  if (value.decision === "continue") {
    if (
      !CONTINUE_REASONS.has(value.reason) ||
      value.selectorKind !== null ||
      value.ruleId !== null ||
      value.failClosed
    ) {
      return false;
    }
    if (value.reason === "no_policy") {
      return value.policyRevision === null && value.policySha256 === null;
    }
    if (!Number.isSafeInteger(value.policyRevision) || !value.policySha256) return false;
    if (value.reason === "no_match") return value.senderIdentityState === "verified";
    return (
      (value.reason === "sender_unidentified" &&
        value.senderIdentityState === "unidentified") ||
      (value.reason === "sender_unverifiable" &&
        value.senderIdentityState === "unverifiable")
    );
  }
  if (!DENY_REASONS.has(value.reason)) return false;
  if (value.reason === "policy_invalid") {
    return Boolean(
      value.policyRevision === null &&
        value.policySha256 === null &&
        value.ruleId === null &&
        value.selectorKind === null &&
        value.failClosed,
    );
  }
  if (!Number.isSafeInteger(value.policyRevision) || !value.policySha256) return false;
  if (value.reason === "identity_unverifiable") {
    return Boolean(
      value.senderIdentityState === "unverifiable" &&
        value.ruleId === null &&
        value.selectorKind === null &&
        value.failClosed,
    );
  }
  if (value.failClosed || value.ruleId === null) return false;
  if (value.reason === "sender_denied") {
    return value.senderIdentityState === "verified" && value.selectorKind === "sender-node";
  }
  if (value.reason === "project_denied") {
    return value.senderIdentityState === "verified" && value.selectorKind === "sender-project";
  }
  return Boolean(
    value.selectorKind === "unknown-sender" &&
      ((value.reason === "sender_unidentified" &&
        value.senderIdentityState === "unidentified") ||
        (value.reason === "sender_unverifiable" &&
          value.senderIdentityState === "unverifiable")),
  );
}

function projectEvidence(value, evidenceSource) {
  if (!EVIDENCE_SOURCES.has(evidenceSource)) {
    throw new Error("Inbound Policy evidence source is invalid");
  }
  if (!validEvidence(value)) {
    return {
      projectionState: "invalid",
      evidenceSource,
      decision: null,
      reason: null,
      senderIdentityState: null,
      selectorKind: null,
      failClosed: null,
      policyRevision: null,
      matchedRule: null,
    };
  }
  return {
    projectionState: "valid",
    evidenceSource,
    decision: value.decision,
    reason: value.reason,
    senderIdentityState: value.senderIdentityState,
    selectorKind: value.selectorKind,
    failClosed: value.failClosed,
    policyRevision: value.policyRevision,
    matchedRule: value.ruleId !== null,
  };
}

function rawObservations(delivery) {
  const observations = [];
  if (delivery?.inboundPolicy !== undefined) {
    observations.push([delivery.inboundPolicy, "initial-admission"]);
  }
  for (const attempt of delivery?.attempts || []) {
    if (attempt?.inboundPolicySnapshot !== undefined) {
      observations.push([attempt.inboundPolicySnapshot, "delivery-attempt"]);
    }
  }
  for (const evidence of delivery?.evidence || []) {
    if (evidence?.inboundPolicy !== undefined) {
      observations.push([evidence.inboundPolicy, "terminal-evidence"]);
    }
  }
  return observations;
}

export function latestInboundPolicyEvidence(delivery) {
  const observation = rawObservations(delivery).at(-1);
  return observation ? projectEvidence(...observation) : null;
}

export function traceInboundPolicyEvidence(
  delivery,
  { limit = INBOUND_POLICY_TRACE_LIMIT } = {},
) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > INBOUND_POLICY_TRACE_LIMIT) {
    throw new Error(
      `Inbound Policy trace limit must be between 1 and ${INBOUND_POLICY_TRACE_LIMIT}`,
    );
  }
  const observations = rawObservations(delivery);
  const returned = observations.slice(-limit).map((observation) =>
    projectEvidence(...observation),
  );
  return {
    observationCount: observations.length,
    returnedObservationCount: returned.length,
    remainingObservationCount: Math.max(0, observations.length - returned.length),
    latest: returned.at(-1) || null,
    observations: returned,
  };
}

function deliveriesFor(record) {
  return record?.teamDeliveries || record?.groupDeliveries ||
    (record?.delivery ? [record.delivery] : []);
}

export function summarizeInboundPolicyEvidence(records, policyRecords = []) {
  const reasonCounts = new Map();
  let evaluatedRecipientCount = 0;
  let deniedRecipientCount = 0;
  let continuedRecipientCount = 0;
  let invalidEvidenceCount = 0;
  let failClosedDenialCount = 0;
  for (const record of records || []) {
    for (const delivery of deliveriesFor(record)) {
      const evidence = latestInboundPolicyEvidence(delivery);
      if (!evidence) continue;
      evaluatedRecipientCount += 1;
      if (evidence.projectionState !== "valid") {
        invalidEvidenceCount += 1;
        continue;
      }
      if (evidence.decision === "deny") {
        deniedRecipientCount += 1;
        if (evidence.failClosed) failClosedDenialCount += 1;
      } else {
        continuedRecipientCount += 1;
      }
      reasonCounts.set(evidence.reason, (reasonCounts.get(evidence.reason) || 0) + 1);
    }
  }
  return {
    status: "available",
    configuredTargetCount: policyRecords.length,
    configuredRuleCount: policyRecords.reduce(
      (total, record) => total + (Array.isArray(record?.rules) ? record.rules.length : 0),
      0,
    ),
    evaluatedRecipientCount,
    deniedRecipientCount,
    continuedRecipientCount,
    invalidEvidenceCount,
    failClosedDenialCount,
    countsByReason: Object.fromEntries(
      [...reasonCounts.entries()].sort(([left], [right]) => left.localeCompare(right)),
    ),
  };
}

export function unavailableInboundPolicySummary() {
  return {
    status: "unavailable",
    configuredTargetCount: null,
    configuredRuleCount: null,
    evaluatedRecipientCount: null,
    deniedRecipientCount: null,
    continuedRecipientCount: null,
    invalidEvidenceCount: null,
    failClosedDenialCount: null,
    countsByReason: {},
  };
}
