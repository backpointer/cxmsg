import { listDeliveryLedgerIndexed } from "./delivery-ledger.js";
import { listJobs } from "./jobs.js";
import { listMessageBodies } from "./message-bodies.js";
import { listQuarantine } from "./route-admission.js";

export const DELIVERY_RETENTION_MIN_AGE_MS = 90 * 24 * 60 * 60 * 1_000;
export const MESSAGE_BODY_RETENTION_MIN_AGE_MS = 30 * 24 * 60 * 60 * 1_000;
export const QUARANTINE_RETENTION_MIN_AGE_MS = 30 * 24 * 60 * 60 * 1_000;

const SCOPES = new Set(["all", "ledger", "bodies", "quarantine"]);
const TERMINAL_DELIVERY_STATES = new Set(["turn_started", "expired", "cancelled"]);

function timestamp(label, value) {
  const parsed = Date.parse(value || "");
  if (!Number.isFinite(parsed)) throw new Error(`${label} must be an ISO timestamp`);
  return parsed;
}

function protectedMessageIds(ledger, jobs) {
  const protectedIds = new Map();
  const protect = (messageId, reason) => {
    if (!messageId) return;
    const reasons = protectedIds.get(messageId) || new Set();
    reasons.add(reason);
    protectedIds.set(messageId, reasons);
  };
  for (const record of ledger) {
    const replyTo = record.logicalMessage.replyToMessageId;
    if (replyTo) {
      protect(record.logicalMessage.messageId, "reply_chain");
      protect(replyTo, "reply_chain");
    }
  }
  for (const job of jobs) {
    if (job.correlation?.kind !== "peer-reply") continue;
    protect(job.correlation.logicalMessageId, "job_correlation");
    protect(job.correlation.replyToMessageId, "job_correlation");
  }
  return protectedIds;
}

function addReason(reasons, reason) {
  if (!reasons.includes(reason)) reasons.push(reason);
}

function minimumAge(scope) {
  if (scope === "ledger" || scope === "all") return DELIVERY_RETENTION_MIN_AGE_MS;
  if (scope === "bodies") return MESSAGE_BODY_RETENTION_MIN_AGE_MS;
  return QUARANTINE_RETENTION_MIN_AGE_MS;
}

function emptyCategory() {
  return { eligible: [], blocked: [], retainedByAge: 0, estimatedBytes: 0 };
}

export function planRetention(
  {
    before,
    scope = "all",
    ledger = [],
    bodies = [],
    quarantine = [],
    jobs = [],
  },
  { now = Date.now() } = {},
) {
  if (!SCOPES.has(scope)) {
    throw new Error("retention scope must be all, ledger, bodies, or quarantine");
  }
  const cutoff = timestamp("retention cutoff", before);
  if (!Number.isFinite(now)) throw new Error("retention clock is invalid");
  if (cutoff > now - minimumAge(scope)) {
    const days = minimumAge(scope) / (24 * 60 * 60 * 1_000);
    throw new Error(`retention cutoff must preserve at least ${days} days for ${scope}`);
  }

  const include = (kind) => scope === "all" || scope === kind;
  const protectedIds = protectedMessageIds(ledger, jobs);
  const ledgerById = new Map(
    ledger.map((record) => [record.logicalMessage.messageId, record]),
  );
  const categories = {
    ledger: emptyCategory(),
    bodies: emptyCategory(),
    quarantine: emptyCategory(),
  };

  if (include("ledger")) {
    for (const record of ledger) {
      const messageId = record.logicalMessage.messageId;
      const updatedAt = timestamp("Delivery updatedAt", record.delivery.updatedAt);
      if (updatedAt >= cutoff) {
        categories.ledger.retainedByAge += 1;
        continue;
      }
      const reasons = [];
      if (record.delivery.admissionState !== "admitted") {
        addReason(reasons, "quarantined_delivery");
      }
      if (!TERMINAL_DELIVERY_STATES.has(record.delivery.state)) {
        addReason(reasons, `nonterminal_${record.delivery.state}`);
      }
      for (const reason of protectedIds.get(messageId) || []) addReason(reasons, reason);
      const candidate = {
        messageId,
        state: record.delivery.state,
        updatedAt: record.delivery.updatedAt,
        estimatedBytes: Buffer.byteLength(JSON.stringify(record), "utf8"),
      };
      if (reasons.length) categories.ledger.blocked.push({ ...candidate, reasons });
      else {
        categories.ledger.eligible.push(candidate);
        categories.ledger.estimatedBytes += candidate.estimatedBytes;
      }
    }
  }

  if (include("bodies")) {
    for (const body of bodies) {
      const createdAt = timestamp("Message Body createdAt", body.createdAt);
      if (createdAt >= cutoff) {
        categories.bodies.retainedByAge += 1;
        continue;
      }
      const reasons = [];
      const linked = ledgerById.get(body.messageId);
      if (
        linked &&
        (linked.delivery.admissionState !== "admitted" ||
          !TERMINAL_DELIVERY_STATES.has(linked.delivery.state))
      ) {
        addReason(reasons, "delivery_not_terminal");
      }
      for (const reason of protectedIds.get(body.messageId) || []) addReason(reasons, reason);
      const candidate = {
        messageId: body.messageId,
        createdAt: body.createdAt,
        estimatedBytes: body.bodyBytes,
      };
      if (reasons.length) categories.bodies.blocked.push({ ...candidate, reasons });
      else {
        categories.bodies.eligible.push(candidate);
        categories.bodies.estimatedBytes += candidate.estimatedBytes;
      }
    }
  }

  if (include("quarantine")) {
    for (const record of quarantine) {
      const quarantinedAt = timestamp("Quarantine timestamp", record.quarantinedAt);
      if (quarantinedAt >= cutoff) {
        categories.quarantine.retainedByAge += 1;
        continue;
      }
      const candidate = {
        messageId: record.logicalMessageId,
        quarantinedAt: record.quarantinedAt,
        reason: record.reason,
        estimatedBytes: Number.isSafeInteger(record.messageBytes)
          ? record.messageBytes
          : 0,
      };
      categories.quarantine.eligible.push(candidate);
      categories.quarantine.estimatedBytes += candidate.estimatedBytes;
    }
  }

  return {
    schemaVersion: 1,
    generatedAt: new Date(now).toISOString(),
    cutoff: new Date(cutoff).toISOString(),
    scope,
    policy: {
      automaticDeletion: false,
      mutationEnabled: false,
      minimumAgeDays: {
        ledger: DELIVERY_RETENTION_MIN_AGE_MS / (24 * 60 * 60 * 1_000),
        bodies: MESSAGE_BODY_RETENTION_MIN_AGE_MS / (24 * 60 * 60 * 1_000),
        quarantine: QUARANTINE_RETENTION_MIN_AGE_MS / (24 * 60 * 60 * 1_000),
      },
      protectedEvidence: [
        "nonterminal_delivery",
        "unknown_delivery",
        "reply_chain",
        "job_correlation",
        "quarantined_delivery",
      ],
    },
    categories,
  };
}

export async function buildRetentionPlan(
  { before, scope = "all" },
  {
    ledgerReader = listDeliveryLedgerIndexed,
    bodyReader = listMessageBodies,
    quarantineReader = listQuarantine,
    jobReader = listJobs,
    now = Date.now(),
  } = {},
) {
  if (!SCOPES.has(scope)) {
    throw new Error("retention scope must be all, ledger, bodies, or quarantine");
  }
  const needsLedger = scope === "all" || scope === "ledger" || scope === "bodies";
  const needsBodies = scope === "all" || scope === "bodies";
  const needsQuarantine = scope === "all" || scope === "quarantine";
  const [ledger, bodies] = await Promise.all([
    needsLedger ? ledgerReader() : [],
    needsBodies ? bodyReader() : [],
  ]);
  return planRetention(
    {
      before,
      scope,
      ledger,
      bodies,
      quarantine: needsQuarantine ? quarantineReader() : [],
      jobs: needsLedger ? jobReader() : [],
    },
    { now },
  );
}
