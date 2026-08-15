import { createHash } from "node:crypto";
import { listDeliveryLedgerIndexed } from "./delivery-ledger.js";
import { listJobs } from "./jobs.js";
import { listMessageBodies } from "./message-bodies.js";
import { listQuarantineForRetention } from "./route-admission.js";
import { listConversationMessageIds } from "./conversations.js";
import { listGroupConversationMessageIds } from "./group-conversations.js";

export const DELIVERY_RETENTION_MIN_AGE_MS = 90 * 24 * 60 * 60 * 1_000;
export const MESSAGE_BODY_RETENTION_MIN_AGE_MS = 30 * 24 * 60 * 60 * 1_000;
export const QUARANTINE_RETENTION_MIN_AGE_MS = 30 * 24 * 60 * 60 * 1_000;

const SCOPES = new Set(["all", "ledger", "bodies", "quarantine"]);
const TERMINAL_DELIVERY_STATES = new Set([
  "turn_started",
  "failed",
  "expired",
  "cancelled",
]);

function timestamp(label, value) {
  const parsed = Date.parse(value || "");
  if (!Number.isFinite(parsed)) throw new Error(`${label} must be an ISO timestamp`);
  return parsed;
}

function protectedMessageIds(ledger, jobs, conversationMessageIds = []) {
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
  for (const messageId of conversationMessageIds) {
    protect(messageId, "conversation_history");
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

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function selectionDigest(plan) {
  return sha256(JSON.stringify({
    schemaVersion: plan.schemaVersion,
    cutoff: plan.cutoff,
    scope: plan.scope,
    eligible: Object.fromEntries(
      Object.entries(plan.categories).map(([kind, category]) => [
        kind,
        category.eligible.map((candidate) => ({ ...candidate })).sort((left, right) =>
          left.messageId.localeCompare(right.messageId),
        ),
      ]),
    ),
  }));
}

export function planRetention(
  {
    before,
    scope = "all",
    ledger = [],
    bodies = [],
    quarantine = [],
    jobs = [],
    conversationMessageIds = [],
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
  const protectedIds = protectedMessageIds(
    ledger,
    jobs,
    conversationMessageIds,
  );
  const ledgerById = new Map(
    ledger.map((record) => [record.logicalMessage.messageId, record]),
  );
  const categories = {
    ledger: emptyCategory(),
    bodies: emptyCategory(),
    quarantine: emptyCategory(),
  };
  const coupledQuarantineIds = new Set();

  if (include("quarantine")) {
    for (const record of quarantine) {
      const quarantinedAt = timestamp("Quarantine timestamp", record.quarantinedAt);
      if (quarantinedAt >= cutoff) {
        categories.quarantine.retainedByAge += 1;
        continue;
      }
      const reasons = [];
      const linked = ledgerById.get(record.logicalMessageId);
      for (const reason of protectedIds.get(record.logicalMessageId) || []) {
        addReason(reasons, reason);
      }
      if (linked) {
        if (scope !== "all") {
          addReason(reasons, "linked_delivery_requires_all_scope");
        }
        if (timestamp("linked Delivery updatedAt", linked.delivery.updatedAt) >= cutoff) {
          addReason(reasons, "linked_delivery_retained_by_age");
        }
        if (
          linked.delivery.admissionState !== "quarantined" ||
          linked.logicalMessage.from !== record.from ||
          linked.delivery.target !== record.target ||
          linked.logicalMessage.createdAt !== record.quarantinedAt ||
          linked.logicalMessage.body.bytes !== record.messageBytes ||
          linked.logicalMessage.body.sha256 !== record.messageSha256
        ) {
          addReason(reasons, "quarantine_ledger_mismatch");
        }
      }
      const candidate = {
        messageId: record.logicalMessageId,
        quarantinedAt: record.quarantinedAt,
        reason: record.reason,
        estimatedBytes: Number.isSafeInteger(record.messageBytes)
          ? record.messageBytes
          : 0,
      };
      if (reasons.length) categories.quarantine.blocked.push({ ...candidate, reasons });
      else {
        categories.quarantine.eligible.push(candidate);
        categories.quarantine.estimatedBytes += candidate.estimatedBytes;
        if (linked) coupledQuarantineIds.add(record.logicalMessageId);
      }
    }
  }

  if (include("ledger")) {
    for (const record of ledger) {
      const messageId = record.logicalMessage.messageId;
      const deliveries =
        record.teamDeliveries || record.groupDeliveries || [record.delivery];
      const deliveryUpdatedAt = deliveries.reduce(
        (latest, delivery) =>
          delivery.updatedAt > latest ? delivery.updatedAt : latest,
        record.logicalMessage.createdAt,
      );
      const updatedAt = timestamp("Delivery updatedAt", deliveryUpdatedAt);
      if (updatedAt >= cutoff) {
        categories.ledger.retainedByAge += 1;
        continue;
      }
      const reasons = [];
      const coupledQuarantine =
        !record.teamDeliveries &&
        !record.groupDeliveries &&
        record.delivery.admissionState === "quarantined" &&
        coupledQuarantineIds.has(messageId);
      if (
        deliveries.some((delivery) => delivery.admissionState !== "admitted") &&
        !coupledQuarantine
      ) {
        addReason(reasons, "quarantined_delivery");
      }
      if (!coupledQuarantine) {
        for (const delivery of deliveries) {
          if (!TERMINAL_DELIVERY_STATES.has(delivery.state)) {
            addReason(reasons, `nonterminal_${delivery.state}`);
          }
        }
      }
      for (const reason of protectedIds.get(messageId) || []) addReason(reasons, reason);
      const candidate = {
        messageId,
        state:
          deliveries.length === 1
            ? deliveries[0].state
            : [...new Set(deliveries.map((delivery) => delivery.state))].join(","),
        updatedAt: deliveryUpdatedAt,
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
      const linkedDeliveries = linked
        ? linked.teamDeliveries || linked.groupDeliveries || [linked.delivery]
        : [];
      if (
        linkedDeliveries.some(
          (delivery) =>
            delivery.admissionState !== "admitted" ||
            !TERMINAL_DELIVERY_STATES.has(delivery.state),
        )
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

  const plan = {
    schemaVersion: 1,
    generatedAt: new Date(now).toISOString(),
    cutoff: new Date(cutoff).toISOString(),
    scope,
    policy: {
      automaticDeletion: false,
      mutationEnabled: true,
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
        "conversation_history",
        "quarantined_delivery",
      ],
    },
    categories,
  };
  return { ...plan, planDigest: selectionDigest(plan) };
}

export async function buildRetentionPlan(
  { before, scope = "all" },
  {
    ledgerReader = listDeliveryLedgerIndexed,
    bodyReader = listMessageBodies,
    quarantineReader = listQuarantineForRetention,
    jobReader = listJobs,
    conversationReader = () => [
      ...listConversationMessageIds(),
      ...listGroupConversationMessageIds(),
    ],
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
      conversationMessageIds: needsLedger ? conversationReader() : [],
    },
    { now },
  );
}
