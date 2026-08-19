import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  lstatSync,
  readFileSync,
  readdirSync,
} from "node:fs";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { probeAppServerSocket, withAppServer } from "./app-server-client.js";
import { validDirectConversationRecord } from "./conversations.js";
import { validConversationSummary } from "./conversation-summaries.js";
import {
  CLAUDE_BRIDGE_IMPLEMENTATION_REVISION,
  probeClaudeBridge,
} from "./claude-bridge.js";
import {
  DELIVERY_LEDGER_QUOTA_BYTES,
  DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH,
  DELIVERY_LEDGER_INDEX_DIR,
  DELIVERY_LEDGER_MAX_RECORD_BYTES,
  rebuildDeliveryLedgerRecords,
  validDeliveryLedgerIndexRecord,
  validDeliveryLedgerRecord,
} from "./delivery-ledger.js";
import {
  ROUTE_RECONCILE_GRACE_MS,
  SCHEDULER_HEARTBEAT_STALE_MS,
} from "./delivery-policy.js";
import { validGroupConversationRecord } from "./group-conversations.js";
import { hostRelayRequest } from "./host-relay.js";
import {
  classifyInboundPolicyEntry,
  INBOUND_POLICY_FEATURE_ACTIVE,
  INBOUND_POLICY_MAX_RECORD_BYTES,
  INBOUND_POLICY_MAX_RECORDS,
  INBOUND_POLICY_MAX_RULES,
  validInboundPolicyRecord,
} from "./inbound-policy.js";
import {
  MESSAGE_BODY_SEGMENT_BYTES,
  MESSAGE_BODY_STORE_QUOTA_BYTES,
  MAX_STORED_MESSAGE_BYTES,
} from "./message-bodies.js";
import { processIdentity, processState, serviceEvidence } from "./process-state.js";
import { EVENT_LOG_ARCHIVES, EVENT_LOG_MAX_BYTES } from "./runtime.js";
import { failedProbe } from "./socket-probe.js";
import { readThreadMetadata } from "./thread-activity.js";
import {
  validTeamCastPlanRecord,
  validTeamCastSelectionRecord,
} from "./team-cast.js";
import { validTurnLifecycleState } from "./turn-lifecycle.js";
import {
  CXMSG_IMPLEMENTATION_REVISIONS,
  CXMSG_VERSION,
} from "./version.js";

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SESSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const MAX_RECORD_BYTES = 256 * 1024;
const MAX_DIRECTORY_BYTES = 32 * 1024 * 1024;
const MAX_DIRECTORY_RECORDS = 2048;
const PENDING_JOB_STATES = new Set([
  "dispatching",
  "scheduled",
  "queued",
  "running",
  "awaiting_approval",
]);
const OBSERVABLE_CLAUDE_DELIVERY_STATES = new Set([
  "transport_delivered",
  "acknowledged",
  "retry_scheduled",
  "ack_timeout",
  "completion_timeout",
]);
const ENDPOINT_HISTORY_LIMIT = 64;
const ENDPOINT_TRANSPORT_LIMIT = 16;
const CLUSTER_MEMBER_LIMIT = 256;
const CLUSTER_MEMBERSHIP_HISTORY_WARN_LIMIT = 1024;
const ENDPOINT_STATUSES = new Set([
  "reachable",
  "external-writer",
  "unreachable",
  "stale",
  "unknown",
  "mismatched",
]);
const ENDPOINT_DECISIONS = new Set([
  "baseline-imported",
  "selected",
  "replaced",
  "refreshed",
  "older-rejected",
  "conflict-rejected",
]);
const SUCCESSFUL_ENDPOINT_DECISIONS = new Set([
  "baseline-imported",
  "selected",
  "replaced",
  "refreshed",
]);
const ENDPOINT_OBSERVATION_FIELDS = new Set([
  "transport",
  "endpointId",
  "nodeKey",
  "generation",
  "status",
  "decision",
  "address",
  "sessionName",
  "firstObservedAt",
  "lastObservedAt",
  "observationCount",
]);
const ENDPOINT_FIELDS = new Set([
  "transport",
  "endpointId",
  "nodeKey",
  "generation",
  "status",
  "address",
  "sessionName",
  "observedAt",
]);

export function diagnosticCheck({
  id,
  scope,
  status,
  summary,
  verification = null,
  errorCode = null,
  repairable = false,
  remediation = null,
  required = true,
  observedBytes = null,
  limitBytes = null,
}) {
  return Object.fromEntries(
    Object.entries({
      id,
      scope,
      status,
      summary,
      verification,
      errorCode,
      repairable,
      remediation,
      required,
      observedBytes,
      limitBytes,
    }).filter(([, value]) => value !== null),
  );
}

function implementationRevisionCheck({
  id,
  scope,
  label,
  recordedRevision,
  currentRevision,
  unknownCode,
  staleCode,
  remediation,
}) {
  const current = recordedRevision === currentRevision;
  return diagnosticCheck({
    id,
    scope,
    status: current ? "pass" : "warn",
    summary:
      recordedRevision === undefined
        ? `${label} does not identify its running implementation revision`
        : current
          ? `${label} runs the current implementation revision`
          : `${label} runs a different implementation revision`,
    verification: recordedRevision === undefined ? "record-missing" : "record",
    errorCode:
      current
        ? null
        : recordedRevision === undefined
          ? unknownCode
          : staleCode,
    remediation: current ? null : remediation,
    required: false,
  });
}

function safeLabel(value) {
  if (typeof value === "string" && UUID_PATTERN.test(value)) return value.slice(0, 8);
  if (typeof value === "string" && SESSION_PATTERN.test(value)) return value;
  return `record-${createHash("sha256").update(String(value)).digest("hex").slice(0, 10)}`;
}

function errorCode(error, fallback = "EINSPECT") {
  return typeof error?.code === "string" && /^[A-Z0-9_]{1,32}$/.test(error.code)
    ? error.code
    : fallback;
}

function semanticVersion(value) {
  return /(?:^|\s)(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)(?:\s|$)/.exec(
    value || "",
  )?.[1] || null;
}

function secureMetadata(target, expectedType) {
  try {
    const metadata = lstatSync(target);
    if (metadata.isSymbolicLink()) return { status: "symlink", metadata };
    const typeMatches =
      expectedType === "directory"
        ? metadata.isDirectory()
        : expectedType === "socket"
          ? metadata.isSocket()
          : metadata.isFile();
    if (!typeMatches) return { status: "wrong-type", metadata };
    if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
      return { status: "wrong-owner", metadata };
    }
    if ((metadata.mode & 0o077) !== 0) return { status: "broad-mode", metadata };
    return { status: "secure", metadata };
  } catch (error) {
    return {
      status: error?.code === "ENOENT" ? "missing" : "unavailable",
      errorCode: errorCode(error),
    };
  }
}

function metadataFinding(id, scope, label, evidence, { required = true } = {}) {
  if (evidence.status === "secure") {
    return diagnosticCheck({
      id,
      scope,
      status: "pass",
      summary: `${label} has restrictive owner-only metadata`,
      verification: "metadata",
      required,
    });
  }
  if (evidence.status === "missing") {
    return diagnosticCheck({
      id,
      scope,
      status: required ? "fail" : "skipped",
      summary: `${label} is not present`,
      verification: "missing",
      errorCode: "ENOENT",
      required,
    });
  }
  const status = evidence.status === "unavailable" ? "unknown" : "fail";
  return diagnosticCheck({
    id,
    scope,
    status,
    summary: `${label} failed its type, owner, or mode check`,
    verification: evidence.status,
    errorCode: evidence.errorCode || "ESECURESTATE",
    remediation: "Inspect the owner, mode, type, and symlink status from an allowed host context",
    required,
  });
}

function scanJsonDirectory({
  stateDir,
  directoryName,
  scope,
  validate,
  maxRecordBytes = MAX_RECORD_BYTES,
}) {
  const directory = path.join(stateDir, directoryName);
  const checks = [];
  const directoryEvidence = secureMetadata(directory, "directory");
  if (directoryEvidence.status === "missing") {
    checks.push(metadataFinding(`${scope}.directory`, scope, `${scope} directory`, directoryEvidence, { required: false }));
    return { checks, records: [] };
  }
  checks.push(metadataFinding(`${scope}.directory`, scope, `${scope} directory`, directoryEvidence));
  if (directoryEvidence.status !== "secure") return { checks, records: [] };

  let names;
  try {
    names = readdirSync(directory).filter((name) => name.endsWith(".json")).sort();
  } catch (error) {
    checks.push(diagnosticCheck({
      id: `${scope}.records.read`,
      scope,
      status: "unknown",
      summary: `${scope} records could not be enumerated`,
      verification: "unavailable",
      errorCode: errorCode(error),
    }));
    return { checks, records: [] };
  }
  if (names.length > MAX_DIRECTORY_RECORDS) {
    checks.push(diagnosticCheck({
      id: `${scope}.records.bound`,
      scope,
      status: "warn",
      summary: `${scope} record count exceeds the bounded inspection limit`,
      verification: "bounded",
      errorCode: "ERECORDLIMIT",
      remediation: "Inspect excess records from an allowed host context",
    }));
    names = names.slice(0, MAX_DIRECTORY_RECORDS);
  }

  const records = [];
  let parsedBytes = 0;
  for (const name of names) {
    const stem = name.slice(0, -5);
    const label = safeLabel(stem);
    const file = path.join(directory, name);
    const evidence = secureMetadata(file, "file");
    if (evidence.status !== "secure") {
      checks.push(metadataFinding(`${scope}.record.${label}.metadata`, scope, `${scope} record ${label}`, evidence));
      continue;
    }
    if (evidence.metadata.size > maxRecordBytes) {
      checks.push(diagnosticCheck({
        id: `${scope}.record.${label}.size`,
        scope,
        status: "fail",
        summary: `${scope} record ${label} exceeds the bounded parse limit`,
        verification: "metadata",
        errorCode: "ERECORDSIZE",
      }));
      continue;
    }
    if (parsedBytes + evidence.metadata.size > MAX_DIRECTORY_BYTES) {
      checks.push(diagnosticCheck({
        id: `${scope}.records.bytes`,
        scope,
        status: "warn",
        summary: `${scope} records exceed the bounded aggregate parse limit`,
        verification: "bounded",
        errorCode: "ERECORDBYTES",
        remediation: "Inspect excess records from an allowed host context",
      }));
      break;
    }
    parsedBytes += evidence.metadata.size;
    try {
      const record = JSON.parse(readFileSync(file, "utf8"));
      const verdict = validate(record, stem);
      if (!verdict.valid) {
        checks.push(diagnosticCheck({
          id: `${scope}.record.${label}.schema`,
          scope,
          status: "fail",
          summary: `${scope} record ${label} has an invalid schema or filename identity`,
          verification: "schema",
          errorCode: verdict.errorCode || "ESCHEMA",
        }));
        continue;
      }
      records.push(record);
    } catch {
      checks.push(diagnosticCheck({
        id: `${scope}.record.${label}.json`,
        scope,
        status: "fail",
        summary: `${scope} record ${label} is not valid bounded JSON`,
        verification: "parse",
        errorCode: "EJSON",
      }));
    }
  }
  if (!checks.some((check) => check.status === "fail" || check.status === "unknown")) {
    checks.push(diagnosticCheck({
      id: `${scope}.records.schema`,
      scope,
      status: "pass",
      summary: `${records.length} ${scope} record(s) passed bounded schema inspection`,
      verification: "schema",
    }));
  }
  return { checks, records };
}

function inspectDeliveryLedger(
  stateDir,
  { quotaBytes = DELIVERY_LEDGER_QUOTA_BYTES } = {},
) {
  const scope = "delivery-ledger";
  const root = path.join(stateDir, "delivery-ledger");
  const checks = [];
  const rootEvidence = secureMetadata(root, "directory");
  if (rootEvidence.status === "missing") {
    checks.push(
      metadataFinding(`${scope}.directory`, scope, "Delivery Ledger directory", rootEvidence, {
        required: false,
      }),
    );
    return { checks, records: [] };
  }
  checks.push(metadataFinding(`${scope}.directory`, scope, "Delivery Ledger directory", rootEvidence));
  if (rootEvidence.status !== "secure") return { checks, records: [] };

  const events = [];
  let parsedBytes = 0;
  let usageBytes = 0;
  let parseLimitReported = false;
  for (const directoryName of ["segments", "quarantine"]) {
    const directory = path.join(root, directoryName);
    const directoryEvidence = secureMetadata(directory, "directory");
    checks.push(
      metadataFinding(
        `${scope}.${directoryName}.directory`,
        scope,
        `Delivery Ledger ${directoryName} directory`,
        directoryEvidence,
      ),
    );
    if (directoryEvidence.status !== "secure") continue;
    let names;
    try {
      names = readdirSync(directory)
        .filter((name) => /^segment-\d{8}(?:\.partial-[0-9a-f-]+)?\.jsonl$/i.test(name))
        .sort();
    } catch (error) {
      checks.push(
        diagnosticCheck({
          id: `${scope}.${directoryName}.read`,
          scope,
          status: "unknown",
          summary: `Delivery Ledger ${directoryName} could not be enumerated`,
          verification: "unavailable",
          errorCode: errorCode(error),
        }),
      );
      continue;
    }
    for (const name of names) {
      const file = path.join(directory, name);
      const segmentNumber = name.match(/^segment-(\d{8})/i)?.[1] || "unknown";
      const label = `segment-${segmentNumber}`;
      const evidence = secureMetadata(file, "file");
      if (evidence.status !== "secure") {
        checks.push(
          metadataFinding(
            `${scope}.${directoryName}.${label}.metadata`,
            scope,
            `Delivery Ledger segment ${label}`,
            evidence,
          ),
        );
        continue;
      }
      usageBytes += evidence.metadata.size;
      if (parsedBytes + evidence.metadata.size > MAX_DIRECTORY_BYTES) {
        if (!parseLimitReported) {
          checks.push(
            diagnosticCheck({
              id: `${scope}.records.bytes`,
              scope,
              status: "warn",
              summary: "Delivery Ledger exceeds the bounded Doctor parse limit",
              verification: "bounded",
              errorCode: "ERECORDBYTES",
              remediation: "Inspect the Ledger from an allowed host context",
            }),
          );
          parseLimitReported = true;
        }
        continue;
      }
      parsedBytes += evidence.metadata.size;
      let raw;
      try {
        raw = readFileSync(file, "utf8");
      } catch (error) {
        checks.push(
          diagnosticCheck({
            id: `${scope}.${directoryName}.${label}.read`,
            scope,
            status: "unknown",
            summary: `Delivery Ledger segment ${label} could not be read`,
            verification: "unavailable",
            errorCode: errorCode(error),
          }),
        );
        continue;
      }
      const complete = raw.endsWith("\n");
      if (!complete) {
        checks.push(
          diagnosticCheck({
            id: `${scope}.${directoryName}.${label}.partial`,
            scope,
            status: directoryName === "segments" ? "fail" : "warn",
            summary: `Delivery Ledger ${directoryName} segment has an incomplete final record`,
            verification: "records",
            errorCode: "ELEDGERPARTIAL",
            remediation:
              directoryName === "segments"
                ? "Do not dispatch from this Ledger until the writer quarantines the partial segment"
                : null,
          }),
        );
      }
      const lines = raw.split("\n");
      if (!complete) lines.pop();
      else if (lines.at(-1) === "") lines.pop();
      for (const [index, line] of lines.entries()) {
        try {
          if (Buffer.byteLength(line, "utf8") > DELIVERY_LEDGER_MAX_RECORD_BYTES) {
            throw new Error("record-size");
          }
          const record = JSON.parse(line);
          if (!validDeliveryLedgerRecord(record)) throw new Error("record-schema");
          events.push(record);
        } catch {
          checks.push(
            diagnosticCheck({
              id: `${scope}.${directoryName}.${label}.line.${index + 1}`,
              scope,
              status: "fail",
              summary: `Delivery Ledger segment ${label} line ${index + 1} has invalid bounded evidence`,
              verification: "schema",
              errorCode: "ELEDGERSCHEMA",
              remediation:
                "Back up the complete Ledger, then move the whole affected segment to an archive; do not edit or partially delete evidence in place",
            }),
          );
          break;
        }
      }
    }
  }

  const quotaStatus = usageBytes >= quotaBytes ? "fail" : usageBytes >= quotaBytes * 0.9 ? "warn" : "pass";
  checks.push(
    diagnosticCheck({
      id: `${scope}.quota.usage`,
      scope,
      status: quotaStatus,
      summary:
        quotaStatus === "fail"
          ? "Delivery Ledger reached its metadata quota; new sends fail closed while retained evidence remains"
          : quotaStatus === "warn"
            ? "Delivery Ledger exceeds 90 percent of its metadata quota; reserved terminal evidence further reduces headroom"
            : "Delivery Ledger is below 90 percent of its metadata quota; reserved terminal evidence also consumes headroom",
      verification: "metadata",
      errorCode:
        quotaStatus === "fail"
          ? "ELEDGERQUOTA"
          : quotaStatus === "warn"
            ? "ELEDGERQUOTAWARN"
            : null,
      remediation:
        quotaStatus === "pass"
          ? null
          : "Stop new sends and back up the complete Ledger; do not edit, move, or delete segments before a supported retention or purge operation exists",
      required: quotaStatus === "fail",
    }),
  );

  let records = [];
  let projections = null;
  let ledgerProjectionDigests = new Map();
  try {
    const rebuilt = rebuildDeliveryLedgerRecords(events);
    ledgerProjectionDigests = new Map(
      [...rebuilt.entries()].map(([messageId, record]) => [
        messageId,
        createHash("sha256").update(JSON.stringify(record)).digest("hex"),
      ]),
    );
    projections = [...rebuilt.values()];
    records = projections.map((record) => {
      const message = record.logicalMessage;
      const delivery = record.delivery;
      const activeAttempt = delivery.attempts.at(-1) || null;
      const teamStates = record.teamDeliveries?.map((candidate) =>
        candidate.state === "prepared" && candidate.attempts.length > 0
          ? "dispatching"
          : candidate.state,
      );
      const teamStatus = teamStates
        ? new Set(teamStates).size === 1
          ? teamStates[0]
          : "partial"
        : null;
      return {
        version: 2,
        logicalMessageId: message.messageId,
        target: record.teamDeliveries
          ? `team:${message.teamCast.selectionId}`
          : delivery.target,
        ...(record.teamDeliveries
          ? { recipientCount: record.teamDeliveries.length }
          : {}),
        admissionState: delivery.admissionState,
        wakePolicy: delivery.wakePolicy,
        triggerKind:
          delivery.wakePolicy === "after-turn"
            ? "turn"
            : delivery.wakePolicy === "after-job"
              ? "job"
              : null,
        triggerId:
          delivery.wakePolicy === "after-turn"
            ? message.route?.trigger_turn_id || null
            : delivery.wakePolicy === "after-job"
              ? message.route?.trigger_job_id || null
              : null,
        ...(delivery.targetThreadId ? { targetThreadId: delivery.targetThreadId } : {}),
        attemptStartedAt: activeAttempt?.startedAt || null,
        claimLeaseUntil: delivery.claim?.leaseUntil || null,
        errorCode: delivery.errorCode || null,
        latestEvidenceKind: delivery.evidence.at(-1)?.evidenceKind || null,
        updatedAt: record.teamDeliveries
          ? record.teamDeliveries.reduce(
              (latest, candidate) =>
                candidate.updatedAt > latest ? candidate.updatedAt : latest,
              message.createdAt,
            )
          : delivery.updatedAt,
        status:
          teamStatus ||
          (delivery.admissionState === "quarantined"
            ? "quarantined"
            : (delivery.state === "created" && activeAttempt) ||
                (delivery.state === "retryable" && delivery.attempts.length === 2)
              ? "dispatching"
              : delivery.state),
      };
    });
  } catch {
    checks.push(
      diagnosticCheck({
        id: `${scope}.records.rebuild`,
        scope,
        status: "fail",
        summary: "Delivery Ledger evidence cannot be deterministically rebuilt",
        verification: "records",
        errorCode: "ELEDGERREBUILD",
      }),
    );
  }
  const indexRoot = path.join(root, path.basename(DELIVERY_LEDGER_INDEX_DIR));
  const checkpointPath = path.join(
    indexRoot,
    path.basename(DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH),
  );
  const indexMetadata = secureMetadata(indexRoot, "directory");
  if (indexMetadata.status === "missing") {
    checks.push(
      diagnosticCheck({
        id: `${scope}.index.directory`,
        scope,
        status: records.length > 0 ? "warn" : "pass",
        summary:
          records.length > 0
            ? "Delivery Ledger has no rebuildable index"
            : "Delivery Ledger index is not needed for an empty Ledger",
        verification: "metadata",
        errorCode: records.length > 0 ? "ELEDGERINDEXMISSING" : null,
        remediation: records.length > 0 ? "Run cxmsg deliveries rebuild-index" : null,
        required: false,
      }),
    );
  } else if (indexMetadata.status !== "secure") {
    checks.push(
      metadataFinding(
        `${scope}.index.directory`,
        scope,
        "Delivery Ledger index directory",
        indexMetadata,
      ),
    );
  } else {
    const checkpointMetadata = secureMetadata(checkpointPath, "file");
    if (checkpointMetadata.status !== "secure") {
      checks.push(
        metadataFinding(
          `${scope}.index.checkpoint`,
          scope,
          "Delivery Ledger index checkpoint",
          checkpointMetadata,
        ),
      );
    } else {
      let checkpoint = null;
      try {
        checkpoint = JSON.parse(readFileSync(checkpointPath, "utf8"));
      } catch {}
      const checkpointValid = Boolean(
        checkpoint?.version === 1 &&
          Array.isArray(checkpoint.manifest) &&
          /^[0-9a-f]{64}$/.test(checkpoint.manifestSha256 || "") &&
          checkpoint.manifestSha256 ===
            createHash("sha256")
              .update(JSON.stringify(checkpoint.manifest))
              .digest("hex") &&
          Number.isSafeInteger(checkpoint.messageCount) &&
          checkpoint.messageCount >= 0,
      );
      const currentManifest = ["segments", "quarantine"].flatMap((directory) => {
        const absolute = path.join(root, directory);
        try {
          return readdirSync(absolute)
            .filter((name) => /^segment-\d{8}(?:\.partial-[0-9a-f-]+)?\.jsonl$/i.test(name))
            .sort()
            .map((name) => {
              const metadata = lstatSync(path.join(absolute, name));
              return {
                directory,
                name,
                size: metadata.size,
                mtimeMs: metadata.mtimeMs,
              };
            });
        } catch {
          return [];
        }
      });
      const manifestMatches =
        checkpointValid &&
        JSON.stringify(checkpoint.manifest) === JSON.stringify(currentManifest);
      const shardNames = readdirSync(indexRoot)
        .filter((name) => /^[0-9a-f-]{36}\.json$/i.test(name))
        .sort();
      const indexedIds = [];
      const indexedProjectionDigests = new Map();
      let shardsValid = checkpointValid;
      for (const name of shardNames) {
        const messageId = name.slice(0, -5);
        const evidence = secureMetadata(path.join(indexRoot, name), "file");
        if (evidence.status !== "secure" || evidence.metadata.size > DELIVERY_LEDGER_MAX_RECORD_BYTES) {
          shardsValid = false;
          continue;
        }
        try {
          const wrapper = JSON.parse(readFileSync(path.join(indexRoot, name), "utf8"));
          if (!validDeliveryLedgerIndexRecord(wrapper, messageId)) {
            shardsValid = false;
            continue;
          }
          indexedIds.push(messageId);
          indexedProjectionDigests.set(messageId, wrapper.projectionSha256);
        } catch {
          shardsValid = false;
        }
      }
      const ledgerIds = records.map((record) => record.logicalMessageId).sort();
      const identityMatches =
        checkpointValid &&
        manifestMatches &&
        shardsValid &&
        checkpoint.messageCount === indexedIds.length &&
        JSON.stringify(indexedIds.sort()) === JSON.stringify(ledgerIds) &&
        ledgerIds.every(
          (messageId) =>
            indexedProjectionDigests.get(messageId) ===
            ledgerProjectionDigests.get(messageId),
        );
      checks.push(
        diagnosticCheck({
          id: `${scope}.index.consistency`,
          scope,
          status: identityMatches ? "pass" : "warn",
          summary: identityMatches
            ? "Delivery Ledger rebuildable index matches retained message identities"
            : "Delivery Ledger rebuildable index is missing, stale, or invalid",
          verification: "records",
          errorCode: identityMatches ? null : "ELEDGERINDEXSTALE",
          remediation: identityMatches ? null : "Run cxmsg deliveries rebuild-index",
          required: false,
        }),
      );
    }
  }
  if (!checks.some((check) => check.status === "fail" || check.status === "unknown")) {
    checks.push(
      diagnosticCheck({
        id: `${scope}.records.schema`,
        scope,
        status: "pass",
        summary: `${records.length} Delivery Ledger message(s) passed bounded reconstruction`,
        verification: "schema",
      }),
    );
  }
  return { checks, records, projections };
}

function validSession(record, stem) {
  const valid = Boolean(
    record &&
      record.name === stem &&
      SESSION_PATTERN.test(record.name) &&
      UUID_PATTERN.test(record.threadId || "") &&
      typeof record.cwd === "string" &&
      path.isAbsolute(record.cwd),
  );
  if (!valid) return { valid: false, errorCode: "ESESSIONSCHEMA" };
  if (record.allowedDelegators && !Array.isArray(record.allowedDelegators)) {
    return { valid: false, errorCode: "EGRANTSCHEMA" };
  }
  if (
    Array.isArray(record.allowedClaudeRequesters) &&
    record.allowedClaudeRequesters.some(
      (grant) =>
        !grant ||
        !UUID_PATTERN.test(grant.sessionId || "") ||
        !UUID_PATTERN.test(grant.token || "") ||
        typeof grant.permissions !== "string" ||
        !["never", "relay", "auto"].includes(grant.approval || "never"),
    )
  ) {
    return { valid: false, errorCode: "ECLAUDEGRANTSCHEMA" };
  }
  return { valid: true };
}

function validAttachment(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.name === stem &&
        UUID_PATTERN.test(record.threadId || "") &&
        Number.isSafeInteger(record.childPid) &&
        record.childPid > 1 &&
        typeof record.cwd === "string" &&
        path.isAbsolute(record.cwd),
    ),
    errorCode: "EATTACHMENTSCHEMA",
  };
}

function validJob(record, stem) {
  const schedule = record?.schedule;
  const claim = schedule?.claim;
  const failureEvidence = record?.failureEvidence;
  const kind = record?.kind ?? "delegation";
  const taskBody = record?.taskBody;
  const resultObservation = record?.resultObservation;
  const validDelegationTask =
    kind !== "delegation" ||
    (taskBody === undefined || taskBody === null
      ? typeof record.task === "string" &&
        Buffer.byteLength(record.task, "utf8") <= 16 * 1024
      : record.task === null &&
        taskBody.messageId === record.jobId &&
        taskBody.contentRef === `cxmsg-message:${record.jobId}` &&
        Number.isSafeInteger(taskBody.bodyBytes) &&
        taskBody.bodyBytes > 16 * 1024 &&
        taskBody.bodyBytes <= 256 * 1024 &&
        /^[0-9a-f]{64}$/.test(taskBody.bodySha256 || "") &&
        Object.keys(taskBody).every((field) =>
          ["messageId", "contentRef", "bodyBytes", "bodySha256"].includes(field),
        ));
  const validDelegationEvidence =
    kind !== "delegation" ||
    ((record.turnStartAttemptedAt === undefined ||
      record.turnStartAttemptedAt === null ||
      Number.isFinite(Date.parse(record.turnStartAttemptedAt))) &&
      (record.modelTurnStarted === undefined ||
        record.modelTurnStarted === null ||
        typeof record.modelTurnStarted === "boolean") &&
      (record.failureStage === undefined ||
        record.failureStage === null ||
        /^[a-z][a-z-]{0,63}$/.test(record.failureStage)) &&
      (record.rerouteGuidance === undefined ||
        record.rerouteGuidance === null ||
        (typeof record.rerouteGuidance === "string" &&
          Buffer.byteLength(record.rerouteGuidance, "utf8") <= 512)));
  const validFailureEvidence =
    failureEvidence === undefined ||
    failureEvidence === null ||
    (typeof failureEvidence === "object" &&
      /^[A-Z0-9_]{1,32}$/.test(failureEvidence.errorCode || "") &&
      Object.keys(failureEvidence).every((field) =>
        ["errorCode", "observedBytes", "limitBytes"].includes(field),
      ) &&
      (failureEvidence.observedBytes === undefined
        ? failureEvidence.limitBytes === undefined
        : Number.isSafeInteger(failureEvidence.observedBytes) &&
          failureEvidence.observedBytes >= 0 &&
          Number.isSafeInteger(failureEvidence.limitBytes) &&
          failureEvidence.limitBytes >= 0));
  const validResultObservation =
    resultObservation === undefined ||
    resultObservation === null ||
    (typeof resultObservation === "object" &&
      ["available", "missing", "failed"].includes(resultObservation.status) &&
      resultObservation.source === "thread-items" &&
      Number.isFinite(Date.parse(resultObservation.observedAt || "")) &&
      Object.keys(resultObservation).every((field) =>
        [
          "status",
          "source",
          "observedAt",
          "errorCode",
          "observedBytes",
          "limitBytes",
        ].includes(field),
      ) &&
      (resultObservation.status === "available"
        ? resultObservation.errorCode === undefined &&
          resultObservation.observedBytes === undefined &&
          resultObservation.limitBytes === undefined
        : /^[A-Z0-9_]{1,32}$/.test(resultObservation.errorCode || "") &&
          (resultObservation.observedBytes === undefined
            ? resultObservation.limitBytes === undefined
            : Number.isSafeInteger(resultObservation.observedBytes) &&
              resultObservation.observedBytes >= 0 &&
              Number.isSafeInteger(resultObservation.limitBytes) &&
              resultObservation.limitBytes >= 0)));
  const validSchedule =
    schedule === undefined ||
    schedule === null ||
    (schedule.version === 1 &&
      schedule.wakePolicy === "when-idle" &&
      Number.isFinite(Date.parse(schedule.expiresAt || "")) &&
      /^codex:[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(
        schedule.targetNodeKey || "",
      ) &&
      UUID_PATTERN.test(schedule.targetProjectId || "") &&
      /^[0-9a-f]{64}$/.test(schedule.enqueueFingerprint || "") &&
      Number.isSafeInteger(schedule.attemptCount) &&
      schedule.attemptCount >= 0 &&
      (claim === null ||
        (UUID_PATTERN.test(claim?.claimId || "") &&
          UUID_PATTERN.test(claim?.workerId || "") &&
          Number.isFinite(Date.parse(claim?.claimedAt || "")) &&
          Number.isFinite(Date.parse(claim?.leaseUntil || "")))));
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.jobId === stem &&
        UUID_PATTERN.test(record.jobId || "") &&
        (record.kind === undefined || typeof record.kind === "string") &&
        (record.executionThreadId === undefined ||
          record.executionThreadId === null ||
          UUID_PATTERN.test(record.executionThreadId)) &&
        typeof record.status === "string" &&
        validDelegationTask &&
        validDelegationEvidence &&
        validFailureEvidence &&
        validResultObservation &&
        validSchedule &&
        (record.status !== "scheduled" ||
          (record.kind === "delegation" && schedule)),
    ),
    errorCode: "EJOBSCHEMA",
  };
}

function validBridge(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.target === stem &&
        UUID_PATTERN.test(record.targetThreadId || "") &&
        Number.isSafeInteger(record.pid) &&
        record.pid > 1 &&
        typeof record.socketPath === "string" &&
        Number.isSafeInteger(record.startedAt) &&
        (record.cxmsgVersion === undefined ||
          typeof record.cxmsgVersion === "string") &&
        (record.implementationRevision === undefined ||
          (Number.isSafeInteger(record.implementationRevision) &&
            record.implementationRevision > 0)),
    ),
    errorCode: "EBRIDGESCHEMA",
  };
}

function validRouteBinding(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.sessionName === stem &&
        SESSION_PATTERN.test(record.sessionName || "") &&
        UUID_PATTERN.test(record.threadId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.projectId || "") &&
        (record.projectKey === undefined || UUID_PATTERN.test(record.projectKey)) &&
        (record.nodeKey === undefined ||
          record.nodeKey === `codex:${record.threadId}`) &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.role || ""),
    ),
    errorCode: "EROUTEBINDINGSCHEMA",
  };
}

function validInboundPolicy(record, stem) {
  return {
    valid: validInboundPolicyRecord(record, stem),
    errorCode: "EINBOUNDPOLICYSCHEMA",
  };
}

function validRouteDelivery(record, stem) {
  const expectedFingerprint = createHash("sha256")
    .update(JSON.stringify(record?.route ?? null))
    .digest("hex");
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.logicalMessageId === stem &&
        UUID_PATTERN.test(record.logicalMessageId || "") &&
        SESSION_PATTERN.test(record.from || "") &&
        SESSION_PATTERN.test(record.target || "") &&
        (record.targetThreadId === undefined ||
          UUID_PATTERN.test(record.targetThreadId || "")) &&
        /^[0-9a-f]{64}$/.test(record.messageSha256 || "") &&
        record.routeFingerprint === expectedFingerprint &&
        ["admitted", "quarantined"].includes(record.admissionState) &&
        ["dispatching", "turn_started", "unknown", "quarantined"].includes(
          record.status,
        ),
    ),
    errorCode: "EROUTEDELIVERYSCHEMA",
  };
}

function validQuarantine(record, stem) {
  const messageBytes = Buffer.byteLength(record?.message || "", "utf8");
  const expectedDigest = createHash("sha256")
    .update(record?.message || "")
    .digest("hex");
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.logicalMessageId === stem &&
        UUID_PATTERN.test(record.logicalMessageId || "") &&
        SESSION_PATTERN.test(record.from || "") &&
        SESSION_PATTERN.test(record.target || "") &&
        typeof record.reason === "string" &&
        typeof record.message === "string" &&
        messageBytes <= MAX_STORED_MESSAGE_BYTES &&
        record.messageBytes === messageBytes &&
        record.messageSha256 === expectedDigest,
    ),
    errorCode: "EQUARANTINESCHEMA",
  };
}

function validDirectoryProject(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.projectId === stem &&
        UUID_PATTERN.test(record.projectId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.routingId || "") &&
        ["git-common-dir", "canonical-root"].includes(record.discovery?.kind) &&
        typeof record.discovery?.key === "string" &&
        path.isAbsolute(record.discovery.key) &&
        Array.isArray(record.rootAliases) &&
        record.rootAliases.every(
          (alias) => typeof alias?.path === "string" && path.isAbsolute(alias.path),
        ),
    ),
    errorCode: "EPROJECTSCHEMA",
  };
}

function validDirectoryProjectTransition(record, stem) {
  const match = /^([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})--([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i.exec(
    stem,
  );
  const allowedFields = new Set([
    "version",
    "transitionId",
    "kind",
    "projectId",
    "fromDiscovery",
    "toDiscovery",
    "toRoot",
    "createdAt",
  ]);
  const validDiscovery = (discovery) =>
    ["git-common-dir", "canonical-root"].includes(discovery?.kind) &&
    typeof discovery.key === "string" &&
    path.isAbsolute(discovery.key);
  return {
    valid: Boolean(
      match &&
        record?.version === 1 &&
        record.projectId === match[1].toLowerCase() &&
        record.transitionId === match[2].toLowerCase() &&
        record.kind === "move" &&
        validDiscovery(record.fromDiscovery) &&
        validDiscovery(record.toDiscovery) &&
        (record.fromDiscovery.kind !== record.toDiscovery.kind ||
          record.fromDiscovery.key !== record.toDiscovery.key) &&
        typeof record.toRoot === "string" &&
        path.isAbsolute(record.toRoot) &&
        Number.isFinite(Date.parse(record.createdAt || "")) &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "EPROJECTTRANSITIONSCHEMA",
  };
}

function validDirectoryNode(record, stem) {
  const expectedStem = `${record?.runtimeKind || ""}--${record?.nativeId || ""}`;
  const endpoints = Object.entries(record?.selectedEndpoints || {});
  return {
    valid: Boolean(
      record?.version === 1 &&
        expectedStem === stem &&
        ["codex", "claude"].includes(record.runtimeKind) &&
        UUID_PATTERN.test(record.nativeId || "") &&
        record.nodeKey === `${record.runtimeKind}:${record.nativeId}` &&
        UUID_PATTERN.test(record.projectId || "") &&
        Array.isArray(record.aliases) &&
        record.aliases.every(
          (alias) =>
            typeof alias?.value === "string" &&
            alias.value.length > 0 &&
            alias.value.length <= 128,
        ) &&
        endpoints.length <= ENDPOINT_TRANSPORT_LIMIT &&
        endpoints.every(
          ([transport, endpoint]) =>
            endpoint?.transport === transport &&
            endpoint.nodeKey === record.nodeKey &&
            Number.isSafeInteger(endpoint.generation) &&
            endpoint.generation >= 0 &&
            ENDPOINT_STATUSES.has(endpoint.status) &&
            Number.isFinite(Date.parse(endpoint.observedAt || "")) &&
            (endpoint.address === undefined ||
              (typeof endpoint.address === "string" &&
                Buffer.byteLength(endpoint.address, "utf8") <= 1_024 &&
                !/[\u0000-\u001f\u007f]/.test(endpoint.address))) &&
            (endpoint.sessionName === undefined ||
              (typeof endpoint.sessionName === "string" &&
                endpoint.sessionName.length <= 128)) &&
            Object.keys(endpoint).every((field) => ENDPOINT_FIELDS.has(field)),
        ) &&
        (record.endpointHistory === undefined ||
          (Array.isArray(record.endpointHistory) &&
            record.endpointHistory.length <= ENDPOINT_HISTORY_LIMIT &&
            record.endpointHistory.every(
              (observation) =>
                observation?.nodeKey === record.nodeKey &&
                /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(
                  observation.transport || "",
                ) &&
                /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(
                  observation.endpointId || "",
                ) &&
                Number.isSafeInteger(observation.generation) &&
                observation.generation >= 0 &&
                ENDPOINT_STATUSES.has(observation.status) &&
                ENDPOINT_DECISIONS.has(observation.decision) &&
                Number.isFinite(
                  Date.parse(observation.firstObservedAt || ""),
                ) &&
                Number.isFinite(Date.parse(observation.lastObservedAt || "")) &&
                Date.parse(observation.firstObservedAt) <=
                  Date.parse(observation.lastObservedAt) &&
                Number.isSafeInteger(observation.observationCount) &&
                observation.observationCount >= 1 &&
                (observation.address === undefined ||
                  (typeof observation.address === "string" &&
                    Buffer.byteLength(observation.address, "utf8") <= 1_024 &&
                    !/[\u0000-\u001f\u007f]/.test(observation.address))) &&
                (observation.sessionName === undefined ||
                  (typeof observation.sessionName === "string" &&
                    observation.sessionName.length <= 128)) &&
                Object.keys(observation).every((field) =>
                  ENDPOINT_OBSERVATION_FIELDS.has(field),
                ),
            ))),
    ),
    errorCode: "ENODESCHEMA",
  };
}

function validDirectoryNodeTombstone(record, stem) {
  const expectedStem = `${record?.runtimeKind || ""}--${record?.nativeId || ""}`;
  const allowedFields = new Set([
    "version",
    "nodeKey",
    "runtimeKind",
    "nativeId",
    "projectId",
    "lastSafeLabel",
    "removedAt",
    "reason",
  ]);
  return {
    valid: Boolean(
      record?.version === 1 &&
        expectedStem === stem &&
        ["codex", "claude"].includes(record.runtimeKind) &&
        UUID_PATTERN.test(record.nativeId || "") &&
        record.nodeKey === `${record.runtimeKind}:${record.nativeId}` &&
        UUID_PATTERN.test(record.projectId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(
          record.lastSafeLabel || "",
        ) &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.reason || "") &&
        Number.isFinite(Date.parse(record.removedAt || "")) &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "ENODETOMBSTONESCHEMA",
  };
}

function validDirectorySuccessor(record, stem) {
  const expectedStem = String(record?.successorNodeKey || "").replace(":", "--");
  const nodeKeyPattern = /^(?:codex|claude):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
  const allowedFields = new Set([
    "version",
    "predecessorNodeKey",
    "successorNodeKey",
    "projectId",
    "linkedAt",
  ]);
  return {
    valid: Boolean(
      record?.version === 1 &&
        expectedStem === stem &&
        nodeKeyPattern.test(record.predecessorNodeKey || "") &&
        nodeKeyPattern.test(record.successorNodeKey || "") &&
        record.predecessorNodeKey !== record.successorNodeKey &&
        UUID_PATTERN.test(record.projectId || "") &&
        Number.isFinite(Date.parse(record.linkedAt || "")) &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "ESUCCESSORSCHEMA",
  };
}

function validDirectoryExecutionThread(record, stem) {
  const allowedFields = new Set([
    "version",
    "kind",
    "threadId",
    "jobId",
    "sourceThreadId",
    "sourceNodeKey",
    "projectId",
    "creationMode",
    "classifiedAt",
  ]);
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.kind === "execution-thread" &&
        record.threadId === stem &&
        UUID_PATTERN.test(record.threadId || "") &&
        UUID_PATTERN.test(record.jobId || "") &&
        UUID_PATTERN.test(record.sourceThreadId || "") &&
        record.threadId !== record.sourceThreadId &&
        (record.sourceNodeKey === undefined ||
          record.sourceNodeKey === `codex:${record.sourceThreadId}`) &&
        (record.projectId === undefined || UUID_PATTERN.test(record.projectId)) &&
        Boolean(record.sourceNodeKey) === Boolean(record.projectId) &&
        ["fork", "explicit-fresh", "start-fallback", "legacy-observed"].includes(
          record.creationMode,
        ) &&
        Number.isFinite(Date.parse(record.classifiedAt || "")) &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "EEXECUTIONTHREADSCHEMA",
  };
}

function validClusterMembers(members) {
  const nodeKeyPattern = /^(?:codex|claude):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
  return Boolean(
    Array.isArray(members) &&
      members.length <= CLUSTER_MEMBER_LIMIT &&
      members.every((member) => nodeKeyPattern.test(member)) &&
      members.every((member, index) => index === 0 || members[index - 1] < member),
  );
}

function clusterMembershipTransitionMatches(current, next) {
  if (
    next?.membershipVersion !== current?.membershipVersion + 1 ||
    Date.parse(next?.createdAt || "") <
      Date.parse(current?.updatedAt || current?.createdAt || "")
  ) {
    return false;
  }
  const before = new Set(current.members);
  const after = new Set(next.members);
  const added = next.members.filter((member) => !before.has(member));
  const removed = current.members.filter((member) => !after.has(member));
  return next.changeKind === "member-added"
    ? added.length === 1 &&
        removed.length === 0 &&
        added[0] === next.changedNodeKey
    : next.changeKind === "member-removed"
      ? removed.length === 1 &&
        added.length === 0 &&
        removed[0] === next.changedNodeKey
      : false;
}

function validDirectoryCluster(record, stem) {
  const allowedFields = new Set([
    "version",
    "clusterId",
    "routingId",
    "membershipVersion",
    "members",
    "createdAt",
    "updatedAt",
  ]);
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.clusterId === stem &&
        UUID_PATTERN.test(record.clusterId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.routingId || "") &&
        Number.isSafeInteger(record.membershipVersion) &&
        record.membershipVersion >= 1 &&
        validClusterMembers(record.members) &&
        Number.isFinite(Date.parse(record.createdAt || "")) &&
        Number.isFinite(Date.parse(record.updatedAt || "")) &&
        Date.parse(record.createdAt) <= Date.parse(record.updatedAt) &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "ECLUSTERSCHEMA",
  };
}

function validDirectoryClusterMembership(record, stem) {
  const match = /^([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})--([0-9]{10})$/i.exec(
    stem,
  );
  const allowedFields = new Set([
    "version",
    "clusterId",
    "membershipVersion",
    "members",
    "changeKind",
    "changedNodeKey",
    "createdAt",
  ]);
  const changedNodePattern = /^(?:codex|claude):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
  const filenameVersion = match ? Number(match[2]) : null;
  const created = record?.changeKind === "created";
  return {
    valid: Boolean(
      match &&
        record?.version === 1 &&
        record.clusterId === match[1].toLowerCase() &&
        record.membershipVersion === filenameVersion &&
        Number.isSafeInteger(record.membershipVersion) &&
        record.membershipVersion >= 1 &&
        validClusterMembers(record.members) &&
        ["created", "member-added", "member-removed"].includes(
          record.changeKind,
        ) &&
        (created
          ? record.membershipVersion === 1 && record.changedNodeKey === undefined
          : changedNodePattern.test(record.changedNodeKey || "")) &&
        Number.isFinite(Date.parse(record.createdAt || "")) &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "ECLUSTERMEMBERSHIPSCHEMA",
  };
}

function validDirectoryClusterTombstone(record, stem) {
  const allowedFields = new Set([
    "version",
    "clusterId",
    "routingId",
    "lastMembershipVersion",
    "removedAt",
    "reason",
  ]);
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.clusterId === stem &&
        UUID_PATTERN.test(record.clusterId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.routingId || "") &&
        Number.isSafeInteger(record.lastMembershipVersion) &&
        record.lastMembershipVersion >= 1 &&
        Number.isFinite(Date.parse(record.removedAt || "")) &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.reason || "") &&
        Object.keys(record).every((field) => allowedFields.has(field)),
    ),
    errorCode: "ECLUSTERTOMBSTONESCHEMA",
  };
}

export function inspectNodeDirectory({ stateDir, sessions = [], jobs = [] } = {}) {
  const projects = scanJsonDirectory({
    stateDir,
    directoryName: "directory/projects",
    scope: "directory-projects",
    validate: validDirectoryProject,
  });
  const projectTransitions = scanJsonDirectory({
    stateDir,
    directoryName: "directory/project-transitions",
    scope: "directory-project-transitions",
    validate: validDirectoryProjectTransition,
  });
  const nodes = scanJsonDirectory({
    stateDir,
    directoryName: "directory/nodes",
    scope: "directory-nodes",
    validate: validDirectoryNode,
  });
  const tombstones = scanJsonDirectory({
    stateDir,
    directoryName: "directory/tombstones/nodes",
    scope: "directory-node-tombstones",
    validate: validDirectoryNodeTombstone,
  });
  const successors = scanJsonDirectory({
    stateDir,
    directoryName: "directory/successors",
    scope: "directory-successors",
    validate: validDirectorySuccessor,
  });
  const executionThreads = scanJsonDirectory({
    stateDir,
    directoryName: "directory/execution-threads",
    scope: "directory-execution-threads",
    validate: validDirectoryExecutionThread,
  });
  const clusters = scanJsonDirectory({
    stateDir,
    directoryName: "directory/clusters",
    scope: "directory-clusters",
    validate: validDirectoryCluster,
  });
  const clusterMemberships = scanJsonDirectory({
    stateDir,
    directoryName: "directory/cluster-memberships",
    scope: "directory-cluster-memberships",
    validate: validDirectoryClusterMembership,
  });
  const clusterTombstones = scanJsonDirectory({
    stateDir,
    directoryName: "directory/tombstones/clusters",
    scope: "directory-cluster-tombstones",
    validate: validDirectoryClusterTombstone,
  });
  const checks = [
    ...projects.checks,
    ...projectTransitions.checks,
    ...nodes.checks,
    ...tombstones.checks,
    ...successors.checks,
    ...executionThreads.checks,
    ...clusters.checks,
    ...clusterMemberships.checks,
    ...clusterTombstones.checks,
  ];
  const projectIds = new Set(projects.records.map((record) => record.projectId));
  const seenRoutingIds = new Set();
  const seenDiscoveries = new Set();
  for (const project of projects.records) {
    const discovery = `${project.discovery.kind}:${project.discovery.key}`;
    const duplicateRouting = seenRoutingIds.has(project.routingId);
    const duplicateDiscovery = seenDiscoveries.has(discovery);
    checks.push(
      diagnosticCheck({
        id: `directory-projects.identity.${project.projectId.slice(0, 8)}`,
        scope: "directory-projects",
        status: duplicateRouting || duplicateDiscovery ? "fail" : "pass",
        summary:
          duplicateRouting || duplicateDiscovery
            ? "Project has a duplicate routing or discovery identity"
            : `Project ${project.routingId} has unique routing and discovery identity`,
        verification: "records",
        errorCode:
          duplicateRouting || duplicateDiscovery ? "EPROJECTIDENTITY" : null,
      }),
    );
    seenRoutingIds.add(project.routingId);
    seenDiscoveries.add(discovery);
  }
  const projectsById = new Map(
    projects.records.map((record) => [record.projectId, record]),
  );
  const transitionsByProject = new Map();
  for (const transition of projectTransitions.records) {
    const records = transitionsByProject.get(transition.projectId) || [];
    records.push(transition);
    transitionsByProject.set(transition.projectId, records);
  }
  for (const [projectId, transitions] of transitionsByProject) {
    const project = projectsById.get(projectId);
    const outgoing = new Map();
    const incoming = new Map();
    let branched = false;
    const discoveryKey = (value) => `${value.kind}:${value.key}`;
    for (const transition of transitions) {
      const from = discoveryKey(transition.fromDiscovery);
      const to = discoveryKey(transition.toDiscovery);
      if ((outgoing.has(from) && outgoing.get(from) !== to) || incoming.has(to)) {
        branched = true;
      }
      outgoing.set(from, to);
      incoming.set(to, from);
    }
    const starts = [...outgoing.keys()].filter((key) => !incoming.has(key));
    let cursor = starts[0] || null;
    const visited = new Set();
    while (cursor && outgoing.has(cursor) && !visited.has(cursor)) {
      visited.add(cursor);
      cursor = outgoing.get(cursor);
    }
    const cyclic = Boolean(cursor && visited.has(cursor));
    const connected = starts.length === 1 && visited.size === outgoing.size;
    const current = project ? discoveryKey(project.discovery) : null;
    const complete = Boolean(project && !branched && !cyclic && connected && current === cursor);
    const recoverable = Boolean(
      project &&
        !branched &&
        !cyclic &&
        connected &&
        (outgoing.has(current) || incoming.has(current)),
    );
    checks.push(
      diagnosticCheck({
        id: `directory-project-transitions.chain.${projectId.slice(0, 8)}`,
        scope: "directory-project-transitions",
        status: complete ? "pass" : recoverable ? "warn" : "fail",
        summary: !project
          ? "Project transition history references a missing Project"
          : branched || !connected
            ? "Project transition history is branched or disconnected"
            : cyclic
              ? "Project transition history contains a cycle"
              : recoverable
                ? "Project move transition is durable but its Project head is incomplete"
                : complete
                  ? "Project move history forms one chain ending at the current identity"
                  : "Project head does not match its transition history",
        verification: "records",
        errorCode: complete
          ? null
          : recoverable
            ? "EPROJECTMOVEINCOMPLETE"
            : "EPROJECTTRANSITIONAMBIGUOUS",
        remediation: recoverable
          ? "Repeat the exact project move command; Doctor does not mutate Project identity"
          : complete
            ? null
            : "Inspect owner-private Project transitions; do not merge, split, or rewrite identities automatically",
        required: !recoverable,
      }),
    );
  }
  const sessionThreadIds = new Set(sessions.map((record) => record.threadId));
  for (const node of nodes.records) {
    const projectExists = projectIds.has(node.projectId);
    checks.push(
      diagnosticCheck({
        id: `directory-nodes.project.${safeLabel(node.nodeKey)}`,
        scope: "directory-nodes",
        status: projectExists ? "pass" : "fail",
        summary: projectExists
          ? "Node references an existing private Project identity"
          : "Node references a missing private Project identity",
        verification: "records",
        errorCode: projectExists ? null : "ENODEPROJECT",
      }),
    );
    if (node.runtimeKind === "codex" && !sessionThreadIds.has(node.nativeId)) {
      checks.push(
        diagnosticCheck({
          id: `directory-nodes.registration.${safeLabel(node.nodeKey)}`,
          scope: "directory-nodes",
          status: "warn",
          summary: "Codex Node no longer has a registered addressable session",
          verification: "registry",
          errorCode: "ENODEUNREGISTERED",
          remediation:
            "Explicitly remove or Tombstone the Node after confirming the runtime session is retired",
          required: false,
        }),
      );
    }
    const selected = new Map(Object.entries(node.selectedEndpoints));
    if (node.endpointHistory === undefined) {
      if (selected.size) {
        checks.push(
          diagnosticCheck({
            id: `directory-nodes.endpoint-history.${safeLabel(node.nodeKey)}`,
            scope: "directory-nodes",
            status: "warn",
            summary: "Node has selected Endpoint evidence from before history tracking",
            verification: "legacy-record",
            errorCode: "EENDPOINTHISTORYLEGACY",
            remediation:
              "Synchronize the Node once to import selected Endpoints as bounded baseline history",
            required: false,
          }),
        );
      }
      continue;
    }
    const latestSuccessful = new Map();
    let historyConsistent = true;
    let previousTime = -Infinity;
    for (const observation of node.endpointHistory) {
      const observedAt = Date.parse(observation.lastObservedAt);
      if (observedAt < previousTime) historyConsistent = false;
      previousTime = observedAt;
      const previous = latestSuccessful.get(observation.transport);
      if (SUCCESSFUL_ENDPOINT_DECISIONS.has(observation.decision)) {
        if (previous && observation.generation < previous.generation) {
          historyConsistent = false;
        }
        if (previous && observation.decision === "selected") {
          historyConsistent = false;
        }
        if (
          previous &&
          observation.decision === "replaced" &&
          observation.generation <= previous.generation
        ) {
          historyConsistent = false;
        }
        if (
          previous &&
          observation.decision === "refreshed" &&
          (observation.generation !== previous.generation ||
            observation.endpointId !== previous.endpointId)
        ) {
          historyConsistent = false;
        }
        latestSuccessful.set(observation.transport, observation);
      } else if (observation.decision === "older-rejected") {
        if (!previous || observation.generation >= previous.generation) {
          historyConsistent = false;
        }
      } else if (observation.decision === "conflict-rejected") {
        if (
          !previous ||
          observation.generation !== previous.generation ||
          observation.endpointId === previous.endpointId
        ) {
          historyConsistent = false;
        }
      }
    }
    for (const [transport, endpoint] of selected) {
      const latest = latestSuccessful.get(transport);
      if (
        !latest ||
        latest.endpointId !== endpoint.endpointId ||
        latest.generation !== endpoint.generation ||
        latest.status !== endpoint.status ||
        (latest.address || null) !== (endpoint.address || null) ||
        (latest.sessionName || null) !== (endpoint.sessionName || null)
      ) {
        historyConsistent = false;
      }
    }
    if (
      [...latestSuccessful.keys()].some((transport) => !selected.has(transport))
    ) {
      historyConsistent = false;
    }
    checks.push(
      diagnosticCheck({
        id: `directory-nodes.endpoint-history.${safeLabel(node.nodeKey)}`,
        scope: "directory-nodes",
        status: historyConsistent ? "pass" : "fail",
        summary: historyConsistent
          ? "Endpoint history is bounded and consistent with selected Endpoint evidence"
          : "Endpoint history conflicts with selection or rejection invariants",
        verification: "records",
        errorCode: historyConsistent ? null : "EENDPOINTHISTORY",
        remediation: historyConsistent
          ? null
          : "Inspect the owner-only Node record; Doctor will not rewrite Endpoint history",
      }),
    );
  }
  const liveNodes = new Map(nodes.records.map((record) => [record.nodeKey, record]));
  const retiredNodes = new Map(
    tombstones.records.map((record) => [record.nodeKey, record]),
  );
  for (const tombstone of tombstones.records) {
    const projectExists = projectIds.has(tombstone.projectId);
    const duplicateLifecycle = liveNodes.has(tombstone.nodeKey);
    checks.push(
      diagnosticCheck({
        id: `directory-node-tombstones.project.${safeLabel(tombstone.nodeKey)}`,
        scope: "directory-node-tombstones",
        status: projectExists ? "pass" : "fail",
        summary: projectExists
          ? "Node Tombstone references an existing private Project identity"
          : "Node Tombstone references a missing private Project identity",
        verification: "records",
        errorCode: projectExists ? null : "ENODEPROJECT",
      }),
      diagnosticCheck({
        id: `directory-node-tombstones.lifecycle.${safeLabel(tombstone.nodeKey)}`,
        scope: "directory-node-tombstones",
        status: duplicateLifecycle ? "fail" : "pass",
        summary: duplicateLifecycle
          ? "Node identity exists as both live and tombstoned"
          : "Node Tombstone has no conflicting live Node",
        verification: "records",
        errorCode: duplicateLifecycle ? "ENODELIFECYCLE" : null,
        remediation: duplicateLifecycle
          ? "Inspect the interrupted lifecycle transition; Doctor will not choose or delete either record"
          : null,
      }),
    );
  }
  const identities = new Map([...liveNodes, ...retiredNodes]);
  const liveClusters = new Map(
    clusters.records.map((record) => [record.clusterId, record]),
  );
  const retiredClusters = new Map(
    clusterTombstones.records.map((record) => [record.clusterId, record]),
  );
  const allClusterIds = new Set([
    ...liveClusters.keys(),
    ...retiredClusters.keys(),
  ]);
  const seenClusterRoutingIds = new Set();
  for (const cluster of [...clusters.records, ...clusterTombstones.records]) {
    const lifecycleConflict =
      liveClusters.has(cluster.clusterId) && retiredClusters.has(cluster.clusterId);
    const duplicateRouting = seenClusterRoutingIds.has(cluster.routingId);
    const crossNamespace =
      allClusterIds.has(cluster.routingId) && cluster.routingId !== cluster.clusterId;
    seenClusterRoutingIds.add(cluster.routingId);
    checks.push(
      diagnosticCheck({
        id: `directory-clusters.identity.${safeLabel(cluster.clusterId)}`,
        scope: liveClusters.has(cluster.clusterId)
          ? "directory-clusters"
          : "directory-cluster-tombstones",
        status:
          lifecycleConflict || duplicateRouting || crossNamespace ? "fail" : "pass",
        summary: lifecycleConflict
          ? "Cluster identity exists as both live and tombstoned"
          : duplicateRouting
            ? "Cluster routing identity is duplicated"
            : crossNamespace
              ? "Cluster routing identity collides with another stable Cluster identity"
            : "Cluster has unique stable and routing identity",
        verification: "records",
        errorCode: lifecycleConflict
          ? "ECLUSTERLIFECYCLE"
          : duplicateRouting
            ? "ECLUSTERIDENTITY"
            : crossNamespace
              ? "ECLUSTERIDENTITY"
            : null,
        remediation:
          lifecycleConflict || duplicateRouting || crossNamespace
            ? "Inspect the owner-only Cluster records; Doctor will not choose, merge, or delete them"
            : null,
      }),
    );
  }

  const membershipsByCluster = new Map();
  for (const snapshot of clusterMemberships.records) {
    const history = membershipsByCluster.get(snapshot.clusterId) || [];
    history.push(snapshot);
    membershipsByCluster.set(snapshot.clusterId, history);
  }
  const allClusters = new Map([...liveClusters, ...retiredClusters]);
  for (const [clusterId, history] of membershipsByCluster) {
    if (!allClusters.has(clusterId)) {
      checks.push(
        diagnosticCheck({
          id: `directory-cluster-memberships.owner.${safeLabel(clusterId)}`,
          scope: "directory-cluster-memberships",
          status: "fail",
          summary: "Cluster membership history references no live or Tombstoned Cluster",
          verification: "records",
          errorCode: "ECLUSTERMEMBERSHIPORPHAN",
        }),
      );
    }
    history.sort((left, right) => left.membershipVersion - right.membershipVersion);
  }
  for (const [clusterId, cluster] of allClusters) {
    const history = membershipsByCluster.get(clusterId) || [];
    const lastVersion = liveClusters.has(clusterId)
      ? cluster.membershipVersion
      : cluster.lastMembershipVersion;
    let consistent = history.length === lastVersion;
    let previous = null;
    let previousTime = -Infinity;
    for (let index = 0; index < history.length; index += 1) {
      const snapshot = history[index];
      if (snapshot.membershipVersion !== index + 1) consistent = false;
      const snapshotTime = Date.parse(snapshot.createdAt);
      if (snapshotTime < previousTime) consistent = false;
      previousTime = snapshotTime;
      if (!previous) {
        if (
          snapshot.changeKind !== "created" ||
          snapshot.members.length !== 0
        ) {
          consistent = false;
        }
      } else {
        const before = new Set(previous.members);
        const after = new Set(snapshot.members);
        const added = snapshot.members.filter((member) => !before.has(member));
        const removed = previous.members.filter((member) => !after.has(member));
        if (
          snapshot.changeKind === "member-added"
            ? added.length !== 1 ||
              removed.length !== 0 ||
              added[0] !== snapshot.changedNodeKey
            : snapshot.changeKind === "member-removed"
              ? removed.length !== 1 ||
                added.length !== 0 ||
                removed[0] !== snapshot.changedNodeKey
              : true
        ) {
          consistent = false;
        }
      }
      if (snapshot.members.some((member) => !identities.has(member))) {
        consistent = false;
      }
      previous = snapshot;
    }
    if (
      liveClusters.has(clusterId) &&
      (!previous ||
        previous.membershipVersion !== cluster.membershipVersion ||
        JSON.stringify(previous.members) !== JSON.stringify(cluster.members) ||
        history[0]?.createdAt !== cluster.createdAt ||
        previous.createdAt !== cluster.updatedAt)
    ) {
      consistent = false;
    }
    if (
      retiredClusters.has(clusterId) &&
      previous &&
      Date.parse(previous.createdAt) > Date.parse(cluster.removedAt)
    ) {
      consistent = false;
    }
    const liveCluster = liveClusters.get(clusterId);
    const headSnapshot = liveCluster
      ? history.find(
          (snapshot) =>
            snapshot.membershipVersion === liveCluster.membershipVersion,
        )
      : null;
    const redoSnapshot = liveCluster
      ? history.find(
          (snapshot) =>
            snapshot.membershipVersion === liveCluster.membershipVersion + 1,
        )
      : null;
    const recoverableRedo = Boolean(
      liveCluster &&
        history.length === liveCluster.membershipVersion + 1 &&
        history.every(
          (snapshot, index) => snapshot.membershipVersion === index + 1,
        ) &&
        headSnapshot &&
        JSON.stringify(headSnapshot.members) ===
          JSON.stringify(liveCluster.members) &&
        headSnapshot.createdAt === liveCluster.updatedAt &&
        redoSnapshot &&
        clusterMembershipTransitionMatches(liveCluster, redoSnapshot) &&
        redoSnapshot.members.every((member) => identities.has(member)) &&
        !existsSync(
          path.join(
            stateDir,
            "directory",
            "cluster-memberships",
            `${clusterId}--${String(liveCluster.membershipVersion + 2).padStart(10, "0")}.json`,
          ),
        ),
    );
    checks.push(
      diagnosticCheck({
        id: `directory-cluster-memberships.history.${safeLabel(clusterId)}`,
        scope: "directory-cluster-memberships",
        status: consistent ? "pass" : recoverableRedo ? "warn" : "fail",
        summary: consistent
          ? "Cluster membership history is complete, ordered, and identity-resolved"
          : recoverableRedo
            ? "Cluster has one deterministic membership snapshot awaiting head redo"
          : "Cluster membership history conflicts with version, transition, or Node reference invariants",
        verification: "records",
        errorCode: consistent
          ? null
          : recoverableRedo
            ? "ECLUSTERMEMBERSHIPREDO"
            : "ECLUSTERMEMBERSHIP",
        remediation: consistent
          ? null
          : recoverableRedo
            ? "Run cxmsg directory cluster recover for this Cluster; Doctor remains read-only"
            : "Inspect the owner-only immutable snapshots; Doctor will not synthesize or rewrite membership history",
        required: !recoverableRedo,
      }),
    );
    if (history.length >= CLUSTER_MEMBERSHIP_HISTORY_WARN_LIMIT) {
      checks.push(
        diagnosticCheck({
          id: `directory-cluster-memberships.retention.${safeLabel(clusterId)}`,
          scope: "directory-cluster-memberships",
          status: "warn",
          summary: "Cluster membership history reached the operator review threshold",
          verification: "bounded",
          errorCode: "ECLUSTERMEMBERSHIPRETENTION",
          remediation:
            "Review retention before further high-frequency membership churn; cxmsg does not purge immutable history automatically",
          required: false,
        }),
      );
    }
  }
  const predecessorBySuccessor = new Map();
  for (const relation of successors.records) {
    const predecessor = identities.get(relation.predecessorNodeKey);
    const successor = identities.get(relation.successorNodeKey);
    const referencesExist = Boolean(predecessor && successor);
    const sameProject = Boolean(
      referencesExist &&
        predecessor.projectId === relation.projectId &&
        successor.projectId === relation.projectId,
    );
    const duplicate = predecessorBySuccessor.has(relation.successorNodeKey);
    predecessorBySuccessor.set(
      relation.successorNodeKey,
      relation.predecessorNodeKey,
    );
    checks.push(
      diagnosticCheck({
        id: `directory-successors.reference.${safeLabel(relation.successorNodeKey)}`,
        scope: "directory-successors",
        status: referencesExist && sameProject && !duplicate ? "pass" : "fail",
        summary: !referencesExist
          ? "Successor relation references a missing Node identity"
          : !sameProject
            ? "Successor relation crosses or misstates its Project identity"
            : duplicate
              ? "Successor Node has multiple predecessor relations"
              : "Successor relation references same-Project Node identities",
        verification: "records",
        errorCode: !referencesExist
          ? "ESUCCESSORREFERENCE"
          : !sameProject
            ? "ESUCCESSORPROJECT"
            : duplicate
              ? "ESUCCESSORDUPLICATE"
              : null,
      }),
    );
  }
  const cycleNodes = new Set();
  for (const start of predecessorBySuccessor.keys()) {
    const pathNodes = new Set();
    let cursor = start;
    while (cursor && predecessorBySuccessor.has(cursor)) {
      if (pathNodes.has(cursor)) {
        cycleNodes.add(start);
        break;
      }
      pathNodes.add(cursor);
      cursor = predecessorBySuccessor.get(cursor);
    }
  }
  checks.push(
    diagnosticCheck({
      id: "directory-successors.graph.acyclic",
      scope: "directory-successors",
      status: cycleNodes.size ? "fail" : "pass",
      summary: cycleNodes.size
        ? "Successor relations contain a cycle"
        : "Successor relations form an acyclic predecessor graph",
      verification: "records",
      errorCode: cycleNodes.size ? "ESUCCESSORCYCLE" : null,
      remediation: cycleNodes.size
        ? "Inspect successor records and remove only the explicitly invalid relation outside Doctor"
        : null,
    }),
  );
  const jobsById = new Map(jobs.map((record) => [record.jobId, record]));
  const executionJobs = new Set();
  for (const execution of executionThreads.records) {
    const job = jobsById.get(execution.jobId);
    const collidesWithNode = identities.has(`codex:${execution.threadId}`);
    const addressable = sessionThreadIds.has(execution.threadId);
    const duplicateJob = executionJobs.has(execution.jobId);
    executionJobs.add(execution.jobId);
    checks.push(
      diagnosticCheck({
        id: `directory-execution-threads.identity.${safeLabel(execution.threadId)}`,
        scope: "directory-execution-threads",
        status: collidesWithNode || addressable || duplicateJob ? "fail" : "pass",
        summary: collidesWithNode
          ? "Execution Thread identity collides with a live or Tombstoned Node"
          : addressable
            ? "Execution Thread is incorrectly registered as an addressable session"
            : duplicateJob
              ? "Job references multiple Execution Thread records"
              : "Execution Thread remains non-addressable and has unique Job provenance",
        verification: "records",
        errorCode: collidesWithNode
          ? "EEXECUTIONNODECOLLISION"
          : addressable
            ? "EEXECUTIONADDRESSABLE"
            : duplicateJob
              ? "EEXECUTIONJOBDUPLICATE"
              : null,
      }),
    );
    const jobMatches = Boolean(
      job &&
        (job.kind ?? "delegation") === "delegation" &&
        ["fork", "fresh"].includes(job.execution) &&
        (execution.creationMode === "explicit-fresh") ===
          (job.execution === "fresh") &&
        job.targetThreadId === execution.sourceThreadId &&
        job.threadId === execution.threadId &&
        (job.executionThreadId === undefined ||
          job.executionThreadId === null ||
          job.executionThreadId === execution.threadId),
    );
    checks.push(
      diagnosticCheck({
        id: `directory-execution-threads.job.${safeLabel(execution.threadId)}`,
        scope: "directory-execution-threads",
        status: jobMatches ? "pass" : "fail",
        summary: jobMatches
          ? "Execution Thread matches its retained isolated Delegation evidence"
          : "Execution Thread does not match a retained isolated Delegation",
        verification: "jobs",
        errorCode: jobMatches ? null : "EEXECUTIONJOB",
      }),
    );
    if (execution.sourceNodeKey) {
      const source = identities.get(execution.sourceNodeKey);
      const sourceMatches = Boolean(
        source && source.projectId === execution.projectId,
      );
      checks.push(
        diagnosticCheck({
          id: `directory-execution-threads.source.${safeLabel(execution.threadId)}`,
          scope: "directory-execution-threads",
          status: sourceMatches ? "pass" : "fail",
          summary: sourceMatches
            ? "Execution Thread references a same-Project source Node identity"
            : "Execution Thread source Node or Project identity is missing or mismatched",
          verification: "records",
          errorCode: sourceMatches ? null : "EEXECUTIONSOURCE",
        }),
      );
    }
  }
  return checks;
}

function exactStringSet(left, right) {
  return (
    Array.isArray(left) &&
    Array.isArray(right) &&
    JSON.stringify([...left].sort()) === JSON.stringify([...right].sort())
  );
}

function identityRecordMap(nodes, tombstones) {
  return new Map(
    [...nodes.records, ...tombstones.records].map((record) => [
      record.nodeKey,
      record,
    ]),
  );
}

export function inspectConversationState({ stateDir, jobs = [] } = {}) {
  const direct = scanJsonDirectory({
    stateDir,
    directoryName: "conversations/direct",
    scope: "direct-conversations",
    maxRecordBytes: 4 * 1024 * 1024,
    validate: (record, stem) => ({
      valid:
        validDirectConversationRecord(record) &&
        record.conversationId === stem.toLowerCase(),
      errorCode: "EDIRECTCONVERSATIONSCHEMA",
    }),
  });
  const groups = scanJsonDirectory({
    stateDir,
    directoryName: "conversations/group",
    scope: "group-conversations",
    maxRecordBytes: 4 * 1024 * 1024,
    validate: (record, stem) => ({
      valid:
        validGroupConversationRecord(record) &&
        record.conversationId === stem.toLowerCase(),
      errorCode: "EGROUPCONVERSATIONSCHEMA",
    }),
  });
  const summaries = scanJsonDirectory({
    stateDir,
    directoryName: "conversations/summaries",
    scope: "conversation-summaries",
    maxRecordBytes: 32 * 1024,
    validate: (record, stem) => ({
      valid:
        validConversationSummary(record) &&
        `${record.kind}--${record.conversationId}` === stem.toLowerCase(),
      errorCode: "ECONVERSATIONSUMMARYSCHEMA",
    }),
  });
  const plans = scanJsonDirectory({
    stateDir,
    directoryName: "team-casts/plans",
    scope: "team-cast-plans",
    maxRecordBytes: 64 * 1024,
    validate: (record, stem) => ({
      valid:
        validTeamCastPlanRecord(record) && record.planId === stem.toLowerCase(),
      errorCode: "ETEAMCASTPLANSCHEMA",
    }),
  });
  const selections = scanJsonDirectory({
    stateDir,
    directoryName: "team-casts/selections",
    scope: "team-cast-selections",
    maxRecordBytes: 64 * 1024,
    validate: (record, stem) => ({
      valid:
        validTeamCastSelectionRecord(record) &&
        record.selectionId === stem.toLowerCase(),
      errorCode: "ETEAMCASTSELECTIONSCHEMA",
    }),
  });
  const projects = scanJsonDirectory({
    stateDir,
    directoryName: "directory/projects",
    scope: "coordination-projects",
    validate: validDirectoryProject,
  });
  const nodes = scanJsonDirectory({
    stateDir,
    directoryName: "directory/nodes",
    scope: "coordination-nodes",
    validate: validDirectoryNode,
  });
  const tombstones = scanJsonDirectory({
    stateDir,
    directoryName: "directory/tombstones/nodes",
    scope: "coordination-node-tombstones",
    validate: validDirectoryNodeTombstone,
  });
  const ledger = inspectDeliveryLedger(stateDir);
  const checks = [
    ...direct.checks,
    ...groups.checks,
    ...summaries.checks,
    ...plans.checks,
    ...selections.checks,
  ];
  const summaryDirectory = path.join(stateDir, "conversations", "summaries");
  try {
    const names = readdirSync(summaryDirectory);
    let staleArtifacts = 0;
    let unexpectedEntries = 0;
    for (const name of names) {
      if (/^(direct|group)--[0-9a-f-]{36}\.json$/i.test(name)) continue;
      if (/^(direct|group)--[0-9a-f-]{36}\.json\.[0-9a-f-]{36}\.tmp$/i.test(name)) {
        const metadata = lstatSync(path.join(summaryDirectory, name));
        if (Date.now() - metadata.mtimeMs >= 30_000) staleArtifacts += 1;
        continue;
      }
      unexpectedEntries += 1;
    }
    if (staleArtifacts || unexpectedEntries) {
      checks.push(
        diagnosticCheck({
          id: "conversation-summaries.artifacts",
          scope: "conversation-summaries",
          status: "warn",
          summary: "Conversation summary storage contains stale or unexpected artifacts",
          verification: "metadata",
          errorCode: staleArtifacts
            ? "ECONVERSATIONSUMMARYTEMPSTALE"
            : "ECONVERSATIONSUMMARYUNEXPECTED",
          required: false,
        }),
      );
    }
  } catch {}
  const identities = identityRecordMap(nodes, tombstones);
  const projectIds = new Set(projects.records.map((record) => record.projectId));
  const projections = Array.isArray(ledger.projections)
    ? ledger.projections
    : null;
  const ledgerByMessage = new Map(
    (projections || []).map((record) => [record.logicalMessage.messageId, record]),
  );
  const jobsById = new Map(jobs.map((record) => [record.jobId, record]));
  const summariesByConversation = new Map(
    summaries.records.map((record) => [
      `${record.kind}:${record.conversationId}`,
      record,
    ]),
  );

  function retainedConversationEvidence(conversation) {
    try {
      const filename = path.join(
        stateDir,
        "conversations",
        conversation.kind,
        `${conversation.conversationId}.json`,
      );
      const metadata = lstatSync(filename);
      if (
        !metadata.isFile() ||
        metadata.isSymbolicLink() ||
        metadata.uid !== process.getuid() ||
        metadata.nlink !== 1 ||
        (metadata.mode & 0o077) !== 0 ||
        metadata.size > 4 * 1024 * 1024
      ) {
        return null;
      }
      return createHash("sha256")
        .update(
          `${metadata.dev}:${metadata.ino}:${metadata.size}:${metadata.mtimeMs}`,
        )
        .digest("hex");
    } catch {
      return null;
    }
  }

  function expectedSummary(conversation) {
    const directConversation = conversation.kind === "direct";
    const selected = conversation.messages.reduce((current, message) => {
      if (!current) return message;
      const difference = Date.parse(message.recordedAt) - Date.parse(current.recordedAt);
      return difference > 0 ||
        (difference === 0 && message.sequence > current.sequence)
        ? message
        : current;
    }, null);
    return {
      version: 1,
      kind: conversation.kind,
      conversationId: conversation.conversationId,
      currentMembers: directConversation
        ? [...conversation.currentMembers]
        : [...conversation.membershipSnapshots.at(-1).members],
      label: directConversation ? null : conversation.label,
      lastActivityAt: selected
        ? new Date(selected.recordedAt).toISOString()
        : null,
      lastMessageId: selected?.logicalMessageId || null,
      lastSenderNodeKey: selected?.senderNodeKey || null,
      lastSourceKind: selected
        ? directConversation
          ? selected.sourceKind
          : "delivery-ledger"
        : null,
      lastSequence: selected?.sequence || 0,
      messageCount: conversation.messages.length,
      conversationUpdatedAt: conversation.updatedAt,
      recordEvidence: retainedConversationEvidence(conversation),
    };
  }

  const conversationsBySummaryKey = new Map(
    [...direct.records, ...groups.records].map((record) => [
      `${record.kind}:${record.conversationId}`,
      record,
    ]),
  );
  for (const [key, conversation] of conversationsBySummaryKey) {
    const expected = expectedSummary(conversation);
    const activityFields = [
      conversation.lastActivityAt,
      conversation.lastMessageId,
      conversation.lastSenderNodeKey,
    ];
    const activityCacheValid =
      activityFields.every((value) => value === undefined) ||
      (conversation.lastActivityAt === expected.lastActivityAt &&
        conversation.lastMessageId === expected.lastMessageId &&
        conversation.lastSenderNodeKey === expected.lastSenderNodeKey);
    checks.push(
      diagnosticCheck({
        id: `conversation-summaries.activity.${safeLabel(conversation.conversationId)}`,
        scope: "conversation-summaries",
        status: activityCacheValid ? "pass" : "warn",
        summary: activityCacheValid
          ? "Conversation activity cache matches retained message ordering"
          : "Conversation activity cache will be rederived on the next write",
        verification: "records",
        errorCode: activityCacheValid ? null : "ECONVERSATIONACTIVITYSTALE",
        required: false,
      }),
    );
    const summary = summariesByConversation.get(key);
    const valid = Boolean(
      summary && isDeepStrictEqual(summary, expected),
    );
    checks.push(
      diagnosticCheck({
        id: `conversation-summaries.record.${safeLabel(conversation.conversationId)}`,
        scope: "conversation-summaries",
        status: valid ? "pass" : "warn",
        summary: valid
          ? "Recent Conversation summary matches its retained Conversation"
          : "Recent Conversation summary is missing or stale",
        verification: "records",
        errorCode: valid ? null : "ECONVERSATIONSUMMARYSTALE",
        required: false,
      }),
    );
  }
  for (const summary of summaries.records) {
    const key = `${summary.kind}:${summary.conversationId}`;
    if (conversationsBySummaryKey.has(key)) continue;
    checks.push(
      diagnosticCheck({
        id: `conversation-summaries.orphan.${safeLabel(summary.conversationId)}`,
        scope: "conversation-summaries",
        status: "warn",
        summary: "Recent Conversation summary has no retained Conversation",
        verification: "records",
        errorCode: "ECONVERSATIONSUMMARYORPHAN",
        required: false,
      }),
    );
  }

  for (const conversation of direct.records) {
    const label = safeLabel(conversation.conversationId);
    const referencedNodes = new Set([
      ...conversation.members,
      ...conversation.currentMembers,
      ...conversation.migrations.flatMap((migration) => [
        migration.predecessorNodeKey,
        migration.successorNodeKey,
      ]),
    ]);
    const records = [...referencedNodes].map((nodeKey) => identities.get(nodeKey));
    const identityValid = records.every(Boolean);
    const referencedProjects = new Set(records.filter(Boolean).map((record) => record.projectId));
    const projectValid =
      identityValid &&
      referencedProjects.size === 1 &&
      projectIds.has([...referencedProjects][0]);
    checks.push(
      diagnosticCheck({
        id: `direct-conversations.identity.${label}`,
        scope: "direct-conversations",
        status: identityValid && projectValid ? "pass" : "fail",
        summary:
          identityValid && projectValid
            ? "Direct Conversation members resolve to one retained Project identity"
            : "Direct Conversation member or Project identity is missing or inconsistent",
        verification: "directory",
        errorCode:
          identityValid && projectValid ? null : "ECONVERSATIONIDENTITY",
      }),
    );
    let sourceValid = true;
    let sourceUnavailable = false;
    for (const message of conversation.messages) {
      if (message.sourceKind === "claude-job") {
        if (!jobsById.has(message.logicalMessageId)) sourceValid = false;
        continue;
      }
      if (!projections) {
        sourceUnavailable = true;
        continue;
      }
      const source = ledgerByMessage.get(message.logicalMessageId);
      if (
        !source ||
        source.logicalMessage.senderNodeKey !== message.senderNodeKey ||
        source.delivery?.targetNodeKey !== message.recipientNodeKey ||
        source.logicalMessage.conversationId !== message.conversationId ||
        source.logicalMessage.conversationSequence !== message.sequence
      ) {
        sourceValid = false;
      }
    }
    checks.push(
      diagnosticCheck({
        id: `direct-conversations.sources.${label}`,
        scope: "direct-conversations",
        status: sourceUnavailable ? "unknown" : sourceValid ? "pass" : "fail",
        summary: sourceUnavailable
          ? "Direct Conversation source evidence could not be rebuilt"
          : sourceValid
            ? "Direct Conversation messages match their retained Ledger or Job source"
            : "Direct Conversation message source is missing or mismatched",
        verification: sourceUnavailable ? "unavailable" : "records",
        errorCode: sourceUnavailable
          ? "ECONVERSATIONLEDGERUNAVAILABLE"
          : sourceValid
            ? null
            : "ECONVERSATIONSOURCE",
      }),
    );
  }

  for (const conversation of groups.records) {
    const label = safeLabel(conversation.conversationId);
    const members = new Set(
      conversation.membershipSnapshots.flatMap((snapshot) => snapshot.members),
    );
    const memberRecords = [...members].map((nodeKey) => identities.get(nodeKey));
    const identityValid = memberRecords.every(
      (record) => record?.projectId === conversation.projectId,
    );
    const projectValid = projectIds.has(conversation.projectId);
    checks.push(
      diagnosticCheck({
        id: `group-conversations.identity.${label}`,
        scope: "group-conversations",
        status: identityValid && projectValid ? "pass" : "fail",
        summary:
          identityValid && projectValid
            ? "Group Conversation snapshots resolve to their retained Project identity"
            : "Group Conversation member or Project identity is missing or inconsistent",
        verification: "directory",
        errorCode:
          identityValid && projectValid ? null : "EGROUPCONVERSATIONIDENTITY",
      }),
    );
    let fanoutValid = true;
    let sourceUnavailable = false;
    for (const message of conversation.messages) {
      if (!projections) {
        sourceUnavailable = true;
        continue;
      }
      const source = ledgerByMessage.get(message.logicalMessageId);
      const envelope = source?.logicalMessage?.group;
      const recipientNodeKeys = (source?.groupDeliveries || []).map(
        (delivery) => delivery.targetNodeKey,
      );
      if (
        !source ||
        source.logicalMessage.senderNodeKey !== message.senderNodeKey ||
        envelope?.conversationId !== conversation.conversationId ||
        envelope.sequence !== message.sequence ||
        envelope.membershipVersion !== message.membershipVersion ||
        !exactStringSet(envelope.recipientNodeKeys, message.recipientNodeKeys) ||
        !exactStringSet(recipientNodeKeys, message.recipientNodeKeys)
      ) {
        fanoutValid = false;
      }
    }
    checks.push(
      diagnosticCheck({
        id: `group-conversations.fanout.${label}`,
        scope: "group-conversations",
        status: sourceUnavailable ? "unknown" : fanoutValid ? "pass" : "fail",
        summary: sourceUnavailable
          ? "Group fan-out Ledger evidence could not be rebuilt"
          : fanoutValid
            ? "Group messages match their immutable membership and per-recipient Deliveries"
            : "Group message fan-out is missing or mismatched",
        verification: sourceUnavailable ? "unavailable" : "delivery-ledger",
        errorCode: sourceUnavailable
          ? "EGROUPLEDGERUNAVAILABLE"
          : fanoutValid
            ? null
            : "EGROUPFANOUT",
      }),
    );
  }

  const plansById = new Map(plans.records.map((record) => [record.planId, record]));
  const selectionsById = new Map(
    selections.records.map((record) => [record.selectionId, record]),
  );
  for (const plan of plans.records) {
    const participants = [plan.senderNodeKey, ...plan.recipientNodeKeys];
    const identityValid = participants.every(
      (nodeKey) => identities.get(nodeKey)?.projectId === plan.projectId,
    );
    checks.push(
      diagnosticCheck({
        id: `team-cast-plans.identity.${safeLabel(plan.planId)}`,
        scope: "team-cast-plans",
        status: identityValid && projectIds.has(plan.projectId) ? "pass" : "fail",
        summary:
          identityValid && projectIds.has(plan.projectId)
            ? "Team Cast plan participants resolve to one retained Project"
            : "Team Cast plan participant or Project identity is missing or inconsistent",
        verification: "directory",
        errorCode:
          identityValid && projectIds.has(plan.projectId)
            ? null
            : "ETEAMCASTIDENTITY",
      }),
    );
  }
  for (const selection of selections.records) {
    const plan = plansById.get(selection.planId);
    const selectionValid = Boolean(
      plan &&
        plan.senderNodeKey === selection.senderNodeKey &&
        plan.projectId === selection.projectId &&
        plan.expiresAt === selection.expiresAt &&
        selection.recipientNodeKeys.every((nodeKey) =>
          plan.recipientNodeKeys.includes(nodeKey),
        ) &&
        (selection.wakePolicy !== "wake-all" ||
          exactStringSet(selection.recipientNodeKeys, plan.recipientNodeKeys)),
    );
    checks.push(
      diagnosticCheck({
        id: `team-cast-selections.plan.${safeLabel(selection.selectionId)}`,
        scope: "team-cast-selections",
        status: selectionValid ? "pass" : "fail",
        summary: selectionValid
          ? "Team Cast selection is a bounded subset of its immutable plan"
          : "Team Cast selection has a missing or mismatched immutable plan",
        verification: "records",
        errorCode: selectionValid ? null : "ETEAMCASTSELECTIONPLAN",
      }),
    );
  }
  if (projections) {
    for (const record of projections.filter(
      (candidate) => candidate.logicalMessage.teamCast,
    )) {
      const envelope = record.logicalMessage.teamCast;
      const plan = plansById.get(envelope.planId);
      const selection = selectionsById.get(envelope.selectionId);
      const recipientNodeKeys = (record.teamDeliveries || []).map(
        (delivery) => delivery.targetNodeKey,
      );
      const consistent = Boolean(
        plan &&
          selection &&
          selection.planId === plan.planId &&
          record.logicalMessage.senderNodeKey === selection.senderNodeKey &&
          envelope.projectId === selection.projectId &&
          envelope.wakePolicy === selection.wakePolicy &&
          envelope.recipientSetSha256 === selection.recipientSetSha256 &&
          exactStringSet(envelope.recipientNodeKeys, selection.recipientNodeKeys) &&
          exactStringSet(recipientNodeKeys, selection.recipientNodeKeys),
      );
      checks.push(
        diagnosticCheck({
          id: `team-cast-deliveries.fanout.${safeLabel(record.logicalMessage.messageId)}`,
          scope: "team-cast-deliveries",
          status: consistent ? "pass" : "fail",
          summary: consistent
            ? "Team Cast Ledger fan-out matches its immutable plan and selection"
            : "Team Cast Ledger fan-out is missing or mismatched",
          verification: "delivery-ledger",
          errorCode: consistent ? null : "ETEAMCASTFANOUT",
        }),
      );
    }
  } else if (selections.records.length > 0) {
    checks.push(
      diagnosticCheck({
        id: "team-cast-deliveries.ledger",
        scope: "team-cast-deliveries",
        status: "unknown",
        summary: "Team Cast Ledger evidence could not be rebuilt",
        verification: "unavailable",
        errorCode: "ETEAMCASTLEDGERUNAVAILABLE",
      }),
    );
  }
  return checks;
}

export function inspectRepairState({ stateDir } = {}) {
  const scope = "repairs";
  const root = path.join(stateDir, "repairs");
  const transactionsDirectory = path.join(root, "transactions");
  const receiptsDirectory = path.join(root, "receipts");
  const checks = [];
  const rootEvidence = secureMetadata(root, "directory");
  if (rootEvidence.status === "missing") {
    return [
      metadataFinding(
        "repairs.directory",
        scope,
        "Repair state directory",
        rootEvidence,
        { required: false },
      ),
    ];
  }
  checks.push(
    metadataFinding(
      "repairs.directory",
      scope,
      "Repair state directory",
      rootEvidence,
    ),
  );
  if (rootEvidence.status !== "secure") return checks;
  const unexpectedRootEntries = readdirSync(root).filter(
    (name) => !["mutation.lock", "transactions", "receipts"].includes(name),
  );
  if (unexpectedRootEntries.length > 0) {
    checks.push(
      diagnosticCheck({
        id: "repairs.entries",
        scope,
        status: "fail",
        summary: "Repair state contains unexpected top-level entries",
        verification: "records",
        errorCode: "EREPAIRSTATE",
      }),
    );
  }
  const transactionsEvidence = secureMetadata(
    transactionsDirectory,
    "directory",
  );
  const receiptsEvidence = secureMetadata(receiptsDirectory, "directory");
  checks.push(
    metadataFinding(
      "repairs.transactions.directory",
      scope,
      "Repair transaction directory",
      transactionsEvidence,
    ),
    metadataFinding(
      "repairs.receipts.directory",
      scope,
      "Repair receipt directory",
      receiptsEvidence,
    ),
  );
  if (
    transactionsEvidence.status !== "secure" ||
    receiptsEvidence.status !== "secure"
  ) {
    return checks;
  }
  const allReceiptNames = readdirSync(receiptsDirectory).sort();
  const receiptNames = allReceiptNames
    .filter((name) => /^[0-9a-f-]{36}\.json$/i.test(name))
    .sort();
  if (receiptNames.length !== allReceiptNames.length) {
    checks.push(
      diagnosticCheck({
        id: "repairs.receipts.entries",
        scope,
        status: "fail",
        summary: "Repair receipt storage contains unexpected entries",
        verification: "records",
        errorCode: "EREPAIRSTATE",
      }),
    );
  }
  const receiptByTransaction = new Map();
  for (const name of receiptNames) {
    const transactionId = name.slice(0, -5).toLowerCase();
    const filename = path.join(receiptsDirectory, name);
    const evidence = secureMetadata(filename, "file");
    if (evidence.status !== "secure" || evidence.metadata.size > MAX_RECORD_BYTES) {
      checks.push(
        metadataFinding(
          `repairs.receipt.${safeLabel(transactionId)}.metadata`,
          scope,
          "Repair receipt",
          evidence.status === "secure" ? { status: "wrong-type" } : evidence,
        ),
      );
      continue;
    }
    try {
      const receipt = JSON.parse(readFileSync(filename, "utf8"));
      const valid = Boolean(
        receipt?.schemaVersion === 1 &&
          receipt.transactionId === transactionId &&
          receipt.findingId &&
          [
            "cluster-membership-redo",
            "delivery-ledger-index-rebuild",
            "inbound-policy-stale-artifact-purge",
            "legacy-job-kind-migration",
          ].includes(
            receipt.repairKind,
          ) &&
          /^[0-9a-f]{64}$/.test(receipt.planDigest || "") &&
          ["completed", "failed"].includes(receipt.status) &&
          Number.isFinite(Date.parse(receipt.startedAt || "")) &&
          Number.isFinite(Date.parse(receipt.completedAt || ""))
      );
      if (valid) receiptByTransaction.set(transactionId, receipt);
      else {
        checks.push(
          diagnosticCheck({
            id: `repairs.receipt.${safeLabel(transactionId)}.schema`,
            scope,
            status: "fail",
            summary: "Repair receipt failed its bounded audit schema",
            verification: "schema",
            errorCode: "EREPAIRRECEIPT",
          }),
        );
      }
    } catch {
      checks.push(
        diagnosticCheck({
          id: `repairs.receipt.${safeLabel(transactionId)}.json`,
          scope,
          status: "fail",
          summary: "Repair receipt is not valid bounded JSON",
          verification: "parse",
          errorCode: "EREPAIRRECEIPT",
        }),
      );
    }
  }
  const allTransactionNames = readdirSync(transactionsDirectory).sort();
  const transactionNames = allTransactionNames
    .filter((name) => /^[0-9a-f-]{36}$/i.test(name))
    .sort();
  if (transactionNames.length !== allTransactionNames.length) {
    checks.push(
      diagnosticCheck({
        id: "repairs.transactions.entries",
        scope,
        status: "fail",
        summary: "Repair transaction storage contains unexpected entries",
        verification: "records",
        errorCode: "EREPAIRSTATE",
      }),
    );
  }
  const knownTransactions = new Set();
  for (const transactionId of transactionNames) {
    const normalizedId = transactionId.toLowerCase();
    knownTransactions.add(normalizedId);
    const transactionDirectory = path.join(
      transactionsDirectory,
      transactionId,
    );
    const directoryEvidence = secureMetadata(transactionDirectory, "directory");
    if (directoryEvidence.status !== "secure") {
      checks.push(
        metadataFinding(
          `repairs.transaction.${safeLabel(normalizedId)}.metadata`,
          scope,
          "Repair transaction",
          directoryEvidence,
        ),
      );
      continue;
    }
    const manifestPath = path.join(transactionDirectory, "manifest.json");
    const manifestEvidence = secureMetadata(manifestPath, "file");
    if (
      manifestEvidence.status !== "secure" ||
      manifestEvidence.metadata.size > MAX_RECORD_BYTES
    ) {
      checks.push(
        metadataFinding(
          `repairs.transaction.${safeLabel(normalizedId)}.manifest`,
          scope,
          "Repair transaction manifest",
          manifestEvidence.status === "secure"
            ? { status: "wrong-type" }
            : manifestEvidence,
        ),
      );
      continue;
    }
    let manifest;
    try {
      manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    } catch {
      manifest = null;
    }
    const phase = manifest?.phase;
    const receipt = receiptByTransaction.get(normalizedId) || null;
    const plan = manifest?.plan;
    const planBase = plan && typeof plan === "object"
      ? Object.fromEntries(
          Object.entries(plan).filter(([field]) => field !== "planDigest"),
        )
      : null;
    const planValid = Boolean(
      /^[0-9a-f]{64}$/.test(plan?.planDigest || "") &&
        createHash("sha256").update(JSON.stringify(planBase)).digest("hex") ===
          plan.planDigest
    );
    const baseValid = Boolean(
      manifest?.schemaVersion === 1 &&
        manifest.transactionId === normalizedId &&
        [
          "initializing",
          "prepared",
          "mutation-started",
          "mutated",
          "completed",
          "failed",
        ].includes(phase) &&
        planValid &&
        Number.isFinite(Date.parse(manifest.startedAt || "")) &&
        Number.isFinite(Date.parse(manifest.updatedAt || ""))
    );
    const backupFiles = Array.isArray(manifest?.backup?.files)
      ? manifest.backup.files
      : [];
    const backupRequired = [
      "prepared",
      "mutation-started",
      "mutated",
      "completed",
    ].includes(phase);
    let backupValid = Boolean(
      (!manifest?.backup && !backupRequired) ||
        ([
          "cluster-head",
          "delivery-ledger-index",
          "inbound-policy-artifacts",
          "legacy-job",
        ].includes(
          manifest?.backup?.kind,
        ) &&
          Array.isArray(manifest.backup.files) &&
          manifest.backup.files.length <= 4_097),
    );
    for (const backup of backupFiles) {
      if (
        !/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(backup?.name || "") ||
        !Number.isSafeInteger(backup.bytes) ||
        backup.bytes < 0
      ) {
        backupValid = false;
        break;
      }
      const subdirectory = manifest.backup.kind === "delivery-ledger-index"
        ? "index"
        : manifest.backup.kind === "inbound-policy-artifacts"
          ? "inbound-policy-artifacts"
          : "";
      const filename = path.join(transactionDirectory, subdirectory, backup.name);
      const evidence = secureMetadata(filename, "file");
      if (
        evidence.status !== "secure" ||
        evidence.metadata.size !== backup.bytes ||
        !/^[0-9a-f]{64}$/.test(backup.sha256 || "") ||
        createHash("sha256").update(readFileSync(filename)).digest("hex") !==
          backup.sha256
      ) {
        backupValid = false;
        break;
      }
    }
    const expectedRootEntries = new Set(["manifest.json"]);
    if (manifest?.backup?.kind === "cluster-head") {
      for (const backup of backupFiles) {
        expectedRootEntries.add(backup.name);
      }
    } else if (manifest?.backup?.kind === "delivery-ledger-index") {
      expectedRootEntries.add("index");
    } else if (manifest?.backup?.kind === "inbound-policy-artifacts") {
      expectedRootEntries.add("inbound-policy-artifacts");
    } else if (manifest?.backup?.kind === "legacy-job") {
      for (const backup of backupFiles) {
        expectedRootEntries.add(backup.name);
      }
    }
    if (
      readdirSync(transactionDirectory).some(
        (name) => !expectedRootEntries.has(name),
      )
    ) {
      backupValid = false;
    }
    if (
      backupValid &&
      manifest?.backup?.kind === "delivery-ledger-index"
    ) {
      const expectedIndexEntries = new Set(
        backupFiles.map((backup) => backup.name),
      );
      const indexDirectory = path.join(transactionDirectory, "index");
      if (
        secureMetadata(indexDirectory, "directory").status !== "secure" ||
        readdirSync(indexDirectory).some(
          (name) => !expectedIndexEntries.has(name),
        )
      ) {
        backupValid = false;
      }
    }
    if (
      backupValid &&
      manifest?.backup?.kind === "inbound-policy-artifacts"
    ) {
      const expectedArtifactEntries = new Set(
        backupFiles.map((backup) => backup.name),
      );
      const artifactDirectory = path.join(
        transactionDirectory,
        "inbound-policy-artifacts",
      );
      if (
        secureMetadata(artifactDirectory, "directory").status !== "secure" ||
        readdirSync(artifactDirectory).some(
          (name) => !expectedArtifactEntries.has(name),
        )
      ) {
        backupValid = false;
      }
    }
    const terminal = ["completed", "failed"].includes(phase);
    const receiptValid = Boolean(
      receipt &&
        receipt.planDigest === manifest.plan?.planDigest &&
        receipt.status === phase &&
        (phase !== "completed" ||
          manifest.receiptSha256 ===
            createHash("sha256").update(JSON.stringify(receipt)).digest("hex")),
    );
    const consistent = baseValid && backupValid && (!terminal || receiptValid);
    const incomplete = baseValid && backupValid && !terminal;
    checks.push(
      diagnosticCheck({
        id: `repairs.transaction.${safeLabel(normalizedId)}.consistency`,
        scope,
        status: consistent ? (incomplete || phase === "failed" ? "warn" : "pass") : "fail",
        summary: !consistent
          ? "Repair transaction, backup, and receipt evidence are inconsistent"
          : incomplete
            ? "Repair transaction stopped before a terminal audit receipt"
            : phase === "failed"
              ? "Repair attempt failed with a retained audit receipt and any prepared backup"
              : "Repair transaction has a verified backup and terminal audit receipt",
        verification: "records",
        errorCode: !consistent
          ? "EREPAIRCONSISTENCY"
          : incomplete
            ? "EREPAIRINCOMPLETE"
            : phase === "failed"
              ? receipt.errorCode || "EREPAIRFAILED"
              : null,
        required: !incomplete && phase !== "failed",
      }),
    );
  }
  for (const transactionId of receiptByTransaction.keys()) {
    if (knownTransactions.has(transactionId)) continue;
    checks.push(
      diagnosticCheck({
        id: `repairs.receipt.${safeLabel(transactionId)}.orphan`,
        scope,
        status: "fail",
        summary: "Repair receipt has no retained transaction and backup",
        verification: "records",
        errorCode: "EREPAIRRECEIPTORPHAN",
      }),
    );
  }
  return checks;
}

export function inspectRepairRetentionState({ stateDir } = {}) {
  const scope = "repair-retention";
  const root = path.join(stateDir, "repair-retention");
  const transactionsDirectory = path.join(root, "transactions");
  const receiptsDirectory = path.join(root, "receipts");
  const checks = [];
  const rootEvidence = secureMetadata(root, "directory");
  if (rootEvidence.status === "missing") {
    return [
      metadataFinding(
        "repair-retention.directory",
        scope,
        "Repair archive directory",
        rootEvidence,
        { required: false },
      ),
    ];
  }
  checks.push(metadataFinding(
    "repair-retention.directory",
    scope,
    "Repair archive directory",
    rootEvidence,
  ));
  if (rootEvidence.status !== "secure") return checks;
  if (readdirSync(root).some((name) => !["transactions", "receipts"].includes(name))) {
    checks.push(diagnosticCheck({
      id: "repair-retention.entries",
      scope,
      status: "fail",
      summary: "Repair archive contains unexpected top-level entries",
      verification: "records",
      errorCode: "EREPAIRARCHIVESTATE",
    }));
  }
  const transactionsEvidence = secureMetadata(transactionsDirectory, "directory");
  const receiptsEvidence = secureMetadata(receiptsDirectory, "directory");
  checks.push(
    metadataFinding(
      "repair-retention.transactions.directory",
      scope,
      "Repair archive transaction directory",
      transactionsEvidence,
    ),
    metadataFinding(
      "repair-retention.receipts.directory",
      scope,
      "Repair archive receipt directory",
      receiptsEvidence,
    ),
  );
  if (
    transactionsEvidence.status !== "secure" ||
    receiptsEvidence.status !== "secure"
  ) return checks;

  const receiptNames = readdirSync(receiptsDirectory).sort();
  const validReceiptName = (name) =>
    /^[0-9a-f-]{36}\.json$/i.test(name) ||
    /^[0-9a-f-]{36}\.restore-[0-9a-f-]{36}\.json$/i.test(name);
  if (receiptNames.some((name) => !validReceiptName(name))) {
    checks.push(diagnosticCheck({
      id: "repair-retention.receipts.entries",
      scope,
      status: "fail",
      summary: "Repair archive receipt storage contains unexpected entries",
      verification: "records",
      errorCode: "EREPAIRARCHIVESTATE",
    }));
  }
  const receiptByName = new Map();
  for (const name of receiptNames.filter(validReceiptName)) {
    const filename = path.join(receiptsDirectory, name);
    const evidence = secureMetadata(filename, "file");
    if (evidence.status !== "secure" || evidence.metadata.size > MAX_RECORD_BYTES) {
      checks.push(metadataFinding(
        `repair-retention.receipt.${safeLabel(name)}.metadata`,
        scope,
        "Repair archive receipt",
        evidence.status === "secure" ? { status: "wrong-type" } : evidence,
      ));
      continue;
    }
    try {
      receiptByName.set(name, JSON.parse(readFileSync(filename, "utf8")));
    } catch {
      checks.push(diagnosticCheck({
        id: `repair-retention.receipt.${safeLabel(name)}.json`,
        scope,
        status: "fail",
        summary: "Repair archive receipt is malformed",
        verification: "parse",
        errorCode: "EREPAIRARCHIVERECEIPT",
      }));
    }
  }

  const treeDigest = (directory, prefix = "", depth = 0) => {
    if (depth > 6 || secureMetadata(directory, "directory").status !== "secure") {
      return null;
    }
    const files = [];
    for (const name of readdirSync(directory).sort()) {
      if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(name)) return null;
      const filename = path.join(directory, name);
      const relative = prefix ? `${prefix}/${name}` : name;
      const metadata = lstatSync(filename);
      if (metadata.isDirectory()) {
        const nested = treeDigest(filename, relative, depth + 1);
        if (!nested) return null;
        files.push(...nested.files);
      } else {
        const evidence = secureMetadata(filename, "file");
        if (evidence.status !== "secure") return null;
        const contents = readFileSync(filename);
        files.push({
          name: relative,
          bytes: metadata.size,
          sha256: createHash("sha256").update(contents).digest("hex"),
        });
      }
    }
    return {
      files,
      sha256: createHash("sha256").update(JSON.stringify(files)).digest("hex"),
    };
  };

  const archiveIds = readdirSync(transactionsDirectory).sort();
  const knownReceiptNames = new Set();
  for (const archiveId of archiveIds) {
    const archiveDirectory = path.join(transactionsDirectory, archiveId);
    let consistent = /^[0-9a-f-]{36}$/i.test(archiveId) &&
      secureMetadata(archiveDirectory, "directory").status === "secure";
    let manifest = null;
    if (consistent) {
      const manifestPath = path.join(archiveDirectory, "manifest.json");
      const evidence = secureMetadata(manifestPath, "file");
      consistent = evidence.status === "secure" &&
        evidence.metadata.size <= MAX_RECORD_BYTES;
      if (consistent) {
        try {
          manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
        } catch {
          consistent = false;
        }
      }
    }
    const items = manifest?.items;
    consistent = Boolean(
      consistent &&
        manifest?.schemaVersion === 1 &&
        manifest.archiveId === archiveId &&
        ["archiving", "committed", "restoring", "restored"].includes(manifest.status) &&
        /^[0-9a-f]{64}$/.test(manifest.planDigest || "") &&
        Number.isFinite(Date.parse(manifest.createdAt || "")) &&
        Array.isArray(items) &&
        items.length > 0 &&
        new Set(items.map((item) => item.transactionId)).size === items.length
    );
    const itemsDirectory = path.join(archiveDirectory, "items");
    if (
      consistent &&
      readdirSync(archiveDirectory).some(
        (name) => !["manifest.json", "items"].includes(name),
      )
    ) {
      consistent = false;
    }
    if (consistent && secureMetadata(itemsDirectory, "directory").status !== "secure") {
      consistent = false;
    }
    for (const item of consistent ? items : []) {
      if (
        !/^[0-9a-f-]{36}$/i.test(item?.transactionId || "") ||
        !/^[0-9a-f]{64}$/.test(item.transactionSha256 || "") ||
        !/^[0-9a-f]{64}$/.test(item.receiptSha256 || "")
      ) {
        consistent = false;
        break;
      }
      const itemDirectory = path.join(itemsDirectory, item.transactionId);
      if (secureMetadata(itemDirectory, "directory").status !== "secure") {
        consistent = false;
        break;
      }
      if (manifest.status === "committed") {
        const transaction = treeDigest(path.join(itemDirectory, "transaction"));
        const receiptPath = path.join(itemDirectory, "receipt.json");
        if (
          item.state !== "archived" ||
          readdirSync(itemDirectory).some(
            (name) => !["transaction", "receipt.json"].includes(name),
          ) ||
          transaction?.sha256 !== item.transactionSha256 ||
          secureMetadata(receiptPath, "file").status !== "secure" ||
          createHash("sha256").update(readFileSync(receiptPath)).digest("hex") !==
            item.receiptSha256
        ) {
          consistent = false;
          break;
        }
      } else if (manifest.status === "restored") {
        if (item.state !== "restored" || readdirSync(itemDirectory).length !== 0) {
          consistent = false;
          break;
        }
      }
    }
    const terminal = ["committed", "restored"].includes(manifest?.status);
    if (consistent && terminal) {
      const archiveReceiptName = `${archiveId}.json`;
      const archiveReceipt = receiptByName.get(archiveReceiptName);
      knownReceiptNames.add(archiveReceiptName);
      consistent = Boolean(
        archiveReceipt?.schemaVersion === 1 &&
          archiveReceipt.outcome === "archived" &&
          archiveReceipt.archiveId === archiveId &&
          manifest.receiptSha256 ===
            createHash("sha256").update(JSON.stringify(archiveReceipt)).digest("hex")
      );
      if (consistent && manifest.status === "restored") {
        const restoreName = `${archiveId}.restore-${manifest.restore?.restoreId}.json`;
        const restoreReceipt = receiptByName.get(restoreName);
        knownReceiptNames.add(restoreName);
        consistent = Boolean(
          restoreReceipt?.schemaVersion === 1 &&
            restoreReceipt.outcome === "restored" &&
            restoreReceipt.archiveId === archiveId &&
            restoreReceipt.restoreId === manifest.restore?.restoreId &&
            manifest.restore?.receiptSha256 ===
              createHash("sha256").update(JSON.stringify(restoreReceipt)).digest("hex")
        );
      }
    }
    const incomplete = consistent && !terminal;
    checks.push(diagnosticCheck({
      id: `repair-retention.transaction.${safeLabel(archiveId)}.consistency`,
      scope,
      status: consistent ? (incomplete ? "warn" : "pass") : "fail",
      summary: !consistent
        ? "Repair archive transaction, item, or receipt evidence is inconsistent"
        : incomplete
          ? "Repair archive transaction requires explicit recovery"
          : manifest.status === "restored"
            ? "Repair archive was restored with consistent terminal evidence"
            : "Repair archive retains consistent recoverable evidence",
      verification: "records",
      errorCode: !consistent
        ? "EREPAIRARCHIVECONSISTENCY"
        : incomplete
          ? "EREPAIRARCHIVEINCOMPLETE"
          : null,
      required: !incomplete,
    }));
  }
  for (const name of receiptByName.keys()) {
    if (knownReceiptNames.has(name)) continue;
    checks.push(diagnosticCheck({
      id: `repair-retention.receipt.${safeLabel(name)}.orphan`,
      scope,
      status: "fail",
      summary: "Repair archive receipt has no matching terminal transaction",
      verification: "records",
      errorCode: "EREPAIRARCHIVERECEIPTORPHAN",
    }));
  }
  return checks;
}

export function inspectRouteState({
  stateDir,
  sessions = [],
  target = null,
  now = Date.now(),
  reconcileGraceMs = ROUTE_RECONCILE_GRACE_MS,
  ledgerQuotaBytes = DELIVERY_LEDGER_QUOTA_BYTES,
  processStateFn = processState,
  schedulerRevision = CXMSG_IMPLEMENTATION_REVISIONS.scheduler,
  inboundPolicyFeatureActive = INBOUND_POLICY_FEATURE_ACTIVE,
} = {}) {
  const bindings = scanJsonDirectory({
    stateDir,
    directoryName: "route-bindings",
    scope: "route-bindings",
    validate: validRouteBinding,
  });
  const deliveries = scanJsonDirectory({
    stateDir,
    directoryName: "route-deliveries",
    scope: "route-deliveries",
    validate: validRouteDelivery,
  });
  const ledger = inspectDeliveryLedger(stateDir, { quotaBytes: ledgerQuotaBytes });
  const quarantine = scanJsonDirectory({
    stateDir,
    directoryName: "quarantine",
    scope: "quarantine",
    validate: validQuarantine,
    maxRecordBytes: MAX_STORED_MESSAGE_BYTES + 16 * 1024,
  });
  const scheduleSuccessors = scanJsonDirectory({
    stateDir,
    directoryName: "directory/successors",
    scope: "schedule-successors",
    validate: validDirectorySuccessor,
  });
  const checks = [
    ...bindings.checks,
    ...deliveries.checks,
    ...ledger.checks,
    ...quarantine.checks,
    ...scheduleSuccessors.checks,
  ];
  const inboundPolicyEvidenceCount = Array.isArray(ledger.projections)
    ? ledger.projections.filter((record) => {
        const deliveries = [
          record.delivery,
          ...(record.groupDeliveries || []),
          ...(record.teamDeliveries || []),
        ].filter(Boolean);
        return deliveries.some(
          (delivery) =>
            delivery.admissionState === "denied" ||
            delivery.inboundPolicy ||
            delivery.attempts?.some(
              (attempt) => attempt.inboundPolicySnapshot,
            ) ||
            delivery.evidence?.some((evidence) => evidence.inboundPolicy),
        );
      }).length
    : 0;
  const inactiveEvidenceValid =
    inboundPolicyFeatureActive || inboundPolicyEvidenceCount === 0;
  checks.push(
    diagnosticCheck({
      id: "inbound-policies.inactive-evidence",
      scope: "inbound-policies",
      status: inactiveEvidenceValid ? "pass" : "fail",
      summary: inactiveEvidenceValid
        ? "The Inbound Policy feature gate and durable delivery evidence agree"
        : "Durable Inbound Policy evidence exists while the feature gate is off",
      verification: "records",
      errorCode: inactiveEvidenceValid ? null : "EINBOUNDPOLICYBYPASS",
      remediation: inactiveEvidenceValid
        ? null
        : "Do not treat staged policy evidence as active enforcement; inspect the writer that bypassed the shared feature gate",
    }),
  );
  const inTargetScope = (record) =>
    !target ||
    record.target === target ||
    record.from === target ||
    record.logicalMessage?.from === target;
  const scopedBindings = target
    ? bindings.records.filter((record) => record.sessionName === target)
    : bindings.records;
  const scopedLegacyDeliveries = deliveries.records.filter(inTargetScope);
  const scopedLedgerDeliveries = ledger.records.filter(inTargetScope);
  const scopedQuarantine = quarantine.records.filter(inTargetScope);
  const predecessorNodeKeys = new Set(
    scheduleSuccessors.records.map((record) => record.predecessorNodeKey),
  );
  const sessionsByName = new Map(sessions.map((record) => [record.name, record]));
  const legacyMessageIds = new Set(
    scopedLegacyDeliveries.map((record) => record.logicalMessageId),
  );
  for (const delivery of scopedLedgerDeliveries) {
    if (!legacyMessageIds.has(delivery.logicalMessageId)) continue;
    checks.push(
      diagnosticCheck({
        id: `delivery-ledger.duplicate.${safeLabel(delivery.logicalMessageId)}`,
        scope: "delivery-ledger",
        status: "fail",
        summary: "A Logical Message ID exists in both the Ledger and legacy Route Delivery storage",
        verification: "records",
        errorCode: "ELEDGERDUPLICATEIDENTITY",
        remediation:
          "Back up all cxmsg runtime state, then move the legacy record to an operator archive; reconciliation updates only the Ledger",
      }),
    );
  }
  for (const binding of scopedBindings) {
    const current = sessionsByName.get(binding.sessionName);
    const matches = current?.threadId === binding.threadId;
    checks.push(
      diagnosticCheck({
        id: `route-bindings.session.${safeLabel(binding.sessionName)}`,
        scope: "route-bindings",
        status: matches ? "pass" : "fail",
        summary: matches
          ? `Route binding for ${binding.sessionName} matches its registered thread`
          : `Route binding for ${binding.sessionName} does not match a registered thread`,
        verification: "registry",
        errorCode: matches ? null : "EROUTEIDENTITY",
        remediation: matches
          ? null
          : "Re-bind the intended registered session; do not release quarantined messages automatically",
      }),
    );
  }
  for (const delivery of [...scopedLegacyDeliveries, ...scopedLedgerDeliveries]) {
    const deliveryScope = delivery.version === 2 ? "delivery-ledger" : "route-deliveries";
    if (delivery.targetThreadId) {
      const target = sessionsByName.get(delivery.target);
      const identityMatches = target?.threadId === delivery.targetThreadId;
      checks.push(
        diagnosticCheck({
          id: `${deliveryScope}.target.${safeLabel(delivery.logicalMessageId)}`,
          scope: deliveryScope,
          status: identityMatches ? "pass" : "fail",
          summary: identityMatches
            ? "Route Delivery retains its registered target thread identity"
            : "Route Delivery target no longer matches its registered thread identity",
          verification: "registry",
          errorCode: identityMatches ? null : "EROUTETARGETIDENTITY",
          remediation: identityMatches
            ? null
            : "Do not replay this Delivery; reconciliation is bound to the original target thread",
        }),
      );
    }
    if (delivery.version === 2 && delivery.status === "dispatching") {
      if (
        Number.isFinite(Date.parse(delivery.attemptStartedAt || "")) &&
        now - Date.parse(delivery.attemptStartedAt) >= reconcileGraceMs
      ) {
        checks.push(
          diagnosticCheck({
            id: `delivery-ledger.stale-attempt.${safeLabel(delivery.logicalMessageId)}`,
            scope: "delivery-ledger",
            status: "warn",
            summary: "A Delivery attempt exceeded the shared reconciliation grace without evidence",
            verification: "records",
            errorCode: "ELEDGERATTEMPTSTALE",
            remediation: `Run cxmsg route reconcile ${delivery.logicalMessageId}; absence is not retry permission`,
            required: false,
          }),
        );
      }
      continue;
    }
    if (
      delivery.version === 2 &&
      delivery.status === "scheduled" &&
      delivery.claimLeaseUntil &&
      Date.parse(delivery.claimLeaseUntil) <= now
    ) {
      checks.push(
        diagnosticCheck({
          id: `delivery-ledger.expired-claim.${safeLabel(delivery.logicalMessageId)}`,
          scope: "schedules",
          status: "warn",
          summary: "A scheduled Delivery retains an expired claim lease",
          verification: "records",
          errorCode: "ESCHEDULECLAIMEXPIRED",
          remediation: "Start the scheduler to reclaim the Delivery; do not replay it manually",
          required: false,
        }),
      );
    }
    if (
      delivery.version === 2 &&
      delivery.status === "scheduled" &&
      delivery.targetThreadId &&
      predecessorNodeKeys.has(`codex:${delivery.targetThreadId}`)
    ) {
      checks.push(
        diagnosticCheck({
          id: `schedules.target.predecessor.${safeLabel(delivery.logicalMessageId)}`,
          scope: "schedules",
          status: "warn",
          summary: "Scheduled Delivery targets a predecessor Node and will remain blocked",
          verification: "records",
          errorCode: "ETARGETPREDECESSOR",
          remediation:
            "Cancel this schedule and explicitly enqueue a new Logical Message for the intended successor; cxmsg never transfers Delivery or authority",
          required: false,
        }),
      );
    }
    if (
      delivery.version === 2 &&
      delivery.status === "scheduled" &&
      delivery.triggerKind === "turn"
    ) {
      checks.push(
        diagnosticCheck({
          id: `schedules.trigger.turn.${safeLabel(delivery.logicalMessageId)}`,
          scope: "schedules",
          status: "pass",
          summary: "Scheduled Delivery retains an exact bounded turn trigger identity",
          verification: "records",
          required: false,
        }),
      );
    }
    if (
      delivery.version === 2 &&
      delivery.status === "scheduled" &&
      delivery.triggerKind === "job"
    ) {
      const jobPath = path.join(stateDir, "jobs", `${delivery.triggerId}.json`);
      const metadata = secureMetadata(jobPath, "file");
      let validJob = false;
      let blockedJob = false;
      if (metadata.status === "secure") {
        try {
          const job = JSON.parse(readFileSync(jobPath, "utf8"));
          validJob =
            job?.version === 1 &&
            job.jobId === delivery.triggerId &&
            typeof job.status === "string";
          blockedJob = validJob && job.status === "unknown";
        } catch {}
      }
      const missing = metadata.status === "missing";
      checks.push(
        diagnosticCheck({
          id: `schedules.trigger.job.${safeLabel(delivery.logicalMessageId)}`,
          scope: "schedules",
          status: validJob && !blockedJob ? "pass" : missing || blockedJob ? "warn" : "fail",
          summary: validJob
            ? blockedJob
              ? "Scheduled Delivery references a Job with unverifiable terminal state"
              : "Scheduled Delivery references an existing bounded Job identity"
            : missing
              ? "Scheduled Delivery references a missing Job"
              : "Scheduled Delivery Job trigger failed private schema validation",
          verification: metadata.status === "secure" ? "records" : metadata.status,
          errorCode:
            validJob && !blockedJob
              ? null
              : blockedJob
                ? "ETRIGGERBLOCKED"
                : missing
                  ? "ETRIGGERJOBMISSING"
                  : "ETRIGGERJOBSCHEMA",
          remediation:
            validJob && !blockedJob
              ? null
              : "Do not dispatch manually; restore or reconcile the exact referenced Job evidence",
          required: false,
        }),
      );
    }
    if (["dispatching", "unknown"].includes(delivery.status)) {
      const legacyIdentity = !delivery.targetThreadId;
      const reconciledUnknown =
        !legacyIdentity &&
        delivery.status === "unknown" &&
        delivery.errorCode === "EACCEPTANCEUNVERIFIED" &&
        delivery.latestEvidenceKind === "reconciliation";
      checks.push(
        diagnosticCheck({
          id: `${deliveryScope}.reconcile.${safeLabel(delivery.logicalMessageId)}`,
          scope: deliveryScope,
          status: "warn",
          summary: legacyIdentity
            ? "Unconfirmed legacy Route Delivery lacks pinned target identity"
            : reconciledUnknown
              ? "Route Delivery was reconciled without positive acceptance evidence"
              : "Route Delivery requires positive App Server acceptance reconciliation",
          verification: "records",
          errorCode: legacyIdentity
            ? "EROUTELEGACYIDENTITY"
            : reconciledUnknown
              ? "EROUTERECONCILEDUNKNOWN"
              : "EROUTEUNCONFIRMED",
          remediation: legacyIdentity
            ? "Do not replay this legacy Delivery; create a new logical message only after operator review"
            : reconciledUnknown
              ? "Retain this unknown Delivery; do not replay or repeat reconciliation without new positive evidence"
              : `Run cxmsg route reconcile ${delivery.logicalMessageId}; absence is not replay permission`,
          required: false,
        }),
      );
    }
  }
  checks.push(
    diagnosticCheck({
      id: "quarantine.records.count",
      scope: "quarantine",
      status: scopedQuarantine.length > 0 ? "warn" : "pass",
      summary:
        scopedQuarantine.length > 0
          ? `${scopedQuarantine.length} quarantined route message(s) require operator review`
          : "No routed peer messages are quarantined",
      verification: "records",
      errorCode: scopedQuarantine.length > 0 ? "EQUARANTINED" : null,
      remediation:
        scopedQuarantine.length > 0
          ? "Inspect metadata with cxmsg quarantine list; this release intentionally has no automatic release"
          : null,
      required: false,
    }),
  );
  const scheduled = scopedLedgerDeliveries.filter(
    (delivery) => delivery.status === "scheduled",
  );
  const lifecyclePath = path.join(stateDir, "turn-lifecycle.json");
  const lifecycleMetadata = secureMetadata(lifecyclePath, "file");
  if (!["missing", "secure"].includes(lifecycleMetadata.status)) {
    checks.push(
      metadataFinding(
        "schedules.lifecycle.metadata",
        "schedules",
        "Turn Lifecycle projection",
        lifecycleMetadata,
      ),
    );
  } else if (lifecycleMetadata.status === "secure") {
    let lifecycle = null;
    try {
      lifecycle = JSON.parse(readFileSync(lifecyclePath, "utf8"));
    } catch {}
    const lifecycleValid = validTurnLifecycleState(lifecycle);
    checks.push(
      diagnosticCheck({
        id: "schedules.lifecycle.schema",
        scope: "schedules",
        status: lifecycleValid ? "pass" : "fail",
        summary: lifecycleValid
          ? "Turn Lifecycle projection is valid and metadata-only"
          : "Turn Lifecycle projection is invalid",
        verification: "schema",
        errorCode: lifecycleValid ? null : "ETURNLIFECYCLESCHEMA",
        required: false,
      }),
    );
  }
  const schedulerPath = path.join(stateDir, "scheduler.json");
  const schedulerIntentPath = path.join(stateDir, "scheduler.intent.json");
  const schedulerIntentMetadata = secureMetadata(schedulerIntentPath, "file");
  let schedulerIntent = null;
  if (schedulerIntentMetadata.status === "secure") {
    try {
      const candidate = JSON.parse(readFileSync(schedulerIntentPath, "utf8"));
      if (
        candidate?.version === 1 &&
        ["running", "stopped"].includes(candidate.desiredState) &&
        Number.isFinite(Date.parse(candidate.changedAt || ""))
      ) {
        schedulerIntent = candidate;
      }
    } catch {}
  }
  if (!["missing", "secure"].includes(schedulerIntentMetadata.status)) {
    checks.push(
      metadataFinding(
        "schedules.worker.intent.metadata",
        "schedules",
        "Scheduler desired-state record",
        schedulerIntentMetadata,
      ),
    );
  } else if (schedulerIntentMetadata.status === "secure" && !schedulerIntent) {
    checks.push(
      diagnosticCheck({
        id: "schedules.worker.intent.schema",
        scope: "schedules",
        status: "fail",
        summary: "Scheduler desired-state record is invalid",
        verification: "schema",
        errorCode: "ESCHEDULERINTENTSCHEMA",
        required: false,
      }),
    );
  }
  const schedulerMetadata = secureMetadata(schedulerPath, "file");
  if (schedulerMetadata.status === "missing") {
    const missingError =
      schedulerIntent?.desiredState === "running"
        ? "ESCHEDULERCRASHED"
        : schedulerIntent?.desiredState === "stopped"
          ? "ESCHEDULERSTOPPED"
          : "ESCHEDULERDOWN";
    checks.push(
      diagnosticCheck({
        id: "schedules.worker",
        scope: "schedules",
        status:
          scheduled.length > 0 || schedulerIntent?.desiredState === "running"
            ? "warn"
            : "pass",
        summary:
          scheduled.length > 0
            ? schedulerIntent?.desiredState === "running"
              ? `${scheduled.length} scheduled Delivery record(s) lost their intended scheduler worker`
              : `${scheduled.length} scheduled Delivery record(s) have no registered scheduler worker`
            : schedulerIntent?.desiredState === "running"
              ? "The intended Scheduler worker is missing"
              : "No scheduled Deliveries require a scheduler worker",
        verification: "metadata",
        errorCode:
          scheduled.length > 0 || schedulerIntent?.desiredState === "running"
            ? missingError
            : null,
        remediation:
          scheduled.length > 0 || schedulerIntent?.desiredState === "running"
            ? "Run cxmsg scheduler start"
            : null,
        required: false,
      }),
    );
  } else if (schedulerMetadata.status !== "secure") {
    checks.push(
      metadataFinding(
        "schedules.worker.metadata",
        "schedules",
        "Scheduler worker record",
        schedulerMetadata,
      ),
    );
  } else {
    let scheduler = null;
    try {
      scheduler = JSON.parse(readFileSync(schedulerPath, "utf8"));
    } catch {}
    const valid = Boolean(
      [1, 2].includes(scheduler?.version) &&
        Number.isSafeInteger(scheduler.pid) &&
        scheduler.pid > 1 &&
        UUID_PATTERN.test(scheduler.workerId || "") &&
        Number.isFinite(Date.parse(scheduler.startedAt || "")) &&
        (scheduler.version === 1 ||
          Number.isFinite(Date.parse(scheduler.heartbeatAt || ""))) &&
        (scheduler.cxmsgVersion === undefined ||
          (typeof scheduler.cxmsgVersion === "string" &&
            scheduler.cxmsgVersion.length >= 1 &&
            scheduler.cxmsgVersion.length <= 64)) &&
        (scheduler.implementationRevision === undefined ||
          (Number.isSafeInteger(scheduler.implementationRevision) &&
            scheduler.implementationRevision >= 1)),
    );
    if (!valid) {
      checks.push(
        diagnosticCheck({
          id: "schedules.worker.schema",
          scope: "schedules",
          status: "fail",
          summary: "Scheduler worker record is invalid",
          verification: "schema",
          errorCode: "ESCHEDULERSCHEMA",
        }),
      );
    } else {
      checks.push(
        implementationRevisionCheck({
          id: "schedules.worker.implementation",
          scope: "schedules",
          label: "Scheduler worker",
          recordedRevision: scheduler.implementationRevision,
          currentRevision: schedulerRevision,
          unknownCode: "ESCHEDULERVERSIONUNKNOWN",
          staleCode: "ESCHEDULERSTALECODE",
          remediation:
            "Restart the Scheduler from an allowed host context, then rerun Doctor; Doctor will not restart it",
        }),
      );
      const workerState = processStateFn(scheduler.pid);
      const heartbeatStale =
        scheduler.version === 2 &&
        now - Date.parse(scheduler.heartbeatAt) > SCHEDULER_HEARTBEAT_STALE_MS;
      const passError =
        scheduler.version === 2 && /^[A-Z0-9_]{1,32}$/.test(scheduler.lastErrorCode || "")
          ? scheduler.lastErrorCode
          : null;
      const liveStatus =
        scheduler.version === 1 || heartbeatStale || passError ? "warn" : "pass";
      const liveSummary =
        scheduler.version === 1
          ? "Scheduler worker predates heartbeat health evidence"
          : heartbeatStale
            ? "Scheduler worker heartbeat is stale"
            : passError
              ? "Scheduler worker recorded a bounded pass failure"
              : "Scheduler worker process and heartbeat are live";
      const liveError =
        scheduler.version === 1
          ? "ESCHEDULERLEGACY"
          : heartbeatStale
            ? "ESCHEDULERSTALLED"
            : passError
              ? "ESCHEDULERPASS"
              : null;
      const missingError =
        schedulerIntent?.desiredState === "stopped"
          ? "ESCHEDULERSTOPPED"
          : "ESCHEDULERCRASHED";
      checks.push(
        diagnosticCheck({
          id: "schedules.worker.process",
          scope: "schedules",
          status:
            workerState === "alive"
              ? liveStatus
              : workerState === "unverified"
                ? "unknown"
                : scheduled.length > 0 || schedulerIntent?.desiredState === "running"
                  ? "warn"
                  : "pass",
          summary:
            workerState === "alive"
              ? liveSummary
              : workerState === "unverified"
                ? "Scheduler worker process cannot be verified by this caller"
                : schedulerIntent?.desiredState === "stopped"
                  ? "Scheduler worker was intentionally stopped"
                  : "Scheduler worker exited while its desired state remained running",
          verification: "process",
          errorCode:
            workerState === "alive"
              ? liveError
              : workerState === "unverified"
                ? "ESCHEDULERUNVERIFIED"
                : missingError,
          remediation:
            workerState === "alive" && liveStatus === "warn"
              ? "Restart the scheduler from an allowed host context after inspecting its bounded log and Delivery state"
              : workerState === "missing" && scheduled.length > 0
              ? "Run cxmsg scheduler start"
              : null,
          required: false,
        }),
      );
    }
  }
  return checks;
}

export function inspectInboundPolicies({
  stateDir,
  featureActive = INBOUND_POLICY_FEATURE_ACTIVE,
} = {}) {
  const scope = "inbound-policies";
  const policies = scanJsonDirectory({
    stateDir,
    directoryName: "inbound-policies",
    scope,
    validate: validInboundPolicy,
    maxRecordBytes: INBOUND_POLICY_MAX_RECORD_BYTES,
  });
  const nodes = scanJsonDirectory({
    stateDir,
    directoryName: "directory/nodes",
    scope: "inbound-policy-nodes",
    validate: validDirectoryNode,
  });
  const tombstones = scanJsonDirectory({
    stateDir,
    directoryName: "directory/tombstones/nodes",
    scope: "inbound-policy-node-tombstones",
    validate: validDirectoryNodeTombstone,
  });
  const projects = scanJsonDirectory({
    stateDir,
    directoryName: "directory/projects",
    scope: "inbound-policy-projects",
    validate: validDirectoryProject,
  });
  const successors = scanJsonDirectory({
    stateDir,
    directoryName: "directory/successors",
    scope: "inbound-policy-successors",
    validate: validDirectorySuccessor,
  });
  const checks = [...policies.checks];
  const policyDirectory = path.join(stateDir, "inbound-policies");
  const policyDirectoryEvidence = secureMetadata(policyDirectory, "directory");
  if (policyDirectoryEvidence.status === "secure") {
    try {
      let activeTransients = 0;
      let staleTransients = 0;
      let unexpectedEntries = 0;
      for (const name of readdirSync(policyDirectory)) {
        const evidence = secureMetadata(path.join(policyDirectory, name), "file");
        if (evidence.status !== "secure") {
          checks.push(
            metadataFinding(
              `${scope}.entry.${createHash("sha256").update(name).digest("hex").slice(0, 8)}`,
              scope,
              "Inbound policy directory entry",
              evidence,
            ),
          );
          unexpectedEntries += 1;
          continue;
        }
        const classification = classifyInboundPolicyEntry(
          name,
          evidence.metadata.mtimeMs,
        );
        if (classification === "transient") activeTransients += 1;
        else if (classification === "stale-transient") staleTransients += 1;
        else if (classification === "unexpected") unexpectedEntries += 1;
      }
      const entriesValid = staleTransients === 0 && unexpectedEntries === 0;
      checks.push(
        diagnosticCheck({
          id: `${scope}.entries`,
          scope,
          status: entriesValid ? "pass" : "fail",
          summary:
            entriesValid
              ? activeTransients === 0
                ? "Inbound policy directory contains only bounded policy records"
                : "Inbound policy directory contains bounded records and an active mutation artifact"
              : staleTransients > 0
                ? "Inbound policy directory contains a stale mutation artifact"
                : "Inbound policy directory contains an unexpected entry",
          verification: "metadata",
          errorCode: entriesValid
            ? null
            : staleTransients > 0
              ? "EINBOUNDPOLICYTRANSIENTSTALE"
              : "EINBOUNDPOLICYUNEXPECTED",
        }),
      );
    } catch (error) {
      checks.push(
        diagnosticCheck({
          id: `${scope}.entries`,
          scope,
          status: "unknown",
          summary: "Inbound policy directory entries could not be inspected",
          verification: "unavailable",
          errorCode: errorCode(error),
        }),
      );
    }
    const lockEvidence = secureMetadata(
      path.join(stateDir, "inbound-policies.lock"),
      "file",
    );
    if (lockEvidence.status !== "missing") {
      checks.push(
        metadataFinding(
          `${scope}.lock`,
          scope,
          "Inbound policy mutation lock",
          lockEvidence,
          { required: false },
        ),
      );
    }
  }
  const policyByTarget = new Map(
    policies.records.map((record) => [record.targetNodeKey, record]),
  );
  const nodeKeys = new Set(nodes.records.map((record) => record.nodeKey));
  const tombstoneKeys = new Set(
    tombstones.records.map((record) => record.nodeKey),
  );
  const projectIds = new Set(
    projects.records.map((record) => record.projectId),
  );
  const totalRules = policies.records.reduce(
    (total, record) => total + record.rules.length,
    0,
  );
  const quotaValid =
    policies.records.length <= INBOUND_POLICY_MAX_RECORDS &&
    totalRules <= INBOUND_POLICY_MAX_RULES;
  checks.push(
    diagnosticCheck({
      id: `${scope}.quota`,
      scope,
      status: quotaValid ? "pass" : "fail",
      summary: quotaValid
        ? "Inbound policy records and rules remain within global quotas"
        : "Inbound policy records or rules exceed global quotas",
      verification: "bounded",
      errorCode: quotaValid ? null : "EINBOUNDPOLICYQUOTA",
    }),
  );
  const activationValid = featureActive || policies.records.length === 0;
  checks.push(
    diagnosticCheck({
      id: `${scope}.activation`,
      scope,
      status: activationValid ? "pass" : "fail",
      summary: activationValid
        ? "No inactive Inbound Peer Message Policy can imply partial enforcement"
        : "Inbound policy records exist before every ordinary path is integrated",
      verification: "configuration",
      errorCode: activationValid ? null : "EINBOUNDPOLICYINACTIVE",
      remediation: activationValid
        ? null
        : "Do not rely on or expose policy mutation until the cross-path integration gate passes",
    }),
  );
  for (const policy of policies.records) {
    const label = createHash("sha256")
      .update(policy.targetNodeKey)
      .digest("hex")
      .slice(0, 8);
    const targetLive = nodeKeys.has(policy.targetNodeKey);
    const targetTombstoned = tombstoneKeys.has(policy.targetNodeKey);
    checks.push(
      diagnosticCheck({
        id: `${scope}.target.${label}`,
        scope,
        status: targetLive && !targetTombstoned ? "pass" : "warn",
        summary:
          targetLive && !targetTombstoned
            ? "Inbound policy target is a live stable Node"
            : "Inbound policy target is missing or tombstoned",
        verification: "records",
        errorCode:
          targetLive && !targetTombstoned ? null : "EINBOUNDPOLICYTARGET",
        required: false,
      }),
    );
    for (const rule of policy.rules) {
      const ruleLabel = rule.ruleId.slice(0, 8);
      if (rule.selectorKind === "sender-node") {
        const live = nodeKeys.has(rule.selectorNodeKey);
        const tombstoned = tombstoneKeys.has(rule.selectorNodeKey);
        checks.push(
          diagnosticCheck({
            id: `${scope}.rule.${ruleLabel}.node`,
            scope,
            status: live && !tombstoned ? "pass" : "warn",
            summary:
              live && !tombstoned
                ? "Inbound sender-Node selector references a live Node"
                : "Inbound sender-Node selector references a missing or tombstoned Node",
            verification: "records",
            errorCode: live && !tombstoned ? null : "EINBOUNDPOLICYSENDERNODE",
            required: false,
          }),
        );
      } else if (rule.selectorKind === "sender-project") {
        const exists = projectIds.has(rule.projectId);
        checks.push(
          diagnosticCheck({
            id: `${scope}.rule.${ruleLabel}.project`,
            scope,
            status: exists ? "pass" : "warn",
            summary: exists
              ? "Inbound sender-Project selector references a stable Project"
              : "Inbound sender-Project selector references a missing Project",
            verification: "records",
            errorCode: exists ? null : "EINBOUNDPOLICYSENDERPROJECT",
            required: false,
          }),
        );
      }
    }
  }
  for (const relation of successors.records) {
    if (
      !policyByTarget.has(relation.predecessorNodeKey) ||
      policyByTarget.has(relation.successorNodeKey)
    ) {
      continue;
    }
    checks.push(
      diagnosticCheck({
        id: `${scope}.successor.${createHash("sha256")
          .update(relation.successorNodeKey)
          .digest("hex")
          .slice(0, 8)}`,
        scope,
        status: "warn",
        summary: "A policy-bearing predecessor has a Successor with no explicit policy",
        verification: "records",
        errorCode: "EINBOUNDPOLICYSUCCESSORGAP",
        remediation: "Review the Successor independently; policy is never transferred automatically",
        required: false,
      }),
    );
  }
  return checks;
}

export function inspectRuntime({ codexBin = process.env.CODEX_BIN || "codex", run = spawnSync } = {}) {
  const major = Number.parseInt(process.versions.node.split(".")[0], 10);
  const checks = [
    diagnosticCheck({
      id: "runtime.node.version",
      scope: "runtime",
      status: major >= 20 ? "pass" : "fail",
      summary: major >= 20 ? "Node.js satisfies the minimum major version" : "Node.js is below the required major version",
      verification: "runtime",
      errorCode: major >= 20 ? null : "ENODEVERSION",
      remediation: major >= 20 ? null : "Use Node.js 20 or newer",
    }),
    diagnosticCheck({
      id: "runtime.cxmsg.version",
      scope: "runtime",
      status: /^\d+\.\d+\.\d+/.test(CXMSG_VERSION) ? "pass" : "fail",
      summary: `cxmsg version metadata is available (${CXMSG_VERSION})`,
      verification: "package",
    }),
  ];
  const result = run(codexBin, ["--version"], { encoding: "utf8" });
  checks.push(diagnosticCheck({
    id: "runtime.codex.version",
    scope: "runtime",
    status: result.status === 0 ? "pass" : "unknown",
    summary: result.status === 0 ? "Codex CLI version metadata is available" : "Codex CLI version could not be inspected",
    verification: result.status === 0 ? "process-output" : "unavailable",
    errorCode: result.status === 0 ? null : "ECODEXVERSION",
    remediation: result.status === 0 ? null : "Run doctor where the configured Codex executable is available",
  }));
  return checks;
}

export function inspectState({ stateDir, target = null } = {}) {
  const checks = [];
  const rootEvidence = secureMetadata(stateDir, "directory");
  checks.push(metadataFinding("state.root.metadata", "state", "cxmsg state directory", rootEvidence));
  if (rootEvidence.status !== "secure") {
    return { checks, sessions: [], attachments: [], jobs: [], bridges: [] };
  }

  let eventNames = [];
  try {
    eventNames = readdirSync(stateDir)
      .filter((name) => /^events\.jsonl(?:\.\d+)?$/.test(name))
      .sort();
  } catch (error) {
    checks.push(diagnosticCheck({
      id: "state.events.segments",
      scope: "state",
      status: "unknown",
      summary: "Coordination event segments could not be enumerated",
      verification: "unavailable",
      errorCode: errorCode(error),
      required: false,
    }));
  }
  let invalidEventSegments = 0;
  for (const name of eventNames) {
    const evidence = secureMetadata(path.join(stateDir, name), "file");
    if (evidence.status !== "secure" || evidence.metadata.size > EVENT_LOG_MAX_BYTES) {
      invalidEventSegments += 1;
    }
  }
  const tooManyEventSegments = eventNames.length > EVENT_LOG_ARCHIVES + 1;
  if (!checks.some((check) => check.id === "state.events.segments")) {
    checks.push(diagnosticCheck({
      id: "state.events.segments",
      scope: "state",
      status: invalidEventSegments || tooManyEventSegments ? "warn" : "pass",
      summary:
        invalidEventSegments || tooManyEventSegments
          ? "Coordination event segments exceed their owner, mode, size, or count bound"
          : `${eventNames.length} coordination event segment(s) satisfy the bounded retention policy`,
      verification: "metadata",
      errorCode: invalidEventSegments || tooManyEventSegments ? "EEVENTBOUND" : null,
      remediation: invalidEventSegments || tooManyEventSegments ? "Allow the next coordination event to rotate legacy oversized state; do not delete active state from Doctor" : null,
      required: false,
    }));
  }

  const sessions = scanJsonDirectory({ stateDir, directoryName: "sessions", scope: "sessions", validate: validSession });
  const attachments = scanJsonDirectory({ stateDir, directoryName: "attachments", scope: "attachments", validate: validAttachment });
  const jobs = scanJsonDirectory({ stateDir, directoryName: "jobs", scope: "jobs", validate: validJob });
  const bridges = scanJsonDirectory({ stateDir, directoryName: "claude-bridges", scope: "bridges", validate: validBridge });
  checks.push(...sessions.checks, ...attachments.checks, ...jobs.checks, ...bridges.checks);

  if (target && !sessions.records.some((record) => record.name === target)) {
    checks.push(diagnosticCheck({
      id: "sessions.target.exists",
      scope: "sessions",
      status: "fail",
      summary: `Target session ${target} is not registered`,
      verification: "registry",
      errorCode: "EUNKNOWNTARGET",
    }));
  }

  const byThread = new Map();
  for (const record of sessions.records) {
    const previous = byThread.get(record.threadId);
    if (previous) {
      checks.push(diagnosticCheck({
        id: `sessions.thread.${record.threadId.slice(0, 8)}.duplicate`,
        scope: "sessions",
        status: "fail",
        summary: "Multiple session names reference the same Codex thread",
        verification: "registry",
        errorCode: "EDUPLICATETHREAD",
      }));
    }
    byThread.set(record.threadId, record.name);
  }

  return {
    checks,
    allSessions: sessions.records,
    sessions: target
      ? sessions.records.filter((record) => record.name === target)
      : sessions.records,
    attachments: target
      ? attachments.records.filter((record) => record.name === target)
      : attachments.records,
    jobs: target
      ? jobs.records.filter((record) => record.target === target || record.from === target)
      : jobs.records,
    bridges: target
      ? bridges.records.filter((record) => record.target === target)
      : bridges.records,
  };
}

export function inspectMessageBodies({
  stateDir,
  quotaBytes = MESSAGE_BODY_STORE_QUOTA_BYTES,
  segmentBytes = MESSAGE_BODY_SEGMENT_BYTES,
} = {}) {
  const scope = "message-bodies";
  const root = path.join(stateDir, "message-bodies");
  const rootEvidence = secureMetadata(root, "directory");
  if (rootEvidence.status === "missing") {
    return [
      metadataFinding(
        "message-bodies.directory",
        scope,
        "Message Body Store directory",
        rootEvidence,
        { required: false },
      ),
    ];
  }
  const checks = [
    metadataFinding(
      "message-bodies.directory",
      scope,
      "Message Body Store directory",
      rootEvidence,
    ),
  ];
  if (rootEvidence.status !== "secure") return checks;

  const collections = [
    {
      name: "segments",
      pattern: /^segment-\d{8}\.jsonl$/,
      partial: false,
    },
    {
      name: "quarantine",
      pattern: /^segment-\d{8}\.partial-[0-9a-f-]+\.jsonl$/i,
      partial: true,
    },
  ];
  let totalBytes = 0;
  let partialSegments = 0;
  for (const collection of collections) {
    const directory = path.join(root, collection.name);
    const evidence = secureMetadata(directory, "directory");
    checks.push(
      metadataFinding(
        `message-bodies.${collection.name}.directory`,
        scope,
        `Message Body Store ${collection.name} directory`,
        evidence,
      ),
    );
    if (evidence.status !== "secure") continue;
    let names;
    try {
      names = readdirSync(directory).sort();
    } catch (error) {
      checks.push(
        diagnosticCheck({
          id: `message-bodies.${collection.name}.enumeration`,
          scope,
          status: "unknown",
          summary: `Message Body Store ${collection.name} could not be enumerated`,
          verification: "unavailable",
          errorCode: errorCode(error),
        }),
      );
      continue;
    }
    const invalidNames = names.filter((name) => !collection.pattern.test(name));
    if (invalidNames.length > 0) {
      checks.push(
        diagnosticCheck({
          id: `message-bodies.${collection.name}.names`,
          scope,
          status: "fail",
          summary: `Message Body Store ${collection.name} contains unexpected entries`,
          verification: "metadata",
          errorCode: "EMESSAGEBODYNAME",
        }),
      );
    }
    for (const name of names.filter((candidate) => collection.pattern.test(candidate))) {
      const fileEvidence = secureMetadata(path.join(directory, name), "file");
      const label = safeLabel(name);
      if (fileEvidence.status !== "secure") {
        checks.push(
          metadataFinding(
            `message-bodies.${collection.name}.${label}.metadata`,
            scope,
            `Message Body Store ${collection.name} segment`,
            fileEvidence,
          ),
        );
        continue;
      }
      totalBytes += fileEvidence.metadata.size;
      if (fileEvidence.metadata.size > segmentBytes) {
        checks.push(
          diagnosticCheck({
            id: `message-bodies.${collection.name}.${label}.size`,
            scope,
            status: "fail",
            summary: `Message Body Store ${collection.name} segment exceeds its size bound`,
            verification: "metadata",
            errorCode: "EMESSAGEBODYSEGMENT",
          }),
        );
      }
      if (collection.partial) partialSegments += 1;
    }
  }

  checks.push(
    diagnosticCheck({
      id: "message-bodies.quarantine.count",
      scope,
      status: partialSegments > 0 ? "warn" : "pass",
      summary:
        partialSegments > 0
          ? `${partialSegments} quarantined Message Body Store segment(s) require retained operator review`
          : "No partial Message Body Store segments are quarantined",
      verification: "metadata",
      errorCode: partialSegments > 0 ? "EMESSAGEBODYPARTIAL" : null,
      remediation:
        partialSegments > 0
          ? "Retain quarantined segments until an explicit audited purge policy exists"
          : null,
      required: false,
    }),
  );
  const quotaRatio = quotaBytes > 0 ? totalBytes / quotaBytes : Infinity;
  checks.push(
    diagnosticCheck({
      id: "message-bodies.quota.usage",
      scope,
      status: quotaRatio >= 0.9 ? "warn" : "pass",
      summary:
        quotaRatio > 1
          ? "Message Body Store exceeds its configured write quota"
          : quotaRatio >= 0.9
            ? "Message Body Store is at least 90 percent of its configured write quota"
            : "Message Body Store is below 90 percent of its configured write quota",
      verification: "metadata",
      errorCode:
        quotaRatio > 1
          ? "EMESSAGEBODYQUOTA"
          : quotaRatio >= 0.9
            ? "EMESSAGEBODYQUOTANEAR"
            : null,
      remediation:
        quotaRatio >= 0.9
          ? "Do not delete segments manually; define and run an explicit audited purge after retained references are accounted for"
          : null,
      required: false,
    }),
  );
  return checks;
}

export function inspectJobs(jobs, { now = Date.now(), processStateFn = processState } = {}) {
  const checks = [];
  let active = 0;
  let legacy = 0;
  for (const job of jobs) {
    const kind = job.kind || "delegation";
    if (!job.kind) legacy += 1;
    const label = job.jobId.slice(0, 8);
    if (["failed", "missing"].includes(job.resultObservation?.status)) {
      checks.push(diagnosticCheck({
        id: `jobs.execution.${label}.result-observation`,
        scope: "jobs",
        status: "warn",
        summary: `Delegation ${label} completed but its final result could not be observed`,
        verification: "record:thread-items",
        errorCode: job.resultObservation.errorCode || "ERESULTOBSERVATION",
        observedBytes: job.resultObservation.observedBytes ?? null,
        limitBytes: job.resultObservation.limitBytes ?? null,
        remediation:
          "Treat the terminal turn as durable execution evidence; inspect the retained thread result separately and do not rerun automatically",
        required: false,
      }));
    }
    if (job.mirrorDelivery?.status === "failed") {
      checks.push(diagnosticCheck({
        id: `jobs.delivery.${label}.mirror`,
        scope: "jobs",
        status: "warn",
        summary: `Delegation ${label} is terminal but its optional peer mirror failed`,
        verification: "record",
        errorCode: job.mirrorDelivery.errorCode || "EPEERDELIVERY",
        remediation:
          "Treat the Job result as durable; inspect or retry coordination delivery separately",
        required: false,
      }));
    }
    if (
      kind === "delegation" &&
      ["failed", "unknown"].includes(job.status) &&
      typeof job.failureStage === "string"
    ) {
      const turnEvidence = job.modelTurnStarted;
      checks.push(diagnosticCheck({
        id: `jobs.execution.${label}.failure`,
        scope: "jobs",
        status: "warn",
        summary:
          turnEvidence === true
            ? `Delegation ${label} failed after positive model-turn start evidence`
            : turnEvidence === false
              ? `Delegation ${label} failed before any model turn started`
              : `Delegation ${label} failed with unverified model-turn acceptance`,
        verification: `record:${job.failureStage}`,
        errorCode: job.failureCode || "EDELEGATIONWORKER",
        observedBytes: job.failureEvidence?.observedBytes ?? null,
        limitBytes: job.failureEvidence?.limitBytes ?? null,
        remediation:
          job.rerouteGuidance ||
          (turnEvidence === null
            ? "Do not retry or reroute automatically while turn acceptance is unverified"
            : turnEvidence === false
              ? "Resolve the recorded transport or preflight failure before using a new Job ID"
              : "Inspect the retained turn and Job result evidence before any follow-up"),
        required: false,
      }));
    }
    if (
      kind === "claude-request" &&
      !PENDING_JOB_STATES.has(job.status) &&
      ["pending", "failed"].includes(job.reply?.status)
    ) {
      checks.push(diagnosticCheck({
        id: `jobs.delivery.${label}.claude-reply`,
        scope: "jobs",
        status: "warn",
        summary: `Claude request ${label} is terminal but its peer response was not delivered`,
        verification: "record",
        errorCode: job.reply?.errorCode || "EPEERDELIVERY",
        remediation:
          "Keep the durable Job result and retry only the correlated Claude response delivery",
        required: false,
      }));
    }
    if (
      !PENDING_JOB_STATES.has(job.status) &&
      !(kind === "claude-delivery" &&
        OBSERVABLE_CLAUDE_DELIVERY_STATES.has(job.status))
    ) {
      continue;
    }
    active += 1;
    if (kind === "delegation") {
      if (job.status === "scheduled") {
        const expired = Date.parse(job.schedule?.expiresAt || "") <= now;
        const claimExpired =
          !expired &&
          job.schedule?.claim &&
          Date.parse(job.schedule.claim.leaseUntil) <= now;
        checks.push(diagnosticCheck({
          id: `jobs.delegation.${label}.schedule`,
          scope: "jobs",
          status: expired || claimExpired ? "warn" : "pass",
          summary: expired
            ? `Scheduled Delegation ${label} is past its expiry`
            : claimExpired
              ? `Scheduled Delegation ${label} has an expired dispatch claim`
              : `Scheduled Delegation ${label} has valid queued metadata`,
          verification: "record",
          errorCode: expired
            ? "EDELEGATIONEXPIRED"
            : claimExpired
              ? "EDELEGATIONCLAIMEXPIRED"
              : null,
          remediation:
            expired || claimExpired
              ? "Keep the Scheduler running so it can reconcile the retained Job"
              : null,
          required: false,
        }));
        continue;
      }
      if (!Number.isSafeInteger(job.workerPid)) {
        const age = now - Date.parse(job.createdAt);
        checks.push(diagnosticCheck({
          id: `jobs.worker.${label}.missing`,
          scope: "jobs",
          status: Number.isFinite(age) && age <= 10_000 ? "warn" : "fail",
          summary: `Pending Delegation ${label} has no registered worker`,
          verification: "record",
          errorCode: "EWORKERMISSING",
          remediation: "Inspect the correlated Job from an allowed host context",
        }));
        continue;
      }
      const state = processStateFn(job.workerPid);
      checks.push(diagnosticCheck({
        id: `jobs.worker.${label}.process`,
        scope: "jobs",
        status: state === "alive" ? "pass" : state === "unverified" ? "unknown" : "fail",
        summary:
          state === "alive"
            ? `Pending Delegation ${label} has a live worker`
            : state === "unverified"
              ? `Pending Delegation ${label} worker cannot be verified by this caller`
              : `Pending Delegation ${label} worker is missing`,
        verification: state,
        errorCode: state === "alive" ? null : state === "unverified" ? "EPERM" : "ESRCH",
      }));
    }
    if (
      kind === "claude-delivery" &&
      job.status === "transport_delivered" &&
      Date.parse(job.delivery?.ackDeadlineAt) <= now
    ) {
      checks.push(diagnosticCheck({
        id: `jobs.claude.${label}.ack-overdue`,
        scope: "jobs",
        status: "warn",
        summary: `Claude Delivery ${label} is past its ACK deadline without reconciliation`,
        verification: "record",
        errorCode: "EACKOVERDUE",
      }));
    }
    if (kind === "claude-delivery" && job.status === "acknowledged") {
      const completionDeadline = Date.parse(
        job.delivery?.completionDeadlineAt || "",
      );
      if (!Number.isFinite(completionDeadline)) {
        checks.push(diagnosticCheck({
          id: `jobs.claude.${label}.completion-deadline`,
          scope: "jobs",
          status: "fail",
          summary: `Acknowledged Claude Delivery ${label} has no valid completion deadline`,
          verification: "record",
          errorCode: "ECOMPLETIONDEADLINE",
        }));
      } else if (completionDeadline <= now) {
        checks.push(diagnosticCheck({
          id: `jobs.claude.${label}.completion-overdue`,
          scope: "jobs",
          status: "warn",
          summary: `Acknowledged Claude Delivery ${label} is past its completion deadline`,
          verification: "record",
          errorCode: "ECOMPLETIONOVERDUE",
        }));
      }
    }
    if (
      kind === "claude-delivery" &&
      ["ack_timeout", "completion_timeout"].includes(job.status) &&
      job.wake?.status === "delivered"
    ) {
      checks.push(diagnosticCheck({
        id: `jobs.claude.${label}.reconciliation`,
        scope: "jobs",
        status: "fail",
        summary: `Claude Delivery ${label} has a delivered reply wake without terminal Delivery reconciliation`,
        verification: "record",
        errorCode: "EUNRECONCILEDREPLY",
      }));
    }
  }
  if (legacy > 0) {
    checks.push(diagnosticCheck({
      id: "jobs.records.legacy-kind",
      scope: "jobs",
      status: "warn",
      summary: `${legacy} legacy Job record(s) rely on the implicit Delegation kind`,
      verification: "schema-compatible",
      errorCode: "ELEGACYJOB",
      remediation:
        "Run cxmsg repair plan jobs.records.legacy-kind, confirm its exact digest, and repeat one-record Repair until the finding clears",
    }));
  }
  if (checks.length === 0) {
    checks.push(diagnosticCheck({
      id: "jobs.nonterminal.consistency",
      scope: "jobs",
      status: "pass",
      summary: `${active} nonterminal Job(s) have no locally provable inconsistency`,
      verification: "record",
    }));
  }
  return checks;
}

export function inspectAttachments(attachments, { processStateFn = processState } = {}) {
  if (attachments.length === 0) {
    return [diagnosticCheck({
      id: "attachments.process.consistency",
      scope: "attachments",
      status: "pass",
      summary: "No active TUI attachment records require inspection",
      verification: "record",
      required: false,
    })];
  }
  return attachments.map((record) => {
    const state = processStateFn(record.childPid);
    return diagnosticCheck({
      id: `attachments.${safeLabel(record.name)}.process`,
      scope: "attachments",
      status: state === "alive" ? "pass" : state === "unverified" ? "unknown" : "warn",
      summary:
        state === "alive"
          ? `TUI attachment ${record.name} has a live process`
          : state === "unverified"
            ? `TUI attachment ${record.name} cannot be verified by this caller`
            : `TUI attachment ${record.name} has a missing process`,
      verification: state,
      errorCode: state === "alive" ? null : state === "unverified" ? "EPERM" : "ESRCH",
      required: false,
    });
  });
}

export async function inspectPermissions(
  sessions,
  jobs,
  {
    deep = false,
    scopeSessions = sessions,
    withServer = withAppServer,
    socketPath,
  } = {},
) {
  const checks = [];
  const sessionNames = new Set(sessions.map((record) => record.name));
  let delegationEdges = 0;
  let claudeGrants = 0;
  let invalidReferences = 0;
  const requestedProfiles = new Map();

  for (const record of scopeSessions) {
    for (const sender of record.allowedDelegators || []) {
      delegationEdges += 1;
      if (!SESSION_PATTERN.test(sender) || !sessionNames.has(sender)) invalidReferences += 1;
    }
    for (const grant of record.allowedClaudeRequesters || []) {
      claudeGrants += 1;
      if (grant.permissions) {
        const profiles = requestedProfiles.get(record.cwd) || new Set();
        profiles.add(grant.permissions);
        requestedProfiles.set(record.cwd, profiles);
      }
    }
  }
  for (const job of jobs) {
    if (!job.permissions) continue;
    const target = sessions.find((record) => record.name === job.target);
    if (!target) continue;
    const profiles = requestedProfiles.get(target.cwd) || new Set();
    profiles.add(job.permissions);
    requestedProfiles.set(target.cwd, profiles);
  }

  checks.push(diagnosticCheck({
    id: "permissions.grants.references",
    scope: "permissions",
    status: invalidReferences === 0 ? "pass" : "fail",
    summary:
      invalidReferences === 0
        ? `${delegationEdges} Delegation relationship(s) and ${claudeGrants} Claude grant(s) have valid local references`
        : `${invalidReferences} Delegation relationship reference(s) do not resolve to registered sessions`,
    verification: "registry",
    errorCode: invalidReferences === 0 ? null : "EGRANTTARGET",
  }));

  if (!deep) {
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: "skipped",
      summary: "Permission profile resolution requires --deep",
      verification: "not-requested",
      required: false,
    }));
    return checks;
  }
  if (requestedProfiles.size === 0) {
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: "pass",
      summary: "No stored permission profile references require resolution",
      verification: "registry",
    }));
    return checks;
  }

  let missing = 0;
  let blocked = 0;
  try {
    await withServer(async (client) => {
      for (const [cwd, requested] of requestedProfiles) {
        const result = await client.request("permissionProfile/list", { cwd });
        const available = new Map((result.data || []).map((profile) => [profile.id, profile]));
        for (const profileId of requested) {
          const profile = available.get(profileId);
          if (!profile) missing += 1;
          else if (!profile.allowed) blocked += 1;
        }
      }
    }, { socketPath });
    const invalid = missing + blocked;
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: invalid === 0 ? "pass" : "fail",
      summary:
        invalid === 0
          ? "All stored permission profile references are available and allowed"
          : `${missing} stored profile reference(s) are missing and ${blocked} are blocked`,
      verification: "app-server",
      errorCode: invalid === 0 ? null : "EPERMISSIONPROFILE",
      remediation: invalid === 0 ? null : "Review the stored grant or Job permission profile; Doctor will not change it",
    }));
  } catch (error) {
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: ["EPERM", "EACCES", "ETIMEDOUT"].includes(error?.code) ? "unknown" : "fail",
      summary: "Stored permission profile references could not be resolved",
      verification: ["EPERM", "EACCES"].includes(error?.code) ? "sandbox-denied" : "unavailable",
      errorCode: errorCode(error),
      remediation: "Run doctor --deep from an allowed host context",
    }));
  }
  return checks;
}

function readManagedPid(pidPath) {
  const evidence = secureMetadata(pidPath, "file");
  if (evidence.status !== "secure") return { pid: null, evidence };
  if (evidence.metadata.size > 128) {
    return { pid: null, evidence: { status: "invalid" } };
  }
  try {
    const raw = readFileSync(pidPath, "utf8").trim();
    const pid = Number(raw);
    return {
      pid: Number.isSafeInteger(pid) && pid > 1 ? pid : null,
      evidence: Number.isSafeInteger(pid) && pid > 1 ? evidence : { status: "invalid" },
    };
  } catch (error) {
    return { pid: null, evidence: { status: "unavailable", errorCode: errorCode(error) } };
  }
}

export async function inspectAppServer({
  pidPath,
  socketPath,
  deep = false,
  processStateFn = processState,
  processIdentityFn = processIdentity,
  probe = probeAppServerSocket,
  codexBin = process.env.CODEX_BIN || "codex",
  run = spawnSync,
} = {}) {
  const checks = [];
  const managed = readManagedPid(pidPath);
  checks.push(metadataFinding("app-server.pid.record", "app-server", "App Server PID record", managed.evidence, { required: false }));
  const socketEvidence = secureMetadata(socketPath, "socket");
  checks.push(metadataFinding("app-server.socket.metadata", "app-server", "App Server socket", socketEvidence));
  const processEvidence = processStateFn(managed.pid);
  const identity = managed.pid
    ? processIdentityFn(managed.pid, ["app-server", socketPath]).state
    : "unavailable";
  const probeResult = deep
    ? await probe(socketPath)
    : failedProbe(Object.assign(new Error("deep handshake not requested"), { code: "ESKIPPED" }));
  const evidence = deep
    ? serviceEvidence({
        process: processEvidence,
        identity,
        socketProbe: probeResult,
        socketPresent: socketEvidence.status === "secure",
      })
    : null;
  let status;
  let verification;
  let summary;
  let code = null;
  if (deep) {
    status = evidence.status === "running" ? "pass" : evidence.status === "stopped" || evidence.status === "stale" || evidence.status === "mismatched" ? "fail" : "unknown";
    verification = probeResult.state === "denied" ? "sandbox-denied" : probeResult.state;
    summary =
      evidence.status === "running"
        ? "App Server identity handshake succeeded"
        : evidence.status === "unreachable"
          ? "App Server socket exists but this caller cannot connect"
          : `App Server evidence reports ${evidence.status}`;
    code = probeResult.errorCode;
  } else if (socketEvidence.status === "secure" && processEvidence === "alive" && identity === "matched") {
    status = "pass";
    verification = "identity";
    summary = "App Server has matching passive process and socket evidence";
  } else if (socketEvidence.status === "missing" && processEvidence === "missing") {
    status = "fail";
    verification = "missing";
    summary = "App Server process and socket are absent";
    code = "ENOENT";
  } else {
    status = "unknown";
    verification = processEvidence === "unverified" ? "sandbox-denied" : "passive";
    summary = "App Server passive evidence is incomplete; use --deep for a handshake";
    code = processEvidence === "unverified" ? "EPERM" : null;
  }
  checks.push(diagnosticCheck({
    id: deep ? "app-server.socket.connect" : "app-server.passive.evidence",
    scope: "app-server",
    status,
    summary,
    verification,
    errorCode: code,
    remediation: status === "unknown" ? "Run doctor --deep from an allowed host context" : null,
  }));
  if (deep && evidence.status === "running") {
    const cliResult = run(codexBin, ["--version"], { encoding: "utf8" });
    const cliVersion =
      cliResult.status === 0 ? semanticVersion(cliResult.stdout) : null;
    const serverVersion = probeResult.appServerVersion || null;
    const versionsKnown = Boolean(cliVersion && serverVersion);
    const versionsMatch = versionsKnown && cliVersion === serverVersion;
    checks.push(
      diagnosticCheck({
        id: "app-server.version.compatibility",
        scope: "app-server",
        status: versionsMatch ? "pass" : "warn",
        summary: !versionsKnown
          ? "App Server and configured Codex CLI versions could not both be identified"
          : versionsMatch
            ? "App Server and configured Codex CLI versions match"
            : "App Server and configured Codex CLI versions differ",
        verification: versionsKnown ? "handshake" : "unavailable",
        errorCode: versionsMatch
          ? null
          : versionsKnown
            ? "EAPPSERVERVERSIONMISMATCH"
            : "EAPPSERVERVERSIONUNKNOWN",
        remediation: versionsMatch
          ? null
          : versionsKnown
            ? "Restart the App Server from the intended Codex installation, then rerun Doctor; Doctor will not restart it"
            : "Run doctor --deep where the configured Codex executable and App Server handshake are both available",
        required: false,
      }),
    );
  }
  return checks;
}

export async function inspectBridges(
  bridges,
  sessions,
  {
    deep = false,
    processStateFn = processState,
    processIdentityFn = processIdentity,
    probe = probeClaudeBridge,
    workerFragment = "claude-bridge-worker.js",
    currentRevision = CLAUDE_BRIDGE_IMPLEMENTATION_REVISION,
  } = {},
) {
  if (bridges.length === 0) {
    return [diagnosticCheck({
      id: "bridges.availability",
      scope: "bridges",
      status: "skipped",
      summary: "No Claude bridge records are registered",
      verification: "registry",
      required: false,
    })];
  }
  const sessionsByName = new Map(sessions.map((record) => [record.name, record]));
  const checks = [];
  for (const record of bridges) {
    const label = safeLabel(record.target);
    const recordedRevision = record.implementationRevision;
    checks.push(
      implementationRevisionCheck({
        id: `bridges.${label}.implementation`,
        scope: "bridges",
        label: `Claude bridge ${record.target}`,
        recordedRevision,
        currentRevision,
        unknownCode: "EBRIDGEVERSIONUNKNOWN",
        staleCode: "EBRIDGESTALECODE",
        remediation:
          "Restart this bridge from an allowed host context, then rerun doctor; Doctor will not restart it",
      }),
    );
    const session = sessionsByName.get(record.target);
    if (!session || session.threadId !== record.targetThreadId) {
      checks.push(diagnosticCheck({
        id: `bridges.${label}.target`,
        scope: "bridges",
        status: "fail",
        summary: `Claude bridge ${record.target} does not match its registered Codex target`,
        verification: "registry",
        errorCode: "EBRIDGETARGET",
      }));
      continue;
    }
    const processEvidence = processStateFn(record.pid);
    const identity = processIdentityFn(record.pid, [workerFragment, record.target]).state;
    if (!deep) {
      const socketEvidence = secureMetadata(record.socketPath, "socket");
      const ok = processEvidence === "alive" && identity === "matched" && socketEvidence.status === "secure";
      checks.push(diagnosticCheck({
        id: `bridges.${label}.passive`,
        scope: "bridges",
        status: ok ? "pass" : processEvidence === "missing" || identity === "mismatched" ? "fail" : "unknown",
        summary: ok ? `Claude bridge ${record.target} has matching passive evidence` : `Claude bridge ${record.target} passive evidence is incomplete`,
        verification: ok ? "identity" : processEvidence === "unverified" ? "sandbox-denied" : "passive",
        errorCode: processEvidence === "unverified" ? "EPERM" : null,
      }));
      continue;
    }
    const socketProbe = await probe(record);
    const evidence = serviceEvidence({
      process: processEvidence,
      identity,
      socketProbe,
      socketPresent: existsSync(record.socketPath),
    });
    checks.push(diagnosticCheck({
      id: `bridges.${label}.handshake`,
      scope: "bridges",
      status: evidence.status === "running" ? "pass" : ["stopped", "stale", "mismatched"].includes(evidence.status) ? "fail" : "unknown",
      summary: evidence.status === "running" ? `Claude bridge ${record.target} identity handshake succeeded` : `Claude bridge ${record.target} reports ${evidence.status}`,
      verification: socketProbe.state === "denied" ? "sandbox-denied" : socketProbe.state,
      errorCode: socketProbe.errorCode,
      remediation: evidence.status === "unreachable" ? "Run doctor from an allowed host context" : null,
    }));
  }
  return checks;
}

export async function inspectRelay({
  recordPath,
  deep = false,
  processStateFn = processState,
  processIdentityFn = processIdentity,
  request = hostRelayRequest,
  workerFragment = "host-relay-worker.js",
  currentRevision = CXMSG_IMPLEMENTATION_REVISIONS.hostRelay,
} = {}) {
  const evidence = secureMetadata(recordPath, "file");
  if (evidence.status === "missing") {
    return [metadataFinding("relay.record.metadata", "relay", "Host relay record", evidence, { required: false })];
  }
  const checks = [metadataFinding("relay.record.metadata", "relay", "Host relay record", evidence)];
  if (evidence.status !== "secure" || evidence.metadata.size > MAX_RECORD_BYTES) return checks;
  let record;
  try {
    record = JSON.parse(readFileSync(recordPath, "utf8"));
  } catch {
    checks.push(diagnosticCheck({ id: "relay.record.schema", scope: "relay", status: "fail", summary: "Host relay record is invalid bounded JSON", verification: "parse", errorCode: "EJSON" }));
    return checks;
  }
  const valid = Boolean(
    record?.version === 1 &&
      Number.isSafeInteger(record.pid) &&
      record.pid > 1 &&
      Number.isInteger(record.port) &&
      record.port > 0 &&
      record.port <= 65_535 &&
      typeof record.token === "string" &&
      record.token.length >= 16 &&
      Number.isSafeInteger(record.startedAt) &&
      (record.cxmsgVersion === undefined ||
        (typeof record.cxmsgVersion === "string" &&
          record.cxmsgVersion.length >= 1 &&
          record.cxmsgVersion.length <= 64)) &&
      (record.implementationRevision === undefined ||
        (Number.isSafeInteger(record.implementationRevision) &&
          record.implementationRevision >= 1)),
  );
  if (!valid) {
    checks.push(diagnosticCheck({ id: "relay.record.schema", scope: "relay", status: "fail", summary: "Host relay record schema is invalid", verification: "schema", errorCode: "ERELAYSCHEMA" }));
    return checks;
  }
  checks.push(
    implementationRevisionCheck({
      id: "relay.implementation",
      scope: "relay",
      label: "Host relay",
      recordedRevision: record.implementationRevision,
      currentRevision,
      unknownCode: "ERELAYVERSIONUNKNOWN",
      staleCode: "ERELAYSTALECODE",
      remediation:
        "Restart the host relay from an allowed host context, then rerun Doctor; Doctor will not restart it",
    }),
  );
  const state = processStateFn(record.pid);
  const identity = processIdentityFn(record.pid, [workerFragment, String(record.port)]).state;
  if (!deep) {
    const ok = state === "alive" && identity === "matched";
    checks.push(diagnosticCheck({
      id: "relay.passive.evidence",
      scope: "relay",
      status: ok ? "pass" : state === "missing" || identity === "mismatched" ? "fail" : "unknown",
      summary: ok ? "Host relay has matching passive process evidence" : "Host relay passive evidence is incomplete",
      verification: ok ? "identity" : state === "unverified" ? "sandbox-denied" : "passive",
      errorCode: state === "unverified" ? "EPERM" : null,
    }));
    return checks;
  }
  try {
    const health = await request("/health", { record });
    const matched =
      health.pid === record.pid &&
      health.port === record.port &&
      health.startedAt === record.startedAt &&
      (record.cxmsgVersion === undefined ||
        health.cxmsgVersion === record.cxmsgVersion) &&
      (record.implementationRevision === undefined ||
        health.implementationRevision === record.implementationRevision);
    checks.push(diagnosticCheck({
      id: "relay.health.connect",
      scope: "relay",
      status: matched ? "pass" : "fail",
      summary: matched ? "Host relay identity handshake succeeded" : "Host relay identity handshake mismatched its record",
      verification: matched ? "handshake" : "mismatched",
      errorCode: matched ? null : "EIDENTITY",
    }));
  } catch (error) {
    const denied = ["EPERM", "EACCES", "ETIMEDOUT"].includes(error?.code);
    checks.push(diagnosticCheck({
      id: "relay.health.connect",
      scope: "relay",
      status: denied && state !== "missing" ? "unknown" : "fail",
      summary: denied ? "Host relay exists but this caller cannot connect" : "Host relay health handshake failed",
      verification: denied ? "sandbox-denied" : "unavailable",
      errorCode: errorCode(error),
      remediation: denied ? "Run doctor from an allowed host context" : null,
    }));
  }
  return checks;
}

export async function inspectRegisteredThreads(
  sessions,
  { deep = false, withServer = withAppServer, socketPath } = {},
) {
  if (!deep) {
    return [diagnosticCheck({
      id: "sessions.thread.resolve",
      scope: "sessions",
      status: "skipped",
      summary: "Thread resolution requires --deep",
      verification: "not-requested",
      required: false,
    })];
  }
  if (sessions.length === 0) {
    return [diagnosticCheck({
      id: "sessions.thread.resolve",
      scope: "sessions",
      status: "skipped",
      summary: "No registered Codex threads require resolution",
      verification: "registry",
      required: false,
    })];
  }
  const checks = [];
  try {
    await withServer(async (client) => {
      for (const record of sessions) {
        try {
          const thread = await readThreadMetadata(client, record.threadId);
          checks.push(diagnosticCheck({
            id: `sessions.${safeLabel(record.name)}.thread`,
            scope: "sessions",
            status: thread?.id === record.threadId ? "pass" : "fail",
            summary: thread?.id === record.threadId ? `Session ${record.name} resolves through metadata-only thread/read` : `Session ${record.name} resolved to a mismatched thread`,
            verification: "app-server",
            errorCode: thread?.id === record.threadId ? null : "EIDENTITY",
          }));
        } catch (error) {
          checks.push(diagnosticCheck({
            id: `sessions.${safeLabel(record.name)}.thread`,
            scope: "sessions",
            status: "fail",
            summary: `Session ${record.name} does not resolve through metadata-only thread/read`,
            verification: "app-server",
            errorCode: errorCode(error, "ETHREADREAD"),
          }));
        }
      }
    }, { socketPath });
  } catch (error) {
    checks.push(diagnosticCheck({
      id: "sessions.thread.connect",
      scope: "sessions",
      status: ["EPERM", "EACCES", "ETIMEDOUT"].includes(error?.code) ? "unknown" : "fail",
      summary: "Registered threads could not be inspected through the App Server",
      verification: ["EPERM", "EACCES"].includes(error?.code) ? "sandbox-denied" : "unavailable",
      errorCode: errorCode(error),
      remediation: "Run doctor --deep from an allowed host context",
    }));
  }
  return checks;
}
