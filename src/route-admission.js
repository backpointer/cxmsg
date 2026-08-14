import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import {
  appendDeliveryEvidence,
  beginImmediateDelivery,
  commitSingleRecipientDelivery,
  readDeliveryLedger,
} from "./delivery-ledger.js";
import { withFileLock } from "./file-lock.js";
import {
  MAX_STORED_MESSAGE_BYTES,
  storeMessageBody,
} from "./message-bodies.js";
import { MAX_MESSAGE_BYTES } from "./messaging.js";
import {
  findProjectByRoutingId,
  nodeKey as directoryNodeKey,
  readExecutionThread,
  readNode,
  readNodeTombstone,
} from "./node-directory.js";
import { writeCoordinationEvent } from "./observability.js";
import { readSessionRecord } from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const ROUTE_BINDINGS_DIR = path.join(CXMSG_STATE_DIR, "route-bindings");
export const ROUTE_DELIVERIES_DIR = path.join(CXMSG_STATE_DIR, "route-deliveries");
export const QUARANTINE_DIR = path.join(CXMSG_STATE_DIR, "quarantine");
export const ROUTE_RECONCILE_GRACE_MS = 30_000;

const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SESSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function validateName(label, value) {
  if (!NAME_PATTERN.test(value || "")) {
    throw new Error(`${label} must be 1-128 safe identifier characters`);
  }
  return value;
}

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value;
}

function ensureDirectory(directory) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  chmodSync(directory, 0o700);
}

function atomicWrite(directory, filename, value) {
  ensureDirectory(directory);
  const destination = path.join(directory, filename);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, destination);
  return value;
}

function readJson(filename) {
  try {
    return JSON.parse(readFileSync(filename, "utf8"));
  } catch {
    return null;
  }
}

function sessionFilename(sessionName) {
  if (!SESSION_PATTERN.test(sessionName || "")) {
    throw new Error("session name must be a valid cxmsg session name");
  }
  return `${sessionName}.json`;
}

function bindingPath(sessionName) {
  return path.join(ROUTE_BINDINGS_DIR, sessionFilename(sessionName));
}

function messageFilename(messageId) {
  return `${validateUuid("logical message id", messageId)}.json`;
}

export function normalizeRoute(route, logicalMessageId = route?.logical_message_id) {
  if (!route) return null;
  if (route.schema_version !== undefined && route.schema_version !== 1) {
    throw new Error("route schema_version must be 1");
  }
  const normalized = {
    schema_version: 1,
    project_id: validateName("project_id", route.project_id),
    target_role: validateName("target_role", route.target_role),
    logical_message_id: validateUuid(
      "logical_message_id",
      logicalMessageId || route.logical_message_id,
    ),
    payload_type: validateName("payload_type", route.payload_type || "coordination"),
    wake_policy: route.wake_policy || "immediate",
  };
  if (normalized.wake_policy !== "immediate") {
    throw new Error("Phase 2.5 routed send supports only wake_policy=immediate");
  }
  if (route.task_id) normalized.task_id = validateName("task_id", route.task_id);
  if (route.sender_role) {
    normalized.sender_role = validateName("sender_role", route.sender_role);
  }
  if (route.expiry) {
    const expiry = new Date(route.expiry);
    if (!Number.isFinite(expiry.getTime())) throw new Error("expiry must be an ISO timestamp");
    normalized.expiry = expiry.toISOString();
  }
  return normalized;
}

export function writeRouteBinding({
  sessionName,
  threadId,
  projectId,
  projectKey = null,
  nodeKey = null,
  role,
}) {
  const now = new Date().toISOString();
  const previous = readRouteBinding(sessionName);
  const expectedNodeKey = directoryNodeKey("codex", threadId);
  if (nodeKey && nodeKey !== expectedNodeKey) {
    throw new Error("Node key does not match the bound Codex thread");
  }
  return atomicWrite(ROUTE_BINDINGS_DIR, sessionFilename(sessionName), {
    version: 1,
    sessionName,
    threadId: validateUuid("thread id", threadId),
    projectId: validateName("project id", projectId),
    ...(projectKey ? { projectKey: validateUuid("Project key", projectKey) } : {}),
    ...(nodeKey ? { nodeKey } : {}),
    role: validateName("role", role),
    boundAt: previous?.boundAt || now,
    updatedAt: now,
  });
}

function validRouteBindingRecord(record, sessionName) {
  return Boolean(
    record?.version === 1 &&
    record.sessionName === sessionName &&
    UUID_PATTERN.test(record.threadId || "") &&
    NAME_PATTERN.test(record.projectId || "") &&
    (record.projectKey === undefined || UUID_PATTERN.test(record.projectKey)) &&
    (record.nodeKey === undefined ||
      record.nodeKey === directoryNodeKey("codex", record.threadId)) &&
    NAME_PATTERN.test(record.role || ""),
  );
}

export function routeBindingState(sessionName) {
  const filename = bindingPath(sessionName);
  let metadata;
  try {
    metadata = lstatSync(filename);
  } catch (error) {
    if (error?.code === "ENOENT") return { state: "missing", record: null };
    return { state: "invalid", record: null, errorCode: error?.code || "EBINDINGSTAT" };
  }
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    return { state: "invalid", record: null, errorCode: "EBINDINGMETADATA" };
  }
  try {
    const record = JSON.parse(readFileSync(filename, "utf8"));
    if (!validRouteBindingRecord(record, sessionName)) {
      return { state: "invalid", record: null, errorCode: "EBINDINGSCHEMA" };
    }
    return { state: "valid", record };
  } catch {
    return { state: "invalid", record: null, errorCode: "EBINDINGJSON" };
  }
}

export function readRouteBinding(sessionName) {
  return routeBindingState(sessionName).record;
}

export function listRouteBindings() {
  if (!existsSync(ROUTE_BINDINGS_DIR)) return [];
  return readdirSync(ROUTE_BINDINGS_DIR)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .filter((name) => SESSION_PATTERN.test(name.slice(0, -5)))
    .map((name) => routeBindingState(name.slice(0, -5)))
    .filter((state) => state.state === "valid")
    .map((state) => state.record);
}

function admission(
  bindingState,
  route,
  senderBindingState,
  targetRecord,
  senderRecord,
  now = Date.now(),
) {
  if (bindingState.state === "invalid") {
    return { state: "quarantined", reason: "binding_invalid" };
  }
  const binding = bindingState.record;
  const senderBinding = senderBindingState.record;
  if (senderRecord && readExecutionThread(senderRecord.threadId)) {
    return { state: "quarantined", reason: "sender_execution_thread" };
  }
  if (targetRecord && readExecutionThread(targetRecord.threadId)) {
    return { state: "quarantined", reason: "target_execution_thread" };
  }
  if (senderRecord && readNodeTombstone("codex", senderRecord.threadId)) {
    return { state: "quarantined", reason: "sender_node_retired" };
  }
  if (targetRecord && readNodeTombstone("codex", targetRecord.threadId)) {
    return { state: "quarantined", reason: "target_node_retired" };
  }
  if (route?.sender_role) {
    if (senderBindingState.state === "invalid") {
      return { state: "quarantined", reason: "sender_binding_invalid" };
    }
    if (senderBindingState.state === "missing") {
      return { state: "quarantined", reason: "sender_unbound" };
    }
    if (!senderRecord || senderBinding.threadId !== senderRecord.threadId) {
      return { state: "quarantined", reason: "sender_identity_mismatch" };
    }
    if (
      senderBinding.nodeKey &&
      senderBinding.nodeKey !== directoryNodeKey("codex", senderRecord.threadId)
    ) {
      return { state: "quarantined", reason: "sender_node_mismatch" };
    }
    if (senderBinding.nodeKey) {
      if (readNodeTombstone("codex", senderRecord.threadId)) {
        return { state: "quarantined", reason: "sender_node_retired" };
      }
      const senderNode = readNode("codex", senderRecord.threadId);
      if (!senderNode) {
        return { state: "quarantined", reason: "sender_node_missing" };
      }
      if (
        senderBinding.projectKey &&
        senderNode.projectId !== senderBinding.projectKey
      ) {
        return { state: "quarantined", reason: "sender_project_identity_mismatch" };
      }
    }
    if (senderBinding.projectId !== route.project_id) {
      return { state: "quarantined", reason: "sender_project_mismatch" };
    }
    if (route.sender_role !== senderBinding.role) {
      return { state: "quarantined", reason: "sender_role_mismatch" };
    }
  }
  if (bindingState.state === "missing") {
    return { state: "admitted", reason: "legacy-unbound" };
  }
  if (targetRecord && binding.threadId !== targetRecord.threadId) {
    return { state: "quarantined", reason: "target_identity_mismatch" };
  }
  if (
    targetRecord &&
    binding.nodeKey &&
    binding.nodeKey !== directoryNodeKey("codex", targetRecord.threadId)
  ) {
    return { state: "quarantined", reason: "target_node_mismatch" };
  }
  if (targetRecord && binding.nodeKey) {
    const targetNode = readNode("codex", targetRecord.threadId);
    if (!targetNode) {
      return { state: "quarantined", reason: "target_node_missing" };
    }
    if (binding.projectKey && targetNode.projectId !== binding.projectKey) {
      return { state: "quarantined", reason: "project_identity_mismatch" };
    }
  }
  if (!route) return { state: "quarantined", reason: "missing_route" };
  if (route.project_id !== binding.projectId) {
    return { state: "quarantined", reason: "project_mismatch" };
  }
  if (route.target_role !== binding.role) {
    return { state: "quarantined", reason: "role_mismatch" };
  }
  if (binding.projectKey) {
    const project = findProjectByRoutingId(route.project_id);
    if (!project) return { state: "quarantined", reason: "project_identity_missing" };
    if (project.projectId !== binding.projectKey) {
      return { state: "quarantined", reason: "project_identity_mismatch" };
    }
  }
  if (route.expiry && Date.parse(route.expiry) <= now) {
    return { state: "quarantined", reason: "expired" };
  }
  return { state: "admitted", reason: "binding_match" };
}

function deliveryPath(messageId) {
  return path.join(ROUTE_DELIVERIES_DIR, messageFilename(messageId));
}

function deliveryLockPath(messageId) {
  return path.join(ROUTE_DELIVERIES_DIR, `${validateUuid("logical message id", messageId)}.lock`);
}

function validLegacyRouteDeliveryRecord(record, messageId) {
  return Boolean(
    record?.version === 1 &&
      record.logicalMessageId === messageId &&
      UUID_PATTERN.test(record.logicalMessageId || "") &&
      NAME_PATTERN.test(record.from || "") &&
      NAME_PATTERN.test(record.target || "") &&
      (record.targetThreadId === undefined ||
        UUID_PATTERN.test(record.targetThreadId || "")) &&
      Number.isSafeInteger(record.messageBytes) &&
      record.messageBytes > 0 &&
      /^[0-9a-f]{64}$/.test(record.messageSha256 || "") &&
      record.routeFingerprint === routeFingerprint(record.route ?? null) &&
      ["admitted", "quarantined"].includes(record.admissionState) &&
      ["dispatching", "turn_started", "unknown", "quarantined"].includes(
        record.status,
      ) &&
      Number.isFinite(Date.parse(record.createdAt || "")) &&
      Number.isFinite(Date.parse(record.updatedAt || "")),
  );
}

function ledgerRouteDelivery(record) {
  if (!record) return null;
  const message = record.logicalMessage;
  const delivery = record.delivery;
  const activeAttempt = delivery.attempts.at(-1) || null;
  const status =
    delivery.admissionState === "quarantined"
      ? "quarantined"
      : delivery.state === "created" && activeAttempt
        ? "dispatching"
        : delivery.state;
  return {
    version: 2,
    logicalMessageId: message.messageId,
    deliveryId: delivery.deliveryId,
    from: message.from,
    target: delivery.target,
    ...(delivery.targetThreadId
      ? { targetThreadId: delivery.targetThreadId }
      : {}),
    messageBytes: message.body.bytes,
    messageSha256: message.body.sha256,
    contentRef: message.body.contentRef,
    routeFingerprint: message.routeFingerprint,
    route: message.route,
    admissionState: delivery.admissionState,
    admissionReason: delivery.admissionReason,
    status,
    wakeAttemptedAt: delivery.attempts[0]?.startedAt || null,
    createdAt: message.createdAt,
    updatedAt: delivery.updatedAt,
    turnId: delivery.turnId || null,
    delivery: delivery.transportResult || null,
    errorCode: delivery.errorCode || null,
    attemptId: activeAttempt?.attemptId || null,
    attemptCount: delivery.attempts.length,
  };
}

export function routeDeliveryState(messageId) {
  try {
    const ledgerRecord = readDeliveryLedger(messageId);
    if (ledgerRecord) {
      return { state: "valid", record: ledgerRouteDelivery(ledgerRecord) };
    }
  } catch (error) {
    return {
      state: "invalid",
      record: null,
      errorCode: error?.code || "ELEDGERVALIDATION",
    };
  }
  const filename = deliveryPath(messageId);
  let metadata;
  try {
    metadata = lstatSync(filename);
  } catch (error) {
    if (error?.code === "ENOENT") return { state: "missing", record: null };
    return {
      state: "invalid",
      record: null,
      errorCode: error?.code || "EDELIVERYSTAT",
    };
  }
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    metadata.size > MAX_STORED_MESSAGE_BYTES ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid()) ||
    (metadata.mode & 0o077) !== 0
  ) {
    return { state: "invalid", record: null, errorCode: "EDELIVERYMETADATA" };
  }
  try {
    const record = JSON.parse(readFileSync(filename, "utf8"));
    if (!validLegacyRouteDeliveryRecord(record, messageId)) {
      return { state: "invalid", record: null, errorCode: "EDELIVERYSCHEMA" };
    }
    return { state: "valid", record };
  } catch {
    return { state: "invalid", record: null, errorCode: "EDELIVERYJSON" };
  }
}

export function readRouteDelivery(messageId) {
  return routeDeliveryState(messageId).record;
}

function boundedErrorCode(error) {
  return typeof error?.code === "string" && /^[A-Z0-9_]{1,32}$/.test(error.code)
    ? error.code
    : "EDISPATCHUNKNOWN";
}

function routeFingerprint(route) {
  return createHash("sha256").update(JSON.stringify(route)).digest("hex");
}

function publicOutcome(record, extra = {}) {
  return {
    logicalMessageId: record.logicalMessageId,
    admissionState: record.admissionState,
    reason: record.admissionReason,
    status: record.status,
    target: record.target,
    delivery: record.delivery,
    turnId: record.turnId,
    deduplicated: false,
    ...extra,
  };
}

export async function routePeerMessage(
  {
    from,
    target,
    message,
    route = null,
    logicalMessageId = route?.logical_message_id || randomUUID(),
  },
  dispatch,
  { log = writeCoordinationEvent } = {},
) {
  validateName("sender", from);
  validateName("target", target);
  validateUuid("logical message id", logicalMessageId);
  if (typeof message !== "string" || !message.trim()) throw new Error("message must not be empty");
  if (Buffer.byteLength(message, "utf8") > MAX_STORED_MESSAGE_BYTES) {
    throw new Error(`message exceeds ${MAX_STORED_MESSAGE_BYTES} bytes`);
  }
  const normalizedRoute = normalizeRoute(route, logicalMessageId);
  const messageSha256 = createHash("sha256").update(message).digest("hex");
  const fingerprint = routeFingerprint(normalizedRoute);
  ensureDirectory(ROUTE_DELIVERIES_DIR);

  let prepared;
  await withFileLock(deliveryLockPath(logicalMessageId), async () => {
    const deliveryState = routeDeliveryState(logicalMessageId);
    if (deliveryState.state === "invalid") {
      throw new Error(`Route Delivery failed validation: ${logicalMessageId}`);
    }
    const existing = deliveryState.record;
    if (existing) {
      if (
        existing.from !== from ||
        existing.target !== target ||
        existing.messageSha256 !== messageSha256 ||
        existing.routeFingerprint !== fingerprint
      ) {
        throw new Error(`logical message idempotency conflict: ${logicalMessageId}`);
      }
      prepared = { existing };
      return;
    }

    const targetBinding = routeBindingState(target);
    const senderBinding = routeBindingState(from);
    const targetRecord = readSessionRecord(target);
    const decision = admission(
      targetBinding,
      normalizedRoute,
      senderBinding,
      targetRecord,
      readSessionRecord(from),
    );
    const now = new Date().toISOString();
    const messageBytes = Buffer.byteLength(message, "utf8");
    const bodyReference =
      decision.state === "admitted" && messageBytes > MAX_MESSAGE_BYTES
        ? await storeMessageBody({ messageId: logicalMessageId, body: message })
        : null;
    if (decision.state === "quarantined") {
      atomicWrite(QUARANTINE_DIR, messageFilename(logicalMessageId), {
        version: 1,
        logicalMessageId,
        from,
        target,
        route: normalizedRoute,
        reason: decision.reason,
        message,
        messageBytes,
        messageSha256,
        quarantinedAt: now,
      });
    }
    const committed = await commitSingleRecipientDelivery({
      logicalMessage: {
        messageId: logicalMessageId,
        from,
        body: {
          messageId: logicalMessageId,
          bytes: messageBytes,
          sha256: messageSha256,
          contentRef: bodyReference?.contentRef || null,
        },
        route: normalizedRoute,
        routeFingerprint: fingerprint,
        createdAt: now,
      },
      target,
      targetThreadId: targetRecord?.threadId || null,
      admissionState: decision.state,
      admissionReason: decision.reason,
      wakePolicy: normalizedRoute?.wake_policy || "immediate",
      now,
    });
    const record = ledgerRouteDelivery(committed.record);
    prepared = { record };
  });

  if (prepared.existing) {
    await log({
      kind: "route-admission",
      phase: "deduplication",
      correlationId: logicalMessageId,
      target,
      outcome: "deduplicated",
      errorCode: prepared.existing.status,
    });
    return publicOutcome(prepared.existing, { deduplicated: true });
  }
  await log({
    kind: "route-admission",
    phase: "decision",
    correlationId: logicalMessageId,
    target,
    outcome: prepared.record.admissionState,
    errorCode: prepared.record.admissionReason,
  });
  if (prepared.record.admissionState === "quarantined") {
    return publicOutcome(prepared.record);
  }

  const attempt = await beginImmediateDelivery(logicalMessageId);
  try {
    const result = await dispatch({
      logicalMessageId,
      route: normalizedRoute,
    });
    const completed = ledgerRouteDelivery(
      await appendDeliveryEvidence(logicalMessageId, {
        attemptId: attempt.attemptId,
        state: "turn_started",
        evidenceKind: "dispatch-result",
        transportResult: result.delivery,
        turnId: result.turnId,
      }),
    );
    return publicOutcome(completed, { result });
  } catch (error) {
    await appendDeliveryEvidence(logicalMessageId, {
      attemptId: attempt.attemptId,
      state: "unknown",
      evidenceKind: "dispatch-result",
      errorCode: boundedErrorCode(error),
    });
    throw error;
  }
}

export async function reconcileRouteDelivery(
  logicalMessageId,
  inspect,
  {
    log = writeCoordinationEvent,
    now = Date.now(),
    dispatchingGraceMs = ROUTE_RECONCILE_GRACE_MS,
  } = {},
) {
  validateUuid("logical message id", logicalMessageId);
  let prepared;
  await withFileLock(deliveryLockPath(logicalMessageId), async () => {
    const state = routeDeliveryState(logicalMessageId);
    if (state.state === "missing") {
      throw new Error(`unknown Route Delivery: ${logicalMessageId}`);
    }
    if (state.state === "invalid") {
      throw new Error(`Route Delivery failed validation: ${logicalMessageId}`);
    }
    const record = state.record;
    if (record.admissionState !== "admitted") {
      throw new Error("Quarantined Route Deliveries cannot be reconciled or replayed");
    }
    if (record.status === "turn_started") {
      prepared = { terminal: record };
      return;
    }
    if (!record.targetThreadId) {
      throw new Error(
        "Legacy Route Delivery lacks pinned target thread identity; replay is forbidden",
      );
    }
    if (!["dispatching", "unknown"].includes(record.status)) {
      throw new Error(`Route Delivery status cannot be reconciled: ${record.status}`);
    }
    if (
      record.status === "dispatching" &&
      now - Date.parse(record.updatedAt) < dispatchingGraceMs
    ) {
      throw new Error("Route Delivery is still within its active dispatch grace period");
    }
    const targetRecord = readSessionRecord(record.target);
    if (!targetRecord || targetRecord.threadId !== record.targetThreadId) {
      throw new Error("Route Delivery target identity changed; replay is forbidden");
    }
    prepared = { record };
  });
  if (prepared.terminal) {
    return publicOutcome(prepared.terminal, {
      reconciled: false,
      reconciliation: "already-confirmed",
    });
  }

  const evidence = await inspect({
    logicalMessageId,
    target: prepared.record.target,
    targetThreadId: prepared.record.targetThreadId,
  });
  const accepted =
    evidence?.state === "accepted" && UUID_PATTERN.test(evidence.turnId || "");
  const reconciledAt = new Date(now).toISOString();
  const updated = await withFileLock(deliveryLockPath(logicalMessageId), async () => {
    const state = routeDeliveryState(logicalMessageId);
    if (state.state !== "valid") {
      throw new Error(`Route Delivery changed or failed validation: ${logicalMessageId}`);
    }
    const current = state.record;
    if (current.targetThreadId !== prepared.record.targetThreadId) {
      throw new Error("Route Delivery target identity changed during reconciliation");
    }
    if (current.status === "turn_started") return current;
    if (!["dispatching", "unknown"].includes(current.status)) {
      throw new Error("Route Delivery state changed during reconciliation");
    }
    if (current.version === 2) {
      return ledgerRouteDelivery(
        await appendDeliveryEvidence(logicalMessageId, {
          attemptId: current.attemptId,
          state: accepted ? "turn_started" : "unknown",
          evidenceKind: "reconciliation",
          turnId: accepted ? evidence.turnId : null,
          transportResult: accepted ? "reconciled" : null,
          errorCode: accepted ? null : "EACCEPTANCEUNVERIFIED",
          observedAt: reconciledAt,
        }),
      );
    }
    return atomicWrite(ROUTE_DELIVERIES_DIR, messageFilename(logicalMessageId), {
      ...current,
      status: accepted ? "turn_started" : "unknown",
      ...(accepted
        ? {
            delivery: "reconciled",
            turnId: evidence.turnId,
            errorCode: null,
          }
        : { errorCode: "EACCEPTANCEUNVERIFIED" }),
      reconciledAt,
      updatedAt: reconciledAt,
    });
  });
  const confirmed = updated.status === "turn_started";
  await log({
    kind: "route-delivery",
    phase: "reconciliation",
    correlationId: logicalMessageId,
    target: updated.target,
    outcome: confirmed ? "turn_started" : "unknown",
    errorCode: confirmed ? null : "EACCEPTANCEUNVERIFIED",
  });
  return publicOutcome(updated, {
    reconciled: accepted,
    reconciliation: accepted
      ? "accepted"
      : confirmed
        ? "concurrent-confirmation"
        : "not-observed",
    evidenceComplete: evidence?.complete === true,
    pagesInspected: Number.isSafeInteger(evidence?.pagesInspected)
      ? evidence.pagesInspected
      : 0,
  });
}

export function listQuarantine() {
  if (!existsSync(QUARANTINE_DIR)) return [];
  return readdirSync(QUARANTINE_DIR)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .map((name) => readJson(path.join(QUARANTINE_DIR, name)))
    .filter(Boolean)
    .map(({ message, ...record }) => record);
}

export function parseTypedPeerEnvelope(text) {
  if (typeof text !== "string" || !text.trim().startsWith("{")) return null;
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    return null;
  }
  if (parsed?.protocol !== "cxmsg-route/1" || typeof parsed.message !== "string") {
    return null;
  }
  const route = normalizeRoute(parsed, parsed.logical_message_id);
  return { message: parsed.message, route, logicalMessageId: route.logical_message_id };
}
