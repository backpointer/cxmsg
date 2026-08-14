import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import { MAX_STORED_MESSAGE_BYTES } from "./message-bodies.js";
import { readSessionRecord } from "./registry.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const ROUTE_BINDINGS_DIR = path.join(CXMSG_STATE_DIR, "route-bindings");
export const ROUTE_DELIVERIES_DIR = path.join(CXMSG_STATE_DIR, "route-deliveries");
export const QUARANTINE_DIR = path.join(CXMSG_STATE_DIR, "quarantine");

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

export function writeRouteBinding({ sessionName, threadId, projectId, role }) {
  const now = new Date().toISOString();
  const previous = readRouteBinding(sessionName);
  return atomicWrite(ROUTE_BINDINGS_DIR, sessionFilename(sessionName), {
    version: 1,
    sessionName,
    threadId: validateUuid("thread id", threadId),
    projectId: validateName("project id", projectId),
    role: validateName("role", role),
    boundAt: previous?.boundAt || now,
    updatedAt: now,
  });
}

export function readRouteBinding(sessionName) {
  const record = readJson(path.join(ROUTE_BINDINGS_DIR, sessionFilename(sessionName)));
  return record?.version === 1 &&
    record.sessionName === sessionName &&
    UUID_PATTERN.test(record.threadId || "") &&
    NAME_PATTERN.test(record.projectId || "") &&
    NAME_PATTERN.test(record.role || "")
    ? record
    : null;
}

export function listRouteBindings() {
  if (!existsSync(ROUTE_BINDINGS_DIR)) return [];
  return readdirSync(ROUTE_BINDINGS_DIR)
    .filter((name) => name.endsWith(".json"))
    .sort()
    .map((name) => readJson(path.join(ROUTE_BINDINGS_DIR, name)))
    .filter(
      (record) =>
        record?.version === 1 &&
        SESSION_PATTERN.test(record.sessionName || "") &&
        UUID_PATTERN.test(record.threadId || "") &&
        NAME_PATTERN.test(record.projectId || "") &&
        NAME_PATTERN.test(record.role || ""),
    );
}

function admission(
  binding,
  route,
  senderBinding,
  targetRecord,
  senderRecord,
  now = Date.now(),
) {
  if (!binding) return { state: "admitted", reason: "legacy-unbound" };
  if (targetRecord && binding.threadId !== targetRecord.threadId) {
    return { state: "quarantined", reason: "target_identity_mismatch" };
  }
  if (!route) return { state: "quarantined", reason: "missing_route" };
  if (route.project_id !== binding.projectId) {
    return { state: "quarantined", reason: "project_mismatch" };
  }
  if (route.target_role !== binding.role) {
    return { state: "quarantined", reason: "role_mismatch" };
  }
  if (
    route.sender_role &&
    senderBinding &&
    senderRecord &&
    senderBinding.threadId !== senderRecord.threadId
  ) {
    return { state: "quarantined", reason: "sender_identity_mismatch" };
  }
  if (
    route.sender_role &&
    senderBinding &&
    route.sender_role !== senderBinding.role
  ) {
    return { state: "quarantined", reason: "sender_role_mismatch" };
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

function readRouteDelivery(messageId) {
  return readJson(deliveryPath(messageId));
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
    const existing = readRouteDelivery(logicalMessageId);
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

    const targetBinding = readRouteBinding(target);
    const senderBinding = readRouteBinding(from);
    const decision = admission(
      targetBinding,
      normalizedRoute,
      senderBinding,
      readSessionRecord(target),
      readSessionRecord(from),
    );
    const now = new Date().toISOString();
    const record = {
      version: 1,
      logicalMessageId,
      from,
      target,
      messageBytes: Buffer.byteLength(message, "utf8"),
      messageSha256,
      routeFingerprint: fingerprint,
      route: normalizedRoute,
      admissionState: decision.state,
      admissionReason: decision.reason,
      status: decision.state === "admitted" ? "dispatching" : "quarantined",
      wakeAttemptedAt: decision.state === "admitted" ? now : null,
      createdAt: now,
      updatedAt: now,
      turnId: null,
      delivery: null,
      errorCode: null,
    };
    if (decision.state === "quarantined") {
      atomicWrite(QUARANTINE_DIR, messageFilename(logicalMessageId), {
        version: 1,
        logicalMessageId,
        from,
        target,
        route: normalizedRoute,
        reason: decision.reason,
        message,
        messageBytes: record.messageBytes,
        messageSha256,
        quarantinedAt: now,
      });
    }
    atomicWrite(ROUTE_DELIVERIES_DIR, messageFilename(logicalMessageId), record);
    prepared = { record };
  });

  if (prepared.existing) {
    return publicOutcome(prepared.existing, { deduplicated: true });
  }
  if (prepared.record.admissionState === "quarantined") {
    return publicOutcome(prepared.record);
  }

  try {
    const result = await dispatch({
      logicalMessageId,
      route: normalizedRoute,
    });
    const completed = await withFileLock(deliveryLockPath(logicalMessageId), async () => {
      const current = readRouteDelivery(logicalMessageId);
      return atomicWrite(ROUTE_DELIVERIES_DIR, messageFilename(logicalMessageId), {
        ...current,
        status: "turn_started",
        delivery: result.delivery,
        turnId: result.turnId,
        updatedAt: new Date().toISOString(),
      });
    });
    return publicOutcome(completed, { result });
  } catch (error) {
    await withFileLock(deliveryLockPath(logicalMessageId), async () => {
      const current = readRouteDelivery(logicalMessageId);
      atomicWrite(ROUTE_DELIVERIES_DIR, messageFilename(logicalMessageId), {
        ...current,
        status: "unknown",
        errorCode: boundedErrorCode(error),
        updatedAt: new Date().toISOString(),
      });
    });
    throw error;
  }
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
