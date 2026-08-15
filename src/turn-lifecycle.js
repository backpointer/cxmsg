import { randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { CXMSG_STATE_DIR } from "./runtime.js";
import { summarizeTurnLifecycle } from "./thread-activity.js";

export const TURN_LIFECYCLE_PATH = path.join(
  CXMSG_STATE_DIR,
  "turn-lifecycle.json",
);
export const TURN_LIFECYCLE_RECENT_LIMIT = 8;

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const THREAD_STATUS_TYPES = new Set(["active", "idle", "notLoaded", "systemError"]);
const TURN_STATUSES = new Set([
  "inProgress",
  "completed",
  "failed",
  "interrupted",
  "cancelled",
  "canceled",
]);

function emptyState() {
  return {
    version: 1,
    observationSequence: 0,
    connection: null,
    threads: {},
  };
}

function validTimestamp(value) {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function validThreadProjection(threadId, projection) {
  return Boolean(
    UUID_PATTERN.test(threadId) &&
      projection &&
      THREAD_STATUS_TYPES.has(projection.status) &&
      (projection.activeTurnId === null || UUID_PATTERN.test(projection.activeTurnId)) &&
      Array.isArray(projection.recentTerminalTurnIds) &&
      projection.recentTerminalTurnIds.length <= TURN_LIFECYCLE_RECENT_LIMIT &&
      projection.recentTerminalTurnIds.every((turnId) => UUID_PATTERN.test(turnId)) &&
      Number.isSafeInteger(projection.lastSequence) &&
      projection.lastSequence > 0 &&
      validTimestamp(projection.observedAt) &&
      ["notification", "catch-up"].includes(projection.source),
  );
}

export function validTurnLifecycleState(state) {
  if (
    state?.version !== 1 ||
    !Number.isSafeInteger(state.observationSequence) ||
    state.observationSequence < 0 ||
    !state.threads ||
    Array.isArray(state.threads) ||
    typeof state.threads !== "object"
  ) {
    return false;
  }
  if (state.connection !== null) {
    const connection = state.connection;
    if (
      !UUID_PATTERN.test(connection.epoch || "") ||
      !["connected", "disconnected"].includes(connection.state) ||
      !validTimestamp(connection.connectedAt) ||
      (connection.disconnectedAt !== null && !validTimestamp(connection.disconnectedAt)) ||
      (connection.appServerVersion !== null &&
        !/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(connection.appServerVersion))
    ) {
      return false;
    }
  }
  return Object.entries(state.threads).every(([threadId, projection]) =>
    validThreadProjection(threadId, projection),
  );
}

function durableAtomicWrite(destination, value) {
  mkdirSync(path.dirname(destination), { recursive: true, mode: 0o700 });
  chmodSync(path.dirname(destination), 0o700);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  let descriptor;
  try {
    descriptor = openSync(temporary, "wx", 0o600);
    writeFileSync(descriptor, `${JSON.stringify(value, null, 2)}\n`);
    fsyncSync(descriptor);
    closeSync(descriptor);
    descriptor = undefined;
    renameSync(temporary, destination);
    const parent = openSync(path.dirname(destination), "r");
    try {
      fsyncSync(parent);
    } finally {
      closeSync(parent);
    }
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
    if (existsSync(temporary)) unlinkSync(temporary);
  }
}

export function readTurnLifecycle() {
  try {
    const state = JSON.parse(readFileSync(TURN_LIFECYCLE_PATH, "utf8"));
    return validTurnLifecycleState(state) ? state : null;
  } catch {
    return null;
  }
}

function updateState(mutator) {
  const existing = readTurnLifecycle();
  if (existing === null && existsSync(TURN_LIFECYCLE_PATH)) {
    throw new Error("Turn Lifecycle state is malformed");
  }
  const state = existing || emptyState();
  mutator(state);
  if (!validTurnLifecycleState(state)) throw new Error("invalid Turn Lifecycle state");
  durableAtomicWrite(TURN_LIFECYCLE_PATH, state);
  return structuredClone(state);
}

export function beginTurnLifecycleConnection({ appServerVersion = null, now } = {}) {
  const observedAt = now || new Date().toISOString();
  if (!validTimestamp(observedAt)) throw new Error("invalid lifecycle connection timestamp");
  const epoch = randomUUID();
  const state = updateState((current) => {
    current.connection = {
      epoch,
      state: "connected",
      connectedAt: observedAt,
      disconnectedAt: null,
      appServerVersion,
    };
  });
  return { epoch, state };
}

export function endTurnLifecycleConnection(epoch, { now } = {}) {
  if (!UUID_PATTERN.test(epoch || "")) throw new Error("invalid lifecycle connection epoch");
  const observedAt = now || new Date().toISOString();
  if (!validTimestamp(observedAt)) throw new Error("invalid lifecycle disconnect timestamp");
  return updateState((state) => {
    if (state.connection?.epoch !== epoch) return;
    state.connection = {
      ...state.connection,
      state: "disconnected",
      disconnectedAt: observedAt,
    };
  });
}

function applyObservation(state, threadId, observation, source, observedAt) {
  if (!UUID_PATTERN.test(threadId || "")) return false;
  const previous = state.threads[threadId] || null;
  const status = observation.status || previous?.status || "notLoaded";
  if (!THREAD_STATUS_TYPES.has(status)) return false;
  const activeTurnId = UUID_PATTERN.test(observation.activeTurnId || "")
    ? observation.activeTurnId
    : observation.activeTurnId === null
      ? null
      : previous?.activeTurnId || null;
  const terminal = [
    ...(observation.recentTerminalTurnIds || []),
    ...(previous?.recentTerminalTurnIds || []),
  ].filter((turnId, index, all) => UUID_PATTERN.test(turnId) && all.indexOf(turnId) === index)
    .slice(0, TURN_LIFECYCLE_RECENT_LIMIT);
  state.observationSequence += 1;
  state.threads[threadId] = {
    status,
    activeTurnId,
    recentTerminalTurnIds: terminal,
    lastSequence: state.observationSequence,
    observedAt,
    source,
  };
  return true;
}

export function observeTurnLifecycleNotification(message, { now } = {}) {
  const observedAt = now || new Date().toISOString();
  if (!validTimestamp(observedAt)) throw new Error("invalid lifecycle observation timestamp");
  const method = message?.method;
  const params = message?.params || {};
  let threadId = params.threadId;
  let observation = null;
  if (method === "thread/status/changed") {
    const status = params.status?.type || null;
    observation = {
      status,
      ...(["idle", "notLoaded", "systemError"].includes(status)
        ? { activeTurnId: null }
        : {}),
    };
  } else if (["turn/started", "turn/completed"].includes(method)) {
    const turn = params.turn;
    if (!UUID_PATTERN.test(turn?.id || "") || !TURN_STATUSES.has(turn.status)) return null;
    observation = {
      status: method === "turn/started" ? "active" : "idle",
      activeTurnId: method === "turn/started" ? turn.id : null,
      recentTerminalTurnIds: method === "turn/completed" ? [turn.id] : [],
    };
  } else {
    return null;
  }
  let applied = false;
  const state = updateState((current) => {
    applied = applyObservation(current, threadId, observation, "notification", observedAt);
  });
  return applied ? state.threads[threadId] : null;
}

export function observeTurnLifecycleCatchUp(thread, page, { now } = {}) {
  const observedAt = now || new Date().toISOString();
  if (!validTimestamp(observedAt)) throw new Error("invalid lifecycle catch-up timestamp");
  const summary = summarizeTurnLifecycle(page);
  const threadId = thread?.id;
  let applied = false;
  const state = updateState((current) => {
    applied = applyObservation(
      current,
      threadId,
      {
        status: thread?.status?.type,
        activeTurnId:
          thread?.status?.type === "active" ? summary.activeTurnId : null,
        recentTerminalTurnIds: summary.recentTerminalTurnIds,
      },
      "catch-up",
      observedAt,
    );
  });
  return applied ? state.threads[threadId] : null;
}
