import { randomUUID } from "node:crypto";
import { AsyncLocalStorage } from "node:async_hooks";
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  unlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { withFileLock } from "./file-lock.js";
import { processState } from "./process-state.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const RETENTION_STATE_DIR = path.join(CXMSG_STATE_DIR, "retention");
export const RETENTION_WRITERS_DIR = path.join(RETENTION_STATE_DIR, "writers");
export const RETENTION_MUTATION_LOCK_PATH = path.join(
  RETENTION_STATE_DIR,
  "mutation.lock",
);

const WRITER_LEASE_MS = 30_000;
const WRITER_HEARTBEAT_MS = 5_000;
const WRITER_DRAIN_TIMEOUT_MS = 10_000;
const context = new AsyncLocalStorage();

function ensurePrivateDirectory(directory) {
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const metadata = lstatSync(directory);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error(`Retention path is not a directory: ${path.basename(directory)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Retention directory is owned by another user: ${path.basename(directory)}`);
  }
  chmodSync(directory, 0o700);
}

function ensureBarrier() {
  ensurePrivateDirectory(CXMSG_STATE_DIR);
  ensurePrivateDirectory(RETENTION_STATE_DIR);
  ensurePrivateDirectory(RETENTION_WRITERS_DIR);
}

function busyError() {
  const error = new Error("Retention mutation is in progress; retry the write later");
  error.code = "ERETENTIONBUSY";
  return error;
}

function assertPrivateLease(filename) {
  const metadata = lstatSync(filename);
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.nlink !== 1) {
    throw new Error(`Retention writer lease is not a private regular file: ${path.basename(filename)}`);
  }
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Retention writer lease is owned by another user: ${path.basename(filename)}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`Retention writer lease permissions are too broad: ${path.basename(filename)}`);
  }
  return metadata;
}

function removeOwnedLease(filename, token) {
  try {
    const parsed = JSON.parse(readFileSync(filename, "utf8"));
    if (parsed?.token !== token) return false;
    unlinkSync(filename);
    return true;
  } catch {
    return false;
  }
}

function writerLeaseNames() {
  if (!existsSync(RETENTION_WRITERS_DIR)) return [];
  return readdirSync(RETENTION_WRITERS_DIR)
    .filter((name) => /^[0-9a-f-]{36}\.json$/i.test(name))
    .sort();
}

function liveWriterLeases(now = Date.now()) {
  const live = [];
  for (const name of writerLeaseNames()) {
    const filename = path.join(RETENTION_WRITERS_DIR, name);
    let metadata;
    try {
      metadata = assertPrivateLease(filename);
    } catch (error) {
      if (error?.code === "ENOENT") continue;
      throw error;
    }
    let lease;
    try {
      lease = JSON.parse(readFileSync(filename, "utf8"));
    } catch (error) {
      if (error?.code === "ENOENT") continue;
      throw new Error(`Retention writer lease is malformed: ${name}`);
    }
    if (
      lease?.version !== 1 ||
      !Number.isSafeInteger(lease.pid) ||
      lease.pid < 2 ||
      typeof lease.token !== "string" ||
      lease.token !== name.slice(0, -5)
    ) {
      throw new Error(`Retention writer lease is invalid: ${name}`);
    }
    if (
      now - metadata.mtimeMs > WRITER_LEASE_MS &&
      processState(lease.pid) === "missing"
    ) {
      removeOwnedLease(filename, lease.token);
      continue;
    }
    live.push({ filename, lease });
  }
  return live;
}

export function retentionMutationActive() {
  return context.getStore()?.mode === "mutation";
}

export function assertRetentionMutation() {
  if (!retentionMutationActive()) {
    throw new Error("Retention mutation lease is required");
  }
}

export function withRetentionWriter(callback) {
  if (typeof callback !== "function") throw new Error("Retention writer callback is required");
  const active = context.getStore();
  if (active?.mode === "writer" || active?.mode === "mutation") return callback();

  ensureBarrier();
  if (existsSync(RETENTION_MUTATION_LOCK_PATH)) throw busyError();
  const token = randomUUID();
  const filename = path.join(RETENTION_WRITERS_DIR, `${token}.json`);
  writeFileSync(
    filename,
    `${JSON.stringify({ version: 1, pid: process.pid, token, createdAt: Date.now() })}\n`,
    { flag: "wx", mode: 0o600 },
  );
  if (existsSync(RETENTION_MUTATION_LOCK_PATH)) {
    removeOwnedLease(filename, token);
    throw busyError();
  }

  const heartbeat = setInterval(() => {
    try {
      const now = new Date();
      utimesSync(filename, now, now);
    } catch {}
  }, WRITER_HEARTBEAT_MS);
  heartbeat.unref();
  const release = () => {
    clearInterval(heartbeat);
    removeOwnedLease(filename, token);
  };

  return context.run({ mode: "writer", token }, () => {
    let result;
    try {
      result = callback();
    } catch (error) {
      release();
      throw error;
    }
    if (result && typeof result.then === "function") {
      return Promise.resolve(result).finally(release);
    }
    release();
    return result;
  });
}

export async function withRetentionMutation(
  callback,
  { drainTimeoutMs = WRITER_DRAIN_TIMEOUT_MS } = {},
) {
  if (typeof callback !== "function") throw new Error("Retention mutation callback is required");
  const active = context.getStore();
  if (active?.mode === "mutation") return callback();
  if (active?.mode === "writer") {
    throw new Error("Retention writer cannot upgrade to a mutation lease");
  }
  if (!Number.isSafeInteger(drainTimeoutMs) || drainTimeoutMs < 1) {
    throw new Error("Retention writer drain timeout is invalid");
  }

  ensureBarrier();
  return withFileLock(RETENTION_MUTATION_LOCK_PATH, async () => {
    const deadline = Date.now() + drainTimeoutMs;
    while (liveWriterLeases().length > 0) {
      if (Date.now() >= deadline) {
        throw new Error("timed out draining Retention writers");
      }
      await delay(25);
    }
    return context.run({ mode: "mutation" }, callback);
  });
}
