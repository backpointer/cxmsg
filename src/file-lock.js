import { randomUUID } from "node:crypto";
import {
  mkdirSync,
  readFileSync,
  renameSync,
  statSync,
  unlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { processState } from "./process-state.js";

const DEFAULT_LEASE_MS = 30_000;
const HEARTBEAT_MS = 5_000;
const POLL_MS = 50;

function readOwner(target) {
  try {
    const raw = readFileSync(target, "utf8");
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      const legacyPid = /^\d+$/.test(raw.trim()) ? Number(raw.trim()) : null;
      parsed = Number.isSafeInteger(legacyPid) && legacyPid > 1
        ? { pid: legacyPid, token: null }
        : null;
    }
    if (Number.isSafeInteger(parsed) && parsed > 1) {
      parsed = { pid: parsed, token: null };
    }
    if (!parsed) return null;
    return {
      raw,
      pid: Number.isSafeInteger(parsed.pid) ? parsed.pid : null,
      token: typeof parsed.token === "string" ? parsed.token : null,
      mtimeMs: statSync(target).mtimeMs,
    };
  } catch {
    return null;
  }
}

function removeIfOwned(target, token) {
  const owner = readOwner(target);
  if (!owner || owner.token !== token) return false;
  const released = `${target}.released.${token}`;
  try {
    renameSync(target, released);
    const moved = readOwner(released);
    if (moved?.token !== token) return false;
    unlinkSync(released);
    return true;
  } catch {
    return false;
  }
}

async function reapExpiredOwner(target, observed, leaseMs, token) {
  if (
    !observed ||
    !observed.pid ||
    Date.now() - observed.mtimeMs <= leaseMs ||
    processState(observed.pid) !== "missing"
  ) {
    return false;
  }
  await delay(Math.floor(Math.random() * 25));
  const current = readOwner(target);
  if (!current || current.raw !== observed.raw) return false;
  const stale = `${target}.stale.${token}`;
  try {
    renameSync(target, stale);
    const moved = readOwner(stale);
    if (moved?.raw !== observed.raw) {
      try {
        renameSync(stale, target);
      } catch {}
      return false;
    }
    unlinkSync(stale);
    return true;
  } catch {
    return false;
  }
}

export async function withFileLock(
  target,
  callback,
  { timeoutMs = 10_000, leaseMs = DEFAULT_LEASE_MS } = {},
) {
  const token = randomUUID();
  const deadline = Date.now() + timeoutMs;
  const owner = `${JSON.stringify({
    version: 1,
    pid: process.pid,
    token,
    createdAt: Date.now(),
  })}\n`;

  mkdirSync(path.dirname(target), { recursive: true, mode: 0o700 });
  while (true) {
    try {
      writeFileSync(target, owner, { flag: "wx", mode: 0o600 });
      break;
    } catch (error) {
      if (error.code !== "EEXIST") throw error;
      await reapExpiredOwner(target, readOwner(target), leaseMs, token);
      if (Date.now() >= deadline) {
        throw new Error(`timed out acquiring lock: ${path.basename(target)}`);
      }
      await delay(POLL_MS);
    }
  }

  const heartbeat = setInterval(() => {
    const current = readOwner(target);
    if (current?.token !== token) return;
    try {
      const now = new Date();
      utimesSync(target, now, now);
    } catch {}
  }, Math.min(HEARTBEAT_MS, Math.max(100, Math.floor(leaseMs / 3))));
  heartbeat.unref();
  try {
    return await callback();
  } finally {
    clearInterval(heartbeat);
    removeIfOwned(target, token);
  }
}
