import { existsSync, promises as fs, readFileSync } from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { probeAppServerSocket, withAppServer } from "./app-server-client.js";
import {
  attachmentCommandMatches,
  readAttachmentRecord,
  sessionPresentation,
} from "./attachments.js";
import { claudeBridgeState } from "./claude-bridge.js";
import {
  listClaudeRequestGrants,
} from "./claude-grants.js";
import { listClaudePeers } from "./claude-messaging.js";
import { listJobs } from "./jobs.js";
import { processIdentity, processState } from "./process-state.js";
import { listSessionRecords } from "./registry.js";
import { DEFAULT_SOCKET_PATH, PID_PATH, socketPath } from "./runtime.js";

const moduleDirectory = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_WEB_ROOT = path.resolve(moduleDirectory, "..", "web");
const LOOPBACK_HOST = "127.0.0.1";

const STATIC_ROUTES = new Map([
  ["/dashboard", ["dashboard.html", "text/html; charset=utf-8"]],
  ["/orchestration", ["orchestration.html", "text/html; charset=utf-8"]],
  ["/app.js", ["app.js", "text/javascript; charset=utf-8"]],
  ["/topology.js", ["topology.js", "text/javascript; charset=utf-8"]],
  ["/styles.css", ["styles.css", "text/css; charset=utf-8"]],
]);

function readServerPid() {
  try {
    const pid = Number.parseInt(readFileSync(PID_PATH, "utf8").trim(), 10);
    return Number.isSafeInteger(pid) && pid > 1 ? pid : null;
  } catch {
    return null;
  }
}

export function publicJob(job) {
  return {
    jobId: job.jobId,
    kind: job.kind,
    from: job.from,
    target: job.target,
    status: job.status,
    permissions: job.permissions,
    targetThreadId: job.targetThreadId,
    threadId: job.threadId,
    turnId: job.turnId,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    completedAt: job.completedAt,
    hasResult: Boolean(job.result),
    hasError: Boolean(job.error),
    replyStatus: job.reply?.status || null,
    sourceName: job.source?.name || null,
    sourceSessionId: job.source?.sessionId || null,
  };
}

function publicClaudePeer(peer) {
  return {
    pid: peer.pid,
    name: peer.name,
    sessionId: peer.sessionId,
    cwd: peer.cwd,
    status: peer.status,
    sessionStatus: peer.sessionStatus,
    verification: peer.verification,
    errorCode: peer.errorCode,
    kind: peer.kind,
  };
}

export function liveAttachment(
  name,
  {
    read = readAttachmentRecord,
    state = processState,
    identity = processIdentity,
  } = {},
) {
  const attachment = read(name);
  if (!attachment) return null;
  if (state(attachment.childPid) === "missing") return null;
  const evidence = identity(attachment.childPid, []);
  if (evidence.state !== "matched") return attachment;
  return attachmentCommandMatches(attachment, evidence.command)
    ? attachment
    : null;
}

async function permissionProfiles(client, cwd, cache) {
  if (!cache.has(cwd)) {
    cache.set(
      cwd,
      client
        .request("permissionProfile/list", { cwd })
        .then((result) => result.data || [])
        .catch(() => []),
    );
  }
  return cache.get(cwd);
}

async function codexSessionSnapshot(client, record, profileCache) {
  const attachment = liveAttachment(record.name);
  const bridge = await claudeBridgeState(record.name);
  let thread = null;
  let error = null;
  try {
    const result = await client.request("thread/read", {
      threadId: record.threadId,
      includeTurns: false,
    });
    thread = result.thread;
  } catch (caught) {
    error = caught.message;
  }

  const profiles = await permissionProfiles(client, thread?.cwd || record.cwd, profileCache);
  return {
    name: record.name,
    threadId: record.threadId,
    cwd: thread?.cwd || record.cwd,
    status: thread?.status?.type || "unavailable",
    updatedAt: thread?.updatedAt || null,
    presentation: sessionPresentation(record, attachment),
    attachedPid: attachment?.childPid || null,
    adopted: Boolean(record.adopted),
    delegators: [...(record.allowedDelegators || [])].sort(),
    claudeGrants: publicClaudeRequestGrants(record),
    bridge: {
      status: bridge.status,
      running: bridge.running,
      pid: bridge.record?.pid || null,
      verification:
        bridge.socketProbe?.state === "denied"
          ? "sandbox-denied"
          : bridge.socketProbe?.state || null,
      errorCode: bridge.socketProbe?.errorCode || null,
    },
    permissionProfiles: profiles.map((profile) => ({
      id: profile.id,
      allowed: profile.allowed,
      description: profile.description || null,
    })),
    hasError: Boolean(error),
  };
}

function publicClaudeRequestGrants(record) {
  return listClaudeRequestGrants(record).map((grant) => ({
    sessionId: grant.sessionId,
    name: grant.name,
    permissions: grant.permissions,
    approval: grant.approval || "never",
    grantedAt: grant.grantedAt,
  }));
}

export async function buildWebSnapshot({
  connect = withAppServer,
  sessions = listSessionRecords,
  claudePeers = listClaudePeers,
  jobs = listJobs,
  appServerProbe = probeAppServerSocket,
} = {}) {
  const records = sessions();
  const [codexSessions, liveClaudePeers] = await Promise.all([
    connect(async (client) => {
      const profileCache = new Map();
      return Promise.all(
        records.map((record) => codexSessionSnapshot(client, record, profileCache)),
      );
    }),
    claudePeers(),
  ]);
  const currentSocketPath = socketPath();
  const managedServer = currentSocketPath === DEFAULT_SOCKET_PATH;
  const pid = managedServer ? readServerPid() : null;
  const socketPresent = existsSync(currentSocketPath);
  const socketProbe = await appServerProbe(currentSocketPath);

  return {
    generatedAt: new Date().toISOString(),
    server: {
      status:
        socketProbe.state === "healthy"
          ? "running"
          : socketProbe.state === "denied" || socketProbe.state === "timeout"
            ? "unreachable"
            : socketPresent
              ? "unknown"
              : "stopped",
      running: socketProbe.state === "healthy",
      pid,
      transport: "unix",
      socketPresent,
      socketHealthy: socketProbe.state === "healthy",
      verification:
        socketProbe.state === "denied" ? "sandbox-denied" : socketProbe.state,
      errorCode: socketProbe.errorCode,
    },
    codexSessions: codexSessions.sort((left, right) =>
      left.name.localeCompare(right.name),
    ),
    claudeSessions: liveClaudePeers
      .map(publicClaudePeer)
      .sort((left, right) => left.name.localeCompare(right.name)),
    jobs: jobs()
      .map(publicJob)
      .sort((left, right) => String(right.createdAt).localeCompare(String(left.createdAt))),
  };
}

function requestHostAllowed(value) {
  const host = String(value || "").toLowerCase();
  return (
    host === "localhost" ||
    host.startsWith("localhost:") ||
    host === LOOPBACK_HOST ||
    host.startsWith(`${LOOPBACK_HOST}:`)
  );
}

function responseHeaders(contentType) {
  return {
    "cache-control": "no-store",
    "content-security-policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'",
    "content-type": contentType,
    "referrer-policy": "no-referrer",
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
  };
}

function send(res, status, contentType, body, headOnly = false) {
  res.writeHead(status, responseHeaders(contentType));
  res.end(headOnly ? undefined : body);
}

export function createWebRequestHandler({
  snapshot = buildWebSnapshot,
  webRoot = DEFAULT_WEB_ROOT,
  snapshotCacheMs = 1_000,
} = {}) {
  let cachedSnapshot = null;
  let cacheExpiresAt = 0;
  let snapshotInFlight = null;
  const currentSnapshot = async () => {
    if (cachedSnapshot && Date.now() < cacheExpiresAt) return cachedSnapshot;
    if (!snapshotInFlight) {
      snapshotInFlight = Promise.resolve(snapshot())
        .then((value) => {
          cachedSnapshot = value;
          cacheExpiresAt = Date.now() + snapshotCacheMs;
          return value;
        })
        .finally(() => {
          snapshotInFlight = null;
        });
    }
    return snapshotInFlight;
  };
  return async function handleRequest(req, res) {
    const headOnly = req.method === "HEAD";
    if (req.method !== "GET" && !headOnly) {
      send(res, 405, "application/json; charset=utf-8", JSON.stringify({ error: "method not allowed" }));
      return;
    }
    if (!requestHostAllowed(req.headers.host)) {
      send(res, 403, "application/json; charset=utf-8", JSON.stringify({ error: "loopback host required" }), headOnly);
      return;
    }

    const pathname = new URL(req.url || "/", `http://${req.headers.host}`).pathname;
    if (pathname === "/") {
      res.writeHead(302, { location: "/dashboard", "cache-control": "no-store" });
      res.end();
      return;
    }
    if (pathname === "/api/snapshot") {
      try {
        const body = JSON.stringify(await currentSnapshot());
        send(res, 200, "application/json; charset=utf-8", body, headOnly);
      } catch (error) {
        send(
          res,
          503,
          "application/json; charset=utf-8",
          JSON.stringify({ error: "snapshot unavailable" }),
          headOnly,
        );
      }
      return;
    }

    const route = STATIC_ROUTES.get(pathname);
    if (!route) {
      send(res, 404, "text/plain; charset=utf-8", "Not found\n", headOnly);
      return;
    }
    try {
      const body = await fs.readFile(path.join(webRoot, route[0]));
      send(res, 200, route[1], body, headOnly);
    } catch {
      send(res, 500, "text/plain; charset=utf-8", "Web asset unavailable\n", headOnly);
    }
  };
}

export async function startWebServer({
  port = 4173,
  snapshot = buildWebSnapshot,
  webRoot = DEFAULT_WEB_ROOT,
  snapshotCacheMs = 1_000,
} = {}) {
  if (!Number.isInteger(port) || port < 0 || port > 65_535) {
    throw new Error("web port must be an integer between 0 and 65535");
  }
  const server = http.createServer(
    createWebRequestHandler({ snapshot, webRoot, snapshotCacheMs }),
  );
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, LOOPBACK_HOST, resolve);
  });
  const address = server.address();
  return {
    server,
    host: LOOPBACK_HOST,
    port: address.port,
    origin: `http://${LOOPBACK_HOST}:${address.port}`,
  };
}
