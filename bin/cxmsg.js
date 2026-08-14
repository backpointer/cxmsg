#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import {
  chmodSync,
  closeSync,
  existsSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import {
  probeAppServerSocket,
  withAppServer,
} from "../src/app-server-client.js";
import {
  attachmentCommandMatches,
  listAttachmentRecords,
  markAttachmentDetachRequested,
  readAttachmentRecord,
  removeAttachmentRecord,
  sessionPresentation,
  writeAttachmentRecord,
} from "../src/attachments.js";
import {
  listClaudePeers,
  resolveClaudePeer,
} from "../src/claude-messaging.js";
import {
  createClaudeDeliveryJob,
  refreshClaudeDelivery,
  sendClaudeDeliveryJob,
} from "../src/claude-delivery.js";
import {
  DEFAULT_CLAUDE_PERMISSION_PROFILE,
  listClaudeRequestGrants,
  publicClaudeRequestGrant,
  removeClaudeRequestGrant,
  upsertClaudeRequestGrant,
  validateClaudeSessionId,
} from "../src/claude-grants.js";
import {
  bridgeLogPath,
  claudeBridgeState,
  closeBridgeLog,
  openBridgeLog,
  removeStaleClaudeBridgeRecord,
} from "../src/claude-bridge.js";
import { replyToClaudeRequest } from "../src/claude-requests.js";
import { decideApproval } from "../src/approvals.js";
import {
  doctorExitCode,
  renderDoctorText,
  runDoctor,
} from "../src/doctor.js";
import {
  APPROVAL_MODES,
  EXECUTION_MODES,
  MIRROR_MODES,
} from "../src/delegation-worker.js";
import {
  activeJobsForTarget,
  createJob,
  failJobIfWorkerExited,
  isPendingJob,
  newJobId,
  readJob,
  refreshJob,
  updateJob,
} from "../src/jobs.js";
import {
  CXMSG_STATE_DIR,
  DEFAULT_SOCKET_PATH,
  LOG_PATH,
  PID_PATH,
  socketUrl,
} from "../src/runtime.js";
import {
  processIdentity,
  processState,
  serviceEvidence,
} from "../src/process-state.js";
import {
  deliverPeerMessage,
  storedSessionName,
  validateMessage,
  validateSessionName,
} from "../src/messaging.js";
import { readThreadMetadata } from "../src/thread-activity.js";
import {
  listSessionRecords,
  readSessionRecord,
  removeSessionRecord,
  withSessionLock,
  writeSessionRecord,
} from "../src/registry.js";
import { startWebServer } from "../src/web-server.js";
import {
  HOST_RELAY_LOG_PATH,
  HOST_RELAY_RECORD_PATH,
  hostRelayRequest,
  readHostRelayRecord,
} from "../src/host-relay.js";

const codexBin = process.env.CODEX_BIN || "codex";
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const claudeBridgeWorker = path.join(scriptDir, "claude-bridge-worker.js");
const delegationWorker = path.join(scriptDir, "delegation-worker.js");
const hostRelayWorker = path.join(scriptDir, "host-relay-worker.js");

function usage(exitCode = 0) {
  const output = `Usage:
  cxmsg server start|status|stop
  cxmsg open <name> [-- <codex resume options>]
  cxmsg attach <name> [-- <codex resume options>]
  cxmsg detach <name>
  cxmsg status <name> [--json]
  cxmsg session <name> [-- <codex resume options>]  Legacy alias for 'open'
  cxmsg create <name>
  cxmsg register <name> <thread-id>
  cxmsg remove <name>
  cxmsg peers [--json]
  cxmsg send [--from <name>] <target> <message...>
  cxmsg grant <sender> <target>
  cxmsg revoke <sender> <target>
  cxmsg permissions <target> [--json]
  cxmsg doctor [--json] [--deep] [--target <session-name>]
  cxmsg delegate [--from <name>] [--permissions <profile>] [--execution fork|inline]
                 [--approval never|relay|auto] [--approval-timeout <seconds>]
                 [--mirror none|summary|full] <target> <task...>
  cxmsg wait <job-id> [--timeout <seconds>] [--json]
  cxmsg result <job-id> [--json]
  cxmsg approvals <job-id> [--json]
  cxmsg approve <job-id> <approval-id>
  cxmsg deny <job-id> <approval-id>
  cxmsg web [--port <number>]
  cxmsg relay start|status|stop [--port <number>]
  cxmsg claude peers [--json]
  cxmsg claude bridge start|status|stop <codex-session>
  cxmsg claude send [--from <codex-session>] <claude-session> <message...>
  cxmsg claude grant [--permissions <profile>] [--approval never|relay|auto]
                     <claude-session> <codex-session>
  cxmsg claude revoke <claude-session-id> <codex-session>
  cxmsg claude grants <codex-session> [--json]
  cxmsg claude retry <job-id>
  cxmsg claude delivery <job-id> [--json]

Environment:
  CODEX_BIN          Codex executable (default: codex)
  CXMSG_SOCKET       Optional custom app-server socket
  CODEX_SESSION_NAME Sender identity set automatically by 'cxmsg open'
`;
  (exitCode === 0 ? process.stdout : process.stderr).write(output);
  process.exit(exitCode);
}

function runCodex(
  args,
  { inherit = false, env = process.env, onSpawn = null } = {},
) {
  if (inherit) {
    return new Promise((resolve, reject) => {
      const child = spawn(codexBin, args, { stdio: "inherit", env });
      child.once("error", reject);
      try {
        if (onSpawn) onSpawn(child);
      } catch (error) {
        if (child.pid) child.kill("SIGTERM");
        reject(error);
        return;
      }
      child.once("exit", (code, signal) => {
        resolve({ code: code ?? (signal ? null : 1), signal });
      });
    });
  }

  const result = spawnSync(codexBin, args, { encoding: "utf8", env });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || "codex command failed").trim());
  }
  return result.stdout.trim();
}

function readServerPid() {
  try {
    const pid = Number.parseInt(readFileSync(PID_PATH, "utf8").trim(), 10);
    return Number.isSafeInteger(pid) && pid > 1 ? pid : null;
  } catch {
    return null;
  }
}

function processCommand(pid) {
  const identity = processIdentity(pid, []);
  return identity.state === "matched" ? identity.command : null;
}

function liveAttachment(name) {
  const record = readAttachmentRecord(name);
  if (!record) return null;
  const state = processState(record.childPid);
  if (state === "missing") {
    removeAttachmentRecord(name, record.childPid);
    return null;
  }
  const command = processCommand(record.childPid);
  if (command && attachmentCommandMatches(record, command)) return record;
  if (!command) return record;
  removeAttachmentRecord(name, record.childPid);
  return null;
}

async function serverState() {
  const pid = readServerPid();
  const process = processState(pid);
  const identity = pid
    ? processIdentity(pid, ["app-server", DEFAULT_SOCKET_PATH]).state
    : "unavailable";
  const socketProbe = await probeAppServerSocket(DEFAULT_SOCKET_PATH);
  const socketPresent = existsSync(DEFAULT_SOCKET_PATH);
  return {
    pid,
    socketPresent,
    process,
    identity,
    socketProbe,
    socketHealthy: socketProbe.state === "healthy",
    ...serviceEvidence({ process, identity, socketProbe, socketPresent }),
  };
}

async function waitUntil(predicate, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

async function startServer() {
  const current = await serverState();
  if (current.running) return current.pid;
  if (current.status === "unreachable") {
    throw new Error(
      `app-server appears active, but this caller cannot connect to its UDS: ${current.socketProbe.error}. The current sandbox may prohibit Unix socket access`,
    );
  }
  if (current.pid && !current.safeToRemove) {
    throw new Error(
      `cannot verify whether app-server pid ${current.pid} is stale; refusing to replace its socket`,
    );
  }

  mkdirSync(CXMSG_STATE_DIR, { recursive: true, mode: 0o700 });
  chmodSync(CXMSG_STATE_DIR, 0o700);
  if (existsSync(DEFAULT_SOCKET_PATH)) unlinkSync(DEFAULT_SOCKET_PATH);
  if (existsSync(PID_PATH)) unlinkSync(PID_PATH);

  const logFd = openSync(LOG_PATH, "a", 0o600);
  const child = spawn(
    codexBin,
    ["app-server", "--listen", `unix://${DEFAULT_SOCKET_PATH}`],
    {
      detached: true,
      stdio: ["ignore", logFd, logFd],
    },
  );
  let childError = null;
  child.once("error", (error) => {
    childError = error;
  });
  child.unref();
  closeSync(logFd);
  if (!child.pid) throw new Error("failed to start app-server");
  writeFileSync(PID_PATH, `${child.pid}\n`, { mode: 0o600 });

  const ready = await waitUntil(
    async () => {
      if (childError) return true;
      if (processState(child.pid) === "missing") return false;
      try {
        const metadata = lstatSync(DEFAULT_SOCKET_PATH);
        if (!metadata.isSocket()) return false;
        if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
          return false;
        }
        chmodSync(DEFAULT_SOCKET_PATH, 0o600);
      } catch {
        return false;
      }
      return (await probeAppServerSocket(DEFAULT_SOCKET_PATH)).state === "healthy";
    },
  );
  if (childError) throw childError;
  if (!ready) {
    throw new Error(`app-server did not become ready; inspect ${LOG_PATH}`);
  }
  return child.pid;
}

async function ensureServer() {
  if (process.env.CXMSG_SOCKET) return;
  await startServer();
}

async function createOrFindSession(client, name, cwd = process.cwd()) {
  return withSessionLock(name, async () => {
    const existing = readSessionRecord(name);
    if (existing) {
      try {
        const read = await client.request("thread/read", {
          threadId: existing.threadId,
          includeTurns: false,
        });
        return { thread: read.thread, created: false };
      } catch (error) {
        const missing = /not found|does not exist/i.test(error.message);
        const staleEmpty =
          !existing.adopted && /thread not loaded|no rollout found/i.test(error.message);
        if (!missing && !staleEmpty) throw error;
        removeSessionRecord(name);
      }
    }

    const result = await client.request("thread/start", {
      cwd: path.resolve(cwd),
      serviceName: "cxmsg",
    });
    const storedName = storedSessionName(name);
    await client.request("thread/name/set", {
      threadId: result.thread.id,
      name: storedName,
    });
    writeSessionRecord({
      name,
      threadId: result.thread.id,
      cwd: result.thread.cwd,
      createdAt: new Date().toISOString(),
    });
    return {
      thread: { ...result.thread, name: storedName },
      created: true,
    };
  });
}

async function commandServer(action) {
  if (!action || !["start", "status", "stop"].includes(action)) usage(2);
  if (process.env.CXMSG_SOCKET) {
    throw new Error("server lifecycle commands are unavailable with CXMSG_SOCKET");
  }

  if (action === "start") {
    const pid = await startServer();
    process.stdout.write(`Codex app-server is running (pid ${pid}).\n`);
    return;
  }
  if (action === "stop") {
    const attachments = listAttachmentRecords()
      .map((record) => liveAttachment(record.name))
      .filter(Boolean);
    if (attachments.length) {
      throw new Error(
        `cannot stop app-server while remote TUI attachments are active: ${attachments.map((record) => record.name).join(", ")}`,
      );
    }
    const state = await serverState();
    if (!state.running) {
      if (state.safeToSignal) {
        process.kill(state.pid, "SIGTERM");
        await waitUntil(() => processState(state.pid) === "missing", 5_000);
        if (processState(state.pid) !== "missing") {
          throw new Error(`app-server pid ${state.pid} did not stop`);
        }
        const stopped = await serverState();
        if (stopped.running) {
          throw new Error(
            "app-server listener remained reachable after its managed pid stopped",
          );
        }
        if (existsSync(PID_PATH)) unlinkSync(PID_PATH);
        if (existsSync(DEFAULT_SOCKET_PATH)) unlinkSync(DEFAULT_SOCKET_PATH);
        process.stdout.write("Codex app-server stopped.\n");
        return;
      }
      if (state.pid && !state.safeToRemove) {
        throw new Error(
          `cannot verify app-server pid ${state.pid}; refusing to signal or remove its socket`,
        );
      }
      process.stdout.write("Codex app-server is not running.\n");
      return;
    }
    if (!state.safeToSignal) {
      throw new Error(
        `app-server is reachable but pid ${state.pid || "-"} identity is unverified; refusing to signal it`,
      );
    }
    process.kill(state.pid, "SIGTERM");
    await waitUntil(() => processState(state.pid) === "missing", 5_000);
    if (processState(state.pid) !== "missing") {
      throw new Error(`app-server pid ${state.pid} did not stop`);
    }
    const stopped = await serverState();
    if (stopped.running) {
      throw new Error("app-server listener remained reachable after its managed pid stopped");
    }
    if (existsSync(PID_PATH)) unlinkSync(PID_PATH);
    if (existsSync(DEFAULT_SOCKET_PATH)) unlinkSync(DEFAULT_SOCKET_PATH);
    process.stdout.write("Codex app-server stopped.\n");
    return;
  }

  const state = await serverState();
  const verification = state.safeToSignal
    ? "identity"
    : state.socketProbe.state === "healthy"
      ? "socket"
      : state.socketProbe.state === "denied"
        ? "sandbox-denied"
        : state.socketProbe.state;
  const error = state.socketProbe.errorCode
    ? `\terror=${state.socketProbe.errorCode}`
    : "";
  process.stdout.write(
    `${state.status}\tpid=${state.pid || "-"}\tsocket=${DEFAULT_SOCKET_PATH}\tverification=${verification}${error}\n`,
  );
}

async function commandWeb(args) {
  let port = 4173;
  while (args.length) {
    const option = args.shift();
    if (option === "--port") port = Number(args.shift());
    else throw new Error(`unknown web option: ${option}`);
  }
  await ensureServer();
  const web = await startWebServer({ port });
  process.stdout.write(
    `cxmsg web is running on ${web.origin}\n` +
      `Dashboard: ${web.origin}/dashboard\n` +
      `Orchestration: ${web.origin}/orchestration\n`,
  );

  await new Promise((resolve, reject) => {
    let closing = false;
    const close = () => {
      if (closing) return;
      closing = true;
      web.server.close((error) => (error ? reject(error) : resolve()));
    };
    process.once("SIGINT", close);
    process.once("SIGTERM", close);
    web.server.once("error", reject);
  });
}

async function relayState() {
  const record = readHostRelayRecord();
  if (!record) return { status: "stopped", record: null, error: null };
  try {
    const health = await hostRelayRequest("/health", { record });
    const matched =
      health.pid === record.pid &&
      health.port === record.port &&
      health.startedAt === record.startedAt;
    return {
      status: matched ? "running" : "mismatched",
      record,
      error: matched ? null : "host relay identity mismatch",
    };
  } catch (error) {
    const process = processState(record.pid);
    const denied = ["EPERM", "EACCES", "ETIMEDOUT"].includes(error.code);
    return {
      status: denied && process !== "missing" ? "unreachable" : "unknown",
      record,
      error: error.message,
      errorCode: error.code || null,
    };
  }
}

async function commandRelay(args) {
  const action = args.shift();
  if (!action || !["start", "status", "stop"].includes(action)) usage(2);
  let port = 4174;
  if (args[0] === "--port") {
    args.shift();
    port = Number(args.shift());
  }
  if (args.length || !Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error("relay port must be an integer between 1 and 65535");
  }
  const state = await relayState();
  if (action === "status") {
    process.stdout.write(
      `${state.status}\tpid=${state.record?.pid || "-"}\tport=${state.record?.port || "-"}${state.errorCode ? `\terror=${state.errorCode}` : ""}\n`,
    );
    return;
  }
  if (action === "stop") {
    if (!state.record) {
      process.stdout.write("cxmsg host relay is not running.\n");
      return;
    }
    const processStatus = processState(state.record.pid);
    const identity = processIdentity(
      state.record.pid,
      [hostRelayWorker, String(state.record.port)],
    ).state;
    if (processStatus !== "alive" || identity !== "matched") {
      throw new Error(
        `host relay pid ${state.record.pid} identity is unverified; refusing to signal it`,
      );
    }
    process.kill(state.record.pid, "SIGTERM");
    await waitUntil(() => processState(state.record.pid) === "missing", 5_000);
    if (processState(state.record.pid) !== "missing") {
      throw new Error(`host relay pid ${state.record.pid} did not stop`);
    }
    if (existsSync(HOST_RELAY_RECORD_PATH)) unlinkSync(HOST_RELAY_RECORD_PATH);
    process.stdout.write("cxmsg host relay stopped.\n");
    return;
  }
  if (state.status === "running") {
    process.stdout.write(`cxmsg host relay is running (pid ${state.record.pid}).\n`);
    return;
  }
  if (state.record && processState(state.record.pid) !== "missing") {
    throw new Error(
      `host relay pid ${state.record.pid} cannot be verified as stale; refusing to replace it`,
    );
  }
  if (existsSync(HOST_RELAY_RECORD_PATH)) unlinkSync(HOST_RELAY_RECORD_PATH);
  const logFd = openSync(HOST_RELAY_LOG_PATH, "a", 0o600);
  const child = spawn(process.execPath, [hostRelayWorker, String(port)], {
    detached: true,
    stdio: ["ignore", logFd, logFd],
  });
  let childError = null;
  child.once("error", (error) => {
    childError = error;
  });
  child.unref();
  closeSync(logFd);
  const ready = await waitUntil(async () => {
    if (childError) return true;
    return (await relayState()).status === "running";
  });
  if (childError) throw childError;
  if (!ready) throw new Error(`host relay did not become ready; inspect ${HOST_RELAY_LOG_PATH}`);
  process.stdout.write(`started cxmsg host relay (pid ${child.pid}, port ${port}).\n`);
}

async function commandCreate(name) {
  validateSessionName(name);
  await ensureServer();
  const result = await withAppServer((client) =>
    createOrFindSession(client, name),
  );
  process.stdout.write(
    `${result.created ? "created" : "found"} ${name} ${result.thread.id}\n`,
  );
}

async function commandRegister(name, threadId) {
  validateSessionName(name);
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(threadId || "")) {
    throw new Error("thread-id must be a UUID");
  }
  await ensureServer();

  const thread = await withAppServer(async (client) => {
    try {
      const result = await client.request("thread/read", {
        threadId,
        includeTurns: false,
      });
      return result.thread;
    } catch (error) {
      throw new Error(
        `thread ${threadId} is not addressable by this App Server: ${error.message}`,
      );
    }
  });

  await withSessionLock(name, async () => {
    const existing = readSessionRecord(name);
    if (existing && existing.threadId !== threadId) {
      throw new Error(
        `session name ${name} is already registered to ${existing.threadId}`,
      );
    }
    writeSessionRecord({
      name,
      threadId,
      cwd: thread.cwd,
      createdAt: existing?.createdAt || new Date().toISOString(),
      adopted: true,
    });
  });
  process.stdout.write(`registered ${name} ${threadId}\n`);
}

async function commandRemove(name) {
  validateSessionName(name);
  const attachment = liveAttachment(name);
  if (attachment) {
    throw new Error(`session ${name} is attached by pid ${attachment.childPid}; detach it first`);
  }
  if ((await claudeBridgeState(name)).running) {
    throw new Error(`Claude bridge for ${name} is running; stop it before removing the session`);
  }
  await ensureServer();
  const record = readSessionRecord(name);
  if (!record) throw new Error(`unknown Codex session: ${name}`);

  await withAppServer(async (client) => {
    try {
      await client.request("thread/delete", { threadId: record.threadId });
    } catch (error) {
      if (!/not found|does not exist|not persisted|thread not loaded|no rollout/i.test(error.message)) {
        throw error;
      }
    }
  });
  removeSessionRecord(name);
  process.stdout.write(`removed ${name} ${record.threadId}\n`);
}

async function markSessionManaged(name) {
  await withSessionLock(name, async () => {
    const current = readSessionRecord(name);
    if (!current) return;
    writeSessionRecord({
      ...current,
      managedByCxmsgAt: new Date().toISOString(),
    });
  });
}

async function commandOpen(name, rest, requireExisting = false) {
  validateSessionName(name);
  const attached = liveAttachment(name);
  if (attached) {
    throw new Error(`session ${name} is already attached by pid ${attached.childPid}`);
  }
  await ensureServer();
  let result;
  if (requireExisting) {
    const record = readSessionRecord(name);
    if (!record) throw new Error(`unknown Codex session: ${name}`);
    result = await withAppServer(async (client) => {
      const read = await client.request("thread/read", {
        threadId: record.threadId,
        includeTurns: false,
      });
      return { thread: read.thread, created: false };
    });
  } else {
    result = await withAppServer((client) => createOrFindSession(client, name));
  }

  const separator = rest.indexOf("--");
  const codexArgs = separator === -1 ? rest : rest.slice(separator + 1);
  const env = {
    ...process.env,
    CODEX_SESSION_NAME: name,
    PATH: `${scriptDir}:${process.env.PATH || ""}`,
  };
  let childPid = null;
  const outcome = await runCodex(
    ["resume", "--remote", socketUrl(), ...codexArgs, result.thread.id],
    {
      inherit: true,
      env,
      onSpawn(child) {
        childPid = child.pid;
        writeAttachmentRecord({
          version: 1,
          name,
          threadId: result.thread.id,
          childPid: child.pid,
          parentPid: process.pid,
          cwd: result.thread.cwd,
          startedAt: new Date().toISOString(),
        });
      },
    },
  );
  const attachment = readAttachmentRecord(name);
  const detachRequested = Boolean(
    attachment &&
      attachment.childPid === childPid &&
      attachment.detachRequestedAt,
  );
  if (childPid) removeAttachmentRecord(name, childPid);
  if (detachRequested || (!outcome.signal && outcome.code === 0)) {
    await markSessionManaged(name);
  }
  if (outcome.signal && !detachRequested) {
    throw new Error(`codex exited from signal ${outcome.signal}`);
  }
  process.exitCode = outcome.code ?? 0;
}

async function commandDetach(name) {
  validateSessionName(name);
  const session = readSessionRecord(name);
  if (!session) throw new Error(`unknown Codex session: ${name}`);
  const attachment = liveAttachment(name);
  if (!attachment) {
    process.stdout.write(`${name} is already in the background.\n`);
    return;
  }
  const command = processCommand(attachment.childPid);
  if (!command || !attachmentCommandMatches(attachment, command)) {
    throw new Error(
      `refusing to signal pid ${attachment.childPid}: it is not the ${name} remote Codex TUI`,
    );
  }
  markAttachmentDetachRequested(name, attachment.childPid);
  await markSessionManaged(name);
  const currentCommand = processCommand(attachment.childPid);
  if (currentCommand && !attachmentCommandMatches(attachment, currentCommand)) {
    throw new Error(
      `refusing to signal pid ${attachment.childPid}: its process identity changed`,
    );
  }
  if (currentCommand) process.kill(attachment.childPid, "SIGTERM");
  const stopped = await waitUntil(
    () => processState(attachment.childPid) === "missing",
    5_000,
  );
  if (!stopped) {
    throw new Error(`remote Codex TUI pid ${attachment.childPid} did not stop`);
  }
  removeAttachmentRecord(name, attachment.childPid);
  process.stdout.write(
    `detached ${name}; thread ${session.threadId} remains available in the background.\n`,
  );
}

async function commandStatus(name, jsonOutput) {
  validateSessionName(name);
  const record = readSessionRecord(name);
  if (!record) throw new Error(`unknown Codex session: ${name}`);
  const attachment = liveAttachment(name);
  await ensureServer();

  const state = await withAppServer(async (client) => {
    try {
      const thread = await readThreadMetadata(client, record.threadId);
      const threadStatus = thread.status?.type || "unknown";
      return {
        name,
        threadId: record.threadId,
        presentation: sessionPresentation(record, attachment),
        activity: threadStatus === "active" ? "working" : threadStatus,
        threadStatus,
        attachedPid: attachment?.childPid || null,
        cwd: thread.cwd || record.cwd,
        updatedAt: thread.updatedAt || null,
        managedByCxmsgAt: record.managedByCxmsgAt || null,
        error: null,
      };
    } catch (error) {
      return {
        name,
        threadId: record.threadId,
        presentation: sessionPresentation(record, attachment),
        activity: "unavailable",
        threadStatus: "unavailable",
        attachedPid: attachment?.childPid || null,
        cwd: record.cwd,
        updatedAt: null,
        managedByCxmsgAt: record.managedByCxmsgAt || null,
        error: error.message,
      };
    }
  });
  await Promise.all(activeJobsForTarget(name).map((job) => failJobIfWorkerExited(job)));
  const activeJobs = activeJobsForTarget(name);
  const awaitingApprovals = activeJobs.filter(
    (job) => job.status === "awaiting_approval",
  ).length;
  state.threadActivity = state.activity;
  state.delegatedActivity = awaitingApprovals
    ? "awaiting_approval"
    : activeJobs.length
      ? "working"
      : "idle";
  state.activeJobs = activeJobs.length;
  state.awaitingApprovals = awaitingApprovals;
  state.effectiveActivity =
    state.activity === "working"
      ? "working"
      : awaitingApprovals
        ? "awaiting-approval"
        : activeJobs.length
          ? "delegated-working"
          : state.activity;

  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
    return;
  }
  process.stdout.write(
    `${state.name}\t${state.presentation}\t${state.effectiveActivity}\tpid=${state.attachedPid || "-"}\tjobs=${state.activeJobs}\t${state.threadId}\n`,
  );
  if (state.error) process.stdout.write(`warning: ${state.error}\n`);
}

async function commandPeers(jsonOutput) {
  await ensureServer();
  const peers = await withAppServer(async (client) => {
    return Promise.all(
      listSessionRecords().map(async (record) => {
        try {
          const result = await client.request("thread/read", {
            threadId: record.threadId,
            includeTurns: false,
          });
          return {
            name: record.name,
            id: record.threadId,
            status: result.thread.status?.type || "unknown",
            cwd: result.thread.cwd,
            updatedAt: result.thread.updatedAt,
            delegators: record.allowedDelegators || [],
          };
        } catch {
          return {
            name: record.name,
            id: record.threadId,
            status: "unavailable",
            cwd: record.cwd,
            updatedAt: null,
            delegators: record.allowedDelegators || [],
          };
        }
      }),
    );
  });

  for (const peer of peers) {
    const activeJobs = activeJobsForTarget(peer.name);
    const awaitingApprovals = activeJobs.filter(
      (job) => job.status === "awaiting_approval",
    ).length;
    peer.threadStatus = peer.status;
    peer.activeJobs = activeJobs.length;
    peer.awaitingApprovals = awaitingApprovals;
    if (peer.status !== "active" && awaitingApprovals) {
      peer.status = "awaitingApproval";
    } else if (peer.status !== "active" && activeJobs.length) {
      peer.status = "delegatedWorking";
    }
  }

  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(peers, null, 2)}\n`);
    return;
  }
  if (peers.length === 0) {
    process.stdout.write("No cxmsg sessions found.\n");
    return;
  }
  for (const peer of peers) {
    process.stdout.write(
      `${peer.name}\t${peer.status}\t${peer.id.slice(0, 8)}\t${peer.cwd}\n`,
    );
  }
}

async function claudeBridgeOperationState(target) {
  const state = await claudeBridgeState(target);
  const identity = state.record
    ? processIdentity(state.record.pid, [claudeBridgeWorker, ` ${target}`]).state
    : "unavailable";
  return {
    ...state,
    identity,
    ...serviceEvidence({
      process: state.process,
      identity,
      socketProbe: state.socketProbe,
      socketPresent: state.socketPresent,
    }),
  };
}

async function commandClaudeBridge(action, target) {
  if (!action || !["start", "status", "stop"].includes(action)) usage(2);
  validateSessionName(target);

  if (action === "status") {
    const state = await claudeBridgeOperationState(target);
    const verification = state.safeToSignal
      ? "identity"
      : state.socketProbe.state === "healthy"
        ? "socket"
        : state.socketProbe.state === "denied"
          ? "sandbox-denied"
          : state.socketProbe.state;
    const error = state.socketProbe.errorCode
      ? `\terror=${state.socketProbe.errorCode}`
      : "";
    process.stdout.write(
      `${state.status}\ttarget=${target}\tpid=${state.record?.pid || "-"}\tsocket=${state.record?.socketPath || "-"}\tverification=${verification}${error}\n`,
    );
    return;
  }

  if (action === "stop") {
    const state = await claudeBridgeOperationState(target);
    if (!state.running) {
      if (state.safeToSignal) {
        process.kill(state.record.pid, "SIGTERM");
        await waitUntil(
          () => processState(state.record.pid) === "missing",
          5_000,
        );
        if (processState(state.record.pid) !== "missing") {
          throw new Error(`Claude bridge pid ${state.record.pid} did not stop`);
        }
        await removeStaleClaudeBridgeRecord(target);
        process.stdout.write(`stopped Claude bridge for ${target}\n`);
        return;
      }
      if (state.record && !state.safeToRemove) {
        throw new Error(
          `cannot verify Claude bridge pid ${state.record.pid}; refusing to signal or remove its socket`,
        );
      }
      await removeStaleClaudeBridgeRecord(target);
      process.stdout.write(`Claude bridge for ${target} is not running.\n`);
      return;
    }
    if (!state.safeToSignal) {
      throw new Error(
        `Claude bridge is reachable but pid ${state.record.pid} identity is unverified; refusing to signal it`,
      );
    }
    process.kill(state.record.pid, "SIGTERM");
    await waitUntil(() => processState(state.record.pid) === "missing", 5_000);
    if (processState(state.record.pid) !== "missing") {
      throw new Error(`Claude bridge pid ${state.record.pid} did not stop`);
    }
    await removeStaleClaudeBridgeRecord(target);
    process.stdout.write(`stopped Claude bridge for ${target}\n`);
    return;
  }

  const targetRecord = readSessionRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);
  await ensureServer();
  const current = await claudeBridgeOperationState(target);
  if (current.running) {
    process.stdout.write(
      `Claude bridge for ${target} is running (pid ${current.record.pid}).\n`,
    );
    return;
  }
  if (current.status === "unreachable") {
    throw new Error(
      `Claude bridge appears active, but this caller cannot connect to its UDS: ${current.socketProbe.error}. The current sandbox may prohibit Unix socket access`,
    );
  }
  if (current.record && !current.safeToRemove) {
    throw new Error(
      `cannot verify whether Claude bridge pid ${current.record.pid} is stale; refusing to replace its socket`,
    );
  }
  await removeStaleClaudeBridgeRecord(target);

  const logFd = openBridgeLog(target);
  const child = spawn(process.execPath, [claudeBridgeWorker, target], {
    detached: true,
    stdio: ["ignore", logFd, logFd],
  });
  let childError = null;
  child.once("error", (error) => {
    childError = error;
  });
  child.unref();
  closeBridgeLog(logFd);
  const ready = await waitUntil(() => {
    if (childError) return true;
    return claudeBridgeState(target).then(
      (state) => state.running && state.record.pid === child.pid,
    );
  });
  if (childError) throw childError;
  if (!ready) {
    throw new Error(`Claude bridge did not become ready; inspect ${bridgeLogPath(target)}`);
  }
  process.stdout.write(
    `started Claude bridge codex-${target} (pid ${child.pid})\n`,
  );
}

async function commandClaudePeers(jsonOutput) {
  const peers = await listClaudePeers();
  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(peers, null, 2)}\n`);
    return;
  }
  if (peers.length === 0) {
    process.stdout.write("No live Claude sessions found.\n");
    return;
  }
  for (const peer of peers) {
    process.stdout.write(
      `${peer.name}\t${peer.status || "unknown"}\t${peer.sessionId?.slice(0, 8) || "-"}\t${peer.address}\t${peer.cwd || "-"}\n`,
    );
  }
}

async function commandClaudeSend(args) {
  let from = process.env.CODEX_SESSION_NAME || "";
  if (args[0] === "--from") {
    from = args[1] || "";
    args = args.slice(2);
  }
  validateSessionName(from);
  const target = args.shift();
  const message = validateMessage(args.join(" "));
  const sourceRecord = readSessionRecord(from);
  if (!sourceRecord) throw new Error(`unknown Codex session: ${from}`);
  const relaySend = async (jobId = null) => {
    try {
      const delivery = await hostRelayRequest("/v1/claude/send", {
        method: "POST",
        body: { from, target, message, jobId },
      });
      process.stdout.write(
        `queued Claude delivery ${delivery.jobId} via host relay (${delivery.status}, attempt ${delivery.attempt})\n`,
      );
      return;
    } catch (error) {
      const sandboxHint = ["EPERM", "EACCES"].includes(error.code)
        ? " The current sandbox also blocks loopback TCP; invoke cxmsg through an allowed host-side tool"
        : " Start 'cxmsg relay start' from an allowed host terminal";
      throw new Error(
        `Claude UDS is unavailable to this caller and host relay connection failed before delivery: ${error.message}. destination-attempted=false.${sandboxHint}`,
      );
    }
  };
  const bridge = await claudeBridgeState(from);
  if (!bridge.running) {
    if (bridge.status === "unreachable") {
      await relaySend();
      return;
    }
    throw new Error(`Claude bridge for ${from} is ${bridge.status}`);
  }

  const peer = resolveClaudePeer(await listClaudePeers(), target);
  if (peer.status === "unreachable") {
    await relaySend();
    return;
  }
  let job = await createClaudeDeliveryJob({
    from,
    sourceRecord,
    peer,
    message,
  });
  job = await sendClaudeDeliveryJob(bridge.record, sourceRecord, job);
  if (job.status === "unreachable" && job.delivery?.errorCode === "EPERM") {
    await relaySend(job.jobId);
    return;
  }
  if (job.status !== "transport_delivered") {
    throw new Error(
      `Claude delivery ${job.jobId} failed: ${job.error || job.status}`,
    );
  }
  process.stdout.write(
    `queued Claude delivery ${job.jobId} to ${peer.name} (transport_delivered, awaiting ACK)\n`,
  );
}

async function commandClaudeGrant(args) {
  let permissions = DEFAULT_CLAUDE_PERMISSION_PROFILE;
  let approval = "never";
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--permissions") permissions = args.shift() || "";
    else if (option === "--approval") approval = args.shift() || "";
    else throw new Error(`unknown Claude grant option: ${option}`);
  }
  if (!permissions) throw new Error("permission profile is required");
  if (!APPROVAL_MODES.has(approval)) {
    throw new Error("approval must be never, relay, or auto");
  }
  const claudeTarget = args.shift();
  const codexTarget = validateSessionName(args.shift());
  if (args.length) throw new Error(`unexpected argument: ${args[0]}`);

  await ensureServer();
  const peer = resolveClaudePeer(await listClaudePeers(), claudeTarget);
  if (!peer.sessionId) {
    throw new Error(`Claude session ${peer.name} does not advertise a session-id`);
  }
  const record = readSessionRecord(codexTarget);
  if (!record) throw new Error(`unknown Codex session: ${codexTarget}`);
  const profiles = await withAppServer((client) =>
    availablePermissionProfiles(client, record.cwd),
  );
  const selected = profiles.find((profile) => profile.id === permissions);
  if (!selected) throw new Error(`unknown permission profile: ${permissions}`);
  if (!selected.allowed) throw new Error(`permission profile is blocked: ${permissions}`);

  await withSessionLock(codexTarget, async () => {
    const current = readSessionRecord(codexTarget);
    if (!current) throw new Error(`unknown Codex session: ${codexTarget}`);
    writeSessionRecord(
      upsertClaudeRequestGrant(current, peer, permissions, approval),
    );
  });
  const grant = listClaudeRequestGrants(readSessionRecord(codexTarget)).find(
    (candidate) => candidate.sessionId === peer.sessionId,
  );
  process.stdout.write(
    `granted Claude ${peer.name} (${peer.sessionId}) -> ${codexTarget} with ${permissions}, approval=${approval}\n` +
      `grant-token ${grant.token}\n`,
  );
}

async function commandClaudeRevoke(claudeSessionId, codexTarget) {
  validateClaudeSessionId(claudeSessionId);
  validateSessionName(codexTarget);
  await withSessionLock(codexTarget, async () => {
    const record = readSessionRecord(codexTarget);
    if (!record) throw new Error(`unknown Codex session: ${codexTarget}`);
    writeSessionRecord(removeClaudeRequestGrant(record, claudeSessionId));
  });
  process.stdout.write(`revoked Claude ${claudeSessionId} -> ${codexTarget}\n`);
}

function commandClaudeGrants(codexTarget, jsonOutput) {
  validateSessionName(codexTarget);
  const record = readSessionRecord(codexTarget);
  if (!record) throw new Error(`unknown Codex session: ${codexTarget}`);
  const grants = listClaudeRequestGrants(record);
  if (jsonOutput) {
    process.stdout.write(
      `${JSON.stringify(grants.map(publicClaudeRequestGrant), null, 2)}\n`,
    );
    return;
  }
  if (grants.length === 0) {
    process.stdout.write(`No Claude request grants for ${codexTarget}.\n`);
    return;
  }
  for (const grant of grants) {
    process.stdout.write(
      `${grant.sessionId}\t${grant.permissions}\t${grant.name || "-"}\t${grant.token.slice(0, 8)}…\n`,
    );
  }
}

async function commandClaudeRetry(jobId) {
  const job = readJob(jobId);
  if (!job) throw new Error(`unknown job: ${jobId}`);
  if (job.kind === "claude-delivery") {
    const sourceRecord = readSessionRecord(job.from);
    if (!sourceRecord) throw new Error(`unknown Codex session: ${job.from}`);
    const bridge = await claudeBridgeState(job.from);
    let retried;
    if (bridge.status === "unreachable") {
      retried = await hostRelayRequest("/v1/claude/send", {
        method: "POST",
        body: {
          from: job.from,
          target: job.claudeTarget?.sessionId || job.target,
          message: job.task,
          jobId,
        },
      });
      process.stdout.write(
        `retried Claude delivery ${jobId} via host relay (${retried.status})\n`,
      );
      return;
    }
    if (!bridge.running) {
      throw new Error(`Claude bridge for ${job.from} is ${bridge.status}`);
    }
    retried = await sendClaudeDeliveryJob(bridge.record, sourceRecord, job);
    if (retried.status !== "transport_delivered") {
      throw new Error(
        `Claude delivery retry failed: ${retried.error || retried.status}`,
      );
    }
    process.stdout.write(
      `retried Claude delivery ${jobId} (attempt ${retried.delivery.attempt}, awaiting ACK)\n`,
    );
    return;
  }
  if (job.kind !== "claude-request") {
    throw new Error(`job is not a Claude request: ${jobId}`);
  }
  if (isPendingJob(job)) throw new Error(`Claude request is still ${job.status}`);
  const targetRecord = readSessionRecord(job.target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${job.target}`);
  const bridge = await claudeBridgeState(job.target);
  if (!bridge.running) {
    if (bridge.status === "unreachable") {
      throw new Error(
        `Claude bridge appears active, but this caller cannot connect to its UDS: ${bridge.socketProbe.error}. The current sandbox may prohibit Unix socket access`,
      );
    }
    throw new Error(
      `Claude bridge for ${job.target} is not running; run: cxmsg claude bridge start ${job.target}`,
    );
  }
  const retried = await replyToClaudeRequest(
    bridge.record,
    targetRecord,
    job,
  );
  if (retried.reply?.status !== "delivered") {
    throw new Error(
      `Claude reply retry failed: ${retried.reply?.error || "unknown error"}`,
    );
  }
  process.stdout.write(
    `delivered Claude response ${retried.reply.messageId} for job ${jobId}\n`,
  );
}

async function commandClaudeDelivery(jobId, jsonOutput) {
  let job = readJob(jobId);
  if (!job || job.kind !== "claude-delivery") {
    throw new Error(`unknown Claude delivery: ${jobId}`);
  }
  job = await refreshClaudeDelivery(job);
  const value = {
    jobId: job.jobId,
    from: job.from,
    target: job.claudeTarget?.name || job.target,
    status: job.status,
    attempt: job.delivery?.attempt || 0,
    maxAttempts: job.delivery?.maxAttempts || 0,
    transportStatus: job.delivery?.transportStatus || null,
    deliveredAt: job.delivery?.deliveredAt || null,
    ackDeadlineAt: job.delivery?.ackDeadlineAt || null,
    nextAttemptAt: job.delivery?.nextAttemptAt || null,
    errorCode: job.delivery?.errorCode || null,
    ack: job.ack || null,
    wake: job.wake || null,
    error: job.error || null,
  };
  if (jsonOutput) process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
  else {
    process.stdout.write(
      `${value.jobId}\t${value.status}\tattempt=${value.attempt}/${value.maxAttempts}\ttarget=${value.target}${value.errorCode ? `\terror=${value.errorCode}` : ""}\n`,
    );
  }
}

async function commandClaude(args) {
  const action = args.shift();
  if (action === "peers") {
    await commandClaudePeers(args.includes("--json"));
    return;
  }
  if (action === "bridge") {
    await commandClaudeBridge(args[0], args[1]);
    return;
  }
  if (action === "send") {
    await commandClaudeSend(args);
    return;
  }
  if (action === "grant") {
    await commandClaudeGrant(args);
    return;
  }
  if (action === "revoke") {
    await commandClaudeRevoke(args[0], args[1]);
    return;
  }
  if (action === "grants") {
    commandClaudeGrants(args[0], args.includes("--json"));
    return;
  }
  if (action === "retry") {
    await commandClaudeRetry(args[0]);
    return;
  }
  if (action === "delivery") {
    const jobId = args.shift();
    const jsonOutput = args[0] === "--json";
    if (args.length > (jsonOutput ? 1 : 0)) {
      throw new Error(`unexpected option: ${args[0]}`);
    }
    await commandClaudeDelivery(jobId, jsonOutput);
    return;
  }
  usage(2);
}

async function commandGrant(sender, target, revoke = false) {
  validateSessionName(sender);
  validateSessionName(target);
  await withSessionLock(target, async () => {
    const record = readSessionRecord(target);
    if (!record) throw new Error(`unknown Codex session: ${target}`);
    const allowed = new Set(record.allowedDelegators || []);
    if (revoke) allowed.delete(sender);
    else allowed.add(sender);
    writeSessionRecord({
      ...record,
      allowedDelegators: [...allowed].sort(),
    });
  });
  process.stdout.write(
    `${revoke ? "revoked" : "granted"} ${sender} -> ${target}\n`,
  );
}

async function availablePermissionProfiles(client, cwd) {
  const result = await client.request("permissionProfile/list", { cwd });
  return result.data || [];
}

async function commandPermissions(target, jsonOutput) {
  validateSessionName(target);
  await ensureServer();
  const record = readSessionRecord(target);
  if (!record) throw new Error(`unknown Codex session: ${target}`);
  const profiles = await withAppServer((client) =>
    availablePermissionProfiles(client, record.cwd),
  );
  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(profiles, null, 2)}\n`);
    return;
  }
  for (const profile of profiles) {
    process.stdout.write(
      `${profile.id}\t${profile.allowed ? "allowed" : "blocked"}${profile.description ? `\t${profile.description}` : ""}\n`,
    );
  }
}

function parseDelegationArgs(args) {
  let from = process.env.CODEX_SESSION_NAME || "";
  let permissions = null;
  let execution = "fork";
  let approval = "never";
  let mirror = "none";
  let approvalTimeoutSeconds = 600;
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--from") from = args.shift() || "";
    else if (option === "--permissions") permissions = args.shift() || "";
    else if (option === "--execution") execution = args.shift() || "";
    else if (option === "--approval") approval = args.shift() || "";
    else if (option === "--mirror") mirror = args.shift() || "";
    else if (option === "--approval-timeout") {
      approvalTimeoutSeconds = Number(args.shift());
    }
    else throw new Error(`unknown delegate option: ${option}`);
  }
  if (!EXECUTION_MODES.has(execution)) {
    throw new Error("execution must be fork or inline");
  }
  if (!APPROVAL_MODES.has(approval)) {
    throw new Error("approval must be never, relay, or auto");
  }
  if (!MIRROR_MODES.has(mirror)) {
    throw new Error("mirror must be none, summary, or full");
  }
  if (
    !Number.isFinite(approvalTimeoutSeconds) ||
    approvalTimeoutSeconds < 1 ||
    approvalTimeoutSeconds > 86_400
  ) {
    throw new Error("approval timeout must be between 1 and 86400 seconds");
  }
  if (execution === "inline" && mirror !== "none") {
    throw new Error("inline execution already preserves context; mirror must be none");
  }
  return {
    from: validateSessionName(from),
    permissions,
    execution,
    approval,
    mirror,
    approvalTimeoutSeconds,
    target: validateSessionName(args.shift()),
    task: validateMessage(args.join(" ")),
  };
}

async function commandDelegate(args) {
  const {
    from,
    permissions,
    execution,
    approval,
    mirror,
    approvalTimeoutSeconds,
    target,
    task,
  } = parseDelegationArgs([...args]);
  if (from === target) throw new Error("cannot delegate to the same session");
  await ensureServer();
  const record = readSessionRecord(target);
  if (!record) throw new Error(`unknown Codex session: ${target}`);
  if (!(record.allowedDelegators || []).includes(from)) {
    throw new Error(
      `delegation not granted; run: cxmsg grant ${from} ${target}`,
    );
  }

  const jobId = newJobId();
  const job = createJob({
    jobId,
    from,
    target,
    targetThreadId: record.threadId,
    threadId: null,
    task,
    permissions,
    execution,
    approval,
    mirror,
    approvalTimeoutSeconds,
  });

  try {
    await withAppServer(async (client) => {
      if (permissions) {
        const profiles = await availablePermissionProfiles(client, record.cwd);
        const selected = profiles.find((profile) => profile.id === permissions);
        if (!selected) throw new Error(`unknown permission profile: ${permissions}`);
        if (!selected.allowed) {
          throw new Error(`permission profile is blocked: ${permissions}`);
        }
      }
      const thread = await readThreadMetadata(client, record.threadId);
      if (thread.status?.type === "active") {
        throw new Error("target session already has an active turn");
      }
    });
    await updateJob(job, { status: "queued" });
    const child = spawn(process.execPath, [delegationWorker, jobId], {
      detached: true,
      stdio: "ignore",
    });
    let childError = null;
    child.once("error", (error) => {
      childError = error;
    });
    child.unref();
    if (!child.pid) throw new Error("failed to start delegation worker");
    await updateJob(job, {
      workerPid: child.pid,
      workerStartedAt: new Date().toISOString(),
    });
    const workerReady = await waitUntil(() => {
      if (childError) return true;
      if (processState(child.pid) === "missing") return true;
      const current = readJob(jobId);
      return Boolean(current?.turnId || (current && !isPendingJob(current)));
    });
    if (childError) throw childError;
    let started = readJob(jobId);
    if (!started) throw new Error(`delegation job disappeared: ${jobId}`);
    started = await failJobIfWorkerExited(started);
    if (started.status === "failed") throw new Error(started.error || "delegation failed");
    if (!workerReady) throw new Error("delegation worker did not start before the readiness deadline");
    process.stdout.write(
      `delegated ${jobId} to ${target} (${execution}, ${approval}, turn ${started.turnId || "pending"})\n`,
    );
  } catch (error) {
    const current = readJob(jobId) || job;
    if (isPendingJob(current)) {
      await updateJob(current, {
        status: "failed",
        error: error.message,
        completedAt: new Date().toISOString(),
      });
    }
    throw error;
  }
}

async function commandApprovals(args) {
  const jobId = args.shift();
  const jsonOutput = args[0] === "--json";
  if (args.length > (jsonOutput ? 1 : 0)) throw new Error(`unexpected option: ${args[0]}`);
  const job = readJob(jobId);
  if (!job) throw new Error(`unknown job: ${jobId}`);
  const approvals = job.approvals || [];
  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(approvals, null, 2)}\n`);
    return;
  }
  if (!approvals.length) {
    process.stdout.write(`No approvals for job ${jobId}.\n`);
    return;
  }
  for (const approval of approvals) {
    const detail =
      approval.request?.reason ||
      approval.request?.command ||
      approval.method;
    process.stdout.write(
      `${approval.approvalId}\t${approval.status}\t${approval.method}\t${detail}\n`,
    );
  }
}

async function commandApprovalDecision(args, action) {
  const jobId = args.shift();
  const approvalId = args.shift();
  if (args.length) throw new Error(`unexpected argument: ${args[0]}`);
  await decideApproval(jobId, approvalId, action);
  process.stdout.write(`${action === "approve" ? "approved" : "denied"} ${approvalId} for job ${jobId}\n`);
}

function parseJobOptions(args, allowTimeout) {
  const jobId = args.shift();
  let jsonOutput = false;
  let timeoutSeconds = 60;
  while (args.length) {
    const option = args.shift();
    if (option === "--json") jsonOutput = true;
    else if (allowTimeout && option === "--timeout") {
      timeoutSeconds = Number(args.shift());
    } else throw new Error(`unknown option: ${option}`);
  }
  if (
    allowTimeout &&
    (!Number.isFinite(timeoutSeconds) || timeoutSeconds < 0 || timeoutSeconds > 86_400)
  ) {
    throw new Error("timeout must be between 0 and 86400 seconds");
  }
  return { jobId, jsonOutput, timeoutSeconds };
}

function printJob(job, jsonOutput) {
  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(job, null, 2)}\n`);
    return;
  }
  if (job.status === "completed" && job.result) {
    process.stdout.write(`${job.result}\n`);
    return;
  }
  const suffix = job.error ? `\t${job.error}` : "";
  process.stdout.write(`${job.status}\t${job.jobId}${suffix}\n`);
}

async function loadAndRefreshJob(jobId) {
  let job = readJob(jobId);
  if (!job) throw new Error(`unknown job: ${jobId}`);
  if (job.status === "running") {
    await ensureServer();
    job = await withAppServer((client) => refreshJob(client, job));
  }
  return failJobIfWorkerExited(job);
}

async function commandResult(args) {
  const { jobId, jsonOutput } = parseJobOptions([...args], false);
  printJob(await loadAndRefreshJob(jobId), jsonOutput);
}

async function commandWait(args) {
  const { jobId, jsonOutput, timeoutSeconds } = parseJobOptions(
    [...args],
    true,
  );
  const deadline = Date.now() + timeoutSeconds * 1_000;
  let job;
  do {
    job = await loadAndRefreshJob(jobId);
    if (!isPendingJob(job)) break;
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for job ${jobId}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  } while (true);
  printJob(job, jsonOutput);
}

async function commandSend(args) {
  let from = process.env.CODEX_SESSION_NAME || "";
  if (args[0] === "--from") {
    from = args[1] || "";
    args = args.slice(2);
  }
  validateSessionName(from);

  const target = validateSessionName(args[0]);
  const message = validateMessage(args.slice(1).join(" "));
  if (from === target) throw new Error("cannot send a peer message to the same session");

  await ensureServer();
  const delivery = await withAppServer(async (client) => {
    const targetRecord = readSessionRecord(target);
    if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);
    const thread = await readThreadMetadata(client, targetRecord.threadId);
    return deliverPeerMessage(client, thread, { from, message });
  });

  process.stdout.write(
    `delivered ${delivery.messageId} to ${target} (${delivery.delivery}, turn ${delivery.turnId})\n`,
  );
}

async function commandDoctor(args) {
  let deep = false;
  let jsonOutput = false;
  let target = null;
  while (args.length) {
    const option = args.shift();
    if (option === "--deep") deep = true;
    else if (option === "--json") jsonOutput = true;
    else if (option === "--target") {
      try {
        target = validateSessionName(args.shift());
      } catch (error) {
        error.exitCode = 2;
        throw error;
      }
    } else {
      const error = new Error(`unknown doctor option: ${option}`);
      error.exitCode = 2;
      throw error;
    }
  }

  let report;
  try {
    report = await runDoctor({ deep, target });
  } catch (error) {
    error.exitCode = 2;
    throw error;
  }
  process.stdout.write(
    jsonOutput ? `${JSON.stringify(report, null, 2)}\n` : renderDoctorText(report),
  );
  process.exitCode = doctorExitCode(report);
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  switch (command) {
    case "server":
      await commandServer(args[0]);
      break;
    case "create":
      await commandCreate(args[0]);
      break;
    case "register":
      await commandRegister(args[0], args[1]);
      break;
    case "remove":
      await commandRemove(args[0]);
      break;
    case "session":
    case "open":
      await commandOpen(args[0], args.slice(1));
      break;
    case "attach":
      await commandOpen(args[0], args.slice(1), true);
      break;
    case "detach":
    case "background":
      await commandDetach(args[0]);
      break;
    case "status":
      await commandStatus(args[0], args.includes("--json"));
      break;
    case "peers":
      await commandPeers(args.includes("--json"));
      break;
    case "send":
      await commandSend(args);
      break;
    case "grant":
      await commandGrant(args[0], args[1]);
      break;
    case "revoke":
      await commandGrant(args[0], args[1], true);
      break;
    case "permissions":
      await commandPermissions(args[0], args.includes("--json"));
      break;
    case "doctor":
      await commandDoctor(args);
      break;
    case "delegate":
      await commandDelegate(args);
      break;
    case "wait":
      await commandWait(args);
      break;
    case "result":
      await commandResult(args);
      break;
    case "approvals":
      await commandApprovals(args);
      break;
    case "approve":
      await commandApprovalDecision(args, "approve");
      break;
    case "deny":
      await commandApprovalDecision(args, "deny");
      break;
    case "web":
      await commandWeb(args);
      break;
    case "relay":
      await commandRelay(args);
      break;
    case "claude":
      await commandClaude(args);
      break;
    case "help":
    case "--help":
    case "-h":
      usage(0);
      break;
    default:
      usage(2);
  }
}

main().catch((error) => {
  process.stderr.write(`cxmsg: ${error.message}\n`);
  process.exitCode = error.exitCode || 1;
});
