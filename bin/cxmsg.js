#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import {
  chmodSync,
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { withAppServer } from "../src/app-server-client.js";
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
  buildClaudePeerFrame,
  listClaudePeers,
  resolveClaudePeer,
  sendClaudePeerFrame,
} from "../src/claude-messaging.js";
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
import {
  createJob,
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
  deliverDelegatedTask,
  deliverPeerMessage,
  storedSessionName,
  validateMessage,
  validateSessionName,
} from "../src/messaging.js";
import {
  listSessionRecords,
  readSessionRecord,
  removeSessionRecord,
  withSessionLock,
  writeSessionRecord,
} from "../src/registry.js";

const codexBin = process.env.CODEX_BIN || "codex";
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const claudeBridgeWorker = path.join(scriptDir, "claude-bridge-worker.js");

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
  cxmsg delegate [--from <name>] [--permissions <profile>] <target> <task...>
  cxmsg wait <job-id> [--timeout <seconds>] [--json]
  cxmsg result <job-id> [--json]
  cxmsg claude peers [--json]
  cxmsg claude bridge start|status|stop <codex-session>
  cxmsg claude send [--from <codex-session>] <claude-session> <message...>
  cxmsg claude grant [--permissions <profile>] <claude-session> <codex-session>
  cxmsg claude revoke <claude-session-id> <codex-session>
  cxmsg claude grants <codex-session> [--json]
  cxmsg claude retry <job-id>

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

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function processMatchesServer(pid) {
  const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
    encoding: "utf8",
  });
  return (
    result.status === 0 &&
    result.stdout.includes("app-server") &&
    result.stdout.includes(DEFAULT_SOCKET_PATH)
  );
}

function processCommand(pid) {
  const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
    encoding: "utf8",
  });
  return result.status === 0 ? result.stdout.trim() : null;
}

function liveAttachment(name) {
  const record = readAttachmentRecord(name);
  if (!record) return null;
  const command = processCommand(record.childPid);
  if (command && attachmentCommandMatches(record, command)) return record;
  removeAttachmentRecord(name, record.childPid);
  return null;
}

function serverState() {
  const pid = readServerPid();
  return {
    pid,
    running: Boolean(pid && processExists(pid) && processMatchesServer(pid)),
    socketPresent: existsSync(DEFAULT_SOCKET_PATH),
  };
}

async function waitUntil(predicate, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  return false;
}

async function startServer() {
  const current = serverState();
  if (current.running && current.socketPresent) return current.pid;

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
  child.unref();
  closeSync(logFd);
  writeFileSync(PID_PATH, `${child.pid}\n`, { mode: 0o600 });

  const ready = await waitUntil(
    () => processExists(child.pid) && existsSync(DEFAULT_SOCKET_PATH),
  );
  if (!ready) {
    throw new Error(`app-server did not become ready; inspect ${LOG_PATH}`);
  }
  chmodSync(DEFAULT_SOCKET_PATH, 0o600);
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
    const state = serverState();
    if (!state.running) {
      process.stdout.write("Codex app-server is not running.\n");
      return;
    }
    process.kill(state.pid, "SIGTERM");
    await waitUntil(() => !processExists(state.pid), 5_000);
    if (processExists(state.pid)) {
      throw new Error(`app-server pid ${state.pid} did not stop`);
    }
    if (existsSync(PID_PATH)) unlinkSync(PID_PATH);
    if (existsSync(DEFAULT_SOCKET_PATH)) unlinkSync(DEFAULT_SOCKET_PATH);
    process.stdout.write("Codex app-server stopped.\n");
    return;
  }

  const state = serverState();
  process.stdout.write(
    `${state.running ? "running" : "stopped"}\tpid=${state.pid || "-"}\tsocket=${DEFAULT_SOCKET_PATH}\n`,
  );
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
  if (claudeBridgeState(name).running) {
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
    () => !processExists(attachment.childPid),
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
      const result = await client.request("thread/read", {
        threadId: record.threadId,
        includeTurns: true,
      });
      const threadStatus = result.thread.status?.type || "unknown";
      return {
        name,
        threadId: record.threadId,
        presentation: sessionPresentation(record, attachment),
        activity: threadStatus === "active" ? "working" : threadStatus,
        threadStatus,
        attachedPid: attachment?.childPid || null,
        cwd: result.thread.cwd || record.cwd,
        updatedAt: result.thread.updatedAt || null,
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

  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(state, null, 2)}\n`);
    return;
  }
  process.stdout.write(
    `${state.name}\t${state.presentation}\t${state.activity}\tpid=${state.attachedPid || "-"}\t${state.threadId}\n`,
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

function processMatchesClaudeBridge(pid, target) {
  const result = spawnSync("ps", ["-p", String(pid), "-o", "command="], {
    encoding: "utf8",
  });
  return (
    result.status === 0 &&
    result.stdout.includes(claudeBridgeWorker) &&
    result.stdout.includes(` ${target}`)
  );
}

async function commandClaudeBridge(action, target) {
  if (!action || !["start", "status", "stop"].includes(action)) usage(2);
  validateSessionName(target);

  if (action === "status") {
    const state = claudeBridgeState(target);
    const running = Boolean(
      state.running && processMatchesClaudeBridge(state.record.pid, target),
    );
    process.stdout.write(
      `${running ? "running" : "stopped"}\ttarget=${target}\tpid=${state.record?.pid || "-"}\tsocket=${state.record?.socketPath || "-"}\n`,
    );
    return;
  }

  if (action === "stop") {
    const state = claudeBridgeState(target);
    if (!state.running) {
      removeStaleClaudeBridgeRecord(target);
      process.stdout.write(`Claude bridge for ${target} is not running.\n`);
      return;
    }
    if (!processMatchesClaudeBridge(state.record.pid, target)) {
      throw new Error(`refusing to stop pid ${state.record.pid}: it is not the ${target} Claude bridge`);
    }
    process.kill(state.record.pid, "SIGTERM");
    await waitUntil(() => !processExists(state.record.pid), 5_000);
    if (processExists(state.record.pid)) {
      throw new Error(`Claude bridge pid ${state.record.pid} did not stop`);
    }
    removeStaleClaudeBridgeRecord(target);
    process.stdout.write(`stopped Claude bridge for ${target}\n`);
    return;
  }

  const targetRecord = readSessionRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);
  await ensureServer();
  const current = claudeBridgeState(target);
  if (current.running && processMatchesClaudeBridge(current.record.pid, target)) {
    process.stdout.write(
      `Claude bridge for ${target} is running (pid ${current.record.pid}).\n`,
    );
    return;
  }
  removeStaleClaudeBridgeRecord(target);

  const logFd = openBridgeLog(target);
  const child = spawn(process.execPath, [claudeBridgeWorker, target], {
    detached: true,
    stdio: ["ignore", logFd, logFd],
  });
  child.unref();
  closeBridgeLog(logFd);
  const ready = await waitUntil(() => {
    const state = claudeBridgeState(target);
    return state.running && state.record.pid === child.pid;
  });
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
  const bridge = claudeBridgeState(from);
  if (
    !bridge.running ||
    !processMatchesClaudeBridge(bridge.record.pid, from)
  ) {
    throw new Error(`Claude bridge for ${from} is not running; run: cxmsg claude bridge start ${from}`);
  }

  const peer = resolveClaudePeer(await listClaudePeers(), target);
  const frame = buildClaudePeerFrame({
    fromSocket: bridge.record.socketPath,
    fromName: `codex-${from}`,
    fromSession: sourceRecord.threadId,
    message,
  });
  const delivery = await sendClaudePeerFrame(peer.socketPath, frame);
  process.stdout.write(
    `delivered ${delivery.messageId} from ${from} to Claude ${peer.name} (${peer.address})\n`,
  );
}

async function commandClaudeGrant(args) {
  let permissions = DEFAULT_CLAUDE_PERMISSION_PROFILE;
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--permissions") permissions = args.shift() || "";
    else throw new Error(`unknown Claude grant option: ${option}`);
  }
  if (!permissions) throw new Error("permission profile is required");
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
    writeSessionRecord(upsertClaudeRequestGrant(current, peer, permissions));
  });
  const grant = listClaudeRequestGrants(readSessionRecord(codexTarget)).find(
    (candidate) => candidate.sessionId === peer.sessionId,
  );
  process.stdout.write(
    `granted Claude ${peer.name} (${peer.sessionId}) -> ${codexTarget} with ${permissions}\n` +
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
  if (job.kind !== "claude-request") {
    throw new Error(`job is not a Claude request: ${jobId}`);
  }
  if (isPendingJob(job)) throw new Error(`Claude request is still ${job.status}`);
  const targetRecord = readSessionRecord(job.target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${job.target}`);
  const bridge = claudeBridgeState(job.target);
  if (
    !bridge.running ||
    !processMatchesClaudeBridge(bridge.record.pid, job.target)
  ) {
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
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--from") from = args.shift() || "";
    else if (option === "--permissions") permissions = args.shift() || "";
    else throw new Error(`unknown delegate option: ${option}`);
  }
  return {
    from: validateSessionName(from),
    permissions,
    target: validateSessionName(args.shift()),
    task: validateMessage(args.join(" ")),
  };
}

async function commandDelegate(args) {
  const { from, permissions, target, task } = parseDelegationArgs([...args]);
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
  });

  try {
    const delivery = await withAppServer(async (client) => {
      if (permissions) {
        const profiles = await availablePermissionProfiles(client, record.cwd);
        const selected = profiles.find((profile) => profile.id === permissions);
        if (!selected) throw new Error(`unknown permission profile: ${permissions}`);
        if (!selected.allowed) {
          throw new Error(`permission profile is blocked: ${permissions}`);
        }
      }
      const read = await client.request("thread/read", {
        threadId: record.threadId,
        includeTurns: true,
      });
      if (read.thread.status?.type === "active") {
        throw new Error("target session already has an active turn");
      }

      const forkParams = {
        threadId: record.threadId,
        approvalPolicy: "never",
        deferGoalContinuation: true,
      };
      if (permissions) forkParams.permissions = permissions;

      let executionThread;
      try {
        const forked = await client.request("thread/fork", forkParams);
        executionThread = forked.thread;
      } catch (error) {
        if (!/no rollout found|thread not loaded/i.test(error.message)) throw error;
        const startParams = {
          cwd: record.cwd,
          serviceName: "cxmsg-delegate",
          approvalPolicy: "never",
        };
        if (permissions) startParams.permissions = permissions;
        const started = await client.request("thread/start", startParams);
        executionThread = started.thread;
      }

      return deliverDelegatedTask(client, executionThread, {
        from,
        target,
        task,
        jobId,
      });
    });
    updateJob(job, {
      status: "running",
      threadId: delivery.threadId,
      turnId: delivery.turnId,
      turnStartedAt: new Date().toISOString(),
    });
    process.stdout.write(
      `delegated ${jobId} to ${target} (turn ${delivery.turnId})\n`,
    );
  } catch (error) {
    updateJob(job, { status: "failed", error: error.message });
    throw error;
  }
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
  if (job.status !== "running") return job;
  await ensureServer();
  job = await withAppServer((client) => refreshJob(client, job));
  return job;
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
    const read = await client.request("thread/read", {
      threadId: targetRecord.threadId,
      includeTurns: true,
    });
    return deliverPeerMessage(client, read.thread, { from, message });
  });

  process.stdout.write(
    `delivered ${delivery.messageId} to ${target} (${delivery.delivery}, turn ${delivery.turnId})\n`,
  );
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
    case "delegate":
      await commandDelegate(args);
      break;
    case "wait":
      await commandWait(args);
      break;
    case "result":
      await commandResult(args);
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
  process.exitCode = 1;
});
