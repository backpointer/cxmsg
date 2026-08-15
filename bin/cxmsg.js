#!/usr/bin/env node

import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
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
  cancelScheduledDelivery,
  findDeliveryByReplyHandle,
  listDeliveryLedgerIndexed,
  readDeliveryLedgerIndexed,
  REPLY_HANDLE_PATTERN,
  rebuildDeliveryLedgerIndex,
} from "../src/delivery-ledger.js";
import {
  APPROVAL_MODES,
  EXECUTION_MODES,
  MIRROR_MODES,
} from "../src/delegation-worker.js";
import {
  availablePermissionProfiles,
  captureScheduledDelegationTarget,
  validateDelegationAuthority,
} from "../src/delegation-authority.js";
import {
  directConversationHistory,
  ensureDirectConversation,
  listDirectConversations,
  migrateDirectConversationMember,
  publicDirectConversation,
  readDirectConversation,
} from "../src/conversations.js";
import {
  acknowledgeGroupInbox,
  changeGroupMember,
  ensureGroupConversation,
  listGroupConversations,
  listGroupInbox,
  publicGroupConversation,
  readGroupConversation,
  storeOnlyGroupMessage,
} from "../src/group-conversations.js";
import {
  publicTeamCastMentionSelection,
  publicTeamCastPlan,
  prepareTeamCastMentionMessage,
  readTeamCastMentionSelection,
  readTeamCastPlan,
  resolveTeamCastMentionSelection,
  resolveTeamCastPlan,
} from "../src/team-cast.js";
import {
  activeJobsForTarget,
  createJob,
  createScheduledDelegationJob,
  failJobIfWorkerExited,
  isPendingJob,
  listJobs,
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
  validateStoredMessage,
  validateSessionName,
} from "../src/messaging.js";
import {
  messageBodyInfo,
  readMessageBody,
} from "../src/message-bodies.js";
import {
  addClusterMember,
  addSuccessor,
  classifyExecutionThread,
  ensureCluster,
  ensureProject,
  findClusterByRoutingId,
  findProjectByRoutingId,
  listClusterMemberships,
  listClusterTombstones,
  listClusters,
  listExecutionThreads,
  listNodeTombstones,
  listNodes,
  listProjects,
  listProjectTransitions,
  listSuccessors,
  nodeKey,
  moveProject,
  projectContainsPath,
  publicCluster,
  publicClusterMembership,
  publicClusterTombstone,
  publicNode,
  publicNodeTombstone,
  publicProject,
  publicProjectTransition,
  publicExecutionThread,
  publicSuccessor,
  readExecutionThread,
  readCluster,
  readNode,
  readProject,
  recoverClusterMembership,
  removeClusterMember,
  tombstoneCluster,
  tombstoneNode,
  upsertNode,
} from "../src/node-directory.js";
import {
  listQuarantine,
  listRouteBindings,
  reconcileRouteDelivery,
  retryRouteDelivery,
  routeBindingState,
  planPeerReply,
  routePeerMessage,
  writeRouteBinding,
} from "../src/route-admission.js";
import {
  findClientUserMessage,
  findThreadTurn,
  isTerminalTurnStatus,
  listRecentTurns,
  readThreadMetadata,
  summarizeTurnLifecycle,
} from "../src/thread-activity.js";
import {
  listSessionRecords,
  readSessionRecord,
  removeSessionRecord,
  sessionAllowsAppServerResume,
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
import {
  readSchedulerRecord,
  readSchedulerIntent,
  SCHEDULER_LIFECYCLE_LOCK_PATH,
  SCHEDULER_LOG_PATH,
  SCHEDULER_RECORD_PATH,
  writeSchedulerIntent,
} from "../src/scheduler.js";
import { SCHEDULER_HEARTBEAT_STALE_MS } from "../src/delivery-policy.js";
import { withFileLock } from "../src/file-lock.js";
import { writeCoordinationEvent } from "../src/observability.js";
import { buildRetentionPlan } from "../src/retention.js";
import {
  purgeRetention,
  recoverRetentionTransactions,
  restoreRetention,
} from "../src/retention-transaction.js";

const codexBin = process.env.CODEX_BIN || "codex";
const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const claudeBridgeWorker = path.join(scriptDir, "claude-bridge-worker.js");
const delegationWorker = path.join(scriptDir, "delegation-worker.js");
const hostRelayWorker = path.join(scriptDir, "host-relay-worker.js");
const schedulerWorker = path.join(scriptDir, "scheduler-worker.js");

function usage(exitCode = 0) {
  const output = `Usage:
  cxmsg server start|status|stop
  cxmsg scheduler start|status|stop
  cxmsg deliveries list [--status <state>] [--json]
  cxmsg deliveries show <logical-message-id> [--json]
  cxmsg deliveries cancel <logical-message-id> [--json]
  cxmsg deliveries rebuild-index [--json]
  cxmsg retention plan --before <ISO timestamp> [--scope all|ledger|bodies|quarantine] [--json]
  cxmsg retention purge --before <ISO timestamp> [--scope all|ledger|bodies|quarantine] --confirm <plan-digest> [--json]
  cxmsg retention restore <backup-id> --confirm <backup-id> [--json]
  cxmsg retention recover [--json]
  cxmsg open <name> [-- <codex resume options>]
  cxmsg attach <name> [-- <codex resume options>]
  cxmsg detach <name>
  cxmsg status <name> [--json]
  cxmsg session <name> [-- <codex resume options>]  Legacy alias for 'open'
  cxmsg create <name>
  cxmsg register <name> <thread-id>
  cxmsg remove <name>
  cxmsg peers [--json]
  cxmsg send [--from <name>] [--project <id>] [--target-role <role>]
             [--sender-role <role>] [--task <id>] [--logical-message-id <uuid>]
             [--payload-type <type>] [--expiry <timestamp>]
             [--wake-policy immediate|when-idle|after-turn|after-job]
             [--after-turn <turn-id>|--after-job <job-id>]
             [--] <target> <message...>
  cxmsg reply [--from <name>] [--logical-message-id <uuid>]
              <reply-to-message-id|reply-handle> <message...>
  cxmsg route bind <session> --project <id> --role <role>
  cxmsg route show <session> [--json]
  cxmsg route list [--json]
  cxmsg route reconcile <logical-message-id> [--json]
  cxmsg route retry <logical-message-id> [--json]
  cxmsg quarantine list [--json]
  cxmsg directory project ensure <routing-id> <root> [--json]
  cxmsg directory project move <routing-id|project-id> <root> [--json] [--paths]
  cxmsg directory project-transitions [--project <routing-id|project-id>] [--json] [--paths]
  cxmsg directory sync --project <routing-id> [--codex-only|--claude-only] [--json]
  cxmsg directory projects [--json] [--paths]
  cxmsg directory cluster ensure <routing-id> [--json]
  cxmsg directory cluster show <routing-id|cluster-id> [--json] [--members] [--history]
  cxmsg directory cluster member <add|remove> <routing-id|cluster-id> <codex|claude> <native-id> [--json] [--members]
  cxmsg directory cluster recover <routing-id|cluster-id> [--json] [--members]
  cxmsg directory cluster tombstone <routing-id|cluster-id> [--reason <id>] [--json]
  cxmsg directory clusters [--json] [--members]
  cxmsg directory cluster-tombstones [--json]
  cxmsg directory nodes [--json] [--endpoints] [--history]
  cxmsg directory node show <codex|claude> <native-id> [--json] [--endpoints] [--history]
  cxmsg directory node tombstone <codex|claude> <native-id> [--reason <id>] [--json]
  cxmsg directory tombstones [--json]
  cxmsg directory successor add <codex|claude> <native-id> <codex|claude> <native-id> [--json]
  cxmsg directory successors [--json]
  cxmsg directory execution sync [--json]
  cxmsg directory execution-threads [--json]
  cxmsg directory execution-thread show <thread-id> [--json]
  cxmsg conversation direct ensure <codex|claude> <native-id> <codex|claude> <native-id> [--json]
  cxmsg conversation list [--json]
  cxmsg conversation show <conversation-id> [--json]
  cxmsg conversation history <conversation-id> [--limit <count>] [--before <sequence>] [--json]
  cxmsg conversation migrate <conversation-id> <codex|claude> <predecessor-id> <codex|claude> <successor-id> [--json]
  cxmsg conversation group ensure <label> <node-key> <node-key> <node-key...> [--id <uuid>] [--json]
  cxmsg conversation group list [--json] [--members]
  cxmsg conversation group show <conversation-id> [--json] [--members] [--history]
  cxmsg conversation group member <add|remove> <conversation-id> <node-key> [--json] [--members]
  cxmsg conversation group send <conversation-id> --from <node-key> --expiry <timestamp>
             [--logical-message-id <uuid>] [--reply-to <uuid>] [--json] -- <message...>
  cxmsg inbox list <node-key> [--limit <count>] [--all] [--json]
  cxmsg inbox ack <node-key> <conversation-id> <sequence> [--json]
  cxmsg team resolve --from <node-key> (--conversation <id> | --cluster <id> | --project <uuid> --role <role>)
             [--plan-id <uuid>] [--recipients] [--json]
  cxmsg team plan <plan-id> [--recipients] [--json]
  cxmsg team select-mentions --plan <plan-id> --from <node-key>
             --mention <node-key> [--mention <node-key>...] [--selection-id <uuid>]
             [--recipients] [--json]
  cxmsg team selection <selection-id> [--recipients] [--json]
  cxmsg team prepare --selection <selection-id> --from <node-key>
             [--logical-message-id <uuid>] [--json] -- <message...>
  cxmsg message info <message-id|reply-handle|content-ref> [--json]
  cxmsg message show <message-id|reply-handle|content-ref> [--offset <bytes>] [--limit <bytes>] [--json]
  cxmsg grant <sender> <target>
  cxmsg revoke <sender> <target>
  cxmsg permissions <target> [--json]
  cxmsg doctor [--json] [--deep] [--target <session-name>]
  cxmsg delegate [--from <name>] [--permissions <profile>] [--execution fork|inline]
                 [--approval never|relay|auto] [--approval-timeout <seconds>]
                 [--mirror none|summary|full] [--when-idle --expiry <timestamp>]
                 [--job-id <uuid>] <target> <task...>
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

function schedulerState() {
  const record = readSchedulerRecord();
  const intent = readSchedulerIntent();
  if (!record) {
    return {
      status: intent?.desiredState === "running" ? "crashed" : "stopped",
      record: null,
      intent,
      process: "missing",
      identity: "unavailable",
    };
  }
  const process = processState(record.pid);
  const identity =
    process === "missing"
      ? "unavailable"
      : processIdentity(record.pid, [schedulerWorker]).state;
  const heartbeatAgeMs = record.heartbeatAt
    ? Math.max(0, Date.now() - Date.parse(record.heartbeatAt))
    : null;
  const status =
    process === "alive" &&
    identity === "matched" &&
    record.version === 2 &&
    heartbeatAgeMs > SCHEDULER_HEARTBEAT_STALE_MS
      ? "stalled"
      : process === "alive" && identity === "matched"
        ? "running"
      : process === "missing"
        ? intent?.desiredState === "stopped" ? "stopped" : "crashed"
        : "unreachable";
  return { status, record, intent, process, identity, heartbeatAgeMs };
}

async function startScheduler() {
  return withFileLock(SCHEDULER_LIFECYCLE_LOCK_PATH, async () => {
    const current = schedulerState();
    if (current.status === "running") return current.record.pid;
    if (current.record && current.process !== "missing") {
      throw new Error(
        `cannot verify scheduler pid ${current.record.pid}; refusing to replace it`,
      );
    }
    if (current.record && existsSync(SCHEDULER_RECORD_PATH)) {
      unlinkSync(SCHEDULER_RECORD_PATH);
    }
    writeSchedulerIntent("running");
    mkdirSync(CXMSG_STATE_DIR, { recursive: true, mode: 0o700 });
    chmodSync(CXMSG_STATE_DIR, 0o700);
    const logFd = openSync(SCHEDULER_LOG_PATH, "a", 0o600);
    const child = spawn(process.execPath, [schedulerWorker], {
      detached: true,
      stdio: ["ignore", logFd, logFd],
    });
    closeSync(logFd);
    let childError = null;
    child.once("error", (error) => {
      childError = error;
    });
    child.unref();
    if (!child.pid) throw new Error("failed to start scheduler worker");
    const ready = await waitUntil(() => {
      if (childError) return true;
      const record = readSchedulerRecord();
      return record?.pid === child.pid || processState(child.pid) === "missing";
    });
    if (childError) throw childError;
    const started = schedulerState();
    if (!ready || started.status !== "running") {
      throw new Error(`scheduler did not become ready; inspect ${SCHEDULER_LOG_PATH}`);
    }
    return started.record.pid;
  });
}

async function stopScheduler() {
  return withFileLock(SCHEDULER_LIFECYCLE_LOCK_PATH, async () => {
    const state = schedulerState();
    if (!state.record) {
      writeSchedulerIntent("stopped");
      return false;
    }
    if (state.process === "missing") {
      if (existsSync(SCHEDULER_RECORD_PATH)) unlinkSync(SCHEDULER_RECORD_PATH);
      writeSchedulerIntent("stopped");
      return false;
    }
    if (state.process !== "alive" || state.identity !== "matched") {
      throw new Error(
        `scheduler pid ${state.record.pid} identity is unverified; refusing to signal it`,
      );
    }
    process.kill(state.record.pid, "SIGTERM");
    await waitUntil(() => processState(state.record.pid) === "missing", 5_000);
    if (processState(state.record.pid) !== "missing") {
      throw new Error(`scheduler pid ${state.record.pid} did not stop`);
    }
    if (existsSync(SCHEDULER_RECORD_PATH)) unlinkSync(SCHEDULER_RECORD_PATH);
    writeSchedulerIntent("stopped");
    return true;
  });
}

async function ensureScheduler() {
  await ensureServer();
  return startScheduler();
}

async function commandScheduler(action) {
  if (!action || !["start", "status", "stop"].includes(action)) usage(2);
  if (action === "start") {
    const pid = await ensureScheduler();
    process.stdout.write(`cxmsg scheduler is running (pid ${pid}).\n`);
    return;
  }
  if (action === "stop") {
    const stopped = await stopScheduler();
    process.stdout.write(`cxmsg scheduler is ${stopped ? "stopped" : "not running"}.\n`);
    return;
  }
  const state = schedulerState();
  process.stdout.write(
    `${state.status}\tpid=${state.record?.pid || "-"}\tverification=${state.identity}` +
      `${state.record?.heartbeatAt ? `\theartbeat=${state.record.heartbeatAt}` : ""}` +
      `${state.record?.lastErrorCode ? `\terror=${state.record.lastErrorCode}` : ""}\n`,
  );
}

function deliveryProjection(record) {
  if (Array.isArray(record.teamDeliveries)) {
    const states = {};
    for (const delivery of record.teamDeliveries) {
      states[delivery.state] = (states[delivery.state] || 0) + 1;
    }
    return {
      logicalMessageId: record.logicalMessage.messageId,
      kind: "team-cast",
      planId: record.logicalMessage.teamCast.planId,
      selectionId: record.logicalMessage.teamCast.selectionId,
      projectId: record.logicalMessage.teamCast.projectId,
      from: record.logicalMessage.from,
      target: `team:${record.logicalMessage.teamCast.selectionId}`,
      replyToMessageId: null,
      admissionState: "admitted",
      admissionReason: "team_cast_plan",
      wakePolicy: record.logicalMessage.teamCast.wakePolicy,
      status: Object.keys(states).length === 1 ? Object.keys(states)[0] : "partial",
      recipientCount: record.teamDeliveries.length,
      states,
      recipients: record.teamDeliveries.map((delivery) => ({
        deliveryId: delivery.deliveryId,
        targetNodeKey: delivery.targetNodeKey,
        status: delivery.state,
        errorCode: delivery.errorCode || null,
      })),
      createdAt: record.logicalMessage.createdAt,
      updatedAt: record.teamDeliveries.reduce(
        (latest, delivery) =>
          delivery.updatedAt > latest ? delivery.updatedAt : latest,
        record.logicalMessage.createdAt,
      ),
      expiry: record.logicalMessage.teamCast.expiresAt,
      body: {
        bytes: record.logicalMessage.body.bytes,
        sha256: record.logicalMessage.body.sha256,
        contentRef: record.logicalMessage.body.contentRef,
      },
    };
  }
  if (Array.isArray(record.groupDeliveries)) {
    const states = {};
    for (const delivery of record.groupDeliveries) {
      states[delivery.state] = (states[delivery.state] || 0) + 1;
    }
    return {
      logicalMessageId: record.logicalMessage.messageId,
      kind: "group",
      conversationId: record.logicalMessage.group.conversationId,
      conversationSequence: record.logicalMessage.group.sequence,
      membershipVersion: record.logicalMessage.group.membershipVersion,
      from: record.logicalMessage.from,
      target: `group:${record.logicalMessage.group.conversationId}`,
      replyToMessageId: record.logicalMessage.replyToMessageId || null,
      admissionState: "admitted",
      admissionReason: "group_membership",
      wakePolicy: "store-only",
      status: Object.keys(states).length === 1 ? Object.keys(states)[0] : "partial",
      recipientCount: record.groupDeliveries.length,
      states,
      recipients: record.groupDeliveries.map((delivery) => ({
        deliveryId: delivery.deliveryId,
        targetNodeKey: delivery.targetNodeKey,
        status: delivery.state,
        errorCode: delivery.errorCode || null,
      })),
      createdAt: record.logicalMessage.createdAt,
      updatedAt: record.groupDeliveries.reduce(
        (latest, delivery) =>
          delivery.updatedAt > latest ? delivery.updatedAt : latest,
        record.logicalMessage.createdAt,
      ),
      expiry: record.logicalMessage.group.expiry,
      body: {
        bytes: record.logicalMessage.body.bytes,
        sha256: record.logicalMessage.body.sha256,
        contentRef: record.logicalMessage.body.contentRef,
      },
    };
  }
  const delivery = record.delivery;
  const activeAttempt = delivery.attempts.at(-1) || null;
  const status =
    delivery.admissionState === "quarantined"
      ? "quarantined"
      : (["created", "scheduled"].includes(delivery.state) && activeAttempt) ||
          (delivery.state === "retryable" && delivery.attempts.length === 2)
        ? "dispatching"
        : delivery.state;
  return {
    logicalMessageId: record.logicalMessage.messageId,
    deliveryId: delivery.deliveryId,
    from: record.logicalMessage.from,
    target: delivery.target,
    replyHandle: delivery.replyHandle || null,
    replyToMessageId: record.logicalMessage.replyToMessageId || null,
    admissionState: delivery.admissionState,
    admissionReason: delivery.admissionReason,
    wakePolicy: delivery.wakePolicy,
    status,
    createdAt: record.logicalMessage.createdAt,
    updatedAt: delivery.updatedAt,
    expiry: record.logicalMessage.route?.expiry || null,
    trigger:
      delivery.wakePolicy === "after-turn"
        ? {
            kind: "turn",
            id: record.logicalMessage.route.trigger_turn_id,
          }
        : delivery.wakePolicy === "after-job"
          ? {
              kind: "job",
              id: record.logicalMessage.route.trigger_job_id,
            }
          : null,
    body: {
      bytes: record.logicalMessage.body.bytes,
      sha256: record.logicalMessage.body.sha256,
      contentRef: record.logicalMessage.body.contentRef,
    },
    attemptCount: delivery.attempts.length,
    evidenceCount: delivery.evidence.length,
    claim: delivery.claim
      ? {
          claimedAt: delivery.claim.claimedAt,
          leaseUntil: delivery.claim.leaseUntil,
        }
      : null,
    turnId: delivery.turnId || null,
    errorCode: delivery.errorCode || null,
  };
}

function printDelivery(record, jsonOutput) {
  const projected = deliveryProjection(record);
  process.stdout.write(
    jsonOutput
      ? `${JSON.stringify(projected, null, 2)}\n`
      : `${projected.logicalMessageId}\t${projected.target}\t${projected.status}\t${projected.wakePolicy}\n`,
  );
}

async function commandDeliveries(args) {
  const operation = args.shift();
  if (operation === "list") {
    let status = null;
    let jsonOutput = false;
    while (args.length) {
      const option = args.shift();
      if (option === "--json") jsonOutput = true;
      else if (option === "--status") {
        status = args.shift();
        if (!status || status.startsWith("--")) {
          throw new Error("deliveries list --status requires a state");
        }
      }
      else throw new Error(`unknown deliveries list option: ${option}`);
    }
    const records = (await listDeliveryLedgerIndexed())
      .map(deliveryProjection)
      .filter((record) => !status || record.status === status);
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(records, null, 2)}\n`
        : records
            .map(
              (record) =>
                `${record.logicalMessageId}\t${record.target}\t${record.status}\t${record.wakePolicy}`,
            )
            .join("\n") + (records.length ? "\n" : ""),
    );
    return;
  }
  if (operation === "show") {
    const messageId = args.shift();
    const jsonOutput = args.includes("--json");
    if (!messageId || args.some((value) => value !== "--json")) {
      throw new Error("deliveries show requires one logical message id and optional --json");
    }
    const record = await readDeliveryLedgerIndexed(messageId);
    if (!record) throw new Error(`unknown Delivery Ledger message: ${messageId}`);
    printDelivery(record, jsonOutput);
    return;
  }
  if (operation === "cancel") {
    const messageId = args.shift();
    const jsonOutput = args.includes("--json");
    if (!messageId || args.some((value) => value !== "--json")) {
      throw new Error("deliveries cancel requires one logical message id and optional --json");
    }
    const result = await cancelScheduledDelivery(messageId);
    await writeCoordinationEvent({
      kind: "scheduled-delivery",
      phase: "cancellation",
      correlationId: messageId,
      target: result.record.delivery.target,
      outcome: result.cancelled ? "cancelled" : "already-cancelled",
      errorCode: result.cancelled ? "EDELIVERYCANCELLED" : null,
    });
    if (jsonOutput) {
      process.stdout.write(
        `${JSON.stringify({ ...deliveryProjection(result.record), cancelled: result.cancelled }, null, 2)}\n`,
      );
    } else {
      process.stdout.write(
        `${result.cancelled ? "cancelled" : "already-cancelled"} ${messageId}\n`,
      );
    }
    return;
  }
  if (operation === "rebuild-index") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) {
      throw new Error("deliveries rebuild-index accepts only --json");
    }
    const result = await rebuildDeliveryLedgerIndex();
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(result, null, 2)}\n`
        : `rebuilt Delivery Ledger index for ${result.messageCount} message(s)\n`,
    );
    return;
  }
  usage(2);
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
    const schedulerPid = await startScheduler();
    process.stdout.write(
      `Codex app-server is running (pid ${pid}); scheduler pid ${schedulerPid}.\n`,
    );
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
    await stopScheduler();
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
  if (readExecutionThread(threadId)) {
    throw new Error("Execution Threads cannot be registered as addressable sessions");
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
  await tombstoneNode("codex", record.threadId, {
    reason: "session-removed",
    missingOk: true,
  });
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
      let recent = { data: [], nextCursor: "unavailable" };
      try {
        recent = await listRecentTurns(client, record.threadId, {
          itemsView: "notLoaded",
        });
      } catch {}
      const lifecycle = summarizeTurnLifecycle(recent);
      return {
        name,
        threadId: record.threadId,
        presentation: sessionPresentation(record, attachment),
        activity: threadStatus === "active" ? "working" : threadStatus,
        threadStatus,
        attachedPid: attachment?.childPid || null,
        cwd: thread.cwd || record.cwd,
        updatedAt: thread.updatedAt || null,
        ...lifecycle,
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
        activeTurnId: null,
        recentTerminalTurnIds: [],
        recentTurnWindowComplete: false,
        managedByCxmsgAt: record.managedByCxmsgAt || null,
        error: error.message,
      };
    }
  });
  await Promise.all(activeJobsForTarget(name).map((job) => failJobIfWorkerExited(job)));
  const activeJobs = activeJobsForTarget(name);
  const scheduledJobs = activeJobs.filter((job) => job.status === "scheduled");
  const workingJobs = activeJobs.filter((job) => job.status !== "scheduled");
  const awaitingApprovals = activeJobs.filter(
    (job) => job.status === "awaiting_approval",
  ).length;
  state.threadActivity = state.activity;
  state.delegatedActivity = awaitingApprovals
    ? "awaiting_approval"
    : workingJobs.length
      ? "working"
      : scheduledJobs.length
        ? "scheduled"
      : "idle";
  state.activeJobs = activeJobs.length;
  state.scheduledJobs = scheduledJobs.length;
  state.awaitingApprovals = awaitingApprovals;
  state.effectiveActivity =
    state.activity === "working"
      ? "working"
      : awaitingApprovals
        ? "awaiting-approval"
        : workingJobs.length
          ? "delegated-working"
          : scheduledJobs.length
            ? "delegation-scheduled"
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
    const scheduledJobs = activeJobs.filter((job) => job.status === "scheduled");
    const workingJobs = activeJobs.filter((job) => job.status !== "scheduled");
    const awaitingApprovals = activeJobs.filter(
      (job) => job.status === "awaiting_approval",
    ).length;
    peer.threadStatus = peer.status;
    peer.activeJobs = activeJobs.length;
    peer.scheduledJobs = scheduledJobs.length;
    peer.awaitingApprovals = awaitingApprovals;
    if (peer.status !== "active" && awaitingApprovals) {
      peer.status = "awaitingApproval";
    } else if (peer.status !== "active" && workingJobs.length) {
      peer.status = "delegatedWorking";
    } else if (peer.status !== "active" && scheduledJobs.length) {
      peer.status = "delegationScheduled";
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

async function sendClaudeFromCodex({
  from,
  target,
  message,
  logicalMessageId = null,
  replyToMessageId = null,
}) {
  validateSessionName(from);
  validateMessage(message);
  const sourceRecord = readSessionRecord(from);
  if (!sourceRecord) throw new Error(`unknown Codex session: ${from}`);
  const relaySend = async (jobId = null) => {
    try {
      const delivery = await hostRelayRequest("/v1/claude/send", {
        method: "POST",
        body: {
          from,
          target,
          message,
          jobId,
          logicalMessageId,
          replyToMessageId,
        },
      });
      return { ...delivery, via: "host-relay" };
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
      return relaySend();
    }
    throw new Error(`Claude bridge for ${from} is ${bridge.status}`);
  }

  const peer = resolveClaudePeer(await listClaudePeers(), target);
  if (peer.status === "unreachable") {
    return relaySend();
  }
  let job = await createClaudeDeliveryJob({
    from,
    sourceRecord,
    peer,
    message,
    logicalMessageId,
    replyToMessageId,
  });
  if (job.deduplicated) {
    return { ...job, via: "deduplicated", peerName: peer.name };
  }
  job = await sendClaudeDeliveryJob(bridge.record, sourceRecord, job);
  if (job.status === "unreachable" && job.delivery?.errorCode === "EPERM") {
    return relaySend(job.jobId);
  }
  if (job.status !== "transport_delivered") {
    throw new Error(
      `Claude delivery ${job.jobId} failed: ${job.error || job.status}`,
    );
  }
  return { ...job, via: "claude-uds", peerName: peer.name };
}

async function commandClaudeSend(args) {
  let from = process.env.CODEX_SESSION_NAME || "";
  if (args[0] === "--from") {
    from = args[1] || "";
    args = args.slice(2);
  }
  const target = args.shift();
  const message = validateMessage(args.join(" "));
  const delivery = await sendClaudeFromCodex({ from, target, message });
  process.stdout.write(
    delivery.via === "host-relay"
      ? `queued Claude delivery ${delivery.jobId} via host relay (${delivery.status}, attempt ${delivery.attempt})\n`
      : `queued Claude delivery ${delivery.jobId} to ${delivery.peerName} (transport_delivered, awaiting ACK)\n`,
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
  let whenIdle = false;
  let expiry = null;
  let jobId = null;
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--from") from = args.shift() || "";
    else if (option === "--permissions") permissions = args.shift() || "";
    else if (option === "--execution") execution = args.shift() || "";
    else if (option === "--approval") approval = args.shift() || "";
    else if (option === "--mirror") mirror = args.shift() || "";
    else if (option === "--when-idle") whenIdle = true;
    else if (option === "--expiry") expiry = args.shift() || "";
    else if (option === "--job-id") jobId = args.shift() || "";
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
  if (whenIdle !== Boolean(expiry)) {
    throw new Error("scheduled Delegation requires both --when-idle and --expiry");
  }
  if (expiry) {
    const parsed = new Date(expiry);
    const delay = parsed.getTime() - Date.now();
    if (!Number.isFinite(parsed.getTime()) || delay <= 0 || delay > 7 * 24 * 60 * 60 * 1_000) {
      throw new Error("scheduled Delegation expiry must be in the future and no more than 7 days away");
    }
    expiry = parsed.toISOString();
  }
  return {
    from: validateSessionName(from),
    permissions,
    execution,
    approval,
    mirror,
    approvalTimeoutSeconds,
    whenIdle,
    expiry,
    jobId,
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
    whenIdle,
    expiry,
    jobId: requestedJobId,
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

  if (whenIdle) {
    const identity = captureScheduledDelegationTarget(record);
    const jobId = requestedJobId || newJobId();
    const spec = {
      jobId,
      from,
      target,
      targetThreadId: record.threadId,
      task,
      permissions,
      execution,
      approval,
      mirror,
      approvalTimeoutSeconds,
      expiresAt: expiry,
      ...identity,
    };
    await withAppServer(async (client) => {
      await validateDelegationAuthority(
        {
          kind: "delegation",
          ...spec,
          schedule: {
            version: 1,
            wakePolicy: "when-idle",
            expiresAt: expiry,
            ...identity,
          },
        },
        client,
      );
      await readThreadMetadata(client, record.threadId);
    });
    const queued = await createScheduledDelegationJob(spec);
    await ensureScheduler();
    process.stdout.write(
      `${queued.created ? "scheduled" : "deduplicated"} Delegation ${jobId} to ${target} (when-idle)\n`,
    );
    return;
  }

  const jobId = requestedJobId || newJobId();
  const job = createJob({
    jobId,
    from,
    target,
    targetThreadId: record.threadId,
    threadId: record.threadId,
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

async function validatePeerTrigger({ route, targetThreadId }) {
  if (route.wake_policy === "after-job") {
    const job = readJob(route.trigger_job_id);
    if (!job || typeof job.status !== "string") {
      throw new Error(`after-job trigger does not exist or is invalid: ${route.trigger_job_id}`);
    }
    return;
  }
  if (route.wake_policy !== "after-turn") return;
  if (!targetThreadId) throw new Error("after-turn target has no registered thread");
  await ensureServer();
  const turn = await withAppServer((client) =>
    findThreadTurn(client, targetThreadId, route.trigger_turn_id),
  );
  if (!turn) throw new Error(`after-turn trigger does not exist: ${route.trigger_turn_id}`);
  if (turn.status !== "inProgress" && !isTerminalTurnStatus(turn.status)) {
    throw new Error("after-turn trigger has an unsupported status");
  }
}

async function commandSend(args) {
  let from = process.env.CODEX_SESSION_NAME || "";
  const routeOptions = {};
  let logicalMessageId = null;
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--") break;
    const value = args.shift();
    if (!value) throw new Error(`${option} requires a value`);
    if (option === "--from") from = value;
    else if (option === "--project") routeOptions.project_id = value;
    else if (option === "--target-role") routeOptions.target_role = value;
    else if (option === "--sender-role") routeOptions.sender_role = value;
    else if (option === "--task") routeOptions.task_id = value;
    else if (option === "--logical-message-id") logicalMessageId = value;
    else if (option === "--payload-type") routeOptions.payload_type = value;
    else if (option === "--expiry") routeOptions.expiry = value;
    else if (option === "--wake-policy") routeOptions.wake_policy = value;
    else if (option === "--after-turn") {
      routeOptions.wake_policy = "after-turn";
      routeOptions.trigger_turn_id = value;
    } else if (option === "--after-job") {
      routeOptions.wake_policy = "after-job";
      routeOptions.trigger_job_id = value;
    }
    else throw new Error(`unknown send option: ${option}`);
  }
  validateSessionName(from);

  const target = validateSessionName(args[0]);
  const message = validateStoredMessage(args.slice(1).join(" "));
  if (from === target) throw new Error("cannot send a peer message to the same session");
  const targetRecord = readSessionRecord(target);
  if (!targetRecord) throw new Error(`unknown Codex session: ${target}`);

  const routed = Object.keys(routeOptions).length > 0;
  if (routed && (!routeOptions.project_id || !routeOptions.target_role)) {
    throw new Error("routed send requires both --project and --target-role");
  }
  if (routed && !logicalMessageId) logicalMessageId = randomUUID();
  const route = routed
    ? {
        schema_version: 1,
        logical_message_id: logicalMessageId,
        ...routeOptions,
      }
    : null;

  const outcome = await routePeerMessage(
    { from, target, message, route, ...(logicalMessageId ? { logicalMessageId } : {}) },
    async ({ logicalMessageId: messageId, route: admittedRoute, replyHandle }) => {
      await ensureServer();
      return withAppServer(async (client) => {
        const thread = await readThreadMetadata(client, targetRecord.threadId);
        return deliverPeerMessage(client, thread, {
          from,
          message,
          messageId,
          replyHandle,
          route: admittedRoute,
        }, {
          allowResume: sessionAllowsAppServerResume(targetRecord),
        });
      });
    },
    { validateTrigger: validatePeerTrigger },
  );

  if (outcome.admissionState === "quarantined") {
    process.stderr.write(
      `cxmsg: quarantined ${outcome.logicalMessageId} for ${target}: ${outcome.reason}\n`,
    );
    process.exitCode = 1;
    return;
  }
  if (outcome.status === "scheduled") {
    await ensureScheduler();
    process.stdout.write(
      `${outcome.deduplicated ? "deduplicated" : "scheduled"} ${outcome.logicalMessageId} for ${target} (${routeOptions.wake_policy})\n`,
    );
    return;
  }
  if (outcome.deduplicated) {
    process.stdout.write(
      `deduplicated ${outcome.logicalMessageId} for ${target} (${outcome.status})\n`,
    );
    return;
  }

  const delivery = outcome.result;
  process.stdout.write(
    `delivered ${delivery.messageId} to ${target} (${delivery.delivery}, turn ${delivery.turnId})\n`,
  );
}

async function commandReply(args) {
  let from = process.env.CODEX_SESSION_NAME || "";
  let logicalMessageId = randomUUID();
  while (args[0]?.startsWith("--")) {
    const option = args.shift();
    if (option === "--") break;
    const value = args.shift();
    if (!value) throw new Error(`${option} requires a value`);
    if (option === "--from") from = value;
    else if (option === "--logical-message-id") logicalMessageId = value;
    else throw new Error(`unknown reply option: ${option}`);
  }
  validateSessionName(from);
  const replyToMessageId = args.shift();
  const message = validateStoredMessage(args.join(" "));
  const reply = planPeerReply({ from, replyToMessageId, logicalMessageId });
  if (reply.targetRuntime === "claude") {
    validateMessage(message);
    const delivery = await sendClaudeFromCodex({
      from,
      target: reply.targetNativeId,
      message,
      logicalMessageId: reply.logicalMessageId,
      replyToMessageId: reply.replyToMessageId,
    });
    process.stdout.write(
      `replied ${reply.logicalMessageId} to ${replyToMessageId} for ${reply.target} ` +
        `(Claude delivery ${delivery.jobId}, ${delivery.status})\n`,
    );
    return;
  }
  const targetRecord = readSessionRecord(reply.target);

  const outcome = await routePeerMessage(
    { ...reply, message },
    async ({
      logicalMessageId: messageId,
      route: admittedRoute,
      replyToMessageId: admittedReplyTo,
      replyHandle,
    }) => {
      await ensureServer();
      return withAppServer(async (client) => {
        const thread = await readThreadMetadata(client, targetRecord.threadId);
        return deliverPeerMessage(client, thread, {
          from,
          message,
          messageId,
          replyTo: admittedReplyTo,
          replyHandle,
          route: admittedRoute,
        }, {
          allowResume: sessionAllowsAppServerResume(targetRecord),
        });
      });
    },
  );

  if (outcome.admissionState === "quarantined") {
    process.stderr.write(
      `cxmsg: quarantined reply ${outcome.logicalMessageId} for ${reply.target}: ${outcome.reason}\n`,
    );
    process.exitCode = 1;
    return;
  }
  process.stdout.write(
    `${outcome.deduplicated ? "deduplicated" : "replied"} ${outcome.logicalMessageId} ` +
      `to ${replyToMessageId} for ${reply.target}\n`,
  );
}

async function commandRoute(args) {
  const operation = args.shift();
  if (operation === "bind") {
    const sessionName = validateSessionName(args.shift());
    const sessionRecord = readSessionRecord(sessionName);
    if (!sessionRecord) {
      throw new Error(`unknown Codex session: ${sessionName}`);
    }
    let projectId = null;
    let role = null;
    while (args.length) {
      const option = args.shift();
      const value = args.shift();
      if (!value || value.startsWith("--")) {
        throw new Error(`${option} requires a value`);
      }
      if (option === "--project") projectId = value;
      else if (option === "--role") role = value;
      else throw new Error(`unknown route bind option: ${option}`);
    }
    if (!projectId || !role) {
      throw new Error("route bind requires --project and --role");
    }
    const directoryProject = findProjectByRoutingId(projectId);
    let directoryNode = null;
    if (directoryProject) {
      if (!projectContainsPath(directoryProject, sessionRecord.cwd)) {
        throw new Error(
          `session ${sessionName} does not belong to Project ${projectId}`,
        );
      }
      directoryNode = (
        await upsertNode({
          runtimeKind: "codex",
          nativeId: sessionRecord.threadId,
          displayName: sessionName,
          projectId: directoryProject.projectId,
          endpoint: {
            transport: "codex-app-server",
            endpointId: `app-server:${sessionRecord.threadId}`,
            generation: Date.parse(sessionRecord.createdAt || "") || 0,
            status: "unknown",
            sessionName,
          },
        })
      ).record;
    }
    const binding = writeRouteBinding({
      sessionName,
      threadId: sessionRecord.threadId,
      projectId,
      projectKey: directoryProject?.projectId || null,
      nodeKey: directoryNode?.nodeKey || null,
      role,
    });
    process.stdout.write(
      `bound ${binding.sessionName} to ${binding.projectId}/${binding.role}\n`,
    );
    return;
  }
  if (operation === "show") {
    const sessionName = validateSessionName(args.shift());
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) {
      throw new Error("route show contains an unknown option");
    }
    const state = routeBindingState(sessionName);
    if (state.state === "invalid") {
      throw new Error(`route binding for session ${sessionName} is invalid`);
    }
    const binding = state.record;
    if (!binding) throw new Error(`no route binding for session: ${sessionName}`);
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(binding, null, 2)}\n`
        : `${binding.sessionName}\t${binding.projectId}\t${binding.role}\n`,
    );
    return;
  }
  if (operation === "list") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) {
      throw new Error("route list contains an unknown option");
    }
    const bindings = listRouteBindings();
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(bindings, null, 2)}\n`
        : `${bindings.map((binding) => `${binding.sessionName}\t${binding.projectId}\t${binding.role}`).join("\n")}${bindings.length ? "\n" : ""}`,
    );
    return;
  }
  if (operation === "reconcile") {
    const logicalMessageId = args.shift();
    const jsonOutput = args.includes("--json");
    if (!logicalMessageId || args.some((value) => value !== "--json")) {
      throw new Error("route reconcile requires one logical message ID");
    }
    const outcome = await reconcileRouteDelivery(
      logicalMessageId,
      async ({ targetThreadId }) => {
        await ensureServer();
        return withAppServer((client) =>
          findClientUserMessage(client, targetThreadId, logicalMessageId),
        );
      },
    );
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(outcome, null, 2)}\n`
        : `route ${outcome.logicalMessageId} ${outcome.status} (${outcome.reconciliation})\n`,
    );
    if (outcome.status === "unknown") process.exitCode = 1;
    return;
  }
  if (operation === "retry") {
    const logicalMessageId = args.shift();
    const jsonOutput = args.includes("--json");
    if (!logicalMessageId || args.some((value) => value !== "--json")) {
      throw new Error("route retry requires one logical message ID");
    }
    const outcome = await retryRouteDelivery(
      logicalMessageId,
      async (payload) => {
        const targetRecord = readSessionRecord(payload.target);
        if (!targetRecord || targetRecord.threadId !== payload.targetThreadId) {
          const error = new Error("Route Delivery target identity changed during retry");
          error.code = "ETARGETIDENTITY";
          throw error;
        }
        await ensureServer();
        return withAppServer(async (client) => {
          const thread = await readThreadMetadata(client, payload.targetThreadId);
          return deliverPeerMessage(
            client,
            thread,
            {
              from: payload.from,
              message: payload.message,
              messageId: payload.logicalMessageId,
              replyHandle: payload.replyHandle,
              route: payload.route,
            },
            { allowResume: sessionAllowsAppServerResume(targetRecord) },
          );
        });
      },
    );
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(outcome, null, 2)}\n`
        : `route ${outcome.logicalMessageId} ${outcome.status} (retry attempt 2/2)\n`,
    );
    if (outcome.status !== "turn_started") process.exitCode = 1;
    return;
  }
  usage(2);
}

function jsonOrLines(records, jsonOutput, render) {
  process.stdout.write(
    jsonOutput
      ? `${JSON.stringify(records, null, 2)}\n`
      : `${records.map(render).join("\n")}${records.length ? "\n" : ""}`,
  );
}

function codexEndpoint(record) {
  return {
    transport: "codex-app-server",
    endpointId: `app-server:${record.threadId}`,
    generation: Date.parse(record.createdAt || "") || 0,
    status: "unknown",
    sessionName: record.name,
  };
}

function claudeEndpoint(peer) {
  const reachable = ["socket", "identity"].includes(peer.verification);
  return {
    transport: "claude-uds",
    endpointId: `claude:${peer.sessionId}:${peer.pid}`,
    generation: Number.isSafeInteger(peer.startedAt)
      ? peer.startedAt
      : 0,
    status: reachable
      ? "reachable"
      : peer.status === "unreachable"
        ? "unreachable"
        : "unknown",
    address: peer.address,
  };
}

async function commandDirectory(args) {
  const operation = args.shift();
  if (operation === "project") {
    const projectOperation = args.shift();
    if (!["ensure", "move"].includes(projectOperation)) usage(2);
    const identity = args.shift();
    const root = args.shift();
    const jsonOutput = args.includes("--json");
    const includePaths = args.includes("--paths");
    if (
      !identity ||
      !root ||
      args.some((value) => !["--json", "--paths"].includes(value)) ||
      (projectOperation === "ensure" && includePaths)
    ) {
      usage(2);
    }
    const result =
      projectOperation === "ensure"
        ? { project: await ensureProject({ routingId: identity, root }), moved: false }
        : await moveProject({ project: identity, root });
    const output = {
      ...publicProject(result.project, {
        includePaths: projectOperation === "ensure" || includePaths,
      }),
      ...(projectOperation === "move"
        ? {
            moved: result.moved,
            transition: result.transition
              ? publicProjectTransition(result.transition, { includePaths })
              : null,
          }
        : {}),
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `project ${output.routingId} ${output.projectId} (${output.discoveryKind})${
            projectOperation === "move" ? ` moved=${output.moved}` : ""
          }\n`,
    );
    return;
  }
  if (operation === "project-transitions") {
    let project = null;
    let jsonOutput = false;
    let includePaths = false;
    while (args.length) {
      const option = args.shift();
      if (option === "--project") project = args.shift();
      else if (option === "--json") jsonOutput = true;
      else if (option === "--paths") includePaths = true;
      else usage(2);
    }
    let projectId = null;
    if (project) {
      const byId = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(project)
        ? readProject(project)
        : null;
      const byRouting = findProjectByRoutingId(project);
      if (byId && byRouting && byId.projectId !== byRouting.projectId) {
        throw new Error(`ambiguous Project identity: ${project}`);
      }
      const record = byId || byRouting;
      if (!record) throw new Error(`unknown Project: ${project}`);
      projectId = record.projectId;
    }
    const transitions = listProjectTransitions(projectId).map((record) =>
      publicProjectTransition(record, { includePaths }),
    );
    jsonOrLines(
      transitions,
      jsonOutput,
      (record) =>
        `${record.projectId}\t${record.kind}\t${record.transitionId}\t${record.fromDiscoveryKind}->${record.toDiscoveryKind}`,
    );
    return;
  }
  if (operation === "projects") {
    const jsonOutput = args.includes("--json");
    const includePaths = args.includes("--paths");
    if (args.some((value) => !["--json", "--paths"].includes(value))) usage(2);
    const projects = listProjects().map((record) =>
      publicProject(record, { includePaths }),
    );
    jsonOrLines(
      projects,
      jsonOutput,
      (project) => `${project.routingId}\t${project.projectId}\t${project.rootCount}`,
    );
    return;
  }
  if (operation === "clusters") {
    const jsonOutput = args.includes("--json");
    const includeMembers = args.includes("--members");
    if (args.some((value) => !["--json", "--members"].includes(value))) {
      usage(2);
    }
    const clusters = listClusters().map((record) =>
      publicCluster(record, { includeMembers }),
    );
    jsonOrLines(
      clusters,
      jsonOutput,
      (cluster) =>
        `${cluster.routingId}\t${cluster.clusterId}\t${cluster.membershipVersion}\t${cluster.memberCount}`,
    );
    return;
  }
  if (operation === "cluster-tombstones") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const tombstones = listClusterTombstones().map(publicClusterTombstone);
    jsonOrLines(
      tombstones,
      jsonOutput,
      (record) =>
        `${record.routingId}\t${record.clusterId}\t${record.lastMembershipVersion}\t${record.reason}`,
    );
    return;
  }
  if (operation === "cluster") {
    const clusterOperation = args.shift();
    if (clusterOperation === "ensure") {
      const routingId = args.shift();
      const jsonOutput = args.includes("--json");
      if (!routingId || args.some((value) => value !== "--json")) usage(2);
      const output = publicCluster(await ensureCluster({ routingId }));
      process.stdout.write(
        jsonOutput
          ? `${JSON.stringify(output, null, 2)}\n`
          : `cluster ${output.routingId} ${output.clusterId} v${output.membershipVersion}\n`,
      );
      return;
    }
    if (clusterOperation === "show") {
      const identity = args.shift();
      const jsonOutput = args.includes("--json");
      const includeMembers = args.includes("--members");
      const includeHistory = args.includes("--history");
      if (
        !identity ||
        args.some(
          (value) => !["--json", "--members", "--history"].includes(value),
        )
      ) {
        usage(2);
      }
      const uuidPattern = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
      const byId = uuidPattern.test(identity) ? readCluster(identity) : null;
      const byRouting = findClusterByRoutingId(identity);
      if (byId && byRouting && byId.clusterId !== byRouting.clusterId) {
        throw new Error(`ambiguous Cluster identity: ${identity}`);
      }
      const record = byId || byRouting;
      if (!record) throw new Error(`unknown Cluster: ${identity}`);
      const output = {
        ...publicCluster(record, { includeMembers }),
        ...(includeHistory
          ? {
              membershipHistory: listClusterMemberships(record.clusterId).map(
                (snapshot) =>
                  publicClusterMembership(snapshot, { includeMembers }),
              ),
            }
          : {}),
      };
      process.stdout.write(
        jsonOutput
          ? `${JSON.stringify(output, null, 2)}\n`
          : `${output.routingId}\t${output.clusterId}\t${output.membershipVersion}\t${output.memberCount}\n`,
      );
      return;
    }
    if (clusterOperation === "member") {
      const memberOperation = args.shift();
      const cluster = args.shift();
      const runtimeKind = args.shift();
      const nativeId = args.shift();
      const jsonOutput = args.includes("--json");
      const includeMembers = args.includes("--members");
      if (
        !["add", "remove"].includes(memberOperation) ||
        !cluster ||
        !runtimeKind ||
        !nativeId ||
        args.some((value) => !["--json", "--members"].includes(value))
      ) {
        usage(2);
      }
      const mutate =
        memberOperation === "add" ? addClusterMember : removeClusterMember;
      const output = publicCluster(
        await mutate({
          cluster,
          memberNodeKey: nodeKey(runtimeKind, nativeId),
        }),
        { includeMembers },
      );
      process.stdout.write(
        jsonOutput
          ? `${JSON.stringify(output, null, 2)}\n`
          : `cluster ${output.routingId} v${output.membershipVersion} members=${output.memberCount}\n`,
      );
      return;
    }
    if (clusterOperation === "recover") {
      const identity = args.shift();
      const jsonOutput = args.includes("--json");
      const includeMembers = args.includes("--members");
      if (
        !identity ||
        args.some((value) => !["--json", "--members"].includes(value))
      ) {
        usage(2);
      }
      const recovered = await recoverClusterMembership(identity);
      const output = {
        recovered: recovered.recovered,
        cluster: publicCluster(recovered.record, { includeMembers }),
      };
      process.stdout.write(
        jsonOutput
          ? `${JSON.stringify(output, null, 2)}\n`
          : `${output.recovered ? "recovered" : "consistent"} ${output.cluster.routingId} v${output.cluster.membershipVersion}\n`,
      );
      return;
    }
    if (clusterOperation === "tombstone") {
      const identity = args.shift();
      const jsonOutput = args.includes("--json");
      let reason = "explicit";
      const remaining = [];
      while (args.length) {
        const option = args.shift();
        if (option === "--reason") reason = args.shift();
        else if (option !== "--json") remaining.push(option);
      }
      if (!identity || !reason || remaining.length) usage(2);
      const output = publicClusterTombstone(
        await tombstoneCluster(identity, { reason }),
      );
      process.stdout.write(
        jsonOutput
          ? `${JSON.stringify(output, null, 2)}\n`
          : `cluster-tombstone ${output.routingId} ${output.clusterId} v${output.lastMembershipVersion}\n`,
      );
      return;
    }
    usage(2);
  }
  if (operation === "nodes") {
    const jsonOutput = args.includes("--json");
    const includeEndpoints = args.includes("--endpoints");
    const includeHistory = args.includes("--history");
    if (
      args.some(
        (value) => !["--json", "--endpoints", "--history"].includes(value),
      )
    ) {
      usage(2);
    }
    const nodes = listNodes().map((record) =>
      publicNode(record, { includeEndpoints, includeHistory }),
    );
    jsonOrLines(
      nodes,
      jsonOutput,
      (node) => `${node.nodeKey}\t${node.projectId}\t${node.aliases.at(-1)?.value || "-"}`,
    );
    return;
  }
  if (operation === "tombstones") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const tombstones = listNodeTombstones().map(publicNodeTombstone);
    jsonOrLines(
      tombstones,
      jsonOutput,
      (record) =>
        `${record.nodeKey}\t${record.projectId}\t${record.lastSafeLabel}\t${record.reason}`,
    );
    return;
  }
  if (operation === "successors") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const successors = listSuccessors().map(publicSuccessor);
    jsonOrLines(
      successors,
      jsonOutput,
      (record) =>
        `${record.predecessorNodeKey}\t${record.successorNodeKey}\t${record.projectId}`,
    );
    return;
  }
  if (operation === "successor") {
    if (args.shift() !== "add") usage(2);
    const predecessorRuntime = args.shift();
    const predecessorNativeId = args.shift();
    const successorRuntime = args.shift();
    const successorNativeId = args.shift();
    const jsonOutput = args.includes("--json");
    if (
      !predecessorRuntime ||
      !predecessorNativeId ||
      !successorRuntime ||
      !successorNativeId ||
      args.some((value) => value !== "--json")
    ) {
      usage(2);
    }
    const successor = publicSuccessor(
      await addSuccessor({
        predecessorNodeKey: nodeKey(predecessorRuntime, predecessorNativeId),
        successorNodeKey: nodeKey(successorRuntime, successorNativeId),
      }),
    );
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(successor, null, 2)}\n`
        : `successor ${successor.predecessorNodeKey} -> ${successor.successorNodeKey}\n`,
    );
    return;
  }
  if (operation === "execution-threads") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const records = listExecutionThreads().map(publicExecutionThread);
    jsonOrLines(
      records,
      jsonOutput,
      (record) =>
        `${record.threadId}\t${record.jobId}\t${record.creationMode}`,
    );
    return;
  }
  if (operation === "execution-thread") {
    if (args.shift() !== "show") usage(2);
    const threadId = args.shift();
    const jsonOutput = args.includes("--json");
    if (!threadId || args.some((value) => value !== "--json")) usage(2);
    const record = readExecutionThread(threadId);
    if (!record) throw new Error(`unknown Execution Thread: ${threadId}`);
    const output = publicExecutionThread(record);
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${output.threadId}\t${output.jobId}\t${output.creationMode}\n`,
    );
    return;
  }
  if (operation === "execution") {
    if (args.shift() !== "sync") usage(2);
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const uuidPattern = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
    const classified = [];
    for (const job of listJobs()) {
      if (
        (job.kind ?? "delegation") !== "delegation" ||
        job.execution !== "fork" ||
        !uuidPattern.test(job.jobId || "") ||
        !uuidPattern.test(job.targetThreadId || "") ||
        !uuidPattern.test(job.threadId || "") ||
        !uuidPattern.test(job.turnId || "") ||
        job.threadId === job.targetThreadId ||
        ["dispatching", "queued"].includes(job.status)
      ) {
        continue;
      }
      const existing = readExecutionThread(job.threadId);
      const record =
        existing ||
        (await classifyExecutionThread({
          threadId: job.threadId,
          jobId: job.jobId,
          sourceThreadId: job.targetThreadId,
          creationMode: "legacy-observed",
        }));
      classified.push(publicExecutionThread(record));
    }
    jsonOrLines(
      classified,
      jsonOutput,
      (record) =>
        `${record.threadId}\t${record.jobId}\t${record.creationMode}`,
    );
    return;
  }
  if (operation === "node") {
    const nodeOperation = args.shift();
    const runtimeKind = args.shift();
    const nativeId = args.shift();
    const jsonOutput = args.includes("--json");
    if (nodeOperation === "tombstone") {
      let reason = "explicit";
      const remaining = [];
      while (args.length) {
        const option = args.shift();
        if (option === "--reason") reason = args.shift();
        else if (option !== "--json") remaining.push(option);
      }
      if (!runtimeKind || !nativeId || !reason || remaining.length) usage(2);
      const record = publicNodeTombstone(
        await tombstoneNode(runtimeKind, nativeId, { reason }),
      );
      process.stdout.write(
        jsonOutput
          ? `${JSON.stringify(record, null, 2)}\n`
          : `tombstoned ${record.nodeKey} (${record.reason})\n`,
      );
      return;
    }
    if (nodeOperation !== "show") usage(2);
    const includeEndpoints = args.includes("--endpoints");
    const includeHistory = args.includes("--history");
    if (
      !runtimeKind ||
      !nativeId ||
      args.some(
        (value) => !["--json", "--endpoints", "--history"].includes(value),
      )
    ) {
      usage(2);
    }
    const record = readNode(runtimeKind, nativeId);
    if (!record) throw new Error(`unknown Directory Node: ${runtimeKind}:${nativeId}`);
    const node = publicNode(record, { includeEndpoints, includeHistory });
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(node, null, 2)}\n`
        : `${node.nodeKey}\t${node.projectId}\t${node.aliases.at(-1)?.value || "-"}\n`,
    );
    return;
  }
  if (operation === "sync") {
    let routingId = null;
    let includeCodex = true;
    let includeClaude = true;
    let jsonOutput = false;
    while (args.length) {
      const option = args.shift();
      if (option === "--project") routingId = args.shift();
      else if (option === "--codex-only") includeClaude = false;
      else if (option === "--claude-only") includeCodex = false;
      else if (option === "--json") jsonOutput = true;
      else throw new Error(`unknown directory sync option: ${option}`);
    }
    if (!routingId || (!includeCodex && !includeClaude)) usage(2);
    const project = findProjectByRoutingId(routingId);
    if (!project) throw new Error(`unknown Project routing id: ${routingId}`);
    const synchronized = [];
    if (includeCodex) {
      for (const record of listSessionRecords()) {
        if (!projectContainsPath(project, record.cwd)) continue;
        const result = await upsertNode({
          runtimeKind: "codex",
          nativeId: record.threadId,
          displayName: record.name,
          projectId: project.projectId,
          endpoint: codexEndpoint(record),
        });
        synchronized.push(publicNode(result.record));
      }
    }
    if (includeClaude) {
      for (const peer of await listClaudePeers()) {
        if (!projectContainsPath(project, peer.cwd)) continue;
        const result = await upsertNode({
          runtimeKind: "claude",
          nativeId: peer.sessionId,
          displayName: peer.name,
          projectId: project.projectId,
          endpoint: claudeEndpoint(peer),
        });
        synchronized.push(publicNode(result.record));
      }
    }
    jsonOrLines(
      synchronized,
      jsonOutput,
      (node) => `${node.nodeKey}\t${node.projectId}\t${node.aliases.at(-1)?.value || "-"}`,
    );
    return;
  }
  usage(2);
}

function conversationNodeKey(runtimeKind, nativeId) {
  if (!["codex", "claude"].includes(runtimeKind)) {
    throw new Error("Conversation runtime must be codex or claude");
  }
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(nativeId || "")) {
    throw new Error("Conversation native id must be a UUID");
  }
  return `${runtimeKind}:${nativeId.toLowerCase()}`;
}

function stableNodeKey(value) {
  const match = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i.exec(
    value || "",
  );
  if (!match) throw new Error("stable Node key must be codex:<uuid> or claude:<uuid>");
  return `${match[1].toLowerCase()}:${match[2].toLowerCase()}`;
}

async function commandGroupConversation(args) {
  const operation = args.shift();
  if (operation === "ensure") {
    const label = args.shift();
    let conversationId = null;
    let jsonOutput = false;
    const members = [];
    while (args.length) {
      const value = args.shift();
      if (value === "--json") jsonOutput = true;
      else if (value === "--id") {
        conversationId = args.shift();
        if (!conversationId) throw new Error("group ensure --id requires a UUID");
      } else if (value.startsWith("--")) {
        throw new Error(`unknown group ensure option: ${value}`);
      } else members.push(stableNodeKey(value));
    }
    const result = await ensureGroupConversation({
      conversationId,
      label,
      members,
    });
    const output = {
      ...publicGroupConversation(result.conversation, { members: true }),
      created: result.created,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.created ? "created" : "found"} Group Conversation ${output.conversationId}\n`,
    );
    return;
  }
  if (operation === "list") {
    const jsonOutput = args.includes("--json");
    const includeMembers = args.includes("--members");
    if (args.some((value) => !["--json", "--members"].includes(value))) usage(2);
    const records = listGroupConversations().map((record) =>
      publicGroupConversation(record, { members: includeMembers }),
    );
    jsonOrLines(
      records,
      jsonOutput,
      (record) =>
        `${record.conversationId}\t${record.label}\tmembers=${record.memberCount}\tmessages=${record.messageCount}`,
    );
    return;
  }
  if (operation === "show") {
    const conversationId = args.shift();
    const jsonOutput = args.includes("--json");
    const includeMembers = args.includes("--members");
    const includeHistory = args.includes("--history");
    if (
      !conversationId ||
      args.some(
        (value) => !["--json", "--members", "--history"].includes(value),
      )
    ) {
      usage(2);
    }
    const record = readGroupConversation(conversationId);
    if (!record) throw new Error(`unknown Group Conversation: ${conversationId}`);
    const output = publicGroupConversation(record, {
      members: includeMembers,
      history: includeHistory,
    });
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${output.conversationId}\t${output.label}\tmembers=${output.memberCount}\tmessages=${output.messageCount}\n`,
    );
    return;
  }
  if (operation === "member") {
    const action = args.shift();
    const conversationId = args.shift();
    const nodeKey = stableNodeKey(args.shift());
    const jsonOutput = args.includes("--json");
    const includeMembers = args.includes("--members");
    if (args.some((value) => !["--json", "--members"].includes(value))) usage(2);
    const result = await changeGroupMember({ conversationId, action, nodeKey });
    const output = {
      ...publicGroupConversation(result.conversation, {
        members: includeMembers,
      }),
      changed: result.changed,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.changed ? "changed" : "unchanged"} Group Conversation ${output.conversationId} membership=${output.membershipVersion}\n`,
    );
    return;
  }
  if (operation === "send") {
    const separator = args.indexOf("--");
    if (separator < 0) {
      throw new Error("group send requires -- before the message body");
    }
    const options = args.slice(0, separator);
    const message = args.slice(separator + 1).join(" ");
    const conversationId = options.shift();
    let senderNodeKey = null;
    let expiry = null;
    let logicalMessageId = randomUUID();
    let replyToMessageId = null;
    let jsonOutput = false;
    while (options.length) {
      const option = options.shift();
      if (option === "--json") jsonOutput = true;
      else if (option === "--from") senderNodeKey = stableNodeKey(options.shift());
      else if (option === "--expiry") expiry = options.shift();
      else if (option === "--logical-message-id") {
        logicalMessageId = options.shift();
      } else if (option === "--reply-to") replyToMessageId = options.shift();
      else throw new Error(`unknown group send option: ${option}`);
    }
    if (!conversationId || !senderNodeKey || !expiry || !message) usage(2);
    const result = await storeOnlyGroupMessage({
      conversationId,
      senderNodeKey,
      message,
      logicalMessageId,
      replyToMessageId,
      expiry,
    });
    const output = {
      conversationId,
      logicalMessageId: result.message.logicalMessageId,
      sequence: result.message.sequence,
      membershipVersion: result.message.membershipVersion,
      recipientCount: result.message.recipientNodeKeys.length,
      wakePolicy: "store-only",
      created: result.created,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.created ? "stored" : "deduplicated"} ${output.logicalMessageId}\trecipients=${output.recipientCount}\twake=store-only\n`,
    );
    return;
  }
  usage(2);
}

async function commandConversation(args) {
  const operation = args.shift();
  if (operation === "group") {
    await commandGroupConversation(args);
    return;
  }
  if (operation === "direct") {
    if (args.shift() !== "ensure") usage(2);
    const first = conversationNodeKey(args.shift(), args.shift());
    const second = conversationNodeKey(args.shift(), args.shift());
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const result = await ensureDirectConversation(first, second);
    const output = {
      ...publicDirectConversation(result.conversation),
      created: result.created,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.created ? "created" : "found"} Direct Conversation ${output.conversationId}\n`,
    );
    return;
  }
  if (operation === "list") {
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const records = listDirectConversations().map(publicDirectConversation);
    jsonOrLines(
      records,
      jsonOutput,
      (record) =>
        `${record.conversationId}\t${record.messageCount}\t${record.currentMembers.map((member) => member.nodeKey).join(" <-> ")}`,
    );
    return;
  }
  if (operation === "show") {
    const conversationId = args.shift();
    const jsonOutput = args.includes("--json");
    if (!conversationId || args.some((value) => value !== "--json")) usage(2);
    const record = readDirectConversation(conversationId);
    if (!record) throw new Error(`unknown Direct Conversation: ${conversationId}`);
    const output = publicDirectConversation(record);
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${output.conversationId}\t${output.messageCount}\t${output.currentMembers.map((member) => member.nodeKey).join(" <-> ")}\n`,
    );
    return;
  }
  if (operation === "history") {
    const conversationId = args.shift();
    let limit = 50;
    let beforeSequence = Number.MAX_SAFE_INTEGER;
    let jsonOutput = false;
    while (args.length) {
      const option = args.shift();
      if (option === "--json") jsonOutput = true;
      else if (option === "--limit") limit = Number(args.shift());
      else if (option === "--before") beforeSequence = Number(args.shift());
      else usage(2);
    }
    const records = directConversationHistory(conversationId, {
      limit,
      beforeSequence,
    });
    jsonOrLines(
      records,
      jsonOutput,
      (record) =>
        `${record.sequence}\t${record.logicalMessageId}\t${record.status}\t${record.senderNodeKey}->${record.recipientNodeKey}`,
    );
    return;
  }
  if (operation === "migrate") {
    const conversationId = args.shift();
    const predecessorNodeKey = conversationNodeKey(args.shift(), args.shift());
    const successorNodeKey = conversationNodeKey(args.shift(), args.shift());
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const result = await migrateDirectConversationMember({
      conversationId,
      predecessorNodeKey,
      successorNodeKey,
    });
    const output = {
      ...publicDirectConversation(result.conversation),
      migrated: result.migrated,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.migrated ? "migrated" : "unchanged"} Direct Conversation ${conversationId}\n`,
    );
    return;
  }
  usage(2);
}

async function commandInbox(args) {
  const operation = args.shift();
  if (operation === "list") {
    const nodeKey = stableNodeKey(args.shift());
    let limit = 50;
    let includeAcknowledged = false;
    let jsonOutput = false;
    while (args.length) {
      const option = args.shift();
      if (option === "--json") jsonOutput = true;
      else if (option === "--all") includeAcknowledged = true;
      else if (option === "--limit") limit = Number(args.shift());
      else throw new Error(`unknown inbox list option: ${option}`);
    }
    const records = listGroupInbox(nodeKey, { limit, includeAcknowledged });
    jsonOrLines(
      records,
      jsonOutput,
      (record) =>
        `${record.conversationId}\t${record.sequence}\t${record.logicalMessageId}\t${record.status}\t${record.acknowledged ? "acknowledged" : "unread"}`,
    );
    return;
  }
  if (operation === "ack") {
    const nodeKey = stableNodeKey(args.shift());
    const conversationId = args.shift();
    const sequence = Number(args.shift());
    const jsonOutput = args.includes("--json");
    if (args.some((value) => value !== "--json")) usage(2);
    const result = await acknowledgeGroupInbox({
      nodeKey,
      conversationId,
      sequence,
    });
    const output = { nodeKey, conversationId, sequence, changed: result.changed };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.changed ? "acknowledged" : "unchanged"} ${conversationId}:${sequence}\n`,
    );
    return;
  }
  usage(2);
}

async function commandTeam(args) {
  const operation = args.shift();
  if (operation === "prepare") {
    let selectionId = null;
    let senderNodeKey = null;
    let logicalMessageId;
    let jsonOutput = false;
    let message = null;
    while (args.length) {
      const option = args.shift();
      if (option === "--") {
        message = args.join(" ");
        args.length = 0;
      } else if (option === "--selection") selectionId = args.shift();
      else if (option === "--from") senderNodeKey = stableNodeKey(args.shift());
      else if (option === "--logical-message-id") logicalMessageId = args.shift();
      else if (option === "--json") jsonOutput = true;
      else throw new Error(`unknown Team Cast prepare option: ${option}`);
    }
    if (!selectionId || !senderNodeKey || !message) {
      throw new Error(
        "team prepare requires --selection, --from, and -- <message>",
      );
    }
    const result = await prepareTeamCastMentionMessage({
      selectionId,
      senderNodeKey,
      message,
      ...(logicalMessageId ? { logicalMessageId } : {}),
    });
    const output = {
      logicalMessageId: result.logicalMessageId,
      selectionId: result.selection.selectionId,
      recipientCount: result.selection.recipientCount,
      wakePolicy: result.selection.wakePolicy,
      status: "prepared",
      created: result.created,
      deliveryStarted: false,
      body: result.body,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.created ? "prepared" : "unchanged"}\t${output.logicalMessageId}` +
          `\trecipients=${output.recipientCount}\tdelivery-started=false\n`,
    );
    return;
  }
  if (operation === "selection") {
    const selectionId = args.shift();
    const includeRecipients = args.includes("--recipients");
    const jsonOutput = args.includes("--json");
    if (
      !selectionId ||
      args.some((value) => !["--recipients", "--json"].includes(value))
    ) {
      usage(2);
    }
    const selection = readTeamCastMentionSelection(selectionId);
    if (!selection) {
      throw new Error(`unknown Team Cast mention selection: ${selectionId}`);
    }
    const output = publicTeamCastMentionSelection(selection, {
      includeRecipients,
    });
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${output.selectionId}\tmention-wake\trecipients=${output.recipientCount}` +
          `\twake-ceiling=${output.estimatedWakeTurns}\t${output.recipientSetSha256}\n`,
    );
    return;
  }
  if (operation === "select-mentions") {
    let planId = null;
    let senderNodeKey = null;
    let selectionId;
    const mentionedNodeKeys = [];
    let includeRecipients = false;
    let jsonOutput = false;
    while (args.length) {
      const option = args.shift();
      if (option === "--plan") planId = args.shift();
      else if (option === "--from") senderNodeKey = stableNodeKey(args.shift());
      else if (option === "--mention") {
        mentionedNodeKeys.push(stableNodeKey(args.shift()));
      } else if (option === "--selection-id") selectionId = args.shift();
      else if (option === "--recipients") includeRecipients = true;
      else if (option === "--json") jsonOutput = true;
      else throw new Error(`unknown Team Cast mention option: ${option}`);
    }
    if (!planId || !senderNodeKey) {
      throw new Error("team select-mentions requires --plan and --from");
    }
    const result = await resolveTeamCastMentionSelection({
      planId,
      senderNodeKey,
      mentionedNodeKeys,
      ...(selectionId ? { selectionId } : {}),
    });
    const output = {
      ...publicTeamCastMentionSelection(result.selection, {
        includeRecipients,
      }),
      created: result.created,
      deliveryStarted: false,
    };
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${result.created ? "selected" : "unchanged"}\t${output.selectionId}` +
          `\trecipients=${output.recipientCount}\twake-ceiling=${output.estimatedWakeTurns}` +
          `\tdelivery-started=false\n`,
    );
    return;
  }
  if (operation === "plan") {
    const planId = args.shift();
    const includeRecipients = args.includes("--recipients");
    const jsonOutput = args.includes("--json");
    if (
      !planId ||
      args.some((value) => !["--recipients", "--json"].includes(value))
    ) {
      usage(2);
    }
    const plan = readTeamCastPlan(planId);
    if (!plan) throw new Error(`unknown Team Cast plan: ${planId}`);
    const output = publicTeamCastPlan(plan, { includeRecipients });
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(output, null, 2)}\n`
        : `${output.planId}\t${output.selector.kind}\trecipients=${output.recipientCount}` +
          `\twake-ceiling=${output.estimatedWakeTurns}\t${output.recipientSetSha256}\n`,
    );
    return;
  }
  if (operation !== "resolve") usage(2);
  let senderNodeKey = null;
  let planId;
  let conversationId = null;
  let clusterId = null;
  let projectId = null;
  let role = null;
  let includeRecipients = false;
  let jsonOutput = false;
  while (args.length) {
    const option = args.shift();
    if (option === "--from") senderNodeKey = stableNodeKey(args.shift());
    else if (option === "--plan-id") planId = args.shift();
    else if (option === "--conversation") conversationId = args.shift();
    else if (option === "--cluster") clusterId = args.shift();
    else if (option === "--project") projectId = args.shift();
    else if (option === "--role") role = args.shift();
    else if (option === "--recipients") includeRecipients = true;
    else if (option === "--json") jsonOutput = true;
    else throw new Error(`unknown team resolve option: ${option}`);
  }
  if (!senderNodeKey) throw new Error("team resolve requires --from");
  const selectorCount =
    Number(Boolean(conversationId)) +
    Number(Boolean(clusterId)) +
    Number(Boolean(projectId || role));
  if (
    selectorCount !== 1 ||
    (Boolean(projectId) !== Boolean(role))
  ) {
    throw new Error("team resolve requires exactly one complete selector");
  }
  const selector = conversationId
    ? { kind: "conversation", id: conversationId }
    : clusterId
      ? { kind: "cluster", id: clusterId }
      : { kind: "project-role", projectId, role };
  const result = await resolveTeamCastPlan({
    senderNodeKey,
    selector,
    ...(planId ? { planId } : {}),
  });
  const output = {
    ...publicTeamCastPlan(result.plan, { includeRecipients }),
    created: result.created,
    deliveryStarted: false,
  };
  process.stdout.write(
    jsonOutput
      ? `${JSON.stringify(output, null, 2)}\n`
      : `${result.created ? "resolved" : "unchanged"}\t${output.planId}` +
        `\trecipients=${output.recipientCount}\twake-ceiling=${output.estimatedWakeTurns}` +
        `\tdelivery-started=false\n`,
  );
}

async function commandQuarantine(args) {
  const operation = args.shift();
  if (operation !== "list") usage(2);
  const jsonOutput = args.includes("--json");
  if (args.some((value) => value !== "--json")) {
    throw new Error("quarantine list contains an unknown option");
  }
  const records = listQuarantine();
  process.stdout.write(
    jsonOutput
      ? `${JSON.stringify(records, null, 2)}\n`
      : `${records.map((record) => `${record.logicalMessageId}\t${record.target}\t${record.reason}`).join("\n")}${records.length ? "\n" : ""}`,
  );
}

function parseBodyReadInteger(option, value, { allowZero = false } = {}) {
  if (!/^\d+$/.test(value || "")) {
    throw new Error(`${option} must be an integer`);
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isSafeInteger(parsed) || (allowZero ? parsed < 0 : parsed < 1)) {
    throw new Error(`${option} is out of range`);
  }
  return parsed;
}

async function commandMessage(args) {
  const operation = args.shift();
  let reference = args.shift();
  if (!reference || !["info", "show"].includes(operation)) usage(2);
  let jsonOutput = false;
  let offset = 0;
  let limit;
  while (args.length) {
    const option = args.shift();
    if (option === "--json") jsonOutput = true;
    else if (option === "--offset") {
      offset = parseBodyReadInteger(option, args.shift(), { allowZero: true });
    } else if (option === "--limit") {
      limit = parseBodyReadInteger(option, args.shift());
    } else {
      throw new Error(`unknown message option: ${option}`);
    }
  }

  if (REPLY_HANDLE_PATTERN.test(reference)) {
    const current = process.env.CODEX_SESSION_NAME || "";
    validateSessionName(current);
    const record = readSessionRecord(current);
    if (!record) throw new Error(`unknown Codex session: ${current}`);
    const delivery = findDeliveryByReplyHandle({
      replyHandle: reference,
      target: current,
      targetThreadId: record.threadId,
    });
    if (!delivery) throw new Error(`unknown peer reply handle: ${reference}`);
    reference = delivery.logicalMessage.messageId;
  }

  if (operation === "info") {
    if (offset !== 0 || limit !== undefined) {
      throw new Error("message info does not accept offset or limit");
    }
    const info = messageBodyInfo(reference);
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(info, null, 2)}\n`
        : `${info.messageId}\t${info.bodyBytes}\t${info.bodySha256}\n`,
    );
    return;
  }

  const result = readMessageBody(reference, { offset, ...(limit ? { limit } : {}) });
  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return;
  }
  process.stdout.write(result.text);
  if (!result.complete) {
    process.stderr.write(
      `\ncxmsg: partial message body; next offset ${result.nextOffset} of ${result.bodyBytes} bytes\n`,
    );
  }
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

async function commandRetention(args) {
  const operation = args.shift();
  if (!["plan", "purge", "restore", "recover"].includes(operation)) usage(2);
  let before = null;
  let scope = "all";
  let jsonOutput = false;
  let confirm = null;
  const backupId = operation === "restore" ? args.shift() || null : null;
  while (args.length) {
    const option = args.shift();
    if (option === "--before") before = args.shift() || null;
    else if (option === "--scope") scope = args.shift() || "";
    else if (option === "--confirm") confirm = args.shift() || null;
    else if (option === "--json") jsonOutput = true;
    else throw new Error(`unknown retention ${operation} option: ${option}`);
  }
  if (operation === "recover") {
    const recovered = await recoverRetentionTransactions();
    if (jsonOutput) process.stdout.write(`${JSON.stringify({ recovered }, null, 2)}\n`);
    else process.stdout.write(`retention recovery complete\trecovered=${recovered.length}\n`);
    return;
  }
  if (operation === "restore") {
    if (!backupId) throw new Error("retention restore requires <backup-id>");
    if (confirm !== backupId) {
      throw new Error("retention restore requires --confirm with the exact backup id");
    }
    const receipt = await restoreRetention({ backupId });
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(receipt, null, 2)}\n`
        : `retention restored\tbackup-id=${receipt.backupId}\trestore-id=${receipt.restoreId}\n`,
    );
    return;
  }
  if (!before) throw new Error(`retention ${operation} requires --before <ISO timestamp>`);
  if (operation === "purge") {
    if (!confirm) throw new Error("retention purge requires --confirm <plan-digest>");
    const receipt = await purgeRetention({
      before,
      scope,
      expectedPlanDigest: confirm,
    });
    process.stdout.write(
      jsonOutput
        ? `${JSON.stringify(receipt, null, 2)}\n`
        : `retention purge committed\tbackup-id=${receipt.backupId}` +
          `\tplan-digest=${receipt.planDigest}\n`,
    );
    return;
  }
  const plan = await buildRetentionPlan({ before, scope });
  if (jsonOutput) {
    process.stdout.write(`${JSON.stringify(plan, null, 2)}\n`);
    return;
  }
  process.stdout.write(
    `retention plan; automatic deletion=false, explicit mutation=true, cutoff=${plan.cutoff}` +
      `, plan-digest=${plan.planDigest}\n`,
  );
  for (const [kind, category] of Object.entries(plan.categories)) {
    process.stdout.write(
      `${kind}\teligible=${category.eligible.length}\tblocked=${category.blocked.length}` +
        `\tretained-by-age=${category.retainedByAge}\testimated-bytes=${category.estimatedBytes}\n`,
    );
  }
}

async function main() {
  const [command, ...args] = process.argv.slice(2);
  switch (command) {
    case "server":
      await commandServer(args[0]);
      break;
    case "scheduler":
      await commandScheduler(args[0]);
      break;
    case "deliveries":
      await commandDeliveries(args);
      break;
    case "conversation":
      await commandConversation(args);
      break;
    case "inbox":
      await commandInbox(args);
      break;
    case "team":
      await commandTeam(args);
      break;
    case "retention":
      await commandRetention(args);
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
    case "reply":
      await commandReply(args);
      break;
    case "route":
      await commandRoute(args);
      break;
    case "quarantine":
      await commandQuarantine(args);
      break;
    case "directory":
      await commandDirectory(args);
      break;
    case "message":
      await commandMessage(args);
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
