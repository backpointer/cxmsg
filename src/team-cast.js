import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { readDirectConversation } from "./conversations.js";
import {
  appendTeamCastRecipientEvidence,
  beginTeamCastRecipientDelivery,
  commitPreparedTeamCastDelivery,
  readDeliveryLedgerIndexed,
  scheduleTeamCastRecipientDelivery,
} from "./delivery-ledger.js";
import { withFileLock } from "./file-lock.js";
import { readGroupConversation } from "./group-conversations.js";
import {
  MAX_STORED_MESSAGE_BYTES,
  readWholeMessageBody,
  storeMessageBody,
} from "./message-bodies.js";
import {
  findClusterByRoutingId,
  readCluster,
  readClusterMembership,
  readExecutionThread,
  readNode,
  readNodeTombstone,
  readProject,
} from "./node-directory.js";
import { readSessionRecord } from "./registry.js";
import { listRouteBindingsStrict } from "./route-admission.js";
import { withRetentionWriter } from "./retention-barrier.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const TEAM_CAST_DIR = path.join(CXMSG_STATE_DIR, "team-casts");
export const TEAM_CAST_PLANS_DIR = path.join(TEAM_CAST_DIR, "plans");
export const TEAM_CAST_SELECTIONS_DIR = path.join(TEAM_CAST_DIR, "selections");
const TEAM_CAST_LOCK_PATH = path.join(TEAM_CAST_DIR, "plans.lock");
const TEAM_CAST_SELECTION_LOCK_PATH = path.join(TEAM_CAST_DIR, "selections.lock");
const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const NODE_KEY_PATTERN = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i;
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const PLAN_LIMIT = 2_048;
const RECIPIENT_LIMIT = 64;
const MENTION_LIMIT = 16;
const PLAN_TTL_MS = 15 * 60 * 1_000;
const PLAN_MAX_BYTES = 64 * 1024;
const PLAN_FIELDS = new Set([
  "version",
  "planId",
  "senderNodeKey",
  "projectId",
  "selector",
  "recipientNodeKeys",
  "recipientSetSha256",
  "estimatedWakeTurns",
  "createdAt",
  "expiresAt",
]);
const SELECTION_FIELDS = new Set([
  "version",
  "selectionId",
  "planId",
  "senderNodeKey",
  "projectId",
  "wakePolicy",
  "recipientNodeKeys",
  "recipientSetSha256",
  "estimatedWakeTurns",
  "createdAt",
  "expiresAt",
]);

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value.toLowerCase();
}

function normalizeNodeKey(value) {
  const match = NODE_KEY_PATTERN.exec(value || "");
  if (!match) throw new Error("Team Cast identity must be a stable Node key");
  return `${match[1].toLowerCase()}:${match[2].toLowerCase()}`;
}

function parseNodeKey(value) {
  const nodeKey = normalizeNodeKey(value);
  const separator = nodeKey.indexOf(":");
  return {
    nodeKey,
    runtimeKind: nodeKey.slice(0, separator),
    nativeId: nodeKey.slice(separator + 1),
  };
}

function ensureDirectory(directory) {
  if (!existsSync(directory)) mkdirSync(directory, { mode: 0o700 });
  assertPrivateDirectory(directory);
  chmodSync(directory, 0o700);
}

function assertPrivateDirectory(directory) {
  const metadata = lstatSync(directory);
  if (
    !metadata.isDirectory() ||
    metadata.isSymbolicLink() ||
    metadata.uid !== process.getuid()
  ) {
    throw new Error("Team Cast storage must be owner-controlled");
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error("Team Cast storage failed privacy validation");
  }
}

function ensureStorage() {
  ensureDirectory(TEAM_CAST_DIR);
  ensureDirectory(TEAM_CAST_PLANS_DIR);
  ensureDirectory(TEAM_CAST_SELECTIONS_DIR);
}

function planPath(planId) {
  return path.join(
    TEAM_CAST_PLANS_DIR,
    `${validateUuid("Team Cast plan id", planId)}.json`,
  );
}

function validSelector(selector) {
  if (!selector || typeof selector !== "object" || Array.isArray(selector)) {
    return false;
  }
  if (selector.kind === "conversation") {
    return Boolean(
      UUID_PATTERN.test(selector.conversationId || "") &&
        ["direct", "group"].includes(selector.conversationKind) &&
        Number.isSafeInteger(selector.membershipVersion) &&
        selector.membershipVersion >= 0 &&
        Object.keys(selector).every((field) =>
          [
            "kind",
            "conversationId",
            "conversationKind",
            "membershipVersion",
          ].includes(field),
        ),
    );
  }
  if (selector.kind === "cluster") {
    return Boolean(
      UUID_PATTERN.test(selector.clusterId || "") &&
        Number.isSafeInteger(selector.membershipVersion) &&
        selector.membershipVersion >= 1 &&
        Object.keys(selector).every((field) =>
          ["kind", "clusterId", "membershipVersion"].includes(field),
        ),
    );
  }
  if (selector.kind === "project-role") {
    return Boolean(
      UUID_PATTERN.test(selector.projectId || "") &&
        IDENTIFIER_PATTERN.test(selector.role || "") &&
        /^[0-9a-f]{64}$/.test(selector.bindingSetSha256 || "") &&
        Object.keys(selector).every((field) =>
          ["kind", "projectId", "role", "bindingSetSha256"].includes(field),
        ),
    );
  }
  return false;
}

function validPlan(plan) {
  return Boolean(
    plan?.version === 1 &&
      UUID_PATTERN.test(plan.planId || "") &&
      NODE_KEY_PATTERN.test(plan.senderNodeKey || "") &&
      UUID_PATTERN.test(plan.projectId || "") &&
      validSelector(plan.selector) &&
      Array.isArray(plan.recipientNodeKeys) &&
      plan.recipientNodeKeys.length >= 1 &&
      plan.recipientNodeKeys.length <= RECIPIENT_LIMIT &&
      plan.recipientNodeKeys.every((nodeKey) => NODE_KEY_PATTERN.test(nodeKey)) &&
      JSON.stringify([...plan.recipientNodeKeys].sort()) ===
        JSON.stringify(plan.recipientNodeKeys) &&
      new Set(plan.recipientNodeKeys).size === plan.recipientNodeKeys.length &&
      !plan.recipientNodeKeys.includes(plan.senderNodeKey) &&
      /^[0-9a-f]{64}$/.test(plan.recipientSetSha256 || "") &&
      plan.recipientSetSha256 ===
        createHash("sha256")
          .update(JSON.stringify(plan.recipientNodeKeys))
          .digest("hex") &&
      plan.estimatedWakeTurns === plan.recipientNodeKeys.length &&
      Number.isFinite(Date.parse(plan.createdAt || "")) &&
      Number.isFinite(Date.parse(plan.expiresAt || "")) &&
      Date.parse(plan.expiresAt) - Date.parse(plan.createdAt) === PLAN_TTL_MS &&
      Object.keys(plan).every((field) => PLAN_FIELDS.has(field)),
  );
}

export function validTeamCastPlanRecord(record) {
  return validPlan(record);
}

function validTeamCastSelection(selection) {
  const recipientLimit =
    selection?.wakePolicy === "mention-wake"
      ? MENTION_LIMIT
      : selection?.wakePolicy === "wake-all"
        ? RECIPIENT_LIMIT
        : 0;
  return Boolean(
    selection?.version === 1 &&
      UUID_PATTERN.test(selection.selectionId || "") &&
      UUID_PATTERN.test(selection.planId || "") &&
      NODE_KEY_PATTERN.test(selection.senderNodeKey || "") &&
      UUID_PATTERN.test(selection.projectId || "") &&
      ["mention-wake", "wake-all"].includes(selection.wakePolicy) &&
      Array.isArray(selection.recipientNodeKeys) &&
      selection.recipientNodeKeys.length >= 1 &&
      selection.recipientNodeKeys.length <= recipientLimit &&
      selection.recipientNodeKeys.every((nodeKey) =>
        NODE_KEY_PATTERN.test(nodeKey),
      ) &&
      JSON.stringify([...selection.recipientNodeKeys].sort()) ===
        JSON.stringify(selection.recipientNodeKeys) &&
      new Set(selection.recipientNodeKeys).size ===
        selection.recipientNodeKeys.length &&
      !selection.recipientNodeKeys.includes(selection.senderNodeKey) &&
      /^[0-9a-f]{64}$/.test(selection.recipientSetSha256 || "") &&
      selection.recipientSetSha256 ===
        createHash("sha256")
          .update(JSON.stringify(selection.recipientNodeKeys))
          .digest("hex") &&
      selection.estimatedWakeTurns === selection.recipientNodeKeys.length &&
      Number.isFinite(Date.parse(selection.createdAt || "")) &&
      Number.isFinite(Date.parse(selection.expiresAt || "")) &&
      Date.parse(selection.expiresAt) > Date.parse(selection.createdAt) &&
      Object.keys(selection).every((field) => SELECTION_FIELDS.has(field)),
  );
}

export function validTeamCastSelectionRecord(record) {
  return validTeamCastSelection(record);
}

function secureRead(filename) {
  try {
    const metadata = lstatSync(filename);
    if (
      !metadata.isFile() ||
      metadata.isSymbolicLink() ||
      metadata.nlink !== 1 ||
      metadata.uid !== process.getuid() ||
      (metadata.mode & 0o077) !== 0 ||
      metadata.size > PLAN_MAX_BYTES
    ) {
      return null;
    }
    const plan = JSON.parse(readFileSync(filename, "utf8"));
    return validPlan(plan) ? plan : null;
  } catch {
    return null;
  }
}

function writePlan(plan) {
  if (!validPlan(plan)) throw new Error("invalid Team Cast plan");
  const serialized = `${JSON.stringify(plan, null, 2)}\n`;
  if (Buffer.byteLength(serialized, "utf8") > PLAN_MAX_BYTES) {
    throw new Error("Team Cast plan exceeds its bounded size");
  }
  const destination = planPath(plan.planId);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  const descriptor = openSync(
    temporary,
    constants.O_CREAT |
      constants.O_EXCL |
      constants.O_WRONLY |
      constants.O_NOFOLLOW,
    0o600,
  );
  try {
    writeFileSync(descriptor, serialized);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  renameSync(temporary, destination);
  const directoryDescriptor = openSync(TEAM_CAST_PLANS_DIR, constants.O_RDONLY);
  try {
    fsyncSync(directoryDescriptor);
  } finally {
    closeSync(directoryDescriptor);
  }
  return plan;
}

function selectionPath(selectionId) {
  return path.join(
    TEAM_CAST_SELECTIONS_DIR,
    `${validateUuid("Team Cast selection id", selectionId)}.json`,
  );
}

function secureReadSelection(filename) {
  try {
    const metadata = lstatSync(filename);
    if (
      !metadata.isFile() ||
      metadata.isSymbolicLink() ||
      metadata.nlink !== 1 ||
      metadata.uid !== process.getuid() ||
      (metadata.mode & 0o077) !== 0 ||
      metadata.size > PLAN_MAX_BYTES
    ) {
      return null;
    }
    const selection = JSON.parse(readFileSync(filename, "utf8"));
    return validTeamCastSelection(selection) ? selection : null;
  } catch {
    return null;
  }
}

function writeSelection(selection) {
  if (!validTeamCastSelection(selection)) {
    throw new Error("invalid Team Cast selection");
  }
  const serialized = `${JSON.stringify(selection, null, 2)}\n`;
  if (Buffer.byteLength(serialized, "utf8") > PLAN_MAX_BYTES) {
    throw new Error("Team Cast selection exceeds its bounded size");
  }
  const destination = selectionPath(selection.selectionId);
  const temporary = `${destination}.${randomUUID()}.tmp`;
  const descriptor = openSync(
    temporary,
    constants.O_CREAT |
      constants.O_EXCL |
      constants.O_WRONLY |
      constants.O_NOFOLLOW,
    0o600,
  );
  try {
    writeFileSync(descriptor, serialized);
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
  renameSync(temporary, destination);
  const directoryDescriptor = openSync(
    TEAM_CAST_SELECTIONS_DIR,
    constants.O_RDONLY,
  );
  try {
    fsyncSync(directoryDescriptor);
  } finally {
    closeSync(directoryDescriptor);
  }
  return selection;
}

async function persistSelection(selection) {
  ensureStorage();
  return withFileLock(TEAM_CAST_SELECTION_LOCK_PATH, async () => {
    const names = readdirSync(TEAM_CAST_SELECTIONS_DIR).filter((name) =>
      name.endsWith(".json"),
    );
    if (names.length > PLAN_LIMIT) {
      throw new Error("Team Cast selection limit exceeded");
    }
    const existing = secureReadSelection(selectionPath(selection.selectionId));
    if (existing) {
      if (
        existing.planId !== selection.planId ||
        existing.senderNodeKey !== selection.senderNodeKey ||
        existing.wakePolicy !== selection.wakePolicy ||
        existing.recipientSetSha256 !== selection.recipientSetSha256
      ) {
        throw new Error(
          `Team Cast selection idempotency conflict: ${selection.selectionId}`,
        );
      }
      return { selection: existing, created: false };
    }
    if (existsSync(selectionPath(selection.selectionId))) {
      throw new Error(
        `Team Cast selection failed validation: ${selection.selectionId}`,
      );
    }
    if (names.length >= PLAN_LIMIT) {
      throw new Error("Team Cast selection limit reached");
    }
    return { selection: writeSelection(selection), created: true };
  });
}

function exactLiveNode(nodeKey, projectId) {
  const identity = parseNodeKey(nodeKey);
  if (
    identity.runtimeKind === "codex" &&
    readExecutionThread(identity.nativeId)
  ) {
    throw new Error(`Team Cast recipient is an Execution Thread: ${nodeKey}`);
  }
  if (readNodeTombstone(identity.runtimeKind, identity.nativeId)) {
    throw new Error(`Team Cast recipient is Tombstoned: ${nodeKey}`);
  }
  const node = readNode(identity.runtimeKind, identity.nativeId);
  if (!node || node.nodeKey !== nodeKey) {
    throw new Error(`Team Cast recipient is not a live Node: ${nodeKey}`);
  }
  if (node.projectId !== projectId) {
    throw new Error(`Team Cast recipient crosses the sender Project: ${nodeKey}`);
  }
  return node;
}

function resolveCluster(identity) {
  const byId = UUID_PATTERN.test(identity || "") ? readCluster(identity) : null;
  const byRouting = findClusterByRoutingId(identity);
  if (byId && byRouting && byId.clusterId !== byRouting.clusterId) {
    throw new Error(`ambiguous Cluster identity: ${identity}`);
  }
  const cluster = byId || byRouting;
  if (!cluster) throw new Error(`unknown Cluster: ${identity}`);
  return cluster;
}

function conversationSelection(conversationId, senderNodeKey) {
  const direct = readDirectConversation(conversationId);
  const group = readGroupConversation(conversationId);
  if (direct && group) throw new Error("ambiguous Conversation identity");
  const conversation = group || direct;
  const members = group
    ? conversation.membershipSnapshots.at(-1).members
    : conversation.currentMembers;
  if (!members.includes(senderNodeKey)) {
    throw new Error("Team Cast sender is not a current Conversation member");
  }
  return {
    members,
    selector: {
      kind: "conversation",
      conversationId: conversation.conversationId,
      conversationKind: group ? "group" : "direct",
      membershipVersion: group ? conversation.membershipVersion : 0,
    },
  };
}

function clusterSelection(clusterIdentity, senderNodeKey) {
  const cluster = resolveCluster(clusterIdentity);
  const snapshot = readClusterMembership(
    cluster.clusterId,
    cluster.membershipVersion,
  );
  if (
    !snapshot ||
    JSON.stringify(snapshot.members) !== JSON.stringify(cluster.members)
  ) {
    throw new Error(
      "Cluster current membership conflicts with its immutable snapshot",
    );
  }
  if (!cluster.members.includes(senderNodeKey)) {
    throw new Error("Team Cast sender is not a current Cluster member");
  }
  return {
    members: cluster.members,
    selector: {
      kind: "cluster",
      clusterId: cluster.clusterId,
      membershipVersion: cluster.membershipVersion,
    },
  };
}

function projectRoleSelection({ projectId, role }) {
  projectId = validateUuid("Project id", projectId);
  if (!readProject(projectId)) throw new Error(`unknown Project: ${projectId}`);
  if (!IDENTIFIER_PATTERN.test(role || "")) throw new Error("Team Cast role is invalid");
  const bindings = listRouteBindingsStrict()
    .filter(
      (binding) => binding.projectKey === projectId && binding.role === role,
    )
    .sort((left, right) => left.sessionName.localeCompare(right.sessionName));
  const byNode = new Map();
  for (const binding of bindings) {
    if (!binding.nodeKey) {
      throw new Error("Project-role selector requires stable Node-bound routes");
    }
    const session = readSessionRecord(binding.sessionName);
    if (
      !session ||
      session.threadId !== binding.threadId ||
      binding.nodeKey !== `codex:${binding.threadId.toLowerCase()}`
    ) {
      throw new Error(`Project-role binding is stale: ${binding.sessionName}`);
    }
    if (byNode.has(binding.nodeKey)) {
      throw new Error(`Project-role selector is ambiguous for ${binding.nodeKey}`);
    }
    byNode.set(binding.nodeKey, binding);
  }
  if (byNode.size === 0) throw new Error("Project-role selector has no recipients");
  const bindingEvidence = [...byNode.values()].map((binding) => ({
    sessionName: binding.sessionName,
    threadId: binding.threadId,
    nodeKey: binding.nodeKey,
    updatedAt: binding.updatedAt,
  }));
  return {
    members: [...byNode.keys()].sort(),
    selector: {
      kind: "project-role",
      projectId,
      role,
      bindingSetSha256: createHash("sha256")
        .update(JSON.stringify(bindingEvidence))
        .digest("hex"),
    },
  };
}

export async function resolveTeamCastPlan({
  senderNodeKey,
  selector,
  planId = randomUUID(),
  now = new Date().toISOString(),
}) {
  senderNodeKey = normalizeNodeKey(senderNodeKey);
  planId = validateUuid("Team Cast plan id", planId);
  if (!Number.isFinite(Date.parse(now))) throw new Error("Team Cast timestamp is invalid");
  const senderIdentity = parseNodeKey(senderNodeKey);
  const sender = readNode(senderIdentity.runtimeKind, senderIdentity.nativeId);
  if (!sender || sender.nodeKey !== senderNodeKey) {
    throw new Error("Team Cast sender must be a live Node");
  }
  if (!readProject(sender.projectId)) {
    throw new Error("Team Cast sender Project is unavailable");
  }
  let selected;
  if (selector?.kind === "conversation") {
    selected = conversationSelection(selector.id, senderNodeKey);
  } else if (selector?.kind === "cluster") {
    selected = clusterSelection(selector.id, senderNodeKey);
  } else if (selector?.kind === "project-role") {
    if (selector.projectId !== sender.projectId) {
      throw new Error("Project-role selector crosses the sender Project");
    }
    selected = projectRoleSelection(selector);
  } else {
    throw new Error("Team Cast selector is invalid");
  }
  const recipientNodeKeys = [...new Set(selected.members)]
    .filter((nodeKey) => nodeKey !== senderNodeKey)
    .map(normalizeNodeKey)
    .sort();
  if (
    recipientNodeKeys.length < 1 ||
    recipientNodeKeys.length > RECIPIENT_LIMIT
  ) {
    throw new Error(`Team Cast requires 1-${RECIPIENT_LIMIT} recipients`);
  }
  for (const nodeKey of recipientNodeKeys) exactLiveNode(nodeKey, sender.projectId);
  const plan = {
    version: 1,
    planId,
    senderNodeKey,
    projectId: sender.projectId,
    selector: selected.selector,
    recipientNodeKeys,
    recipientSetSha256: createHash("sha256")
      .update(JSON.stringify(recipientNodeKeys))
      .digest("hex"),
    estimatedWakeTurns: recipientNodeKeys.length,
    createdAt: now,
    expiresAt: new Date(Date.parse(now) + PLAN_TTL_MS).toISOString(),
  };
  ensureStorage();
  return withFileLock(TEAM_CAST_LOCK_PATH, async () => {
    const names = readdirSync(TEAM_CAST_PLANS_DIR)
      .filter((name) => name.endsWith(".json"));
    if (names.length > PLAN_LIMIT) throw new Error("Team Cast plan limit exceeded");
    const existing = secureRead(planPath(planId));
    if (existing) {
      if (
        existing.senderNodeKey !== plan.senderNodeKey ||
        JSON.stringify(existing.selector) !== JSON.stringify(plan.selector) ||
        existing.recipientSetSha256 !== plan.recipientSetSha256
      ) {
        throw new Error(`Team Cast plan idempotency conflict: ${planId}`);
      }
      return { plan: existing, created: false };
    }
    if (existsSync(planPath(planId))) {
      throw new Error(`Team Cast plan failed validation: ${planId}`);
    }
    if (names.length >= PLAN_LIMIT) throw new Error("Team Cast plan limit reached");
    return { plan: writePlan(plan), created: true };
  });
}

export function readTeamCastPlan(planId) {
  if (!existsSync(TEAM_CAST_PLANS_DIR)) return null;
  for (const directory of [TEAM_CAST_DIR, TEAM_CAST_PLANS_DIR]) {
    assertPrivateDirectory(directory);
  }
  const plan = secureRead(planPath(planId));
  if (!plan && existsSync(planPath(planId))) {
    throw new Error(`Team Cast plan failed validation: ${planId}`);
  }
  return plan;
}

export function publicTeamCastPlan(plan, { includeRecipients = false } = {}) {
  if (!validPlan(plan)) throw new Error("invalid Team Cast plan");
  return {
    version: plan.version,
    planId: plan.planId,
    senderNodeKey: plan.senderNodeKey,
    projectId: plan.projectId,
    selector: structuredClone(plan.selector),
    recipientCount: plan.recipientNodeKeys.length,
    recipientSetSha256: plan.recipientSetSha256,
    estimatedWakeTurns: plan.estimatedWakeTurns,
    createdAt: plan.createdAt,
    expiresAt: plan.expiresAt,
    expired: Date.parse(plan.expiresAt) <= Date.now(),
    ...(includeRecipients
      ? { recipientNodeKeys: [...plan.recipientNodeKeys] }
      : {}),
  };
}

export async function resolveTeamCastMentionSelection({
  planId,
  senderNodeKey,
  mentionedNodeKeys,
  selectionId = randomUUID(),
  now = new Date().toISOString(),
}) {
  planId = validateUuid("Team Cast plan id", planId);
  selectionId = validateUuid("Team Cast selection id", selectionId);
  senderNodeKey = normalizeNodeKey(senderNodeKey);
  if (!Number.isFinite(Date.parse(now))) {
    throw new Error("Team Cast selection timestamp is invalid");
  }
  const plan = readTeamCastPlan(planId);
  if (!plan) throw new Error(`unknown Team Cast plan: ${planId}`);
  if (plan.senderNodeKey !== senderNodeKey) {
    throw new Error("Team Cast selection sender does not own the plan");
  }
  if (Date.parse(plan.expiresAt) <= Date.parse(now)) {
    throw new Error("Team Cast plan expired before mention selection");
  }
  if (
    !Array.isArray(mentionedNodeKeys) ||
    mentionedNodeKeys.length < 1 ||
    mentionedNodeKeys.length > MENTION_LIMIT
  ) {
    throw new Error(`mention-wake requires 1-${MENTION_LIMIT} explicit mentions`);
  }
  const normalized = mentionedNodeKeys.map(normalizeNodeKey);
  if (new Set(normalized).size !== normalized.length) {
    throw new Error("mention-wake contains duplicate recipients");
  }
  const recipients = normalized.sort();
  for (const nodeKey of recipients) {
    if (!plan.recipientNodeKeys.includes(nodeKey)) {
      throw new Error(`mention-wake recipient is outside the fixed plan: ${nodeKey}`);
    }
    exactLiveNode(nodeKey, plan.projectId);
  }
  const selection = {
    version: 1,
    selectionId,
    planId,
    senderNodeKey,
    projectId: plan.projectId,
    wakePolicy: "mention-wake",
    recipientNodeKeys: recipients,
    recipientSetSha256: createHash("sha256")
      .update(JSON.stringify(recipients))
      .digest("hex"),
    estimatedWakeTurns: recipients.length,
    createdAt: now,
    expiresAt: plan.expiresAt,
  };
  return persistSelection(selection);
}

export async function resolveTeamCastWakeAllSelection({
  planId,
  senderNodeKey,
  selectionId = randomUUID(),
  now = new Date().toISOString(),
}) {
  planId = validateUuid("Team Cast plan id", planId);
  selectionId = validateUuid("Team Cast selection id", selectionId);
  senderNodeKey = normalizeNodeKey(senderNodeKey);
  if (!Number.isFinite(Date.parse(now))) {
    throw new Error("Team Cast selection timestamp is invalid");
  }
  const plan = readTeamCastPlan(planId);
  if (!plan) throw new Error(`unknown Team Cast plan: ${planId}`);
  if (plan.senderNodeKey !== senderNodeKey) {
    throw new Error("Team Cast selection sender does not own the plan");
  }
  if (Date.parse(plan.expiresAt) <= Date.parse(now)) {
    throw new Error("Team Cast plan expired before wake-all selection");
  }
  for (const nodeKey of plan.recipientNodeKeys) {
    exactLiveNode(nodeKey, plan.projectId);
  }
  const selection = {
    version: 1,
    selectionId,
    planId,
    senderNodeKey,
    projectId: plan.projectId,
    wakePolicy: "wake-all",
    recipientNodeKeys: [...plan.recipientNodeKeys],
    recipientSetSha256: plan.recipientSetSha256,
    estimatedWakeTurns: plan.estimatedWakeTurns,
    createdAt: now,
    expiresAt: plan.expiresAt,
  };
  return persistSelection(selection);
}

export function readTeamCastSelection(selectionId) {
  if (!existsSync(TEAM_CAST_SELECTIONS_DIR)) return null;
  for (const directory of [TEAM_CAST_DIR, TEAM_CAST_SELECTIONS_DIR]) {
    assertPrivateDirectory(directory);
  }
  const selection = secureReadSelection(selectionPath(selectionId));
  if (!selection && existsSync(selectionPath(selectionId))) {
    throw new Error(
      `Team Cast selection failed validation: ${selectionId}`,
    );
  }
  return selection;
}

export function readTeamCastMentionSelection(selectionId) {
  return readTeamCastSelection(selectionId);
}

export function publicTeamCastSelection(
  selection,
  { includeRecipients = false } = {},
) {
  if (!validTeamCastSelection(selection)) {
    throw new Error("invalid Team Cast selection");
  }
  return {
    version: selection.version,
    selectionId: selection.selectionId,
    planId: selection.planId,
    senderNodeKey: selection.senderNodeKey,
    projectId: selection.projectId,
    wakePolicy: selection.wakePolicy,
    recipientCount: selection.recipientNodeKeys.length,
    recipientSetSha256: selection.recipientSetSha256,
    estimatedWakeTurns: selection.estimatedWakeTurns,
    createdAt: selection.createdAt,
    expiresAt: selection.expiresAt,
    expired: Date.parse(selection.expiresAt) <= Date.now(),
    ...(includeRecipients
      ? { recipientNodeKeys: [...selection.recipientNodeKeys] }
      : {}),
  };
}

export function publicTeamCastMentionSelection(selection, options = {}) {
  return publicTeamCastSelection(selection, options);
}

export async function prepareTeamCastMentionMessage(
  {
    selectionId,
    senderNodeKey,
    logicalMessageId = randomUUID(),
    message,
    now = new Date().toISOString(),
  },
  {
    bodyStore = storeMessageBody,
    ledgerCommit = commitPreparedTeamCastDelivery,
  } = {},
) {
  selectionId = validateUuid("Team Cast selection id", selectionId);
  logicalMessageId = validateUuid("logical message id", logicalMessageId);
  senderNodeKey = normalizeNodeKey(senderNodeKey);
  if (!Number.isFinite(Date.parse(now))) {
    throw new Error("Team Cast preparation timestamp is invalid");
  }
  if (typeof message !== "string" || !message.trim()) {
    throw new Error("Team Cast message must not be empty");
  }
  const messageBytes = Buffer.byteLength(message, "utf8");
  if (messageBytes > MAX_STORED_MESSAGE_BYTES) {
    throw new Error(
      `Team Cast message exceeds ${MAX_STORED_MESSAGE_BYTES} bytes`,
    );
  }
  return withRetentionWriter(async () => {
    const selection = readTeamCastSelection(selectionId);
    if (!selection) {
      throw new Error(`unknown Team Cast selection: ${selectionId}`);
    }
    if (selection.senderNodeKey !== senderNodeKey) {
      throw new Error("Team Cast preparation sender does not own the selection");
    }
    if (Date.parse(selection.expiresAt) <= Date.parse(now)) {
      throw new Error("Team Cast selection expired before preparation");
    }
    const plan = readTeamCastPlan(selection.planId);
    if (
      !plan ||
      plan.senderNodeKey !== selection.senderNodeKey ||
      plan.projectId !== selection.projectId ||
      (selection.wakePolicy === "wake-all" &&
        (selection.recipientSetSha256 !== plan.recipientSetSha256 ||
          JSON.stringify(selection.recipientNodeKeys) !==
            JSON.stringify(plan.recipientNodeKeys))) ||
      (selection.wakePolicy === "mention-wake" &&
        selection.recipientNodeKeys.some(
          (nodeKey) => !plan.recipientNodeKeys.includes(nodeKey),
        ))
    ) {
      throw new Error("Team Cast selection no longer matches its fixed plan");
    }
    const senderIdentity = parseNodeKey(senderNodeKey);
    const sender = readNode(senderIdentity.runtimeKind, senderIdentity.nativeId);
    if (!sender || sender.projectId !== selection.projectId) {
      throw new Error("Team Cast preparation sender is not a live Project Node");
    }
    for (const nodeKey of selection.recipientNodeKeys) {
      exactLiveNode(nodeKey, selection.projectId);
    }
    const body = await bodyStore({
      messageId: logicalMessageId,
      body: message,
    });
    if (Date.parse(selection.expiresAt) <= Date.parse(body.createdAt)) {
      throw new Error("Team Cast selection expired before body persistence");
    }
    const route = {
      schema_version: 1,
      kind: "team-cast",
      plan_id: selection.planId,
      selection_id: selection.selectionId,
      project_id: selection.projectId,
      wake_policy: selection.wakePolicy,
      expiry: selection.expiresAt,
    };
    const logicalMessage = {
      messageId: logicalMessageId,
      from: senderNodeKey,
      senderThreadId: senderNodeKey.startsWith("codex:")
        ? senderIdentity.nativeId
        : null,
      senderNodeKey,
      body: {
        messageId: logicalMessageId,
        bytes: body.bodyBytes,
        sha256: body.bodySha256,
        contentRef: body.contentRef,
      },
      route,
      routeFingerprint: createHash("sha256")
        .update(JSON.stringify(route))
        .digest("hex"),
      createdAt: body.createdAt,
      teamCast: {
        version: 1,
        planId: selection.planId,
        selectionId: selection.selectionId,
        projectId: selection.projectId,
        wakePolicy: selection.wakePolicy,
        recipientNodeKeys: selection.recipientNodeKeys,
        recipientSetSha256: selection.recipientSetSha256,
        expiresAt: selection.expiresAt,
      },
    };
    const ledger = await ledgerCommit({
      logicalMessage,
      recipients: selection.recipientNodeKeys.map((nodeKey) => ({
        nodeKey,
        targetThreadId: nodeKey.startsWith("codex:")
          ? nodeKey.slice("codex:".length)
          : null,
      })),
      now: body.createdAt,
    });
    return {
      selection: publicTeamCastSelection(selection),
      logicalMessageId,
      body: {
        bytes: body.bodyBytes,
        sha256: body.bodySha256,
        contentRef: body.contentRef,
      },
      ledger: ledger.record,
      created: ledger.created,
      deliveryStarted: false,
    };
  });
}

export async function dispatchPreparedTeamCastMessage(
  { logicalMessageId, now = new Date().toISOString() },
  {
    preflightRecipient,
    dispatchRecipient,
    bodyRead = readWholeMessageBody,
    scheduleRecipient = scheduleTeamCastRecipientDelivery,
  },
) {
  logicalMessageId = validateUuid("logical message id", logicalMessageId);
  if (!Number.isFinite(Date.parse(now))) {
    throw new Error("Team Cast dispatch timestamp is invalid");
  }
  if (
    typeof preflightRecipient !== "function" ||
    typeof dispatchRecipient !== "function"
  ) {
    throw new Error("Team Cast dispatch requires explicit transport adapters");
  }
  let record = await readDeliveryLedgerIndexed(logicalMessageId);
  if (!record?.logicalMessage?.teamCast || !Array.isArray(record.teamDeliveries)) {
    throw new Error(`unknown prepared Team Cast message: ${logicalMessageId}`);
  }
  if (Date.parse(record.logicalMessage.teamCast.expiresAt) <= Date.parse(now)) {
    throw new Error("Team Cast message expired before dispatch");
  }
  const pending = record.teamDeliveries.filter(
    (delivery) => delivery.state === "prepared" && delivery.attempts.length === 0,
  );
  const contexts = new Map();
  for (const delivery of pending) {
    exactLiveNode(delivery.targetNodeKey, record.logicalMessage.teamCast.projectId);
    contexts.set(
      delivery.targetNodeKey,
      await preflightRecipient({
        targetNodeKey: delivery.targetNodeKey,
        targetThreadId: delivery.targetThreadId,
        projectId: record.logicalMessage.teamCast.projectId,
      }),
    );
  }
  const message = bodyRead(record.logicalMessage.body.contentRef);
  if (
    Buffer.byteLength(message, "utf8") !== record.logicalMessage.body.bytes ||
    createHash("sha256").update(message).digest("hex") !==
      record.logicalMessage.body.sha256
  ) {
    throw new Error("Team Cast Message Body conflicts with Ledger evidence");
  }

  const outcomes = [];
  for (const delivery of record.teamDeliveries) {
    if (delivery.state !== "prepared") {
      outcomes.push({
        targetNodeKey: delivery.targetNodeKey,
        status: delivery.state,
        attempted: false,
      });
      continue;
    }
    if (delivery.attempts.length > 0) {
      outcomes.push({
        targetNodeKey: delivery.targetNodeKey,
        status: "unresolved_attempt",
        attempted: false,
      });
      continue;
    }
    const context = contexts.get(delivery.targetNodeKey);
    if (
      ["when-idle", "after-turn", "after-job"].includes(
        context?.scheduleWakePolicy,
      )
    ) {
      try {
        const scheduled = await scheduleRecipient(
          logicalMessageId,
          delivery.targetNodeKey,
          {
            now,
            wakePolicy: context.scheduleWakePolicy,
            triggerTurnId: context.triggerTurnId ?? null,
            triggerJobId: context.triggerJobId ?? null,
          },
        );
        outcomes.push({
          targetNodeKey: delivery.targetNodeKey,
          status: "scheduled",
          attempted: false,
          scheduled: scheduled.scheduled,
        });
      } catch {
        outcomes.push({
          targetNodeKey: delivery.targetNodeKey,
          status: "schedule_failed",
          attempted: false,
          scheduled: false,
          errorCode: "ESCHEDULEFAILED",
        });
      }
      continue;
    }
    const begun = await beginTeamCastRecipientDelivery(
      logicalMessageId,
      delivery.targetNodeKey,
      {
        now,
        transport:
          context?.transport ||
          "codex-app-server",
      },
    );
    if (!begun.created) {
      outcomes.push({
        targetNodeKey: delivery.targetNodeKey,
        status: "unresolved_attempt",
        attempted: false,
      });
      continue;
    }
    let evidence;
    try {
      evidence = await dispatchRecipient({
        context,
        attemptId: begun.attempt.attemptId,
        targetNodeKey: delivery.targetNodeKey,
        targetThreadId: delivery.targetThreadId,
        logicalMessageId,
        senderNodeKey: record.logicalMessage.senderNodeKey,
        message,
        route: structuredClone(record.logicalMessage.route),
      });
    } catch {
      evidence = {
        state: "unknown",
        turnId: null,
        transportResult: "dispatch-threw-before-evidence",
        errorCode: "EDISPATCHUNKNOWN",
      };
    }
    const stored = await appendTeamCastRecipientEvidence(
      logicalMessageId,
      delivery.targetNodeKey,
      {
        attemptId: begun.attempt.attemptId,
        state: evidence?.state,
        turnId: evidence?.turnId ?? null,
        transportResult: evidence?.transportResult ?? null,
        errorCode: evidence?.errorCode ?? null,
      },
    );
    const updated = stored.record.teamDeliveries.find(
      (candidate) => candidate.targetNodeKey === delivery.targetNodeKey,
    );
    outcomes.push({
      targetNodeKey: delivery.targetNodeKey,
      status: updated.state,
      attempted: true,
      turnId: updated.turnId || null,
      errorCode: updated.errorCode || null,
    });
  }
  record = await readDeliveryLedgerIndexed(logicalMessageId);
  return { logicalMessageId, outcomes, record };
}
