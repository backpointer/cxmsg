import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import { readJob } from "./jobs.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const NODE_DIRECTORY_DIR = path.join(CXMSG_STATE_DIR, "directory");
export const NODES_DIR = path.join(NODE_DIRECTORY_DIR, "nodes");
export const PROJECTS_DIR = path.join(NODE_DIRECTORY_DIR, "projects");
export const NODE_TOMBSTONES_DIR = path.join(
  NODE_DIRECTORY_DIR,
  "tombstones",
  "nodes",
);
export const SUCCESSORS_DIR = path.join(NODE_DIRECTORY_DIR, "successors");
export const EXECUTION_THREADS_DIR = path.join(
  NODE_DIRECTORY_DIR,
  "execution-threads",
);
export const CLUSTERS_DIR = path.join(NODE_DIRECTORY_DIR, "clusters");
export const CLUSTER_MEMBERSHIPS_DIR = path.join(
  NODE_DIRECTORY_DIR,
  "cluster-memberships",
);
export const CLUSTER_TOMBSTONES_DIR = path.join(
  NODE_DIRECTORY_DIR,
  "tombstones",
  "clusters",
);
export const ENDPOINT_HISTORY_LIMIT = 64;
export const ENDPOINT_TRANSPORT_LIMIT = 16;
export const CLUSTER_MEMBER_LIMIT = 256;

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const RUNTIME_KINDS = new Set(["codex", "claude"]);
const ENDPOINT_STATUSES = new Set([
  "reachable",
  "external-writer",
  "unreachable",
  "stale",
  "unknown",
  "mismatched",
]);
const ENDPOINT_DECISIONS = new Set([
  "baseline-imported",
  "selected",
  "replaced",
  "refreshed",
  "older-rejected",
  "conflict-rejected",
]);
const SUCCESSFUL_ENDPOINT_DECISIONS = new Set([
  "baseline-imported",
  "selected",
  "replaced",
  "refreshed",
]);
const ENDPOINT_OBSERVATION_FIELDS = new Set([
  "transport",
  "endpointId",
  "nodeKey",
  "generation",
  "status",
  "decision",
  "address",
  "sessionName",
  "firstObservedAt",
  "lastObservedAt",
  "observationCount",
]);
const ENDPOINT_FIELDS = new Set([
  "transport",
  "endpointId",
  "nodeKey",
  "generation",
  "status",
  "address",
  "sessionName",
  "observedAt",
]);
const NODE_TOMBSTONE_FIELDS = new Set([
  "version",
  "nodeKey",
  "runtimeKind",
  "nativeId",
  "projectId",
  "lastSafeLabel",
  "removedAt",
  "reason",
]);
const SUCCESSOR_FIELDS = new Set([
  "version",
  "predecessorNodeKey",
  "successorNodeKey",
  "projectId",
  "linkedAt",
]);
const EXECUTION_THREAD_FIELDS = new Set([
  "version",
  "kind",
  "threadId",
  "jobId",
  "sourceThreadId",
  "sourceNodeKey",
  "projectId",
  "creationMode",
  "classifiedAt",
]);
const EXECUTION_CREATION_MODES = new Set([
  "fork",
  "start-fallback",
  "legacy-observed",
]);
const CLUSTER_FIELDS = new Set([
  "version",
  "clusterId",
  "routingId",
  "membershipVersion",
  "members",
  "createdAt",
  "updatedAt",
]);
const CLUSTER_MEMBERSHIP_FIELDS = new Set([
  "version",
  "clusterId",
  "membershipVersion",
  "members",
  "changeKind",
  "changedNodeKey",
  "createdAt",
]);
const CLUSTER_TOMBSTONE_FIELDS = new Set([
  "version",
  "clusterId",
  "routingId",
  "lastMembershipVersion",
  "removedAt",
  "reason",
]);
const CLUSTER_CHANGE_KINDS = new Set([
  "created",
  "member-added",
  "member-removed",
]);

function validateUuid(label, value) {
  if (!UUID_PATTERN.test(value || "")) throw new Error(`${label} must be a UUID`);
  return value;
}

function validateIdentifier(label, value) {
  if (!IDENTIFIER_PATTERN.test(value || "")) {
    throw new Error(`${label} must be 1-128 safe identifier characters`);
  }
  return value;
}

function validateRuntimeKind(runtimeKind) {
  if (!RUNTIME_KINDS.has(runtimeKind)) {
    throw new Error("runtime kind must be codex or claude");
  }
  return runtimeKind;
}

function validateAlias(alias) {
  if (
    typeof alias !== "string" ||
    alias.length < 1 ||
    alias.length > 128 ||
    /[\u0000-\u001f\u007f]/.test(alias)
  ) {
    throw new Error("Node alias must be 1-128 printable characters");
  }
  return alias;
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

function canonicalPath(candidate) {
  const resolved = path.resolve(candidate || "");
  if (!path.isAbsolute(resolved)) throw new Error("Project root must be absolute");
  return realpathSync(resolved);
}

export function nodeKey(runtimeKind, nativeId) {
  return `${validateRuntimeKind(runtimeKind)}:${validateUuid("native id", nativeId).toLowerCase()}`;
}

function nodeFilename(runtimeKind, nativeId) {
  return `${nodeKey(runtimeKind, nativeId).replace(":", "--")}.json`;
}

function nodePath(runtimeKind, nativeId) {
  return path.join(NODES_DIR, nodeFilename(runtimeKind, nativeId));
}

function nodeTombstonePath(runtimeKind, nativeId) {
  return path.join(NODE_TOMBSTONES_DIR, nodeFilename(runtimeKind, nativeId));
}

function parseNodeKey(identity) {
  const match = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i.exec(
    identity || "",
  );
  if (!match) throw new Error("Node key must contain a runtime kind and UUID");
  return {
    runtimeKind: validateRuntimeKind(match[1].toLowerCase()),
    nativeId: validateUuid("native id", match[2]).toLowerCase(),
  };
}

function successorFilename(successorNodeKey) {
  const { runtimeKind, nativeId } = parseNodeKey(successorNodeKey);
  return `${runtimeKind}--${nativeId}.json`;
}

function successorPath(successorNodeKey) {
  return path.join(SUCCESSORS_DIR, successorFilename(successorNodeKey));
}

function executionThreadPath(threadId) {
  return path.join(
    EXECUTION_THREADS_DIR,
    `${validateUuid("execution thread id", threadId).toLowerCase()}.json`,
  );
}

function clusterPath(clusterId) {
  return path.join(
    CLUSTERS_DIR,
    `${validateUuid("cluster id", clusterId).toLowerCase()}.json`,
  );
}

function clusterTombstonePath(clusterId) {
  return path.join(
    CLUSTER_TOMBSTONES_DIR,
    `${validateUuid("cluster id", clusterId).toLowerCase()}.json`,
  );
}

function clusterMembershipFilename(clusterId, membershipVersion) {
  clusterId = validateUuid("cluster id", clusterId).toLowerCase();
  if (!Number.isSafeInteger(membershipVersion) || membershipVersion < 1) {
    throw new Error("Cluster membership version must be a positive integer");
  }
  return `${clusterId}--${String(membershipVersion).padStart(10, "0")}.json`;
}

function clusterMembershipPath(clusterId, membershipVersion) {
  return path.join(
    CLUSTER_MEMBERSHIPS_DIR,
    clusterMembershipFilename(clusterId, membershipVersion),
  );
}

function projectPath(projectId) {
  return path.join(PROJECTS_DIR, `${validateUuid("project id", projectId)}.json`);
}

export function discoverProjectRoot(root, { run = spawnSync } = {}) {
  const canonicalRoot = canonicalPath(root);
  const result = run(
    "git",
    ["-C", canonicalRoot, "rev-parse", "--path-format=absolute", "--git-common-dir"],
    { encoding: "utf8" },
  );
  if (result.status === 0 && result.stdout?.trim()) {
    const commonDir = canonicalPath(result.stdout.trim());
    return {
      kind: "git-common-dir",
      key: commonDir,
      root: canonicalRoot,
    };
  }
  return { kind: "canonical-root", key: canonicalRoot, root: canonicalRoot };
}

function validProject(record) {
  return Boolean(
    record?.version === 1 &&
      UUID_PATTERN.test(record.projectId || "") &&
      IDENTIFIER_PATTERN.test(record.routingId || "") &&
      ["git-common-dir", "canonical-root"].includes(record.discovery?.kind) &&
      typeof record.discovery?.key === "string" &&
      path.isAbsolute(record.discovery.key) &&
      Array.isArray(record.rootAliases) &&
      record.rootAliases.every(
        (alias) => typeof alias?.path === "string" && path.isAbsolute(alias.path),
      ),
  );
}

function validNode(record) {
  const endpoints = Object.entries(record?.selectedEndpoints || {});
  return Boolean(
    record?.version === 1 &&
      RUNTIME_KINDS.has(record.runtimeKind) &&
      UUID_PATTERN.test(record.nativeId || "") &&
      record.nodeKey === nodeKey(record.runtimeKind, record.nativeId) &&
      UUID_PATTERN.test(record.projectId || "") &&
      Array.isArray(record.aliases) &&
      record.aliases.every(
        (alias) =>
          typeof alias?.value === "string" &&
          alias.value.length > 0 &&
          alias.value.length <= 128,
      ) &&
      record.selectedEndpoints &&
      typeof record.selectedEndpoints === "object" &&
      endpoints.length <= ENDPOINT_TRANSPORT_LIMIT &&
      endpoints.every(
        ([transport, endpoint]) =>
          IDENTIFIER_PATTERN.test(transport) &&
          endpoint?.transport === transport &&
          endpoint.nodeKey === record.nodeKey &&
          IDENTIFIER_PATTERN.test(endpoint.endpointId || "") &&
          Number.isSafeInteger(endpoint.generation) &&
          endpoint.generation >= 0 &&
          ENDPOINT_STATUSES.has(endpoint.status) &&
          Number.isFinite(Date.parse(endpoint.observedAt || "")) &&
          (endpoint.address === undefined ||
            (typeof endpoint.address === "string" &&
              Buffer.byteLength(endpoint.address, "utf8") <= 1_024 &&
              !/[\u0000-\u001f\u007f]/.test(endpoint.address))) &&
          (endpoint.sessionName === undefined ||
            (typeof endpoint.sessionName === "string" &&
              endpoint.sessionName.length <= 128)) &&
          Object.keys(endpoint).every((field) => ENDPOINT_FIELDS.has(field)),
      ) &&
      (record.endpointHistory === undefined ||
        (Array.isArray(record.endpointHistory) &&
          record.endpointHistory.length <= ENDPOINT_HISTORY_LIMIT &&
          record.endpointHistory.every(
            (observation) =>
              observation?.nodeKey === record.nodeKey &&
              IDENTIFIER_PATTERN.test(observation.transport || "") &&
              IDENTIFIER_PATTERN.test(observation.endpointId || "") &&
              Number.isSafeInteger(observation.generation) &&
              observation.generation >= 0 &&
              ENDPOINT_STATUSES.has(observation.status) &&
              ENDPOINT_DECISIONS.has(observation.decision) &&
              Number.isFinite(Date.parse(observation.firstObservedAt || "")) &&
              Number.isFinite(Date.parse(observation.lastObservedAt || "")) &&
              Date.parse(observation.firstObservedAt) <=
                Date.parse(observation.lastObservedAt) &&
              Number.isSafeInteger(observation.observationCount) &&
              observation.observationCount >= 1 &&
              (observation.address === undefined ||
                (typeof observation.address === "string" &&
                  Buffer.byteLength(observation.address, "utf8") <= 1_024 &&
                  !/[\u0000-\u001f\u007f]/.test(observation.address))) &&
              (observation.sessionName === undefined ||
                (typeof observation.sessionName === "string" &&
                  observation.sessionName.length <= 128)) &&
              Object.keys(observation).every((field) =>
                ENDPOINT_OBSERVATION_FIELDS.has(field),
              ),
          ))),
  );
}

function validNodeTombstone(record) {
  return Boolean(
    record?.version === 1 &&
      RUNTIME_KINDS.has(record.runtimeKind) &&
      UUID_PATTERN.test(record.nativeId || "") &&
      record.nodeKey === nodeKey(record.runtimeKind, record.nativeId) &&
      UUID_PATTERN.test(record.projectId || "") &&
      IDENTIFIER_PATTERN.test(record.lastSafeLabel || "") &&
      IDENTIFIER_PATTERN.test(record.reason || "") &&
      Number.isFinite(Date.parse(record.removedAt || "")) &&
      Object.keys(record).every((field) => NODE_TOMBSTONE_FIELDS.has(field)),
  );
}

function validSuccessor(record) {
  if (
    record?.version !== 1 ||
    !UUID_PATTERN.test(record.projectId || "") ||
    !Number.isFinite(Date.parse(record.linkedAt || ""))
  ) {
    return false;
  }
  try {
    parseNodeKey(record.predecessorNodeKey);
    parseNodeKey(record.successorNodeKey);
    return (
      record.predecessorNodeKey !== record.successorNodeKey &&
      Object.keys(record).every((field) => SUCCESSOR_FIELDS.has(field))
    );
  } catch {
    return false;
  }
}

function validExecutionThread(record) {
  return Boolean(
    record?.version === 1 &&
      record.kind === "execution-thread" &&
      UUID_PATTERN.test(record.threadId || "") &&
      UUID_PATTERN.test(record.jobId || "") &&
      UUID_PATTERN.test(record.sourceThreadId || "") &&
      record.threadId !== record.sourceThreadId &&
      (record.sourceNodeKey === undefined ||
        record.sourceNodeKey === nodeKey("codex", record.sourceThreadId)) &&
      (record.projectId === undefined || UUID_PATTERN.test(record.projectId)) &&
      Boolean(record.sourceNodeKey) === Boolean(record.projectId) &&
      EXECUTION_CREATION_MODES.has(record.creationMode) &&
      Number.isFinite(Date.parse(record.classifiedAt || "")) &&
      Object.keys(record).every((field) => EXECUTION_THREAD_FIELDS.has(field)),
  );
}

function validClusterMembers(members) {
  if (!Array.isArray(members) || members.length > CLUSTER_MEMBER_LIMIT) {
    return false;
  }
  try {
    return (
      members.every((identity) => {
        parseNodeKey(identity);
        return true;
      }) &&
      members.every((identity, index) => index === 0 || members[index - 1] < identity)
    );
  } catch {
    return false;
  }
}

function validCluster(record) {
  return Boolean(
    record?.version === 1 &&
      UUID_PATTERN.test(record.clusterId || "") &&
      IDENTIFIER_PATTERN.test(record.routingId || "") &&
      Number.isSafeInteger(record.membershipVersion) &&
      record.membershipVersion >= 1 &&
      validClusterMembers(record.members) &&
      Number.isFinite(Date.parse(record.createdAt || "")) &&
      Number.isFinite(Date.parse(record.updatedAt || "")) &&
      Date.parse(record.createdAt) <= Date.parse(record.updatedAt) &&
      Object.keys(record).every((field) => CLUSTER_FIELDS.has(field)),
  );
}

function validClusterMembership(record) {
  if (
    record?.version !== 1 ||
    !UUID_PATTERN.test(record.clusterId || "") ||
    !Number.isSafeInteger(record.membershipVersion) ||
    record.membershipVersion < 1 ||
    !validClusterMembers(record.members) ||
    !CLUSTER_CHANGE_KINDS.has(record.changeKind) ||
    !Number.isFinite(Date.parse(record.createdAt || "")) ||
    !Object.keys(record).every((field) => CLUSTER_MEMBERSHIP_FIELDS.has(field))
  ) {
    return false;
  }
  if (record.changeKind === "created") {
    return record.membershipVersion === 1 && record.changedNodeKey === undefined;
  }
  try {
    parseNodeKey(record.changedNodeKey);
    return true;
  } catch {
    return false;
  }
}

function validClusterTombstone(record) {
  return Boolean(
    record?.version === 1 &&
      UUID_PATTERN.test(record.clusterId || "") &&
      IDENTIFIER_PATTERN.test(record.routingId || "") &&
      Number.isSafeInteger(record.lastMembershipVersion) &&
      record.lastMembershipVersion >= 1 &&
      Number.isFinite(Date.parse(record.removedAt || "")) &&
      IDENTIFIER_PATTERN.test(record.reason || "") &&
      Object.keys(record).every((field) => CLUSTER_TOMBSTONE_FIELDS.has(field)),
  );
}

export function readProject(projectId) {
  const record = readJson(projectPath(projectId));
  return validProject(record) ? record : null;
}

export function listProjects() {
  if (!existsSync(PROJECTS_DIR)) return [];
  return readdirSync(PROJECTS_DIR)
    .filter((name) => UUID_PATTERN.test(name.slice(0, -5)) && name.endsWith(".json"))
    .sort()
    .map((name) => readJson(path.join(PROJECTS_DIR, name)))
    .filter(validProject);
}

export function findProjectByRoutingId(routingId) {
  validateIdentifier("routing id", routingId);
  return listProjects().find((record) => record.routingId === routingId) || null;
}

export async function ensureProject({
  routingId,
  root,
  projectId = randomUUID(),
  discover = discoverProjectRoot,
}) {
  validateIdentifier("routing id", routingId);
  projectId = validateUuid("project id", projectId).toLowerCase();
  const discovery = discover(root);
  if (
    !["git-common-dir", "canonical-root"].includes(discovery?.kind) ||
    typeof discovery.key !== "string" ||
    !path.isAbsolute(discovery.key) ||
    typeof discovery.root !== "string" ||
    !path.isAbsolute(discovery.root)
  ) {
    throw new Error("Project discovery returned invalid canonical evidence");
  }
  ensureDirectory(NODE_DIRECTORY_DIR);
  ensureDirectory(PROJECTS_DIR);
  return withFileLock(path.join(NODE_DIRECTORY_DIR, "projects.lock"), async () => {
    const projects = listProjects();
    const byRouting = projects.find((record) => record.routingId === routingId);
    const byId = projects.find((record) => record.projectId === projectId);
    const byDiscovery = projects.find(
      (record) =>
        record.discovery.kind === discovery.kind &&
        record.discovery.key === discovery.key,
    );
    if (byRouting && byDiscovery && byRouting.projectId !== byDiscovery.projectId) {
      throw new Error("Project routing identity conflicts with discovery evidence");
    }
    if (byRouting && !byDiscovery) {
      throw new Error("Project routing identity already belongs to another root");
    }
    if (byDiscovery && byDiscovery.routingId !== routingId) {
      throw new Error("Project root already belongs to another routing identity");
    }
    const existing = byRouting || byDiscovery;
    if (byId && byId !== existing) {
      throw new Error("Project ID already belongs to another identity");
    }
    if (!existing && existsSync(projectPath(projectId))) {
      throw new Error("Project ID record exists but failed schema validation");
    }
    const now = new Date().toISOString();
    if (existing) {
      const aliases = [...existing.rootAliases];
      const alias = aliases.find((candidate) => candidate.path === discovery.root);
      if (alias) alias.lastSeenAt = now;
      else aliases.push({ path: discovery.root, firstSeenAt: now, lastSeenAt: now });
      return atomicWrite(PROJECTS_DIR, `${existing.projectId}.json`, {
        ...existing,
        rootAliases: aliases,
        updatedAt: now,
      });
    }
    return atomicWrite(PROJECTS_DIR, `${projectId}.json`, {
      version: 1,
      projectId: projectId.toLowerCase(),
      routingId,
      discovery: { kind: discovery.kind, key: discovery.key },
      rootAliases: [
        { path: discovery.root, firstSeenAt: now, lastSeenAt: now },
      ],
      createdAt: now,
      updatedAt: now,
    });
  });
}

export function projectContainsPath(project, cwd, { discover = discoverProjectRoot } = {}) {
  if (!validProject(project)) return false;
  let context;
  try {
    context = discover(cwd);
  } catch {
    return false;
  }
  if (project.discovery.kind === "git-common-dir") {
    return context.kind === "git-common-dir" && context.key === project.discovery.key;
  }
  return project.rootAliases.some((alias) => {
    const relative = path.relative(alias.path, context.root);
    return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
  });
}

export function readNode(runtimeKind, nativeId) {
  const record = readJson(nodePath(runtimeKind, nativeId));
  return validNode(record) ? record : null;
}

export function listNodes() {
  if (!existsSync(NODES_DIR)) return [];
  return readdirSync(NODES_DIR)
    .filter((name) => /^(?:codex|claude)--[0-9a-f-]{36}\.json$/i.test(name))
    .sort()
    .map((name) => readJson(path.join(NODES_DIR, name)))
    .filter(validNode);
}

export function readNodeTombstone(runtimeKind, nativeId) {
  const record = readJson(nodeTombstonePath(runtimeKind, nativeId));
  return validNodeTombstone(record) ? record : null;
}

export function listNodeTombstones() {
  if (!existsSync(NODE_TOMBSTONES_DIR)) return [];
  return readdirSync(NODE_TOMBSTONES_DIR)
    .filter((name) => /^(?:codex|claude)--[0-9a-f-]{36}\.json$/i.test(name))
    .sort()
    .map((name) => readJson(path.join(NODE_TOMBSTONES_DIR, name)))
    .filter(validNodeTombstone);
}

export function readSuccessor(successorNodeKey) {
  const identity = parseNodeKey(successorNodeKey);
  const normalized = nodeKey(identity.runtimeKind, identity.nativeId);
  const record = readJson(successorPath(normalized));
  return validSuccessor(record) && record.successorNodeKey === normalized
    ? record
    : null;
}

export function listSuccessors() {
  if (!existsSync(SUCCESSORS_DIR)) return [];
  return readdirSync(SUCCESSORS_DIR)
    .filter((name) => /^(?:codex|claude)--[0-9a-f-]{36}\.json$/i.test(name))
    .sort()
    .map((name) => readJson(path.join(SUCCESSORS_DIR, name)))
    .filter(validSuccessor);
}

export function readExecutionThread(threadId) {
  const normalized = validateUuid("execution thread id", threadId).toLowerCase();
  const record = readJson(executionThreadPath(normalized));
  return validExecutionThread(record) && record.threadId === normalized
    ? record
    : null;
}

export function listExecutionThreads() {
  if (!existsSync(EXECUTION_THREADS_DIR)) return [];
  return readdirSync(EXECUTION_THREADS_DIR)
    .filter((name) => UUID_PATTERN.test(name.slice(0, -5)) && name.endsWith(".json"))
    .sort()
    .map((name) => readJson(path.join(EXECUTION_THREADS_DIR, name)))
    .filter(validExecutionThread);
}

export function readCluster(clusterId) {
  const normalized = validateUuid("cluster id", clusterId).toLowerCase();
  const record = readJson(clusterPath(normalized));
  return validCluster(record) && record.clusterId === normalized ? record : null;
}

export function listClusters() {
  if (!existsSync(CLUSTERS_DIR)) return [];
  return readdirSync(CLUSTERS_DIR)
    .filter((name) => UUID_PATTERN.test(name.slice(0, -5)) && name.endsWith(".json"))
    .sort()
    .map((name) => readJson(path.join(CLUSTERS_DIR, name)))
    .filter(validCluster);
}

export function findClusterByRoutingId(routingId) {
  validateIdentifier("Cluster routing id", routingId);
  return listClusters().find((record) => record.routingId === routingId) || null;
}

export function readClusterTombstone(clusterId) {
  const normalized = validateUuid("cluster id", clusterId).toLowerCase();
  const record = readJson(clusterTombstonePath(normalized));
  return validClusterTombstone(record) && record.clusterId === normalized
    ? record
    : null;
}

export function listClusterTombstones() {
  if (!existsSync(CLUSTER_TOMBSTONES_DIR)) return [];
  return readdirSync(CLUSTER_TOMBSTONES_DIR)
    .filter((name) => UUID_PATTERN.test(name.slice(0, -5)) && name.endsWith(".json"))
    .sort()
    .map((name) => readJson(path.join(CLUSTER_TOMBSTONES_DIR, name)))
    .filter(validClusterTombstone);
}

export function readClusterMembership(clusterId, membershipVersion) {
  const normalized = validateUuid("cluster id", clusterId).toLowerCase();
  const record = readJson(clusterMembershipPath(normalized, membershipVersion));
  return validClusterMembership(record) &&
    record.clusterId === normalized &&
    record.membershipVersion === membershipVersion
    ? record
    : null;
}

export function listClusterMemberships(clusterId = null) {
  if (!existsSync(CLUSTER_MEMBERSHIPS_DIR)) return [];
  const normalized = clusterId
    ? validateUuid("cluster id", clusterId).toLowerCase()
    : null;
  return readdirSync(CLUSTER_MEMBERSHIPS_DIR)
    .filter((name) =>
      /^[0-9a-f-]{36}--[0-9]{10}\.json$/i.test(name) &&
      (!normalized || name.startsWith(`${normalized}--`)),
    )
    .sort()
    .map((name) => readJson(path.join(CLUSTER_MEMBERSHIPS_DIR, name)))
    .filter(validClusterMembership);
}

function writeClusterMembership(record) {
  const filename = clusterMembershipFilename(
    record.clusterId,
    record.membershipVersion,
  );
  if (existsSync(path.join(CLUSTER_MEMBERSHIPS_DIR, filename))) {
    throw new Error("Cluster membership snapshot already exists");
  }
  return atomicWrite(CLUSTER_MEMBERSHIPS_DIR, filename, record);
}

function assertClusterMembershipHead(cluster) {
  const head = readClusterMembership(
    cluster.clusterId,
    cluster.membershipVersion,
  );
  if (!head || JSON.stringify(head.members) !== JSON.stringify(cluster.members)) {
    throw new Error(
      "Cluster current membership conflicts with its immutable snapshot history",
    );
  }
  return head;
}

function validClusterMembershipTransition(current, next) {
  if (
    next.clusterId !== current.clusterId ||
    next.membershipVersion !== current.membershipVersion + 1 ||
    Date.parse(next.createdAt) < Date.parse(current.updatedAt)
  ) {
    return false;
  }
  const before = new Set(current.members);
  const after = new Set(next.members);
  const added = next.members.filter((member) => !before.has(member));
  const removed = current.members.filter((member) => !after.has(member));
  return next.changeKind === "member-added"
    ? added.length === 1 &&
        removed.length === 0 &&
        added[0] === next.changedNodeKey
    : next.changeKind === "member-removed"
      ? removed.length === 1 &&
        added.length === 0 &&
        removed[0] === next.changedNodeKey
      : false;
}

function recoverClusterMembershipLocked(current) {
  assertClusterMembershipHead(current);
  const nextVersion = current.membershipVersion + 1;
  const nextPath = clusterMembershipPath(current.clusterId, nextVersion);
  if (!existsSync(nextPath)) return { record: current, recovered: false };
  const next = readClusterMembership(current.clusterId, nextVersion);
  if (!next) {
    throw new Error("Cluster orphan membership snapshot failed schema validation");
  }
  if (existsSync(clusterMembershipPath(current.clusterId, nextVersion + 1))) {
    throw new Error("Cluster has multiple unapplied membership snapshots");
  }
  if (!validClusterMembershipTransition(current, next)) {
    throw new Error("Cluster orphan membership snapshot has an invalid transition");
  }
  for (const member of next.members) {
    if (!nodeOrTombstone(member)) {
      throw new Error("Cluster orphan membership snapshot references a missing Node");
    }
  }
  const record = atomicWrite(CLUSTERS_DIR, `${current.clusterId}.json`, {
    ...current,
    membershipVersion: next.membershipVersion,
    members: next.members,
    updatedAt: next.createdAt,
  });
  return { record, recovered: true };
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

export async function ensureCluster({
  routingId,
  clusterId = randomUUID(),
}) {
  validateIdentifier("Cluster routing id", routingId);
  clusterId = validateUuid("cluster id", clusterId).toLowerCase();
  ensureDirectory(CLUSTERS_DIR);
  ensureDirectory(CLUSTER_MEMBERSHIPS_DIR);
  ensureDirectory(CLUSTER_TOMBSTONES_DIR);
  return withFileLock(path.join(NODE_DIRECTORY_DIR, "clusters.lock"), async () => {
    const clusters = listClusters();
    const tombstones = listClusterTombstones();
    const byRouting = clusters.find((record) => record.routingId === routingId);
    const byId = clusters.find((record) => record.clusterId === clusterId);
    const crossNamespace = [...clusters, ...tombstones].find(
      (record) =>
        (record.clusterId === routingId && record.clusterId !== clusterId) ||
        (record.routingId === clusterId && record.routingId !== routingId),
    );
    const retired = tombstones.find(
      (record) => record.clusterId === clusterId || record.routingId === routingId,
    );
    if (retired || existsSync(clusterTombstonePath(clusterId))) {
      throw new Error("Cluster identity is tombstoned; automatic reactivation is forbidden");
    }
    if (crossNamespace) {
      throw new Error("Cluster routing and stable identity namespaces conflict");
    }
    if (byId && byId !== byRouting) {
      throw new Error("Cluster ID already belongs to another routing identity");
    }
    if (byRouting) {
      return recoverClusterMembershipLocked(byRouting).record;
    }
    if (existsSync(clusterPath(clusterId))) {
      throw new Error("Cluster record exists but failed schema validation");
    }
    const initialPath = clusterMembershipPath(clusterId, 1);
    if (existsSync(initialPath)) {
      const initial = readClusterMembership(clusterId, 1);
      if (
        !initial ||
        initial.changeKind !== "created" ||
        initial.members.length !== 0 ||
        existsSync(clusterMembershipPath(clusterId, 2))
      ) {
        throw new Error("Cluster initial orphan snapshot cannot be recovered");
      }
      return atomicWrite(CLUSTERS_DIR, `${clusterId}.json`, {
        version: 1,
        clusterId,
        routingId,
        membershipVersion: 1,
        members: [],
        createdAt: initial.createdAt,
        updatedAt: initial.createdAt,
      });
    }
    const now = new Date().toISOString();
    writeClusterMembership({
      version: 1,
      clusterId,
      membershipVersion: 1,
      members: [],
      changeKind: "created",
      createdAt: now,
    });
    return atomicWrite(CLUSTERS_DIR, `${clusterId}.json`, {
      version: 1,
      clusterId,
      routingId,
      membershipVersion: 1,
      members: [],
      createdAt: now,
      updatedAt: now,
    });
  });
}

async function changeClusterMember({ cluster, memberNodeKey, add }) {
  const parsed = parseNodeKey(memberNodeKey);
  memberNodeKey = nodeKey(parsed.runtimeKind, parsed.nativeId);
  ensureDirectory(CLUSTERS_DIR);
  ensureDirectory(CLUSTER_MEMBERSHIPS_DIR);
  return withFileLock(path.join(NODE_DIRECTORY_DIR, "clusters.lock"), async () => {
    let current = resolveCluster(cluster);
    if (existsSync(clusterTombstonePath(current.clusterId))) {
      throw new Error("Cluster lifecycle conflicts with a Tombstone");
    }
    current = recoverClusterMembershipLocked(current).record;
    if (add) {
      const live = readNode(parsed.runtimeKind, parsed.nativeId);
      const retired = readNodeTombstone(parsed.runtimeKind, parsed.nativeId);
      if (retired) throw new Error(`Cluster member Node is tombstoned: ${memberNodeKey}`);
      if (!live) {
        if (existsSync(nodePath(parsed.runtimeKind, parsed.nativeId))) {
          throw new Error(`Cluster member Node failed schema validation: ${memberNodeKey}`);
        }
        throw new Error(`Cluster member Node is not live: ${memberNodeKey}`);
      }
      if (current.members.includes(memberNodeKey)) return current;
      if (current.members.length >= CLUSTER_MEMBER_LIMIT) {
        throw new Error("Cluster member limit reached");
      }
    } else if (!current.members.includes(memberNodeKey)) {
      return current;
    }
    const members = add
      ? [...current.members, memberNodeKey].sort()
      : current.members.filter((identity) => identity !== memberNodeKey);
    const membershipVersion = current.membershipVersion + 1;
    const now = new Date().toISOString();
    writeClusterMembership({
      version: 1,
      clusterId: current.clusterId,
      membershipVersion,
      members,
      changeKind: add ? "member-added" : "member-removed",
      changedNodeKey: memberNodeKey,
      createdAt: now,
    });
    return atomicWrite(CLUSTERS_DIR, `${current.clusterId}.json`, {
      ...current,
      membershipVersion,
      members,
      updatedAt: now,
    });
  });
}

export async function addClusterMember({ cluster, memberNodeKey }) {
  return changeClusterMember({ cluster, memberNodeKey, add: true });
}

export async function removeClusterMember({ cluster, memberNodeKey }) {
  return changeClusterMember({ cluster, memberNodeKey, add: false });
}

export async function recoverClusterMembership(identity) {
  ensureDirectory(CLUSTERS_DIR);
  ensureDirectory(CLUSTER_MEMBERSHIPS_DIR);
  return withFileLock(path.join(NODE_DIRECTORY_DIR, "clusters.lock"), async () => {
    const current = resolveCluster(identity);
    if (existsSync(clusterTombstonePath(current.clusterId))) {
      throw new Error("Cluster lifecycle conflicts with a Tombstone");
    }
    return recoverClusterMembershipLocked(current);
  });
}

export async function tombstoneCluster(
  identity,
  { reason = "explicit", missingOk = false } = {},
) {
  validateIdentifier("Cluster Tombstone reason", reason);
  ensureDirectory(CLUSTERS_DIR);
  ensureDirectory(CLUSTER_TOMBSTONES_DIR);
  return withFileLock(path.join(NODE_DIRECTORY_DIR, "clusters.lock"), async () => {
    const tombstoneById = UUID_PATTERN.test(identity || "")
      ? readClusterTombstone(identity)
      : null;
    const tombstoneByRouting = listClusterTombstones().find(
      (record) => record.routingId === identity,
    );
    if (
      tombstoneById &&
      tombstoneByRouting &&
      tombstoneById.clusterId !== tombstoneByRouting.clusterId
    ) {
      throw new Error(`ambiguous Cluster identity: ${identity}`);
    }
    const existingTombstone = tombstoneById || tombstoneByRouting;
    if (existingTombstone) return existingTombstone;
    let current;
    try {
      current = resolveCluster(identity);
    } catch (error) {
      if (missingOk && /^unknown Cluster:/.test(error.message)) return null;
      throw error;
    }
    current = recoverClusterMembershipLocked(current).record;
    if (existsSync(clusterTombstonePath(current.clusterId))) {
      throw new Error("Cluster Tombstone exists but failed schema validation");
    }
    const removed = atomicWrite(
      CLUSTER_TOMBSTONES_DIR,
      `${current.clusterId}.json`,
      {
        version: 1,
        clusterId: current.clusterId,
        routingId: current.routingId,
        lastMembershipVersion: current.membershipVersion,
        removedAt: new Date().toISOString(),
        reason,
      },
    );
    unlinkSync(clusterPath(current.clusterId));
    return removed;
  });
}

function nodeOrTombstone(identity) {
  const { runtimeKind, nativeId } = parseNodeKey(identity);
  const live = readNode(runtimeKind, nativeId);
  const tombstone = readNodeTombstone(runtimeKind, nativeId);
  if (existsSync(nodePath(runtimeKind, nativeId)) && !live) {
    throw new Error(`Node record failed schema validation: ${identity}`);
  }
  if (existsSync(nodeTombstonePath(runtimeKind, nativeId)) && !tombstone) {
    throw new Error(`Node Tombstone failed schema validation: ${identity}`);
  }
  if (live && tombstone) {
    throw new Error(
      `Node is both live and tombstoned: ${identity}; Doctor inspection is required`,
    );
  }
  return live || tombstone;
}

function normalizeEndpoint(nodeIdentity, endpoint) {
  if (!endpoint) return null;
  const transport = validateIdentifier("endpoint transport", endpoint.transport);
  const generation = Number(endpoint.generation);
  if (!Number.isSafeInteger(generation) || generation < 0) {
    throw new Error("Endpoint generation must be a non-negative integer");
  }
  if (!ENDPOINT_STATUSES.has(endpoint.status)) {
    throw new Error("Endpoint status is invalid");
  }
  const address = endpoint.address === undefined ? null : String(endpoint.address);
  if (
    address &&
    (Buffer.byteLength(address, "utf8") > 1_024 ||
      /[\u0000-\u001f\u007f]/.test(address))
  ) {
    throw new Error("Endpoint address must be at most 1024 printable UTF-8 bytes");
  }
  const sessionName =
    endpoint.sessionName === undefined ? null : String(endpoint.sessionName);
  if (sessionName && sessionName.length > 128) {
    throw new Error("Endpoint session name exceeds 128 characters");
  }
  return {
    transport,
    endpointId: validateIdentifier("endpoint id", endpoint.endpointId),
    nodeKey: nodeIdentity,
    generation,
    status: endpoint.status,
    ...(address ? { address } : {}),
    ...(sessionName ? { sessionName } : {}),
    observedAt: new Date().toISOString(),
  };
}

function endpointObservation(candidate, decision, observedAt = candidate.observedAt) {
  return {
    transport: candidate.transport,
    endpointId: candidate.endpointId,
    nodeKey: candidate.nodeKey,
    generation: candidate.generation,
    status: candidate.status,
    decision,
    ...(candidate.address ? { address: candidate.address } : {}),
    ...(candidate.sessionName ? { sessionName: candidate.sessionName } : {}),
    firstObservedAt: observedAt,
    lastObservedAt: observedAt,
    observationCount: 1,
  };
}

function sameEndpointObservation(left, right) {
  return Boolean(
    left &&
      left.transport === right.transport &&
      left.endpointId === right.endpointId &&
      left.nodeKey === right.nodeKey &&
      left.generation === right.generation &&
      left.status === right.status &&
      left.decision === right.decision &&
      (left.address || null) === (right.address || null) &&
      (left.sessionName || null) === (right.sessionName || null),
  );
}

function compactEndpointHistory(history, selectedEndpoints) {
  if (history.length <= ENDPOINT_HISTORY_LIMIT) return history;
  const required = new Set();
  for (const selected of Object.values(selectedEndpoints)) {
    for (let index = history.length - 1; index >= 0; index -= 1) {
      const observation = history[index];
      if (
        SUCCESSFUL_ENDPOINT_DECISIONS.has(observation.decision) &&
        observation.transport === selected.transport &&
        observation.endpointId === selected.endpointId &&
        observation.generation === selected.generation
      ) {
        required.add(index);
        break;
      }
    }
  }
  const retained = new Set(required);
  for (
    let index = history.length - 1;
    index >= 0 && retained.size < ENDPOINT_HISTORY_LIMIT;
    index -= 1
  ) {
    retained.add(index);
  }
  return history.filter((_, index) => retained.has(index));
}

function appendEndpointObservation(history, candidate, decision, selectedEndpoints) {
  const observation = endpointObservation(candidate, decision);
  const last = history.at(-1);
  if (sameEndpointObservation(last, observation)) {
    history[history.length - 1] = {
      ...last,
      lastObservedAt: observation.lastObservedAt,
      observationCount: Math.min(
        Number.MAX_SAFE_INTEGER,
        last.observationCount + 1,
      ),
    };
  } else {
    history.push(observation);
  }
  return compactEndpointHistory(history, selectedEndpoints);
}

function initialEndpointHistory(current, now) {
  if (Array.isArray(current?.endpointHistory)) {
    return current.endpointHistory.map((observation) => ({ ...observation }));
  }
  return Object.values(current?.selectedEndpoints || {}).map((selected) =>
    endpointObservation(
      selected,
      "baseline-imported",
      Number.isFinite(Date.parse(selected.observedAt || ""))
        ? selected.observedAt
        : now,
    ),
  );
}

export async function upsertNode({
  runtimeKind,
  nativeId,
  displayName,
  projectId,
  endpoint = null,
}) {
  const identity = nodeKey(runtimeKind, nativeId);
  validateAlias(displayName);
  validateUuid("project id", projectId);
  if (!readProject(projectId)) throw new Error(`unknown Project: ${projectId}`);
  const candidate = normalizeEndpoint(identity, endpoint);
  ensureDirectory(NODES_DIR);
  return withFileLock(path.join(NODES_DIR, `${identity.replace(":", "--")}.lock`), async () => {
    if (runtimeKind === "codex") {
      const execution = readExecutionThread(nativeId);
      if (execution || existsSync(executionThreadPath(nativeId))) {
        throw new Error(
          execution
            ? "Execution Threads cannot be promoted to addressable Nodes"
            : "Execution Thread record exists but failed schema validation",
        );
      }
    }
    const tombstone = readNodeTombstone(runtimeKind, nativeId);
    if (tombstone || existsSync(nodeTombstonePath(runtimeKind, nativeId))) {
      throw new Error(
        tombstone
          ? "Node is tombstoned; automatic reactivation is forbidden"
          : "Node Tombstone exists but failed schema validation",
      );
    }
    const current = readNode(runtimeKind, nativeId);
    if (!current && existsSync(nodePath(runtimeKind, nativeId))) {
      throw new Error("Node record exists but failed schema validation");
    }
    if (current && current.projectId !== projectId) {
      throw new Error("Node already belongs to another Project; explicit migration is required");
    }
    const now = new Date().toISOString();
    const aliases = current ? [...current.aliases] : [];
    const alias = aliases.find((record) => record.value === displayName);
    if (alias) alias.lastSeenAt = now;
    else aliases.push({ value: displayName, firstSeenAt: now, lastSeenAt: now });
    const selectedEndpoints = { ...(current?.selectedEndpoints || {}) };
    if (
      candidate &&
      !selectedEndpoints[candidate.transport] &&
      Object.keys(selectedEndpoints).length >= ENDPOINT_TRANSPORT_LIMIT
    ) {
      throw new Error("Node Endpoint transport limit reached");
    }
    let endpointHistory = initialEndpointHistory(current, now);
    let endpointSelection = "none";
    let endpointConflict = null;
    if (candidate) {
      const selected = selectedEndpoints[candidate.transport];
      if (!selected || candidate.generation > selected.generation) {
        selectedEndpoints[candidate.transport] = candidate;
        endpointSelection = selected ? "replaced" : "selected";
      } else if (
        candidate.generation === selected.generation &&
        candidate.endpointId === selected.endpointId
      ) {
        selectedEndpoints[candidate.transport] = candidate;
        endpointSelection = "refreshed";
      } else if (candidate.generation === selected.generation) {
        endpointSelection = "conflict-rejected";
        endpointConflict = new Error(
          "Endpoint generation collision has conflicting identity; rejected observation was recorded",
        );
      } else {
        endpointSelection = "older-rejected";
      }
      endpointHistory = appendEndpointObservation(
        endpointHistory,
        candidate,
        endpointSelection,
        selectedEndpoints,
      );
    }
    if (endpointConflict) {
      atomicWrite(NODES_DIR, nodeFilename(runtimeKind, nativeId), {
        ...current,
        endpointHistory,
        updatedAt: now,
      });
      throw endpointConflict;
    }
    const record = atomicWrite(NODES_DIR, nodeFilename(runtimeKind, nativeId), {
      version: 1,
      nodeKey: identity,
      runtimeKind,
      nativeId: nativeId.toLowerCase(),
      projectId: projectId.toLowerCase(),
      aliases,
      selectedEndpoints,
      endpointHistory,
      createdAt: current?.createdAt || now,
      updatedAt: now,
    });
    return { record, endpointSelection };
  });
}

export async function tombstoneNode(
  runtimeKind,
  nativeId,
  { reason = "explicit", missingOk = false } = {},
) {
  const identity = nodeKey(runtimeKind, nativeId);
  validateIdentifier("Tombstone reason", reason);
  ensureDirectory(NODES_DIR);
  ensureDirectory(NODE_TOMBSTONES_DIR);
  return withFileLock(
    path.join(NODES_DIR, `${identity.replace(":", "--")}.lock`),
    async () => {
      const current = readNode(runtimeKind, nativeId);
      const tombstone = readNodeTombstone(runtimeKind, nativeId);
      const liveExists = existsSync(nodePath(runtimeKind, nativeId));
      const tombstoneExists = existsSync(nodeTombstonePath(runtimeKind, nativeId));
      if (liveExists && !current) {
        throw new Error("Node record exists but failed schema validation");
      }
      if (tombstoneExists && !tombstone) {
        throw new Error("Node Tombstone exists but failed schema validation");
      }
      if (current && tombstone) {
        throw new Error("Node is both live and tombstoned; Doctor inspection is required");
      }
      if (tombstone) return tombstone;
      if (!current) {
        if (missingOk) return null;
        throw new Error(`unknown Directory Node: ${identity}`);
      }
      const removed = atomicWrite(
        NODE_TOMBSTONES_DIR,
        nodeFilename(runtimeKind, nativeId),
        {
          version: 1,
          nodeKey: identity,
          runtimeKind,
          nativeId: nativeId.toLowerCase(),
          projectId: current.projectId,
          lastSafeLabel: IDENTIFIER_PATTERN.test(
            current.aliases.at(-1)?.value || "",
          )
            ? current.aliases.at(-1).value
            : identity,
          removedAt: new Date().toISOString(),
          reason,
        },
      );
      unlinkSync(nodePath(runtimeKind, nativeId));
      return removed;
    },
  );
}

export async function addSuccessor({ predecessorNodeKey, successorNodeKey }) {
  const predecessorIdentity = parseNodeKey(predecessorNodeKey);
  const successorIdentity = parseNodeKey(successorNodeKey);
  predecessorNodeKey = nodeKey(
    predecessorIdentity.runtimeKind,
    predecessorIdentity.nativeId,
  );
  successorNodeKey = nodeKey(
    successorIdentity.runtimeKind,
    successorIdentity.nativeId,
  );
  if (predecessorNodeKey === successorNodeKey) {
    throw new Error("A Node cannot be its own successor");
  }
  ensureDirectory(SUCCESSORS_DIR);
  return withFileLock(path.join(NODE_DIRECTORY_DIR, "successors.lock"), async () => {
    const predecessor = nodeOrTombstone(predecessorNodeKey);
    if (!predecessor) throw new Error(`unknown predecessor Node: ${predecessorNodeKey}`);
    const successor = readNode(
      successorIdentity.runtimeKind,
      successorIdentity.nativeId,
    );
    const successorTombstone = readNodeTombstone(
      successorIdentity.runtimeKind,
      successorIdentity.nativeId,
    );
    if (
      existsSync(
        nodeTombstonePath(
          successorIdentity.runtimeKind,
          successorIdentity.nativeId,
        ),
      ) &&
      !successorTombstone
    ) {
      throw new Error(
        `Successor Node Tombstone failed schema validation: ${successorNodeKey}`,
      );
    }
    if (successor && successorTombstone) {
      throw new Error(
        `Successor Node is both live and tombstoned: ${successorNodeKey}`,
      );
    }
    if (!successor) throw new Error(`successor Node is not live: ${successorNodeKey}`);
    if (predecessor.projectId !== successor.projectId) {
      throw new Error("Successor Nodes must belong to the same Project");
    }
    const existing = readSuccessor(successorNodeKey);
    if (existing) {
      if (existing.predecessorNodeKey === predecessorNodeKey) return existing;
      throw new Error("Successor Node already has another predecessor");
    }
    if (existsSync(successorPath(successorNodeKey))) {
      throw new Error("Successor record exists but failed schema validation");
    }
    let cursor = predecessorNodeKey;
    const visited = new Set();
    while (cursor) {
      if (cursor === successorNodeKey) {
        throw new Error("Successor relation would create a cycle");
      }
      if (visited.has(cursor)) {
        throw new Error("Existing successor records contain a cycle");
      }
      visited.add(cursor);
      cursor = readSuccessor(cursor)?.predecessorNodeKey || null;
    }
    return atomicWrite(SUCCESSORS_DIR, successorFilename(successorNodeKey), {
      version: 1,
      predecessorNodeKey,
      successorNodeKey,
      projectId: successor.projectId,
      linkedAt: new Date().toISOString(),
    });
  });
}

export async function classifyExecutionThread({
  threadId,
  jobId,
  sourceThreadId,
  creationMode,
}) {
  threadId = validateUuid("execution thread id", threadId).toLowerCase();
  jobId = validateUuid("job id", jobId).toLowerCase();
  sourceThreadId = validateUuid("source thread id", sourceThreadId).toLowerCase();
  if (threadId === sourceThreadId) {
    throw new Error("Inline source threads are not Execution Threads");
  }
  if (!EXECUTION_CREATION_MODES.has(creationMode)) {
    throw new Error("Execution Thread creation mode is invalid");
  }
  const job = readJob(jobId);
  if (
    !job ||
    (job.kind ?? "delegation") !== "delegation" ||
    job.execution !== "fork" ||
    job.targetThreadId !== sourceThreadId ||
    ![sourceThreadId, threadId].includes(job.threadId) ||
    (job.executionThreadId && job.executionThreadId !== threadId)
  ) {
    throw new Error("Execution Thread does not match a retained fork Delegation");
  }
  if (
    creationMode === "legacy-observed" &&
    (job.threadId !== threadId ||
      !UUID_PATTERN.test(job.turnId || "") ||
      ["dispatching", "queued"].includes(job.status))
  ) {
    throw new Error("Legacy Execution Thread lacks strong retained Job evidence");
  }
  ensureDirectory(NODES_DIR);
  ensureDirectory(EXECUTION_THREADS_DIR);
  return withFileLock(
    path.join(NODES_DIR, `codex--${threadId}.lock`),
    async () => {
      if (
        (existsSync(nodePath("codex", threadId)) &&
          !readNode("codex", threadId)) ||
        (existsSync(nodeTombstonePath("codex", threadId)) &&
          !readNodeTombstone("codex", threadId))
      ) {
        throw new Error("Execution Thread identity has invalid Node lifecycle state");
      }
      if (readNode("codex", threadId) || readNodeTombstone("codex", threadId)) {
        throw new Error("Addressable or Tombstoned Nodes cannot be Execution Threads");
      }
      return withFileLock(
        path.join(NODE_DIRECTORY_DIR, "execution-threads.lock"),
        async () => {
          const existing = readExecutionThread(threadId);
          if (existing) {
            if (
              existing.jobId === jobId &&
              existing.sourceThreadId === sourceThreadId &&
              existing.creationMode === creationMode
            ) {
              return existing;
            }
            throw new Error(
              "Execution Thread identity already has different provenance",
            );
          }
          if (existsSync(executionThreadPath(threadId))) {
            throw new Error(
              "Execution Thread record exists but failed schema validation",
            );
          }
          const duplicateJob = listExecutionThreads().find(
            (record) => record.jobId === jobId,
          );
          if (duplicateJob) {
            throw new Error("Job already belongs to another Execution Thread");
          }
          const sourceTombstone = readNodeTombstone("codex", sourceThreadId);
          if (sourceTombstone) {
            throw new Error(
              "Tombstoned source Nodes cannot create Execution Threads",
            );
          }
          const sourceNode = readNode("codex", sourceThreadId);
          if (
            (existsSync(nodePath("codex", sourceThreadId)) && !sourceNode) ||
            (existsSync(nodeTombstonePath("codex", sourceThreadId)) &&
              !sourceTombstone)
          ) {
            throw new Error("Source Node lifecycle state failed validation");
          }
          return atomicWrite(EXECUTION_THREADS_DIR, `${threadId}.json`, {
            version: 1,
            kind: "execution-thread",
            threadId,
            jobId,
            sourceThreadId,
            ...(sourceNode
              ? {
                  sourceNodeKey: sourceNode.nodeKey,
                  projectId: sourceNode.projectId,
                }
              : {}),
            creationMode,
            classifiedAt: new Date().toISOString(),
          });
        },
      );
    },
  );
}

export function publicProject(record, { includePaths = false } = {}) {
  if (!validProject(record)) throw new Error("invalid Project record");
  return {
    version: record.version,
    projectId: record.projectId,
    routingId: record.routingId,
    discoveryKind: record.discovery.kind,
    rootCount: record.rootAliases.length,
    ...(includePaths
      ? {
          discoveryKey: record.discovery.key,
          rootAliases: record.rootAliases,
        }
      : {}),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}

export function publicNode(
  record,
  { includeEndpoints = false, includeHistory = false } = {},
) {
  if (!validNode(record)) throw new Error("invalid Node record");
  return {
    version: record.version,
    nodeKey: record.nodeKey,
    runtimeKind: record.runtimeKind,
    nativeId: record.nativeId,
    projectId: record.projectId,
    aliases: record.aliases,
    endpointTransports: Object.keys(record.selectedEndpoints).sort(),
    endpointHistoryCount: record.endpointHistory?.length || 0,
    ...(includeEndpoints ? { selectedEndpoints: record.selectedEndpoints } : {}),
    ...(includeHistory ? { endpointHistory: record.endpointHistory || [] } : {}),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}

export function publicNodeTombstone(record) {
  if (!validNodeTombstone(record)) {
    throw new Error("invalid Node Tombstone record");
  }
  return {
    version: record.version,
    nodeKey: record.nodeKey,
    runtimeKind: record.runtimeKind,
    nativeId: record.nativeId,
    projectId: record.projectId,
    lastSafeLabel: record.lastSafeLabel,
    removedAt: record.removedAt,
    reason: record.reason,
  };
}

export function publicSuccessor(record) {
  if (!validSuccessor(record)) throw new Error("invalid successor record");
  return {
    version: record.version,
    predecessorNodeKey: record.predecessorNodeKey,
    successorNodeKey: record.successorNodeKey,
    projectId: record.projectId,
    linkedAt: record.linkedAt,
  };
}

export function publicExecutionThread(record) {
  if (!validExecutionThread(record)) {
    throw new Error("invalid Execution Thread record");
  }
  return {
    version: record.version,
    kind: record.kind,
    threadId: record.threadId,
    jobId: record.jobId,
    sourceThreadId: record.sourceThreadId,
    ...(record.sourceNodeKey ? { sourceNodeKey: record.sourceNodeKey } : {}),
    ...(record.projectId ? { projectId: record.projectId } : {}),
    creationMode: record.creationMode,
    classifiedAt: record.classifiedAt,
  };
}

export function publicCluster(record, { includeMembers = false } = {}) {
  if (!validCluster(record)) throw new Error("invalid Cluster record");
  return {
    version: record.version,
    clusterId: record.clusterId,
    routingId: record.routingId,
    membershipVersion: record.membershipVersion,
    memberCount: record.members.length,
    ...(includeMembers ? { members: record.members } : {}),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}

export function publicClusterMembership(
  record,
  { includeMembers = false } = {},
) {
  if (!validClusterMembership(record)) {
    throw new Error("invalid Cluster membership snapshot");
  }
  return {
    version: record.version,
    clusterId: record.clusterId,
    membershipVersion: record.membershipVersion,
    memberCount: record.members.length,
    changeKind: record.changeKind,
    ...(includeMembers
      ? {
          ...(record.changedNodeKey
            ? { changedNodeKey: record.changedNodeKey }
            : {}),
          members: record.members,
        }
      : {}),
    createdAt: record.createdAt,
  };
}

export function publicClusterTombstone(record) {
  if (!validClusterTombstone(record)) {
    throw new Error("invalid Cluster Tombstone record");
  }
  return { ...record };
}
