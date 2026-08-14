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
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { withFileLock } from "./file-lock.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const NODE_DIRECTORY_DIR = path.join(CXMSG_STATE_DIR, "directory");
export const NODES_DIR = path.join(NODE_DIRECTORY_DIR, "nodes");
export const PROJECTS_DIR = path.join(NODE_DIRECTORY_DIR, "projects");

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
      Object.entries(record.selectedEndpoints).every(
        ([transport, endpoint]) =>
          IDENTIFIER_PATTERN.test(transport) &&
          endpoint?.transport === transport &&
          endpoint.nodeKey === record.nodeKey &&
          IDENTIFIER_PATTERN.test(endpoint.endpointId || "") &&
          Number.isSafeInteger(endpoint.generation) &&
          endpoint.generation >= 0 &&
          ENDPOINT_STATUSES.has(endpoint.status),
      ),
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
  return {
    transport,
    endpointId: validateIdentifier("endpoint id", endpoint.endpointId),
    nodeKey: nodeIdentity,
    generation,
    status: endpoint.status,
    ...(endpoint.address ? { address: String(endpoint.address) } : {}),
    ...(endpoint.sessionName ? { sessionName: String(endpoint.sessionName) } : {}),
    observedAt: new Date().toISOString(),
  };
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
    let endpointSelection = "none";
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
        throw new Error("Endpoint generation collision has conflicting identity");
      } else {
        endpointSelection = "older-rejected";
      }
    }
    const record = atomicWrite(NODES_DIR, nodeFilename(runtimeKind, nativeId), {
      version: 1,
      nodeKey: identity,
      runtimeKind,
      nativeId: nativeId.toLowerCase(),
      projectId: projectId.toLowerCase(),
      aliases,
      selectedEndpoints,
      createdAt: current?.createdAt || now,
      updatedAt: now,
    });
    return { record, endpointSelection };
  });
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

export function publicNode(record, { includeEndpoints = false } = {}) {
  if (!validNode(record)) throw new Error("invalid Node record");
  return {
    version: record.version,
    nodeKey: record.nodeKey,
    runtimeKind: record.runtimeKind,
    nativeId: record.nativeId,
    projectId: record.projectId,
    aliases: record.aliases,
    endpointTransports: Object.keys(record.selectedEndpoints).sort(),
    ...(includeEndpoints ? { selectedEndpoints: record.selectedEndpoints } : {}),
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}
