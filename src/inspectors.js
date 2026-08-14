import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  lstatSync,
  readFileSync,
  readdirSync,
} from "node:fs";
import path from "node:path";
import { probeAppServerSocket, withAppServer } from "./app-server-client.js";
import {
  CLAUDE_BRIDGE_IMPLEMENTATION_REVISION,
  probeClaudeBridge,
} from "./claude-bridge.js";
import { hostRelayRequest } from "./host-relay.js";
import {
  MESSAGE_BODY_SEGMENT_BYTES,
  MESSAGE_BODY_STORE_QUOTA_BYTES,
  MAX_STORED_MESSAGE_BYTES,
} from "./message-bodies.js";
import { processIdentity, processState, serviceEvidence } from "./process-state.js";
import { EVENT_LOG_ARCHIVES, EVENT_LOG_MAX_BYTES } from "./runtime.js";
import { failedProbe } from "./socket-probe.js";
import { readThreadMetadata } from "./thread-activity.js";
import { CXMSG_VERSION } from "./version.js";

const UUID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const SESSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const MAX_RECORD_BYTES = 256 * 1024;
const MAX_DIRECTORY_BYTES = 32 * 1024 * 1024;
const MAX_DIRECTORY_RECORDS = 2048;
const PENDING_JOB_STATES = new Set([
  "dispatching",
  "queued",
  "running",
  "awaiting_approval",
]);

export function diagnosticCheck({
  id,
  scope,
  status,
  summary,
  verification = null,
  errorCode = null,
  repairable = false,
  remediation = null,
  required = true,
}) {
  return Object.fromEntries(
    Object.entries({
      id,
      scope,
      status,
      summary,
      verification,
      errorCode,
      repairable,
      remediation,
      required,
    }).filter(([, value]) => value !== null),
  );
}

function safeLabel(value) {
  if (typeof value === "string" && UUID_PATTERN.test(value)) return value.slice(0, 8);
  if (typeof value === "string" && SESSION_PATTERN.test(value)) return value;
  return `record-${createHash("sha256").update(String(value)).digest("hex").slice(0, 10)}`;
}

function errorCode(error, fallback = "EINSPECT") {
  return typeof error?.code === "string" && /^[A-Z0-9_]{1,32}$/.test(error.code)
    ? error.code
    : fallback;
}

function secureMetadata(target, expectedType) {
  try {
    const metadata = lstatSync(target);
    if (metadata.isSymbolicLink()) return { status: "symlink", metadata };
    const typeMatches =
      expectedType === "directory"
        ? metadata.isDirectory()
        : expectedType === "socket"
          ? metadata.isSocket()
          : metadata.isFile();
    if (!typeMatches) return { status: "wrong-type", metadata };
    if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
      return { status: "wrong-owner", metadata };
    }
    if ((metadata.mode & 0o077) !== 0) return { status: "broad-mode", metadata };
    return { status: "secure", metadata };
  } catch (error) {
    return {
      status: error?.code === "ENOENT" ? "missing" : "unavailable",
      errorCode: errorCode(error),
    };
  }
}

function metadataFinding(id, scope, label, evidence, { required = true } = {}) {
  if (evidence.status === "secure") {
    return diagnosticCheck({
      id,
      scope,
      status: "pass",
      summary: `${label} has restrictive owner-only metadata`,
      verification: "metadata",
      required,
    });
  }
  if (evidence.status === "missing") {
    return diagnosticCheck({
      id,
      scope,
      status: required ? "fail" : "skipped",
      summary: `${label} is not present`,
      verification: "missing",
      errorCode: "ENOENT",
      required,
    });
  }
  const status = evidence.status === "unavailable" ? "unknown" : "fail";
  return diagnosticCheck({
    id,
    scope,
    status,
    summary: `${label} failed its type, owner, or mode check`,
    verification: evidence.status,
    errorCode: evidence.errorCode || "ESECURESTATE",
    remediation: "Inspect the owner, mode, type, and symlink status from an allowed host context",
    required,
  });
}

function scanJsonDirectory({
  stateDir,
  directoryName,
  scope,
  validate,
  maxRecordBytes = MAX_RECORD_BYTES,
}) {
  const directory = path.join(stateDir, directoryName);
  const checks = [];
  const directoryEvidence = secureMetadata(directory, "directory");
  if (directoryEvidence.status === "missing") {
    checks.push(metadataFinding(`${scope}.directory`, scope, `${scope} directory`, directoryEvidence, { required: false }));
    return { checks, records: [] };
  }
  checks.push(metadataFinding(`${scope}.directory`, scope, `${scope} directory`, directoryEvidence));
  if (directoryEvidence.status !== "secure") return { checks, records: [] };

  let names;
  try {
    names = readdirSync(directory).filter((name) => name.endsWith(".json")).sort();
  } catch (error) {
    checks.push(diagnosticCheck({
      id: `${scope}.records.read`,
      scope,
      status: "unknown",
      summary: `${scope} records could not be enumerated`,
      verification: "unavailable",
      errorCode: errorCode(error),
    }));
    return { checks, records: [] };
  }
  if (names.length > MAX_DIRECTORY_RECORDS) {
    checks.push(diagnosticCheck({
      id: `${scope}.records.bound`,
      scope,
      status: "warn",
      summary: `${scope} record count exceeds the bounded inspection limit`,
      verification: "bounded",
      errorCode: "ERECORDLIMIT",
      remediation: "Inspect excess records from an allowed host context",
    }));
    names = names.slice(0, MAX_DIRECTORY_RECORDS);
  }

  const records = [];
  let parsedBytes = 0;
  for (const name of names) {
    const stem = name.slice(0, -5);
    const label = safeLabel(stem);
    const file = path.join(directory, name);
    const evidence = secureMetadata(file, "file");
    if (evidence.status !== "secure") {
      checks.push(metadataFinding(`${scope}.record.${label}.metadata`, scope, `${scope} record ${label}`, evidence));
      continue;
    }
    if (evidence.metadata.size > maxRecordBytes) {
      checks.push(diagnosticCheck({
        id: `${scope}.record.${label}.size`,
        scope,
        status: "fail",
        summary: `${scope} record ${label} exceeds the bounded parse limit`,
        verification: "metadata",
        errorCode: "ERECORDSIZE",
      }));
      continue;
    }
    if (parsedBytes + evidence.metadata.size > MAX_DIRECTORY_BYTES) {
      checks.push(diagnosticCheck({
        id: `${scope}.records.bytes`,
        scope,
        status: "warn",
        summary: `${scope} records exceed the bounded aggregate parse limit`,
        verification: "bounded",
        errorCode: "ERECORDBYTES",
        remediation: "Inspect excess records from an allowed host context",
      }));
      break;
    }
    parsedBytes += evidence.metadata.size;
    try {
      const record = JSON.parse(readFileSync(file, "utf8"));
      const verdict = validate(record, stem);
      if (!verdict.valid) {
        checks.push(diagnosticCheck({
          id: `${scope}.record.${label}.schema`,
          scope,
          status: "fail",
          summary: `${scope} record ${label} has an invalid schema or filename identity`,
          verification: "schema",
          errorCode: verdict.errorCode || "ESCHEMA",
        }));
        continue;
      }
      records.push(record);
    } catch {
      checks.push(diagnosticCheck({
        id: `${scope}.record.${label}.json`,
        scope,
        status: "fail",
        summary: `${scope} record ${label} is not valid bounded JSON`,
        verification: "parse",
        errorCode: "EJSON",
      }));
    }
  }
  if (!checks.some((check) => check.status === "fail" || check.status === "unknown")) {
    checks.push(diagnosticCheck({
      id: `${scope}.records.schema`,
      scope,
      status: "pass",
      summary: `${records.length} ${scope} record(s) passed bounded schema inspection`,
      verification: "schema",
    }));
  }
  return { checks, records };
}

function validSession(record, stem) {
  const valid = Boolean(
    record &&
      record.name === stem &&
      SESSION_PATTERN.test(record.name) &&
      UUID_PATTERN.test(record.threadId || "") &&
      typeof record.cwd === "string" &&
      path.isAbsolute(record.cwd),
  );
  if (!valid) return { valid: false, errorCode: "ESESSIONSCHEMA" };
  if (record.allowedDelegators && !Array.isArray(record.allowedDelegators)) {
    return { valid: false, errorCode: "EGRANTSCHEMA" };
  }
  if (
    Array.isArray(record.allowedClaudeRequesters) &&
    record.allowedClaudeRequesters.some(
      (grant) =>
        !grant ||
        !UUID_PATTERN.test(grant.sessionId || "") ||
        !UUID_PATTERN.test(grant.token || "") ||
        typeof grant.permissions !== "string" ||
        !["never", "relay", "auto"].includes(grant.approval || "never"),
    )
  ) {
    return { valid: false, errorCode: "ECLAUDEGRANTSCHEMA" };
  }
  return { valid: true };
}

function validAttachment(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.name === stem &&
        UUID_PATTERN.test(record.threadId || "") &&
        Number.isSafeInteger(record.childPid) &&
        record.childPid > 1 &&
        typeof record.cwd === "string" &&
        path.isAbsolute(record.cwd),
    ),
    errorCode: "EATTACHMENTSCHEMA",
  };
}

function validJob(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.jobId === stem &&
        UUID_PATTERN.test(record.jobId || "") &&
        (record.kind === undefined || typeof record.kind === "string") &&
        typeof record.status === "string",
    ),
    errorCode: "EJOBSCHEMA",
  };
}

function validBridge(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.target === stem &&
        UUID_PATTERN.test(record.targetThreadId || "") &&
        Number.isSafeInteger(record.pid) &&
        record.pid > 1 &&
        typeof record.socketPath === "string" &&
        Number.isSafeInteger(record.startedAt) &&
        (record.cxmsgVersion === undefined ||
          typeof record.cxmsgVersion === "string") &&
        (record.implementationRevision === undefined ||
          (Number.isSafeInteger(record.implementationRevision) &&
            record.implementationRevision > 0)),
    ),
    errorCode: "EBRIDGESCHEMA",
  };
}

function validRouteBinding(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.sessionName === stem &&
        SESSION_PATTERN.test(record.sessionName || "") &&
        UUID_PATTERN.test(record.threadId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.projectId || "") &&
        (record.projectKey === undefined || UUID_PATTERN.test(record.projectKey)) &&
        (record.nodeKey === undefined ||
          record.nodeKey === `codex:${record.threadId}`) &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.role || ""),
    ),
    errorCode: "EROUTEBINDINGSCHEMA",
  };
}

function validRouteDelivery(record, stem) {
  const expectedFingerprint = createHash("sha256")
    .update(JSON.stringify(record?.route ?? null))
    .digest("hex");
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.logicalMessageId === stem &&
        UUID_PATTERN.test(record.logicalMessageId || "") &&
        SESSION_PATTERN.test(record.from || "") &&
        SESSION_PATTERN.test(record.target || "") &&
        /^[0-9a-f]{64}$/.test(record.messageSha256 || "") &&
        record.routeFingerprint === expectedFingerprint &&
        ["admitted", "quarantined"].includes(record.admissionState) &&
        ["dispatching", "turn_started", "unknown", "quarantined"].includes(
          record.status,
        ),
    ),
    errorCode: "EROUTEDELIVERYSCHEMA",
  };
}

function validQuarantine(record, stem) {
  const messageBytes = Buffer.byteLength(record?.message || "", "utf8");
  const expectedDigest = createHash("sha256")
    .update(record?.message || "")
    .digest("hex");
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.logicalMessageId === stem &&
        UUID_PATTERN.test(record.logicalMessageId || "") &&
        SESSION_PATTERN.test(record.from || "") &&
        SESSION_PATTERN.test(record.target || "") &&
        typeof record.reason === "string" &&
        typeof record.message === "string" &&
        messageBytes <= MAX_STORED_MESSAGE_BYTES &&
        record.messageBytes === messageBytes &&
        record.messageSha256 === expectedDigest,
    ),
    errorCode: "EQUARANTINESCHEMA",
  };
}

function validDirectoryProject(record, stem) {
  return {
    valid: Boolean(
      record?.version === 1 &&
        record.projectId === stem &&
        UUID_PATTERN.test(record.projectId || "") &&
        /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(record.routingId || "") &&
        ["git-common-dir", "canonical-root"].includes(record.discovery?.kind) &&
        typeof record.discovery?.key === "string" &&
        path.isAbsolute(record.discovery.key) &&
        Array.isArray(record.rootAliases) &&
        record.rootAliases.every(
          (alias) => typeof alias?.path === "string" && path.isAbsolute(alias.path),
        ),
    ),
    errorCode: "EPROJECTSCHEMA",
  };
}

function validDirectoryNode(record, stem) {
  const expectedStem = `${record?.runtimeKind || ""}--${record?.nativeId || ""}`;
  const endpoints = Object.entries(record?.selectedEndpoints || {});
  return {
    valid: Boolean(
      record?.version === 1 &&
        expectedStem === stem &&
        ["codex", "claude"].includes(record.runtimeKind) &&
        UUID_PATTERN.test(record.nativeId || "") &&
        record.nodeKey === `${record.runtimeKind}:${record.nativeId}` &&
        UUID_PATTERN.test(record.projectId || "") &&
        Array.isArray(record.aliases) &&
        record.aliases.every(
          (alias) =>
            typeof alias?.value === "string" &&
            alias.value.length > 0 &&
            alias.value.length <= 128,
        ) &&
        endpoints.every(
          ([transport, endpoint]) =>
            endpoint?.transport === transport &&
            endpoint.nodeKey === record.nodeKey &&
            Number.isSafeInteger(endpoint.generation) &&
            endpoint.generation >= 0 &&
            [
              "reachable",
              "external-writer",
              "unreachable",
              "stale",
              "unknown",
              "mismatched",
            ].includes(endpoint.status),
        ),
    ),
    errorCode: "ENODESCHEMA",
  };
}

export function inspectNodeDirectory({ stateDir, sessions = [] } = {}) {
  const projects = scanJsonDirectory({
    stateDir,
    directoryName: "directory/projects",
    scope: "directory-projects",
    validate: validDirectoryProject,
  });
  const nodes = scanJsonDirectory({
    stateDir,
    directoryName: "directory/nodes",
    scope: "directory-nodes",
    validate: validDirectoryNode,
  });
  const checks = [...projects.checks, ...nodes.checks];
  const projectIds = new Set(projects.records.map((record) => record.projectId));
  const seenRoutingIds = new Set();
  const seenDiscoveries = new Set();
  for (const project of projects.records) {
    const discovery = `${project.discovery.kind}:${project.discovery.key}`;
    const duplicateRouting = seenRoutingIds.has(project.routingId);
    const duplicateDiscovery = seenDiscoveries.has(discovery);
    checks.push(
      diagnosticCheck({
        id: `directory-projects.identity.${project.projectId.slice(0, 8)}`,
        scope: "directory-projects",
        status: duplicateRouting || duplicateDiscovery ? "fail" : "pass",
        summary:
          duplicateRouting || duplicateDiscovery
            ? "Project has a duplicate routing or discovery identity"
            : `Project ${project.routingId} has unique routing and discovery identity`,
        verification: "records",
        errorCode:
          duplicateRouting || duplicateDiscovery ? "EPROJECTIDENTITY" : null,
      }),
    );
    seenRoutingIds.add(project.routingId);
    seenDiscoveries.add(discovery);
  }
  const sessionThreadIds = new Set(sessions.map((record) => record.threadId));
  for (const node of nodes.records) {
    const projectExists = projectIds.has(node.projectId);
    checks.push(
      diagnosticCheck({
        id: `directory-nodes.project.${safeLabel(node.nodeKey)}`,
        scope: "directory-nodes",
        status: projectExists ? "pass" : "fail",
        summary: projectExists
          ? "Node references an existing private Project identity"
          : "Node references a missing private Project identity",
        verification: "records",
        errorCode: projectExists ? null : "ENODEPROJECT",
      }),
    );
    if (node.runtimeKind === "codex" && !sessionThreadIds.has(node.nativeId)) {
      checks.push(
        diagnosticCheck({
          id: `directory-nodes.registration.${safeLabel(node.nodeKey)}`,
          scope: "directory-nodes",
          status: "warn",
          summary: "Codex Node no longer has a registered addressable session",
          verification: "registry",
          errorCode: "ENODEUNREGISTERED",
          remediation:
            "Retain the Node until explicit Tombstone lifecycle support is implemented",
          required: false,
        }),
      );
    }
  }
  return checks;
}

export function inspectRouteState({ stateDir, sessions = [] } = {}) {
  const bindings = scanJsonDirectory({
    stateDir,
    directoryName: "route-bindings",
    scope: "route-bindings",
    validate: validRouteBinding,
  });
  const deliveries = scanJsonDirectory({
    stateDir,
    directoryName: "route-deliveries",
    scope: "route-deliveries",
    validate: validRouteDelivery,
  });
  const quarantine = scanJsonDirectory({
    stateDir,
    directoryName: "quarantine",
    scope: "quarantine",
    validate: validQuarantine,
    maxRecordBytes: MAX_STORED_MESSAGE_BYTES + 16 * 1024,
  });
  const checks = [
    ...bindings.checks,
    ...deliveries.checks,
    ...quarantine.checks,
  ];
  const sessionsByName = new Map(sessions.map((record) => [record.name, record]));
  for (const binding of bindings.records) {
    const current = sessionsByName.get(binding.sessionName);
    const matches = current?.threadId === binding.threadId;
    checks.push(
      diagnosticCheck({
        id: `route-bindings.session.${safeLabel(binding.sessionName)}`,
        scope: "route-bindings",
        status: matches ? "pass" : "fail",
        summary: matches
          ? `Route binding for ${binding.sessionName} matches its registered thread`
          : `Route binding for ${binding.sessionName} does not match a registered thread`,
        verification: "registry",
        errorCode: matches ? null : "EROUTEIDENTITY",
        remediation: matches
          ? null
          : "Re-bind the intended registered session; do not release quarantined messages automatically",
      }),
    );
  }
  checks.push(
    diagnosticCheck({
      id: "quarantine.records.count",
      scope: "quarantine",
      status: quarantine.records.length > 0 ? "warn" : "pass",
      summary:
        quarantine.records.length > 0
          ? `${quarantine.records.length} quarantined route message(s) require operator review`
          : "No routed peer messages are quarantined",
      verification: "records",
      errorCode: quarantine.records.length > 0 ? "EQUARANTINED" : null,
      remediation:
        quarantine.records.length > 0
          ? "Inspect metadata with cxmsg quarantine list; this release intentionally has no automatic release"
          : null,
      required: false,
    }),
  );
  return checks;
}

export function inspectRuntime({ codexBin = process.env.CODEX_BIN || "codex", run = spawnSync } = {}) {
  const major = Number.parseInt(process.versions.node.split(".")[0], 10);
  const checks = [
    diagnosticCheck({
      id: "runtime.node.version",
      scope: "runtime",
      status: major >= 20 ? "pass" : "fail",
      summary: major >= 20 ? "Node.js satisfies the minimum major version" : "Node.js is below the required major version",
      verification: "runtime",
      errorCode: major >= 20 ? null : "ENODEVERSION",
      remediation: major >= 20 ? null : "Use Node.js 20 or newer",
    }),
    diagnosticCheck({
      id: "runtime.cxmsg.version",
      scope: "runtime",
      status: /^\d+\.\d+\.\d+/.test(CXMSG_VERSION) ? "pass" : "fail",
      summary: `cxmsg version metadata is available (${CXMSG_VERSION})`,
      verification: "package",
    }),
  ];
  const result = run(codexBin, ["--version"], { encoding: "utf8" });
  checks.push(diagnosticCheck({
    id: "runtime.codex.version",
    scope: "runtime",
    status: result.status === 0 ? "pass" : "unknown",
    summary: result.status === 0 ? "Codex CLI version metadata is available" : "Codex CLI version could not be inspected",
    verification: result.status === 0 ? "process-output" : "unavailable",
    errorCode: result.status === 0 ? null : "ECODEXVERSION",
    remediation: result.status === 0 ? null : "Run doctor where the configured Codex executable is available",
  }));
  return checks;
}

export function inspectState({ stateDir, target = null } = {}) {
  const checks = [];
  const rootEvidence = secureMetadata(stateDir, "directory");
  checks.push(metadataFinding("state.root.metadata", "state", "cxmsg state directory", rootEvidence));
  if (rootEvidence.status !== "secure") {
    return { checks, sessions: [], attachments: [], jobs: [], bridges: [] };
  }

  let eventNames = [];
  try {
    eventNames = readdirSync(stateDir)
      .filter((name) => /^events\.jsonl(?:\.\d+)?$/.test(name))
      .sort();
  } catch (error) {
    checks.push(diagnosticCheck({
      id: "state.events.segments",
      scope: "state",
      status: "unknown",
      summary: "Coordination event segments could not be enumerated",
      verification: "unavailable",
      errorCode: errorCode(error),
      required: false,
    }));
  }
  let invalidEventSegments = 0;
  for (const name of eventNames) {
    const evidence = secureMetadata(path.join(stateDir, name), "file");
    if (evidence.status !== "secure" || evidence.metadata.size > EVENT_LOG_MAX_BYTES) {
      invalidEventSegments += 1;
    }
  }
  const tooManyEventSegments = eventNames.length > EVENT_LOG_ARCHIVES + 1;
  if (!checks.some((check) => check.id === "state.events.segments")) {
    checks.push(diagnosticCheck({
      id: "state.events.segments",
      scope: "state",
      status: invalidEventSegments || tooManyEventSegments ? "warn" : "pass",
      summary:
        invalidEventSegments || tooManyEventSegments
          ? "Coordination event segments exceed their owner, mode, size, or count bound"
          : `${eventNames.length} coordination event segment(s) satisfy the bounded retention policy`,
      verification: "metadata",
      errorCode: invalidEventSegments || tooManyEventSegments ? "EEVENTBOUND" : null,
      remediation: invalidEventSegments || tooManyEventSegments ? "Allow the next coordination event to rotate legacy oversized state; do not delete active state from Doctor" : null,
      required: false,
    }));
  }

  const sessions = scanJsonDirectory({ stateDir, directoryName: "sessions", scope: "sessions", validate: validSession });
  const attachments = scanJsonDirectory({ stateDir, directoryName: "attachments", scope: "attachments", validate: validAttachment });
  const jobs = scanJsonDirectory({ stateDir, directoryName: "jobs", scope: "jobs", validate: validJob });
  const bridges = scanJsonDirectory({ stateDir, directoryName: "claude-bridges", scope: "bridges", validate: validBridge });
  checks.push(...sessions.checks, ...attachments.checks, ...jobs.checks, ...bridges.checks);

  if (target && !sessions.records.some((record) => record.name === target)) {
    checks.push(diagnosticCheck({
      id: "sessions.target.exists",
      scope: "sessions",
      status: "fail",
      summary: `Target session ${target} is not registered`,
      verification: "registry",
      errorCode: "EUNKNOWNTARGET",
    }));
  }

  const byThread = new Map();
  for (const record of sessions.records) {
    const previous = byThread.get(record.threadId);
    if (previous) {
      checks.push(diagnosticCheck({
        id: `sessions.thread.${record.threadId.slice(0, 8)}.duplicate`,
        scope: "sessions",
        status: "fail",
        summary: "Multiple session names reference the same Codex thread",
        verification: "registry",
        errorCode: "EDUPLICATETHREAD",
      }));
    }
    byThread.set(record.threadId, record.name);
  }

  return {
    checks,
    allSessions: sessions.records,
    sessions: target
      ? sessions.records.filter((record) => record.name === target)
      : sessions.records,
    attachments: target
      ? attachments.records.filter((record) => record.name === target)
      : attachments.records,
    jobs: target
      ? jobs.records.filter((record) => record.target === target || record.from === target)
      : jobs.records,
    bridges: target
      ? bridges.records.filter((record) => record.target === target)
      : bridges.records,
  };
}

export function inspectMessageBodies({
  stateDir,
  quotaBytes = MESSAGE_BODY_STORE_QUOTA_BYTES,
  segmentBytes = MESSAGE_BODY_SEGMENT_BYTES,
} = {}) {
  const scope = "message-bodies";
  const root = path.join(stateDir, "message-bodies");
  const rootEvidence = secureMetadata(root, "directory");
  if (rootEvidence.status === "missing") {
    return [
      metadataFinding(
        "message-bodies.directory",
        scope,
        "Message Body Store directory",
        rootEvidence,
        { required: false },
      ),
    ];
  }
  const checks = [
    metadataFinding(
      "message-bodies.directory",
      scope,
      "Message Body Store directory",
      rootEvidence,
    ),
  ];
  if (rootEvidence.status !== "secure") return checks;

  const collections = [
    {
      name: "segments",
      pattern: /^segment-\d{8}\.jsonl$/,
      partial: false,
    },
    {
      name: "quarantine",
      pattern: /^segment-\d{8}\.partial-[0-9a-f-]+\.jsonl$/i,
      partial: true,
    },
  ];
  let totalBytes = 0;
  let partialSegments = 0;
  for (const collection of collections) {
    const directory = path.join(root, collection.name);
    const evidence = secureMetadata(directory, "directory");
    checks.push(
      metadataFinding(
        `message-bodies.${collection.name}.directory`,
        scope,
        `Message Body Store ${collection.name} directory`,
        evidence,
      ),
    );
    if (evidence.status !== "secure") continue;
    let names;
    try {
      names = readdirSync(directory).sort();
    } catch (error) {
      checks.push(
        diagnosticCheck({
          id: `message-bodies.${collection.name}.enumeration`,
          scope,
          status: "unknown",
          summary: `Message Body Store ${collection.name} could not be enumerated`,
          verification: "unavailable",
          errorCode: errorCode(error),
        }),
      );
      continue;
    }
    const invalidNames = names.filter((name) => !collection.pattern.test(name));
    if (invalidNames.length > 0) {
      checks.push(
        diagnosticCheck({
          id: `message-bodies.${collection.name}.names`,
          scope,
          status: "fail",
          summary: `Message Body Store ${collection.name} contains unexpected entries`,
          verification: "metadata",
          errorCode: "EMESSAGEBODYNAME",
        }),
      );
    }
    for (const name of names.filter((candidate) => collection.pattern.test(candidate))) {
      const fileEvidence = secureMetadata(path.join(directory, name), "file");
      const label = safeLabel(name);
      if (fileEvidence.status !== "secure") {
        checks.push(
          metadataFinding(
            `message-bodies.${collection.name}.${label}.metadata`,
            scope,
            `Message Body Store ${collection.name} segment`,
            fileEvidence,
          ),
        );
        continue;
      }
      totalBytes += fileEvidence.metadata.size;
      if (fileEvidence.metadata.size > segmentBytes) {
        checks.push(
          diagnosticCheck({
            id: `message-bodies.${collection.name}.${label}.size`,
            scope,
            status: "fail",
            summary: `Message Body Store ${collection.name} segment exceeds its size bound`,
            verification: "metadata",
            errorCode: "EMESSAGEBODYSEGMENT",
          }),
        );
      }
      if (collection.partial) partialSegments += 1;
    }
  }

  checks.push(
    diagnosticCheck({
      id: "message-bodies.quarantine.count",
      scope,
      status: partialSegments > 0 ? "warn" : "pass",
      summary:
        partialSegments > 0
          ? `${partialSegments} quarantined Message Body Store segment(s) require retained operator review`
          : "No partial Message Body Store segments are quarantined",
      verification: "metadata",
      errorCode: partialSegments > 0 ? "EMESSAGEBODYPARTIAL" : null,
      remediation:
        partialSegments > 0
          ? "Retain quarantined segments until an explicit audited purge policy exists"
          : null,
      required: false,
    }),
  );
  const quotaRatio = quotaBytes > 0 ? totalBytes / quotaBytes : Infinity;
  checks.push(
    diagnosticCheck({
      id: "message-bodies.quota.usage",
      scope,
      status: quotaRatio >= 0.9 ? "warn" : "pass",
      summary:
        quotaRatio > 1
          ? "Message Body Store exceeds its configured write quota"
          : quotaRatio >= 0.9
            ? "Message Body Store is at least 90 percent of its configured write quota"
            : "Message Body Store is below 90 percent of its configured write quota",
      verification: "metadata",
      errorCode:
        quotaRatio > 1
          ? "EMESSAGEBODYQUOTA"
          : quotaRatio >= 0.9
            ? "EMESSAGEBODYQUOTANEAR"
            : null,
      remediation:
        quotaRatio >= 0.9
          ? "Do not delete segments manually; define and run an explicit audited purge after retained references are accounted for"
          : null,
      required: false,
    }),
  );
  return checks;
}

export function inspectJobs(jobs, { now = Date.now(), processStateFn = processState } = {}) {
  const checks = [];
  let active = 0;
  let legacy = 0;
  for (const job of jobs) {
    const kind = job.kind || "delegation";
    if (!job.kind) legacy += 1;
    if (!PENDING_JOB_STATES.has(job.status)) continue;
    active += 1;
    const label = job.jobId.slice(0, 8);
    if (kind === "delegation") {
      if (!Number.isSafeInteger(job.workerPid)) {
        const age = now - Date.parse(job.createdAt);
        checks.push(diagnosticCheck({
          id: `jobs.worker.${label}.missing`,
          scope: "jobs",
          status: Number.isFinite(age) && age <= 10_000 ? "warn" : "fail",
          summary: `Pending Delegation ${label} has no registered worker`,
          verification: "record",
          errorCode: "EWORKERMISSING",
          remediation: "Inspect the correlated Job from an allowed host context",
        }));
        continue;
      }
      const state = processStateFn(job.workerPid);
      checks.push(diagnosticCheck({
        id: `jobs.worker.${label}.process`,
        scope: "jobs",
        status: state === "alive" ? "pass" : state === "unverified" ? "unknown" : "fail",
        summary:
          state === "alive"
            ? `Pending Delegation ${label} has a live worker`
            : state === "unverified"
              ? `Pending Delegation ${label} worker cannot be verified by this caller`
              : `Pending Delegation ${label} worker is missing`,
        verification: state,
        errorCode: state === "alive" ? null : state === "unverified" ? "EPERM" : "ESRCH",
      }));
    }
    if (
      kind === "claude-delivery" &&
      job.status === "transport_delivered" &&
      Date.parse(job.delivery?.ackDeadlineAt) <= now
    ) {
      checks.push(diagnosticCheck({
        id: `jobs.claude.${label}.ack-overdue`,
        scope: "jobs",
        status: "warn",
        summary: `Claude Delivery ${label} is past its ACK deadline without reconciliation`,
        verification: "record",
        errorCode: "EACKOVERDUE",
      }));
    }
    if (
      kind === "claude-delivery" &&
      ["ack_timeout", "completion_timeout"].includes(job.status) &&
      job.wake?.status === "delivered"
    ) {
      checks.push(diagnosticCheck({
        id: `jobs.claude.${label}.reconciliation`,
        scope: "jobs",
        status: "fail",
        summary: `Claude Delivery ${label} has a delivered reply wake without terminal Delivery reconciliation`,
        verification: "record",
        errorCode: "EUNRECONCILEDREPLY",
      }));
    }
  }
  if (legacy > 0) {
    checks.push(diagnosticCheck({
      id: "jobs.records.legacy-kind",
      scope: "jobs",
      status: "warn",
      summary: `${legacy} legacy Job record(s) rely on the implicit Delegation kind`,
      verification: "schema-compatible",
      errorCode: "ELEGACYJOB",
      remediation: "Retain the records; a future explicit migration may add the kind field",
    }));
  }
  if (checks.length === 0) {
    checks.push(diagnosticCheck({
      id: "jobs.nonterminal.consistency",
      scope: "jobs",
      status: "pass",
      summary: `${active} nonterminal Job(s) have no locally provable inconsistency`,
      verification: "record",
    }));
  }
  return checks;
}

export function inspectAttachments(attachments, { processStateFn = processState } = {}) {
  if (attachments.length === 0) {
    return [diagnosticCheck({
      id: "attachments.process.consistency",
      scope: "attachments",
      status: "pass",
      summary: "No active TUI attachment records require inspection",
      verification: "record",
      required: false,
    })];
  }
  return attachments.map((record) => {
    const state = processStateFn(record.childPid);
    return diagnosticCheck({
      id: `attachments.${safeLabel(record.name)}.process`,
      scope: "attachments",
      status: state === "alive" ? "pass" : state === "unverified" ? "unknown" : "warn",
      summary:
        state === "alive"
          ? `TUI attachment ${record.name} has a live process`
          : state === "unverified"
            ? `TUI attachment ${record.name} cannot be verified by this caller`
            : `TUI attachment ${record.name} has a missing process`,
      verification: state,
      errorCode: state === "alive" ? null : state === "unverified" ? "EPERM" : "ESRCH",
      required: false,
    });
  });
}

export async function inspectPermissions(
  sessions,
  jobs,
  {
    deep = false,
    scopeSessions = sessions,
    withServer = withAppServer,
    socketPath,
  } = {},
) {
  const checks = [];
  const sessionNames = new Set(sessions.map((record) => record.name));
  let delegationEdges = 0;
  let claudeGrants = 0;
  let invalidReferences = 0;
  const requestedProfiles = new Map();

  for (const record of scopeSessions) {
    for (const sender of record.allowedDelegators || []) {
      delegationEdges += 1;
      if (!SESSION_PATTERN.test(sender) || !sessionNames.has(sender)) invalidReferences += 1;
    }
    for (const grant of record.allowedClaudeRequesters || []) {
      claudeGrants += 1;
      if (grant.permissions) {
        const profiles = requestedProfiles.get(record.cwd) || new Set();
        profiles.add(grant.permissions);
        requestedProfiles.set(record.cwd, profiles);
      }
    }
  }
  for (const job of jobs) {
    if (!job.permissions) continue;
    const target = sessions.find((record) => record.name === job.target);
    if (!target) continue;
    const profiles = requestedProfiles.get(target.cwd) || new Set();
    profiles.add(job.permissions);
    requestedProfiles.set(target.cwd, profiles);
  }

  checks.push(diagnosticCheck({
    id: "permissions.grants.references",
    scope: "permissions",
    status: invalidReferences === 0 ? "pass" : "fail",
    summary:
      invalidReferences === 0
        ? `${delegationEdges} Delegation relationship(s) and ${claudeGrants} Claude grant(s) have valid local references`
        : `${invalidReferences} Delegation relationship reference(s) do not resolve to registered sessions`,
    verification: "registry",
    errorCode: invalidReferences === 0 ? null : "EGRANTTARGET",
  }));

  if (!deep) {
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: "skipped",
      summary: "Permission profile resolution requires --deep",
      verification: "not-requested",
      required: false,
    }));
    return checks;
  }
  if (requestedProfiles.size === 0) {
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: "pass",
      summary: "No stored permission profile references require resolution",
      verification: "registry",
    }));
    return checks;
  }

  let missing = 0;
  let blocked = 0;
  try {
    await withServer(async (client) => {
      for (const [cwd, requested] of requestedProfiles) {
        const result = await client.request("permissionProfile/list", { cwd });
        const available = new Map((result.data || []).map((profile) => [profile.id, profile]));
        for (const profileId of requested) {
          const profile = available.get(profileId);
          if (!profile) missing += 1;
          else if (!profile.allowed) blocked += 1;
        }
      }
    }, { socketPath });
    const invalid = missing + blocked;
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: invalid === 0 ? "pass" : "fail",
      summary:
        invalid === 0
          ? "All stored permission profile references are available and allowed"
          : `${missing} stored profile reference(s) are missing and ${blocked} are blocked`,
      verification: "app-server",
      errorCode: invalid === 0 ? null : "EPERMISSIONPROFILE",
      remediation: invalid === 0 ? null : "Review the stored grant or Job permission profile; Doctor will not change it",
    }));
  } catch (error) {
    checks.push(diagnosticCheck({
      id: "permissions.profiles.resolve",
      scope: "permissions",
      status: ["EPERM", "EACCES", "ETIMEDOUT"].includes(error?.code) ? "unknown" : "fail",
      summary: "Stored permission profile references could not be resolved",
      verification: ["EPERM", "EACCES"].includes(error?.code) ? "sandbox-denied" : "unavailable",
      errorCode: errorCode(error),
      remediation: "Run doctor --deep from an allowed host context",
    }));
  }
  return checks;
}

function readManagedPid(pidPath) {
  const evidence = secureMetadata(pidPath, "file");
  if (evidence.status !== "secure") return { pid: null, evidence };
  if (evidence.metadata.size > 128) {
    return { pid: null, evidence: { status: "invalid" } };
  }
  try {
    const raw = readFileSync(pidPath, "utf8").trim();
    const pid = Number(raw);
    return {
      pid: Number.isSafeInteger(pid) && pid > 1 ? pid : null,
      evidence: Number.isSafeInteger(pid) && pid > 1 ? evidence : { status: "invalid" },
    };
  } catch (error) {
    return { pid: null, evidence: { status: "unavailable", errorCode: errorCode(error) } };
  }
}

export async function inspectAppServer({
  pidPath,
  socketPath,
  deep = false,
  processStateFn = processState,
  processIdentityFn = processIdentity,
  probe = probeAppServerSocket,
} = {}) {
  const checks = [];
  const managed = readManagedPid(pidPath);
  checks.push(metadataFinding("app-server.pid.record", "app-server", "App Server PID record", managed.evidence, { required: false }));
  const socketEvidence = secureMetadata(socketPath, "socket");
  checks.push(metadataFinding("app-server.socket.metadata", "app-server", "App Server socket", socketEvidence));
  const processEvidence = processStateFn(managed.pid);
  const identity = managed.pid
    ? processIdentityFn(managed.pid, ["app-server", socketPath]).state
    : "unavailable";
  const probeResult = deep
    ? await probe(socketPath)
    : failedProbe(Object.assign(new Error("deep handshake not requested"), { code: "ESKIPPED" }));
  const evidence = deep
    ? serviceEvidence({
        process: processEvidence,
        identity,
        socketProbe: probeResult,
        socketPresent: socketEvidence.status === "secure",
      })
    : null;
  let status;
  let verification;
  let summary;
  let code = null;
  if (deep) {
    status = evidence.status === "running" ? "pass" : evidence.status === "stopped" || evidence.status === "stale" || evidence.status === "mismatched" ? "fail" : "unknown";
    verification = probeResult.state === "denied" ? "sandbox-denied" : probeResult.state;
    summary =
      evidence.status === "running"
        ? "App Server identity handshake succeeded"
        : evidence.status === "unreachable"
          ? "App Server socket exists but this caller cannot connect"
          : `App Server evidence reports ${evidence.status}`;
    code = probeResult.errorCode;
  } else if (socketEvidence.status === "secure" && processEvidence === "alive" && identity === "matched") {
    status = "pass";
    verification = "identity";
    summary = "App Server has matching passive process and socket evidence";
  } else if (socketEvidence.status === "missing" && processEvidence === "missing") {
    status = "fail";
    verification = "missing";
    summary = "App Server process and socket are absent";
    code = "ENOENT";
  } else {
    status = "unknown";
    verification = processEvidence === "unverified" ? "sandbox-denied" : "passive";
    summary = "App Server passive evidence is incomplete; use --deep for a handshake";
    code = processEvidence === "unverified" ? "EPERM" : null;
  }
  checks.push(diagnosticCheck({
    id: deep ? "app-server.socket.connect" : "app-server.passive.evidence",
    scope: "app-server",
    status,
    summary,
    verification,
    errorCode: code,
    remediation: status === "unknown" ? "Run doctor --deep from an allowed host context" : null,
  }));
  return checks;
}

export async function inspectBridges(
  bridges,
  sessions,
  {
    deep = false,
    processStateFn = processState,
    processIdentityFn = processIdentity,
    probe = probeClaudeBridge,
    workerFragment = "claude-bridge-worker.js",
    currentRevision = CLAUDE_BRIDGE_IMPLEMENTATION_REVISION,
  } = {},
) {
  if (bridges.length === 0) {
    return [diagnosticCheck({
      id: "bridges.availability",
      scope: "bridges",
      status: "skipped",
      summary: "No Claude bridge records are registered",
      verification: "registry",
      required: false,
    })];
  }
  const sessionsByName = new Map(sessions.map((record) => [record.name, record]));
  const checks = [];
  for (const record of bridges) {
    const label = safeLabel(record.target);
    const recordedRevision = record.implementationRevision;
    checks.push(diagnosticCheck({
      id: `bridges.${label}.implementation`,
      scope: "bridges",
      status:
        recordedRevision === currentRevision
          ? "pass"
          : "warn",
      summary:
        recordedRevision === undefined
          ? `Claude bridge ${record.target} does not identify its running implementation revision`
          : recordedRevision === currentRevision
            ? `Claude bridge ${record.target} runs the current implementation revision`
            : `Claude bridge ${record.target} runs a different implementation revision`,
      verification:
        recordedRevision === undefined
          ? "record-missing"
          : "record",
      errorCode:
        recordedRevision === currentRevision
          ? null
          : recordedRevision === undefined
            ? "EBRIDGEVERSIONUNKNOWN"
            : "EBRIDGESTALECODE",
      remediation:
        recordedRevision === currentRevision
          ? null
          : "Restart this bridge from an allowed host context, then rerun doctor; Doctor will not restart it",
      required: false,
    }));
    const session = sessionsByName.get(record.target);
    if (!session || session.threadId !== record.targetThreadId) {
      checks.push(diagnosticCheck({
        id: `bridges.${label}.target`,
        scope: "bridges",
        status: "fail",
        summary: `Claude bridge ${record.target} does not match its registered Codex target`,
        verification: "registry",
        errorCode: "EBRIDGETARGET",
      }));
      continue;
    }
    const processEvidence = processStateFn(record.pid);
    const identity = processIdentityFn(record.pid, [workerFragment, record.target]).state;
    if (!deep) {
      const socketEvidence = secureMetadata(record.socketPath, "socket");
      const ok = processEvidence === "alive" && identity === "matched" && socketEvidence.status === "secure";
      checks.push(diagnosticCheck({
        id: `bridges.${label}.passive`,
        scope: "bridges",
        status: ok ? "pass" : processEvidence === "missing" || identity === "mismatched" ? "fail" : "unknown",
        summary: ok ? `Claude bridge ${record.target} has matching passive evidence` : `Claude bridge ${record.target} passive evidence is incomplete`,
        verification: ok ? "identity" : processEvidence === "unverified" ? "sandbox-denied" : "passive",
        errorCode: processEvidence === "unverified" ? "EPERM" : null,
      }));
      continue;
    }
    const socketProbe = await probe(record);
    const evidence = serviceEvidence({
      process: processEvidence,
      identity,
      socketProbe,
      socketPresent: existsSync(record.socketPath),
    });
    checks.push(diagnosticCheck({
      id: `bridges.${label}.handshake`,
      scope: "bridges",
      status: evidence.status === "running" ? "pass" : ["stopped", "stale", "mismatched"].includes(evidence.status) ? "fail" : "unknown",
      summary: evidence.status === "running" ? `Claude bridge ${record.target} identity handshake succeeded` : `Claude bridge ${record.target} reports ${evidence.status}`,
      verification: socketProbe.state === "denied" ? "sandbox-denied" : socketProbe.state,
      errorCode: socketProbe.errorCode,
      remediation: evidence.status === "unreachable" ? "Run doctor from an allowed host context" : null,
    }));
  }
  return checks;
}

export async function inspectRelay({
  recordPath,
  deep = false,
  processStateFn = processState,
  processIdentityFn = processIdentity,
  request = hostRelayRequest,
  workerFragment = "host-relay-worker.js",
} = {}) {
  const evidence = secureMetadata(recordPath, "file");
  if (evidence.status === "missing") {
    return [metadataFinding("relay.record.metadata", "relay", "Host relay record", evidence, { required: false })];
  }
  const checks = [metadataFinding("relay.record.metadata", "relay", "Host relay record", evidence)];
  if (evidence.status !== "secure" || evidence.metadata.size > MAX_RECORD_BYTES) return checks;
  let record;
  try {
    record = JSON.parse(readFileSync(recordPath, "utf8"));
  } catch {
    checks.push(diagnosticCheck({ id: "relay.record.schema", scope: "relay", status: "fail", summary: "Host relay record is invalid bounded JSON", verification: "parse", errorCode: "EJSON" }));
    return checks;
  }
  const valid = Boolean(record?.version === 1 && Number.isSafeInteger(record.pid) && record.pid > 1 && Number.isInteger(record.port) && record.port > 0 && record.port <= 65_535 && typeof record.token === "string" && record.token.length >= 16 && Number.isSafeInteger(record.startedAt));
  if (!valid) {
    checks.push(diagnosticCheck({ id: "relay.record.schema", scope: "relay", status: "fail", summary: "Host relay record schema is invalid", verification: "schema", errorCode: "ERELAYSCHEMA" }));
    return checks;
  }
  const state = processStateFn(record.pid);
  const identity = processIdentityFn(record.pid, [workerFragment, String(record.port)]).state;
  if (!deep) {
    const ok = state === "alive" && identity === "matched";
    checks.push(diagnosticCheck({
      id: "relay.passive.evidence",
      scope: "relay",
      status: ok ? "pass" : state === "missing" || identity === "mismatched" ? "fail" : "unknown",
      summary: ok ? "Host relay has matching passive process evidence" : "Host relay passive evidence is incomplete",
      verification: ok ? "identity" : state === "unverified" ? "sandbox-denied" : "passive",
      errorCode: state === "unverified" ? "EPERM" : null,
    }));
    return checks;
  }
  try {
    const health = await request("/health", { record });
    const matched = health.pid === record.pid && health.port === record.port && health.startedAt === record.startedAt;
    checks.push(diagnosticCheck({
      id: "relay.health.connect",
      scope: "relay",
      status: matched ? "pass" : "fail",
      summary: matched ? "Host relay identity handshake succeeded" : "Host relay identity handshake mismatched its record",
      verification: matched ? "handshake" : "mismatched",
      errorCode: matched ? null : "EIDENTITY",
    }));
  } catch (error) {
    const denied = ["EPERM", "EACCES", "ETIMEDOUT"].includes(error?.code);
    checks.push(diagnosticCheck({
      id: "relay.health.connect",
      scope: "relay",
      status: denied && state !== "missing" ? "unknown" : "fail",
      summary: denied ? "Host relay exists but this caller cannot connect" : "Host relay health handshake failed",
      verification: denied ? "sandbox-denied" : "unavailable",
      errorCode: errorCode(error),
      remediation: denied ? "Run doctor from an allowed host context" : null,
    }));
  }
  return checks;
}

export async function inspectRegisteredThreads(
  sessions,
  { deep = false, withServer = withAppServer, socketPath } = {},
) {
  if (!deep) {
    return [diagnosticCheck({
      id: "sessions.thread.resolve",
      scope: "sessions",
      status: "skipped",
      summary: "Thread resolution requires --deep",
      verification: "not-requested",
      required: false,
    })];
  }
  if (sessions.length === 0) {
    return [diagnosticCheck({
      id: "sessions.thread.resolve",
      scope: "sessions",
      status: "skipped",
      summary: "No registered Codex threads require resolution",
      verification: "registry",
      required: false,
    })];
  }
  const checks = [];
  try {
    await withServer(async (client) => {
      for (const record of sessions) {
        try {
          const thread = await readThreadMetadata(client, record.threadId);
          checks.push(diagnosticCheck({
            id: `sessions.${safeLabel(record.name)}.thread`,
            scope: "sessions",
            status: thread?.id === record.threadId ? "pass" : "fail",
            summary: thread?.id === record.threadId ? `Session ${record.name} resolves through metadata-only thread/read` : `Session ${record.name} resolved to a mismatched thread`,
            verification: "app-server",
            errorCode: thread?.id === record.threadId ? null : "EIDENTITY",
          }));
        } catch (error) {
          checks.push(diagnosticCheck({
            id: `sessions.${safeLabel(record.name)}.thread`,
            scope: "sessions",
            status: "fail",
            summary: `Session ${record.name} does not resolve through metadata-only thread/read`,
            verification: "app-server",
            errorCode: errorCode(error, "ETHREADREAD"),
          }));
        }
      }
    }, { socketPath });
  } catch (error) {
    checks.push(diagnosticCheck({
      id: "sessions.thread.connect",
      scope: "sessions",
      status: ["EPERM", "EACCES", "ETIMEDOUT"].includes(error?.code) ? "unknown" : "fail",
      summary: "Registered threads could not be inspected through the App Server",
      verification: ["EPERM", "EACCES"].includes(error?.code) ? "sandbox-denied" : "unavailable",
      errorCode: errorCode(error),
      remediation: "Run doctor --deep from an allowed host context",
    }));
  }
  return checks;
}
