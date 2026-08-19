import path from "node:path";
import {
  inspectAppServer,
  inspectAttachments,
  inspectBridges,
  inspectConversationState,
  inspectInboundPolicies,
  inspectJobs,
  inspectMessageBodies,
  inspectNodeDirectory,
  inspectPermissions,
  inspectRegisteredThreads,
  inspectRepairRetentionState,
  inspectRepairState,
  inspectRelay,
  inspectRouteState,
  inspectRuntime,
  inspectRuntimeLogs,
  inspectState,
} from "./inspectors.js";
import {
  CXMSG_STATE_DIR,
  PID_PATH,
  socketPath as configuredSocketPath,
} from "./runtime.js";

export const DOCTOR_SCHEMA_VERSION = 2;

export function doctorOverall(checks) {
  if (checks.some((check) => check.required !== false && check.status === "fail")) {
    return "unhealthy";
  }
  if (checks.some((check) => ["warn", "unknown", "fail"].includes(check.status))) {
    return "degraded";
  }
  return "healthy";
}

const CHECK_STATUS_PRIORITY = Object.freeze({
  skipped: 0,
  pass: 1,
  warn: 2,
  unknown: 3,
  fail: 4,
});

export function deduplicateDoctorChecks(checks) {
  const byId = new Map();
  for (const check of checks) {
    const previous = byId.get(check.id);
    if (!previous) {
      byId.set(check.id, check);
      continue;
    }
    const previousPriority = CHECK_STATUS_PRIORITY[previous.status] ?? -1;
    const currentPriority = CHECK_STATUS_PRIORITY[check.status] ?? -1;
    const strongest = currentPriority > previousPriority ? check : previous;
    byId.set(check.id, {
      ...strongest,
      required: previous.required !== false || check.required !== false,
    });
  }
  return [...byId.values()];
}

export async function runDoctor({
  deep = false,
  target = null,
  stateDir = CXMSG_STATE_DIR,
  pidPath = PID_PATH,
  socketPath = configuredSocketPath(),
  relayRecordPath = path.join(stateDir, "host-relay.json"),
  adapters = {},
} = {}) {
  const checks = [];
  checks.push(...(adapters.inspectRuntime || inspectRuntime)());
  checks.push(
    ...(adapters.inspectRuntimeLogs || inspectRuntimeLogs)({ stateDir }),
  );
  const state = (adapters.inspectState || inspectState)({ stateDir, target });
  checks.push(...state.checks);
  if (!target) {
    checks.push(
      ...(adapters.inspectMessageBodies || inspectMessageBodies)({ stateDir }),
    );
    checks.push(
      ...(adapters.inspectNodeDirectory || inspectNodeDirectory)({
        stateDir,
        sessions: state.allSessions || state.sessions,
        jobs: state.jobs,
      }),
    );
    checks.push(
      ...(adapters.inspectRouteState || inspectRouteState)({
        stateDir,
        sessions: state.allSessions || state.sessions,
      }),
    );
    checks.push(
      ...(adapters.inspectInboundPolicies || inspectInboundPolicies)({ stateDir }),
    );
    checks.push(
      ...(adapters.inspectConversationState || inspectConversationState)({
        stateDir,
        jobs: state.jobs,
      }),
    );
    checks.push(...(adapters.inspectRepairState || inspectRepairState)({ stateDir }));
    checks.push(
      ...(adapters.inspectRepairRetentionState || inspectRepairRetentionState)({
        stateDir,
      }),
    );
  } else {
    checks.push(
      ...(adapters.inspectRouteState || inspectRouteState)({
        stateDir,
        sessions: state.allSessions || state.sessions,
        target,
      }),
    );
    checks.push({
      id: "doctor.target.scope",
      scope: "doctor",
      status: "pass",
      summary: "Target mode excludes unrelated global historical state",
      verification: "target-filter",
      repairable: false,
      required: false,
    });
  }
  checks.push(...(adapters.inspectJobs || inspectJobs)(state.jobs));
  checks.push(...(adapters.inspectAttachments || inspectAttachments)(state.attachments));
  checks.push(...await (adapters.inspectPermissions || inspectPermissions)(
    state.allSessions || state.sessions,
    state.jobs,
    { deep, socketPath, scopeSessions: state.sessions },
  ));
  checks.push(...await (adapters.inspectAppServer || inspectAppServer)({
    pidPath,
    socketPath,
    deep,
  }));
  checks.push(...await (adapters.inspectBridges || inspectBridges)(
    state.bridges,
    state.sessions,
    { deep },
  ));
  checks.push(...await (adapters.inspectRelay || inspectRelay)({
    recordPath: relayRecordPath,
    deep,
  }));
  checks.push(...await (adapters.inspectRegisteredThreads || inspectRegisteredThreads)(
    state.sessions,
    { deep, socketPath },
  ));
  const scopedChecks = target ? deduplicateDoctorChecks(checks) : checks;
  const operationalChecks = scopedChecks.filter((check) => check.historical !== true);
  const historicalChecks = scopedChecks.filter((check) => check.historical === true);
  return {
    schemaVersion: DOCTOR_SCHEMA_VERSION,
    overall: doctorOverall(scopedChecks),
    operationalOverall: doctorOverall(operationalChecks),
    historicalOverall:
      historicalChecks.length > 0 ? doctorOverall(historicalChecks) : "healthy",
    deep,
    target,
    checks: scopedChecks,
  };
}

export function renderDoctorText(report) {
  const operational = report.operationalOverall || report.overall;
  const lines = [
    `cxmsg doctor: ${operational} (${report.checks.length} checks${report.deep ? ", deep" : ""}; historical=${report.historicalOverall || report.overall})`,
  ];
  for (const check of report.checks) {
    const details = [
      check.verification ? `verification=${check.verification}` : null,
      check.errorCode ? `error=${check.errorCode}` : null,
      Number.isSafeInteger(check.observedBytes)
        ? `observed_bytes=${check.observedBytes}`
        : null,
      Number.isSafeInteger(check.limitBytes)
        ? `limit_bytes=${check.limitBytes}`
        : null,
    ].filter(Boolean);
    lines.push(`${check.status.toUpperCase()}\t${check.id}\t${check.summary}${details.length ? `\t${details.join(" ")}` : ""}`);
    if (check.remediation) lines.push(`  remediation: ${check.remediation}`);
  }
  return `${lines.join("\n")}\n`;
}

export function doctorExitCode(report) {
  return (report.operationalOverall || report.overall) === "healthy" ? 0 : 1;
}
