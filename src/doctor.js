import path from "node:path";
import {
  inspectAppServer,
  inspectAttachments,
  inspectBridges,
  inspectConversationState,
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
  inspectState,
} from "./inspectors.js";
import {
  CXMSG_STATE_DIR,
  PID_PATH,
  socketPath as configuredSocketPath,
} from "./runtime.js";

export const DOCTOR_SCHEMA_VERSION = 1;

export function doctorOverall(checks) {
  if (checks.some((check) => check.required !== false && check.status === "fail")) {
    return "unhealthy";
  }
  if (checks.some((check) => ["warn", "unknown", "fail"].includes(check.status))) {
    return "degraded";
  }
  return "healthy";
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
  const state = (adapters.inspectState || inspectState)({ stateDir, target });
  checks.push(...state.checks);
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
  return {
    schemaVersion: DOCTOR_SCHEMA_VERSION,
    overall: doctorOverall(checks),
    deep,
    target,
    checks,
  };
}

export function renderDoctorText(report) {
  const lines = [
    `cxmsg doctor: ${report.overall} (${report.checks.length} checks${report.deep ? ", deep" : ""})`,
  ];
  for (const check of report.checks) {
    const details = [
      check.verification ? `verification=${check.verification}` : null,
      check.errorCode ? `error=${check.errorCode}` : null,
    ].filter(Boolean);
    lines.push(`${check.status.toUpperCase()}\t${check.id}\t${check.summary}${details.length ? `\t${details.join(" ")}` : ""}`);
    if (check.remediation) lines.push(`  remediation: ${check.remediation}`);
  }
  return `${lines.join("\n")}\n`;
}

export function doctorExitCode(report) {
  return report.overall === "healthy" ? 0 : 1;
}
