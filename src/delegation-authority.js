import {
  findNodeSuccessors,
  listProjects,
  projectContainsPath,
  readNode,
  readProject,
} from "./node-directory.js";
import { readSessionRecord } from "./registry.js";

export const EXECUTION_MODES = new Set(["fork", "fresh", "inline"]);
export const APPROVAL_MODES = new Set(["never", "relay", "auto"]);
export const MIRROR_MODES = new Set(["none", "summary", "full"]);

function authorityError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

export async function availablePermissionProfiles(client, cwd) {
  const result = await client.request("permissionProfile/list", { cwd });
  return result.data || [];
}

export function captureScheduledDelegationTarget(record) {
  if (!record?.threadId) {
    throw authorityError("ETARGETIDENTITY", "scheduled Delegation target is unavailable");
  }
  const node = readNode("codex", record.threadId);
  if (!node) {
    const projects = listProjects().filter((candidate) =>
      projectContainsPath(candidate, record.cwd),
    );
    const guidance = projects.length === 1
      ? `; run: cxmsg directory sync --project ${projects[0].routingId} --codex-only`
      : "; run: cxmsg directory projects --paths, then cxmsg directory sync --project <routing-id> --codex-only";
    throw authorityError(
      "ETARGETNODE",
      `scheduled Delegation requires a synchronized target Node${guidance}`,
    );
  }
  const project = readProject(node.projectId);
  if (!project || !projectContainsPath(project, record.cwd)) {
    throw authorityError(
      "ETARGETPROJECT",
      "scheduled Delegation target Project evidence is unavailable",
    );
  }
  return { targetNodeKey: node.nodeKey, targetProjectId: project.projectId };
}

export async function validateDelegationAuthority(
  job,
  client,
  {
    session = readSessionRecord,
    node = readNode,
    project = readProject,
    containsPath = projectContainsPath,
    successors = findNodeSuccessors,
    permissionProfiles = availablePermissionProfiles,
    now = Date.now(),
  } = {},
) {
  const record = session(job.target);
  if (!record || record.threadId !== job.targetThreadId) {
    throw authorityError(
      "ETARGETIDENTITY",
      "Delegation target identity changed or is unavailable",
    );
  }
  if (!(record.allowedDelegators || []).includes(job.from)) {
    throw authorityError("EGRANTREVOKED", "Delegation grant is absent or revoked");
  }
  if (!EXECUTION_MODES.has(job.execution)) {
    throw authorityError("EEXECUTIONPOLICY", "Delegation execution mode is invalid");
  }
  if (!APPROVAL_MODES.has(job.approval)) {
    throw authorityError("EAPPROVALPOLICY", "Delegation approval policy is invalid");
  }
  if (!MIRROR_MODES.has(job.mirror)) {
    throw authorityError("EMIRRORPOLICY", "Delegation mirror policy is invalid");
  }

  if (job.schedule) {
    if (
      job.schedule.version !== 1 ||
      job.schedule.wakePolicy !== "when-idle" ||
      !Number.isFinite(Date.parse(job.schedule.expiresAt || ""))
    ) {
      throw authorityError("ESCHEDULEPOLICY", "scheduled Delegation policy is invalid");
    }
    if (Date.parse(job.schedule.expiresAt) <= now) {
      throw authorityError("EDELEGATIONEXPIRED", "scheduled Delegation expired");
    }
    const expectedNodeKey = `codex:${record.threadId.toLowerCase()}`;
    const currentNode = node("codex", record.threadId);
    if (
      !currentNode ||
      currentNode.nodeKey !== expectedNodeKey ||
      currentNode.nodeKey !== job.schedule.targetNodeKey
    ) {
      throw authorityError("ETARGETNODE", "scheduled Delegation target Node changed");
    }
    const currentProject = project(currentNode.projectId);
    if (
      !currentProject ||
      currentProject.projectId !== job.schedule.targetProjectId ||
      !containsPath(currentProject, record.cwd)
    ) {
      throw authorityError(
        "ETARGETPROJECT",
        "scheduled Delegation target Project changed",
      );
    }
    let replacements;
    try {
      replacements = successors(currentNode.nodeKey);
    } catch {
      throw authorityError(
        "ESUCCESSORUNAVAILABLE",
        "scheduled Delegation successor evidence is unavailable",
      );
    }
    if (replacements.length > 0) {
      throw authorityError(
        "ETARGETPREDECESSOR",
        "scheduled Delegation target is now a predecessor Node",
      );
    }
  }

  if (job.permissions) {
    const profiles = await permissionProfiles(client, record.cwd);
    const selected = profiles.find((profile) => profile.id === job.permissions);
    if (!selected) {
      throw authorityError(
        "EPERMISSIONPROFILE",
        `unknown permission profile: ${job.permissions}`,
      );
    }
    if (!selected.allowed) {
      throw authorityError(
        "EPERMISSIONBLOCKED",
        `permission profile is blocked: ${job.permissions}`,
      );
    }
  }
  return { record };
}
