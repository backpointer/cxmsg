import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { mutateJob, readJob } from "./jobs.js";

const SUPPORTED_METHODS = new Set([
  "item/commandExecution/requestApproval",
  "item/fileChange/requestApproval",
  "item/permissions/requestApproval",
  "item/tool/requestUserInput",
]);

function optionAnswer(question, approved) {
  const pattern = approved
    ? /^(accept|approve|allow|yes|continue|proceed)/i
    : /^(decline|deny|reject|no|cancel|stop)/i;
  return question.options?.find((option) => pattern.test(option.label))?.label || null;
}

export function approvalResponse(method, params, approved) {
  if (method === "item/commandExecution/requestApproval") {
    return { decision: approved ? "accept" : "decline" };
  }
  if (method === "item/fileChange/requestApproval") {
    return { decision: approved ? "accept" : "decline" };
  }
  if (method === "item/permissions/requestApproval") {
    return { permissions: approved ? params.permissions : {}, scope: "turn" };
  }
  if (method === "item/tool/requestUserInput") {
    const answers = {};
    for (const question of params.questions || []) {
      const answer = optionAnswer(question, approved);
      if (!answer) {
        throw new Error(
          `request ${question.id} requires a typed answer and cannot be ${approved ? "approved" : "denied"} generically`,
        );
      }
      answers[question.id] = { answers: [answer] };
    }
    return { answers };
  }
  throw new Error(`unsupported approval request: ${method}`);
}

function publicRequestParams(method, params) {
  if (method === "item/commandExecution/requestApproval") {
    return {
      reason: params.reason || null,
      command: params.command || null,
      cwd: params.cwd || null,
      networkApprovalContext: params.networkApprovalContext || null,
    };
  }
  if (method === "item/fileChange/requestApproval") {
    return { reason: params.reason || null, grantRoot: params.grantRoot || null };
  }
  if (method === "item/permissions/requestApproval") {
    return {
      reason: params.reason || null,
      cwd: params.cwd || null,
      permissions: params.permissions || {},
    };
  }
  return { questions: params.questions || [] };
}

function approvalJobStatus(current, approvals) {
  if (approvals.some((approval) => approval.status === "pending" && !approval.action)) {
    return "awaiting_approval";
  }
  return ["completed", "failed", "cancelled", "interrupted", "unknown"].includes(
    current.status,
  )
    ? current.status
    : "running";
}

export function createApprovalHandler(jobId) {
  return async ({ method, params = {} }) => {
    if (!SUPPORTED_METHODS.has(method)) {
      throw new Error(`unsupported server request during delegation: ${method}`);
    }
    const initial = readJob(jobId);
    if (!initial) throw new Error(`unknown job: ${jobId}`);
    if (initial.approval === "never") {
      throw new Error("approval requested by a delegation configured with --approval never");
    }
    const approvalId = randomUUID();
    const requestedAt = new Date().toISOString();
    let job = await mutateJob(jobId, (current) => ({
      ...current,
      status: current.approval === "auto" ? current.status : "awaiting_approval",
      approvals: [
        ...(current.approvals || []),
        {
          approvalId,
          method,
          itemId: params.itemId || null,
          request: publicRequestParams(method, params),
          status: current.approval === "auto" ? "approved" : "pending",
          action: current.approval === "auto" ? "approve" : null,
          automatic: current.approval === "auto",
          requestedAt,
          resolvedAt: current.approval === "auto" ? requestedAt : null,
        },
      ],
    }));

    if (job.approval === "auto") return approvalResponse(method, params, true);

    const deadline = Date.now() + (job.approvalTimeoutSeconds || 600) * 1_000;
    while (Date.now() < deadline) {
      job = readJob(jobId);
      const approval = job?.approvals?.find(
        (candidate) => candidate.approvalId === approvalId,
      );
      if (approval?.action) {
        const approved = approval.action === "approve";
        await mutateJob(jobId, (current) => {
          const approvals = current.approvals.map((candidate) =>
            candidate.approvalId === approvalId
              ? {
                  ...candidate,
                  status: approved ? "approved" : "denied",
                  resolvedAt: candidate.resolvedAt || new Date().toISOString(),
                }
              : candidate,
          );
          return {
            ...current,
            status: approvalJobStatus(current, approvals),
            approvals,
          };
        });
        return approvalResponse(method, params, approved);
      }
      await delay(200);
    }

    await mutateJob(jobId, (current) => {
      const approvals = current.approvals.map((candidate) =>
        candidate.approvalId === approvalId
          ? {
              ...candidate,
              status: "timed_out",
              action: "deny",
              resolvedAt: new Date().toISOString(),
            }
          : candidate,
      );
      return {
        ...current,
        status: approvalJobStatus(current, approvals),
        approvals,
      };
    });
    return approvalResponse(method, params, false);
  };
}

export async function decideApproval(jobId, approvalId, action) {
  if (!/^[0-9a-f-]{36}$/i.test(approvalId || "")) {
    throw new Error("approval-id must be a UUID");
  }
  if (!["approve", "deny"].includes(action)) throw new Error("invalid approval action");
  return mutateJob(jobId, (job) => {
    const approval = job.approvals?.find(
      (candidate) => candidate.approvalId === approvalId,
    );
    if (!approval) throw new Error(`unknown approval: ${approvalId}`);
    if (approval.status !== "pending" || approval.action) {
      throw new Error(`approval is already resolved: ${approvalId}`);
    }
    return {
      ...job,
      approvals: job.approvals.map((candidate) =>
        candidate.approvalId === approvalId
          ? { ...candidate, action, decidedAt: new Date().toISOString() }
          : candidate,
      ),
    };
  });
}
