import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

test("relay and auto approvals preserve decisions and audit records", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-approval-state-"));
  process.env.CXMSG_STATE_DIR = stateDir;
  try {
    const { createJob, readJob, updateJob } = await import("../src/jobs.js");
    const { createApprovalHandler, decideApproval } = await import("../src/approvals.js");
    const created = createJob({
      jobId: "42345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      threadId: "execution-thread",
      task: "run command",
      approval: "relay",
      approvalTimeoutSeconds: 5,
    });
    updateJob(created, { status: "running" });
    const pendingResponse = createApprovalHandler(created.jobId)({
      method: "item/commandExecution/requestApproval",
      params: {
        itemId: "command-1",
        command: "npm test",
        cwd: "/project",
      },
    });

    let job;
    for (let attempt = 0; attempt < 20; attempt += 1) {
      job = readJob(created.jobId);
      if (job.status === "awaiting_approval") break;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(job.status, "awaiting_approval");
    assert.equal(job.approvals[0].request.command, "npm test");

    await decideApproval(created.jobId, job.approvals[0].approvalId, "approve");
    assert.deepEqual(await pendingResponse, { decision: "accept" });
    job = readJob(created.jobId);
    assert.equal(job.status, "running");
    assert.equal(job.approvals[0].status, "approved");

    const concurrent = createJob({
      jobId: "62345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      threadId: "execution-thread",
      task: "run two commands",
      approval: "relay",
      approvalTimeoutSeconds: 5,
    });
    updateJob(concurrent, { status: "running" });
    const handler = createApprovalHandler(concurrent.jobId);
    const firstResponse = handler({
      method: "item/commandExecution/requestApproval",
      params: { itemId: "command-2", command: "npm test", cwd: "/project" },
    });
    const secondResponse = handler({
      method: "item/fileChange/requestApproval",
      params: { itemId: "patch-1", reason: "apply patch" },
    });
    for (let attempt = 0; attempt < 20; attempt += 1) {
      job = readJob(concurrent.jobId);
      if (job.approvals.length === 2) break;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(job.approvals.length, 2);
    await decideApproval(concurrent.jobId, job.approvals[0].approvalId, "approve");
    assert.deepEqual(await firstResponse, { decision: "accept" });
    job = readJob(concurrent.jobId);
    assert.equal(job.status, "awaiting_approval");
    await decideApproval(concurrent.jobId, job.approvals[1].approvalId, "deny");
    assert.deepEqual(await secondResponse, { decision: "decline" });
    assert.equal(readJob(concurrent.jobId).status, "running");

    const automatic = createJob({
      jobId: "52345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      threadId: "execution-thread",
      task: "request permissions",
      approval: "auto",
    });
    updateJob(automatic, { status: "running" });
    const requested = { network: { enabled: true } };
    const response = await createApprovalHandler(automatic.jobId)({
      method: "item/permissions/requestApproval",
      params: { itemId: "permission-1", permissions: requested, cwd: "/project" },
    });
    assert.deepEqual(response, { permissions: requested, scope: "turn" });
    const automaticJob = readJob(automatic.jobId);
    assert.equal(automaticJob.approvals[0].automatic, true);
    assert.equal(automaticJob.approvals[0].status, "approved");
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
  }
});
