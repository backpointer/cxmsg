import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

test("job refresh preserves approval records written while thread state is read", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-race-"));
  process.env.CXMSG_STATE_DIR = stateDir;
  try {
    const {
      createJob,
      failJobIfWorkerExited,
      mutateJob,
      readJob,
      refreshJob,
      updateJob,
      withJobLock,
    } = await import("../src/jobs.js");
    const { withFileLock } = await import("../src/file-lock.js");
    const created = createJob({
      jobId: "72345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      targetThreadId: "source-thread",
      threadId: "execution-thread",
      task: "inspect",
    });
    const running = await updateJob(created, {
      status: "running",
      turnId: "turn-1",
      turnStartedAt: new Date(Date.now() - 20_000).toISOString(),
    });

    let releaseRead;
    let markReadStarted;
    const readStarted = new Promise((resolve) => {
      markReadStarted = resolve;
    });
    const readResult = new Promise((resolve) => {
      releaseRead = resolve;
    });
    const refreshing = refreshJob(
      {
        request: async () => {
          markReadStarted();
          return readResult;
        },
      },
      running,
    );

    await readStarted;
    await mutateJob(running.jobId, (current) => ({
      ...current,
      status: "awaiting_approval",
      approvals: [
        {
          approvalId: "82345678-1234-1234-1234-123456789abc",
          status: "pending",
        },
      ],
    }));
    releaseRead({
      data: [{ id: "turn-1", status: "completed", items: [] }],
      nextCursor: null,
    });

    const refreshed = await refreshing;
    assert.equal(refreshed.status, "completed");
    assert.equal(refreshed.approvals.length, 1);
    assert.equal(readJob(running.jobId).approvals[0].status, "pending");

    const approvalRace = createJob({
      jobId: "b2345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      targetThreadId: "source-thread",
      threadId: "execution-thread",
      task: "request approval",
    });
    const approvalRunning = await updateJob(approvalRace, {
      status: "running",
      turnId: "turn-approval",
      turnStartedAt: new Date(Date.now() - 20_000).toISOString(),
    });
    let releaseActiveRead;
    let markActiveReadStarted;
    const activeReadStarted = new Promise((resolve) => {
      markActiveReadStarted = resolve;
    });
    const activeReadResult = new Promise((resolve) => {
      releaseActiveRead = resolve;
    });
    const activeRefresh = refreshJob(
      {
        request: async () => {
          markActiveReadStarted();
          return activeReadResult;
        },
      },
      approvalRunning,
    );
    await activeReadStarted;
    await mutateJob(approvalRunning.jobId, (current) => ({
      ...current,
      status: "awaiting_approval",
      approvals: [{ approvalId: "approval-1", status: "pending" }],
    }));
    releaseActiveRead({
      data: [{ id: "turn-approval", status: "inProgress", items: [] }],
      nextCursor: null,
    });
    assert.equal((await activeRefresh).status, "awaiting_approval");
    assert.equal(readJob(approvalRunning.jobId).status, "awaiting_approval");

    let releaseFirst;
    let markFirstEntered;
    const firstEntered = new Promise((resolve) => {
      markFirstEntered = resolve;
    });
    const firstGate = new Promise((resolve) => {
      releaseFirst = resolve;
    });
    const order = [];
    const first = withJobLock(running.jobId, async () => {
      order.push("first-enter");
      markFirstEntered();
      await firstGate;
      order.push("first-exit");
    });
    await firstEntered;
    const second = withJobLock(running.jobId, async () => {
      order.push("second-enter");
    });
    await new Promise((resolve) => setTimeout(resolve, 75));
    assert.deepEqual(order, ["first-enter"]);
    releaseFirst();
    await Promise.all([first, second]);
    assert.deepEqual(order, ["first-enter", "first-exit", "second-enter"]);

    const legacyLock = path.join(stateDir, "legacy.lock");
    await fs.writeFile(legacyLock, "2147483647\n", { mode: 0o600 });
    const expired = new Date(Date.now() - 60_000);
    await fs.utimes(legacyLock, expired, expired);
    let legacyAcquired = false;
    await withFileLock(
      legacyLock,
      async () => {
        legacyAcquired = true;
      },
      { timeoutMs: 1_000, leaseMs: 10 },
    );
    assert.equal(legacyAcquired, true);

    const orphan = createJob({
      jobId: "92345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      targetThreadId: "source-thread",
      threadId: "execution-thread",
      task: "orphaned work",
    });
    const orphanRunning = await updateJob(orphan, {
      status: "awaiting_approval",
      workerPid: 999_999,
    });
    const failed = await failJobIfWorkerExited(orphanRunning, {
      processStateFn: () => "missing",
    });
    assert.equal(failed.status, "failed");
    assert.equal(failed.failureCode, "worker_exited");

    const uncertain = createJob({
      jobId: "a2345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      targetThreadId: "source-thread",
      threadId: "execution-thread",
      task: "uncertain worker",
    });
    const uncertainRunning = await updateJob(uncertain, {
      status: "running",
      workerPid: 888_888,
    });
    assert.equal(
      (
        await failJobIfWorkerExited(uncertainRunning, {
          processStateFn: () => "unverified",
        })
      ).status,
      "running",
    );

    const missingWorker = createJob({
      jobId: "c2345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      targetThreadId: "source-thread",
      threadId: null,
      task: "missing worker",
    });
    assert.equal(
      (
        await failJobIfWorkerExited(missingWorker, {
          now: Date.parse(missingWorker.createdAt) + 9_000,
        })
      ).status,
      "dispatching",
    );
    const reconciledMissingWorker = await failJobIfWorkerExited(missingWorker, {
      now: Date.parse(missingWorker.createdAt) + 11_000,
    });
    assert.equal(reconciledMissingWorker.status, "failed");
    assert.equal(reconciledMissingWorker.failureCode, "worker_missing");
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
  }
});
