import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-scheduled-delegation-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-scheduled-project-"));
const foreignRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-scheduled-foreign-"));
process.env.CXMSG_STATE_DIR = stateDir;

const directory = await import(`../src/node-directory.js?scheduled=${Date.now()}`);
const registry = await import(`../src/registry.js?scheduled=${Date.now()}`);
const jobs = await import(`../src/jobs.js?scheduled=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?scheduled=${Date.now()}`);
const authority = await import(`../src/delegation-authority.js?scheduled=${Date.now()}`);
const scheduler = await import(`../src/scheduler.js?scheduled=${Date.now()}`);

const ids = {
  project: "12345678-3234-4234-8234-123456789abc",
  target: "22345678-3234-4234-8234-123456789abc",
  successor: "32345678-3234-4234-8234-123456789abc",
  worker: "42345678-3234-4234-8234-123456789abc",
  first: "52345678-3234-4234-8234-123456789abc",
  crash: "62345678-3234-4234-8234-123456789abc",
  dispatch: "72345678-3234-4234-8234-123456789abc",
  revoked: "82345678-3234-4234-8234-123456789abc",
  busy: "92345678-3234-4234-8234-123456789abc",
  expired: "a2345678-3234-4234-8234-123456789abc",
  predecessor: "b2345678-3234-4234-8234-123456789abc",
  permission: "d2345678-3234-4234-8234-123456789abc",
  projectMismatch: "e2345678-3234-4234-8234-123456789abc",
  fresh: "f2345678-3234-4234-8234-123456789abc",
  large: "a3345678-3234-4234-8234-123456789abc",
};

await directory.ensureProject({
  routingId: "scheduled-fixture",
  root: projectRoot,
  projectId: ids.project,
});
await directory.upsertNode({
  runtimeKind: "codex",
  nativeId: ids.target,
  displayName: "worker",
  projectId: ids.project,
});
registry.writeSessionRecord({
  name: "worker",
  threadId: ids.target,
  cwd: projectRoot,
  allowedDelegators: ["coordinator"],
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  rmSync(foreignRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function spec(jobId, overrides = {}) {
  return {
    jobId,
    from: "coordinator",
    target: "worker",
    targetThreadId: ids.target,
    task: "bounded scheduled review",
    permissions: null,
    execution: "fork",
    approval: "never",
    mirror: "none",
    approvalTimeoutSeconds: 600,
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
    targetNodeKey: `codex:${ids.target}`,
    targetProjectId: ids.project,
    ...overrides,
  };
}

const client = {
  request: async (method) => {
    if (method === "permissionProfile/list") {
      return { data: [{ id: ":read-only", allowed: true }] };
    }
    throw new Error(`unexpected request: ${method}`);
  },
};

test("scheduled Delegation enqueue is durable and idempotent by one Job ID", async () => {
  const initialSpec = spec(ids.first);
  const first = await jobs.createScheduledDelegationJob(initialSpec);
  const repeated = await jobs.createScheduledDelegationJob(initialSpec);
  assert.equal(first.created, true);
  assert.equal(repeated.created, false);
  assert.equal(repeated.job.jobId, ids.first);
  assert.equal(repeated.job.status, "scheduled");
  assert.equal(repeated.job.schedule.attemptCount, 0);
  await assert.rejects(
    jobs.createScheduledDelegationJob({ ...initialSpec, task: "changed task" }),
    /idempotency conflict/,
  );
});

test("scheduled Delegation retains large tasks and explicit fresh execution policy", async () => {
  const task = `scheduled large review\n${"retained-line\n".repeat(1_400)}`;
  const taskBody = await bodies.storeMessageBody({
    messageId: ids.large,
    body: task,
  });
  const large = (
    await jobs.createScheduledDelegationJob(
      spec(ids.large, { task: null, taskBody }),
    )
  ).job;
  assert.equal(large.task, null);
  assert.equal(large.taskBody.contentRef, `cxmsg-message:${ids.large}`);

  const fresh = (
    await jobs.createScheduledDelegationJob(
      spec(ids.fresh, { execution: "fresh" }),
    )
  ).job;
  assert.equal(fresh.execution, "fresh");
  await authority.validateDelegationAuthority(fresh, client);
});

test("an expired claim can be recovered but the old dispatcher cannot activate", async () => {
  await jobs.createScheduledDelegationJob(spec(ids.crash));
  const claimed = await jobs.claimScheduledDelegation(ids.crash, {
    workerId: ids.worker,
    leaseMs: 1_000,
    now: new Date(Date.now() - 2_000).toISOString(),
  });
  const replacementWorker = "c2345678-3234-4234-8234-123456789abc";
  const reclaimed = await jobs.claimScheduledDelegation(ids.crash, {
    workerId: replacementWorker,
    leaseMs: 30_000,
  });
  assert.equal(claimed.acquired, true);
  assert.equal(reclaimed.acquired, true);
  assert.equal(
    (await jobs.activateScheduledDelegation(ids.crash, {
      claimId: claimed.claim.claimId,
      workerId: ids.worker,
      workerPid: 1001,
    })).activated,
    false,
  );
  assert.equal(
    (await jobs.activateScheduledDelegation(ids.crash, {
      claimId: reclaimed.claim.claimId,
      workerId: replacementWorker,
      workerPid: 1002,
    })).activated,
    true,
  );
  assert.equal(jobs.readJob(ids.crash).schedule.attemptCount, 1);
});

test("Scheduler starts one claimed worker and preserves the Job correlation ID", async () => {
  const created = (await jobs.createScheduledDelegationJob(spec(ids.dispatch))).job;
  let spawnCount = 0;
  const outcome = await scheduler.dispatchScheduledDelegation(
    created,
    client,
    ids.worker,
    {
      readThread: async () => ({ id: ids.target, status: { type: "idle" } }),
      spawnWorker: async (jobId, claim) => {
        spawnCount += 1;
        const activated = await jobs.activateScheduledDelegation(jobId, {
          claimId: claim.claimId,
          workerId: claim.workerId,
          workerPid: 1003,
        });
        assert.equal(activated.activated, true);
        return 1003;
      },
      log: async () => {
        throw new Error("telemetry unavailable");
      },
    },
  );
  assert.equal(outcome.state, "worker_started");
  assert.equal(outcome.jobId, ids.dispatch);
  assert.equal(spawnCount, 1);
  assert.equal(jobs.readJob(ids.dispatch).status, "queued");
});

test("busy targets retain an unclaimed scheduled Delegation", async () => {
  const created = (await jobs.createScheduledDelegationJob(spec(ids.busy))).job;
  let spawnCount = 0;
  const outcome = await scheduler.dispatchScheduledDelegation(
    created,
    client,
    ids.worker,
    {
      readThread: async () => ({ id: ids.target, status: { type: "active" } }),
      spawnWorker: async () => {
        spawnCount += 1;
      },
      log: async () => {},
    },
  );
  assert.equal(outcome.state, "busy");
  assert.equal(spawnCount, 0);
  assert.equal(jobs.readJob(ids.busy).status, "scheduled");
  assert.equal(jobs.readJob(ids.busy).schedule.claim, null);
});

test("grant revocation and expiry fail without starting a worker", async () => {
  const revoked = (await jobs.createScheduledDelegationJob(spec(ids.revoked))).job;
  registry.writeSessionRecord({
    ...registry.readSessionRecord("worker"),
    allowedDelegators: [],
  });
  let spawnCount = 0;
  const revokedOutcome = await scheduler.dispatchScheduledDelegation(
    revoked,
    client,
    ids.worker,
    {
      readThread: async () => ({ id: ids.target, status: { type: "idle" } }),
      spawnWorker: async () => {
        spawnCount += 1;
      },
      log: async () => {},
    },
  );
  assert.equal(revokedOutcome.errorCode, "EGRANTREVOKED");
  assert.equal(jobs.readJob(ids.revoked).status, "failed");
  assert.equal(spawnCount, 0);
  registry.writeSessionRecord({
    ...registry.readSessionRecord("worker"),
    allowedDelegators: ["coordinator"],
  });

  const expiresAt = new Date(Date.now() + 1_000).toISOString();
  const expired = (
    await jobs.createScheduledDelegationJob(spec(ids.expired, { expiresAt }))
  ).job;
  const expiredOutcome = await scheduler.dispatchScheduledDelegation(
    expired,
    client,
    ids.worker,
    {
      now: () => Date.parse(expiresAt) + 1,
      spawnWorker: async () => {
        spawnCount += 1;
      },
      log: async () => {},
    },
  );
  assert.equal(expiredOutcome.state, "expired");
  assert.equal(jobs.readJob(ids.expired).status, "expired");
  assert.equal(spawnCount, 0);
});

test("permission and Project policy changes fail before worker start", async () => {
  let spawnCount = 0;
  const permission = (
    await jobs.createScheduledDelegationJob(
      spec(ids.permission, { permissions: ":read-only" }),
    )
  ).job;
  const blockedClient = {
    request: async () => ({ data: [{ id: ":read-only", allowed: false }] }),
  };
  const permissionOutcome = await scheduler.dispatchScheduledDelegation(
    permission,
    blockedClient,
    ids.worker,
    {
      spawnWorker: async () => {
        spawnCount += 1;
      },
      log: async () => {},
    },
  );
  assert.equal(permissionOutcome.errorCode, "EPERMISSIONBLOCKED");
  assert.equal(jobs.readJob(ids.permission).status, "failed");

  const projectMismatch = (
    await jobs.createScheduledDelegationJob(spec(ids.projectMismatch))
  ).job;
  registry.writeSessionRecord({
    ...registry.readSessionRecord("worker"),
    cwd: foreignRoot,
  });
  const projectOutcome = await scheduler.dispatchScheduledDelegation(
    projectMismatch,
    client,
    ids.worker,
    {
      spawnWorker: async () => {
        spawnCount += 1;
      },
      log: async () => {},
    },
  );
  assert.equal(projectOutcome.errorCode, "ETARGETPROJECT");
  assert.equal(jobs.readJob(ids.projectMismatch).status, "failed");
  assert.equal(spawnCount, 0);
  registry.writeSessionRecord({
    ...registry.readSessionRecord("worker"),
    cwd: projectRoot,
  });
});

test("a successor link never transfers a scheduled Delegation", async () => {
  const created = (await jobs.createScheduledDelegationJob(spec(ids.predecessor))).job;
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: ids.successor,
    displayName: "successor",
    projectId: ids.project,
  });
  await directory.addSuccessor({
    predecessorNodeKey: `codex:${ids.target}`,
    successorNodeKey: `codex:${ids.successor}`,
  });
  const outcome = await scheduler.dispatchScheduledDelegation(
    created,
    client,
    ids.worker,
    { log: async () => {} },
  );
  assert.equal(outcome.errorCode, "ETARGETPREDECESSOR");
  assert.equal(jobs.readJob(ids.predecessor).status, "failed");
});

test("authority capture binds the exact synchronized Node and Project", async () => {
  const record = registry.readSessionRecord("worker");
  assert.deepEqual(authority.captureScheduledDelegationTarget(record), {
    targetNodeKey: `codex:${ids.target}`,
    targetProjectId: ids.project,
  });
});

test("an unsynchronized target reports the exact safe Directory sync command", () => {
  assert.throws(
    () =>
      authority.captureScheduledDelegationTarget({
        name: "new-reviewer",
        threadId: "f2345678-3234-4234-8234-123456789abc",
        cwd: projectRoot,
      }),
    (error) =>
      error?.code === "ETARGETNODE" &&
      error.message.includes(
        "cxmsg directory sync --project scheduled-fixture --codex-only",
      ),
  );
});
