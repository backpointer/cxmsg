import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  archiveJobs,
  buildJobRetentionPlan,
  inspectJobRetentionState,
  jobRetentionReferences,
  listArchivedJobsForRetention,
  planJobRetention,
  recoverJobRetention,
  restoreJobs,
} from "../src/job-retention.js";
import { planRetention } from "../src/retention.js";

const NOW = Date.parse("2026-08-19T00:00:00.000Z");
const BEFORE = "2026-08-11T00:00:00.000Z";
const COMPLETED = "2026-08-01T00:00:00.000Z";

function job(jobId = randomUUID(), changes = {}) {
  return {
    version: 1,
    jobId,
    kind: "delegation",
    status: "completed",
    task: "bounded task",
    completedAt: COMPLETED,
    createdAt: COMPLETED,
    updatedAt: COMPLETED,
    ...changes,
  };
}

function record(value) {
  const contents = Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
  return {
    job: value,
    bytes: contents.length,
    sha256: createHash("sha256").update(contents).digest("hex"),
  };
}

async function writeJob(root, value) {
  const directory = path.join(root, "jobs");
  await fs.mkdir(directory, { recursive: true, mode: 0o700 });
  await fs.writeFile(
    path.join(directory, `${value.jobId}.json`),
    `${JSON.stringify(value, null, 2)}\n`,
    { mode: 0o600 },
  );
}

const emptyAdapters = {
  ledgerReader: async () => [],
  executionReader: () => [],
  conversationReader: () => [],
  readable: () => {},
};
const directMutation = (callback) => callback();

function runCxmsg(root, args) {
  return spawnSync(process.execPath, [path.resolve("bin/cxmsg.js"), ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: root },
  });
}

test("Job retention protects every durable reference class", () => {
  const ids = Array.from({ length: 4 }, () => randomUUID());
  const jobs = ids.map((id) => job(id));
  jobs.push(job(randomUUID(), {
    correlation: {
      kind: "peer-reply",
      logicalMessageId: randomUUID(),
      replyToMessageId: ids[0],
    },
  }));
  const references = jobRetentionReferences({
    jobs,
    ledger: [
      {
        logicalMessage: { route: { trigger_job_id: ids[1] } },
        deliveries: [{ schedule: { triggerJobId: ids[2] } }],
      },
    ],
    executionThreads: [{ jobId: ids[3] }],
    conversationMessageIds: [ids[2]],
  });
  const plan = planJobRetention(
    { before: BEFORE, records: ids.map((id) => record(jobs.find((item) => item.jobId === id))), references },
    { now: NOW },
  );
  assert.equal(plan.category.eligible.length, 0);
  const reasons = new Map(plan.category.blocked.map((item) => [item.jobId, item.reasons]));
  assert.deepEqual(reasons.get(ids[0]), ["job_reply_correlation"]);
  assert.deepEqual(reasons.get(ids[1]), ["delivery_after_job_trigger"]);
  assert.deepEqual(reasons.get(ids[2]), ["conversation_source", "team_after_job_trigger"]);
  assert.deepEqual(reasons.get(ids[3]), ["execution_thread"]);
  assert.doesNotMatch(JSON.stringify(plan), /bounded task/);
});

test("Job retention blocks ambiguous lifecycle and unresolved Claude responses", () => {
  const records = [
    record(job(randomUUID(), { status: "unknown" })),
    record(job(randomUUID(), { status: "ack_timeout", kind: "claude-delivery" })),
    record(job(randomUUID(), {
      kind: "claude-request",
      reply: { status: "pending" },
    })),
    record(job(randomUUID(), {
      status: "running",
      completedAt: null,
    })),
  ];
  const plan = planJobRetention(
    { before: BEFORE, records },
    { now: NOW },
  );
  assert.equal(plan.category.eligible.length, 0);
  assert.equal(plan.category.blocked.length, 4);
  assert.match(
    JSON.stringify(plan.category.blocked),
    /nonterminal_or_reconcilable|claude_request_reply_unresolved|missing_terminal_timestamp/,
  );
});

test("Job archive is digest-confirmed, recoverable, and exactly restorable", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-"));
  const value = job();
  try {
    await writeJob(root, value);
    const plan = await buildJobRetentionPlan(
      { before: BEFORE },
      { stateDir: root, now: NOW, ...emptyAdapters },
    );
    assert.equal(plan.category.eligible.length, 1);
    const receipt = await archiveJobs(
      { before: BEFORE, expectedPlanDigest: plan.planDigest },
      { stateDir: root, now: NOW, adapters: emptyAdapters, mutation: directMutation },
    );
    assert.equal(receipt.outcome, "archived");
    await assert.rejects(fs.access(path.join(root, "jobs", `${value.jobId}.json`)));
    assert.equal(
      listArchivedJobsForRetention({ stateDir: root, readable: () => {} })[0].jobId,
      value.jobId,
    );
    assert.equal(inspectJobRetentionState({ stateDir: root }).status, "secure");

    const restored = await restoreJobs(
      { archiveId: receipt.archiveId },
      { stateDir: root, mutation: directMutation },
    );
    assert.equal(restored.outcome, "restored");
    const restoredJob = JSON.parse(
      await fs.readFile(path.join(root, "jobs", `${value.jobId}.json`), "utf8"),
    );
    assert.equal(restoredJob.jobId, value.jobId);
    assert.deepEqual(
      listArchivedJobsForRetention({ stateDir: root, readable: () => {} }),
      [],
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job archive rolls forward after a crash without duplicating a Job", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-crash-"));
  const value = job();
  try {
    await writeJob(root, value);
    const plan = await buildJobRetentionPlan(
      { before: BEFORE },
      { stateDir: root, now: NOW, ...emptyAdapters },
    );
    await assert.rejects(
      archiveJobs(
        { before: BEFORE, expectedPlanDigest: plan.planDigest },
        {
          stateDir: root,
          now: NOW,
          adapters: emptyAdapters,
          mutation: directMutation,
          fault(phase) {
            if (phase === "after-job-move") throw new Error("simulated crash");
          },
        },
      ),
      /simulated crash/,
    );
    assert.equal(inspectJobRetentionState({ stateDir: root }).nonterminal, 1);
    const recovered = await recoverJobRetention({ stateDir: root, mutation: directMutation });
    assert.equal(recovered.length, 1);
    assert.equal(recovered[0].outcome, "archived");
    assert.equal(
      listArchivedJobsForRetention({ stateDir: root, readable: () => {} }).length,
      1,
    );
    await assert.rejects(fs.access(path.join(root, "jobs", `${value.jobId}.json`)));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job restore refuses an occupied active identity", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-occupied-"));
  const value = job();
  try {
    await writeJob(root, value);
    const plan = await buildJobRetentionPlan(
      { before: BEFORE },
      { stateDir: root, now: NOW, ...emptyAdapters },
    );
    const receipt = await archiveJobs(
      { before: BEFORE, expectedPlanDigest: plan.planDigest },
      { stateDir: root, now: NOW, adapters: emptyAdapters, mutation: directMutation },
    );
    await writeJob(root, value);
    await assert.rejects(
      restoreJobs(
        { archiveId: receipt.archiveId },
        { stateDir: root, mutation: directMutation },
      ),
      (error) => error?.code === "EJOBARCHIVEPAIR",
    );
    await fs.access(path.join(root, "jobs", `${value.jobId}.json`));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job archive recovery reuses a durable terminal receipt", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-receipt-"));
  const value = job();
  try {
    await writeJob(root, value);
    const plan = await buildJobRetentionPlan(
      { before: BEFORE },
      { stateDir: root, now: NOW, ...emptyAdapters },
    );
    await assert.rejects(
      archiveJobs(
        { before: BEFORE, expectedPlanDigest: plan.planDigest },
        {
          stateDir: root,
          now: NOW,
          adapters: emptyAdapters,
          mutation: directMutation,
          fault(phase) {
            if (phase === "after-archive-receipt") throw new Error("receipt crash");
          },
        },
      ),
      /receipt crash/,
    );
    const transaction = (await fs.readdir(path.join(root, "job-retention", "transactions")))[0];
    const receiptPath = path.join(root, "job-retention", "receipts", `${transaction}.json`);
    const beforeRecovery = JSON.parse(await fs.readFile(receiptPath, "utf8"));
    await recoverJobRetention({ stateDir: root, mutation: directMutation });
    const afterRecovery = JSON.parse(await fs.readFile(receiptPath, "utf8"));
    assert.equal(afterRecovery.committedAt, beforeRecovery.committedAt);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Archived Jobs continue to protect retained Delegation task bodies", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-body-"));
  const jobId = randomUUID();
  const value = job(jobId, {
    completedAt: "2026-04-01T00:00:00.000Z",
    task: null,
    taskBody: {
      messageId: jobId,
      contentRef: `cxmsg-message:${jobId}`,
      bodyBytes: 20_000,
      bodySha256: "1".repeat(64),
    },
  });
  try {
    await writeJob(root, value);
    const plan = await buildJobRetentionPlan(
      { before: BEFORE },
      { stateDir: root, now: NOW, ...emptyAdapters },
    );
    await archiveJobs(
      { before: BEFORE, expectedPlanDigest: plan.planDigest },
      { stateDir: root, now: NOW, adapters: emptyAdapters, mutation: directMutation },
    );
    const archived = listArchivedJobsForRetention({
      stateDir: root,
      readable: () => {},
    });
    const retention = planRetention(
      {
        before: "2026-05-01T00:00:00.000Z",
        scope: "bodies",
        bodies: [{
          contentRef: `cxmsg-message:${jobId}`,
          messageId: jobId,
          bodyBytes: 20_000,
          bodySha256: "1".repeat(64),
          createdAt: "2026-04-01T00:00:00.000Z",
        }],
        jobs: archived,
      },
      { now: NOW },
    );
    assert.equal(retention.categories.bodies.eligible.length, 0);
    assert.match(JSON.stringify(retention.categories.bodies.blocked), /delegation_task/);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job archive rejects stale plans before moving active evidence", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-stale-"));
  const value = job();
  try {
    await writeJob(root, value);
    const plan = await buildJobRetentionPlan(
      { before: BEFORE },
      { stateDir: root, now: NOW, ...emptyAdapters },
    );
    value.updatedAt = "2026-08-02T00:00:00.000Z";
    await writeJob(root, value);
    await assert.rejects(
      archiveJobs(
        { before: BEFORE, expectedPlanDigest: plan.planDigest },
        {
          stateDir: root,
          now: NOW,
          adapters: emptyAdapters,
          mutation: directMutation,
        },
      ),
      (error) => error?.code === "EJOBARCHIVESTALE",
    );
    await fs.access(path.join(root, "jobs", `${value.jobId}.json`));
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job retention CLI requires exact plan and archive confirmations", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-cli-"));
  const value = job();
  try {
    await writeJob(root, value);
    const planned = runCxmsg(root, [
      "jobs",
      "retention",
      "plan",
      "--before",
      BEFORE,
      "--json",
    ]);
    assert.equal(planned.status, 0, planned.stderr);
    const plan = JSON.parse(planned.stdout);
    assert.equal(plan.category.eligible.length, 1);

    const unconfirmed = runCxmsg(root, [
      "jobs",
      "retention",
      "archive",
      "--before",
      BEFORE,
    ]);
    assert.equal(unconfirmed.status, 1);
    assert.match(unconfirmed.stderr, /requires --confirm/);

    const archived = runCxmsg(root, [
      "jobs",
      "retention",
      "archive",
      "--before",
      BEFORE,
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(archived.status, 0, archived.stderr);
    const receipt = JSON.parse(archived.stdout);
    assert.equal(receipt.itemCount, 1);

    const restored = runCxmsg(root, [
      "jobs",
      "retention",
      "restore",
      receipt.archiveId,
      "--confirm",
      receipt.archiveId,
      "--json",
    ]);
    assert.equal(restored.status, 0, restored.stderr);
    assert.equal(JSON.parse(restored.stdout).outcome, "restored");
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("Job archive inspection is read-only for missing and partial state", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-retention-inspect-"));
  try {
    assert.equal(inspectJobRetentionState({ stateDir: root }).status, "missing");
    assert.deepEqual(await fs.readdir(root), []);
    await fs.mkdir(path.join(root, "job-retention"), { mode: 0o700 });
    assert.equal(inspectJobRetentionState({ stateDir: root }).status, "invalid");
    assert.deepEqual(await fs.readdir(path.join(root, "job-retention")), []);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
