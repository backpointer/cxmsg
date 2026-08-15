import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-scheduler-"));
process.env.CXMSG_STATE_DIR = stateDir;

const bodies = await import(`../src/message-bodies.js?team-scheduler=${Date.now()}`);
const ledger = await import(`../src/delivery-ledger.js?team-scheduler=${Date.now()}`);
const scheduler = await import(`../src/scheduler.js?team-scheduler=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const ids = {
  message: "11345678-8234-4234-8234-123456789abc",
  sender: "21345678-8234-4234-8234-123456789abc",
  target: "31345678-8234-4234-8234-123456789abc",
  project: "41345678-8234-4234-8234-123456789abc",
  plan: "51345678-8234-4234-8234-123456789abc",
  selection: "61345678-8234-4234-8234-123456789abc",
  worker: "71345678-8234-4234-8234-123456789abc",
  turn: "81345678-8234-4234-8234-123456789abc",
  expiredMessage: "91345678-8234-4234-8234-123456789abc",
  afterTurnMessage: "a1345678-8234-4234-8234-123456789abc",
  afterJobMessage: "b1345678-8234-4234-8234-123456789abc",
  triggerTurn: "c1345678-8234-4234-8234-123456789abc",
  triggerJob: "d1345678-8234-4234-8234-123456789abc",
};
const senderNodeKey = `codex:${ids.sender}`;
const targetNodeKey = `codex:${ids.target}`;
const createdAt = "2026-08-15T00:00:00.000Z";
const expiresAt = "2026-08-15T00:15:00.000Z";

async function scheduledTeamRecord(messageId = ids.message, schedule = {}) {
  const message = "deliver once after the Team recipient becomes idle";
  const body = await bodies.storeMessageBody({
    messageId,
    body: message,
    now: createdAt,
  });
  const route = {
    schema_version: 1,
    kind: "team-cast",
    plan_id: ids.plan,
    selection_id: ids.selection,
    project_id: ids.project,
    wake_policy: "mention-wake",
    expiry: expiresAt,
  };
  await ledger.commitPreparedTeamCastDelivery({
    logicalMessage: {
      messageId,
      from: senderNodeKey,
      senderThreadId: ids.sender,
      senderNodeKey,
      body: {
        messageId,
        bytes: Buffer.byteLength(message, "utf8"),
        sha256: createHash("sha256").update(message).digest("hex"),
        contentRef: body.contentRef,
      },
      route,
      routeFingerprint: createHash("sha256")
        .update(JSON.stringify(route))
        .digest("hex"),
      createdAt,
      teamCast: {
        version: 1,
        planId: ids.plan,
        selectionId: ids.selection,
        projectId: ids.project,
        wakePolicy: "mention-wake",
        recipientNodeKeys: [targetNodeKey],
        recipientSetSha256: createHash("sha256")
          .update(JSON.stringify([targetNodeKey]))
          .digest("hex"),
        expiresAt,
      },
    },
    recipients: [{ nodeKey: targetNodeKey, targetThreadId: ids.target }],
    now: createdAt,
  });
  await ledger.scheduleTeamCastRecipientDelivery(
    messageId,
    targetNodeKey,
    { now: "2026-08-15T00:00:01.000Z", ...schedule },
  );
  await ledger.rebuildDeliveryLedgerIndex();
}

function teamCandidate(record) {
  return {
    ...structuredClone(record),
    delivery: structuredClone(record.teamDeliveries[0]),
    teamRecipientNodeKey: record.teamDeliveries[0].targetNodeKey,
  };
}

test("Team when-idle survives Busy and Scheduler restart boundaries", async () => {
  await scheduledTeamRecord();
  let busy = true;
  let starts = 0;
  const dispatch = (record, client, workerId, options) =>
    scheduler.dispatchScheduledDelivery(record, client, workerId, {
      ...options,
      sessions: () => [{ name: "team-target", threadId: ids.target }],
      targetIdentity: () => ({ state: "current", nodeKey: targetNodeKey }),
      readThread: async () => ({
        id: ids.target,
        status: { type: busy ? "active" : "idle" },
      }),
      deliver: async (_client, _thread, payload, { beforeStart }) => {
        assert.equal(payload.messageId, ids.message);
        await beforeStart();
        starts += 1;
        return { delivery: "started", turnId: ids.turn };
      },
      log: async () => {},
    });
  const now = () => Date.parse("2026-08-15T00:01:00.000Z");

  const busyPass = await scheduler.runSchedulerPass({}, ids.worker, {
    now,
    dispatch,
    dispatchDelegation: async () => assert.fail("no Delegation is scheduled"),
  });
  assert.deepEqual(busyPass.map((outcome) => outcome.state), ["busy"]);
  let stored = await ledger.readDeliveryLedgerIndexed(ids.message);
  assert.equal(stored.teamDeliveries[0].state, "scheduled");
  assert.equal(stored.teamDeliveries[0].attempts.length, 0);

  busy = false;
  const idlePass = await scheduler.runSchedulerPass({}, ids.worker, {
    now,
    dispatch,
    dispatchDelegation: async () => assert.fail("no Delegation is scheduled"),
  });
  assert.deepEqual(idlePass.map((outcome) => outcome.state), ["turn_started"]);
  assert.equal(starts, 1);
  stored = await ledger.readDeliveryLedgerIndexed(ids.message);
  assert.equal(stored.teamDeliveries[0].state, "turn_started");
  assert.equal(stored.teamDeliveries[0].attempts.length, 1);
  assert.equal(stored.teamDeliveries[0].claim, null);
  await ledger.rebuildDeliveryLedgerIndex();

  const repeated = await scheduler.runSchedulerPass({}, ids.worker, {
    now,
    dispatch,
    dispatchDelegation: async () => assert.fail("no Delegation is scheduled"),
  });
  assert.deepEqual(repeated, []);
  assert.equal(starts, 1);
});

test("an expired Team fallback becomes terminal without target access", async () => {
  await scheduledTeamRecord(ids.expiredMessage);
  const dispatch = (record, client, workerId, options) =>
    scheduler.dispatchScheduledDelivery(record, client, workerId, {
      ...options,
      sessions: () => assert.fail("expiry must not resolve a target session"),
      readThread: async () => assert.fail("expiry must not read target state"),
      log: async () => {},
    });
  const outcomes = await scheduler.runSchedulerPass({}, ids.worker, {
    now: () => Date.parse("2026-08-15T00:16:00.000Z"),
    dispatch,
    dispatchDelegation: async () => assert.fail("no Delegation is scheduled"),
  });
  assert.deepEqual(outcomes.map((outcome) => outcome.state), ["expired"]);
  const stored = await ledger.readDeliveryLedgerIndexed(ids.expiredMessage);
  assert.equal(stored.teamDeliveries[0].state, "expired");
  assert.equal(stored.teamDeliveries[0].attempts.length, 0);
  assert.equal(stored.teamDeliveries[0].claim, null);
});

test("Team after-turn keeps the exact recipient trigger across rebuilds", async () => {
  await scheduledTeamRecord(ids.afterTurnMessage, {
    wakePolicy: "after-turn",
    triggerTurnId: ids.triggerTurn,
  });
  const stored = await ledger.readDeliveryLedgerIndexed(ids.afterTurnMessage);
  assert.equal(stored.teamDeliveries[0].wakePolicy, "after-turn");
  assert.equal(stored.teamDeliveries[0].schedule.triggerTurnId, ids.triggerTurn);
  assert.equal(stored.teamDeliveries[0].schedule.triggerJobId, null);

  let status = "inProgress";
  const readiness = () =>
    scheduler.scheduledTriggerReadiness(teamCandidate(stored), {}, {
      findTurn: async (_client, threadId, turnId) => {
        assert.equal(threadId, ids.target);
        assert.equal(turnId, ids.triggerTurn);
        return { id: ids.triggerTurn, status };
      },
    });
  assert.deepEqual(await readiness(), { state: "waiting-trigger" });
  status = "completed";
  assert.deepEqual(await readiness(), { state: "eligible" });

  await ledger.rebuildDeliveryLedgerIndex();
  const rebuilt = await ledger.readDeliveryLedgerIndexed(ids.afterTurnMessage);
  assert.equal(rebuilt.teamDeliveries[0].schedule.triggerTurnId, ids.triggerTurn);
});

test("Team after-job waits for the exact durable Job and rejects trigger changes", async () => {
  await scheduledTeamRecord(ids.afterJobMessage, {
    wakePolicy: "after-job",
    triggerJobId: ids.triggerJob,
  });
  const stored = await ledger.readDeliveryLedgerIndexed(ids.afterJobMessage);
  assert.equal(stored.teamDeliveries[0].wakePolicy, "after-job");
  assert.equal(stored.teamDeliveries[0].schedule.triggerTurnId, null);
  assert.equal(stored.teamDeliveries[0].schedule.triggerJobId, ids.triggerJob);

  let status = "running";
  const readiness = () =>
    scheduler.scheduledTriggerReadiness(teamCandidate(stored), {}, {
      job: (jobId) => {
        assert.equal(jobId, ids.triggerJob);
        return { jobId: ids.triggerJob, status };
      },
      pendingJob: (job) => job.status === "running",
      refreshPendingJob: async (job) => job,
    });
  assert.deepEqual(await readiness(), { state: "waiting-trigger" });
  status = "completed";
  assert.deepEqual(await readiness(), { state: "eligible" });

  const repeated = await ledger.scheduleTeamCastRecipientDelivery(
    ids.afterJobMessage,
    targetNodeKey,
    { wakePolicy: "after-job", triggerJobId: ids.triggerJob },
  );
  assert.equal(repeated.scheduled, false);
  await assert.rejects(
    ledger.scheduleTeamCastRecipientDelivery(
      ids.afterJobMessage,
      targetNodeKey,
      { wakePolicy: "when-idle" },
    ),
    /another outcome/,
  );
});
