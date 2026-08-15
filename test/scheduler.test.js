import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-scheduler-"));
process.env.CXMSG_STATE_DIR = stateDir;
const ledger = await import(`../src/delivery-ledger.js?scheduler-test=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?scheduler-test=${Date.now()}`);
const scheduler = await import(`../src/scheduler.js?test=${Date.now()}`);
const messaging = await import(`../src/messaging.js?scheduler-test=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const ids = {
  message: "12345678-2234-4234-8234-123456789abc",
  race: "22345678-2234-4234-8234-123456789abc",
  targetThread: "32345678-2234-4234-8234-123456789abc",
  sourceThread: "33345678-2234-4234-8234-123456789abc",
  worker: "42345678-2234-4234-8234-123456789abc",
  turn: "52345678-2234-4234-8234-123456789abc",
  expired: "62345678-2234-4234-8234-123456789abc",
  blocked: "72345678-2234-4234-8234-123456789abc",
  eligible: "82345678-2234-4234-8234-123456789abc",
  triggerTurn: "92345678-2234-4234-8234-123456789abc",
  triggerJob: "a2345678-2234-4234-8234-123456789abc",
  triggerDispatch: "b2345678-2234-4234-8234-123456789abc",
  triggerRegression: "c2345678-2234-4234-8234-123456789abc",
  externalWriter: "d2345678-2234-4234-8234-123456789abc",
  claimLoss: "e2345678-2234-4234-8234-123456789abc",
  predecessor: "f2345678-2234-4234-8234-123456789abc",
  successorRace: "02345678-2234-4234-8234-123456789abc",
};

async function scheduledRecord(messageId, body, wakePolicy = "when-idle", trigger = {}) {
  const route = {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: messageId,
    payload_type: "coordination",
    wake_policy: wakePolicy,
    expiry: "2026-08-15T01:00:00.000Z",
    ...trigger,
  };
  const reference = await bodies.storeMessageBody({ messageId, body });
  return (
    await ledger.commitSingleRecipientDelivery({
      logicalMessage: {
        messageId,
        from: "coordinator",
        body: {
          messageId,
          bytes: Buffer.byteLength(body, "utf8"),
          sha256: createHash("sha256").update(body).digest("hex"),
          contentRef: reference.contentRef,
        },
        route,
        routeFingerprint: createHash("sha256")
          .update(JSON.stringify(route))
          .digest("hex"),
        createdAt: "2026-08-15T00:00:00.000Z",
      },
      target: "auditor",
      targetThreadId: ids.targetThread,
      admissionState: "admitted",
      admissionReason: "binding_match",
      wakePolicy,
      now: "2026-08-15T00:00:00.000Z",
    })
  ).record;
}

test("worker lifecycle writes a versioned heartbeat record and removes it on exit", async () => {
  let observed = null;
  class Client {
    async connect() {
      observed = scheduler.readSchedulerRecord();
    }

    async close() {}
  }
  const record = await scheduler.runSchedulerWorker({ Client, once: true });
  assert.equal(observed.version, 2);
  assert.equal(observed.cxmsgVersion, (await import("../src/version.js")).CXMSG_VERSION);
  assert.equal(
    observed.implementationRevision,
    scheduler.SCHEDULER_IMPLEMENTATION_REVISION,
  );
  assert.equal(observed.pid, process.pid);
  assert.equal(observed.workerId, record.workerId);
  assert.ok(Number.isFinite(Date.parse(observed.heartbeatAt)));
  assert.equal(existsSync(scheduler.SCHEDULER_RECORD_PATH), false);
});

test("Scheduler desired state distinguishes an operator stop from a missing worker", () => {
  scheduler.writeSchedulerIntent("running", {
    now: "2026-08-15T00:00:00.000Z",
  });
  assert.equal(scheduler.readSchedulerIntent().desiredState, "running");
  scheduler.writeSchedulerIntent("stopped", {
    now: "2026-08-15T00:00:01.000Z",
  });
  assert.equal(scheduler.readSchedulerIntent().desiredState, "stopped");
});

test("a lifecycle event wakes the Scheduler before its polling fallback", async () => {
  const signal = scheduler.createSchedulerWakeSignal();
  const waiting = signal.wait(60_000);
  signal.wake();
  assert.equal(await waiting, "event");
});

test("an earlier unready trigger preserves FIFO and blocks its target lane", async () => {
  const blocked = await scheduledRecord(
    ids.blocked,
    "after turn",
    "after-turn",
    { trigger_turn_id: ids.triggerTurn },
  );
  const eligible = await scheduledRecord(ids.eligible, "when idle");
  assert.equal(
    (await scheduler.scheduledTriggerReadiness(blocked, {}, {
      findTurn: async () => ({ id: ids.triggerTurn, status: "inProgress" }),
    })).state,
    "waiting-trigger",
  );
  assert.deepEqual(
    await scheduler.scheduledTriggerReadiness(blocked, {}, {
      findTurn: async () => null,
    }),
    { state: "blocked", errorCode: "ETRIGGERNOTFOUND" },
  );
  assert.deepEqual(
    await scheduler.scheduledTriggerReadiness(blocked, {}, {
      findTurn: async () => ({ id: ids.turn, status: "completed" }),
    }),
    { state: "blocked", errorCode: "ETRIGGERMISMATCH" },
  );
  assert.equal(
    (await scheduler.scheduledTriggerReadiness(blocked, {}, {
      findTurn: async () => ({ id: ids.triggerTurn, status: "completed" }),
    })).state,
    "eligible",
  );

  const dispatched = [];
  await scheduler.runSchedulerPass({}, ids.worker, {
    now: () => Date.parse("2026-08-15T00:00:01.000Z"),
    triggerReadiness: async (record) =>
      record.logicalMessage.messageId === ids.blocked
        ? { state: "blocked", errorCode: "ETRIGGERNOTFOUND" }
        : { state: "eligible" },
    dispatch: async (record) => {
      dispatched.push(record.logicalMessage.messageId);
      return { state: "observed" };
    },
  });
  assert.deepEqual(dispatched, []);

  assert.equal(
    (await scheduler.scheduledTriggerReadiness(
      {
        ...blocked,
        logicalMessage: {
          ...blocked.logicalMessage,
          route: {
            ...blocked.logicalMessage.route,
            wake_policy: "after-job",
            trigger_job_id: ids.triggerJob,
          },
        },
        delivery: { ...blocked.delivery, wakePolicy: "after-job" },
      },
      {},
      { job: () => ({ jobId: ids.triggerJob, status: "failed" }) },
    )).state,
    "eligible",
  );
});

test("lifecycle reconnect catch-up reads one bounded metadata-only page per scheduled target", async () => {
  const calls = [];
  const observations = [];
  const outcomes = await scheduler.reconcileTurnLifecycle({}, {
    readThread: async (_client, threadId) => ({
      id: threadId,
      status: { type: "idle" },
    }),
    listTurns: async (_client, threadId, options) => {
      calls.push({ threadId, options });
      return { data: [], nextCursor: "older" };
    },
    observe: (thread, page) => observations.push({ thread, page }),
  });
  assert.ok(outcomes.length >= 1);
  assert.equal(calls.length, outcomes.length);
  assert.ok(calls.every(({ options }) => options.limit === 8));
  assert.ok(calls.every(({ options }) => options.itemsView === "notLoaded"));
  assert.equal(observations.length, outcomes.length);
});

test("a terminal trigger is rechecked after claim and starts exactly one turn", async () => {
  const record = await scheduledRecord(
    ids.triggerDispatch,
    "triggered dispatch",
    "after-job",
    { trigger_job_id: ids.triggerJob },
  );
  let triggerChecks = 0;
  let starts = 0;
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:02:00.000Z"),
      triggerReadiness: async () => {
        triggerChecks += 1;
        return { state: "eligible" };
      },
      session: () => ({ name: "auditor", threadId: ids.targetThread }),
      readThread: async () => ({ id: ids.targetThread, status: { type: "idle" } }),
      deliver: async (_client, _thread, _payload, options) => {
        starts += 1;
        await options.beforeStart();
        return { delivery: "started", turnId: ids.turn };
      },
      log: async () => {},
    },
  );
  assert.equal(outcome.state, "turn_started");
  assert.equal(triggerChecks, 2);
  assert.equal(starts, 1);
  assert.equal(
    ledger.readDeliveryLedger(ids.triggerDispatch).delivery.state,
    "turn_started",
  );
});

test("a trigger that becomes unverifiable after claim releases without an attempt", async () => {
  const record = await scheduledRecord(
    ids.triggerRegression,
    "trigger regression",
    "after-turn",
    { trigger_turn_id: ids.triggerTurn },
  );
  let triggerChecks = 0;
  let starts = 0;
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:03:00.000Z"),
      triggerReadiness: async () => {
        triggerChecks += 1;
        return triggerChecks === 1
          ? { state: "eligible" }
          : { state: "blocked", errorCode: "ETRIGGERUNAVAILABLE" };
      },
      session: () => ({ name: "auditor", threadId: ids.targetThread }),
      readThread: async () => ({ id: ids.targetThread, status: { type: "idle" } }),
      deliver: async () => {
        starts += 1;
        return { delivery: "started", turnId: ids.turn };
      },
      log: async () => {},
    },
  );
  assert.deepEqual(outcome, {
    state: "blocked",
    errorCode: "ETRIGGERUNAVAILABLE",
    messageId: ids.triggerRegression,
  });
  assert.equal(starts, 0);
  const retained = ledger.readDeliveryLedger(ids.triggerRegression).delivery;
  assert.equal(retained.state, "scheduled");
  assert.equal(retained.claim, null);
  assert.equal(retained.attempts.length, 0);
});

test("when-idle stays queued while busy and starts exactly once after idle", async () => {
  const record = await scheduledRecord(ids.message, "scheduled coordination");
  record.logicalMessage.senderThreadId = ids.sourceThread;
  let active = true;
  let starts = 0;
  const events = [];
  const dependencies = {
    now: () => Date.parse("2026-08-15T00:00:01.000Z"),
    session: () => ({ name: "auditor", threadId: ids.targetThread }),
    readThread: async () => ({
      id: ids.targetThread,
      status: { type: active ? "active" : "idle" },
    }),
    deliver: async (_client, _thread, payload, options) => {
      starts += 1;
      assert.equal(payload.message, "scheduled coordination");
      assert.equal(payload.replyHandle, null);
      assert.equal(payload.legacyReplyMessageId, ids.message);
      await options.beforeStart();
      return { delivery: "started", turnId: ids.turn };
    },
    log: async (event) => events.push(event),
  };

  const busy = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    dependencies,
  );
  assert.equal(busy.state, "busy");
  assert.equal(ledger.readDeliveryLedger(ids.message).delivery.claimCount, 0);

  active = false;
  const legacyRecord = ledger.readDeliveryLedger(ids.message);
  legacyRecord.logicalMessage.senderThreadId = ids.sourceThread;
  const started = await scheduler.dispatchScheduledDelivery(
    legacyRecord,
    {},
    ids.worker,
    dependencies,
  );
  assert.equal(started.state, "turn_started");
  assert.equal(starts, 1);
  assert.equal(ledger.readDeliveryLedger(ids.message).delivery.state, "turn_started");
  assert.deepEqual(
    events.map(({ phase, outcome }) => ({ phase, outcome })),
    [
      { phase: "claim", outcome: "acquired" },
      { phase: "dispatch", outcome: "turn_started" },
    ],
  );
  assert.doesNotMatch(JSON.stringify(events), /scheduled coordination/);
});

test("register-only notLoaded targets stay queued without a resume attempt", async () => {
  const record = await scheduledRecord(ids.externalWriter, "wait for explicit attach");
  let starts = 0;
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:04:00.000Z"),
      session: () => ({
        name: "auditor",
        threadId: ids.targetThread,
        adopted: true,
      }),
      readThread: async () => ({
        id: ids.targetThread,
        status: { type: "notLoaded" },
      }),
      deliver: async () => {
        starts += 1;
        assert.fail("an external writer candidate must not be resumed or started");
      },
    },
  );
  assert.deepEqual(outcome, {
    state: "blocked",
    messageId: ids.externalWriter,
    errorCode: "EEXTERNALWRITERUNVERIFIED",
  });
  assert.equal(starts, 0);
  const retained = ledger.readDeliveryLedger(ids.externalWriter).delivery;
  assert.equal(retained.state, "scheduled");
  assert.equal(retained.claimCount, 0);
  assert.equal(retained.attempts.length, 0);
});

test("a target becoming busy after claim releases it without a dispatch attempt", async () => {
  const record = await scheduledRecord(ids.race, "race coordination");
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:01:00.000Z"),
      session: () => ({ name: "auditor", threadId: ids.targetThread }),
      readThread: async () => ({ id: ids.targetThread, status: { type: "idle" } }),
      deliver: async () => {
        throw new messaging.TargetBusyError();
      },
    },
  );
  assert.equal(outcome.state, "busy");
  const rebuilt = ledger.readDeliveryLedger(ids.race);
  assert.equal(rebuilt.delivery.state, "scheduled");
  assert.equal(rebuilt.delivery.claim, null);
  assert.equal(rebuilt.delivery.claimCount, 1);
  assert.equal(rebuilt.delivery.attempts.length, 0);
});

test("claim loss immediately before start stops the old dispatcher with zero attempts", async () => {
  const record = await scheduledRecord(ids.claimLoss, "claim loss coordination");
  let starts = 0;
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:05:00.000Z"),
      session: () => ({ name: "auditor", threadId: ids.targetThread }),
      readThread: async () => ({ id: ids.targetThread, status: { type: "idle" } }),
      renewClaim: async () => {
        const error = new Error("lease replaced");
        error.code = "ECLAIMLOST";
        throw error;
      },
      deliver: async (_client, _thread, _payload, options) => {
        starts += 1;
        await options.beforeStart();
        assert.fail("a lost claim must stop before the App Server request");
      },
      log: async () => {},
    },
  );
  assert.deepEqual(outcome, {
    state: "claim_lost",
    messageId: ids.claimLoss,
    errorCode: "ECLAIMLOST",
  });
  assert.equal(starts, 1);
  assert.equal(ledger.readDeliveryLedger(ids.claimLoss).delivery.attempts.length, 0);
});

test("a scheduled predecessor target is blocked without following its successor", async () => {
  const record = await scheduledRecord(ids.predecessor, "predecessor coordination");
  const successorThread = "a3345678-2234-4234-8234-123456789abc";
  assert.deepEqual(
    scheduler.scheduledTargetIdentity(record, {
      successors: (nodeKey) => [
        {
          predecessorNodeKey: nodeKey,
          successorNodeKey: `codex:${successorThread}`,
        },
      ],
    }),
    {
      state: "predecessor",
      nodeKey: `codex:${ids.targetThread}`,
      successorNodeKeys: [`codex:${successorThread}`],
    },
  );
  let targetReads = 0;
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:06:00.000Z"),
      triggerReadiness: async () => ({ state: "eligible" }),
      targetIdentity: () => ({ state: "predecessor" }),
      session: () => {
        targetReads += 1;
        return { name: "auditor", threadId: successorThread };
      },
      log: async () => {},
    },
  );
  assert.deepEqual(outcome, {
    state: "blocked",
    messageId: ids.predecessor,
    errorCode: "ETARGETPREDECESSOR",
  });
  assert.equal(targetReads, 0);
  assert.equal(ledger.readDeliveryLedger(ids.predecessor).delivery.attempts.length, 0);
});

test("a successor linked after claim releases the unused claim", async () => {
  const record = await scheduledRecord(ids.successorRace, "successor race");
  let identityChecks = 0;
  let starts = 0;
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T00:07:00.000Z"),
      triggerReadiness: async () => ({ state: "eligible" }),
      targetIdentity: () => ({
        state: identityChecks++ === 0 ? "current" : "predecessor",
      }),
      session: () => ({ name: "auditor", threadId: ids.targetThread }),
      readThread: async () => ({ id: ids.targetThread, status: { type: "idle" } }),
      deliver: async () => {
        starts += 1;
      },
      log: async () => {},
    },
  );
  assert.equal(outcome.errorCode, "ETARGETPREDECESSOR");
  assert.equal(starts, 0);
  const retained = ledger.readDeliveryLedger(ids.successorRace).delivery;
  assert.equal(retained.claim, null);
  assert.equal(retained.attempts.length, 0);
});

test("an expired when-idle Delivery becomes terminal without target access", async () => {
  const record = await scheduledRecord(ids.expired, "expired coordination");
  let targetReads = 0;
  const events = [];
  const outcome = await scheduler.dispatchScheduledDelivery(
    record,
    {},
    ids.worker,
    {
      now: () => Date.parse("2026-08-15T01:00:01.000Z"),
      session: () => {
        targetReads += 1;
        return null;
      },
      log: async (event) => events.push(event),
    },
  );
  assert.equal(outcome.state, "expired");
  assert.equal(targetReads, 0);
  assert.equal(ledger.readDeliveryLedger(ids.expired).delivery.state, "expired");
  assert.equal(events[0].phase, "expiry");
  assert.doesNotMatch(JSON.stringify(events), /expired coordination/);
});
