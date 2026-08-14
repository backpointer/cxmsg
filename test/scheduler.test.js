import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
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
  worker: "42345678-2234-4234-8234-123456789abc",
  turn: "52345678-2234-4234-8234-123456789abc",
  expired: "62345678-2234-4234-8234-123456789abc",
};

async function scheduledRecord(messageId, body) {
  const route = {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: messageId,
    payload_type: "coordination",
    wake_policy: "when-idle",
    expiry: "2026-08-15T01:00:00.000Z",
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
      wakePolicy: "when-idle",
      now: "2026-08-15T00:00:00.000Z",
    })
  ).record;
}

test("when-idle stays queued while busy and starts exactly once after idle", async () => {
  const record = await scheduledRecord(ids.message, "scheduled coordination");
  let active = true;
  let starts = 0;
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
      await options.beforeStart();
      return { delivery: "started", turnId: ids.turn };
    },
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
  const started = await scheduler.dispatchScheduledDelivery(
    ledger.readDeliveryLedger(ids.message),
    {},
    ids.worker,
    dependencies,
  );
  assert.equal(started.state, "turn_started");
  assert.equal(starts, 1);
  assert.equal(ledger.readDeliveryLedger(ids.message).delivery.state, "turn_started");
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

test("an expired when-idle Delivery becomes terminal without target access", async () => {
  const record = await scheduledRecord(ids.expired, "expired coordination");
  let targetReads = 0;
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
    },
  );
  assert.equal(outcome.state, "expired");
  assert.equal(targetReads, 0);
  assert.equal(ledger.readDeliveryLedger(ids.expired).delivery.state, "expired");
});
