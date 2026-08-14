import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  symlinkSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-delivery-ledger-"));
process.env.CXMSG_STATE_DIR = stateDir;
const ledger = await import(`../src/delivery-ledger.js?test=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const ids = {
  message: "11345678-1234-4234-8234-123456789abc",
  targetThread: "21345678-1234-4234-8234-123456789abc",
  turn: "31345678-1234-4234-8234-123456789abc",
  secondMessage: "41345678-1234-4234-8234-123456789abc",
  scheduledMessage: "51345678-1234-4234-8234-123456789abc",
  worker: "61345678-1234-4234-8234-123456789abc",
  recoveryMessage: "81345678-1234-4234-8234-123456789abc",
  boundedMessage: "a1345678-1234-4234-8234-123456789abc",
  rejectedMessage: "b1345678-1234-4234-8234-123456789abc",
  boundedThread: "c1345678-1234-4234-8234-123456789abc",
  cancelledMessage: "d1345678-1234-4234-8234-123456789abc",
};

function logicalMessage(
  messageId = ids.message,
  body = "private coordination body",
  wakePolicy = "immediate",
) {
  const route = {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: messageId,
    payload_type: "coordination",
    wake_policy: wakePolicy,
    ...(wakePolicy === "when-idle" ? { expiry: "2026-08-14T03:00:00.000Z" } : {}),
  };
  return {
    messageId,
    from: "coordinator",
    body: {
      messageId,
      bytes: Buffer.byteLength(body, "utf8"),
      sha256: createHash("sha256").update(body).digest("hex"),
      contentRef: null,
    },
    route,
    routeFingerprint: createHash("sha256")
      .update(JSON.stringify(route))
      .digest("hex"),
    createdAt: "2026-08-14T00:00:00.000Z",
  };
}

test("a Logical Message and its recipient Delivery commit in one private journal record", async () => {
  const committed = await ledger.commitSingleRecipientDelivery({
    logicalMessage: logicalMessage(),
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    now: "2026-08-14T00:00:00.000Z",
  });
  assert.equal(committed.created, true);
  assert.equal(committed.record.delivery.state, "created");
  assert.equal(committed.record.delivery.attempts.length, 0);

  const segments = readdirSync(ledger.DELIVERY_LEDGER_SEGMENTS_DIR);
  assert.equal(segments.length, 1);
  const segment = path.join(ledger.DELIVERY_LEDGER_SEGMENTS_DIR, segments[0]);
  assert.equal(statSync(ledger.DELIVERY_LEDGER_DIR).mode & 0o777, 0o700);
  assert.equal(statSync(segment).mode & 0o777, 0o600);
  const lines = readFileSync(segment, "utf8").trim().split("\n");
  assert.equal(lines.length, 1);
  assert.equal(JSON.parse(lines[0]).recordType, "ledger-batch");
  assert.doesNotMatch(lines[0], /private coordination body/);
});

test("Ledger idempotency preserves one batch and rejects changed content", async () => {
  const duplicate = await ledger.commitSingleRecipientDelivery({
    logicalMessage: logicalMessage(),
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    now: "2026-08-14T00:00:00.000Z",
  });
  assert.equal(duplicate.created, false);

  await assert.rejects(
    ledger.commitSingleRecipientDelivery({
      logicalMessage: logicalMessage(ids.message, "changed private body"),
      target: "auditor",
      targetThreadId: ids.targetThread,
      admissionState: "admitted",
      admissionReason: "binding_match",
      now: "2026-08-14T00:00:00.000Z",
    }),
    /idempotency conflict/,
  );
});

test("attempt and evidence records rebuild the strongest proven state", async () => {
  const attempt = await ledger.beginImmediateDelivery(ids.message, {
    now: "2026-08-14T00:00:01.000Z",
  });
  assert.equal(ledger.readDeliveryLedger(ids.message).delivery.state, "created");
  assert.equal(ledger.readDeliveryLedger(ids.message).delivery.attempts.length, 1);

  const unknown = await ledger.appendDeliveryEvidence(ids.message, {
    attemptId: attempt.attemptId,
    state: "unknown",
    evidenceKind: "dispatch-result",
    errorCode: "EPIPE",
    observedAt: "2026-08-14T00:00:02.000Z",
  });
  assert.equal(unknown.delivery.state, "unknown");

  const accepted = await ledger.appendDeliveryEvidence(ids.message, {
    attemptId: attempt.attemptId,
    state: "turn_started",
    evidenceKind: "reconciliation",
    turnId: ids.turn,
    transportResult: "reconciled",
    observedAt: "2026-08-14T00:00:03.000Z",
  });
  assert.equal(accepted.delivery.state, "turn_started");
  assert.equal(accepted.delivery.turnId, ids.turn);
  assert.equal(ledger.listDeliveryLedger().length, 1);
});

test("when-idle Delivery uses an expiring claim before one dispatch attempt", async () => {
  const scheduled = logicalMessage(ids.scheduledMessage, "scheduled body", "when-idle");
  scheduled.body.contentRef = `cxmsg-message:${ids.scheduledMessage}`;
  const committed = await ledger.commitSingleRecipientDelivery({
    logicalMessage: scheduled,
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    wakePolicy: "when-idle",
    now: "2026-08-14T01:00:00.000Z",
  });
  assert.equal(committed.record.delivery.state, "scheduled");

  const first = await ledger.claimScheduledDelivery(ids.scheduledMessage, {
    workerId: ids.worker,
    leaseMs: 30_000,
    now: "2026-08-14T01:00:01.000Z",
  });
  assert.equal(first.acquired, true);
  const competing = await ledger.claimScheduledDelivery(ids.scheduledMessage, {
    workerId: "71345678-1234-4234-8234-123456789abc",
    leaseMs: 30_000,
    now: "2026-08-14T01:00:02.000Z",
  });
  assert.equal(competing.acquired, false);
  assert.equal(competing.claim.claimId, first.claim.claimId);

  await ledger.releaseScheduledDeliveryClaim(ids.scheduledMessage, {
    claimId: first.claim.claimId,
    workerId: ids.worker,
    reason: "target_busy",
    now: "2026-08-14T01:00:03.000Z",
  });
  const second = await ledger.claimScheduledDelivery(ids.scheduledMessage, {
    workerId: ids.worker,
    leaseMs: 30_000,
    now: "2026-08-14T01:00:04.000Z",
  });
  const attempt = await ledger.beginScheduledDelivery(ids.scheduledMessage, {
    claimId: second.claim.claimId,
    workerId: ids.worker,
    now: "2026-08-14T01:00:05.000Z",
  });
  await ledger.appendDeliveryEvidence(ids.scheduledMessage, {
    attemptId: attempt.attemptId,
    state: "turn_started",
    evidenceKind: "dispatch-result",
    turnId: ids.turn,
    transportResult: "started",
    observedAt: "2026-08-14T01:00:06.000Z",
  });
  const rebuilt = ledger.readDeliveryLedger(ids.scheduledMessage);
  assert.equal(rebuilt.delivery.state, "turn_started");
  assert.equal(rebuilt.delivery.claimCount, 2);
  await assert.rejects(
    ledger.claimScheduledDelivery(ids.scheduledMessage, {
      workerId: ids.worker,
      now: "2026-08-14T01:00:07.000Z",
    }),
    /not claimable/,
  );
});

test("an expired claim is reclaimable after scheduler restart", async () => {
  const scheduled = logicalMessage(ids.recoveryMessage, "recover body", "when-idle");
  scheduled.body.contentRef = `cxmsg-message:${ids.recoveryMessage}`;
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: scheduled,
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    wakePolicy: "when-idle",
    now: "2026-08-14T02:00:00.000Z",
  });
  const abandoned = await ledger.claimScheduledDelivery(ids.recoveryMessage, {
    workerId: ids.worker,
    leaseMs: 1_000,
    now: "2026-08-14T02:00:01.000Z",
  });
  const recovered = await ledger.claimScheduledDelivery(ids.recoveryMessage, {
    workerId: "91345678-1234-4234-8234-123456789abc",
    leaseMs: 30_000,
    now: abandoned.claim.leaseUntil,
  });
  assert.equal(recovered.acquired, true);
  assert.notEqual(recovered.claim.claimId, abandoned.claim.claimId);
  assert.equal(ledger.readDeliveryLedger(ids.recoveryMessage).delivery.claimCount, 2);
});

test("the per-target queue bound rejects before committing another batch", async () => {
  const first = logicalMessage(ids.boundedMessage, "first queued", "when-idle");
  first.body.contentRef = `cxmsg-message:${ids.boundedMessage}`;
  await ledger.commitSingleRecipientDelivery(
    {
      logicalMessage: first,
      target: "bounded-auditor",
      targetThreadId: ids.boundedThread,
      admissionState: "admitted",
      admissionReason: "binding_match",
      wakePolicy: "when-idle",
      now: "2026-08-14T02:00:00.000Z",
    },
    { scheduledPerTargetLimit: 1 },
  );
  const second = logicalMessage(ids.rejectedMessage, "second queued", "when-idle");
  second.body.contentRef = `cxmsg-message:${ids.rejectedMessage}`;
  await assert.rejects(
    ledger.commitSingleRecipientDelivery(
      {
        logicalMessage: second,
        target: "bounded-auditor",
        targetThreadId: ids.boundedThread,
        admissionState: "admitted",
        admissionReason: "binding_match",
        wakePolicy: "when-idle",
        now: "2026-08-14T02:00:01.000Z",
      },
      { scheduledPerTargetLimit: 1 },
    ),
    /queue.*reached 1/,
  );
  assert.equal(ledger.readDeliveryLedger(ids.rejectedMessage), null);
});

test("scheduled cancellation is terminal and the rebuildable index self-recovers", async () => {
  const scheduled = logicalMessage(ids.cancelledMessage, "cancel body", "when-idle");
  scheduled.body.contentRef = `cxmsg-message:${ids.cancelledMessage}`;
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: scheduled,
    target: "cancel-auditor",
    targetThreadId: ids.boundedThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    wakePolicy: "when-idle",
    now: "2026-08-14T02:00:00.000Z",
  });
  const cancelled = await ledger.cancelScheduledDelivery(ids.cancelledMessage, {
    now: "2026-08-14T02:00:01.000Z",
  });
  assert.equal(cancelled.cancelled, true);
  assert.equal(cancelled.record.delivery.state, "cancelled");
  assert.equal(
    (await ledger.cancelScheduledDelivery(ids.cancelledMessage, {
      now: "2026-08-14T02:00:02.000Z",
    })).cancelled,
    false,
  );

  assert.equal(statSync(ledger.DELIVERY_LEDGER_INDEX_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(ledger.DELIVERY_LEDGER_INDEX_CHECKPOINT_PATH).mode & 0o777,
    0o600,
  );
  unlinkSync(path.join(ledger.DELIVERY_LEDGER_INDEX_DIR, `${ids.cancelledMessage}.json`));
  const indexed = await ledger.listDeliveryLedgerIndexed();
  assert.equal(
    indexed.find((record) => record.logicalMessage.messageId === ids.cancelledMessage)
      .delivery.state,
    "cancelled",
  );
  assert.equal(
    statSync(
      path.join(ledger.DELIVERY_LEDGER_INDEX_DIR, `${ids.cancelledMessage}.json`),
    ).mode & 0o777,
    0o600,
  );
  const shard = path.join(
    ledger.DELIVERY_LEDGER_INDEX_DIR,
    `${ids.cancelledMessage}.json`,
  );
  const wrapper = JSON.parse(readFileSync(shard, "utf8"));
  const malformed = structuredClone(wrapper);
  malformed.projection.delivery.state = "scheduled";
  malformed.projectionSha256 = createHash("sha256")
    .update(JSON.stringify(malformed.projection))
    .digest("hex");
  writeFileSync(shard, `${JSON.stringify(malformed)}\n`, { mode: 0o600 });
  await assert.rejects(
    () => ledger.listDeliveryLedgerIndexed(),
    /index entry failed validation/,
  );
  writeFileSync(shard, `${JSON.stringify(wrapper)}\n`, { mode: 0o600 });
});

test("an incomplete active tail is quarantined before a new batch is appended", async () => {
  const active = path.join(
    ledger.DELIVERY_LEDGER_SEGMENTS_DIR,
    readdirSync(ledger.DELIVERY_LEDGER_SEGMENTS_DIR)[0],
  );
  appendFileSync(active, '{"schemaVersion":1', "utf8");

  const second = logicalMessage(ids.secondMessage, "second body");
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: second,
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    now: "2026-08-14T00:00:04.000Z",
  });
  assert.equal(readdirSync(ledger.DELIVERY_LEDGER_QUARANTINE_DIR).length, 1);
  assert.equal(readdirSync(ledger.DELIVERY_LEDGER_SEGMENTS_DIR).length, 1);
  assert.equal(ledger.listDeliveryLedger().length, 6);
});

test("Ledger scans reject broad modes and symlink segments", () => {
  const active = path.join(
    ledger.DELIVERY_LEDGER_SEGMENTS_DIR,
    readdirSync(ledger.DELIVERY_LEDGER_SEGMENTS_DIR)[0],
  );
  chmodSync(active, 0o644);
  assert.throws(
    () => ledger.readDeliveryLedger(ids.message),
    /permissions are too broad/,
  );
  chmodSync(active, 0o600);

  const linked = path.join(
    ledger.DELIVERY_LEDGER_SEGMENTS_DIR,
    "segment-99999999.jsonl",
  );
  symlinkSync(active, linked);
  assert.throws(
    () => ledger.readDeliveryLedger(ids.message),
    /not a regular file/,
  );
  unlinkSync(linked);
});

test("a complete invalid record in Ledger quarantine fails the whole scan closed", () => {
  writeFileSync(
    path.join(
      ledger.DELIVERY_LEDGER_QUARANTINE_DIR,
      "segment-00000003.partial-51345678-1234-4234-8234-123456789abc.jsonl",
    ),
    "{}\n",
    { mode: 0o600 },
  );
  assert.throws(
    () => ledger.readDeliveryLedger(ids.message),
    /invalid Delivery Ledger record/,
  );
});
