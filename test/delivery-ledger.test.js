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
  sourceThread: "12345678-2234-4234-8234-123456789abc",
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
  handleMessage: "e1345678-1234-4234-8234-123456789abc",
  handleCollision: "f1345678-1234-4234-8234-123456789abc",
  retryMessage: "02345678-1234-4234-8234-123456789abc",
  retryTurn: "03345678-1234-4234-8234-123456789abc",
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
    senderThreadId: ids.sourceThread,
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
  assert.match(committed.record.delivery.replyHandle, ledger.REPLY_HANDLE_PATTERN);
  assert.equal(
    ledger.findDeliveryByReplyHandle({
      replyHandle: committed.record.delivery.replyHandle,
      target: "auditor",
      targetThreadId: ids.targetThread,
    }).logicalMessage.messageId,
    ids.message,
  );
  assert.equal(
    ledger.findDeliveryByReplyHandle({
      replyHandle: committed.record.delivery.replyHandle,
      target: "other-auditor",
      targetThreadId: ids.targetThread,
    }),
    null,
  );

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

test("metadata-only Inbound Policy denial evidence has a closed identity schema", async () => {
  const senderNodeKey = `codex:${ids.sourceThread}`;
  const targetNodeKey = `codex:${ids.targetThread}`;
  const denied = {
    schemaVersion: 1,
    recordType: "ledger-batch",
    batchId: "91345678-2234-4234-8234-123456789abc",
    logicalMessage: {
      ...logicalMessage(),
      senderNodeKey,
      senderIdentitySha256: createHash("sha256")
        .update(senderNodeKey)
        .digest("hex"),
    },
    deliveries: [
      {
        deliveryId: "a1345678-2234-4234-8234-123456789abc",
        target: "auditor",
        targetThreadId: ids.targetThread,
        replyHandle: null,
        admissionState: "denied",
        admissionReason: "inbound_policy",
        inboundPolicy: {
          decision: "deny",
          reason: "sender_denied",
          targetNodeKey,
          senderIdentityState: "verified",
          senderNodeKey,
          senderProjectId: "71345678-1234-4234-8234-123456789abc",
          policyRevision: 1,
          policySha256: "a".repeat(64),
          ruleId: "81345678-2234-4234-8234-123456789abc",
          selectorKind: "sender-node",
          failClosed: false,
        },
        wakePolicy: "immediate",
        state: "denied",
        createdAt: "2026-08-14T00:00:00.000Z",
        updatedAt: "2026-08-14T00:00:00.000Z",
      },
    ],
    committedAt: "2026-08-14T00:00:00.000Z",
  };

  assert.equal(ledger.validDeliveryLedgerRecord(denied), true);

  const missingIdentityDigest = structuredClone(denied);
  delete missingIdentityDigest.logicalMessage.senderIdentitySha256;
  assert.equal(ledger.validDeliveryLedgerRecord(missingIdentityDigest), false);

  const mismatchedIdentityDigest = structuredClone(denied);
  mismatchedIdentityDigest.logicalMessage.senderIdentitySha256 = "f".repeat(64);
  assert.equal(ledger.validDeliveryLedgerRecord(mismatchedIdentityDigest), false);

  const claimedUnverifiedIdentity = structuredClone(denied);
  claimedUnverifiedIdentity.deliveries[0].inboundPolicy.senderIdentityState =
    "unverifiable";
  assert.equal(
    ledger.validDeliveryLedgerRecord(claimedUnverifiedIdentity),
    false,
  );
  const extendedEvidence = structuredClone(denied);
  extendedEvidence.deliveries[0].inboundPolicy.unboundedDetail = "forbidden";
  assert.equal(ledger.validDeliveryLedgerRecord(extendedEvidence), false);

  const mismatchedSelector = structuredClone(denied);
  mismatchedSelector.deliveries[0].inboundPolicy.selectorKind =
    "sender-project";
  assert.equal(ledger.validDeliveryLedgerRecord(mismatchedSelector), false);

  const failClosedMatchedRule = structuredClone(denied);
  failClosedMatchedRule.deliveries[0].inboundPolicy.failClosed = true;
  assert.equal(ledger.validDeliveryLedgerRecord(failClosedMatchedRule), false);

  const unverifiableProjectClaim = structuredClone(denied);
  unverifiableProjectClaim.deliveries[0].inboundPolicy.senderIdentityState =
    "unverifiable";
  unverifiableProjectClaim.deliveries[0].inboundPolicy.senderNodeKey = null;
  assert.equal(
    ledger.validDeliveryLedgerRecord(unverifiableProjectClaim),
    false,
  );
});

test("reply handles are recipient-scoped and collisions are retried", async () => {
  const first = await ledger.commitSingleRecipientDelivery(
    {
      logicalMessage: logicalMessage(ids.handleMessage),
      target: "handle-auditor",
      targetThreadId: ids.boundedThread,
      admissionState: "admitted",
      admissionReason: "binding_match",
      now: "2026-08-14T00:00:10.000Z",
    },
    { replyHandleFactory: () => "m:0000000000" },
  );
  let allocations = 0;
  const second = await ledger.commitSingleRecipientDelivery(
    {
      logicalMessage: logicalMessage(ids.handleCollision),
      target: "handle-auditor",
      targetThreadId: ids.boundedThread,
      admissionState: "admitted",
      admissionReason: "binding_match",
      now: "2026-08-14T00:00:11.000Z",
    },
    {
      replyHandleFactory: () =>
        allocations++ === 0 ? "m:0000000000" : "m:1111111111",
    },
  );
  assert.equal(first.record.delivery.replyHandle, "m:0000000000");
  assert.equal(second.record.delivery.replyHandle, "m:1111111111");
  assert.equal(allocations, 2);
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
  assert.equal(ledger.listDeliveryLedger().length, 3);
});

test("one explicit retry reuses the Delivery and records two ordered attempts", async () => {
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: logicalMessage(ids.retryMessage),
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    now: "2026-08-14T00:10:00.000Z",
  });
  const first = await ledger.beginImmediateDelivery(ids.retryMessage, {
    now: "2026-08-14T00:10:01.000Z",
  });
  await ledger.appendDeliveryEvidence(ids.retryMessage, {
    attemptId: first.attemptId,
    state: "retryable",
    evidenceKind: "negative-acceptance",
    errorCode: "EEXPECTEDTURNMISMATCH",
    negativeAcceptanceContract: "codex-app-server/0.147.0",
    observedAt: "2026-08-14T00:10:02.000Z",
  });
  const retry = await ledger.beginRetryDelivery(ids.retryMessage, {
    now: "2026-08-14T00:10:03.000Z",
  });
  assert.equal(retry.retryOfAttemptId, first.attemptId);
  await assert.rejects(
    ledger.beginRetryDelivery(ids.retryMessage),
    /not eligible|duplicate/,
  );
  const accepted = await ledger.appendDeliveryEvidence(ids.retryMessage, {
    attemptId: retry.attemptId,
    state: "turn_started",
    evidenceKind: "dispatch-result",
    turnId: ids.retryTurn,
    transportResult: "started",
    observedAt: "2026-08-14T00:10:04.000Z",
  });
  assert.equal(accepted.delivery.attempts.length, 2);
  assert.equal(accepted.delivery.state, "turn_started");
  assert.equal(
    ledger.readDeliveryLedger(ids.retryMessage).delivery.attempts[1].retryOfAttemptId,
    first.attemptId,
  );
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

  const renewed = await ledger.renewScheduledDeliveryClaim(ids.scheduledMessage, {
    claimId: first.claim.claimId,
    workerId: ids.worker,
    leaseMs: 30_000,
    now: "2026-08-14T01:00:02.000Z",
  });
  assert.equal(renewed.renewed, true);
  assert.equal(renewed.claim.renewalCount, 1);
  assert.ok(Date.parse(renewed.claim.leaseUntil) > Date.parse(first.claim.leaseUntil));

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
  const inboundPolicySnapshot = {
    decision: "continue",
    reason: "no_match",
    targetNodeKey: `codex:${ids.targetThread}`,
    senderIdentityState: "verified",
    senderNodeKey: `codex:${ids.sourceThread}`,
    senderProjectId: "71345678-2234-4234-8234-123456789abc",
    policyRevision: 1,
    policySha256: "a".repeat(64),
    ruleId: null,
    selectorKind: null,
    failClosed: false,
  };
  const attempt = await ledger.beginScheduledDelivery(ids.scheduledMessage, {
    claimId: second.claim.claimId,
    workerId: ids.worker,
    inboundPolicySnapshot,
    now: "2026-08-14T01:00:05.000Z",
  });
  assert.deepEqual(attempt.inboundPolicySnapshot, inboundPolicySnapshot);
  assert.equal(ledger.validDeliveryLedgerRecord(attempt), true);
  assert.equal(
    ledger.validDeliveryLedgerRecord({
      ...attempt,
      inboundPolicySnapshot: {
        ...inboundPolicySnapshot,
        senderIdentityState: "unverifiable",
      },
    }),
    false,
  );
  assert.equal(
    ledger.validDeliveryLedgerRecord({
      ...attempt,
      inboundPolicySnapshot: {
        ...inboundPolicySnapshot,
        unexpected: "field",
      },
    }),
    false,
  );
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

test("an expired or replaced claim cannot be renewed by the old dispatcher", async () => {
  const messageId = "04345678-1234-4234-8234-123456789abc";
  const scheduled = logicalMessage(messageId, "lease loss", "when-idle");
  scheduled.body.contentRef = `cxmsg-message:${messageId}`;
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: scheduled,
    target: "auditor",
    targetThreadId: ids.targetThread,
    admissionState: "admitted",
    admissionReason: "binding_match",
    wakePolicy: "when-idle",
    now: "2026-08-14T03:00:00.000Z",
  });
  const abandoned = await ledger.claimScheduledDelivery(messageId, {
    workerId: ids.worker,
    leaseMs: 1_000,
    now: "2026-08-14T03:00:01.000Z",
  });
  await assert.rejects(
    ledger.renewScheduledDeliveryClaim(messageId, {
      claimId: abandoned.claim.claimId,
      workerId: ids.worker,
      now: abandoned.claim.leaseUntil,
    }),
    (error) => error.code === "ECLAIMLOST",
  );
  assert.equal(ledger.readDeliveryLedger(messageId).delivery.attempts.length, 0);
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
  const expectedRecords = ledger.listDeliveryLedger().length + 1;
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
  assert.equal(ledger.listDeliveryLedger().length, expectedRecords);
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
