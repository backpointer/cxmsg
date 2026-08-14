import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
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
};

function logicalMessage(messageId = ids.message, body = "private coordination body") {
  const route = {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: messageId,
    payload_type: "coordination",
    wake_policy: "immediate",
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
  assert.equal(ledger.listDeliveryLedger().length, 2);
});
