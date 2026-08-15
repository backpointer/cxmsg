import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  mkdtempSync,
  readdirSync,
  rmSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-retention-partial-"));
process.env.CXMSG_STATE_DIR = stateDir;
const ledger = await import(`../src/delivery-ledger.js?test=${Date.now()}`);
const retention = await import("../src/retention.js");
const transaction = await import("../src/retention-transaction.js");

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("purge rejects an incomplete source generation before Tombstones or swap", async () => {
  const messageId = "11345678-1234-4234-8234-123456789abc";
  const turnId = "21345678-1234-4234-8234-123456789abc";
  const body = "retained evidence";
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: {
      messageId,
      from: "sender",
      body: {
        messageId,
        bytes: Buffer.byteLength(body),
        sha256: createHash("sha256").update(body).digest("hex"),
        contentRef: null,
      },
      route: null,
      routeFingerprint: createHash("sha256").update("null").digest("hex"),
      createdAt: "2026-01-01T00:00:00.000Z",
    },
    target: "target",
    admissionState: "admitted",
    admissionReason: "unbound_target",
    now: "2026-01-01T00:00:00.000Z",
  });
  const attempt = await ledger.beginImmediateDelivery(messageId, {
    now: "2026-01-01T00:00:01.000Z",
  });
  await ledger.appendDeliveryEvidence(messageId, {
    attemptId: attempt.attemptId,
    state: "turn_started",
    evidenceKind: "dispatch-result",
    transportResult: "start",
    turnId,
    observedAt: "2026-01-01T00:00:02.000Z",
  });
  const segment = readdirSync(ledger.DELIVERY_LEDGER_SEGMENTS_DIR)
    .filter((name) => name.endsWith(".jsonl"))
    .sort()
    .at(-1);
  appendFileSync(path.join(ledger.DELIVERY_LEDGER_SEGMENTS_DIR, segment), '{"partial"');

  const now = Date.parse("2027-01-01T00:00:00.000Z");
  const before = "2026-09-23T00:00:00.000Z";
  const plan = await retention.buildRetentionPlan(
    { before, scope: "ledger" },
    { now },
  );
  await assert.rejects(
    transaction.purgeRetention(
      { before, scope: "ledger", expectedPlanDigest: plan.planDigest },
      { now },
    ),
    /incomplete segment/,
  );
  assert.deepEqual(ledger.listDeliveryDedupTombstones(), []);
  assert.equal((await ledger.readDeliveryLedgerIndexed(messageId)).delivery.state, "turn_started");
  const recovered = await transaction.recoverRetentionTransactions();
  assert.equal(recovered.at(-1)?.outcome, "abandoned");
});
