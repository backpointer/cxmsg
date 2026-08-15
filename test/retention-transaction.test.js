import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-retention-transaction-"));
process.env.CXMSG_STATE_DIR = stateDir;
const ledger = await import(`../src/delivery-ledger.js?test=${Date.now()}`);
const bodies = await import("../src/message-bodies.js");
const retention = await import("../src/retention.js");
const transaction = await import("../src/retention-transaction.js");
const route = await import("../src/route-admission.js");

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const ids = {
  terminal: "11345678-1234-4234-8234-123456789abc",
  unknown: "21345678-1234-4234-8234-123456789abc",
  quarantined: "31345678-1234-4234-8234-123456789abc",
  terminalTurn: "41345678-1234-4234-8234-123456789abc",
};
const createdAt = "2026-01-01T00:00:00.000Z";
const cutoff = "2026-09-23T00:00:00.000Z";
const retentionNow = Date.parse("2027-01-01T00:00:00.000Z");

function bodyDigest(body) {
  return createHash("sha256").update(body).digest("hex");
}

function logicalMessage(messageId, body, contentRef = null) {
  return {
    messageId,
    from: "sender",
    body: {
      messageId,
      bytes: Buffer.byteLength(body, "utf8"),
      sha256: bodyDigest(body),
      contentRef,
    },
    route: null,
    routeFingerprint: bodyDigest("null"),
    createdAt,
  };
}

async function commitAdmitted(messageId, body, contentRef = null) {
  return ledger.commitSingleRecipientDelivery({
    logicalMessage: logicalMessage(messageId, body, contentRef),
    target: "target",
    admissionState: "admitted",
    admissionReason: "unbound_target",
    now: createdAt,
  });
}

test("purge roll-forwards after an interrupted swap and restore preserves Tombstones", async () => {
  const terminalBody = "private terminal payload";
  const bodyReference = await bodies.storeMessageBody({
    messageId: ids.terminal,
    body: terminalBody,
  });
  await commitAdmitted(ids.terminal, terminalBody, bodyReference.contentRef);
  const terminalAttempt = await ledger.beginImmediateDelivery(ids.terminal, {
    now: "2026-01-01T00:00:01.000Z",
  });
  await ledger.appendDeliveryEvidence(ids.terminal, {
    attemptId: terminalAttempt.attemptId,
    state: "turn_started",
    evidenceKind: "dispatch-result",
    transportResult: "start",
    turnId: ids.terminalTurn,
    observedAt: "2026-01-01T00:00:02.000Z",
  });

  await commitAdmitted(ids.unknown, "ambiguous payload");
  const unknownAttempt = await ledger.beginImmediateDelivery(ids.unknown, {
    now: "2026-01-01T00:00:03.000Z",
  });
  await ledger.appendDeliveryEvidence(ids.unknown, {
    attemptId: unknownAttempt.attemptId,
    state: "unknown",
    evidenceKind: "dispatch-result",
    errorCode: "ETESTUNKNOWN",
    observedAt: "2026-01-01T00:00:04.000Z",
  });

  const quarantinedBody = "private quarantined payload";
  await ledger.commitSingleRecipientDelivery({
    logicalMessage: logicalMessage(ids.quarantined, quarantinedBody),
    target: "target",
    admissionState: "quarantined",
    admissionReason: "project_mismatch",
    now: createdAt,
  });
  mkdirSync(route.QUARANTINE_DIR, { recursive: true, mode: 0o700 });
  writeFileSync(
    path.join(route.QUARANTINE_DIR, `${ids.quarantined}.json`),
    `${JSON.stringify({
      version: 1,
      logicalMessageId: ids.quarantined,
      from: "sender",
      target: "target",
      route: null,
      reason: "project_mismatch",
      message: quarantinedBody,
      messageBytes: Buffer.byteLength(quarantinedBody, "utf8"),
      messageSha256: bodyDigest(quarantinedBody),
      quarantinedAt: createdAt,
    }, null, 2)}\n`,
    { mode: 0o600 },
  );

  const plan = await retention.buildRetentionPlan(
    { before: cutoff, scope: "all" },
    { now: retentionNow },
  );
  assert.equal(plan.policy.mutationEnabled, true);
  assert.deepEqual(
    plan.categories.ledger.eligible.map((candidate) => candidate.messageId).sort(),
    [ids.quarantined, ids.terminal].sort(),
  );
  assert.deepEqual(
    plan.categories.ledger.blocked.map((candidate) => candidate.messageId),
    [ids.unknown],
  );
  assert.deepEqual(
    plan.categories.quarantine.eligible.map((candidate) => candidate.messageId),
    [ids.quarantined],
  );
  await assert.rejects(
    transaction.purgeRetention(
      {
        before: cutoff,
        scope: "all",
        expectedPlanDigest: "0".repeat(64),
      },
      { now: retentionNow },
    ),
    /plan changed/,
  );
  await assert.rejects(
    transaction.purgeRetention(
      {
        before: cutoff,
        scope: "all",
        expectedPlanDigest: plan.planDigest,
      },
      {
        now: retentionNow,
        fault: (phase) => {
          if (phase === "prepared") throw new Error("simulated pre-Tombstone crash");
        },
      },
    ),
    /simulated pre-Tombstone crash/,
  );
  const abandoned = await transaction.recoverRetentionTransactions();
  assert.equal(abandoned.length, 1);
  assert.equal(abandoned[0].outcome, "abandoned");
  assert.deepEqual(ledger.listDeliveryDedupTombstones(), []);

  await assert.rejects(
    transaction.purgeRetention(
      {
        before: cutoff,
        scope: "all",
        expectedPlanDigest: plan.planDigest,
      },
      {
        now: retentionNow,
        fault: (phase) => {
          if (phase === "after-backup-ledger") throw new Error("simulated crash");
        },
      },
    ),
    /simulated crash/,
  );

  const recovered = await transaction.recoverRetentionTransactions();
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].outcome, "committed");
  const backupId = recovered[0].backupId;
  assert.equal((await ledger.readDeliveryLedgerIndexed(ids.terminal)), null);
  assert.equal((await ledger.readDeliveryLedgerIndexed(ids.quarantined)), null);
  assert.equal((await ledger.readDeliveryLedgerIndexed(ids.unknown)).delivery.state, "unknown");
  assert.throws(() => bodies.messageBodyInfo(ids.terminal), /unknown message body/);
  assert.deepEqual(route.listQuarantine(), []);
  assert.deepEqual(
    ledger.listDeliveryDedupTombstones().map((record) => record.messageId).sort(),
    [ids.quarantined, ids.terminal].sort(),
  );

  const receiptFilename = path.join(
    transaction.RETENTION_RECEIPTS_DIR,
    `${backupId}.json`,
  );
  const receiptRaw = readFileSync(receiptFilename, "utf8");
  const receipt = JSON.parse(receiptRaw);
  assert.equal(receipt.outcome, "committed");
  assert.doesNotMatch(JSON.stringify(receipt), /private terminal|private quarantined|bodyBase64|\/tmp\//);
  assert.equal(statSync(receiptFilename).mode & 0o077, 0);

  writeFileSync(
    receiptFilename,
    `${JSON.stringify({ ...receipt, planDigest: "f".repeat(64) }, null, 2)}\n`,
    { mode: 0o600 },
  );
  await assert.rejects(
    transaction.restoreRetention({ backupId }),
    /receipt digest does not match/,
  );
  writeFileSync(receiptFilename, receiptRaw, { mode: 0o600 });

  await assert.rejects(
    transaction.restoreRetention(
      { backupId },
      {
        fault: (phase) => {
          if (phase === "after-restore-backup-ledger") {
            throw new Error("simulated restore crash");
          }
        },
      },
    ),
    /simulated restore crash/,
  );
  const restoreRecovery = await transaction.recoverRetentionTransactions();
  assert.deepEqual(restoreRecovery, [{ backupId, outcome: "restored" }]);
  assert.equal((await ledger.readDeliveryLedgerIndexed(ids.terminal)).delivery.state, "turn_started");
  assert.equal(bodies.messageBodyInfo(ids.terminal).bodySha256, bodyDigest(terminalBody));
  assert.equal(route.listQuarantine().length, 1);
  await assert.rejects(commitAdmitted(ids.terminal, terminalBody, bodyReference.contentRef), /permanently purged/);
  await assert.rejects(
    ledger.commitSingleRecipientDelivery({
      logicalMessage: {
        ...logicalMessage("51345678-1234-4234-8234-123456789abc", "reply"),
        replyToMessageId: ids.terminal,
      },
      target: "target",
      admissionState: "admitted",
      admissionReason: "unbound_target",
      now: createdAt,
    }),
    /references a permanently purged message/,
  );
  await assert.rejects(transaction.restoreRetention({ backupId }), /not the current transaction head/);
});
