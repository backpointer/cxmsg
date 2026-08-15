import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-delivery-tombstone-"));
process.env.CXMSG_STATE_DIR = stateDir;
const ledger = await import(`../src/delivery-ledger.js?test=${Date.now()}`);
const { withRetentionMutation } = await import("../src/retention-barrier.js");

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const messageId = "11345678-1234-4234-8234-123456789abc";
const replyId = "21345678-1234-4234-8234-123456789abc";
const backupId = "31345678-1234-4234-8234-123456789abc";
const turnId = "41345678-1234-4234-8234-123456789abc";

function logicalMessage(id, replyToMessageId = null) {
  const body = `body-${id}`;
  return {
    messageId: id,
    from: "sender",
    ...(replyToMessageId ? { replyToMessageId } : {}),
    body: {
      messageId: id,
      bytes: Buffer.byteLength(body, "utf8"),
      sha256: createHash("sha256").update(body).digest("hex"),
      contentRef: null,
    },
    route: null,
    routeFingerprint: createHash("sha256").update("null").digest("hex"),
    createdAt: "2026-01-01T00:00:00.000Z",
  };
}

function commit(id, replyToMessageId = null) {
  return ledger.commitSingleRecipientDelivery({
    logicalMessage: logicalMessage(id, replyToMessageId),
    target: "target",
    admissionState: "admitted",
    admissionReason: "unbound_target",
    now: "2026-01-01T00:00:00.000Z",
  });
}

test("Delivery dedup Tombstones require mutation authority and permanently block reuse", async () => {
  await commit(messageId);
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

  await assert.rejects(
    ledger.createDeliveryDedupTombstones({ messageIds: [messageId], backupId }),
    /mutation lease is required/,
  );
  const created = await withRetentionMutation(() =>
    ledger.createDeliveryDedupTombstones({
      messageIds: [messageId],
      backupId,
      purgedAt: "2026-08-15T00:00:00.000Z",
    }),
  );
  assert.equal(created.length, 1);
  assert.equal(created[0].messageId, messageId);
  assert.equal(created[0].backupId, backupId);
  assert.deepEqual(
    ledger.listDeliveryDedupTombstones().map((record) => record.messageId),
    [messageId],
  );

  await assert.rejects(commit(messageId), /permanently purged/);
  await assert.rejects(commit(replyId, messageId), /references a permanently purged message/);
});

test("Delivery dedup Tombstones reject nonterminal records", async () => {
  await commit(replyId);
  await assert.rejects(
    withRetentionMutation(() =>
      ledger.createDeliveryDedupTombstones({ messageIds: [replyId], backupId }),
    ),
    /not terminal and purgeable/,
  );
});
