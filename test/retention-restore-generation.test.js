import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-retention-generation-"));
process.env.CXMSG_STATE_DIR = stateDir;
const bodies = await import(`../src/message-bodies.js?test=${Date.now()}`);
const retention = await import("../src/retention.js");
const transaction = await import("../src/retention-transaction.js");

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("restore rejects a changed active generation without overwriting new writes", async () => {
  const selected = "11345678-1234-4234-8234-123456789abc";
  const newer = "21345678-1234-4234-8234-123456789abc";
  await bodies.storeMessageBody({ messageId: selected, body: "old private body" });
  const now = Date.parse("2027-01-01T00:00:00.000Z");
  const before = "2026-11-01T00:00:00.000Z";
  const plan = await retention.buildRetentionPlan(
    { before, scope: "bodies" },
    { now },
  );
  const receipt = await transaction.purgeRetention(
    { before, scope: "bodies", expectedPlanDigest: plan.planDigest },
    { now },
  );
  await bodies.storeMessageBody({ messageId: newer, body: "new private body" });

  await assert.rejects(
    transaction.restoreRetention({ backupId: receipt.backupId }),
    /active generation changed/,
  );
  assert.equal(bodies.messageBodyInfo(newer).bodyBytes, Buffer.byteLength("new private body"));
  assert.throws(() => bodies.messageBodyInfo(selected), /unknown message body/);
});
