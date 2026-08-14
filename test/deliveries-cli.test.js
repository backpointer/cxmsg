import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-deliveries-cli-"));
process.env.CXMSG_STATE_DIR = stateDir;
const ledger = await import(`../src/delivery-ledger.js?cli-test=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?cli-test=${Date.now()}`);
const bin = path.resolve("bin/cxmsg.js");

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function run(...args) {
  return spawnSync(process.execPath, [bin, ...args], {
    cwd: path.resolve("."),
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    encoding: "utf8",
  });
}

test("deliveries CLI lists, shows, cancels, and rebuilds without body disclosure", async () => {
  const messageId = "71345678-3234-4234-8234-123456789abc";
  const threadId = "81345678-3234-4234-8234-123456789abc";
  const triggerJobId = "91345678-3234-4234-8234-123456789abc";
  const body = "private scheduled CLI body";
  const route = {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: messageId,
    payload_type: "coordination",
    wake_policy: "after-job",
    trigger_job_id: triggerJobId,
    expiry: "2026-08-15T01:00:00.000Z",
  };
  const reference = await bodies.storeMessageBody({ messageId, body });
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
    targetThreadId: threadId,
    admissionState: "admitted",
    admissionReason: "binding_match",
    wakePolicy: "after-job",
    now: "2026-08-15T00:00:00.000Z",
  });

  const listed = run("deliveries", "list", "--status", "scheduled", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  assert.equal(JSON.parse(listed.stdout)[0].logicalMessageId, messageId);
  assert.deepEqual(JSON.parse(listed.stdout)[0].trigger, {
    kind: "job",
    id: triggerJobId,
  });
  assert.doesNotMatch(listed.stdout, /private scheduled CLI body/);

  const shown = run("deliveries", "show", messageId, "--json");
  assert.equal(shown.status, 0, shown.stderr);
  assert.equal(JSON.parse(shown.stdout).body.contentRef, reference.contentRef);
  assert.doesNotMatch(shown.stdout, /private scheduled CLI body/);

  const cancelled = run("deliveries", "cancel", messageId, "--json");
  assert.equal(cancelled.status, 0, cancelled.stderr);
  assert.equal(JSON.parse(cancelled.stdout).status, "cancelled");
  assert.equal(JSON.parse(cancelled.stdout).cancelled, true);

  const rebuilt = run("deliveries", "rebuild-index", "--json");
  assert.equal(rebuilt.status, 0, rebuilt.stderr);
  assert.equal(JSON.parse(rebuilt.stdout).messageCount, 1);
});
