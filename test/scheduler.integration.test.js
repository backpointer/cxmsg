import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AppServerClient } from "../src/app-server-client.js";

const integrationEnabled = process.env.CXMSG_SCHEDULER_INTEGRATION === "1";

test(
  "real UDS scheduler starts one model turn for an idle target",
  { skip: !integrationEnabled },
  async () => {
    const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-scheduler-integration-"));
    process.env.CXMSG_STATE_DIR = stateDir;
    const suffix = `${Date.now()}-${randomUUID()}`;
    const ledger = await import(`../src/delivery-ledger.js?integration=${suffix}`);
    const bodies = await import(`../src/message-bodies.js?integration=${suffix}`);
    const scheduler = await import(`../src/scheduler.js?integration=${suffix}`);
    const client = new AppServerClient();
    await client.connect();
    const started = await client.request("thread/start", {
      cwd: process.cwd(),
      serviceName: "cxmsg-scheduler-integration-test",
    });
    const threadId = started.thread.id;
    const messageId = randomUUID();
    const body = "Reply with exactly: scheduler-integration-ok";
    try {
      const content = await bodies.storeMessageBody({ messageId, body });
      const route = {
        schema_version: 1,
        project_id: "cxmsg-integration",
        target_role: "test",
        logical_message_id: messageId,
        payload_type: "coordination",
        wake_policy: "when-idle",
        expiry: new Date(Date.now() + 60_000).toISOString(),
      };
      const committed = await ledger.commitSingleRecipientDelivery({
        logicalMessage: {
          messageId,
          from: "integration-test",
          body: {
            messageId,
            bytes: Buffer.byteLength(body),
            sha256: createHash("sha256").update(body).digest("hex"),
            contentRef: content.contentRef,
          },
          route,
          routeFingerprint: createHash("sha256")
            .update(JSON.stringify(route))
            .digest("hex"),
          createdAt: new Date().toISOString(),
        },
        target: "integration-target",
        targetThreadId: threadId,
        admissionState: "admitted",
        admissionReason: "integration-test",
        wakePolicy: "when-idle",
      });
      const outcome = await scheduler.dispatchScheduledDelivery(
        committed.record,
        client,
        randomUUID(),
        { session: () => ({ name: "integration-target", threadId }) },
      );
      assert.equal(outcome.state, "turn_started");
      assert.equal(ledger.readDeliveryLedger(messageId).delivery.state, "turn_started");
    } finally {
      await client.request("thread/delete", { threadId }).catch(() => {});
      await client.close();
      rmSync(stateDir, { recursive: true, force: true });
      delete process.env.CXMSG_STATE_DIR;
    }
  },
);
