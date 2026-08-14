import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-claude-route-"));
process.env.CXMSG_STATE_DIR = stateDir;
const bridge = await import(`../src/claude-bridge.js?route-test=${Date.now()}`);
const registry = await import(`../src/registry.js?claude-route=${Date.now()}`);
const routes = await import(`../src/route-admission.js?claude-route=${Date.now()}`);
const threadId = "c1345678-1234-4234-8234-123456789abc";
registry.writeSessionRecord({ name: "coordinator", threadId, cwd: path.resolve(".") });
routes.writeRouteBinding({
  sessionName: "coordinator",
  threadId,
  projectId: "hermes",
  role: "coordinator",
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function parsed(messageId, body) {
  return {
    fromName: "claude-auditor",
    fromSession: "d1345678-1234-4234-8234-123456789abc",
    fromAddress: "uds:///private/redacted.sock",
    messageId,
    body,
  };
}

test("Claude bridge quarantines untyped ingress and admits an exact typed route", async () => {
  let dispatches = 0;
  const dependencies = {
    withServer: async (callback) => callback({}),
    readThread: async () => ({ id: threadId }),
    deliver: async (_client, _thread, payload) => {
      dispatches += 1;
      return {
        delivery: "started",
        messageId: payload.messageId,
        turnId: "turn-claude-route",
        payload,
      };
    },
  };

  const quarantined = await bridge.deliverClaudeMessage(
    "coordinator",
    parsed("e1345678-1234-4234-8234-123456789abc", "untyped"),
    dependencies,
  );
  assert.equal(quarantined.delivery, "quarantined");
  assert.equal(quarantined.admission.reason, "missing_route");
  assert.equal(dispatches, 0);

  const logicalMessageId = "f1345678-1234-4234-8234-123456789abc";
  const admitted = await bridge.deliverClaudeMessage(
    "coordinator",
    parsed(
      "01345678-1234-4234-8234-123456789abc",
      JSON.stringify({
        protocol: "cxmsg-route/1",
        schema_version: 1,
        project_id: "hermes",
        target_role: "coordinator",
        logical_message_id: logicalMessageId,
        payload_type: "coordination",
        wake_policy: "immediate",
        message: "typed review result",
      }),
    ),
    dependencies,
  );
  assert.equal(admitted.delivery, "started");
  assert.equal(admitted.messageId, logicalMessageId);
  assert.equal(admitted.payload.route.project_id, "hermes");
  assert.match(admitted.payload.message, /typed review result/);
  assert.equal(dispatches, 1);
});
