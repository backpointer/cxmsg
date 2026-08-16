import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-claude-route-"));
process.env.CXMSG_STATE_DIR = stateDir;
const bridge = await import(`../src/claude-bridge.js?route-test=${Date.now()}`);
const directory = await import(`../src/node-directory.js?claude-route=${Date.now()}`);
const registry = await import(`../src/registry.js?claude-route=${Date.now()}`);
const routes = await import(`../src/route-admission.js?claude-route=${Date.now()}`);
const inbound = await import(`../src/inbound-policy.js?claude-route=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?claude-route=${Date.now()}`);
const threadId = "c1345678-1234-4234-8234-123456789abc";
const project = await directory.ensureProject({ routingId: "hermes", root: path.resolve(".") });
await directory.upsertNode({
  runtimeKind: "claude",
  nativeId: "d1345678-1234-4234-8234-123456789abc",
  displayName: "claude-auditor",
  projectId: project.projectId,
});
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
        turnId: "11345678-2234-4234-8234-123456789abc",
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
  assert.doesNotMatch(admitted.payload.message, /Claude reply address/);
  assert.match(admitted.payload.replyHandle, /^m:[0-9A-HJKMNP-TV-Z]{10}$/);
  assert.equal(
    routes.readRouteDelivery(logicalMessageId).senderNodeKey,
    "claude:d1345678-1234-4234-8234-123456789abc",
  );
  const reply = routes.planPeerReply({
    from: "coordinator",
    replyToMessageId: admitted.payload.replyHandle,
    logicalMessageId: "02345678-1234-4234-8234-123456789abc",
  });
  assert.equal(reply.targetRuntime, "claude");
  assert.equal(reply.targetNativeId, "d1345678-1234-4234-8234-123456789abc");
  assert.equal(dispatches, 1);
});

test("Claude ordinary ingress obeys Inbound Policy without exposing its reason on wire", async () => {
  const logicalMessageId = "12345678-3234-4234-8234-123456789abc";
  await inbound.upsertInboundDenyRule({
    targetNodeKey: directory.nodeKey("codex", threadId),
    selectorKind: "sender-node",
    selectorValue: directory.nodeKey(
      "claude",
      "d1345678-1234-4234-8234-123456789abc",
    ),
  });
  let dispatches = 0;
  const denied = await bridge.deliverClaudeMessage(
    "coordinator",
    parsed(
      "22345678-3234-4234-8234-123456789abc",
      JSON.stringify({
        protocol: "cxmsg-route/1",
        schema_version: 1,
        project_id: "hermes",
        target_role: "coordinator",
        logical_message_id: logicalMessageId,
        payload_type: "coordination",
        wake_policy: "immediate",
        message: "policy denied Claude body",
      }),
    ),
    {
      withServer: async (callback) => callback({}),
      readThread: async () => ({ id: threadId }),
      deliver: async () => {
        dispatches += 1;
        throw new Error("denied Claude ingress must not dispatch");
      },
      policyEvaluator: inbound.evaluateInboundPolicy,
    },
  );

  assert.equal(denied.delivery, "quarantined");
  assert.equal(denied.admission.reason, "route_rejected");
  assert.equal(denied.admission.denialOrigin, undefined);
  assert.equal(dispatches, 0);
  const retained = routes.readRouteDelivery(logicalMessageId);
  assert.equal(retained.admissionState, "denied");
  assert.equal(retained.attemptCount, 0);
  assert.equal(retained.inboundPolicy.reason, "sender_denied");
  assert.throws(
    () => bodies.messageBodyInfo(`cxmsg-message:${logicalMessageId}`),
    /unknown message body/,
  );
  assert.doesNotMatch(
    JSON.stringify(denied),
    /sender_denied|policy denied Claude body/,
  );
});

test("Claude bridge retains the exact reply address only for legacy frames without a session ID", async () => {
  let payload = null;
  const result = await bridge.deliverClaudeMessage(
    "legacy-coordinator",
    {
      ...parsed("11345678-2234-4234-8234-123456789abc", "legacy message"),
      fromSession: null,
    },
    {
      readRecord: () => ({
        name: "legacy-coordinator",
        threadId: "21345678-2234-4234-8234-123456789abc",
        cwd: path.resolve("."),
      }),
      withServer: async (callback) => callback({}),
      readThread: async () => ({ id: "21345678-2234-4234-8234-123456789abc" }),
      deliver: async (_client, _thread, value) => {
        payload = value;
        return {
          delivery: "started",
          messageId: value.messageId,
          turnId: "31345678-2234-4234-8234-123456789abc",
        };
      },
    },
  );
  assert.equal(result.delivery, "started");
  assert.match(payload.message, /Claude reply address: uds:\/\/\/private\/redacted\.sock/);
  assert.equal(payload.replyHandle, null);
});
