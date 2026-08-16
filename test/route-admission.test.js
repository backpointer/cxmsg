import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  appendFileSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-route-admission-"));
process.env.CXMSG_STATE_DIR = stateDir;
const routes = await import(`../src/route-admission.js?test=${Date.now()}`);
const policy = await import(`../src/delivery-policy.js?test=${Date.now()}`);
const ledger = await import(`../src/delivery-ledger.js?route-test=${Date.now()}`);
const registry = await import(`../src/registry.js?route-test=${Date.now()}`);
const directory = await import(`../src/node-directory.js?route-test=${Date.now()}`);
const inbound = await import(`../src/inbound-policy.js?route-test=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?route-test=${Date.now()}`);
registry.writeSessionRecord({
  name: "worker",
  threadId: "81345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "coordinator",
  threadId: "91345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "reconcile-worker",
  threadId: "a7345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "reply-requester",
  threadId: "b9345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "reply-responder",
  threadId: "c9345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "bound-requester",
  threadId: "d9345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "bound-responder",
  threadId: "e9345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
const policyProjectId = "7e345678-1234-4234-8234-123456789abc";
await directory.ensureProject({
  routingId: "route-policy",
  root: path.resolve("."),
  projectId: policyProjectId,
});
for (const [name, threadId] of [
  ["policy-sender", "8e345678-1234-4234-8234-123456789abc"],
  ["policy-worker", "9e345678-1234-4234-8234-123456789abc"],
  ["policy-retry-sender", "ae345678-1234-4234-8234-123456789abc"],
  ["policy-retry-worker", "be345678-1234-4234-8234-123456789abc"],
  ["policy-invalid-worker", "ce345678-1234-4234-8234-123456789abc"],
  ["policy-project-worker", "de345678-1234-4234-8234-123456789abc"],
]) {
  registry.writeSessionRecord({ name, threadId, cwd: path.resolve(".") });
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: threadId,
    displayName: name,
    projectId: policyProjectId,
  });
}
registry.writeSessionRecord({
  name: "policy-unverified-sender",
  threadId: "ee345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const ids = {
  legacy: "11345678-1234-4234-8234-123456789abc",
  admitted: "21345678-1234-4234-8234-123456789abc",
  missing: "31345678-1234-4234-8234-123456789abc",
  mismatch: "41345678-1234-4234-8234-123456789abc",
  expired: "51345678-1234-4234-8234-123456789abc",
  senderMismatch: "61345678-1234-4234-8234-123456789abc",
  senderUnbound: "b2345678-1234-4234-8234-123456789abc",
  senderProjectMismatch: "c3345678-1234-4234-8234-123456789abc",
  targetMismatch: "a1345678-1234-4234-8234-123456789abc",
  invalidSenderBinding: "d4345678-1234-4234-8234-123456789abc",
  invalidTargetBinding: "e5345678-1234-4234-8234-123456789abc",
  reconcileAccepted: "f6345678-1234-4234-8234-123456789abc",
  reconcileUnknown: "a7345678-2234-4234-8234-123456789abc",
  reconcileGrace: "b8345678-2234-4234-8234-123456789abc",
  retainedBody: "d0345678-2234-4234-8234-123456789abc",
  duplicateStore: "e2345678-2234-4234-8234-123456789abc",
  corruptBlocked: "f3345678-2234-4234-8234-123456789abc",
  scheduled: "a4345678-2234-4234-8234-123456789abc",
  afterTurn: "b4345678-2234-4234-8234-123456789abc",
  afterJob: "c4345678-2234-4234-8234-123456789abc",
  crossProjectTrigger: "f4345678-2234-4234-8234-123456789abc",
  replyOriginal: "d9345678-1234-4234-8234-123456789abc",
  reply: "e9345678-1234-4234-8234-123456789abc",
  replyLegacy: "f9345678-1234-4234-8234-123456789abc",
  routedReplyOriginal: "ac345678-1234-4234-8234-123456789abc",
  routedReply: "bc345678-1234-4234-8234-123456789abc",
  claudeReplyOriginal: "cc345678-1234-4234-8234-123456789abc",
  claudeReply: "dc345678-1234-4234-8234-123456789abc",
  retryAccepted: "ed345678-1234-4234-8234-123456789abc",
  retryFailed: "fd345678-1234-4234-8234-123456789abc",
  retryUnknown: "0e345678-1234-4234-8234-123456789abc",
  retryExpired: "2e345678-1234-4234-8234-123456789abc",
  retryCrash: "3e345678-1234-4234-8234-123456789abc",
  policyDenied: "4e345678-1234-4234-8234-123456789abc",
  policyRetry: "5e345678-1234-4234-8234-123456789abc",
  policyReplyOriginal: "6e345678-1234-4234-8234-123456789abc",
  policyReply: "6f345678-1234-4234-8234-123456789abc",
  policyInvalid: "7a345678-1234-4234-8234-123456789abc",
  policyInactive: "7b345678-1234-4234-8234-123456789abc",
  policyScheduled: "7d345678-1234-4234-8234-123456789abc",
  policyUnverifiable: "7e345678-2234-4234-8234-123456789abc",
};

function route(messageId, changes = {}) {
  return {
    schema_version: 1,
    project_id: "hermes",
    target_role: "auditor",
    logical_message_id: messageId,
    payload_type: "coordination",
    wake_policy: "immediate",
    ...changes,
  };
}

async function ensurePolicySenderDeny() {
  return inbound.upsertInboundDenyRule({
    targetNodeKey: directory.nodeKey(
      "codex",
      "9e345678-1234-4234-8234-123456789abc",
    ),
    selectorKind: "sender-node",
    selectorValue: directory.nodeKey(
      "codex",
      "8e345678-1234-4234-8234-123456789abc",
    ),
  });
}

test("unbound targets remain compatible and logical messages wake at most once", async () => {
  let dispatches = 0;
  const first = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "legacy-worker",
      message: "hello",
      logicalMessageId: ids.legacy,
    },
    async () => {
      dispatches += 1;
      return {
        delivery: "started",
        turnId: "01345678-1234-4234-8234-123456789abc",
      };
    },
  );
  const duplicate = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "legacy-worker",
      message: "hello",
      logicalMessageId: ids.legacy,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "turn-duplicate" };
    },
  );

  assert.equal(first.admissionState, "admitted");
  assert.equal(first.reason, "legacy-unbound");
  assert.equal(duplicate.deduplicated, true);
  assert.equal(duplicate.status, "turn_started");
  assert.equal(dispatches, 1);
});

test("Inbound Policy denial is metadata-only, idempotent, and starts zero attempts", async () => {
  const senderNodeKey = directory.nodeKey(
    "codex",
    "8e345678-1234-4234-8234-123456789abc",
  );
  const targetNodeKey = directory.nodeKey(
    "codex",
    "9e345678-1234-4234-8234-123456789abc",
  );
  await ensurePolicySenderDeny();
  const events = [];
  let dispatches = 0;
  const deliver = () => {
    dispatches += 1;
    throw new Error("policy-denied message must not dispatch");
  };
  const input = {
    from: "policy-sender",
    target: "policy-worker",
    message: "private policy-denied body",
    logicalMessageId: ids.policyDenied,
  };
  const first = await routes.routePeerMessage(input, deliver, {
    log: async (event) => events.push(event),
    policyEvaluator: inbound.evaluateInboundPolicy,
  });
  const duplicate = await routes.routePeerMessage(input, deliver, {
    policyEvaluator: inbound.evaluateInboundPolicy,
  });

  assert.equal(first.admissionState, "quarantined");
  assert.equal(first.reason, "route_rejected");
  assert.equal(first.status, "denied");
  assert.equal(first.denialOrigin, undefined);
  assert.equal(duplicate.deduplicated, true);
  assert.equal(dispatches, 0);
  assert.deepEqual(events, [
    {
      kind: "inbound-policy",
      phase: "decision",
      correlationId: ids.policyDenied,
      target: "policy-worker",
      outcome: "denied",
      errorCode: "EINBOUNDDENIED",
      denialOrigin: "inbound-policy",
    },
  ]);

  const routed = routes.readRouteDelivery(ids.policyDenied);
  assert.equal(routed.admissionState, "denied");
  assert.equal(routed.status, "denied");
  assert.equal(routed.attemptCount, 0);
  assert.equal(routed.contentRef, null);
  assert.equal(routed.inboundPolicy.reason, "sender_denied");
  assert.equal(routed.inboundPolicy.senderNodeKey, senderNodeKey);
  assert.equal(routed.inboundPolicy.targetNodeKey, targetNodeKey);
  assert.throws(
    () => bodies.messageBodyInfo(`cxmsg-message:${ids.policyDenied}`),
    /unknown message body/,
  );
  assert.equal(
    routes.listQuarantine().some(
      (record) => record.logicalMessageId === ids.policyDenied,
    ),
    false,
  );
  await assert.rejects(
    routes.routePeerMessage(
      { ...input, message: "changed denied body" },
      deliver,
      { policyEvaluator: inbound.evaluateInboundPolicy },
    ),
    /idempotency conflict/,
  );
});

test("Slice 2 integration remains inactive without the cross-path feature gate", async () => {
  await ensurePolicySenderDeny();
  assert.equal(inbound.INBOUND_POLICY_FEATURE_ACTIVE, false);
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "policy-sender",
      target: "policy-worker",
      message: "inactive policy integration baseline",
      logicalMessageId: ids.policyInactive,
    },
    async () => {
      dispatches += 1;
      return {
        delivery: "started",
        turnId: "7c345678-1234-4234-8234-123456789abc",
      };
    },
  );
  assert.equal(outcome.admissionState, "admitted");
  assert.equal(outcome.status, "turn_started");
  assert.equal(dispatches, 1);
});

test("initial scheduled ingress is denied before Trigger validation or queueing", async () => {
  await ensurePolicySenderDeny();
  let triggerChecks = 0;
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "policy-sender",
      target: "policy-worker",
      message: "scheduled body denied before queueing",
      logicalMessageId: ids.policyScheduled,
      route: route(ids.policyScheduled, {
        wake_policy: "after-job",
        expiry: new Date(Date.now() + 60_000).toISOString(),
        trigger_job_id: "8d345678-1234-4234-8234-123456789abc",
      }),
    },
    async () => {
      dispatches += 1;
    },
    {
      policyEvaluator: inbound.evaluateInboundPolicy,
      validateTrigger: async () => {
        triggerChecks += 1;
      },
    },
  );

  assert.equal(outcome.status, "denied");
  assert.equal(triggerChecks, 0);
  assert.equal(dispatches, 0);
  const retained = routes.readRouteDelivery(ids.policyScheduled);
  assert.equal(retained.status, "denied");
  assert.equal(retained.attemptCount, 0);
  assert.equal(retained.claim, null);
  assert.equal(retained.contentRef, null);
});

test("a direct reply cannot bypass the recipient Inbound Policy", async () => {
  await ensurePolicySenderDeny();
  let replyHandle = null;
  await routes.routePeerMessage(
    {
      from: "policy-worker",
      target: "policy-sender",
      message: "request a policy-filtered reply",
      logicalMessageId: ids.policyReplyOriginal,
    },
    async ({ replyHandle: handle }) => {
      replyHandle = handle;
      return {
        delivery: "started",
        turnId: "7f345678-1234-4234-8234-123456789abc",
      };
    },
    { policyEvaluator: inbound.evaluateInboundPolicy },
  );
  assert.match(replyHandle, /^m:/);
  const reply = routes.planPeerReply({
    from: "policy-sender",
    replyToMessageId: replyHandle,
    logicalMessageId: ids.policyReply,
  });
  let dispatches = 0;
  const denied = await routes.routePeerMessage(
    { ...reply, message: "reply denied by stable sender Node" },
    async () => {
      dispatches += 1;
    },
    { policyEvaluator: inbound.evaluateInboundPolicy },
  );

  assert.equal(denied.admissionState, "quarantined");
  assert.equal(denied.status, "denied");
  assert.equal(dispatches, 0);
  const retained = routes.readRouteDelivery(ids.policyReply);
  assert.equal(retained.replyToMessageId, ids.policyReplyOriginal);
  assert.equal(retained.inboundPolicy.reason, "sender_denied");
  assert.equal(retained.replyHandle, null);
});

test("an invalid Inbound Policy fails closed before body retention or dispatch", async () => {
  const targetNodeKey = directory.nodeKey(
    "codex",
    "ce345678-1234-4234-8234-123456789abc",
  );
  await inbound.upsertInboundDenyRule({
    targetNodeKey,
    selectorKind: "unknown-sender",
  });
  writeFileSync(
    path.join(
      inbound.INBOUND_POLICIES_DIR,
      inbound.inboundPolicyFilename(targetNodeKey),
    ),
    "{}\n",
  );
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "policy-sender",
      target: "policy-invalid-worker",
      message: "body must not survive invalid policy",
      logicalMessageId: ids.policyInvalid,
    },
    async () => {
      dispatches += 1;
    },
    { policyEvaluator: inbound.evaluateInboundPolicy },
  );

  assert.equal(outcome.admissionState, "quarantined");
  assert.equal(outcome.reason, "route_rejected");
  assert.equal(dispatches, 0);
  const retained = routes.readRouteDelivery(ids.policyInvalid);
  assert.equal(retained.inboundPolicy.reason, "policy_invalid");
  assert.equal(retained.contentRef, null);
  assert.equal(retained.attemptCount, 0);
  assert.throws(
    () => bodies.messageBodyInfo(`cxmsg-message:${ids.policyInvalid}`),
    /unknown message body/,
  );
  await inbound.purgeInboundPolicyRecord({
    targetNodeKey,
    confirmSha256: createHash("sha256").update("{}\n").digest("hex"),
  });
});

test("a sender-Project rule fails closed when a claimed sender Node is unverifiable", async () => {
  const targetNodeKey = directory.nodeKey(
    "codex",
    "de345678-1234-4234-8234-123456789abc",
  );
  await inbound.upsertInboundDenyRule({
    targetNodeKey,
    selectorKind: "sender-project",
    selectorValue: policyProjectId,
  });
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "policy-unverified-sender",
      target: "policy-project-worker",
      message: "unverifiable sender claim",
      logicalMessageId: ids.policyUnverifiable,
    },
    async () => {
      dispatches += 1;
    },
    { policyEvaluator: inbound.evaluateInboundPolicy },
  );

  assert.equal(outcome.status, "denied");
  assert.equal(dispatches, 0);
  const retained = routes.readRouteDelivery(ids.policyUnverifiable);
  assert.equal(retained.inboundPolicy.reason, "identity_unverifiable");
  assert.equal(retained.inboundPolicy.senderNodeKey, null);
  assert.equal(retained.senderNodeKey, undefined);
  assert.match(retained.senderIdentitySha256, /^[0-9a-f]{64}$/);
});

const negativeAcceptance = (error) =>
  error?.code === "ESTALEACTIVE"
    ? {
        reason: "expected_turn_mismatch",
        errorCode: "EEXPECTEDTURNMISMATCH",
        contract: "codex-app-server/0.147.0",
      }
    : null;

async function createRetryable(messageId, message) {
  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "coordinator",
        target: "worker",
        message,
        logicalMessageId: messageId,
      },
      async () => {
        throw Object.assign(new Error("active turn changed"), {
          code: "ESTALEACTIVE",
        });
      },
      { classifyRejection: negativeAcceptance },
    ),
    /active turn changed/,
  );
  const record = routes.readRouteDelivery(messageId);
  assert.equal(record.status, "retryable");
  assert.equal(record.attemptCount, 1);
  assert.equal(record.contentRef, `cxmsg-message:${messageId}`);
}

test("a proven rejection permits exactly one explicit retry with the retained body", async () => {
  await createRetryable(ids.retryAccepted, "retry this exact body");
  await assert.rejects(
    routes.retryRouteDelivery(ids.retryAccepted, async () => ({}), {
      classifyRejection: negativeAcceptance,
      now: () => Date.parse(routes.readRouteDelivery(ids.retryAccepted).updatedAt),
    }),
    /backoff has not elapsed/,
  );
  assert.equal(routes.readRouteDelivery(ids.retryAccepted).attemptCount, 1);
  let dispatches = 0;
  const outcome = await routes.retryRouteDelivery(
    ids.retryAccepted,
    async (payload) => {
      dispatches += 1;
      assert.equal(payload.logicalMessageId, ids.retryAccepted);
      assert.equal(payload.message, "retry this exact body");
      assert.equal(payload.targetThreadId, "81345678-1234-4234-8234-123456789abc");
      return {
        delivery: "started",
        turnId: "1e345678-1234-4234-8234-123456789abc",
      };
    },
    {
      classifyRejection: negativeAcceptance,
      now: () =>
        Date.parse(routes.readRouteDelivery(ids.retryAccepted).updatedAt) +
        policy.ORDINARY_RETRY_MIN_DELAY_MS,
    },
  );
  assert.equal(outcome.status, "turn_started");
  assert.equal(outcome.attemptCount, 2);
  assert.equal(dispatches, 1);
  await assert.rejects(
    routes.retryRouteDelivery(ids.retryAccepted, async () => ({})),
    /not eligible/,
  );
});

test("a new Inbound Policy denial terminally blocks Explicit Retry without another attempt", async () => {
  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "policy-retry-sender",
        target: "policy-retry-worker",
        message: "retain this retry body",
        logicalMessageId: ids.policyRetry,
      },
      async () => {
        throw Object.assign(new Error("active turn changed"), {
          code: "ESTALEACTIVE",
        });
      },
      { classifyRejection: negativeAcceptance },
    ),
    /active turn changed/,
  );
  const before = routes.readRouteDelivery(ids.policyRetry);
  assert.equal(before.status, "retryable");
  assert.equal(before.attemptCount, 1);
  const senderNodeKey = directory.nodeKey(
    "codex",
    "ae345678-1234-4234-8234-123456789abc",
  );
  await inbound.upsertInboundDenyRule({
    targetNodeKey: directory.nodeKey(
      "codex",
      "be345678-1234-4234-8234-123456789abc",
    ),
    selectorKind: "sender-node",
    selectorValue: senderNodeKey,
  });

  let dispatches = 0;
  const outcome = await routes.retryRouteDelivery(
    ids.policyRetry,
    async () => {
      dispatches += 1;
    },
    {
      now: () =>
        Date.parse(before.updatedAt) + policy.ORDINARY_RETRY_MIN_DELAY_MS,
      policyEvaluator: inbound.evaluateInboundPolicy,
    },
  );
  assert.equal(outcome.status, "policy_denied");
  assert.equal(outcome.retryAttempted, false);
  assert.equal(outcome.attemptCount, 1);
  assert.equal(dispatches, 0);

  const retained = ledger.readDeliveryLedger(ids.policyRetry);
  assert.equal(retained.delivery.state, "policy_denied");
  assert.equal(retained.delivery.attempts.length, 1);
  assert.equal(
    retained.logicalMessage.body.contentRef,
    `cxmsg-message:${ids.policyRetry}`,
  );
  assert.equal(retained.delivery.evidence.at(-1).attemptId, null);
  assert.equal(
    retained.delivery.evidence.at(-1).inboundPolicy.reason,
    "sender_denied",
  );
  assert.equal(
    "contentRef" in retained.delivery.evidence.at(-1).inboundPolicy,
    false,
  );
});

test("a second proven rejection is failed and an ambiguous retry is unknown", async () => {
  await createRetryable(ids.retryFailed, "bounded rejection");
  const failed = await routes.retryRouteDelivery(
    ids.retryFailed,
    async () => {
      throw Object.assign(new Error("active turn changed again"), {
        code: "ESTALEACTIVE",
      });
    },
    {
      classifyRejection: negativeAcceptance,
      now: () =>
        Date.parse(routes.readRouteDelivery(ids.retryFailed).updatedAt) +
        policy.ORDINARY_RETRY_MIN_DELAY_MS,
    },
  );
  assert.equal(failed.status, "failed");
  assert.equal(failed.errorCode, "EEXPECTEDTURNMISMATCH");

  await createRetryable(ids.retryUnknown, "ambiguous retry");
  const unknown = await routes.retryRouteDelivery(
    ids.retryUnknown,
    async () => {
      throw Object.assign(new Error("socket closed after write"), { code: "EPIPE" });
    },
    {
      classifyRejection: negativeAcceptance,
      now: () =>
        Date.parse(routes.readRouteDelivery(ids.retryUnknown).updatedAt) +
        policy.ORDINARY_RETRY_MIN_DELAY_MS,
    },
  );
  assert.equal(unknown.status, "unknown");
  assert.equal(unknown.errorCode, "EPIPE");
  await assert.rejects(
    routes.retryRouteDelivery(ids.retryUnknown, async () => ({})),
    /not eligible/,
  );
});

test("an unused retry expires without another dispatch attempt", async () => {
  await createRetryable(ids.retryExpired, "expire this retry");
  const observedAt = Date.parse(routes.readRouteDelivery(ids.retryExpired).updatedAt);
  let dispatches = 0;
  const expired = await routes.retryRouteDelivery(
    ids.retryExpired,
    async () => {
      dispatches += 1;
    },
    {
      classifyRejection: negativeAcceptance,
      now: () => observedAt + policy.ORDINARY_RETRY_WINDOW_MS + 1,
    },
  );
  assert.equal(expired.status, "expired");
  assert.equal(expired.retryAttempted, false);
  assert.equal(expired.errorCode, "ERETRYEXPIRED");
  assert.equal(dispatches, 0);
});

test("a crash after the retry attempt is reconciled without a third wake", async () => {
  await createRetryable(ids.retryCrash, "recover this retry attempt");
  const retry = await ledger.beginRetryDelivery(ids.retryCrash, {
    now: new Date(
      Date.parse(routes.readRouteDelivery(ids.retryCrash).updatedAt) +
        policy.ORDINARY_RETRY_MIN_DELAY_MS,
    ).toISOString(),
  });
  assert.equal(routes.readRouteDelivery(ids.retryCrash).status, "dispatching");
  await assert.rejects(
    routes.retryRouteDelivery(ids.retryCrash, async () => ({})),
    /not eligible/,
  );
  const reconciled = await routes.reconcileRouteDelivery(
    ids.retryCrash,
    async ({ logicalMessageId, targetThreadId }) => {
      assert.equal(logicalMessageId, ids.retryCrash);
      assert.equal(targetThreadId, "81345678-1234-4234-8234-123456789abc");
      return {
        state: "accepted",
        turnId: "4e345678-1234-4234-8234-123456789abc",
        complete: true,
        pagesInspected: 1,
      };
    },
    { now: Date.now() + 60_000, dispatchingGraceMs: 0 },
  );
  assert.equal(reconciled.status, "turn_started");
  assert.equal(reconciled.attemptCount, 2);
  assert.equal(reconciled.turnId, "4e345678-1234-4234-8234-123456789abc");
  assert.equal(
    ledger.readDeliveryLedger(ids.retryCrash).delivery.attempts[1].attemptId,
    retry.attemptId,
  );
});

test("peer replies invert pinned thread identities and preserve correlation", async () => {
  await routes.routePeerMessage(
    {
      from: "reply-requester",
      target: "reply-responder",
      message: "which result passed?",
      logicalMessageId: ids.replyOriginal,
    },
    async ({ replyHandle }) => {
      assert.match(replyHandle, /^m:[0-9A-HJKMNP-TV-Z]{10}$/);
      return {
        delivery: "started",
        turnId: "da345678-1234-4234-8234-123456789abc",
      };
    },
  );

  const replyHandle = routes.readRouteDelivery(ids.replyOriginal).replyHandle;
  assert.match(replyHandle, /^m:[0-9A-HJKMNP-TV-Z]{10}$/);

  const reply = routes.planPeerReply({
    from: "reply-responder",
    replyToMessageId: replyHandle,
    logicalMessageId: ids.reply,
  });
  assert.equal(reply.replyReference, replyHandle);
  assert.equal(reply.replyToMessageId, ids.replyOriginal);
  assert.equal(reply.target, "reply-requester");
  assert.equal(reply.expectedSenderThreadId, "c9345678-1234-4234-8234-123456789abc");
  assert.equal(reply.expectedTargetThreadId, "b9345678-1234-4234-8234-123456789abc");
  assert.equal(reply.route, null);

  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    { ...reply, message: "the isolated audit passed" },
    async ({ replyToMessageId }) => {
      dispatches += 1;
      assert.equal(replyToMessageId, ids.replyOriginal);
      return {
        delivery: "started",
        turnId: "ea345678-1234-4234-8234-123456789abc",
      };
    },
  );
  assert.equal(outcome.status, "turn_started");
  assert.equal(dispatches, 1);
  assert.equal(routes.readRouteDelivery(ids.reply).replyToMessageId, ids.replyOriginal);
  const uuidReply = routes.planPeerReply({
    from: "reply-responder",
    replyToMessageId: ids.replyOriginal,
    logicalMessageId: "f9345678-2234-4234-8234-123456789abc",
  });
  assert.equal(uuidReply.replyToMessageId, ids.replyOriginal);
  assert.throws(
    () =>
      routes.planPeerReply({
        from: "reply-requester",
        replyToMessageId: replyHandle,
        logicalMessageId: "fa345678-1234-4234-8234-123456789abc",
      }),
    /unknown peer message/,
  );

  registry.writeSessionRecord({
    name: "reply-requester",
    threadId: "ab345678-1234-4234-8234-123456789abc",
    cwd: path.resolve("."),
  });
  assert.throws(
    () =>
      routes.planPeerReply({
        from: "reply-responder",
        replyToMessageId: replyHandle,
        logicalMessageId: "bb345678-1234-4234-8234-123456789abc",
      }),
    /target thread identity changed/,
  );
});

test("strict replies reject legacy messages without a pinned sender thread", async () => {
  await routes.routePeerMessage(
    {
      from: "legacy-sender",
      target: "reply-responder",
      message: "legacy request",
      logicalMessageId: ids.replyLegacy,
    },
    async () => ({
      delivery: "started",
      turnId: "ca345678-1234-4234-8234-123456789abc",
    }),
  );
  assert.equal(routes.readRouteDelivery(ids.replyLegacy).replyHandle, null);
  assert.throws(
    () =>
      routes.planPeerReply({
        from: "reply-responder",
        replyToMessageId: ids.replyLegacy,
        logicalMessageId: "cb345678-1234-4234-8234-123456789abc",
      }),
    /lacks a pinned reverse-route Node identity/,
  );
});

test("cross-runtime replies resolve a Claude Node without retaining its endpoint", async () => {
  await routes.routePeerMessage(
    {
      from: "claude-reviewer",
      target: "reply-responder",
      message: "review completed",
      logicalMessageId: ids.claudeReplyOriginal,
      senderNode: {
        runtimeKind: "claude",
        nativeId: "7c345678-1234-4234-8234-123456789abc",
      },
    },
    async ({ replyHandle }) => ({
      delivery: "started",
      turnId: "8c345678-1234-4234-8234-123456789abc",
      replyHandle,
    }),
  );
  const original = routes.readRouteDelivery(ids.claudeReplyOriginal);
  assert.match(original.replyHandle, /^m:[0-9A-HJKMNP-TV-Z]{10}$/);
  assert.equal(
    original.senderNodeKey,
    "claude:7c345678-1234-4234-8234-123456789abc",
  );
  assert.equal(JSON.stringify(original).includes("cc-socks"), false);

  const reply = routes.planPeerReply({
    from: "reply-responder",
    replyToMessageId: original.replyHandle,
    logicalMessageId: ids.claudeReply,
  });
  assert.equal(reply.targetRuntime, "claude");
  assert.equal(reply.targetNativeId, "7c345678-1234-4234-8234-123456789abc");
  assert.equal(reply.replyToMessageId, ids.claudeReplyOriginal);
  assert.equal(reply.expectedSenderThreadId, "c9345678-1234-4234-8234-123456789abc");
  assert.equal(reply.expectedTargetThreadId, null);
});

test("routed replies invert project roles and reject altered response routes", async () => {
  routes.writeRouteBinding({
    sessionName: "bound-requester",
    threadId: "d9345678-1234-4234-8234-123456789abc",
    projectId: "hermes",
    role: "implementer",
  });
  routes.writeRouteBinding({
    sessionName: "bound-responder",
    threadId: "e9345678-1234-4234-8234-123456789abc",
    projectId: "hermes",
    role: "reviewer",
  });
  await routes.routePeerMessage(
    {
      from: "bound-requester",
      target: "bound-responder",
      message: "review the pinned result",
      route: route(ids.routedReplyOriginal, {
        target_role: "reviewer",
        sender_role: "implementer",
        task_id: "TASK-REPLY",
      }),
    },
    async () => ({
      delivery: "started",
      turnId: "cc345678-1234-4234-8234-123456789abc",
    }),
  );

  const reply = routes.planPeerReply({
    from: "bound-responder",
    replyToMessageId: ids.routedReplyOriginal,
    logicalMessageId: ids.routedReply,
  });
  assert.deepEqual(reply.route, {
    schema_version: 1,
    project_id: "hermes",
    target_role: "implementer",
    logical_message_id: ids.routedReply,
    payload_type: "response",
    wake_policy: "immediate",
    sender_role: "reviewer",
    task_id: "TASK-REPLY",
  });

  await assert.rejects(
    routes.routePeerMessage(
      {
        ...reply,
        route: { ...reply.route, target_role: "coordinator" },
        message: "misrouted answer",
      },
      async () => assert.fail("an altered reply route must not dispatch"),
    ),
    /routing identity changed/,
  );
  assert.equal(routes.readRouteDelivery(ids.routedReply), null);
});

test("bound targets admit only an exact project and role route", async () => {
  const binding = routes.writeRouteBinding({
    sessionName: "worker",
    threadId: "81345678-1234-4234-8234-123456789abc",
    projectId: "hermes",
    role: "auditor",
  });
  assert.equal(routes.readRouteBinding("worker").role, "auditor");
  assert.equal(statSync(routes.ROUTE_BINDINGS_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(path.join(routes.ROUTE_BINDINGS_DIR, "worker.json")).mode & 0o777,
    0o600,
  );
  assert.equal(binding.projectId, "hermes");

  let dispatches = 0;
  const admitted = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "inspect",
      route: route(ids.admitted),
      logicalMessageId: ids.admitted,
    },
    async ({ logicalMessageId }) => {
      dispatches += 1;
      assert.equal(logicalMessageId, ids.admitted);
      return {
        delivery: "started",
        turnId: "02345678-1234-4234-8234-123456789abc",
      };
    },
  );
  assert.equal(admitted.admissionState, "admitted");
  assert.equal(admitted.reason, "binding_match");
  assert.equal(dispatches, 1);
});

test("large admitted bodies are retained by digest reference before dispatch", async () => {
  const message = "ledger body ".repeat(2_000);
  const outcome = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message,
      route: route(ids.retainedBody),
      logicalMessageId: ids.retainedBody,
    },
    async () => ({
      delivery: "started",
      turnId: "e1345678-2234-4234-8234-123456789abc",
    }),
  );
  assert.equal(outcome.status, "turn_started");
  const retained = routes.readRouteDelivery(ids.retainedBody);
  assert.equal(retained.contentRef, `cxmsg-message:${ids.retainedBody}`);
  assert.equal(retained.messageBytes, Buffer.byteLength(message, "utf8"));
  assert.equal(retained.version, 2);
});

test("when-idle routes retain even short bodies and do not dispatch immediately", async () => {
  let dispatches = 0;
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const outcome = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "deliver after the current turn",
      route: route(ids.scheduled, {
        wake_policy: "when-idle",
        expiry,
      }),
      logicalMessageId: ids.scheduled,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.status, "scheduled");
  assert.equal(dispatches, 0);
  const retained = routes.readRouteDelivery(ids.scheduled);
  assert.equal(retained.contentRef, `cxmsg-message:${ids.scheduled}`);
  assert.equal(retained.attemptCount, 0);

  const duplicate = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "deliver after the current turn",
      route: route(ids.scheduled, {
        wake_policy: "when-idle",
        expiry,
      }),
      logicalMessageId: ids.scheduled,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(duplicate.deduplicated, true);
  assert.equal(duplicate.status, "scheduled");
  assert.equal(dispatches, 0);
});

test("when-idle requires a bounded explicit expiry", () => {
  assert.throws(
    () =>
      routes.normalizeRoute(
        route("b5345678-2234-4234-8234-123456789abc", {
          wake_policy: "when-idle",
          expiry: undefined,
        }),
      ),
    /requires expiry/,
  );
  assert.throws(
    () =>
      routes.normalizeRoute(
        route("c6345678-2234-4234-8234-123456789abc", {
          wake_policy: "when-idle",
          expiry: new Date(Date.now() + routes.MAX_WHEN_IDLE_DELAY_MS + 1_000).toISOString(),
        }),
      ),
    /no more than 7 days/,
  );
});

test("after-turn and after-job require exact validated trigger identities", async () => {
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const triggerTurnId = "d4345678-2234-4234-8234-123456789abc";
  const triggerJobId = "e4345678-2234-4234-8234-123456789abc";
  assert.throws(
    () =>
      routes.normalizeRoute(
        route(ids.afterTurn, { wake_policy: "after-turn", expiry }),
      ),
    /trigger_turn_id/,
  );
  assert.throws(
    () =>
      routes.normalizeRoute(
        route(ids.afterJob, { wake_policy: "after-job", expiry }),
      ),
    /trigger_job_id/,
  );

  let validations = 0;
  const afterTurn = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "follow the exact turn",
      route: route(ids.afterTurn, {
        wake_policy: "after-turn",
        trigger_turn_id: triggerTurnId,
        expiry,
      }),
    },
    async () => assert.fail("scheduled route must not dispatch at enqueue"),
    {
      validateTrigger: async ({ route: validated, targetThreadId }) => {
        validations += 1;
        assert.equal(validated.trigger_turn_id, triggerTurnId);
        assert.equal(targetThreadId, "81345678-1234-4234-8234-123456789abc");
      },
    },
  );
  assert.equal(afterTurn.status, "scheduled");
  assert.equal(validations, 1);
  assert.equal(routes.readRouteDelivery(ids.afterTurn).contentRef, `cxmsg-message:${ids.afterTurn}`);

  await assert.rejects(
    () =>
      routes.routePeerMessage(
        {
          from: "coordinator",
          target: "worker",
          message: "follow the exact job",
          route: route(ids.afterJob, {
            wake_policy: "after-job",
            trigger_job_id: triggerJobId,
            expiry,
          }),
        },
        async () => assert.fail("scheduled route must not dispatch at enqueue"),
      ),
    /requires trigger validation/,
  );
  assert.equal(routes.readRouteDelivery(ids.afterJob), null);

  const quarantined = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "must not inspect a cross-project trigger",
      route: route(ids.crossProjectTrigger, {
        project_id: "stock",
        wake_policy: "after-job",
        trigger_job_id: triggerJobId,
        expiry,
      }),
    },
    async () => assert.fail("quarantined route must not dispatch"),
  );
  assert.equal(quarantined.admissionState, "quarantined");
  assert.equal(quarantined.reason, "project_mismatch");
});

test("missing, mismatched, and expired routes quarantine with zero dispatch", async () => {
  let dispatches = 0;
  const dispatch = async () => {
    dispatches += 1;
    return { delivery: "started", turnId: "forbidden" };
  };
  const missing = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "missing route body",
      logicalMessageId: ids.missing,
    },
    dispatch,
  );
  const mismatch = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "wrong project body",
      route: route(ids.mismatch, { project_id: "stock" }),
      logicalMessageId: ids.mismatch,
    },
    dispatch,
  );
  const expired = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "expired body",
      route: route(ids.expired, { expiry: "2020-01-01T00:00:00.000Z" }),
      logicalMessageId: ids.expired,
    },
    dispatch,
  );

  assert.equal(missing.reason, "missing_route");
  assert.equal(mismatch.reason, "project_mismatch");
  assert.equal(expired.reason, "expired");
  assert.equal(dispatches, 0);
  const quarantined = routes.listQuarantine();
  assert.equal(quarantined.length, 4);
  assert.ok(quarantined.every((record) => !("message" in record)));
  assert.ok(quarantined.every((record) => record.messageSha256));
});

test("an unbound sender cannot claim a sender role and the decision is redacted", async () => {
  let dispatches = 0;
  const events = [];
  const outcome = await routes.routePeerMessage(
    {
      from: "unbound-coordinator",
      target: "worker",
      message: "private body that must not enter the event",
      route: route(ids.senderUnbound, { sender_role: "coordinator" }),
      logicalMessageId: ids.senderUnbound,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
    { log: async (event) => events.push(event) },
  );
  assert.equal(outcome.reason, "sender_unbound");
  assert.equal(dispatches, 0);
  assert.deepEqual(events, [
    {
      kind: "route-admission",
      phase: "decision",
      correlationId: ids.senderUnbound,
      target: "worker",
      outcome: "quarantined",
      errorCode: "sender_unbound",
    },
  ]);
  assert.doesNotMatch(JSON.stringify(events), /private body/);
});

test("a bound sender cannot claim a different sender role", async () => {
  routes.writeRouteBinding({
    sessionName: "coordinator",
    threadId: "91345678-1234-4234-8234-123456789abc",
    projectId: "hermes",
    role: "coordinator",
  });
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "forged sender role",
      route: route(ids.senderMismatch, { sender_role: "reviewer" }),
      logicalMessageId: ids.senderMismatch,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.reason, "sender_role_mismatch");
  assert.equal(dispatches, 0);
});

test("a bound sender role cannot be asserted across a different Project route", async () => {
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "cross-project sender claim",
      route: route(ids.senderProjectMismatch, {
        project_id: "stock",
        sender_role: "coordinator",
      }),
      logicalMessageId: ids.senderProjectMismatch,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.reason, "sender_project_mismatch");
  assert.equal(dispatches, 0);
});

test("a binding does not transfer to a replacement thread with the same name", async () => {
  registry.writeSessionRecord({
    name: "worker",
    threadId: "b1345678-1234-4234-8234-123456789abc",
    cwd: path.resolve("."),
  });
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "worker",
      message: "must re-bind",
      route: route(ids.targetMismatch),
      logicalMessageId: ids.targetMismatch,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.reason, "target_identity_mismatch");
  assert.equal(dispatches, 0);
});

test("logical message conflicts fail and typed envelopes parse explicitly", async () => {
  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "coordinator",
        target: "legacy-worker",
        message: "different",
        logicalMessageId: ids.legacy,
      },
      async () => ({ delivery: "started", turnId: "never" }),
    ),
    /idempotency conflict/,
  );

  const parsed = routes.parseTypedPeerEnvelope(
    JSON.stringify({
      protocol: "cxmsg-route/1",
      ...route("71345678-1234-4234-8234-123456789abc"),
      message: "typed body",
    }),
  );
  assert.equal(parsed.message, "typed body");
  assert.equal(parsed.route.project_id, "hermes");
  assert.equal(routes.parseTypedPeerEnvelope('{"message":"ordinary json"}'), null);
});

test("a symlink sender binding is invalid rather than an unbound role assertion", async () => {
  symlinkSync(
    path.join(routes.ROUTE_BINDINGS_DIR, "coordinator.json"),
    path.join(routes.ROUTE_BINDINGS_DIR, "symlink-sender.json"),
  );
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "symlink-sender",
      target: "worker",
      message: "symlink sender binding",
      route: route(ids.invalidSenderBinding, { sender_role: "coordinator" }),
      logicalMessageId: ids.invalidSenderBinding,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.reason, "sender_binding_invalid");
  assert.equal(dispatches, 0);
});

test("an existing malformed target binding fails closed before context injection", async () => {
  const bindingFile = path.join(routes.ROUTE_BINDINGS_DIR, "worker.json");
  writeFileSync(
    bindingFile,
    `${JSON.stringify({ version: 1, sessionName: "worker", role: "" })}\n`,
    { mode: 0o600 },
  );
  assert.equal(routes.routeBindingState("worker").state, "invalid");
  assert.equal(routes.routeBindingState("missing-worker").state, "missing");
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "unbound-sender",
      target: "worker",
      message: "must remain outside model context",
      route: route(ids.invalidTargetBinding),
      logicalMessageId: ids.invalidTargetBinding,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.reason, "binding_invalid");
  assert.equal(dispatches, 0);
});

test("Route Delivery reconciliation strengthens only positive App Server acceptance evidence", async () => {
  const privateMessage = "reconciliation body must not enter logs";
  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "coordinator",
        target: "reconcile-worker",
        message: privateMessage,
        logicalMessageId: ids.reconcileAccepted,
      },
      async () => {
        const error = new Error("connection closed after an uncertain write");
        error.code = "EPIPE";
        throw error;
      },
    ),
    /uncertain write/,
  );
  const uncertain = routes.readRouteDelivery(ids.reconcileAccepted);
  assert.equal(uncertain.status, "unknown");
  assert.equal(
    uncertain.targetThreadId,
    "a7345678-1234-4234-8234-123456789abc",
  );

  const events = [];
  const accepted = await routes.reconcileRouteDelivery(
    ids.reconcileAccepted,
    async ({ logicalMessageId, targetThreadId }) => {
      assert.equal(logicalMessageId, ids.reconcileAccepted);
      assert.equal(targetThreadId, uncertain.targetThreadId);
      return {
        state: "accepted",
        turnId: "b8345678-1234-4234-8234-123456789abc",
        pagesInspected: 2,
      };
    },
    { log: async (event) => events.push(event) },
  );
  assert.equal(accepted.status, "turn_started");
  assert.equal(accepted.delivery, "reconciled");
  assert.equal(accepted.reconciled, true);
  assert.equal(accepted.turnId, "b8345678-1234-4234-8234-123456789abc");
  assert.doesNotMatch(JSON.stringify(events), /reconciliation body/);

  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "coordinator",
        target: "reconcile-worker",
        message: "another private body",
        logicalMessageId: ids.reconcileUnknown,
      },
      async () => {
        throw Object.assign(new Error("uncertain"), { code: "ECONNRESET" });
      },
    ),
    /uncertain/,
  );
  const notObserved = await routes.reconcileRouteDelivery(
    ids.reconcileUnknown,
    async () => ({
      state: "not-observed",
      complete: true,
      pagesInspected: 1,
    }),
    { log: async () => {} },
  );
  assert.equal(notObserved.status, "unknown");
  assert.equal(notObserved.reconciled, false);
  assert.equal(
    routes.readRouteDelivery(ids.reconcileUnknown).errorCode,
    "EACCEPTANCEUNVERIFIED",
  );

  let releaseDispatch;
  const dispatchGate = new Promise((resolve) => {
    releaseDispatch = resolve;
  });
  const activeDispatch = routes.routePeerMessage(
    {
      from: "coordinator",
      target: "reconcile-worker",
      message: "active dispatch",
      logicalMessageId: ids.reconcileGrace,
    },
    async () => {
      await dispatchGate;
      return {
        delivery: "started",
        turnId: "c9345678-1234-4234-8234-123456789abc",
      };
    },
    { log: async () => {} },
  );
  while (routes.readRouteDelivery(ids.reconcileGrace)?.status !== "dispatching") {
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  let inspections = 0;
  await assert.rejects(
    routes.reconcileRouteDelivery(
      ids.reconcileGrace,
      async () => {
        inspections += 1;
        return { state: "accepted" };
      },
      { log: async () => {} },
    ),
    /active dispatch grace period/,
  );
  assert.equal(inspections, 0);
  const aged = await routes.reconcileRouteDelivery(
    ids.reconcileGrace,
    async () => {
      inspections += 1;
      return { state: "not-observed", complete: true, pagesInspected: 1 };
    },
    {
      log: async () => {},
      now: Date.now() + routes.ROUTE_RECONCILE_GRACE_MS + 1,
    },
  );
  assert.equal(aged.status, "unknown");
  assert.equal(aged.reconciled, false);
  assert.equal(inspections, 1);
  releaseDispatch();
  const completedDispatch = await activeDispatch;
  assert.equal(completedDispatch.status, "turn_started");
});

test("a Logical Message present in Ledger and legacy storage never dispatches twice", async () => {
  const message = "duplicate storage evidence";
  await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "legacy-worker",
      message,
      logicalMessageId: ids.duplicateStore,
    },
    async () => ({
      delivery: "started",
      turnId: "d4345678-2234-4234-8234-123456789abc",
    }),
    { log: async () => {} },
  );
  const now = new Date().toISOString();
  writeFileSync(
    path.join(routes.ROUTE_DELIVERIES_DIR, `${ids.duplicateStore}.json`),
    `${JSON.stringify({
      version: 1,
      logicalMessageId: ids.duplicateStore,
      from: "coordinator",
      target: "legacy-worker",
      messageBytes: Buffer.byteLength(message, "utf8"),
      messageSha256: createHash("sha256").update(message).digest("hex"),
      routeFingerprint: createHash("sha256").update(JSON.stringify(null)).digest("hex"),
      route: null,
      admissionState: "admitted",
      admissionReason: "legacy-unbound",
      status: "turn_started",
      wakeAttemptedAt: now,
      createdAt: now,
      updatedAt: now,
      turnId: "d4345678-2234-4234-8234-123456789abc",
      delivery: "started",
      errorCode: null,
    }, null, 2)}\n`,
    { mode: 0o600 },
  );

  let dispatches = 0;
  const duplicate = await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "legacy-worker",
      message,
      logicalMessageId: ids.duplicateStore,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
    { log: async () => {} },
  );
  assert.equal(duplicate.deduplicated, true);
  assert.equal(duplicate.version, undefined);
  assert.equal(dispatches, 0);
  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "coordinator",
        target: "legacy-worker",
        message: "conflicting body",
        logicalMessageId: ids.duplicateStore,
      },
      async () => {
        dispatches += 1;
        return { delivery: "started", turnId: "forbidden" };
      },
      { log: async () => {} },
    ),
    /idempotency conflict/,
  );
  assert.equal(dispatches, 0);
});

test("a complete invalid Ledger record fails closed before dispatch", async () => {
  const segments = path.join(stateDir, "delivery-ledger", "segments");
  const active = path.join(segments, readdirSync(segments).sort().at(-1));
  appendFileSync(active, '{"schemaVersion":1,"recordType":"invalid"}\n', "utf8");
  let dispatches = 0;
  await assert.rejects(
    routes.routePeerMessage(
      {
        from: "coordinator",
        target: "legacy-worker",
        message: "must not dispatch through a damaged Ledger",
        logicalMessageId: ids.corruptBlocked,
      },
      async () => {
        dispatches += 1;
        return { delivery: "started", turnId: "forbidden" };
      },
      { log: async () => {} },
    ),
    /Route Delivery failed validation/,
  );
  assert.equal(dispatches, 0);
});
