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
const registry = await import(`../src/registry.js?route-test=${Date.now()}`);
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
  assert.equal(quarantined.length, 3);
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
