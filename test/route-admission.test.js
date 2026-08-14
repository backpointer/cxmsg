import assert from "node:assert/strict";
import { mkdtempSync, rmSync, statSync } from "node:fs";
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
  targetMismatch: "a1345678-1234-4234-8234-123456789abc",
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
      return { delivery: "started", turnId: "turn-1" };
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
      return { delivery: "started", turnId: "turn-admitted" };
    },
  );
  assert.equal(admitted.admissionState, "admitted");
  assert.equal(admitted.reason, "binding_match");
  assert.equal(dispatches, 1);
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
