import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-route-cli-"));
process.env.CXMSG_STATE_DIR = stateDir;
const registry = await import(`../src/registry.js?route-cli=${Date.now()}`);
const routes = await import(`../src/route-admission.js?route-cli=${Date.now()}`);
registry.writeSessionRecord({
  name: "worker",
  threadId: "71345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});
registry.writeSessionRecord({
  name: "terminal-worker",
  threadId: "b8345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function cxmsg(...args) {
  return spawnSync(process.execPath, ["bin/cxmsg.js", ...args], {
    cwd: path.resolve("."),
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    encoding: "utf8",
  });
}

test("route binding commands expose explicit project and role metadata", () => {
  const bound = cxmsg(
    "route",
    "bind",
    "worker",
    "--project",
    "hermes",
    "--role",
    "auditor",
  );
  assert.equal(bound.status, 0, bound.stderr);
  assert.match(bound.stdout, /bound worker to hermes\/auditor/);

  const shown = cxmsg("route", "show", "worker", "--json");
  assert.equal(shown.status, 0, shown.stderr);
  assert.deepEqual(
    Object.fromEntries(
      Object.entries(JSON.parse(shown.stdout)).filter(([key]) =>
        ["sessionName", "projectId", "role"].includes(key),
      ),
    ),
    { sessionName: "worker", projectId: "hermes", role: "auditor" },
  );

  const listed = cxmsg("route", "list", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  assert.equal(JSON.parse(listed.stdout).length, 1);
});

test("payload-type reports its routed-send requirement before message creation", () => {
  const sent = cxmsg(
    "send",
    "--from",
    "coordinator",
    "--payload-type",
    "artifact",
    "--",
    "terminal-worker",
    "bounded artifact pointer",
  );
  assert.equal(sent.status, 1);
  assert.match(
    sent.stderr,
    /--payload-type is a routed-send option.*--project.*--target-role/,
  );
  assert.equal(routes.listQuarantine().length, 0);
});

test("unknown Codex targets explain the runtime namespace without creating a message", () => {
  const quarantineCount = routes.listQuarantine().length;
  const sent = cxmsg(
    "send",
    "--from",
    "coordinator",
    "claude-reviewer",
    "bounded review request",
  );
  assert.equal(sent.status, 1);
  assert.match(sent.stderr, /unknown Codex session: claude-reviewer/);
  assert.match(sent.stderr, /code=ETARGETRUNTIME/);
  assert.match(sent.stderr, /cxmsg peers/);
  assert.match(sent.stderr, /cxmsg claude peers/);
  assert.match(sent.stderr, /cxmsg claude send --from coordinator/);
  assert.match(sent.stderr, /codex-<peer>.*not Codex targets/);
  assert.doesNotMatch(sent.stderr, /App Server/);
  assert.equal(routes.listQuarantine().length, quarantineCount);
});

test("a bound target quarantines an unscoped CLI send before App Server access", () => {
  const sent = cxmsg("send", "--from", "coordinator", "worker", "unscoped body");
  assert.equal(sent.status, 1);
  assert.match(sent.stderr, /quarantined .* missing_route/);
  assert.doesNotMatch(sent.stderr, /App Server/);

  const listed = cxmsg("quarantine", "list", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  const records = JSON.parse(listed.stdout);
  assert.equal(records.length, 1);
  assert.equal(records[0].reason, "missing_route");
  assert.equal("message" in records[0], false);
  assert.ok(records[0].messageSha256);
  const events = readFileSync(path.join(stateDir, "events.jsonl"), "utf8");
  assert.match(events, /"kind":"route-admission"/);
  assert.match(events, /"outcome":"quarantined"/);
  assert.doesNotMatch(events, /unscoped body/);
});

test("routed CLI send supports an option delimiter and rejects an unbound claimed role", () => {
  const messageId = "c2345678-1234-4234-8234-123456789abc";
  const sent = cxmsg(
    "send",
    "--from",
    "unbound-coordinator",
    "--project",
    "hermes",
    "--target-role",
    "auditor",
    "--sender-role",
    "coordinator",
    "--logical-message-id",
    messageId,
    "--",
    "worker",
    "--prefixed body",
  );
  assert.equal(sent.status, 1);
  assert.match(sent.stderr, new RegExp(`quarantined ${messageId}.*sender_unbound`));
  assert.doesNotMatch(sent.stderr, /App Server/);
});

test("CLI reports an existing invalid binding and send fails closed", () => {
  writeFileSync(
    path.join(stateDir, "route-bindings", "worker.json"),
    '{"version":1,"sessionName":"worker","role":""}\n',
    { mode: 0o600 },
  );
  const shown = cxmsg("route", "show", "worker", "--json");
  assert.equal(shown.status, 1);
  assert.match(shown.stderr, /route binding .* is invalid/);

  const sent = cxmsg("send", "--from", "coordinator", "worker", "blocked body");
  assert.equal(sent.status, 1);
  assert.match(sent.stderr, /quarantined .*binding_invalid/);
  assert.doesNotMatch(sent.stderr, /App Server/);
});

test("route reconcile reports already-confirmed Delivery without starting App Server", async () => {
  const logicalMessageId = "c9345678-1234-4234-8234-123456789abc";
  await routes.routePeerMessage(
    {
      from: "coordinator",
      target: "terminal-worker",
      message: "already delivered",
      logicalMessageId,
    },
    async () => ({
      delivery: "started",
      turnId: "da345678-1234-4234-8234-123456789abc",
    }),
    { log: async () => {} },
  );

  const reconciled = cxmsg("route", "reconcile", logicalMessageId, "--json");
  assert.equal(reconciled.status, 0, reconciled.stderr);
  const outcome = JSON.parse(reconciled.stdout);
  assert.equal(outcome.status, "turn_started");
  assert.equal(outcome.reconciled, false);
  assert.equal(outcome.reconciliation, "already-confirmed");
});
