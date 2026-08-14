import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-route-cli-"));
process.env.CXMSG_STATE_DIR = stateDir;
const registry = await import(`../src/registry.js?route-cli=${Date.now()}`);
registry.writeSessionRecord({
  name: "worker",
  threadId: "71345678-1234-4234-8234-123456789abc",
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
});
