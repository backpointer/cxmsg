import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-retention-cli-"));
const cli = path.resolve("bin/cxmsg.js");

test.after(() => rmSync(stateDir, { recursive: true, force: true }));

function run(args) {
  return spawnSync(process.execPath, [cli, ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
  });
}

test("retention CLI plans safely and requires exact confirmation for mutation", () => {
  const text = run([
    "retention",
    "plan",
    "--before",
    "2026-01-01T00:00:00Z",
    "--scope",
    "all",
  ]);
  assert.equal(text.status, 0, text.stderr);
  assert.match(text.stdout, /retention plan; automatic deletion=false, explicit mutation=true/);
  assert.match(text.stdout, /plan-digest=[0-9a-f]{64}/);
  assert.match(text.stdout, /ledger\teligible=0\tblocked=0/);

  const json = run([
    "retention",
    "plan",
    "--before",
    "2026-01-01T00:00:00Z",
    "--scope",
    "bodies",
    "--json",
  ]);
  assert.equal(json.status, 0, json.stderr);
  const plan = JSON.parse(json.stdout);
  assert.equal(plan.schemaVersion, 1);
  assert.equal(plan.policy.automaticDeletion, false);
  assert.equal(plan.policy.mutationEnabled, true);
  assert.match(plan.planDigest, /^[0-9a-f]{64}$/);

  const unsafe = run([
    "retention",
    "plan",
    "--before",
    new Date().toISOString(),
    "--scope",
    "ledger",
  ]);
  assert.equal(unsafe.status, 1);
  assert.match(unsafe.stderr, /preserve at least 90 days/);

  const mutation = run(["retention", "purge", "--before", "2026-01-01T00:00:00Z"]);
  assert.equal(mutation.status, 1);
  assert.match(mutation.stderr, /requires --confirm/);
  assert.doesNotMatch(`${mutation.stdout}${mutation.stderr}`, /deleted|removed/);
});
