import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-inbound-cli-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-inbound-project-"));
process.env.CXMSG_STATE_DIR = stateDir;

const directory = await import(`../src/node-directory.js?inbound-cli=${Date.now()}`);
const registry = await import(`../src/registry.js?inbound-cli=${Date.now()}`);
const inbound = await import(`../src/inbound-policy.js?inbound-cli=${Date.now()}`);
const routes = await import(`../src/route-admission.js?inbound-cli=${Date.now()}`);

const ids = {
  project: "18345678-9234-4234-8234-123456789abc",
  target: "28345678-9234-4234-8234-123456789abc",
  sender: "38345678-9234-4234-8234-123456789abc",
  message: "48345678-9234-4234-8234-123456789abc",
  invalidTarget: "58345678-9234-4234-8234-123456789abc",
  unsynchronized: "68345678-9234-4234-8234-123456789abc",
};
const targetNodeKey = `codex:${ids.target}`;
const senderNodeKey = `codex:${ids.sender}`;

function cxmsg(...args) {
  return spawnSync(process.execPath, [path.resolve("bin/cxmsg.js"), ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
  });
}

await directory.ensureProject({
  routingId: "inbound-cli",
  root: projectRoot,
  projectId: ids.project,
});
for (const [nativeId, displayName] of [
  [ids.target, "policy-target"],
  [ids.sender, "policy-sender"],
]) {
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId,
    displayName,
    projectId: ids.project,
  });
  registry.writeSessionRecord({
    name: displayName,
    threadId: nativeId,
    cwd: projectRoot,
  });
}

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("owner CLI adds, lists, deduplicates, and removes one stable sender rule", () => {
  const added = cxmsg(
    "inbound",
    "deny",
    "add",
    "policy-target",
    "--sender-node",
    senderNodeKey,
    "--json",
  );
  assert.equal(added.status, 0, added.stderr);
  const first = JSON.parse(added.stdout);
  assert.equal(first.targetNodeKey, targetNodeKey);
  assert.equal(first.rule.selectorNodeKey, senderNodeKey);
  assert.equal(first.changed, true);

  const duplicate = cxmsg(
    "inbound",
    "deny",
    "add",
    targetNodeKey,
    "--sender-node",
    senderNodeKey,
    "--json",
  );
  assert.equal(duplicate.status, 0, duplicate.stderr);
  assert.equal(JSON.parse(duplicate.stdout).changed, false);
  assert.equal(JSON.parse(duplicate.stdout).rule.ruleId, first.rule.ruleId);

  const listed = cxmsg("inbound", "deny", "list", "policy-target", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  const policies = JSON.parse(listed.stdout);
  assert.equal(policies.length, 1);
  assert.equal(policies[0].rules.length, 1);

  const removed = cxmsg(
    "inbound",
    "deny",
    "remove",
    "policy-target",
    first.rule.ruleId,
    "--json",
  );
  assert.equal(removed.status, 0, removed.stderr);
  assert.equal(JSON.parse(removed.stdout).policyDeleted, true);
  assert.equal(inbound.inboundPolicyState(targetNodeKey).state, "missing");
});

test("Project labels and unknown sender selectors remain explicit and bounded", () => {
  const project = cxmsg(
    "inbound",
    "deny",
    "add",
    "policy-target",
    "--sender-project",
    "inbound-cli",
    "--json",
  );
  assert.equal(project.status, 0, project.stderr);
  assert.equal(JSON.parse(project.stdout).rule.projectId, ids.project);

  const unknown = cxmsg(
    "inbound",
    "deny",
    "add",
    "policy-target",
    "--unknown-sender",
    "--json",
  );
  assert.equal(unknown.status, 0, unknown.stderr);
  const output = JSON.parse(unknown.stdout);
  assert.equal(output.rule.selectorKind, "unknown-sender");
  assert.equal("selectorNodeKey" in output.rule, false);

  const conflicting = cxmsg(
    "inbound",
    "deny",
    "add",
    "policy-target",
    "--unknown-sender",
    "--sender-node",
    senderNodeKey,
  );
  assert.equal(conflicting.status, 1);
  assert.match(conflicting.stderr, /accepts one selector/);
});

test("an unsynchronized target returns one exact safe synchronization command", () => {
  registry.writeSessionRecord({
    name: "policy-unsynchronized",
    threadId: ids.unsynchronized,
    cwd: projectRoot,
  });
  const result = cxmsg(
    "inbound",
    "deny",
    "add",
    "policy-unsynchronized",
    "--unknown-sender",
  );
  assert.equal(result.status, 1);
  assert.match(result.stderr, /ETARGETNODE/);
  assert.match(
    result.stderr,
    /cxmsg directory sync --project inbound-cli --codex-only/,
  );
});

test("active policy denies before dispatch and denial list exposes bounded metadata", async () => {
  let dispatches = 0;
  const outcome = await routes.routePeerMessage(
    {
      from: "policy-sender",
      target: "policy-target",
      message: "bounded denied coordination text",
      logicalMessageId: ids.message,
    },
    async () => {
      dispatches += 1;
    },
  );
  assert.equal(outcome.admissionState, "quarantined");
  assert.equal(outcome.status, "denied");
  assert.equal(dispatches, 0);

  const listed = cxmsg(
    "inbound",
    "denials",
    "list",
    "--target",
    "policy-target",
    "--json",
  );
  assert.equal(listed.status, 0, listed.stderr);
  const records = JSON.parse(listed.stdout);
  assert.equal(records.length, 1);
  assert.equal(records[0].logicalMessageId, ids.message);
  assert.equal(records[0].targetNodeKey, targetNodeKey);
  assert.equal(records[0].status, "denied");
  assert.equal("senderNodeKey" in records[0], false);
  assert.equal("body" in records[0], false);
});

test("Codex sender-visible denial remains generic and starts no App Server", () => {
  const sent = cxmsg(
    "send",
    "--from",
    "policy-sender",
    "policy-target",
    "generic rejection body",
  );
  assert.equal(sent.status, 1);
  assert.match(sent.stderr, /quarantined .*route_rejected/);
  assert.doesNotMatch(sent.stderr, /sender_denied|project_denied|ruleId/);
  assert.doesNotMatch(sent.stderr, /App Server/);
});

test("invalid policy purge requires the exact current byte digest", () => {
  mkdirSync(inbound.INBOUND_POLICIES_DIR, { recursive: true, mode: 0o700 });
  const invalidNodeKey = `codex:${ids.invalidTarget}`;
  const filename = path.join(
    inbound.INBOUND_POLICIES_DIR,
    inbound.inboundPolicyFilename(invalidNodeKey),
  );
  const bytes = Buffer.from('{"invalid":true}\n');
  writeFileSync(filename, bytes, { mode: 0o600 });
  const digest = createHash("sha256").update(bytes).digest("hex");

  const stale = cxmsg(
    "inbound",
    "policy",
    "purge",
    invalidNodeKey,
    "--confirm",
    "0".repeat(64),
  );
  assert.equal(stale.status, 1);
  assert.match(stale.stderr, /EINBOUNDPOLICYSTALE/);
  assert.equal(existsSync(filename), true);

  const purged = cxmsg(
    "inbound",
    "policy",
    "purge",
    invalidNodeKey,
    "--confirm",
    digest,
    "--json",
  );
  assert.equal(purged.status, 0, purged.stderr);
  assert.equal(JSON.parse(purged.stdout).recordSha256, digest);
  assert.equal(existsSync(filename), false);
});
