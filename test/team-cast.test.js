import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-cast-"));
const firstRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-project-"));
const secondRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-project-"));
process.env.CXMSG_STATE_DIR = stateDir;

const directory = await import(`../src/node-directory.js?team=${Date.now()}`);
const conversations = await import(`../src/conversations.js?team=${Date.now()}`);
const groups = await import(`../src/group-conversations.js?team=${Date.now()}`);
const registry = await import(`../src/registry.js?team=${Date.now()}`);
const routes = await import(`../src/route-admission.js?team=${Date.now()}`);
const teams = await import(`../src/team-cast.js?team=${Date.now()}`);

const ids = {
  firstProject: "10345678-5234-4234-8234-123456789abc",
  secondProject: "20345678-5234-4234-8234-123456789abc",
  sender: "30345678-5234-4234-8234-123456789abc",
  first: "40345678-5234-4234-8234-123456789abc",
  second: "50345678-5234-4234-8234-123456789abc",
  cross: "60345678-5234-4234-8234-123456789abc",
  group: "70345678-5234-4234-8234-123456789abc",
  cluster: "80345678-5234-4234-8234-123456789abc",
  crossCluster: "90345678-5234-4234-8234-123456789abc",
  groupPlan: "a0345678-5234-4234-8234-123456789abc",
  directPlan: "b0345678-5234-4234-8234-123456789abc",
  clusterPlan: "c0345678-5234-4234-8234-123456789abc",
  rolePlan: "d0345678-5234-4234-8234-123456789abc",
  crossPlan: "e0345678-5234-4234-8234-123456789abc",
  tombstonePlan: "f0345678-5234-4234-8234-123456789abc",
  cliPlan: "11345678-6234-4234-8234-123456789abc",
};
const keys = Object.fromEntries(
  ["sender", "first", "second", "cross"].map((name) => [
    name,
    `codex:${ids[name]}`,
  ]),
);

await directory.ensureProject({
  routingId: "team-first",
  root: firstRoot,
  projectId: ids.firstProject,
});
await directory.ensureProject({
  routingId: "team-second",
  root: secondRoot,
  projectId: ids.secondProject,
});
for (const name of ["sender", "first", "second", "cross"]) {
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: ids[name],
    displayName: name,
    projectId: name === "cross" ? ids.secondProject : ids.firstProject,
  });
}
const direct = await conversations.ensureDirectConversation(keys.sender, keys.first);
await groups.ensureGroupConversation({
  conversationId: ids.group,
  label: "team-reviewers",
  members: [keys.sender, keys.first, keys.second],
});
await directory.ensureCluster({
  routingId: "team-cluster",
  clusterId: ids.cluster,
});
for (const memberNodeKey of [keys.sender, keys.first, keys.second]) {
  await directory.addClusterMember({ cluster: ids.cluster, memberNodeKey });
}
await directory.ensureCluster({
  routingId: "cross-cluster",
  clusterId: ids.crossCluster,
});
for (const memberNodeKey of [keys.sender, keys.first, keys.cross]) {
  await directory.addClusterMember({ cluster: ids.crossCluster, memberNodeKey });
}
for (const name of ["sender", "first", "second"]) {
  registry.writeSessionRecord({
    name: `team-${name}`,
    threadId: ids[name],
    cwd: firstRoot,
  });
  routes.writeRouteBinding({
    sessionName: `team-${name}`,
    threadId: ids[name],
    projectId: "team-first",
    projectKey: ids.firstProject,
    nodeKey: keys[name],
    role: name === "sender" ? "coordinator" : "reviewer",
  });
}

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(firstRoot, { recursive: true, force: true });
  rmSync(secondRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("Conversation Team Cast plans freeze and redact recipients", async () => {
  const result = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: { kind: "conversation", id: ids.group },
    planId: ids.groupPlan,
  });
  assert.equal(result.created, true);
  assert.deepEqual(result.plan.recipientNodeKeys, [keys.first, keys.second].sort());
  assert.equal(result.plan.selector.conversationKind, "group");
  assert.equal(result.plan.selector.membershipVersion, 1);
  const output = teams.publicTeamCastPlan(result.plan);
  assert.equal(output.recipientCount, 2);
  assert.equal(output.estimatedWakeTurns, 2);
  assert.equal("recipientNodeKeys" in output, false);
  assert.deepEqual(
    teams.publicTeamCastPlan(result.plan, { includeRecipients: true })
      .recipientNodeKeys,
    [keys.first, keys.second].sort(),
  );
  const repeated = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: { kind: "conversation", id: ids.group },
    planId: ids.groupPlan,
  });
  assert.equal(repeated.created, false);
  assert.equal(statSync(teams.TEAM_CAST_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(path.join(teams.TEAM_CAST_PLANS_DIR, `${ids.groupPlan}.json`)).mode &
      0o777,
    0o600,
  );
});

test("Direct and Cluster selectors preserve immutable selector evidence", async () => {
  const directPlan = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: {
      kind: "conversation",
      id: direct.conversation.conversationId,
    },
    planId: ids.directPlan,
  });
  assert.deepEqual(directPlan.plan.recipientNodeKeys, [keys.first]);
  assert.equal(directPlan.plan.selector.conversationKind, "direct");

  const clusterPlan = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: { kind: "cluster", id: "team-cluster" },
    planId: ids.clusterPlan,
  });
  assert.deepEqual(clusterPlan.plan.recipientNodeKeys, [keys.first, keys.second].sort());
  assert.equal(clusterPlan.plan.selector.clusterId, ids.cluster);
  assert.equal(clusterPlan.plan.selector.membershipVersion, 4);
});

test("CLI exposes a redacted resolution plan and starts no delivery", () => {
  const result = spawnSync(
    process.execPath,
    [
      path.resolve("bin/cxmsg.js"),
      "team",
      "resolve",
      "--from",
      keys.sender,
      "--conversation",
      ids.group,
      "--plan-id",
      ids.cliPlan,
      "--json",
    ],
    {
      encoding: "utf8",
      env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  assert.equal(output.deliveryStarted, false);
  assert.equal(output.recipientCount, 2);
  assert.equal("recipientNodeKeys" in output, false);
});

test("Project-role selector requires exact stable bindings", async () => {
  const result = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: {
      kind: "project-role",
      projectId: ids.firstProject,
      role: "reviewer",
    },
    planId: ids.rolePlan,
  });
  assert.deepEqual(result.plan.recipientNodeKeys, [keys.first, keys.second].sort());
  assert.match(result.plan.selector.bindingSetSha256, /^[0-9a-f]{64}$/);

  registry.writeSessionRecord({
    name: "team-first-alias",
    threadId: ids.first,
    cwd: firstRoot,
  });
  routes.writeRouteBinding({
    sessionName: "team-first-alias",
    threadId: ids.first,
    projectId: "team-first",
    projectKey: ids.firstProject,
    nodeKey: keys.first,
    role: "reviewer",
  });
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: {
        kind: "project-role",
        projectId: ids.firstProject,
        role: "reviewer",
      },
    }),
    /ambiguous/,
  );
});

test("a plan id cannot be rebound to another selector", async () => {
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: { kind: "conversation", id: direct.conversation.conversationId },
      planId: ids.groupPlan,
    }),
    /idempotency conflict/,
  );
});

test("cross-Project and Tombstoned recipients fail before a plan is written", async () => {
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: { kind: "cluster", id: ids.crossCluster },
      planId: ids.crossPlan,
    }),
    /crosses the sender Project/,
  );
  assert.equal(teams.readTeamCastPlan(ids.crossPlan), null);

  await directory.tombstoneNode("codex", ids.second, { reason: "test" });
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: { kind: "conversation", id: ids.group },
      planId: ids.tombstonePlan,
    }),
    /Tombstoned/,
  );
  assert.equal(teams.readTeamCastPlan(ids.tombstonePlan), null);
});
