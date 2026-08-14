import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-directory-cli-state-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-directory-cli-root-"));
const canonicalProjectRoot = realpathSync(projectRoot);
process.env.CXMSG_STATE_DIR = stateDir;
const registry = await import(`../src/registry.js?directory-cli=${Date.now()}`);
const directory = await import(`../src/node-directory.js?directory-cli=${Date.now()}`);
const jobs = await import(`../src/jobs.js?directory-cli=${Date.now()}`);
const threadId = "42345678-1234-4234-8234-123456789abc";
const successorThreadId = "52345678-1234-4234-8234-123456789abc";
registry.writeSessionRecord({
  name: "worker",
  threadId,
  cwd: projectRoot,
  createdAt: "2026-08-14T00:00:00.000Z",
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function cxmsg(...args) {
  return spawnSync(process.execPath, ["bin/cxmsg.js", ...args], {
    cwd: path.resolve("."),
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    encoding: "utf8",
  });
}

test("Directory CLI creates an explicit Project and synchronizes Codex Nodes", () => {
  const ensured = cxmsg(
    "directory",
    "project",
    "ensure",
    "hermes",
    projectRoot,
    "--json",
  );
  assert.equal(ensured.status, 0, ensured.stderr);
  const project = JSON.parse(ensured.stdout);
  assert.equal(project.routingId, "hermes");
  assert.equal(project.discoveryKind, "canonical-root");
  assert.deepEqual(project.rootAliases.map((alias) => alias.path), [canonicalProjectRoot]);

  const synchronized = cxmsg(
    "directory",
    "sync",
    "--project",
    "hermes",
    "--codex-only",
    "--json",
  );
  assert.equal(synchronized.status, 0, synchronized.stderr);
  const nodes = JSON.parse(synchronized.stdout);
  assert.equal(nodes.length, 1);
  assert.equal(nodes[0].nodeKey, `codex:${threadId}`);
  assert.equal(nodes[0].projectId, project.projectId);
  assert.equal("selectedEndpoints" in nodes[0], false);

  const listed = cxmsg("directory", "projects", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  const publicProject = JSON.parse(listed.stdout)[0];
  assert.equal("rootAliases" in publicProject, false);
  assert.equal(listed.stdout.includes(canonicalProjectRoot), false);
});

test("route binding adopts stable Directory Project and Node references", () => {
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
  const shown = cxmsg("route", "show", "worker", "--json");
  assert.equal(shown.status, 0, shown.stderr);
  const binding = JSON.parse(shown.stdout);
  assert.match(binding.projectKey, /^[0-9a-f-]{36}$/);
  assert.equal(binding.nodeKey, `codex:${threadId}`);

  const node = cxmsg(
    "directory",
    "node",
    "show",
    "codex",
    threadId,
    "--json",
  );
  assert.equal(node.status, 0, node.stderr);
  assert.equal(JSON.parse(node.stdout).nodeKey, binding.nodeKey);
});

test("Directory CLI manages explicit Cluster membership and private history", () => {
  const ensured = cxmsg(
    "directory",
    "cluster",
    "ensure",
    "hermes-auditors",
    "--json",
  );
  assert.equal(ensured.status, 0, ensured.stderr);
  const cluster = JSON.parse(ensured.stdout);
  assert.equal(cluster.routingId, "hermes-auditors");
  assert.equal(cluster.memberCount, 0);
  assert.equal("members" in cluster, false);

  const added = cxmsg(
    "directory",
    "cluster",
    "member",
    "add",
    "hermes-auditors",
    "codex",
    threadId,
    "--json",
    "--members",
  );
  assert.equal(added.status, 0, added.stderr);
  assert.deepEqual(JSON.parse(added.stdout).members, [`codex:${threadId}`]);

  writeFileSync(
    path.join(
      directory.CLUSTER_MEMBERSHIPS_DIR,
      `${cluster.clusterId}--0000000003.json`,
    ),
    `${JSON.stringify(
      {
        version: 1,
        clusterId: cluster.clusterId,
        membershipVersion: 3,
        members: [],
        changeKind: "member-removed",
        changedNodeKey: `codex:${threadId}`,
        createdAt: new Date().toISOString(),
      },
      null,
      2,
    )}\n`,
    { mode: 0o600 },
  );
  const recovered = cxmsg(
    "directory",
    "cluster",
    "recover",
    "hermes-auditors",
    "--json",
  );
  assert.equal(recovered.status, 0, recovered.stderr);
  assert.equal(JSON.parse(recovered.stdout).recovered, true);
  assert.equal(JSON.parse(recovered.stdout).cluster.membershipVersion, 3);

  const hidden = cxmsg(
    "directory",
    "cluster",
    "show",
    "hermes-auditors",
    "--json",
    "--history",
  );
  assert.equal(hidden.status, 0, hidden.stderr);
  const hiddenRecord = JSON.parse(hidden.stdout);
  assert.equal("members" in hiddenRecord, false);
  assert.equal("members" in hiddenRecord.membershipHistory[1], false);
  assert.equal("changedNodeKey" in hiddenRecord.membershipHistory[1], false);
  assert.doesNotMatch(hidden.stdout, new RegExp(threadId));
  assert.equal(hiddenRecord.membershipHistory[1].memberCount, 1);

  const listed = cxmsg("directory", "clusters", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  assert.equal(JSON.parse(listed.stdout)[0].routingId, "hermes-auditors");
  assert.equal("members" in JSON.parse(listed.stdout)[0], false);

  const retired = cxmsg(
    "directory",
    "cluster",
    "tombstone",
    "hermes-auditors",
    "--reason",
    "test-complete",
    "--json",
  );
  assert.equal(retired.status, 0, retired.stderr);
  assert.equal(JSON.parse(retired.stdout).lastMembershipVersion, 3);
  assert.equal("members" in JSON.parse(retired.stdout), false);
  const tombstones = cxmsg("directory", "cluster-tombstones", "--json");
  assert.equal(tombstones.status, 0, tombstones.stderr);
  assert.equal(JSON.parse(tombstones.stdout).length, 1);
});

test("Directory CLI exposes explicit Tombstone and successor lifecycle", async () => {
  const project = directory.findProjectByRoutingId("hermes");
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: successorThreadId,
    displayName: "successor",
    projectId: project.projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${successorThreadId}`,
      generation: 1,
      status: "reachable",
      address: "uds:/private/endpoint-history.sock",
    },
  });

  const hiddenHistory = cxmsg(
    "directory",
    "node",
    "show",
    "codex",
    successorThreadId,
    "--json",
  );
  assert.equal(hiddenHistory.status, 0, hiddenHistory.stderr);
  assert.equal("endpointHistory" in JSON.parse(hiddenHistory.stdout), false);
  assert.doesNotMatch(hiddenHistory.stdout, /endpoint-history\.sock/);
  const explicitHistory = cxmsg(
    "directory",
    "node",
    "show",
    "codex",
    successorThreadId,
    "--history",
    "--json",
  );
  assert.equal(explicitHistory.status, 0, explicitHistory.stderr);
  assert.equal(
    JSON.parse(explicitHistory.stdout).endpointHistory[0].address,
    "uds:/private/endpoint-history.sock",
  );

  const tombstoned = cxmsg(
    "directory",
    "node",
    "tombstone",
    "codex",
    threadId,
    "--reason",
    "session-removed",
    "--json",
  );
  assert.equal(tombstoned.status, 0, tombstoned.stderr);
  const tombstone = JSON.parse(tombstoned.stdout);
  assert.equal(tombstone.nodeKey, `codex:${threadId}`);
  assert.equal("selectedEndpoints" in tombstone, false);

  const linked = cxmsg(
    "directory",
    "successor",
    "add",
    "codex",
    threadId,
    "codex",
    successorThreadId,
    "--json",
  );
  assert.equal(linked.status, 0, linked.stderr);
  assert.equal(
    JSON.parse(linked.stdout).successorNodeKey,
    `codex:${successorThreadId}`,
  );

  const tombstones = cxmsg("directory", "tombstones", "--json");
  assert.equal(tombstones.status, 0, tombstones.stderr);
  assert.equal(JSON.parse(tombstones.stdout).length, 1);
  assert.doesNotMatch(tombstones.stdout, /app-server:/);

  const successors = cxmsg("directory", "successors", "--json");
  assert.equal(successors.status, 0, successors.stderr);
  assert.equal(JSON.parse(successors.stdout).length, 1);
});

test("Directory CLI explicitly classifies strongly evidenced legacy Execution Threads", () => {
  const legacyJobId = "62345678-1234-4234-8234-123456789abc";
  const legacyExecutionThreadId = "72345678-1234-4234-8234-123456789abc";
  const legacyTurnId = "82345678-1234-4234-8234-123456789abc";
  const created = jobs.createJob({
    jobId: legacyJobId,
    from: "coordinator",
    target: "successor",
    targetThreadId: successorThreadId,
    threadId: successorThreadId,
    task: "private legacy body",
    execution: "fork",
  });
  jobs.writeJob({
    ...created,
    threadId: legacyExecutionThreadId,
    turnId: legacyTurnId,
    status: "completed",
    result: "private legacy result",
  });

  const synchronized = cxmsg("directory", "execution", "sync", "--json");
  assert.equal(synchronized.status, 0, synchronized.stderr);
  const classified = JSON.parse(synchronized.stdout);
  assert.equal(classified.length, 1);
  assert.equal(classified[0].threadId, legacyExecutionThreadId);
  assert.equal(classified[0].sourceNodeKey, `codex:${successorThreadId}`);
  assert.equal(classified[0].creationMode, "legacy-observed");
  assert.doesNotMatch(synchronized.stdout, /private legacy/);

  const shown = cxmsg(
    "directory",
    "execution-thread",
    "show",
    legacyExecutionThreadId,
    "--json",
  );
  assert.equal(shown.status, 0, shown.stderr);
  assert.equal(JSON.parse(shown.stdout).jobId, legacyJobId);

  const listed = cxmsg("directory", "execution-threads", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  assert.equal(JSON.parse(listed.stdout).length, 1);
});
