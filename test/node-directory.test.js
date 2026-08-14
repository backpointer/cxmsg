import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-node-directory-state-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-node-directory-root-"));
process.env.CXMSG_STATE_DIR = stateDir;
const directory = await import(`../src/node-directory.js?test=${Date.now()}`);
const jobs = await import(`../src/jobs.js?node-directory=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const projectId = "12345678-1234-4234-8234-123456789abc";
const nodeId = "22345678-1234-4234-8234-123456789abc";
const successorNodeId = "52345678-1234-4234-8234-123456789abc";
const finalNodeId = "62345678-1234-4234-8234-123456789abc";
const executionThreadId = "a2345678-1234-4234-8234-123456789abc";
const executionJobId = "b2345678-1234-4234-8234-123456789abc";
const otherProjectId = "72345678-1234-4234-8234-123456789abc";
const otherNodeId = "82345678-1234-4234-8234-123456789abc";
const reviewClusterId = "d3345678-1234-4234-8234-123456789abc";
const releaseClusterId = "e3345678-1234-4234-8234-123456789abc";
const initialRecoveryClusterId = "f6345678-1234-4234-8234-123456789abc";
const discovery = (root) => ({
  kind: "canonical-root",
  key: path.resolve(root),
  root: path.resolve(root),
});

test("Project identity is private, stable, and conservative across discovery", async () => {
  const created = await directory.ensureProject({
    routingId: "hermes",
    root: projectRoot,
    projectId,
    discover: discovery,
  });
  const repeated = await directory.ensureProject({
    routingId: "hermes",
    root: projectRoot,
    projectId: "32345678-1234-4234-8234-123456789abc",
    discover: discovery,
  });

  assert.equal(created.projectId, projectId);
  assert.equal(repeated.projectId, projectId);
  assert.equal(directory.findProjectByRoutingId("hermes").projectId, projectId);
  assert.equal(directory.listProjects().length, 1);
  assert.equal(statSync(directory.NODE_DIRECTORY_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(path.join(directory.PROJECTS_DIR, `${projectId}.json`)).mode & 0o777,
    0o600,
  );

  const publicRecord = directory.publicProject(created);
  assert.equal(publicRecord.rootCount, 1);
  assert.equal("discoveryKey" in publicRecord, false);
  assert.equal("rootAliases" in publicRecord, false);

  await assert.rejects(
    directory.ensureProject({
      routingId: "another",
      root: projectRoot,
      discover: discovery,
    }),
    /another routing identity/,
  );
  await assert.rejects(
    directory.ensureProject({
      routingId: "other-root",
      root: path.join(projectRoot, "other"),
      projectId,
      discover: discovery,
    }),
    /Project ID already belongs/,
  );
});

test("real Git worktrees resolve to one Project discovery identity", async () => {
  const repository = mkdtempSync(path.join(os.tmpdir(), "cxmsg-git-project-"));
  const worktreeParent = mkdtempSync(path.join(os.tmpdir(), "cxmsg-git-worktree-"));
  const worktree = path.join(worktreeParent, "checkout");
  const runGit = (cwd, args) => {
    const result = spawnSync("git", args, { cwd, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
  };
  try {
    runGit(repository, ["init", "--quiet"]);
    runGit(repository, ["config", "user.name", "cxmsg test"]);
    runGit(repository, ["config", "user.email", "cxmsg@example.invalid"]);
    writeFileSync(path.join(repository, "fixture.txt"), "fixture\n");
    runGit(repository, ["add", "fixture.txt"]);
    runGit(repository, ["commit", "--quiet", "-m", "fixture"]);
    runGit(repository, ["worktree", "add", "--quiet", "-b", "fixture-worktree", worktree]);

    const primary = directory.discoverProjectRoot(repository);
    const linked = directory.discoverProjectRoot(worktree);
    assert.equal(primary.kind, "git-common-dir");
    assert.equal(linked.kind, "git-common-dir");
    assert.equal(linked.key, primary.key);

    const gitProjectId = "92345678-1234-4234-8234-123456789abc";
    const created = await directory.ensureProject({
      routingId: "git-worktrees",
      root: repository,
      projectId: gitProjectId,
    });
    const repeated = await directory.ensureProject({
      routingId: "git-worktrees",
      root: worktree,
    });
    assert.equal(repeated.projectId, created.projectId);
    assert.equal(repeated.rootAliases.length, 2);
  } finally {
    rmSync(worktreeParent, { recursive: true, force: true });
    rmSync(repository, { recursive: true, force: true });
  }
});

test("Node identity survives alias changes and endpoint selection is monotonic", async () => {
  const first = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: nodeId,
    displayName: "worker",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${nodeId}`,
      generation: 10,
      status: "unknown",
      sessionName: "worker",
    },
  });
  const renamed = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: nodeId,
    displayName: "auditor",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${nodeId}`,
      generation: 11,
      status: "reachable",
      sessionName: "auditor",
    },
  });
  const refreshed = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: nodeId,
    displayName: "auditor",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${nodeId}`,
      generation: 11,
      status: "reachable",
      sessionName: "auditor",
    },
  });
  const refreshedAgain = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: nodeId,
    displayName: "auditor",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${nodeId}`,
      generation: 11,
      status: "reachable",
      sessionName: "auditor",
    },
  });
  const older = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: nodeId,
    displayName: "auditor",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${nodeId}`,
      generation: 9,
      status: "stale",
    },
  });

  assert.equal(first.record.nodeKey, `codex:${nodeId}`);
  assert.equal(renamed.record.nodeKey, first.record.nodeKey);
  assert.deepEqual(
    renamed.record.aliases.map((alias) => alias.value),
    ["worker", "auditor"],
  );
  assert.equal(renamed.endpointSelection, "replaced");
  assert.equal(refreshed.endpointSelection, "refreshed");
  assert.equal(refreshedAgain.endpointSelection, "refreshed");
  assert.equal(older.endpointSelection, "older-rejected");
  assert.equal(
    older.record.selectedEndpoints["codex-app-server"].generation,
    11,
  );
  assert.equal(statSync(directory.NODES_DIR).mode & 0o777, 0o700);

  const publicRecord = directory.publicNode(older.record);
  assert.deepEqual(publicRecord.endpointTransports, ["codex-app-server"]);
  assert.equal(publicRecord.endpointHistoryCount, 4);
  assert.equal("selectedEndpoints" in publicRecord, false);
  assert.equal("endpointHistory" in publicRecord, false);
  assert.deepEqual(
    older.record.endpointHistory.map((observation) => observation.decision),
    ["selected", "replaced", "refreshed", "older-rejected"],
  );
  assert.equal(older.record.endpointHistory[2].observationCount, 2);

  await assert.rejects(
    directory.upsertNode({
      runtimeKind: "codex",
      nativeId: nodeId,
      displayName: "auditor",
      projectId,
      endpoint: {
        transport: "codex-app-server",
        endpointId: "app-server:conflict",
        generation: 11,
        status: "unknown",
      },
    }),
    /generation collision.*recorded/,
  );
  const afterConflict = directory.readNode("codex", nodeId);
  assert.equal(
    afterConflict.selectedEndpoints["codex-app-server"].endpointId,
    `app-server:${nodeId}`,
  );
  assert.equal(afterConflict.endpointHistory.at(-1).decision, "conflict-rejected");
  assert.equal(
    directory.publicNode(afterConflict, { includeHistory: true }).endpointHistory
      .at(-1).endpointId,
    "app-server:conflict",
  );
});

test("Endpoint history remains bounded while preserving selected evidence", async () => {
  for (let index = 0; index < 70; index += 1) {
    await directory.upsertNode({
      runtimeKind: "codex",
      nativeId: nodeId,
      displayName: "auditor",
      projectId,
      endpoint: {
        transport: "codex-app-server",
        endpointId: `older:${index}`,
        generation: index % 10,
        status: "stale",
      },
    });
  }
  const node = directory.readNode("codex", nodeId);
  assert.equal(node.endpointHistory.length, directory.ENDPOINT_HISTORY_LIMIT);
  assert.ok(
    node.endpointHistory.some(
      (observation) =>
        ["selected", "replaced", "refreshed"].includes(observation.decision) &&
        observation.endpointId === `app-server:${nodeId}` &&
        observation.generation === 11,
    ),
  );
  assert.equal(
    node.selectedEndpoints["codex-app-server"].endpointId,
    `app-server:${nodeId}`,
  );
});

test("runtime kind is part of Node identity", async () => {
  const claude = await directory.upsertNode({
    runtimeKind: "claude",
    nativeId: nodeId,
    displayName: "claude-auditor",
    projectId,
    endpoint: {
      transport: "claude-uds",
      endpointId: `claude:${nodeId}:42`,
      generation: 42,
      status: "reachable",
      address: "uds:/private/redacted.sock",
    },
  });
  assert.equal(claude.record.nodeKey, `claude:${nodeId}`);
  assert.notEqual(claude.record.nodeKey, directory.readNode("codex", nodeId).nodeKey);
  assert.equal(directory.listNodes().length, 2);
  assert.equal("selectedEndpoints" in directory.publicNode(claude.record), false);
});

test("a pre-history selected Endpoint imports as baseline on explicit sync", async () => {
  const legacyNodeId = "f3345678-1234-4234-8234-123456789abc";
  const created = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: legacyNodeId,
    displayName: "legacy-endpoint",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${legacyNodeId}`,
      generation: 7,
      status: "reachable",
    },
  });
  const legacy = { ...created.record };
  delete legacy.endpointHistory;
  writeFileSync(
    path.join(directory.NODES_DIR, `codex--${legacyNodeId}.json`),
    `${JSON.stringify(legacy, null, 2)}\n`,
    { mode: 0o600 },
  );

  const synchronized = await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: legacyNodeId,
    displayName: "legacy-endpoint",
    projectId,
    endpoint: {
      transport: "codex-app-server",
      endpointId: `app-server:${legacyNodeId}`,
      generation: 7,
      status: "reachable",
    },
  });
  assert.deepEqual(
    synchronized.record.endpointHistory.map((entry) => entry.decision),
    ["baseline-imported", "refreshed"],
  );
});

test("Execution Threads retain bounded Job provenance without becoming Nodes", async () => {
  jobs.createJob({
    jobId: executionJobId,
    from: "coordinator",
    target: "worker",
    threadId: nodeId,
    task: "private fixture task",
    execution: "fork",
  });
  const classified = await directory.classifyExecutionThread({
    threadId: executionThreadId,
    jobId: executionJobId,
    sourceThreadId: nodeId,
    creationMode: "fork",
  });
  const repeated = await directory.classifyExecutionThread({
    threadId: executionThreadId,
    jobId: executionJobId,
    sourceThreadId: nodeId,
    creationMode: "fork",
  });

  assert.deepEqual(repeated, classified);
  assert.equal(classified.kind, "execution-thread");
  assert.equal(classified.sourceNodeKey, `codex:${nodeId}`);
  assert.equal(classified.projectId, projectId);
  assert.equal(directory.readNode("codex", executionThreadId), null);
  assert.equal(directory.listExecutionThreads().length, 1);
  assert.equal("task" in directory.publicExecutionThread(classified), false);
  assert.equal("permissions" in directory.publicExecutionThread(classified), false);

  await assert.rejects(
    directory.classifyExecutionThread({
      threadId: "f2345678-1234-4234-8234-123456789abc",
      jobId: "a3345678-1234-4234-8234-123456789abc",
      sourceThreadId: nodeId,
      creationMode: "fork",
    }),
    /retained fork Delegation/,
  );

  await assert.rejects(
    directory.upsertNode({
      runtimeKind: "codex",
      nativeId: executionThreadId,
      displayName: "must-not-be-addressable",
      projectId,
    }),
    /cannot be promoted/,
  );

  await assert.rejects(
    directory.classifyExecutionThread({
      threadId: "c2345678-1234-4234-8234-123456789abc",
      jobId: executionJobId,
      sourceThreadId: nodeId,
      creationMode: "fork",
    }),
    /Job already belongs/,
  );
  jobs.createJob({
    jobId: "d2345678-1234-4234-8234-123456789abc",
    from: "coordinator",
    target: "successor",
    threadId: successorNodeId,
    task: "private collision fixture",
    execution: "fork",
  });
  await assert.rejects(
    directory.classifyExecutionThread({
      threadId: nodeId,
      jobId: "d2345678-1234-4234-8234-123456789abc",
      sourceThreadId: successorNodeId,
      creationMode: "fork",
    }),
    /Addressable or Tombstoned Nodes/,
  );
  jobs.createJob({
    jobId: "e2345678-1234-4234-8234-123456789abc",
    from: "coordinator",
    target: "worker",
    threadId: nodeId,
    task: "private inline fixture",
    execution: "fork",
  });
  await assert.rejects(
    directory.classifyExecutionThread({
      threadId: nodeId,
      jobId: "e2345678-1234-4234-8234-123456789abc",
      sourceThreadId: nodeId,
      creationMode: "fork",
    }),
    /Inline source threads/,
  );
});

test("successor links are explicit, same-Project, single-predecessor, and acyclic", async () => {
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: successorNodeId,
    displayName: "successor",
    projectId,
  });
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: finalNodeId,
    displayName: "final",
    projectId,
  });
  const first = await directory.addSuccessor({
    predecessorNodeKey: `codex:${nodeId}`,
    successorNodeKey: `codex:${successorNodeId}`,
  });
  const repeated = await directory.addSuccessor({
    predecessorNodeKey: `codex:${nodeId}`,
    successorNodeKey: `codex:${successorNodeId}`,
  });
  await directory.addSuccessor({
    predecessorNodeKey: `codex:${successorNodeId}`,
    successorNodeKey: `codex:${finalNodeId}`,
  });

  assert.deepEqual(repeated, first);
  assert.equal(directory.listSuccessors().length, 2);
  assert.equal(
    directory.readSuccessor(`codex:${finalNodeId}`).predecessorNodeKey,
    `codex:${successorNodeId}`,
  );
  assert.equal("permissions" in directory.publicSuccessor(first), false);
  assert.equal("conversationId" in directory.publicSuccessor(first), false);

  await assert.rejects(
    directory.addSuccessor({
      predecessorNodeKey: `claude:${nodeId}`,
      successorNodeKey: `codex:${successorNodeId}`,
    }),
    /another predecessor/,
  );
  await assert.rejects(
    directory.addSuccessor({
      predecessorNodeKey: `codex:${finalNodeId}`,
      successorNodeKey: `codex:${nodeId}`,
    }),
    /create a cycle/,
  );

  const otherRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-other-project-"));
  try {
    await directory.ensureProject({
      routingId: "other",
      root: otherRoot,
      projectId: otherProjectId,
      discover: discovery,
    });
    await directory.upsertNode({
      runtimeKind: "codex",
      nativeId: otherNodeId,
      displayName: "other",
      projectId: otherProjectId,
    });
    await assert.rejects(
      directory.addSuccessor({
        predecessorNodeKey: `codex:${nodeId}`,
        successorNodeKey: `codex:${otherNodeId}`,
      }),
      /same Project/,
    );
  } finally {
    rmSync(otherRoot, { recursive: true, force: true });
  }
});

test("Clusters retain explicit versioned membership without creating authority", async () => {
  const created = await directory.ensureCluster({
    routingId: "release-reviewers",
    clusterId: reviewClusterId,
  });
  const repeated = await directory.ensureCluster({
    routingId: "release-reviewers",
    clusterId: "f4345678-1234-4234-8234-123456789abc",
  });
  assert.deepEqual(repeated, created);
  assert.equal(created.membershipVersion, 1);

  const first = await directory.addClusterMember({
    cluster: "release-reviewers",
    memberNodeKey: `codex:${nodeId}`,
  });
  const duplicate = await directory.addClusterMember({
    cluster: reviewClusterId,
    memberNodeKey: `codex:${nodeId}`,
  });
  assert.deepEqual(duplicate, first);
  await directory.addClusterMember({
    cluster: reviewClusterId,
    memberNodeKey: `claude:${nodeId}`,
  });
  const spanning = await directory.addClusterMember({
    cluster: reviewClusterId,
    memberNodeKey: `codex:${otherNodeId}`,
  });

  assert.equal(spanning.membershipVersion, 4);
  assert.deepEqual(spanning.members, [
    `claude:${nodeId}`,
    `codex:${nodeId}`,
    `codex:${otherNodeId}`,
  ]);
  assert.equal(directory.listClusterMemberships(reviewClusterId).length, 4);
  assert.equal(
    directory.readClusterMembership(reviewClusterId, 1).changeKind,
    "created",
  );
  assert.equal(
    directory.readClusterMembership(reviewClusterId, 4).changeKind,
    "member-added",
  );
  assert.equal(
    "members" in directory.publicCluster(spanning),
    false,
  );
  assert.deepEqual(
    directory.publicCluster(spanning, { includeMembers: true }).members,
    spanning.members,
  );
  assert.equal("permissions" in spanning, false);
  assert.equal("conversationId" in spanning, false);
  assert.equal("wakePolicy" in spanning, false);

  const removedMember = await directory.removeClusterMember({
    cluster: "release-reviewers",
    memberNodeKey: `claude:${nodeId}`,
  });
  const repeatedRemoval = await directory.removeClusterMember({
    cluster: reviewClusterId,
    memberNodeKey: `claude:${nodeId}`,
  });
  assert.deepEqual(repeatedRemoval, removedMember);
  assert.equal(removedMember.membershipVersion, 5);
  assert.equal(directory.listClusterMemberships(reviewClusterId).length, 5);

  await directory.ensureCluster({
    routingId: "release-gate",
    clusterId: releaseClusterId,
  });
  await directory.addClusterMember({
    cluster: releaseClusterId,
    memberNodeKey: `codex:${successorNodeId}`,
  });
  await directory.addClusterMember({
    cluster: releaseClusterId,
    memberNodeKey: `codex:${nodeId}`,
  });
  const releaseHead = directory.readClusterMembership(releaseClusterId, 3);
  const releaseHeadPath = path.join(
    directory.CLUSTER_MEMBERSHIPS_DIR,
    `${releaseClusterId}--0000000003.json`,
  );
  writeFileSync(
    releaseHeadPath,
    `${JSON.stringify({ ...releaseHead, members: [] }, null, 2)}\n`,
  );
  await assert.rejects(
    directory.removeClusterMember({
      cluster: releaseClusterId,
      memberNodeKey: `codex:${nodeId}`,
    }),
    /immutable snapshot history/,
  );
  writeFileSync(releaseHeadPath, `${JSON.stringify(releaseHead, null, 2)}\n`);
  const orphanTime = new Date().toISOString();
  writeFileSync(
    path.join(
      directory.CLUSTER_MEMBERSHIPS_DIR,
      `${releaseClusterId}--0000000004.json`,
    ),
    `${JSON.stringify(
      {
        version: 1,
        clusterId: releaseClusterId,
        membershipVersion: 4,
        members: [`codex:${successorNodeId}`],
        changeKind: "member-removed",
        changedNodeKey: `codex:${nodeId}`,
        createdAt: orphanTime,
      },
      null,
      2,
    )}\n`,
    { mode: 0o600 },
  );
  const recovered = await directory.recoverClusterMembership(releaseClusterId);
  assert.equal(recovered.recovered, true);
  assert.equal(recovered.record.membershipVersion, 4);
  assert.deepEqual(recovered.record.members, [`codex:${successorNodeId}`]);
  assert.equal(
    (await directory.recoverClusterMembership(releaseClusterId)).recovered,
    false,
  );
  const retired = await directory.tombstoneCluster("release-gate", {
    reason: "group-retired",
  });
  assert.equal(retired.lastMembershipVersion, 4);
  assert.equal("members" in retired, false);
  assert.equal("permissions" in retired, false);
  assert.equal(directory.readCluster(releaseClusterId), null);
  assert.equal(directory.listClusterMemberships(releaseClusterId).length, 4);
  assert.equal(statSync(directory.CLUSTERS_DIR).mode & 0o777, 0o700);
  assert.equal(statSync(directory.CLUSTER_MEMBERSHIPS_DIR).mode & 0o777, 0o700);
  assert.equal(statSync(directory.CLUSTER_TOMBSTONES_DIR).mode & 0o777, 0o700);

  await assert.rejects(
    directory.ensureCluster({
      routingId: "release-gate",
      clusterId: "f5345678-1234-4234-8234-123456789abc",
    }),
    /automatic reactivation is forbidden/,
  );

  const initialTime = new Date().toISOString();
  writeFileSync(
    path.join(
      directory.CLUSTER_MEMBERSHIPS_DIR,
      `${initialRecoveryClusterId}--0000000001.json`,
    ),
    `${JSON.stringify(
      {
        version: 1,
        clusterId: initialRecoveryClusterId,
        membershipVersion: 1,
        members: [],
        changeKind: "created",
        createdAt: initialTime,
      },
      null,
      2,
    )}\n`,
    { mode: 0o600 },
  );
  const initialRecovered = await directory.ensureCluster({
    routingId: "initial-recovery",
    clusterId: initialRecoveryClusterId,
  });
  assert.equal(initialRecovered.createdAt, initialTime);
  assert.equal(initialRecovered.membershipVersion, 1);
  const initialNextTime = new Date().toISOString();
  writeFileSync(
    path.join(
      directory.CLUSTER_MEMBERSHIPS_DIR,
      `${initialRecoveryClusterId}--0000000002.json`,
    ),
    `${JSON.stringify(
      {
        version: 1,
        clusterId: initialRecoveryClusterId,
        membershipVersion: 2,
        members: [`codex:${successorNodeId}`],
        changeKind: "member-added",
        changedNodeKey: `codex:${successorNodeId}`,
        createdAt: initialNextTime,
      },
      null,
      2,
    )}\n`,
    { mode: 0o600 },
  );
  const automaticallyRecovered = await directory.addClusterMember({
    cluster: "initial-recovery",
    memberNodeKey: `codex:${successorNodeId}`,
  });
  assert.equal(automaticallyRecovered.membershipVersion, 2);
  assert.deepEqual(automaticallyRecovered.members, [`codex:${successorNodeId}`]);
});

test("Node Tombstones retain minimal identity and prevent automatic resurrection", async () => {
  const removed = await directory.tombstoneNode("codex", nodeId, {
    reason: "session-removed",
  });
  const repeated = await directory.tombstoneNode("codex", nodeId);

  assert.deepEqual(repeated, removed);
  assert.equal(directory.readNode("codex", nodeId), null);
  assert.equal(directory.readNodeTombstone("codex", nodeId).nodeKey, `codex:${nodeId}`);
  assert.equal(directory.listNodeTombstones().length, 1);
  assert.equal(removed.lastSafeLabel, "auditor");
  assert.equal(removed.projectId, projectId);
  assert.equal("selectedEndpoints" in removed, false);
  assert.equal("address" in removed, false);
  assert.equal("pid" in removed, false);
  assert.equal("permissions" in removed, false);
  assert.equal(
    existsSync(path.join(directory.NODES_DIR, `codex--${nodeId}.json`)),
    false,
  );
  assert.equal(statSync(directory.NODE_TOMBSTONES_DIR).mode & 0o777, 0o700);
  assert.equal(
    directory.readSuccessor(`codex:${successorNodeId}`).predecessorNodeKey,
    `codex:${nodeId}`,
  );
  assert.ok(
    directory.readCluster(reviewClusterId).members.includes(`codex:${nodeId}`),
  );

  await assert.rejects(
    directory.upsertNode({
      runtimeKind: "codex",
      nativeId: nodeId,
      displayName: "resurrected",
      projectId,
    }),
    /automatic reactivation is forbidden/,
  );
  await assert.rejects(
    directory.addClusterMember({
      cluster: reviewClusterId,
      memberNodeKey: `codex:${nodeId}`,
    }),
    /tombstoned/,
  );
});
