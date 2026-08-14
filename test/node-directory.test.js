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
  assert.equal(older.endpointSelection, "older-rejected");
  assert.equal(
    older.record.selectedEndpoints["codex-app-server"].generation,
    11,
  );
  assert.equal(statSync(directory.NODES_DIR).mode & 0o777, 0o700);

  const publicRecord = directory.publicNode(older.record);
  assert.deepEqual(publicRecord.endpointTransports, ["codex-app-server"]);
  assert.equal("selectedEndpoints" in publicRecord, false);

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
    /generation collision/,
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
    const otherProjectId = "72345678-1234-4234-8234-123456789abc";
    const otherNodeId = "82345678-1234-4234-8234-123456789abc";
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

  await assert.rejects(
    directory.upsertNode({
      runtimeKind: "codex",
      nativeId: nodeId,
      displayName: "resurrected",
      projectId,
    }),
    /automatic reactivation is forbidden/,
  );
});
