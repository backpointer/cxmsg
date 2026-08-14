import assert from "node:assert/strict";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-node-directory-state-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-node-directory-root-"));
process.env.CXMSG_STATE_DIR = stateDir;
const directory = await import(`../src/node-directory.js?test=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const projectId = "12345678-1234-4234-8234-123456789abc";
const nodeId = "22345678-1234-4234-8234-123456789abc";
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
