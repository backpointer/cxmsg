import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-route-directory-state-"));
const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-route-directory-root-"));
process.env.CXMSG_STATE_DIR = stateDir;
const directory = await import(`../src/node-directory.js?route=${Date.now()}`);
const registry = await import(`../src/registry.js?route-directory=${Date.now()}`);
const routes = await import(`../src/route-admission.js?directory=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(root, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const projectId = "52345678-1234-4234-8234-123456789abc";
const threadId = "62345678-1234-4234-8234-123456789abc";
const messageId = "72345678-1234-4234-8234-123456789abc";

test("Route binding pins the stable Directory Project and Node identities", async () => {
  const project = await directory.ensureProject({
    routingId: "hermes",
    root,
    projectId,
    discover: (candidate) => ({
      kind: "canonical-root",
      key: path.resolve(candidate),
      root: path.resolve(candidate),
    }),
  });
  registry.writeSessionRecord({ name: "worker", threadId, cwd: root });
  const node = (
    await directory.upsertNode({
      runtimeKind: "codex",
      nativeId: threadId,
      displayName: "worker",
      projectId,
    })
  ).record;
  routes.writeRouteBinding({
    sessionName: "worker",
    threadId,
    projectId: "hermes",
    projectKey: project.projectId,
    nodeKey: node.nodeKey,
    role: "auditor",
  });

  let dispatches = 0;
  const admitted = await routes.routePeerMessage(
    {
      from: "unbound-sender",
      target: "worker",
      message: "directory-bound route",
      route: {
        schema_version: 1,
        project_id: "hermes",
        target_role: "auditor",
        logical_message_id: messageId,
      },
      logicalMessageId: messageId,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "turn-directory" };
    },
  );
  assert.equal(admitted.reason, "binding_match");
  assert.equal(dispatches, 1);
});

test("a routing label cannot silently replace the bound private Project identity", async () => {
  routes.writeRouteBinding({
    sessionName: "worker",
    threadId,
    projectId: "hermes",
    projectKey: "82345678-1234-4234-8234-123456789abc",
    nodeKey: `codex:${threadId}`,
    role: "auditor",
  });
  let dispatches = 0;
  const secondId = "92345678-1234-4234-8234-123456789abc";
  const outcome = await routes.routePeerMessage(
    {
      from: "unbound-sender",
      target: "worker",
      message: "wrong stable Project",
      route: {
        schema_version: 1,
        project_id: "hermes",
        target_role: "auditor",
        logical_message_id: secondId,
      },
      logicalMessageId: secondId,
    },
    async () => {
      dispatches += 1;
      return { delivery: "started", turnId: "forbidden" };
    },
  );
  assert.equal(outcome.reason, "project_identity_mismatch");
  assert.equal(dispatches, 0);
});
