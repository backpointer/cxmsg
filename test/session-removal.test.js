import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-session-remove-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-session-project-"));
process.env.CXMSG_STATE_DIR = stateDir;
const registry = await import(`../src/registry.js?session-remove=${Date.now()}`);
const directory = await import(
  `../src/node-directory.js?session-remove=${Date.now()}`
);
const removals = await import(
  `../src/session-removal.js?session-remove=${Date.now()}`
);
const inspectors = await import(`../src/inspectors.js?session-remove=${Date.now()}`);

const projectId = "81345678-1234-4234-8234-123456789abc";
const project = await directory.ensureProject({
  routingId: "session-removal-test",
  root: projectRoot,
  projectId,
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

async function createAddressableNode(name, threadId) {
  registry.writeSessionRecord({
    name,
    threadId,
    cwd: projectRoot,
    createdAt: "2026-08-16T00:00:00.000Z",
  });
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: threadId,
    displayName: name,
    projectId: project.projectId,
  });
}

test("session removal retries safely after thread deletion and completes the Tombstone", async () => {
  const name = "crash-before-registry-remove";
  const threadId = "91345678-1234-4234-8234-123456789abc";
  await createAddressableNode(name, threadId);
  let deleteAttempts = 0;

  await assert.rejects(
    removals.finalizeSessionRemovalLocked({
      name,
      threadId,
      async deleteThread() {
        deleteAttempts += 1;
      },
      fault(phase) {
        if (phase === "after-thread-delete") {
          throw new Error("injected crash after thread deletion");
        }
      },
    }),
    /injected crash/,
  );
  assert.ok(registry.readSessionRecord(name));
  assert.ok(directory.readNode("codex", threadId));

  await removals.finalizeSessionRemovalLocked({
    name,
    threadId,
    async deleteThread() {
      deleteAttempts += 1;
      throw new Error("thread not found");
    },
  });
  assert.equal(deleteAttempts, 2);
  assert.equal(registry.readSessionRecord(name), null);
  assert.equal(directory.readNode("codex", threadId), null);
  assert.ok(directory.readNodeTombstone("codex", threadId));
});

test("Doctor reports a live Codex Node left after registry removal", async () => {
  const name = "crash-before-tombstone";
  const threadId = "a1345678-1234-4234-8234-123456789abc";
  await createAddressableNode(name, threadId);

  await assert.rejects(
    removals.finalizeSessionRemovalLocked({
      name,
      threadId,
      async deleteThread() {},
      fault(phase) {
        if (phase === "after-session-record-remove") {
          throw new Error("injected crash before Node Tombstone");
        }
      },
    }),
    /injected crash/,
  );
  assert.equal(registry.readSessionRecord(name), null);
  assert.ok(directory.readNode("codex", threadId));
  assert.equal(directory.readNodeTombstone("codex", threadId), null);

  const finding = inspectors
    .inspectNodeDirectory({ stateDir, sessions: [] })
    .find((check) => check.errorCode === "ENODEUNREGISTERED");
  assert.equal(finding.status, "warn");
  assert.equal(finding.verification, "registry");
  assert.equal(finding.repairable, false);
  assert.doesNotMatch(JSON.stringify(finding), new RegExp(projectRoot));
});
