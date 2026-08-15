import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-consolidate-cli-"));
process.env.CXMSG_STATE_DIR = stateDir;
const registry = await import(`../src/registry.js?consolidate-cli=${Date.now()}`);
const attachments = await import(
  `../src/attachments.js?consolidate-cli=${Date.now()}`
);
const jobs = await import(`../src/jobs.js?consolidate-cli=${Date.now()}`);

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

function register(name, threadId, extra = {}) {
  registry.writeSessionRecord({
    name,
    threadId,
    cwd: path.resolve("."),
    createdAt: "2026-08-16T00:00:00.000Z",
    ...extra,
  });
}

test("consolidate preserves the canonical session and moves attachment metadata", () => {
  const threadId = "11345678-1234-4234-8234-123456789abc";
  register("canonical", threadId, { managedByCxmsgAt: "2026-08-16T00:00:00.000Z" });
  register("duplicate", threadId, { adopted: true });
  attachments.writeAttachmentRecord({
    version: 1,
    name: "duplicate",
    threadId,
    childPid: 98765,
    parentPid: 98764,
    cwd: path.resolve("."),
    startedAt: "2026-08-16T00:01:00.000Z",
  });

  const consolidated = cxmsg("consolidate", "canonical", "duplicate", "--json");
  assert.equal(consolidated.status, 0, consolidated.stderr);
  assert.deepEqual(JSON.parse(consolidated.stdout), {
    canonicalName: "canonical",
    removedAlias: "duplicate",
    threadId,
    attachmentMoved: true,
  });
  assert.equal(registry.readSessionRecord("duplicate"), null);
  assert.equal(registry.readSessionRecord("canonical").managedByCxmsgAt, "2026-08-16T00:00:00.000Z");
  assert.equal(attachments.readAttachmentRecord("duplicate"), null);
  assert.equal(attachments.readAttachmentRecord("canonical").childPid, 98765);
});

test("remove refuses to delete a thread that still has another registered alias", () => {
  const threadId = "21345678-1234-4234-8234-123456789abc";
  register("shared-a", threadId);
  register("shared-b", threadId);

  const removed = cxmsg("remove", "shared-b");
  assert.equal(removed.status, 1);
  assert.match(removed.stderr, /also registered as shared-a.*cxmsg consolidate/);
  assert.ok(registry.readSessionRecord("shared-a"));
  assert.ok(registry.readSessionRecord("shared-b"));
});

test("consolidate refuses to transfer authority or pending work implicitly", () => {
  const grantedThreadId = "31345678-1234-4234-8234-123456789abc";
  register("grant-canonical", grantedThreadId);
  register("grant-duplicate", grantedThreadId, {
    allowedDelegators: ["coordinator"],
  });
  const granted = cxmsg("consolidate", "grant-canonical", "grant-duplicate");
  assert.equal(granted.status, 1);
  assert.match(granted.stderr, /Delegation grants target grant-duplicate/);
  assert.ok(registry.readSessionRecord("grant-duplicate"));

  const busyThreadId = "41345678-1234-4234-8234-123456789abc";
  register("busy-canonical", busyThreadId);
  register("busy-duplicate", busyThreadId);
  jobs.createJob({
    from: "coordinator",
    target: "busy-duplicate",
    threadId: busyThreadId,
    task: "bounded test task",
  });
  const busy = cxmsg("consolidate", "busy-canonical", "busy-duplicate");
  assert.equal(busy.status, 1);
  assert.match(busy.stderr, /pending jobs reference busy-duplicate/);
  assert.ok(registry.readSessionRecord("busy-duplicate"));
});

test("consolidate rejects different threads and conflicting attachment metadata", () => {
  register("different-a", "51345678-1234-4234-8234-123456789abc");
  register("different-b", "61345678-1234-4234-8234-123456789abc");
  const different = cxmsg("consolidate", "different-a", "different-b");
  assert.equal(different.status, 1);
  assert.match(different.stderr, /same thread/);

  const threadId = "71345678-1234-4234-8234-123456789abc";
  register("attached-canonical", threadId);
  register("attached-duplicate", threadId);
  for (const [name, childPid] of [
    ["attached-canonical", 87650],
    ["attached-duplicate", 87651],
  ]) {
    attachments.writeAttachmentRecord({
      version: 1,
      name,
      threadId,
      childPid,
      parentPid: 87649,
      cwd: path.resolve("."),
      startedAt: "2026-08-16T00:02:00.000Z",
    });
  }
  const conflict = cxmsg(
    "consolidate",
    "attached-canonical",
    "attached-duplicate",
  );
  assert.equal(conflict.status, 1);
  assert.match(conflict.stderr, /conflicting attachment metadata/);
  assert.ok(registry.readSessionRecord("attached-duplicate"));
});
