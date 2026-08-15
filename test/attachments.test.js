import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, statSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-attachments-"));
process.env.CXMSG_STATE_DIR = stateDir;
const attachments = await import(`../src/attachments.js?test=${Date.now()}`);
const registry = await import(`../src/registry.js?test=${Date.now()}`);

function record(overrides = {}) {
  return {
    version: 1,
    name: "worker-1",
    threadId: "12345678-1234-4123-8123-123456789abc",
    childPid: 12345,
    parentPid: 12344,
    cwd: "/tmp/project",
    startedAt: "2026-08-12T00:00:00.000Z",
    ...overrides,
  };
}

test("attachment records are private, atomic, and removable by owner pid", () => {
  attachments.writeAttachmentRecord(record());
  assert.deepEqual(attachments.readAttachmentRecord("worker-1"), record());
  assert.equal(statSync(attachments.ATTACHMENTS_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(path.join(attachments.ATTACHMENTS_DIR, "worker-1.json")).mode &
      0o777,
    0o600,
  );
  assert.equal(attachments.removeAttachmentRecord("worker-1", 99999), false);
  assert.ok(readFileSync(path.join(attachments.ATTACHMENTS_DIR, "worker-1.json")));
  assert.equal(attachments.removeAttachmentRecord("worker-1", 12345), true);
  assert.equal(attachments.readAttachmentRecord("worker-1"), null);
});

test("detach requests preserve the attachment identity", () => {
  attachments.writeAttachmentRecord(record());
  const updated = attachments.markAttachmentDetachRequested("worker-1", 12345);
  assert.equal(updated.childPid, 12345);
  assert.match(updated.detachRequestedAt, /^\d{4}-\d{2}-\d{2}T/);
  attachments.removeAttachmentRecord("worker-1");
});

test("attachment command matching requires remote Codex and the exact thread", () => {
  const current = record();
  assert.equal(
    attachments.attachmentCommandMatches(
      current,
      `codex resume --remote unix:///tmp/app.sock ${current.threadId}`,
    ),
    true,
  );
  assert.equal(
    attachments.attachmentCommandMatches(
      current,
      "codex resume --remote unix:///tmp/app.sock another-thread",
    ),
    false,
  );
  assert.equal(
    attachments.attachmentCommandMatches(current, `sleep ${current.threadId}`),
    false,
  );
});

test("presentation distinguishes managed background sessions from adopted writers", () => {
  assert.equal(attachments.sessionPresentation({ adopted: true }), "stored-or-external");
  assert.equal(attachments.sessionPresentation({ adopted: false }), "background");
  assert.equal(attachments.sessionPresentation({}), "background");
  assert.equal(
    attachments.sessionPresentation({ adopted: true, managedByCxmsgAt: "now" }),
    "background",
  );
  assert.equal(
    attachments.sessionPresentation({ adopted: true }, record()),
    "foreground",
  );
  assert.equal(registry.sessionAllowsAppServerResume({ adopted: true }), false);
  assert.equal(
    registry.sessionAllowsAppServerResume({
      adopted: true,
      managedByCxmsgAt: "2026-08-15T00:00:00.000Z",
    }),
    true,
  );
  assert.equal(registry.sessionAllowsAppServerResume({}), true);
});
