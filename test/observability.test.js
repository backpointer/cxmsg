import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test, { after } from "node:test";

const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-events-"));
process.env.CXMSG_STATE_DIR = stateDir;
const {
  coordinationEvent,
  writeCoordinationEvent,
} = await import("../src/observability.js");

after(async () => {
  delete process.env.CXMSG_STATE_DIR;
  await fs.rm(stateDir, { recursive: true, force: true });
});

test("coordination events redact bodies, paths, and unbounded values", () => {
  const event = coordinationEvent({
    kind: "claude-delivery",
    phase: "transport",
    correlationId: "safe-id",
    target: "/private/secret/project",
    outcome: "x".repeat(129),
    errorCode: "EPERM",
    returnErrorCode: "ECONNRESET",
    denialOrigin: "downstream-error",
    body: "must not be logged",
  });

  assert.equal(event.target, "redacted");
  assert.equal(event.outcome, "redacted");
  assert.equal(event.errorCode, "EPERM");
  assert.equal(event.returnErrorCode, "ECONNRESET");
  assert.equal(event.denialOrigin, "downstream-error");
  assert.equal("body" in event, false);
});

test("coordination event log rotates into a bounded owner-only segment set", async () => {
  const maxBytes = 700;
  const archives = 2;
  for (let index = 0; index < 30; index += 1) {
    const written = await writeCoordinationEvent(
      {
        kind: "claude-delivery",
        phase: "transport",
        correlationId: `delivery-${index}`,
        target: "worker",
        attempt: index,
        outcome: "delivered",
      },
      { maxBytes, archives },
    );
    assert.equal(written.correlationId, `delivery-${index}`);
  }

  const names = (await fs.readdir(stateDir))
    .filter((name) => /^events\.jsonl(?:\.\d+)?$/.test(name))
    .sort();
  assert.deepEqual(names, ["events.jsonl", "events.jsonl.1", "events.jsonl.2"]);

  for (const name of names) {
    const target = path.join(stateDir, name);
    const metadata = await fs.stat(target);
    assert.equal(metadata.mode & 0o077, 0);
    assert.ok(metadata.size <= maxBytes);
    const lines = (await fs.readFile(target, "utf8")).trim().split("\n");
    for (const line of lines) {
      const event = JSON.parse(line);
      assert.equal(event.protocol, "cxmsg-event/1");
      assert.equal("body" in event, false);
    }
  }
});

test("coordination event writer refuses symlink segments without touching their target", async () => {
  const outside = path.join(os.tmpdir(), `cxmsg-events-target-${process.pid}`);
  await fs.writeFile(outside, "unchanged\n", { mode: 0o644 });
  await fs.chmod(outside, 0o644);
  const linked = path.join(stateDir, "events.jsonl.1");
  await fs.rm(linked, { force: true });
  await fs.symlink(outside, linked);
  try {
    const written = await writeCoordinationEvent(
      {
        kind: "claude-delivery",
        phase: "transport",
        correlationId: "refused-link",
        outcome: "delivered",
      },
      { maxBytes: 1, archives: 2 },
    );
    assert.equal(written, null);
    assert.equal(await fs.readFile(outside, "utf8"), "unchanged\n");
    assert.equal((await fs.stat(outside)).mode & 0o077, 0o044);
  } finally {
    await fs.rm(linked, { force: true });
    await fs.rm(outside, { force: true });
  }
});
