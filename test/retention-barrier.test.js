import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-retention-barrier-"));
process.env.CXMSG_STATE_DIR = stateDir;
const barrier = await import(`../src/retention-barrier.js?test=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("Retention mutation drains existing writers and excludes new writers", async () => {
  let releaseWriter;
  const heldWriter = barrier.withRetentionWriter(
    () => new Promise((resolve) => {
      releaseWriter = resolve;
    }),
  );

  let mutationStarted = false;
  const mutation = barrier.withRetentionMutation(async () => {
    mutationStarted = true;
    assert.equal(barrier.retentionMutationActive(), true);
    assert.equal(
      barrier.withRetentionWriter(() => "nested mutation write"),
      "nested mutation write",
    );
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.equal(mutationStarted, false);
  assert.throws(
    () => barrier.withRetentionWriter(() => "must not run"),
    (error) => error?.code === "ERETENTIONBUSY",
  );

  releaseWriter();
  await heldWriter;
  await mutation;
  assert.equal(mutationStarted, true);
});

test("nested Retention writers share one lease and mutation upgrades fail closed", async () => {
  const value = await barrier.withRetentionWriter(async () =>
    barrier.withRetentionWriter(() => "nested"),
  );
  assert.equal(value, "nested");
  await barrier.withRetentionWriter(async () => {
    await assert.rejects(
      barrier.withRetentionMutation(() => "unsafe upgrade"),
      /cannot upgrade/,
    );
  });
});
