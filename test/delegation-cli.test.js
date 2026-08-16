import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-delegation-cli-"));
const missingSocket = path.join(stateDir, "missing-app-server.sock");
process.env.CXMSG_STATE_DIR = stateDir;

const registry = await import(`../src/registry.js?delegation-cli=${Date.now()}`);
const jobs = await import(`../src/jobs.js?delegation-cli=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?delegation-cli=${Date.now()}`);

registry.writeSessionRecord({
  name: "worker",
  threadId: "91345678-1234-4234-8234-123456789abc",
  cwd: path.resolve("."),
  allowedDelegators: ["coordinator"],
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function cxmsg(...args) {
  return spawnSync(process.execPath, ["bin/cxmsg.js", ...args], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CXMSG_STATE_DIR: stateDir,
      CXMSG_SOCKET: missingSocket,
    },
    encoding: "utf8",
  });
}

test("immediate Delegation preflight failure creates neither Job nor retained task body", () => {
  const jobId = "a1345678-1234-4234-8234-123456789abc";
  const beforeJobs = jobs.listJobs().length;
  const beforeBodies = bodies.listMessageBodies().length;
  const delegated = cxmsg(
    "delegate",
    "--from",
    "coordinator",
    "--job-id",
    jobId,
    "worker",
    `large task\n${"bounded line\n".repeat(1_400)}`,
  );

  assert.equal(delegated.status, 1);
  assert.match(delegated.stderr, /code=ENOENT/);
  assert.equal(jobs.readJob(jobId), null);
  assert.equal(jobs.listJobs().length, beforeJobs);
  assert.equal(bodies.listMessageBodies().length, beforeBodies);
});
