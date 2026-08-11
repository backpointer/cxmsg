import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

test("job refresh tolerates a newly started turn missing from the first read", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-job-state-"));
  process.env.CXMSG_STATE_DIR = stateDir;
  try {
    const { createJob, refreshJob, updateJob } = await import("../src/jobs.js");
    const created = createJob({
      jobId: "32345678-1234-1234-1234-123456789abc",
      from: "coordinator",
      target: "worker",
      targetThreadId: "source-thread",
      threadId: null,
      task: "inspect",
    });
    const running = updateJob(created, {
      status: "running",
      threadId: "execution-thread",
      turnId: "new-turn",
      turnStartedAt: new Date().toISOString(),
    });
    const refreshed = await refreshJob(
      {
        request: async () => ({
          thread: { id: "execution-thread", turns: [] },
        }),
      },
      running,
    );
    assert.equal(refreshed.status, "running");
    assert.equal(refreshed.error, null);
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
    delete process.env.CXMSG_STATE_DIR;
  }
});
