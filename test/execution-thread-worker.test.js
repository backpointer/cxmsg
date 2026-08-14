import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-execution-worker-"));
process.env.CXMSG_STATE_DIR = stateDir;
const jobs = await import(`../src/jobs.js?execution-worker=${Date.now()}`);
const registry = await import(`../src/registry.js?execution-worker=${Date.now()}`);
const directory = await import(`../src/node-directory.js?execution-worker=${Date.now()}`);
const { runDelegationWorker } = await import(
  `../src/delegation-worker.js?execution-worker=${Date.now()}`
);

const sourceThreadId = "12345678-1234-4234-8234-123456789abc";
const executionThreadId = "22345678-1234-4234-8234-123456789abc";
const turnId = "32345678-1234-4234-8234-123456789abc";
const jobId = "42345678-1234-4234-8234-123456789abc";

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

class FixtureClient {
  static executionThreadId = executionThreadId;
  static turnId = turnId;

  async connect() {}
  async close() {}

  async request(method, params) {
    if (method === "thread/read") {
      return {
        thread: {
          id: params.threadId,
          status: { type: "idle" },
          canAcceptDirectInput: true,
        },
      };
    }
    if (method === "thread/fork") {
      return {
        thread: {
          id: this.constructor.executionThreadId,
          status: { type: "idle" },
          canAcceptDirectInput: true,
        },
      };
    }
    if (method === "turn/start") {
      const classified = directory.readExecutionThread(params.threadId);
      assert.ok(classified, "Execution Thread must be classified before turn/start");
      assert.equal(jobs.readJob(classified.jobId).executionThreadId, params.threadId);
      return { turn: { id: this.constructor.turnId } };
    }
    if (method === "thread/turns/list") {
      return {
        data: [
          {
            id: this.constructor.turnId,
            status: "completed",
            items: [
              { type: "agentMessage", phase: "final_answer", text: "done" },
            ],
          },
        ],
        nextCursor: null,
      };
    }
    throw new Error(`unexpected fixture method: ${method}`);
  }
}

class FallbackClient extends FixtureClient {
  static executionThreadId = "52345678-1234-4234-8234-123456789abc";
  static turnId = "62345678-1234-4234-8234-123456789abc";

  async request(method, params) {
    if (method === "thread/fork") throw new Error("no rollout found");
    if (method === "thread/start") {
      return {
        thread: {
          id: this.constructor.executionThreadId,
          status: { type: "idle" },
          canAcceptDirectInput: true,
        },
      };
    }
    return super.request(method, params);
  }
}

test("fork Delegation classifies its Execution Thread before completion", async () => {
  registry.writeSessionRecord({
    name: "worker",
    threadId: sourceThreadId,
    cwd: path.resolve("."),
  });
  jobs.createJob({
    jobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "bounded fixture task",
    execution: "fork",
  });

  const result = await runDelegationWorker(jobId, { Client: FixtureClient });
  const execution = directory.readExecutionThread(executionThreadId);
  assert.equal(result.status, "completed");
  assert.equal(result.threadId, executionThreadId);
  assert.equal(result.executionThreadId, executionThreadId);
  assert.equal(result.result, "done");
  assert.equal(execution.jobId, jobId);
  assert.equal(execution.sourceThreadId, sourceThreadId);
  assert.equal(execution.creationMode, "fork");
  assert.equal(directory.readNode("codex", executionThreadId), null);
});

test("standalone fallback remains a non-addressable Execution Thread", async () => {
  const fallbackJobId = "72345678-1234-4234-8234-123456789abc";
  jobs.createJob({
    jobId: fallbackJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "fallback fixture task",
    execution: "fork",
  });

  const result = await runDelegationWorker(fallbackJobId, {
    Client: FallbackClient,
  });
  const execution = directory.readExecutionThread(
    FallbackClient.executionThreadId,
  );
  assert.equal(result.status, "completed");
  assert.equal(execution.creationMode, "start-fallback");
  assert.equal(execution.sourceThreadId, sourceThreadId);
  assert.equal(directory.readNode("codex", execution.threadId), null);
});
