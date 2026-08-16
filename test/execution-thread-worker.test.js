import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-execution-worker-"));
process.env.CXMSG_STATE_DIR = stateDir;
const jobs = await import(`../src/jobs.js?execution-worker=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?execution-worker=${Date.now()}`);
const registry = await import(`../src/registry.js?execution-worker=${Date.now()}`);
const directory = await import(`../src/node-directory.js?execution-worker=${Date.now()}`);
const authority = await import(
  `../src/delegation-authority.js?execution-worker=${Date.now()}`
);
const { runDelegationWorker } = await import(
  `../src/delegation-worker.js?execution-worker=${Date.now()}`
);

const sourceThreadId = "12345678-1234-4234-8234-123456789abc";
const executionThreadId = "22345678-1234-4234-8234-123456789abc";
const turnId = "32345678-1234-4234-8234-123456789abc";
const jobId = "42345678-1234-4234-8234-123456789abc";
const projectId = "82345678-1234-4234-8234-123456789abc";

await directory.ensureProject({
  routingId: "execution-worker",
  root: path.resolve("."),
  projectId,
});
await directory.upsertNode({
  runtimeKind: "codex",
  nativeId: sourceThreadId,
  displayName: "worker",
  projectId,
});

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

class FixtureClient {
  static executionThreadId = executionThreadId;
  static turnId = turnId;
  static requests = [];
  static options = null;

  constructor(options = {}) {
    this.constructor.options = options;
  }

  async connect() {}
  async close() {}

  async request(method, params) {
    this.constructor.requests.push({ method, params });
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
      if (params.threadId !== sourceThreadId) {
        assert.ok(classified, "Execution Thread must be classified before turn/start");
        assert.equal(jobs.readJob(classified.jobId).executionThreadId, params.threadId);
      }
      return { turn: { id: this.constructor.turnId } };
    }
    if (method === "thread/turns/list") {
      return {
        data: [
          {
            id: this.constructor.turnId,
            status: "completed",
            items: [],
          },
        ],
        nextCursor: null,
      };
    }
    if (method === "thread/items/list") {
      return {
        data: [
          {
            turnId: this.constructor.turnId,
            item: {
              type: "agentMessage",
              phase: "final_answer",
              text: "done",
            },
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

class ScheduledClient extends FixtureClient {
  static executionThreadId = "92345678-1234-4234-8234-123456789abc";
  static turnId = "a2345678-1234-4234-8234-123456789abc";
}

class LongTaskClient extends FixtureClient {
  static executionThreadId = "b3345678-1234-4234-8234-123456789abc";
  static turnId = "b4345678-1234-4234-8234-123456789abc";
  static requests = [];
}

class ExplicitFreshClient extends FixtureClient {
  static executionThreadId = "a3345678-1234-4234-8234-123456789abc";
  static turnId = "a4345678-1234-4234-8234-123456789abc";
  static requests = [];

  async request(method, params) {
    if (method === "thread/fork") {
      throw new Error("explicit fresh execution must not fork source history");
    }
    if (method === "thread/start") {
      this.constructor.requests.push({ method, params });
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

class DisconnectedTurnClient extends FixtureClient {
  static executionThreadId = "c3345678-1234-4234-8234-123456789abc";
  static requests = [];

  async request(method, params) {
    if (method === "turn/start") {
      this.constructor.requests.push({ method, params });
      const error = new Error("app-server WebSocket is not connected");
      error.code = "EAPPWSNOTCONNECTED";
      throw error;
    }
    return super.request(method, params);
  }
}

class MissingBodyClient extends FixtureClient {
  static executionThreadId = "f5345678-1234-4234-8234-123456789abc";
  static turnId = "f6345678-1234-4234-8234-123456789abc";
}

class FrameFailureClient extends FixtureClient {
  static requests = [];

  async request(method, params) {
    if (method === "thread/fork") {
      this.constructor.requests.push({ method, params });
      const error = new Error("app-server WebSocket frame exceeds the buffer limit");
      error.code = "EAPPWSFRAME";
      error.observedBytes = 1_048_577;
      error.limitBytes = 1_048_576;
      throw error;
    }
    return super.request(method, params);
  }
}

class MirrorFailureClient extends FixtureClient {
  static executionThreadId = "f3345678-1234-4234-8234-123456789abc";
  static turnId = "f4345678-1234-4234-8234-123456789abc";
  static requests = [];

  async request(method, params) {
    if (method === "thread/read") {
      const error = new Error("app-server WebSocket frame exceeds the buffer limit");
      error.code = "EAPPWSFRAME";
      throw error;
    }
    return super.request(method, params);
  }
}

class ResultFrameFailureClient extends FixtureClient {
  static executionThreadId = "d6345678-1234-4234-8234-123456789abc";
  static turnId = "d7345678-1234-4234-8234-123456789abc";
  static requests = [];

  async request(method, params) {
    if (method === "thread/items/list") {
      this.constructor.requests.push({ method, params });
      const error = new Error("app-server WebSocket frame exceeds the buffer limit");
      error.code = "EAPPWSFRAME";
      error.observedBytes = 1_097_173;
      error.limitBytes = 1_048_576;
      throw error;
    }
    return super.request(method, params);
  }
}

class EmptyRolloutRaceClient extends ExplicitFreshClient {
  static executionThreadId = "e6345678-1234-4234-8234-123456789abc";
  static turnId = "e7345678-1234-4234-8234-123456789abc";
  static requests = [];
  static emptyReads = 0;

  async request(method, params) {
    if (method === "thread/turns/list" && this.constructor.emptyReads++ === 0) {
      this.constructor.requests.push({ method, params });
      throw new Error(
        "thread/turns/list failed: thread-store internal error: rollout at /private/runtime/rollout.jsonl is empty",
      );
    }
    return super.request(method, params);
  }
}

test("permission profile errors expose the exact discovery command", () => {
  assert.throws(
    () => authority.requirePermissionProfile(
      [{ id: ":read-only", allowed: true }],
      "read-only",
      "worker",
    ),
    (error) =>
      error?.code === "EPERMISSIONPROFILE" &&
      /cxmsg permissions worker/.test(error.message) &&
      /:read-only/.test(error.message),
  );
  assert.equal(
    authority.requirePermissionProfile(
      [{ id: ":read-only", allowed: true }],
      ":read-only",
      "worker",
    ).id,
    ":read-only",
  );
});

test("fork Delegation classifies its Execution Thread before completion", async () => {
  FixtureClient.requests = [];
  registry.writeSessionRecord({
    name: "worker",
    threadId: sourceThreadId,
    cwd: path.resolve("."),
    allowedDelegators: ["coordinator"],
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
  assert.equal(result.resultObservation.status, "available");
  assert.equal(execution.jobId, jobId);
  assert.equal(execution.sourceThreadId, sourceThreadId);
  assert.equal(execution.creationMode, "fork");
  assert.equal(directory.readNode("codex", executionThreadId), null);
  assert.deepEqual(FixtureClient.options.optOutNotificationMethods, [
    "item/completed",
    "turn/completed",
  ]);
  assert.equal(
    FixtureClient.requests.some((request) => request.method === "thread/read"),
    false,
  );
  assert.equal(
    FixtureClient.requests.find((request) => request.method === "thread/fork")
      .params.threadId,
    sourceThreadId,
  );
  assert.deepEqual(
    FixtureClient.requests.find(
      (request) => request.method === "thread/turns/list",
    ).params,
    {
      threadId: executionThreadId,
      limit: 8,
      sortDirection: "desc",
      itemsView: "notLoaded",
    },
  );
  assert.deepEqual(
    FixtureClient.requests.find(
      (request) => request.method === "thread/items/list",
    ).params,
    {
      threadId: executionThreadId,
      turnId,
      limit: 1,
      sortDirection: "desc",
    },
  );
});

test("inline Delegation retains its metadata preflight", async () => {
  const inlineJobId = "d2345678-1234-4234-8234-123456789abc";
  FixtureClient.requests = [];
  jobs.createJob({
    jobId: inlineJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "inline fixture task",
    execution: "inline",
  });
  const result = await runDelegationWorker(inlineJobId, { Client: FixtureClient });
  assert.equal(result.status, "completed");
  assert.equal(result.threadId, sourceThreadId);
  assert.equal(result.executionThreadId, null);
  assert.equal(
    FixtureClient.requests.some((request) => request.method === "thread/read"),
    true,
  );
});

test("Delegation persists a structured App Server frame failure", async () => {
  const failedJobId = "e2345678-1234-4234-8234-123456789abc";
  jobs.createJob({
    jobId: failedJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "frame failure fixture task",
    execution: "fork",
  });
  await assert.rejects(
    runDelegationWorker(failedJobId, { Client: FrameFailureClient }),
    (error) => error?.code === "EAPPWSFRAME",
  );
  const failed = jobs.readJob(failedJobId);
  assert.equal(failed.status, "failed");
  assert.equal(failed.failureCode, "EAPPWSFRAME");
  assert.deepEqual(failed.failureEvidence, {
    errorCode: "EAPPWSFRAME",
    observedBytes: 1_048_577,
    limitBytes: 1_048_576,
  });
  assert.equal(failed.turnId, null);
  assert.equal(failed.failureStage, "execution-thread");
  assert.equal(failed.modelTurnStarted, false);
  assert.match(failed.rerouteGuidance, /--execution fresh/);
});

test("explicit fresh Delegation preserves source provenance without forking history", async () => {
  const freshJobId = "9345678a-1234-4234-8234-123456789abc";
  ExplicitFreshClient.requests = [];
  jobs.createJob({
    jobId: freshJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "fresh isolated fixture",
    execution: "fresh",
  });
  const result = await runDelegationWorker(freshJobId, {
    Client: ExplicitFreshClient,
  });
  const execution = directory.readExecutionThread(
    ExplicitFreshClient.executionThreadId,
  );
  assert.equal(result.status, "completed");
  assert.equal(result.targetThreadId, sourceThreadId);
  assert.equal(result.executionThreadId, ExplicitFreshClient.executionThreadId);
  assert.equal(execution.sourceThreadId, sourceThreadId);
  assert.equal(execution.creationMode, "explicit-fresh");
  assert.equal(
    ExplicitFreshClient.requests.some((request) => request.method === "thread/fork"),
    false,
  );
});

test("large Delegation tasks use a retained body reference instead of Job plaintext", async () => {
  const longJobId = "d3345678-1234-4234-8234-123456789abc";
  const task = `review retained artifact\n${"bounded-context-line\n".repeat(900)}`;
  const taskBody = await bodies.storeMessageBody({
    messageId: longJobId,
    body: task,
  });
  LongTaskClient.requests = [];
  jobs.createJob({
    jobId: longJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: null,
    taskBody,
    execution: "fork",
  });

  const stored = jobs.readJob(longJobId);
  assert.equal(stored.task, null);
  assert.equal(stored.taskBody.contentRef, `cxmsg-message:${longJobId}`);
  assert.doesNotMatch(JSON.stringify(stored), /bounded-context-line/);

  const result = await runDelegationWorker(longJobId, { Client: LongTaskClient });
  assert.equal(result.status, "completed");
  assert.equal(result.modelTurnStarted, true);
  const start = LongTaskClient.requests.find(
    (request) => request.method === "turn/start",
  );
  const input = JSON.stringify(start.params.input);
  assert.match(input, new RegExp(`cxmsg-message:${longJobId}`));
  assert.match(input, /Preview only/);
  assert.ok(Buffer.byteLength(input, "utf8") < 16 * 1024);
});

test("a missing retained Delegation task fails before model execution", async () => {
  const missingBodyJobId = "f3345678-1234-4234-8234-123456789abc";
  jobs.createJob({
    jobId: missingBodyJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: null,
    taskBody: {
      messageId: missingBodyJobId,
      contentRef: `cxmsg-message:${missingBodyJobId}`,
      bodyBytes: 20_000,
      bodySha256: "a".repeat(64),
    },
    execution: "fork",
  });
  await assert.rejects(
    runDelegationWorker(missingBodyJobId, { Client: MissingBodyClient }),
    (error) => error?.code === "EDELEGATIONTASKBODY",
  );
  const failed = jobs.readJob(missingBodyJobId);
  assert.equal(failed.failureStage, "task-body");
  assert.equal(failed.modelTurnStarted, false);
  assert.equal(failed.turnId, null);
});

test("a disconnected turn start records a proven zero-turn transport failure", async () => {
  const disconnectedJobId = "e3345678-1234-4234-8234-123456789abc";
  jobs.createJob({
    jobId: disconnectedJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "short disconnected fixture",
    execution: "fork",
  });
  await assert.rejects(
    runDelegationWorker(disconnectedJobId, { Client: DisconnectedTurnClient }),
    (error) => error?.code === "EAPPWSNOTCONNECTED",
  );
  const failed = jobs.readJob(disconnectedJobId);
  assert.equal(failed.status, "failed");
  assert.equal(failed.failureCode, "EAPPWSNOTCONNECTED");
  assert.equal(failed.failureStage, "turn-start");
  assert.ok(failed.turnStartAttemptedAt);
  assert.equal(failed.modelTurnStarted, false);
  assert.equal(failed.turnId, null);
  assert.match(failed.rerouteGuidance, /verify cxmsg server connectivity/);
});

test("durable Job completion is independent from peer mirror failure", async () => {
  const mirroredJobId = "f2345678-1234-4234-8234-123456789abc";
  jobs.createJob({
    jobId: mirroredJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "mirror failure fixture task",
    execution: "fork",
    mirror: "summary",
  });
  const result = await runDelegationWorker(mirroredJobId, {
    Client: MirrorFailureClient,
  });
  assert.equal(result.status, "completed");
  assert.equal(result.result, "done");
  assert.equal(result.mirrorDelivery.status, "failed");
  assert.equal(result.mirrorDelivery.errorCode, "EAPPWSFRAME");
});

test("terminal turn evidence survives a bounded result observation failure", async () => {
  const observationJobId = "d5345678-1234-4234-8234-123456789abc";
  jobs.createJob({
    jobId: observationJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "long-history result observation fixture",
    execution: "fork",
  });

  const result = await runDelegationWorker(observationJobId, {
    Client: ResultFrameFailureClient,
  });
  assert.equal(result.status, "completed");
  assert.equal(result.modelTurnStarted, true);
  assert.equal(result.result, null);
  assert.deepEqual(
    {
      status: result.resultObservation.status,
      source: result.resultObservation.source,
      errorCode: result.resultObservation.errorCode,
      observedBytes: result.resultObservation.observedBytes,
      limitBytes: result.resultObservation.limitBytes,
    },
    {
      status: "failed",
      source: "thread-items",
      errorCode: "EAPPWSFRAME",
      observedBytes: 1_097_173,
      limitBytes: 1_048_576,
    },
  );
  assert.equal(result.failureCode, undefined);
  assert.equal(result.failureStage, null);
  assert.deepEqual(
    ResultFrameFailureClient.options.optOutNotificationMethods,
    ["item/completed", "turn/completed"],
  );
});

test("a transient empty rollout is re-observed without failing the started turn", async () => {
  const emptyRaceJobId = "e5345678-1234-4234-8234-123456789abc";
  EmptyRolloutRaceClient.emptyReads = 0;
  EmptyRolloutRaceClient.requests = [];
  jobs.createJob({
    jobId: emptyRaceJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "empty rollout race fixture",
    execution: "fresh",
  });

  const result = await runDelegationWorker(emptyRaceJobId, {
    Client: EmptyRolloutRaceClient,
  });
  assert.equal(result.status, "completed");
  assert.equal(result.result, "done");
  assert.equal(result.modelTurnStarted, true);
  assert.equal(result.failureCode, undefined);
  assert.equal(EmptyRolloutRaceClient.emptyReads >= 2, true);
  assert.doesNotMatch(JSON.stringify(result), /private\/runtime/);
});

test("a persistent empty rollout becomes redacted unknown evidence", async () => {
  const persistentJobId = "f7345678-1234-4234-8234-123456789abc";
  const created = jobs.createJob({
    jobId: persistentJobId,
    from: "coordinator",
    target: "worker",
    threadId: sourceThreadId,
    task: "persistent empty rollout fixture",
    execution: "fresh",
  });
  const running = await jobs.updateJob(created, {
    status: "running",
    threadId: "f8345678-1234-4234-8234-123456789abc",
    turnId: "f9345678-1234-4234-8234-123456789abc",
    turnStartedAt: "2026-08-16T00:00:00.000Z",
    modelTurnStarted: true,
  });
  const result = await jobs.refreshJob(
    {
      async request() {
        throw new Error(
          "thread/turns/list failed: thread-store internal error: rollout at /private/runtime/rollout.jsonl is empty",
        );
      },
    },
    running,
  );
  assert.equal(result.status, "unknown");
  assert.equal(result.failureCode, "EROLLOUTEMPTY");
  assert.equal(result.failureStage, "turn-observation");
  assert.equal(result.modelTurnStarted, true);
  assert.match(result.rerouteGuidance, /do not retry or redelegate/i);
  assert.doesNotMatch(JSON.stringify(result), /private\/runtime/);
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

test("scheduled Delegation worker requires and consumes the exact Scheduler claim", async () => {
  const scheduledJobId = "b2345678-1234-4234-8234-123456789abc";
  const schedulerWorkerId = "c2345678-1234-4234-8234-123456789abc";
  await jobs.createScheduledDelegationJob({
    jobId: scheduledJobId,
    from: "coordinator",
    target: "worker",
    targetThreadId: sourceThreadId,
    task: "scheduled fixture task",
    permissions: null,
    execution: "fork",
    approval: "never",
    mirror: "none",
    approvalTimeoutSeconds: 600,
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
    targetNodeKey: `codex:${sourceThreadId}`,
    targetProjectId: projectId,
  });
  const claimed = await jobs.claimScheduledDelegation(scheduledJobId, {
    workerId: schedulerWorkerId,
    leaseMs: 30_000,
  });
  const result = await runDelegationWorker(scheduledJobId, {
    Client: ScheduledClient,
    scheduleClaim: {
      claimId: claimed.claim.claimId,
      workerId: schedulerWorkerId,
    },
  });
  assert.equal(result.status, "completed");
  assert.equal(result.jobId, scheduledJobId);
  assert.equal(result.schedule.attemptCount, 1);
  assert.equal(result.executionThreadId, ScheduledClient.executionThreadId);
});
