import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-turn-lifecycle-"));
process.env.CXMSG_STATE_DIR = stateDir;
const lifecycle = await import(`../src/turn-lifecycle.js?test=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const threadId = "11345678-1234-4234-8234-123456789abc";
const turnId = "21345678-1234-4234-8234-123456789abc";

test("lifecycle state persists connection epochs and bounded metadata-only observations", () => {
  const connection = lifecycle.beginTurnLifecycleConnection({
    appServerVersion: "0.147.0",
    now: "2026-08-15T00:00:00.000Z",
  });
  lifecycle.observeTurnLifecycleNotification(
    {
      method: "turn/started",
      params: {
        threadId,
        turn: {
          id: turnId,
          status: "inProgress",
          items: [{ type: "userMessage", text: "must not persist" }],
        },
      },
    },
    { now: "2026-08-15T00:00:01.000Z" },
  );
  lifecycle.observeTurnLifecycleNotification(
    {
      method: "turn/completed",
      params: {
        threadId,
        turn: {
          id: turnId,
          status: "completed",
          items: [{ type: "agentMessage", text: "also private" }],
        },
      },
    },
    { now: "2026-08-15T00:00:02.000Z" },
  );
  lifecycle.endTurnLifecycleConnection(connection.epoch, {
    now: "2026-08-15T00:00:03.000Z",
  });

  const state = lifecycle.readTurnLifecycle();
  assert.equal(state.connection.state, "disconnected");
  assert.equal(state.observationSequence, 2);
  assert.equal(state.threads[threadId].activeTurnId, null);
  assert.deepEqual(state.threads[threadId].recentTerminalTurnIds, [turnId]);
  const raw = readFileSync(lifecycle.TURN_LIFECYCLE_PATH, "utf8");
  assert.doesNotMatch(raw, /must not persist|also private|userMessage|agentMessage/);
});

test("catch-up replaces activity from one bounded recent-turn page", () => {
  const nextTurn = "31345678-1234-4234-8234-123456789abc";
  lifecycle.observeTurnLifecycleCatchUp(
    { id: threadId, status: { type: "active" } },
    {
      data: [
        { id: nextTurn, status: "inProgress", items: [{ text: "not retained" }] },
        { id: turnId, status: "completed", items: [] },
      ],
      nextCursor: "older-page",
    },
    { now: "2026-08-15T00:00:04.000Z" },
  );
  const projection = lifecycle.readTurnLifecycle().threads[threadId];
  assert.equal(projection.activeTurnId, nextTurn);
  assert.equal(projection.source, "catch-up");
  assert.equal(existsSync(lifecycle.TURN_LIFECYCLE_PATH), true);
  assert.doesNotMatch(readFileSync(lifecycle.TURN_LIFECYCLE_PATH, "utf8"), /not retained/);

  lifecycle.observeTurnLifecycleCatchUp(
    { id: threadId, status: { type: "idle" } },
    { data: [{ id: nextTurn, status: "inProgress" }], nextCursor: null },
    { now: "2026-08-15T00:00:05.000Z" },
  );
  assert.equal(lifecycle.readTurnLifecycle().threads[threadId].activeTurnId, null);
});

test("unknown notifications do not advance the lifecycle sequence", () => {
  const before = lifecycle.readTurnLifecycle().observationSequence;
  assert.equal(
    lifecycle.observeTurnLifecycleNotification({ method: "item/completed", params: {} }),
    null,
  );
  assert.equal(lifecycle.readTurnLifecycle().observationSequence, before);
});
