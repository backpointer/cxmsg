import assert from "node:assert/strict";
import test from "node:test";
import {
  findClientUserMessage,
  findFinalTurnResult,
  findThreadTurn,
  readThreadForInput,
  readThreadMetadata,
  summarizeTurnLifecycle,
} from "../src/thread-activity.js";

test("turn lifecycle summary exposes IDs and statuses without turn contents", () => {
  const summary = summarizeTurnLifecycle({
    data: [
      { id: "12345678-1234-4234-8234-123456789abc", status: "inProgress", items: [{ text: "private" }] },
      { id: "22345678-1234-4234-8234-123456789abc", status: "completed", items: [{ text: "private" }] },
      { id: "32345678-1234-4234-8234-123456789abc", status: "failed", error: "private" },
      { id: "unbounded-invalid", status: "completed", items: [{ text: "private" }] },
    ],
    nextCursor: "older",
  });
  assert.deepEqual(summary, {
    activeTurnId: "12345678-1234-4234-8234-123456789abc",
    recentTerminalTurnIds: [
      "22345678-1234-4234-8234-123456789abc",
      "32345678-1234-4234-8234-123456789abc",
    ],
    recentTurnWindowComplete: false,
  });
  assert.doesNotMatch(JSON.stringify(summary), /private/);
});

test("thread activity reads metadata and the active turn without full history", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (method === "thread/read") {
        return {
          thread: {
            id: "thread-1",
            status: { type: "active" },
            turns: [],
          },
        };
      }
      if (method === "thread/turns/list") {
        return {
          data: [{ id: "turn-active", status: "inProgress", items: [] }],
          nextCursor: null,
        };
      }
      assert.fail(`unexpected request: ${method}`);
    },
  };

  const metadata = await readThreadMetadata(client, "thread-1");
  const active = await readThreadForInput(client, metadata);

  assert.equal(active.turns[0].id, "turn-active");
  assert.deepEqual(calls, [
    {
      method: "thread/read",
      params: { threadId: "thread-1", includeTurns: false },
    },
    {
      method: "thread/turns/list",
      params: {
        threadId: "thread-1",
        limit: 8,
        sortDirection: "desc",
        itemsView: "notLoaded",
      },
    },
  ]);
});

test("turn lookup paginates bounded summary pages", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (!params.cursor) {
        return {
          data: [{ id: "newer-turn", status: "completed", items: [] }],
          nextCursor: "page-2",
        };
      }
      return {
        data: [
          {
            id: "target-turn",
            status: "completed",
            items: [
              {
                type: "agentMessage",
                phase: "final_answer",
                text: "bounded result",
              },
            ],
          },
        ],
        nextCursor: null,
      };
    },
  };

  const turn = await findThreadTurn(client, "thread-1", "target-turn", {
    pageSize: 1,
    maxPages: 2,
  });

  assert.equal(turn.id, "target-turn");
  assert.deepEqual(
    calls.map(({ method, params }) => ({
      method,
      cursor: params.cursor || null,
      itemsView: params.itemsView,
      limit: params.limit,
    })),
    [
      {
        method: "thread/turns/list",
        cursor: null,
        itemsView: "summary",
        limit: 1,
      },
      {
        method: "thread/turns/list",
        cursor: "page-2",
        itemsView: "summary",
        limit: 1,
      },
    ],
  );
});

test("final result lookup reads one reverse-ordered item per bounded page", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (!params.cursor) {
        return {
          data: [
            {
              turnId: "target-turn",
              item: { type: "reasoning", id: "reasoning-1" },
            },
          ],
          nextCursor: "older-item",
        };
      }
      return {
        data: [
          {
            turnId: "target-turn",
            item: {
              type: "agentMessage",
              phase: "final_answer",
              text: "bounded final result",
            },
          },
        ],
        nextCursor: null,
      };
    },
  };

  const result = await findFinalTurnResult(
    client,
    "thread-1",
    "target-turn",
    { maxPages: 2 },
  );
  assert.deepEqual(result, {
    state: "available",
    result: "bounded final result",
  });
  assert.deepEqual(calls, [
    {
      method: "thread/items/list",
      params: {
        threadId: "thread-1",
        turnId: "target-turn",
        limit: 1,
        sortDirection: "desc",
      },
    },
    {
      method: "thread/items/list",
      params: {
        threadId: "thread-1",
        turnId: "target-turn",
        limit: 1,
        sortDirection: "desc",
        cursor: "older-item",
      },
    },
  ]);
});

test("client message reconciliation reports only positive bounded acceptance evidence", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      if (!params.cursor) {
        return {
          data: [
            {
              id: "turn-newer",
              items: [{ type: "agentMessage", id: "agent-1", text: "private" }],
            },
          ],
          nextCursor: "page-2",
        };
      }
      return {
        data: [
          {
            id: "turn-accepted",
            items: [
              {
                type: "userMessage",
                id: "user-1",
                clientId: "logical-message-1",
                content: [{ type: "text", text: "private body" }],
              },
            ],
          },
        ],
        nextCursor: null,
      };
    },
  };

  const accepted = await findClientUserMessage(
    client,
    "thread-1",
    "logical-message-1",
    { pageSize: 1, maxPages: 2 },
  );
  assert.deepEqual(accepted, {
    state: "accepted",
    turnId: "turn-accepted",
    pagesInspected: 2,
  });
  assert.deepEqual(calls, [
    {
      method: "thread/turns/list",
      params: {
        threadId: "thread-1",
        limit: 1,
        sortDirection: "desc",
        itemsView: "summary",
      },
    },
    {
      method: "thread/turns/list",
      params: {
        threadId: "thread-1",
        limit: 1,
        sortDirection: "desc",
        itemsView: "summary",
        cursor: "page-2",
      },
    },
  ]);

  const incomplete = await findClientUserMessage(
    {
      async request() {
        return { data: [], nextCursor: "more" };
      },
    },
    "thread-1",
    "missing",
    { pageSize: 1, maxPages: 1 },
  );
  assert.deepEqual(incomplete, {
    state: "not-observed",
    complete: false,
    pagesInspected: 1,
  });
});

test("unloaded input threads resume without hydrating turn history", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      return {
        thread: {
          id: "thread-1",
          status: { type: "idle" },
        },
      };
    },
  };

  const thread = await readThreadForInput(client, {
    id: "thread-1",
    status: { type: "notLoaded" },
  });

  assert.equal(thread.status.type, "idle");
  assert.deepEqual(calls, [
    {
      method: "thread/resume",
      params: { threadId: "thread-1", includeTurns: false },
    },
  ]);
});

test("unmanaged unloaded threads are not resumed across a possible external writer", async () => {
  const calls = [];
  const client = {
    async request(method, params) {
      calls.push({ method, params });
      assert.fail("an unmanaged notLoaded thread must not receive App Server requests");
    },
  };

  await assert.rejects(
    readThreadForInput(
      client,
      { id: "thread-external", status: { type: "notLoaded" } },
      { allowResume: false },
    ),
    (error) =>
      error.code === "EEXTERNALWRITERUNVERIFIED" &&
      /external rollout writer/.test(error.message),
  );
  assert.deepEqual(calls, []);
});
