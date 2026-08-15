import { AppServerError } from "./app-server-client.js";

export const RECENT_TURN_LIMIT = 8;
export const MAX_TURN_SEARCH_PAGES = 8;
export const CLIENT_MESSAGE_SEARCH_PAGE_SIZE = 64;
export const MAX_CLIENT_MESSAGE_SEARCH_PAGES = 8;
const TURN_ID_PATTERN = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
export const TERMINAL_TURN_STATUSES = Object.freeze([
  "completed",
  "failed",
  "interrupted",
  "cancelled",
  "canceled",
]);

export function isTerminalTurnStatus(status) {
  return TERMINAL_TURN_STATUSES.includes(status);
}

export function summarizeTurnLifecycle(page) {
  const turns = Array.isArray(page?.data) ? page.data : [];
  return {
    activeTurnId:
      turns.find(
        (turn) =>
          turn?.status === "inProgress" && TURN_ID_PATTERN.test(turn.id || ""),
      )?.id || null,
    recentTerminalTurnIds: turns
      .filter(
        (turn) =>
          TURN_ID_PATTERN.test(turn?.id || "") && isTerminalTurnStatus(turn.status),
      )
      .map((turn) => turn.id),
    recentTurnWindowComplete: !page?.nextCursor,
  };
}

function boundedPositiveInteger(value, fallback, maximum) {
  if (!Number.isSafeInteger(value) || value < 1) return fallback;
  return Math.min(value, maximum);
}

export async function readThreadMetadata(client, threadId) {
  const result = await client.request("thread/read", {
    threadId,
    includeTurns: false,
  });
  return result.thread;
}

export async function listRecentTurns(
  client,
  threadId,
  { limit = RECENT_TURN_LIMIT, cursor = null, itemsView = "notLoaded" } = {},
) {
  const params = {
    threadId,
    limit: boundedPositiveInteger(limit, RECENT_TURN_LIMIT, 64),
    sortDirection: "desc",
    itemsView,
  };
  if (cursor) params.cursor = cursor;
  return client.request("thread/turns/list", params);
}

export async function readThreadForInput(
  client,
  threadOrId,
  { allowResume = true } = {},
) {
  let thread =
    typeof threadOrId === "string"
      ? await readThreadMetadata(client, threadOrId)
      : threadOrId;

  if (thread.status?.type === "notLoaded") {
    if (!allowResume) {
      const error = new AppServerError(
        "thread is not loaded and may have an external rollout writer; attach it with cxmsg before delivery",
      );
      error.code = "EEXTERNALWRITERUNVERIFIED";
      throw error;
    }
    const resumed = await client.request("thread/resume", {
      threadId: thread.id,
      includeTurns: false,
    });
    thread = resumed.thread;
  }

  if (thread.status?.type !== "active") {
    return { ...thread, turns: [] };
  }

  if ((thread.turns || []).some((turn) => turn.status === "inProgress")) {
    return thread;
  }

  const page = await listRecentTurns(client, thread.id, {
    itemsView: "notLoaded",
  });
  const turns = page.data || [];
  if (!turns.some((turn) => turn.status === "inProgress")) {
    throw new AppServerError(
      `active thread has no in-progress turn in its ${RECENT_TURN_LIMIT}-turn activity window`,
    );
  }
  return { ...thread, turns };
}

export async function findThreadTurn(
  client,
  threadId,
  turnId,
  {
    pageSize = RECENT_TURN_LIMIT,
    maxPages = MAX_TURN_SEARCH_PAGES,
    itemsView = "summary",
  } = {},
) {
  const boundedPageSize = boundedPositiveInteger(
    pageSize,
    RECENT_TURN_LIMIT,
    64,
  );
  const boundedMaxPages = boundedPositiveInteger(
    maxPages,
    MAX_TURN_SEARCH_PAGES,
    64,
  );
  let cursor = null;
  for (let pageIndex = 0; pageIndex < boundedMaxPages; pageIndex += 1) {
    const page = await listRecentTurns(client, threadId, {
      limit: boundedPageSize,
      cursor,
      itemsView,
    });
    const turn = (page.data || []).find((candidate) => candidate.id === turnId);
    if (turn) return turn;
    if (!page.nextCursor) return null;
    cursor = page.nextCursor;
  }
  return null;
}

export async function findClientUserMessage(
  client,
  threadId,
  clientId,
  {
    pageSize = CLIENT_MESSAGE_SEARCH_PAGE_SIZE,
    maxPages = MAX_CLIENT_MESSAGE_SEARCH_PAGES,
  } = {},
) {
  const boundedPageSize = boundedPositiveInteger(
    pageSize,
    CLIENT_MESSAGE_SEARCH_PAGE_SIZE,
    CLIENT_MESSAGE_SEARCH_PAGE_SIZE,
  );
  const boundedMaxPages = boundedPositiveInteger(
    maxPages,
    MAX_CLIENT_MESSAGE_SEARCH_PAGES,
    64,
  );
  let cursor = null;
  for (let pageIndex = 0; pageIndex < boundedMaxPages; pageIndex += 1) {
    const page = await listRecentTurns(client, threadId, {
      limit: boundedPageSize,
      cursor,
      itemsView: "summary",
    });
    const match = (page.data || []).find((turn) =>
      (turn.items || []).some(
        (item) => item?.type === "userMessage" && item.clientId === clientId,
      ),
    );
    if (match) {
      return {
        state: "accepted",
        turnId: match.id,
        pagesInspected: pageIndex + 1,
      };
    }
    if (!page.nextCursor) {
      return {
        state: "not-observed",
        complete: true,
        pagesInspected: pageIndex + 1,
      };
    }
    cursor = page.nextCursor;
  }
  return {
    state: "not-observed",
    complete: false,
    pagesInspected: boundedMaxPages,
  };
}
