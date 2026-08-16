import { existsSync, readdirSync } from "node:fs";
import path from "node:path";
import {
  conversationSummaryMatchesRecord,
  scanConversationSummaries,
} from "./conversation-summaries.js";
import { readDeliveryLedger } from "./delivery-ledger.js";
import { withFileLock } from "./file-lock.js";
import { readDirectConversation } from "./conversations.js";
import { readGroupConversation } from "./group-conversations.js";
import { readJob } from "./jobs.js";
import { readNode, readNodeTombstone } from "./node-directory.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

const NODE_KEY_PATTERN = /^(codex|claude):([0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12})$/i;
const RECENT_LIMIT = 200;
const CONVERSATIONS_DIR = path.join(CXMSG_STATE_DIR, "conversations");
const CONVERSATIONS_LOCK_PATH = path.join(CONVERSATIONS_DIR, "mutation.lock");

function normalizeNodeKey(value) {
  const match = NODE_KEY_PATTERN.exec(value || "");
  if (!match) throw new Error("recent Conversations require a stable Node key");
  return `${match[1].toLowerCase()}:${match[2].toLowerCase()}`;
}

function parseNodeKey(value) {
  const nodeKey = normalizeNodeKey(value);
  const separator = nodeKey.indexOf(":");
  return {
    nodeKey,
    runtimeKind: nodeKey.slice(0, separator),
    nativeId: nodeKey.slice(separator + 1),
  };
}

function nodePresentation(value) {
  const identity = parseNodeKey(value);
  const node = readNode(identity.runtimeKind, identity.nativeId);
  if (node?.nodeKey === identity.nodeKey) {
    const alias = [...(node.aliases || [])].sort(
      (left, right) =>
        Date.parse(right.lastSeenAt) - Date.parse(left.lastSeenAt) ||
        left.value.localeCompare(right.value),
    )[0];
    return {
      nodeKey: identity.nodeKey,
      alias: alias?.value || null,
      state: "live",
    };
  }
  return {
    nodeKey: identity.nodeKey,
    alias: null,
    state: readNodeTombstone(identity.runtimeKind, identity.nativeId)
      ? "tombstoned"
      : "missing",
  };
}

function directSourceMatches(record) {
  if (record.lastSourceKind === "delivery-ledger") {
    const source = readDeliveryLedger(record.lastMessageId);
    return Boolean(
      source?.logicalMessage?.conversationId === record.conversationId &&
        source.logicalMessage.conversationSequence === record.lastSequence,
    );
  }
  const source = readJob(record.lastMessageId);
  return Boolean(
    source?.kind === "claude-delivery" &&
      source.conversation?.conversationId === record.conversationId &&
      source.conversation?.sequence === record.lastSequence,
  );
}

function groupSourceMatches(record) {
  const source = readDeliveryLedger(record.lastMessageId);
  return Boolean(
    source?.logicalMessage?.group?.conversationId === record.conversationId &&
      source.logicalMessage.group.sequence === record.lastSequence,
  );
}

function directProjection(record, nodeKey) {
  if (!record.currentMembers.includes(nodeKey) || !record.lastActivityAt) return null;
  const peerNodeKey = record.currentMembers.find((member) => member !== nodeKey);
  return {
    conversationId: record.conversationId,
    kind: "direct",
    peerNodeKey,
    peerAlias: null,
    peerState: null,
    label: null,
    lastActivityAt: record.lastActivityAt,
    lastMessageId: record.lastMessageId,
    lastDirection:
      record.lastSenderNodeKey === nodeKey ? "outbound" : "inbound",
    messageCount: record.messageCount,
    unread: null,
  };
}

function groupProjection(record, nodeKey) {
  if (!record.currentMembers.includes(nodeKey) || !record.lastActivityAt) return null;
  return {
    conversationId: record.conversationId,
    kind: "group",
    peerNodeKey: null,
    peerAlias: null,
    peerState: null,
    label: record.label,
    lastActivityAt: record.lastActivityAt,
    lastMessageId: record.lastMessageId,
    lastDirection:
      record.lastSenderNodeKey === nodeKey ? "outbound" : "inbound",
    messageCount: record.messageCount,
    unread: null,
  };
}

function conversationRecordKeys() {
  const keys = new Set();
  for (const kind of ["direct", "group"]) {
    const directory = path.join(CONVERSATIONS_DIR, kind);
    if (!existsSync(directory)) continue;
    for (const name of readdirSync(directory)) {
      if (!/^[0-9a-f-]{36}\.json$/i.test(name)) continue;
      keys.add(`${kind}:${name.slice(0, -5).toLowerCase()}`);
    }
  }
  return keys;
}

function missingSummaryCountForNode(recordKeys, summaryKeys, nodeKey) {
  let count = 0;
  let unscoped = 0;
  let inspected = 0;
  for (const key of recordKeys) {
    if (summaryKeys.has(key)) continue;
    if (inspected >= 32) {
      unscoped += 1;
      continue;
    }
    inspected += 1;
    const separator = key.indexOf(":");
    const kind = key.slice(0, separator);
    const conversationId = key.slice(separator + 1);
    const record =
      kind === "direct"
        ? readDirectConversation(conversationId)
        : readGroupConversation(conversationId);
    const members =
      kind === "direct"
        ? record?.currentMembers
        : record?.membershipSnapshots?.at(-1)?.members;
    if (members?.includes(nodeKey)) count += 1;
  }
  return { count, unscoped };
}

function recentConversationsUnlocked(
  nodeKey,
  { limit = 50, kind = "all" } = {},
) {
  const records = [];
  const scan = scanConversationSummaries();
  const recordKeys = conversationRecordKeys();
  const summaryKeys = new Set(
    scan.records.map((summary) => `${summary.kind}:${summary.conversationId}`),
  );
  const missing = missingSummaryCountForNode(recordKeys, summaryKeys, nodeKey);
  const diagnostics = {
    summaryMissing: missing.count,
    summaryInspectionTruncated: missing.unscoped,
    summaryStale: 0,
    sourceUnverified: 0,
    invalidSummaries: scan.diagnostics.invalidRecords,
    staleArtifacts: scan.diagnostics.staleArtifacts,
    unexpectedEntries: scan.diagnostics.unexpectedEntries,
    validationTruncated: 0,
  };
  const summaries = scan.records.filter((summary) => {
    const matched = conversationSummaryMatchesRecord(summary);
    if (!matched && summary.currentMembers.includes(nodeKey)) {
      diagnostics.summaryStale += 1;
    }
    return matched;
  });
  if (kind !== "group") {
    records.push(
      ...summaries
        .filter((record) => record.kind === "direct")
        .map((record) => directProjection(record, nodeKey))
        .filter(Boolean),
    );
  }
  if (kind !== "direct") {
    records.push(
      ...summaries
        .filter((record) => record.kind === "group")
        .map((record) => groupProjection(record, nodeKey))
        .filter(Boolean),
    );
  }
  const ordered = records.sort(
      (left, right) =>
        Date.parse(right.lastActivityAt) - Date.parse(left.lastActivityAt) ||
        left.conversationId.localeCompare(right.conversationId),
    );
  if (ordered.length > limit) {
    diagnostics.validationTruncated = ordered.length - limit;
  }
  const selected = [];
  for (const record of ordered.slice(0, limit)) {
    const summary = summaries.find(
      (candidate) =>
        candidate.kind === record.kind &&
        candidate.conversationId === record.conversationId,
    );
    const sourceVerified =
      record.kind === "direct"
        ? directSourceMatches(summary)
        : groupSourceMatches(summary);
    if (!sourceVerified) {
      diagnostics.sourceUnverified += 1;
      continue;
    }
    if (record.kind === "direct") {
      const peer = nodePresentation(record.peerNodeKey);
      record.peerAlias = peer.alias;
      record.peerState = peer.state;
    }
    selected.push({ ...record, sourceVerified: true, degraded: false });
  }
  const complete = Object.entries(diagnostics).every(
    ([key, value]) => key === "validationTruncated" || value === 0,
  );
  return {
    conversations: selected,
    diagnostics,
    complete,
    hasMore: diagnostics.validationTruncated > 0,
  };
}

export async function listRecentConversations(nodeKey, options = {}) {
  nodeKey = normalizeNodeKey(nodeKey);
  const { limit = 50, kind = "all" } = options;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > RECENT_LIMIT) {
    throw new Error(`recent Conversation limit must be between 1 and ${RECENT_LIMIT}`);
  }
  if (!["all", "direct", "group"].includes(kind)) {
    throw new Error("recent Conversation kind must be all, direct, or group");
  }
  if (!existsSync(CONVERSATIONS_DIR)) {
    return {
      conversations: [],
      diagnostics: {
        summaryMissing: 0,
        summaryInspectionTruncated: 0,
        summaryStale: 0,
        sourceUnverified: 0,
        invalidSummaries: 0,
        staleArtifacts: 0,
        unexpectedEntries: 0,
        validationTruncated: 0,
      },
      complete: true,
      hasMore: false,
    };
  }
  return withFileLock(CONVERSATIONS_LOCK_PATH, async () =>
    recentConversationsUnlocked(nodeKey, { limit, kind }),
  );
}
