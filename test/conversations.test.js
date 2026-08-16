import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  lstatSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-conversations-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-conversation-project-"));
process.env.CXMSG_STATE_DIR = stateDir;

const directory = await import(`../src/node-directory.js?conversation=${Date.now()}`);
const conversations = await import(`../src/conversations.js?conversation=${Date.now()}`);
const registry = await import(`../src/registry.js?conversation=${Date.now()}`);
const route = await import(`../src/route-admission.js?conversation=${Date.now()}`);
const ledger = await import(`../src/delivery-ledger.js?conversation=${Date.now()}`);
const claude = await import(`../src/claude-delivery.js?conversation=${Date.now()}`);
const recent = await import(`../src/recent-conversations.js?conversation=${Date.now()}`);
const summaries = await import(
  `../src/conversation-summaries.js?conversation=${Date.now()}`
);
const cxmsgPath = path.resolve("bin/cxmsg.js");

function cxmsg(...args) {
  return spawnSync(process.execPath, [cxmsgPath, ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
  });
}

const ids = {
  project: "12345678-4234-4234-8234-123456789abc",
  first: "22345678-4234-4234-8234-123456789abc",
  second: "32345678-4234-4234-8234-123456789abc",
  successor: "42345678-4234-4234-8234-123456789abc",
  sender: "52345678-4234-4234-8234-123456789abc",
  target: "62345678-4234-4234-8234-123456789abc",
  firstMessage: "72345678-4234-4234-8234-123456789abc",
  replyMessage: "82345678-4234-4234-8234-123456789abc",
  migratedMessage: "92345678-4234-4234-8234-123456789abc",
  crossMessage: "93345678-4234-4234-8234-123456789abc",
  routedMessage: "a2345678-4234-4234-8234-123456789abc",
  routedReply: "b2345678-4234-4234-8234-123456789abc",
  claudeReply: "c2345678-4234-4234-8234-123456789abc",
};

await directory.ensureProject({
  routingId: "conversation-fixture",
  root: projectRoot,
  projectId: ids.project,
});
for (const [runtimeKind, nativeId, displayName] of [
  ["codex", ids.first, "first"],
  ["claude", ids.second, "second"],
  ["codex", ids.successor, "successor"],
  ["codex", ids.sender, "sender"],
  ["codex", ids.target, "target"],
]) {
  await directory.upsertNode({
    runtimeKind,
    nativeId,
    displayName,
    projectId: ids.project,
  });
}

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(projectRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("Direct Conversation identity is canonical for an unordered Node pair", async () => {
  const firstKey = `codex:${ids.first}`;
  const secondKey = `claude:${ids.second}`;
  const created = await conversations.ensureDirectConversation(firstKey, secondKey);
  const reversed = await conversations.ensureDirectConversation(secondKey, firstKey);
  assert.equal(created.created, true);
  assert.equal(reversed.created, false);
  assert.equal(created.conversation.conversationId, reversed.conversation.conversationId);
  assert.equal(
    created.conversation.conversationId,
    conversations.directConversationId(firstKey, secondKey),
  );
});

test("Direct Conversation assigns one durable sequence and deduplicates retries", async () => {
  const first = await conversations.recordDirectMessage({
    logicalMessageId: ids.firstMessage,
    senderNodeKey: `codex:${ids.first}`,
    recipientNodeKey: `claude:${ids.second}`,
    sourceKind: "delivery-ledger",
  });
  const repeated = await conversations.recordDirectMessage({
    logicalMessageId: ids.firstMessage,
    senderNodeKey: `codex:${ids.first}`,
    recipientNodeKey: `claude:${ids.second}`,
    sourceKind: "delivery-ledger",
  });
  const reply = await conversations.recordDirectMessage({
    logicalMessageId: ids.replyMessage,
    senderNodeKey: `claude:${ids.second}`,
    recipientNodeKey: `codex:${ids.first}`,
    replyToMessageId: ids.firstMessage,
    sourceKind: "delivery-ledger",
  });
  assert.equal(first.message.sequence, 1);
  assert.equal(repeated.created, false);
  assert.equal(repeated.message.sequence, 1);
  assert.equal(reply.message.sequence, 2);
  assert.equal(reply.message.crossConversationReply, false);
  await assert.rejects(
    conversations.recordDirectMessage({
      logicalMessageId: ids.firstMessage,
      senderNodeKey: `claude:${ids.second}`,
      recipientNodeKey: `codex:${ids.first}`,
      sourceKind: "delivery-ledger",
    }),
    /idempotency conflict/,
  );
  const staleCache = structuredClone(reply.conversation);
  staleCache.lastActivityAt = "2000-01-01T00:00:00.000Z";
  staleCache.lastMessageId = ids.firstMessage;
  staleCache.lastSenderNodeKey = `codex:${ids.first}`;
  assert.equal(conversations.validDirectConversationRecord(staleCache), true);
  assert.equal(
    conversations.publicDirectConversation(staleCache).lastMessageId,
    ids.replyMessage,
  );
});

test("successor continuation requires explicit Conversation migration", async () => {
  await directory.addSuccessor({
    predecessorNodeKey: `codex:${ids.first}`,
    successorNodeKey: `codex:${ids.successor}`,
  });
  const conversation = conversations.listDirectConversations()[0];
  const activityBeforeMigration = conversations.publicDirectConversation(
    conversation,
  ).lastActivityAt;
  const migrated = await conversations.migrateDirectConversationMember({
    conversationId: conversation.conversationId,
    predecessorNodeKey: `codex:${ids.first}`,
    successorNodeKey: `codex:${ids.successor}`,
  });
  assert.equal(migrated.migrated, true);
  assert.equal(
    conversations.publicDirectConversation(migrated.conversation).lastActivityAt,
    activityBeforeMigration,
  );
  assert.equal(
    (await conversations.migrateDirectConversationMember({
      conversationId: conversation.conversationId,
      predecessorNodeKey: `codex:${ids.first}`,
      successorNodeKey: `codex:${ids.successor}`,
    })).migrated,
    false,
  );
  const continued = await conversations.recordDirectMessage({
    logicalMessageId: ids.migratedMessage,
    senderNodeKey: `codex:${ids.successor}`,
    recipientNodeKey: `claude:${ids.second}`,
    replyToMessageId: ids.replyMessage,
    sourceKind: "claude-job",
  });
  assert.equal(continued.conversation.conversationId, conversation.conversationId);
  assert.equal(continued.message.sequence, 3);
  assert.equal(continued.message.crossConversationReply, false);

  await directory.tombstoneNode("codex", ids.first, { reason: "succeeded" });
  const projected = conversations.publicDirectConversation(
    conversations.readDirectConversation(conversation.conversationId),
  );
  assert.equal(
    projected.members.find((member) => member.nodeKey === `codex:${ids.first}`).state,
    "tombstoned",
  );
  assert.ok(
    projected.currentMembers.some(
      (member) => member.nodeKey === `codex:${ids.successor}`,
    ),
  );
});

test("a reply from a different Node pair keeps bounded cross-Conversation provenance", async () => {
  const parent = conversations.listDirectConversations().find((record) =>
    record.messages.some((message) => message.logicalMessageId === ids.firstMessage),
  );
  const reply = await conversations.recordDirectMessage({
    logicalMessageId: ids.crossMessage,
    senderNodeKey: `codex:${ids.sender}`,
    recipientNodeKey: `claude:${ids.second}`,
    replyToMessageId: ids.firstMessage,
    sourceKind: "delivery-ledger",
  });
  assert.notEqual(reply.conversation.conversationId, parent.conversationId);
  assert.equal(reply.message.parentConversationId, parent.conversationId);
  assert.equal(reply.message.crossConversationReply, true);
});

test("admitted Codex routes attach Conversation ordering without changing delivery", async () => {
  registry.writeSessionRecord({
    name: "sender",
    threadId: ids.sender,
    cwd: projectRoot,
  });
  registry.writeSessionRecord({
    name: "target",
    threadId: ids.target,
    cwd: projectRoot,
  });
  const dispatched = async ({ logicalMessageId }) => ({
    delivery: "started",
    turnId: "d2345678-4234-4234-8234-123456789abc",
    logicalMessageId,
  });
  const first = await route.routePeerMessage(
    {
      from: "sender",
      target: "target",
      message: "conversation route fixture",
      logicalMessageId: ids.routedMessage,
    },
    dispatched,
    { log: async () => {} },
  );
  assert.equal(first.status, "turn_started");
  const stored = ledger.readDeliveryLedger(ids.routedMessage);
  assert.ok(stored.logicalMessage.conversationId);
  assert.equal(stored.logicalMessage.conversationSequence, 1);

  const reply = route.planPeerReply({
    from: "target",
    replyToMessageId: ids.routedMessage,
    logicalMessageId: ids.routedReply,
  });
  await route.routePeerMessage(
    { ...reply, message: "conversation reply fixture" },
    dispatched,
    { log: async () => {} },
  );
  const storedReply = ledger.readDeliveryLedger(ids.routedReply);
  assert.equal(
    storedReply.logicalMessage.conversationId,
    stored.logicalMessage.conversationId,
  );
  assert.equal(storedReply.logicalMessage.conversationSequence, 2);
  const history = conversations.directConversationHistory(
    stored.logicalMessage.conversationId,
  );
  assert.equal(history.length, 2);
  assert.deepEqual(history.map((item) => item.status), ["turn_started", "turn_started"]);
});

test("cross-runtime Claude reply Jobs reuse the parent Conversation", async () => {
  const parent = await conversations.recordDirectMessage({
    logicalMessageId: "e2345678-4234-4234-8234-123456789abc",
    senderNodeKey: `claude:${ids.second}`,
    recipientNodeKey: `codex:${ids.sender}`,
    sourceKind: "delivery-ledger",
  });
  const job = await claude.createClaudeDeliveryJob({
    from: "sender",
    sourceRecord: registry.readSessionRecord("sender"),
    peer: {
      name: "claude-peer",
      sessionId: ids.second,
      address: "/tmp/fixture.sock",
    },
    message: "cross-runtime response",
    logicalMessageId: ids.claudeReply,
    replyToMessageId: parent.message.logicalMessageId,
  });
  assert.equal(job.conversation.conversationId, parent.conversation.conversationId);
  assert.equal(job.conversation.sequence, parent.message.sequence + 1);
  const history = conversations.directConversationHistory(
    parent.conversation.conversationId,
  );
  assert.equal(history.at(-1).sourceKind, "claude-job");
  assert.equal(history.at(-1).status, "queued");
});

test("per-Node recent Conversations are stable, bounded, and activity ordered", async () => {
  const nodeKey = `codex:${ids.sender}`;
  const result = await recent.listRecentConversations(nodeKey, {
    kind: "direct",
  });
  assert.equal(result.complete, true);
  const records = result.conversations;
  assert.ok(records.length >= 2);
  assert.ok(
    records.every(
      (record) =>
        record.kind === "direct" &&
        record.peerNodeKey !== nodeKey &&
        record.unread === null,
    ),
  );
  assert.deepEqual(
    [...records].sort(
      (left, right) =>
        Date.parse(right.lastActivityAt) - Date.parse(left.lastActivityAt) ||
        left.conversationId.localeCompare(right.conversationId),
    ),
    records,
  );
  assert.doesNotMatch(
    JSON.stringify(records),
    /conversation route fixture|cross-runtime response|tmp\/fixture|contentRef/,
  );

  const cli = cxmsg("conversation", "recent", nodeKey, "--kind", "direct", "--json");
  assert.equal(cli.status, 0, cli.stderr);
  assert.deepEqual(JSON.parse(cli.stdout), result);
  const textList = cxmsg("conversation", "recent", nodeKey, "--kind", "direct");
  assert.equal(textList.status, 0, textList.stderr);
  assert.match(textList.stdout, /\tlive\n/);

  const claudeJobPath = path.join(stateDir, "jobs", `${ids.claudeReply}.json`);
  const hiddenJobPath = `${claudeJobPath}.hidden`;
  renameSync(claudeJobPath, hiddenJobPath);
  try {
    const degraded = await recent.listRecentConversations(nodeKey, {
      kind: "direct",
    });
    assert.equal(degraded.complete, false);
    assert.equal(degraded.diagnostics.sourceUnverified, 1);
    assert.equal(
      degraded.conversations.some(
        (record) => record.lastMessageId === ids.claudeReply,
      ),
      false,
    );
  } finally {
    renameSync(hiddenJobPath, claudeJobPath);
  }

  const hidden = `${conversations.DIRECT_CONVERSATIONS_DIR}-hidden`;
  renameSync(conversations.DIRECT_CONVERSATIONS_DIR, hidden);
  try {
    const degraded = await recent.listRecentConversations(nodeKey, {
      kind: "direct",
    });
    assert.equal(degraded.complete, false);
    assert.equal(degraded.conversations.length, 0);
    assert.ok(degraded.diagnostics.summaryStale >= 1);
  } finally {
    renameSync(hidden, conversations.DIRECT_CONVERSATIONS_DIR);
  }
  assert.equal(lstatSync(summaries.CONVERSATION_SUMMARIES_DIR).mode & 0o077, 0);
  for (const name of readdirSync(summaries.CONVERSATION_SUMMARIES_DIR)) {
    assert.equal(
      lstatSync(path.join(summaries.CONVERSATION_SUMMARIES_DIR, name)).mode &
        0o077,
      0,
    );
  }

  const missingSummary = path.join(
    summaries.CONVERSATION_SUMMARIES_DIR,
    `direct--${records[0].conversationId}.json`,
  );
  const missingBytes = readFileSync(missingSummary);
  unlinkSync(missingSummary);
  try {
    const degraded = await recent.listRecentConversations(nodeKey, {
      kind: "direct",
    });
    assert.equal(degraded.complete, false);
    assert.equal(degraded.diagnostics.summaryMissing, 1);
  } finally {
    writeFileSync(missingSummary, missingBytes, { mode: 0o600 });
  }

  const invalidId = "f2345678-4234-4234-8234-123456789abc";
  const invalidSummary = path.join(
    summaries.CONVERSATION_SUMMARIES_DIR,
    `direct--${invalidId}.json`,
  );
  const staleTemporary = `${invalidSummary}.${invalidId}.tmp`;
  writeFileSync(invalidSummary, "{}\n", { mode: 0o600 });
  writeFileSync(staleTemporary, "{}\n", { mode: 0o600 });
  try {
    const degraded = await recent.listRecentConversations(nodeKey, {
      kind: "direct",
    });
    assert.equal(degraded.complete, false);
    assert.deepEqual(degraded.conversations, records);
    assert.equal(degraded.diagnostics.invalidSummaries, 1);
    assert.equal(degraded.diagnostics.staleArtifacts, 1);
  } finally {
    unlinkSync(invalidSummary);
    unlinkSync(staleTemporary);
  }
});

test("Conversation CLI exposes bounded metadata history without message bodies", () => {
  const listed = cxmsg("conversation", "list", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  const records = JSON.parse(listed.stdout);
  assert.ok(records.length >= 3);
  const selected = records.find((record) => record.messageCount === 2);
  const shown = cxmsg(
    "conversation",
    "show",
    selected.conversationId,
    "--json",
  );
  assert.equal(shown.status, 0, shown.stderr);
  const history = cxmsg(
    "conversation",
    "history",
    selected.conversationId,
    "--limit",
    "1",
    "--json",
  );
  assert.equal(history.status, 0, history.stderr);
  assert.equal(JSON.parse(history.stdout).length, 1);
  assert.doesNotMatch(history.stdout, /conversation route fixture|conversation reply fixture/);
  assert.equal(lstatSync(conversations.CONVERSATIONS_DIR).mode & 0o077, 0);
  assert.equal(lstatSync(conversations.DIRECT_CONVERSATIONS_DIR).mode & 0o077, 0);
  for (const name of readdirSync(conversations.DIRECT_CONVERSATIONS_DIR)) {
    assert.equal(
      lstatSync(path.join(conversations.DIRECT_CONVERSATIONS_DIR, name)).mode & 0o077,
      0,
    );
  }
});
