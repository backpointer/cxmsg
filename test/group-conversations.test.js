import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { lstatSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-groups-"));
const projectRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-group-project-"));
process.env.CXMSG_STATE_DIR = stateDir;

const directory = await import(`../src/node-directory.js?groups=${Date.now()}`);
const groups = await import(`../src/group-conversations.js?groups=${Date.now()}`);
const inbound = await import(`../src/inbound-policy.js?groups=${Date.now()}`);
const ledger = await import(`../src/delivery-ledger.js?groups=${Date.now()}`);
const scheduler = await import(`../src/scheduler.js?groups=${Date.now()}`);
const messaging = await import(`../src/messaging.js?groups=${Date.now()}`);
const recent = await import(`../src/recent-conversations.js?groups=${Date.now()}`);
const inspectors = await import(`../src/inspectors.js?groups=${Date.now()}`);
const cxmsgPath = path.resolve("bin/cxmsg.js");

function cxmsg(...args) {
  return spawnSync(process.execPath, [cxmsgPath, ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: stateDir },
  });
}

const ids = {
  project: "11345678-4234-4234-8234-123456789abc",
  conversation: "21345678-4234-4234-8234-123456789abc",
  first: "31345678-4234-4234-8234-123456789abc",
  second: "41345678-4234-4234-8234-123456789abc",
  third: "51345678-4234-4234-8234-123456789abc",
  fourth: "61345678-4234-4234-8234-123456789abc",
  firstMessage: "71345678-4234-4234-8234-123456789abc",
  replyMessage: "81345678-4234-4234-8234-123456789abc",
  crashMessage: "91345678-4234-4234-8234-123456789abc",
  cliMessage: "a1345678-4234-4234-8234-123456789abc",
  policyConversation: "b1345678-4234-4234-8234-123456789abc",
  mixedPolicyMessage: "c1345678-4234-4234-8234-123456789abc",
  allDeniedMessage: "d1345678-4234-4234-8234-123456789abc",
  inactivePolicyMessage: "e1345678-4234-4234-8234-123456789abc",
};
const nodeKeys = {
  first: `codex:${ids.first}`,
  second: `codex:${ids.second}`,
  third: `claude:${ids.third}`,
  fourth: `codex:${ids.fourth}`,
};

await directory.ensureProject({
  routingId: "group-fixture",
  root: projectRoot,
  projectId: ids.project,
});
for (const [runtimeKind, nativeId, displayName] of [
  ["codex", ids.first, "first"],
  ["codex", ids.second, "second"],
  ["claude", ids.third, "third"],
  ["codex", ids.fourth, "fourth"],
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

test("Group Conversation stores versioned membership independently", async () => {
  const created = await groups.ensureGroupConversation({
    conversationId: ids.conversation,
    label: "review-team",
    members: [nodeKeys.third, nodeKeys.first, nodeKeys.second],
  });
  assert.equal(created.created, true);
  assert.deepEqual(
    created.conversation.membershipSnapshots[0].members,
    [nodeKeys.first, nodeKeys.second, nodeKeys.third].sort(),
  );
  const repeated = await groups.ensureGroupConversation({
    label: "review-team",
    members: [nodeKeys.first, nodeKeys.second, nodeKeys.third],
  });
  assert.equal(repeated.created, false);
  const added = await groups.changeGroupMember({
    conversationId: ids.conversation,
    action: "add",
    nodeKey: nodeKeys.fourth,
  });
  assert.equal(added.changed, true);
  assert.equal(added.conversation.membershipVersion, 2);
  const duplicate = await groups.changeGroupMember({
    conversationId: ids.conversation,
    action: "add",
    nodeKey: nodeKeys.fourth,
  });
  assert.equal(duplicate.changed, false);
  assert.equal(duplicate.conversation.membershipVersion, 2);
});

test("store-only fan-out freezes recipients and starts zero attempts", async () => {
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const result = await groups.storeOnlyGroupMessage({
    conversationId: ids.conversation,
    senderNodeKey: nodeKeys.first,
    message: "bounded group context",
    logicalMessageId: ids.firstMessage,
    expiry,
  });
  assert.equal(result.message.membershipVersion, 2);
  assert.deepEqual(result.message.recipientNodeKeys, [
    nodeKeys.second,
    nodeKeys.fourth,
    nodeKeys.third,
  ].sort());
  assert.equal(result.ledger.groupDeliveries.length, 3);
  assert.ok(
    result.ledger.groupDeliveries.every(
      (delivery) =>
        delivery.wakePolicy === "store-only" &&
        delivery.state === "scheduled" &&
        delivery.attempts.length === 0 &&
        delivery.claim === null,
    ),
  );
  const failed = await ledger.appendStoreOnlyGroupDeliveryEvidence(
    ids.firstMessage,
    {
      targetNodeKey: nodeKeys.fourth,
      state: "failed",
      errorCode: "ETARGETUNREACHABLE",
    },
  );
  assert.equal(failed.created, true);
  assert.deepEqual(
    failed.record.groupDeliveries.map((delivery) => delivery.state).sort(),
    ["failed", "scheduled", "scheduled"],
  );
  const duplicateEvidence = await ledger.appendStoreOnlyGroupDeliveryEvidence(
    ids.firstMessage,
    {
      targetNodeKey: nodeKeys.fourth,
      state: "failed",
      errorCode: "ETARGETUNREACHABLE",
    },
  );
  assert.equal(duplicateEvidence.created, false);
  const removed = await groups.changeGroupMember({
    conversationId: ids.conversation,
    action: "remove",
    nodeKey: nodeKeys.fourth,
  });
  assert.equal(removed.conversation.membershipVersion, 3);
  const repeated = await groups.storeOnlyGroupMessage({
    conversationId: ids.conversation,
    senderNodeKey: nodeKeys.first,
    message: "bounded group context",
    logicalMessageId: ids.firstMessage,
    expiry,
  });
  assert.equal(repeated.created, false);
  assert.equal(repeated.message.sequence, result.message.sequence);
  assert.equal(repeated.ledger.groupDeliveries.length, 3);
  assert.equal(
    repeated.ledger.groupDeliveries.find(
      (delivery) => delivery.targetNodeKey === nodeKeys.fourth,
    ).state,
    "failed",
  );
  await assert.rejects(
    groups.storeOnlyGroupMessage({
      conversationId: ids.conversation,
      senderNodeKey: nodeKeys.first,
      message: "changed group context",
      logicalMessageId: ids.firstMessage,
      expiry,
    }),
    /idempotency conflict/,
  );
});

test("Group replies retain parent and bounded hop provenance", async () => {
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const reply = await groups.storeOnlyGroupMessage({
    conversationId: ids.conversation,
    senderNodeKey: nodeKeys.second,
    message: "bounded reply",
    logicalMessageId: ids.replyMessage,
    replyToMessageId: ids.firstMessage,
    expiry,
  });
  assert.equal(reply.message.replyToMessageId, ids.firstMessage);
  assert.equal(reply.message.hopCount, 1);
  assert.equal(reply.ledger.logicalMessage.group.parentMessageId, ids.firstMessage);
  const staleCache = structuredClone(
    groups.readGroupConversation(ids.conversation),
  );
  staleCache.lastActivityAt = "2000-01-01T00:00:00.000Z";
  staleCache.lastMessageId = ids.firstMessage;
  staleCache.lastSenderNodeKey = nodeKeys.first;
  assert.equal(groups.validGroupConversationRecord(staleCache), true);
  assert.equal(
    groups.publicGroupConversation(staleCache).lastMessageId,
    ids.replyMessage,
  );
});

test("a failed Ledger commit is recoverable without a second sequence", async () => {
  const expiry = new Date(Date.now() + 60_000).toISOString();
  await assert.rejects(
    groups.storeOnlyGroupMessage(
      {
        conversationId: ids.conversation,
        senderNodeKey: nodeKeys.first,
        message: "recoverable group message",
        logicalMessageId: ids.crashMessage,
        expiry,
      },
      {
        ledgerCommit: async () => {
          throw new Error("injected ledger interruption");
        },
      },
    ),
    /injected ledger interruption/,
  );
  const before = groups.readGroupConversation(ids.conversation);
  const prepared = before.messages.find(
    (message) => message.logicalMessageId === ids.crashMessage,
  );
  assert.ok(prepared);
  assert.equal(ledger.readDeliveryLedger(ids.crashMessage), null);
  assert.equal(
    groups
      .listGroupInbox(nodeKeys.second, { includeAcknowledged: true })
      .some((entry) => entry.logicalMessageId === ids.crashMessage),
    false,
  );
  const interrupted = await recent.listRecentConversations(nodeKeys.first, {
    kind: "group",
  });
  assert.equal(interrupted.complete, false);
  assert.equal(interrupted.conversations.length, 0);
  assert.equal(interrupted.diagnostics.summaryStale, 1);
  const recovered = await groups.storeOnlyGroupMessage({
    conversationId: ids.conversation,
    senderNodeKey: nodeKeys.first,
    message: "recoverable group message",
    logicalMessageId: ids.crashMessage,
    expiry,
  });
  assert.equal(recovered.message.sequence, prepared.sequence);
  assert.ok(ledger.readDeliveryLedger(ids.crashMessage));
  assert.equal(
    (await recent.listRecentConversations(nodeKeys.first, { kind: "group" }))
      .conversations[0].lastMessageId,
    ids.crashMessage,
  );
});

test("recent Conversation projection includes current Group members only", async () => {
  const result = await recent.listRecentConversations(nodeKeys.second, {
    kind: "group",
  });
  assert.equal(result.complete, true);
  const records = result.conversations;
  assert.equal(records.length, 1);
  assert.equal(records[0].conversationId, ids.conversation);
  assert.equal(records[0].label, "review-team");
  assert.equal(records[0].peerNodeKey, null);
  assert.equal(records[0].unread, null);
  assert.doesNotMatch(
    JSON.stringify(records[0]),
    /bounded group context|recoverable group message|contentRef/,
  );
  assert.deepEqual(
    (await recent.listRecentConversations(nodeKeys.fourth, { kind: "group" }))
      .conversations,
    [],
  );
});

test("bounded inbox is metadata-only and acknowledgement is a separate cursor", async () => {
  const initial = groups.listGroupInbox(nodeKeys.third);
  assert.equal(initial.length, 3);
  assert.ok(initial.every((entry) => entry.acknowledged === false));
  assert.doesNotMatch(JSON.stringify(initial), /bounded group context|bounded reply/);
  const first = initial[0];
  const acknowledged = await groups.acknowledgeGroupInbox({
    nodeKey: nodeKeys.third,
    conversationId: first.conversationId,
    sequence: first.sequence,
  });
  assert.equal(acknowledged.changed, true);
  const unread = groups.listGroupInbox(nodeKeys.third);
  assert.equal(unread.length, 2);
  const all = groups.listGroupInbox(nodeKeys.third, {
    includeAcknowledged: true,
  });
  assert.equal(all.length, 3);
  assert.equal(all[0].acknowledged, true);
  assert.equal(groups.listGroupInbox(nodeKeys.first).length, 1);
});

test("Group Conversation and inbox CLI stay bounded and body-free", () => {
  const listed = cxmsg("conversation", "group", "list", "--json");
  assert.equal(listed.status, 0, listed.stderr);
  assert.equal(JSON.parse(listed.stdout)[0].conversationId, ids.conversation);
  const shown = cxmsg(
    "conversation",
    "group",
    "show",
    ids.conversation,
    "--members",
    "--history",
    "--json",
  );
  assert.equal(shown.status, 0, shown.stderr);
  assert.equal(JSON.parse(shown.stdout).membershipVersion, 3);
  const delivery = cxmsg("deliveries", "show", ids.firstMessage, "--json");
  assert.equal(delivery.status, 0, delivery.stderr);
  assert.equal(JSON.parse(delivery.stdout).status, "partial");
  assert.equal(JSON.parse(delivery.stdout).recipients.length, 3);
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const sent = cxmsg(
    "conversation",
    "group",
    "send",
    ids.conversation,
    "--from",
    nodeKeys.first,
    "--expiry",
    expiry,
    "--logical-message-id",
    ids.cliMessage,
    "--json",
    "--",
    "CLI private body",
  );
  assert.equal(sent.status, 0, sent.stderr);
  assert.equal(JSON.parse(sent.stdout).wakePolicy, "store-only");
  const inbox = cxmsg("inbox", "list", nodeKeys.second, "--all", "--json");
  assert.equal(inbox.status, 0, inbox.stderr);
  assert.ok(
    JSON.parse(inbox.stdout).some(
      (entry) => entry.logicalMessageId === ids.cliMessage,
    ),
  );
  assert.doesNotMatch(inbox.stdout, /CLI private body/);

  const armed = cxmsg(
    "inbox",
    "digest-next",
    nodeKeys.second,
    "--limit",
    "2",
    "--max-bytes",
    "2048",
    "--json",
  );
  assert.equal(armed.status, 0, armed.stderr);
  assert.equal(JSON.parse(armed.stdout).messageLimit, 2);
  const status = cxmsg("inbox", "digest-status", nodeKeys.second, "--json");
  assert.equal(status.status, 0, status.stderr);
  assert.equal(JSON.parse(status.stdout).maxBytes, 2048);
  const cancelled = cxmsg("inbox", "digest-cancel", nodeKeys.second, "--json");
  assert.equal(cancelled.status, 0, cancelled.stderr);
  assert.equal(JSON.parse(cancelled.stdout).changed, true);
});

test("digest-next is bounded, never steers Busy work, and advances only after start", async () => {
  const before = groups.listGroupInbox(nodeKeys.second).length;
  const armed = await groups.requestGroupInboxDigest({
    nodeKey: nodeKeys.second,
    messageLimit: 2,
    maxBytes: 2_048,
  });
  assert.equal(armed.changed, true);
  const composed = groups.composeGroupInboxDigest(nodeKeys.second);
  assert.equal(composed.messageCount, 2);
  assert.ok(Buffer.byteLength(composed.text, "utf8") <= 2_048);
  assert.match(composed.text, /untrusted-group-inbox-digest/);
  assert.match(composed.text, /Presentation only/);

  const busyCalls = [];
  const busy = await messaging.deliverPeerMessage(
    {
      async request(method, params) {
        busyCalls.push({ method, params });
        return { turnId: "busy-turn" };
      },
    },
    {
      id: ids.second,
      name: "cxmsg:second",
      status: { type: "active" },
      turns: [{ id: "busy-turn", status: "inProgress" }],
    },
    { from: "first", message: "do not attach the digest", messageId: randomUUID() },
  );
  assert.equal(busy.delivery, "steered");
  assert.equal(busyCalls[0].method, "turn/steer");
  assert.doesNotMatch(
    JSON.stringify(busyCalls[0].params.input),
    /untrusted-group-inbox-digest/,
  );
  assert.ok(groups.readGroupInboxDigestIntent(nodeKeys.second));

  await assert.rejects(
    messaging.deliverPeerMessage(
      {
        async request() {
          throw new Error("injected App Server rejection");
        },
      },
      {
        id: ids.second,
        name: "cxmsg:second",
        status: { type: "idle" },
        turns: [],
      },
      { from: "first", message: "failed start", messageId: randomUUID() },
    ),
    /injected App Server rejection/,
  );
  assert.ok(groups.readGroupInboxDigestIntent(nodeKeys.second));
  assert.equal(groups.listGroupInbox(nodeKeys.second).length, before);

  const idleCalls = [];
  const started = await messaging.deliverPeerMessage(
    {
      async request(method, params) {
        idleCalls.push({ method, params });
        return { turn: { id: "digest-turn" } };
      },
    },
    {
      id: ids.second,
      name: "cxmsg:second",
      status: { type: "idle" },
      turns: [],
    },
    { from: "first", message: "start with the stored digest", messageId: randomUUID() },
  );
  assert.equal(started.delivery, "started");
  assert.equal(idleCalls[0].method, "turn/start");
  assert.match(
    JSON.stringify(idleCalls[0].params.input),
    /untrusted-group-inbox-digest/,
  );
  assert.deepEqual(started.inboxDigest, {
    included: true,
    cursorAdvanced: true,
    intentConsumed: true,
    errorCode: null,
  });
  assert.equal(groups.readGroupInboxDigestIntent(nodeKeys.second), null);
  assert.equal(groups.listGroupInbox(nodeKeys.second).length, before - 2);
});

test("store-only Group Deliveries are invisible to Scheduler wake discovery", async () => {
  let threadReads = 0;
  const outcomes = await scheduler.reconcileTurnLifecycle(
    {},
    {
      readThread: async () => {
        threadReads += 1;
        return { id: ids.second, status: { type: "idle" } };
      },
      listTurns: async () => ({ data: [], nextCursor: null }),
      observe: async () => {},
    },
  );
  assert.equal(threadReads, 0);
  assert.deepEqual(outcomes, []);
});

test("Group Conversation state is owner-private", () => {
  assert.equal(lstatSync(groups.GROUP_CONVERSATIONS_DIR).mode & 0o077, 0);
  assert.equal(lstatSync(groups.GROUP_INBOX_CURSORS_DIR).mode & 0o077, 0);
  assert.equal(lstatSync(groups.GROUP_INBOX_DIGEST_INTENTS_DIR).mode & 0o077, 0);
  for (const directory of [
    groups.GROUP_CONVERSATIONS_DIR,
    groups.GROUP_INBOX_CURSORS_DIR,
    groups.GROUP_INBOX_DIGEST_INTENTS_DIR,
  ]) {
    for (const name of readdirSync(directory)) {
      assert.equal(lstatSync(path.join(directory, name)).mode & 0o077, 0);
    }
  }
});

test("Group policy denial is recipient-local and all-denied stores no body", async () => {
  await groups.ensureGroupConversation({
    conversationId: ids.policyConversation,
    label: "policy-review-team",
    members: [nodeKeys.first, nodeKeys.second, nodeKeys.third],
  });
  await inbound.upsertInboundDenyRule({
    targetNodeKey: nodeKeys.second,
    selectorKind: "sender-node",
    selectorValue: nodeKeys.first,
  });
  const expiry = new Date(Date.now() + 60_000).toISOString();
  const inactive = await groups.storeOnlyGroupMessage({
    conversationId: ids.policyConversation,
    senderNodeKey: nodeKeys.first,
    message: "inactive Group policy context",
    logicalMessageId: ids.inactivePolicyMessage,
    expiry,
  });
  assert.equal(inbound.INBOUND_POLICY_FEATURE_ACTIVE, false);
  assert.ok(
    inactive.ledger.groupDeliveries.every(
      (delivery) => delivery.admissionState === "admitted",
    ),
  );
  const mixed = await groups.storeOnlyGroupMessage(
    {
      conversationId: ids.policyConversation,
      senderNodeKey: nodeKeys.first,
      message: "mixed recipient policy context",
      logicalMessageId: ids.mixedPolicyMessage,
      expiry,
    },
    { policyEvaluator: inbound.evaluateInboundPolicy },
  );
  assert.match(mixed.ledger.logicalMessage.body.contentRef, /^cxmsg-message:/);
  assert.equal(
    mixed.ledger.logicalMessage.senderIdentitySha256,
    createHash("sha256").update(nodeKeys.first).digest("hex"),
  );
  assert.deepEqual(
    mixed.ledger.groupDeliveries.map(
      ({ targetNodeKey, admissionState, state }) => ({
        targetNodeKey,
        admissionState,
        state,
      }),
    ),
    [
      {
        targetNodeKey: nodeKeys.second,
        admissionState: "denied",
        state: "denied",
      },
      {
        targetNodeKey: nodeKeys.third,
        admissionState: "admitted",
        state: "scheduled",
      },
    ].sort((left, right) =>
      left.targetNodeKey.localeCompare(right.targetNodeKey),
    ),
  );
  assert.equal(
    groups
      .listGroupInbox(nodeKeys.second, { includeAcknowledged: true })
      .some((entry) => entry.logicalMessageId === ids.mixedPolicyMessage),
    false,
  );
  assert.equal(
    groups
      .listGroupInbox(nodeKeys.third, { includeAcknowledged: true })
      .some((entry) => entry.logicalMessageId === ids.mixedPolicyMessage),
    true,
  );

  await inbound.upsertInboundDenyRule({
    targetNodeKey: nodeKeys.third,
    selectorKind: "sender-node",
    selectorValue: nodeKeys.first,
  });
  let bodyWrites = 0;
  const allDenied = await groups.storeOnlyGroupMessage(
    {
      conversationId: ids.policyConversation,
      senderNodeKey: nodeKeys.first,
      message: "metadata only denied group context",
      logicalMessageId: ids.allDeniedMessage,
      expiry,
    },
    {
      policyEvaluator: inbound.evaluateInboundPolicy,
      bodyStore: async () => {
        bodyWrites += 1;
        throw new Error("all-denied must not store a body");
      },
    },
  );
  assert.equal(bodyWrites, 0);
  assert.equal(allDenied.ledger.logicalMessage.body.contentRef, null);
  assert.equal(
    allDenied.ledger.logicalMessage.senderIdentitySha256,
    createHash("sha256").update(nodeKeys.first).digest("hex"),
  );
  assert.ok(
    allDenied.ledger.groupDeliveries.every(
      (delivery) =>
        delivery.admissionState === "denied" &&
        delivery.state === "denied" &&
        delivery.attempts.length === 0,
    ),
  );
  await ledger.rebuildDeliveryLedgerIndex();
  assert.ok(
    (
      await ledger.readDeliveryLedgerIndexed(ids.allDeniedMessage)
    ).groupDeliveries.every((delivery) => delivery.state === "denied"),
  );
  for (const nodeKey of [nodeKeys.second, nodeKeys.third]) {
    assert.equal(
      groups
        .listGroupInbox(nodeKey, { includeAcknowledged: true })
        .some((entry) => entry.logicalMessageId === ids.allDeniedMessage),
      false,
    );
  }
  await assert.rejects(
    ledger.appendStoreOnlyGroupDeliveryEvidence(ids.allDeniedMessage, {
      targetNodeKey: nodeKeys.second,
      state: "expired",
      errorCode: "EDELIVERYEXPIRED",
    }),
    /already has terminal evidence/,
  );
  const inactiveEvidence = inspectors
    .inspectRouteState({ stateDir, sessions: [] })
    .find((check) => check.id === "inbound-policies.inactive-evidence");
  assert.equal(inactiveEvidence.status, "fail");
  assert.equal(inactiveEvidence.errorCode, "EINBOUNDPOLICYBYPASS");
  assert.doesNotMatch(JSON.stringify(inactiveEvidence), /31345678-4234/);
});
