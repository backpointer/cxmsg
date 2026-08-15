import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-cast-"));
const firstRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-project-"));
const secondRoot = mkdtempSync(path.join(os.tmpdir(), "cxmsg-team-project-"));
process.env.CXMSG_STATE_DIR = stateDir;

const directory = await import(`../src/node-directory.js?team=${Date.now()}`);
const conversations = await import(`../src/conversations.js?team=${Date.now()}`);
const groups = await import(`../src/group-conversations.js?team=${Date.now()}`);
const registry = await import(`../src/registry.js?team=${Date.now()}`);
const routes = await import(`../src/route-admission.js?team=${Date.now()}`);
const teams = await import(`../src/team-cast.js?team=${Date.now()}`);
const bodies = await import(`../src/message-bodies.js?team=${Date.now()}`);
const ledger = await import(`../src/delivery-ledger.js?team=${Date.now()}`);

const ids = {
  firstProject: "10345678-5234-4234-8234-123456789abc",
  secondProject: "20345678-5234-4234-8234-123456789abc",
  sender: "30345678-5234-4234-8234-123456789abc",
  first: "40345678-5234-4234-8234-123456789abc",
  second: "50345678-5234-4234-8234-123456789abc",
  cross: "60345678-5234-4234-8234-123456789abc",
  claude: "a0345678-5234-4234-8234-123456789abc",
  group: "70345678-5234-4234-8234-123456789abc",
  cluster: "80345678-5234-4234-8234-123456789abc",
  crossCluster: "90345678-5234-4234-8234-123456789abc",
  groupPlan: "a0345678-5234-4234-8234-123456789abc",
  directPlan: "b0345678-5234-4234-8234-123456789abc",
  clusterPlan: "c0345678-5234-4234-8234-123456789abc",
  rolePlan: "d0345678-5234-4234-8234-123456789abc",
  crossPlan: "e0345678-5234-4234-8234-123456789abc",
  tombstonePlan: "f0345678-5234-4234-8234-123456789abc",
  cliPlan: "11345678-6234-4234-8234-123456789abc",
  mentionSelection: "21345678-6234-4234-8234-123456789abc",
  cliSelection: "31345678-6234-4234-8234-123456789abc",
  teamMessage: "41345678-6234-4234-8234-123456789abc",
  preparedMessage: "51345678-6234-4234-8234-123456789abc",
  cliPreparedMessage: "61345678-6234-4234-8234-123456789abc",
  teamTurn: "71345678-6234-4234-8234-123456789abc",
  fanoutSelection: "81345678-6234-4234-8234-123456789abc",
  fanoutMessage: "91345678-6234-4234-8234-123456789abc",
  fanoutTurn: "a1345678-6234-4234-8234-123456789abc",
  preflightMessage: "b1345678-6234-4234-8234-123456789abc",
  claudePlan: "c1345678-6234-4234-8234-123456789abc",
  claudeSelection: "d1345678-6234-4234-8234-123456789abc",
  claudeMessage: "e1345678-6234-4234-8234-123456789abc",
  claudeTransportMismatch: "f1345678-6234-4234-8234-123456789abc",
  scheduledMessage: "12345678-7234-4234-8234-123456789abc",
  wakeAllSelection: "22345678-7234-4234-8234-123456789abc",
  cliWakeAllSelection: "32345678-7234-4234-8234-123456789abc",
  wakeAllMessage: "42345678-7234-4234-8234-123456789abc",
  wakeAllTurnFirst: "52345678-7234-4234-8234-123456789abc",
  wakeAllScheduleFailure: "62345678-7234-4234-8234-123456789abc",
  wakeAllWideMessage: "72345678-7234-4234-8234-123456789abc",
  wakeAllWidePlan: "82345678-7234-4234-8234-123456789abc",
  wakeAllWideSelection: "92345678-7234-4234-8234-123456789abc",
  afterTurnMessage: "a2345678-7234-4234-8234-123456789abc",
  afterTurnTrigger: "b2345678-7234-4234-8234-123456789abc",
};
const keys = Object.fromEntries(
  ["sender", "first", "second", "cross"].map((name) => [
    name,
    `codex:${ids[name]}`,
  ]),
);
keys.claude = `claude:${ids.claude}`;

await directory.ensureProject({
  routingId: "team-first",
  root: firstRoot,
  projectId: ids.firstProject,
});
await directory.ensureProject({
  routingId: "team-second",
  root: secondRoot,
  projectId: ids.secondProject,
});
for (const name of ["sender", "first", "second", "cross"]) {
  await directory.upsertNode({
    runtimeKind: "codex",
    nativeId: ids[name],
    displayName: name,
    projectId: name === "cross" ? ids.secondProject : ids.firstProject,
  });
}
await directory.upsertNode({
  runtimeKind: "claude",
  nativeId: ids.claude,
  displayName: "claude-reviewer",
  projectId: ids.firstProject,
});
const direct = await conversations.ensureDirectConversation(keys.sender, keys.first);
const claudeDirect = await conversations.ensureDirectConversation(
  keys.sender,
  keys.claude,
);
await groups.ensureGroupConversation({
  conversationId: ids.group,
  label: "team-reviewers",
  members: [keys.sender, keys.first, keys.second],
});
await directory.ensureCluster({
  routingId: "team-cluster",
  clusterId: ids.cluster,
});
for (const memberNodeKey of [keys.sender, keys.first, keys.second]) {
  await directory.addClusterMember({ cluster: ids.cluster, memberNodeKey });
}
await directory.ensureCluster({
  routingId: "cross-cluster",
  clusterId: ids.crossCluster,
});
for (const memberNodeKey of [keys.sender, keys.first, keys.cross]) {
  await directory.addClusterMember({ cluster: ids.crossCluster, memberNodeKey });
}
for (const name of ["sender", "first", "second"]) {
  registry.writeSessionRecord({
    name: `team-${name}`,
    threadId: ids[name],
    cwd: firstRoot,
  });
  routes.writeRouteBinding({
    sessionName: `team-${name}`,
    threadId: ids[name],
    projectId: "team-first",
    projectKey: ids.firstProject,
    nodeKey: keys[name],
    role: name === "sender" ? "coordinator" : "reviewer",
  });
}

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  rmSync(firstRoot, { recursive: true, force: true });
  rmSync(secondRoot, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("Conversation Team Cast plans freeze and redact recipients", async () => {
  const result = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: { kind: "conversation", id: ids.group },
    planId: ids.groupPlan,
  });
  assert.equal(result.created, true);
  assert.deepEqual(result.plan.recipientNodeKeys, [keys.first, keys.second].sort());
  assert.equal(result.plan.selector.conversationKind, "group");
  assert.equal(result.plan.selector.membershipVersion, 1);
  const output = teams.publicTeamCastPlan(result.plan);
  assert.equal(output.recipientCount, 2);
  assert.equal(output.estimatedWakeTurns, 2);
  assert.equal("recipientNodeKeys" in output, false);
  assert.deepEqual(
    teams.publicTeamCastPlan(result.plan, { includeRecipients: true })
      .recipientNodeKeys,
    [keys.first, keys.second].sort(),
  );
  const repeated = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: { kind: "conversation", id: ids.group },
    planId: ids.groupPlan,
  });
  assert.equal(repeated.created, false);
  assert.equal(statSync(teams.TEAM_CAST_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(path.join(teams.TEAM_CAST_PLANS_DIR, `${ids.groupPlan}.json`)).mode &
      0o777,
    0o600,
  );
});

test("Direct and Cluster selectors preserve immutable selector evidence", async () => {
  const directPlan = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: {
      kind: "conversation",
      id: direct.conversation.conversationId,
    },
    planId: ids.directPlan,
  });
  assert.deepEqual(directPlan.plan.recipientNodeKeys, [keys.first]);
  assert.equal(directPlan.plan.selector.conversationKind, "direct");

  const clusterPlan = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: { kind: "cluster", id: "team-cluster" },
    planId: ids.clusterPlan,
  });
  assert.deepEqual(clusterPlan.plan.recipientNodeKeys, [keys.first, keys.second].sort());
  assert.equal(clusterPlan.plan.selector.clusterId, ids.cluster);
  assert.equal(clusterPlan.plan.selector.membershipVersion, 4);
});

test("CLI exposes a redacted resolution plan and starts no delivery", () => {
  const result = spawnSync(
    process.execPath,
    [
      path.resolve("bin/cxmsg.js"),
      "team",
      "resolve",
      "--from",
      keys.sender,
      "--conversation",
      ids.group,
      "--plan-id",
      ids.cliPlan,
      "--json",
    ],
    {
      encoding: "utf8",
      env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  assert.equal(output.deliveryStarted, false);
  assert.equal(output.recipientCount, 2);
  assert.equal("recipientNodeKeys" in output, false);
});

test("mention metadata selects only an explicit bounded plan subset", async () => {
  const result = await teams.resolveTeamCastMentionSelection({
    planId: ids.groupPlan,
    senderNodeKey: keys.sender,
    mentionedNodeKeys: [keys.second],
    selectionId: ids.mentionSelection,
  });
  assert.equal(result.created, true);
  assert.deepEqual(result.selection.recipientNodeKeys, [keys.second]);
  const output = teams.publicTeamCastMentionSelection(result.selection);
  assert.equal(output.wakePolicy, "mention-wake");
  assert.equal(output.recipientCount, 1);
  assert.equal(output.estimatedWakeTurns, 1);
  assert.equal("recipientNodeKeys" in output, false);
  assert.equal(existsSync(path.join(stateDir, "delivery-ledger")), false);
  assert.equal(statSync(teams.TEAM_CAST_SELECTIONS_DIR).mode & 0o777, 0o700);
  assert.equal(
    statSync(
      path.join(
        teams.TEAM_CAST_SELECTIONS_DIR,
        `${ids.mentionSelection}.json`,
      ),
    ).mode & 0o777,
    0o600,
  );

  const repeated = await teams.resolveTeamCastMentionSelection({
    planId: ids.groupPlan,
    senderNodeKey: keys.sender,
    mentionedNodeKeys: [keys.second],
    selectionId: ids.mentionSelection,
  });
  assert.equal(repeated.created, false);
  await assert.rejects(
    teams.resolveTeamCastMentionSelection({
      planId: ids.groupPlan,
      senderNodeKey: keys.sender,
      mentionedNodeKeys: [keys.cross],
    }),
    /outside the fixed plan/,
  );
  await assert.rejects(
    teams.resolveTeamCastMentionSelection({
      planId: ids.groupPlan,
      senderNodeKey: keys.sender,
      mentionedNodeKeys: [keys.second, keys.second],
    }),
    /duplicate recipients/,
  );
});

test("wake-all freezes the complete plan and exposes its wake ceiling", async () => {
  const result = await teams.resolveTeamCastWakeAllSelection({
    planId: ids.groupPlan,
    senderNodeKey: keys.sender,
    selectionId: ids.wakeAllSelection,
  });
  assert.equal(result.created, true);
  const plan = teams.readTeamCastPlan(ids.groupPlan);
  assert.deepEqual(result.selection.recipientNodeKeys, plan.recipientNodeKeys);
  assert.equal(result.selection.recipientSetSha256, plan.recipientSetSha256);
  const output = teams.publicTeamCastSelection(result.selection);
  assert.equal(output.wakePolicy, "wake-all");
  assert.equal(output.recipientCount, 2);
  assert.equal(output.estimatedWakeTurns, 2);
  assert.equal("recipientNodeKeys" in output, false);

  const repeated = await teams.resolveTeamCastWakeAllSelection({
    planId: ids.groupPlan,
    senderNodeKey: keys.sender,
    selectionId: ids.wakeAllSelection,
  });
  assert.equal(repeated.created, false);
  await assert.rejects(
    teams.resolveTeamCastMentionSelection({
      planId: ids.groupPlan,
      senderNodeKey: keys.sender,
      mentionedNodeKeys: plan.recipientNodeKeys,
      selectionId: ids.wakeAllSelection,
    }),
    /idempotency conflict/,
  );
});

test("CLI mention selection remains an explicit zero-delivery operation", () => {
  const result = spawnSync(
    process.execPath,
    [
      path.resolve("bin/cxmsg.js"),
      "team",
      "select-mentions",
      "--plan",
      ids.groupPlan,
      "--from",
      keys.sender,
      "--mention",
      keys.first,
      "--selection-id",
      ids.cliSelection,
      "--json",
    ],
    {
      encoding: "utf8",
      env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  assert.equal(output.deliveryStarted, false);
  assert.equal(output.recipientCount, 1);
  assert.equal("recipientNodeKeys" in output, false);
});

test("CLI wake-all selection remains explicit and starts zero delivery", () => {
  const result = spawnSync(
    process.execPath,
    [
      path.resolve("bin/cxmsg.js"),
      "team",
      "select-all",
      "--plan",
      ids.groupPlan,
      "--from",
      keys.sender,
      "--selection-id",
      ids.cliWakeAllSelection,
      "--json",
    ],
    {
      encoding: "utf8",
      env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  assert.equal(output.wakePolicy, "wake-all");
  assert.equal(output.deliveryStarted, false);
  assert.equal(output.recipientCount, 2);
  assert.equal(output.estimatedWakeTurns, 2);
  assert.equal("recipientNodeKeys" in output, false);
});

test("one Ledger batch prepares every selected Team Cast recipient", async () => {
  const selection = teams.readTeamCastMentionSelection(ids.mentionSelection);
  const message = "bounded Team Cast coordination";
  const body = await bodies.storeMessageBody({
    messageId: ids.teamMessage,
    body: message,
  });
  const route = {
    schema_version: 1,
    kind: "team-cast",
    plan_id: selection.planId,
    selection_id: selection.selectionId,
    project_id: selection.projectId,
    wake_policy: "mention-wake",
    expiry: selection.expiresAt,
  };
  const logicalMessage = {
    messageId: ids.teamMessage,
    from: keys.sender,
    senderThreadId: ids.sender,
    senderNodeKey: keys.sender,
    body: {
      messageId: ids.teamMessage,
      bytes: Buffer.byteLength(message),
      sha256: createHash("sha256").update(message).digest("hex"),
      contentRef: body.contentRef,
    },
    route,
    routeFingerprint: createHash("sha256")
      .update(JSON.stringify(route))
      .digest("hex"),
    createdAt: selection.createdAt,
    teamCast: {
      version: 1,
      planId: selection.planId,
      selectionId: selection.selectionId,
      projectId: selection.projectId,
      wakePolicy: selection.wakePolicy,
      recipientNodeKeys: selection.recipientNodeKeys,
      recipientSetSha256: selection.recipientSetSha256,
      expiresAt: selection.expiresAt,
    },
  };
  const committed = await ledger.commitPreparedTeamCastDelivery({
    logicalMessage,
    recipients: selection.recipientNodeKeys.map((nodeKey) => ({
      nodeKey,
      targetThreadId: nodeKey.slice("codex:".length),
    })),
    now: selection.createdAt,
  });
  assert.equal(committed.created, true);
  assert.equal(committed.record.teamDeliveries.length, 1);
  assert.equal(committed.record.teamDeliveries[0].state, "prepared");
  assert.equal(committed.record.teamDeliveries[0].attempts.length, 0);
  assert.equal(committed.record.delivery.deliveryId,
    committed.record.teamDeliveries[0].deliveryId);
  const legacyProjection = structuredClone(committed.record);
  for (const delivery of legacyProjection.teamDeliveries) {
    delete delivery.schedule;
  }
  delete legacyProjection.delivery.schedule;
  assert.equal(
    ledger.validDeliveryLedgerIndexRecord(
      {
        version: 1,
        messageId: ids.teamMessage,
        projection: legacyProjection,
        projectionSha256: createHash("sha256")
          .update(JSON.stringify(legacyProjection))
          .digest("hex"),
      },
      ids.teamMessage,
    ),
    true,
  );
  const repeated = await ledger.commitPreparedTeamCastDelivery({
    logicalMessage,
    recipients: selection.recipientNodeKeys.map((nodeKey) => ({
      nodeKey,
      targetThreadId: nodeKey.slice("codex:".length),
    })),
    now: selection.createdAt,
  });
  assert.equal(repeated.created, false);
  await assert.rejects(
    ledger.beginImmediateDelivery(ids.teamMessage),
    /already has a dispatch attempt/,
  );
  let rebuilt = await ledger.readDeliveryLedgerIndexed(ids.teamMessage);
  assert.equal(rebuilt.teamDeliveries[0].state, "prepared");
  const attempt = await ledger.beginTeamCastRecipientDelivery(
    ids.teamMessage,
    keys.second,
  );
  assert.equal(attempt.created, true);
  const repeatedAttempt = await ledger.beginTeamCastRecipientDelivery(
    ids.teamMessage,
    keys.second,
  );
  assert.equal(repeatedAttempt.created, false);
  assert.equal(repeatedAttempt.attempt.attemptId, attempt.attempt.attemptId);
  const evidence = await ledger.appendTeamCastRecipientEvidence(
    ids.teamMessage,
    keys.second,
    {
      attemptId: attempt.attempt.attemptId,
      state: "turn_started",
      turnId: ids.teamTurn,
      transportResult: "started",
      errorCode: null,
    },
  );
  assert.equal(evidence.created, true);
  rebuilt = await ledger.readDeliveryLedgerIndexed(ids.teamMessage);
  assert.equal(rebuilt.teamDeliveries[0].state, "turn_started");
  assert.equal(rebuilt.teamDeliveries[0].turnId, ids.teamTurn);
  await assert.rejects(
    ledger.beginTeamCastRecipientDelivery(ids.teamMessage, keys.second),
    /already terminal/,
  );
});

test("wake-all Ledger batches remain bounded at 64 fixed recipients", async () => {
  const recipientNodeKeys = Array.from(
    { length: ledger.TEAM_CAST_RECIPIENT_LIMIT },
    (_, index) =>
      `codex:${String(index + 1).padStart(8, "0")}-1234-4234-8234-123456789abc`,
  ).sort();
  const route = {
    schema_version: 1,
    kind: "team-cast",
    plan_id: ids.wakeAllWidePlan,
    selection_id: ids.wakeAllWideSelection,
    project_id: ids.firstProject,
    wake_policy: "wake-all",
    expiry: "2026-08-15T00:15:00.000Z",
  };
  const logicalMessage = {
    messageId: ids.wakeAllWideMessage,
    from: keys.sender,
    senderThreadId: ids.sender,
    senderNodeKey: keys.sender,
    body: {
      messageId: ids.wakeAllWideMessage,
      bytes: 1,
      sha256: createHash("sha256").update("x").digest("hex"),
      contentRef: `cxmsg-message:${ids.wakeAllWideMessage}`,
    },
    route,
    routeFingerprint: createHash("sha256")
      .update(JSON.stringify(route))
      .digest("hex"),
    createdAt: "2026-08-15T00:00:00.000Z",
    teamCast: {
      version: 1,
      planId: ids.wakeAllWidePlan,
      selectionId: ids.wakeAllWideSelection,
      projectId: ids.firstProject,
      wakePolicy: "wake-all",
      recipientNodeKeys,
      recipientSetSha256: createHash("sha256")
        .update(JSON.stringify(recipientNodeKeys))
        .digest("hex"),
      expiresAt: "2026-08-15T00:15:00.000Z",
    },
  };
  const committed = await ledger.commitPreparedTeamCastDelivery({
    logicalMessage,
    recipients: recipientNodeKeys.map((nodeKey) => ({
      nodeKey,
      targetThreadId: nodeKey.slice("codex:".length),
    })),
    now: logicalMessage.createdAt,
  });
  assert.equal(
    committed.record.teamDeliveries.length,
    ledger.TEAM_CAST_RECIPIENT_LIMIT,
  );
  await assert.rejects(
    ledger.commitPreparedTeamCastDelivery({
      logicalMessage: {
        ...logicalMessage,
        messageId: "a2345678-7234-4234-8234-123456789abc",
      },
      recipients: [
        ...recipientNodeKeys.map((nodeKey) => ({
          nodeKey,
          targetThreadId: nodeKey.slice("codex:".length),
        })),
        {
          nodeKey: "codex:ffffffff-1234-4234-8234-123456789abc",
          targetThreadId: "ffffffff-1234-4234-8234-123456789abc",
        },
      ],
      now: logicalMessage.createdAt,
    }),
    /requires 1-64 fixed recipients/,
  );
});

test("Team Cast preparation persists body and batch without dispatch", async () => {
  const result = await teams.prepareTeamCastMentionMessage({
    selectionId: ids.mentionSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.preparedMessage,
    message: "review the bounded artifact pointer",
  });
  assert.equal(result.created, true);
  assert.equal(result.deliveryStarted, false);
  assert.equal(result.ledger.teamDeliveries[0].state, "prepared");
  assert.equal(result.ledger.teamDeliveries[0].attempts.length, 0);
  assert.match(result.body.contentRef, /^cxmsg-message:/);

  const repeated = await teams.prepareTeamCastMentionMessage({
    selectionId: ids.mentionSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.preparedMessage,
    message: "review the bounded artifact pointer",
  });
  assert.equal(repeated.created, false);
});

test("CLI prepares Team Cast evidence and reports zero delivery", () => {
  const result = spawnSync(
    process.execPath,
    [
      path.resolve("bin/cxmsg.js"),
      "team",
      "prepare",
      "--selection",
      ids.cliSelection,
      "--from",
      keys.sender,
      "--logical-message-id",
      ids.cliPreparedMessage,
      "--json",
      "--",
      "bounded",
      "handoff",
      "pointer",
    ],
    {
      encoding: "utf8",
      env: { ...process.env, CXMSG_STATE_DIR: stateDir },
    },
  );
  assert.equal(result.status, 0, result.stderr);
  const output = JSON.parse(result.stdout);
  assert.equal(output.status, "prepared");
  assert.equal(output.deliveryStarted, false);
  assert.equal(output.recipientCount, 1);
  assert.equal(output.estimatedWakeTurns, 1);
  assert.equal(
    output.estimatedFanoutPayloadBytes,
    Buffer.byteLength("bounded handoff pointer", "utf8"),
  );
  assert.equal("message" in output, false);
});

test("recipient dispatch preserves partial outcomes and never redrives", async () => {
  const selection = await teams.resolveTeamCastMentionSelection({
    planId: ids.groupPlan,
    senderNodeKey: keys.sender,
    mentionedNodeKeys: [keys.first, keys.second],
    selectionId: ids.fanoutSelection,
  });
  await teams.prepareTeamCastMentionMessage({
    selectionId: selection.selection.selectionId,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.fanoutMessage,
    message: "fan out one bounded review pointer",
  });
  const preflighted = [];
  const dispatched = [];
  const result = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.fanoutMessage },
    {
      preflightRecipient: async ({ targetNodeKey }) => {
        preflighted.push(targetNodeKey);
        return { targetNodeKey };
      },
      dispatchRecipient: async ({ targetNodeKey }) => {
        dispatched.push(targetNodeKey);
        return targetNodeKey === keys.first
          ? {
              state: "turn_started",
              turnId: ids.fanoutTurn,
              transportResult: "started",
              errorCode: null,
            }
          : {
              state: "failed",
              turnId: null,
              transportResult: null,
              errorCode: "ETARGETBUSY",
            };
      },
    },
  );
  assert.deepEqual(preflighted, [keys.first, keys.second].sort());
  assert.deepEqual(dispatched, [keys.first, keys.second].sort());
  assert.deepEqual(
    result.record.teamDeliveries.map((delivery) => delivery.state).sort(),
    ["failed", "turn_started"],
  );
  const repeated = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.fanoutMessage },
    {
      preflightRecipient: async () => assert.fail("terminal recipients do not preflight"),
      dispatchRecipient: async () => assert.fail("terminal recipients do not dispatch"),
    },
  );
  assert.ok(repeated.outcomes.every((outcome) => outcome.attempted === false));
});

test("a failed recipient preflight starts zero Team Cast attempts", async () => {
  await teams.prepareTeamCastMentionMessage({
    selectionId: ids.fanoutSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.preflightMessage,
    message: "preflight must be all recipients first",
  });
  await assert.rejects(
    teams.dispatchPreparedTeamCastMessage(
      { logicalMessageId: ids.preflightMessage },
      {
        preflightRecipient: async ({ targetNodeKey }) => {
          if (targetNodeKey === keys.second) throw new Error("injected preflight failure");
          return {};
        },
        dispatchRecipient: async () => assert.fail("dispatch must not start"),
      },
    ),
    /injected preflight failure/,
  );
  const record = await ledger.readDeliveryLedgerIndexed(ids.preflightMessage);
  assert.ok(
    record.teamDeliveries.every(
      (delivery) =>
        delivery.state === "prepared" && delivery.attempts.length === 0,
    ),
  );
});

test("Claude recipient evidence distinguishes transport from model completion", async () => {
  const plan = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: {
      kind: "conversation",
      id: claudeDirect.conversation.conversationId,
    },
    planId: ids.claudePlan,
  });
  const selection = await teams.resolveTeamCastMentionSelection({
    planId: plan.plan.planId,
    senderNodeKey: keys.sender,
    mentionedNodeKeys: [keys.claude],
    selectionId: ids.claudeSelection,
  });
  await teams.prepareTeamCastMentionMessage({
    selectionId: selection.selection.selectionId,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.claudeMessage,
    message: "bounded Claude review pointer",
  });
  let jobId = null;
  const result = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.claudeMessage },
    {
      preflightRecipient: async () => ({ transport: "claude-uds" }),
      dispatchRecipient: async ({ attemptId, targetNodeKey }) => {
        assert.equal(targetNodeKey, keys.claude);
        jobId = attemptId;
        return {
          state: "transport_delivered",
          turnId: null,
          transportResult: `claude-job:${attemptId}`,
          errorCode: null,
        };
      },
    },
  );
  const delivery = result.record.teamDeliveries[0];
  assert.equal(delivery.state, "transport_delivered");
  assert.equal(delivery.attempts[0].transport, "claude-uds");
  assert.equal(delivery.transportResult, `claude-job:${jobId}`);
  assert.equal(delivery.turnId, null);
});

test("Team Cast transport must match the recipient runtime", async () => {
  await teams.prepareTeamCastMentionMessage({
    selectionId: ids.claudeSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.claudeTransportMismatch,
    message: "reject a mismatched runtime transport",
  });
  await assert.rejects(
    teams.dispatchPreparedTeamCastMessage(
      { logicalMessageId: ids.claudeTransportMismatch },
      {
        preflightRecipient: async () => ({
          transport: "codex-app-server",
        }),
        dispatchRecipient: async () => assert.fail("dispatch must not start"),
      },
    ),
    /transport does not match recipient runtime/,
  );
  const record = await ledger.readDeliveryLedgerIndexed(
    ids.claudeTransportMismatch,
  );
  assert.equal(record.teamDeliveries[0].state, "prepared");
  assert.equal(record.teamDeliveries[0].attempts.length, 0);
});

test("a Busy Team Cast recipient can enter the shared when-idle ledger", async () => {
  await teams.prepareTeamCastMentionMessage({
    selectionId: ids.fanoutSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.scheduledMessage,
    message: "schedule only the explicitly Busy recipient",
  });
  const result = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.scheduledMessage },
    {
      preflightRecipient: async ({ targetNodeKey }) =>
        targetNodeKey === keys.second
          ? {
              transport: "codex-app-server",
              scheduleWakePolicy: "when-idle",
            }
          : { transport: "codex-app-server" },
      dispatchRecipient: async () => ({
        state: "turn_started",
        turnId: ids.fanoutTurn,
        transportResult: "started",
        errorCode: null,
      }),
    },
  );
  assert.deepEqual(
    result.outcomes.map(({ targetNodeKey, status, attempted }) => ({
      targetNodeKey,
      status,
      attempted,
    })),
    [
      { targetNodeKey: keys.first, status: "turn_started", attempted: true },
      { targetNodeKey: keys.second, status: "scheduled", attempted: false },
    ].sort((left, right) => left.targetNodeKey.localeCompare(right.targetNodeKey)),
  );
  const scheduled = result.record.teamDeliveries.find(
    (delivery) => delivery.targetNodeKey === keys.second,
  );
  assert.equal(scheduled.wakePolicy, "when-idle");
  assert.equal(scheduled.state, "scheduled");
  assert.equal(scheduled.schedule.wakePolicy, "when-idle");
  assert.equal(scheduled.attempts.length, 0);
  await ledger.rebuildDeliveryLedgerIndex();
  const rebuilt = await ledger.readDeliveryLedgerIndexed(ids.scheduledMessage);
  assert.equal(
    rebuilt.teamDeliveries.find(
      (delivery) => delivery.targetNodeKey === keys.second,
    ).state,
    "scheduled",
  );
});

test("Team dispatch persists an exact per-recipient after-turn trigger", async () => {
  await teams.prepareTeamCastMentionMessage({
    selectionId: ids.fanoutSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.afterTurnMessage,
    message: "deliver the second pointer after its current turn",
  });
  const result = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.afterTurnMessage },
    {
      preflightRecipient: async ({ targetNodeKey }) =>
        targetNodeKey === keys.second
          ? {
              transport: "codex-app-server",
              scheduleWakePolicy: "after-turn",
              triggerTurnId: ids.afterTurnTrigger,
            }
          : { transport: "codex-app-server" },
      dispatchRecipient: async () => ({
        state: "turn_started",
        turnId: ids.fanoutTurn,
        transportResult: "started",
        errorCode: null,
      }),
    },
  );
  const scheduled = result.record.teamDeliveries.find(
    (delivery) => delivery.targetNodeKey === keys.second,
  );
  assert.equal(scheduled.state, "scheduled");
  assert.equal(scheduled.wakePolicy, "after-turn");
  assert.equal(scheduled.schedule.triggerTurnId, ids.afterTurnTrigger);
  assert.equal(scheduled.schedule.triggerJobId, null);
});

test("wake-all reuses recipient dispatch and Busy fallback without implicit steering", async () => {
  const prepared = await teams.prepareTeamCastMentionMessage({
    selectionId: ids.wakeAllSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.wakeAllMessage,
    message: "explicitly wake every frozen plan recipient",
  });
  assert.equal(prepared.selection.wakePolicy, "wake-all");
  assert.equal(prepared.selection.estimatedWakeTurns, 2);
  assert.equal(prepared.ledger.teamDeliveries.length, 2);
  assert.ok(
    prepared.ledger.teamDeliveries.every(
      (delivery) => delivery.wakePolicy === "wake-all",
    ),
  );

  const result = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.wakeAllMessage },
    {
      preflightRecipient: async ({ targetNodeKey }) =>
        targetNodeKey === keys.second
          ? {
              transport: "codex-app-server",
              scheduleWakePolicy: "when-idle",
            }
          : { transport: "codex-app-server" },
      dispatchRecipient: async ({ targetNodeKey }) => {
        assert.equal(targetNodeKey, keys.first);
        return {
          state: "turn_started",
          turnId: ids.wakeAllTurnFirst,
          transportResult: "started",
          errorCode: null,
        };
      },
    },
  );
  assert.deepEqual(
    result.record.teamDeliveries.map((delivery) => delivery.state).sort(),
    ["scheduled", "turn_started"],
  );
  const scheduled = result.record.teamDeliveries.find(
    (delivery) => delivery.targetNodeKey === keys.second,
  );
  assert.equal(scheduled.wakePolicy, "when-idle");
  assert.equal(scheduled.schedule.wakePolicy, "when-idle");
  await ledger.rebuildDeliveryLedgerIndex();
});

test("one wake-all schedule failure stays visible without hiding siblings", async () => {
  await teams.prepareTeamCastMentionMessage({
    selectionId: ids.wakeAllSelection,
    senderNodeKey: keys.sender,
    logicalMessageId: ids.wakeAllScheduleFailure,
    message: "keep one saturated recipient prepared",
  });
  const result = await teams.dispatchPreparedTeamCastMessage(
    { logicalMessageId: ids.wakeAllScheduleFailure },
    {
      preflightRecipient: async ({ targetNodeKey }) =>
        targetNodeKey === keys.second
          ? {
              transport: "codex-app-server",
              scheduleWakePolicy: "when-idle",
            }
          : { transport: "codex-app-server" },
      scheduleRecipient: async () => {
        throw new Error("injected queue saturation");
      },
      dispatchRecipient: async () => ({
        state: "turn_started",
        turnId: ids.wakeAllTurnFirst,
        transportResult: "started",
        errorCode: null,
      }),
    },
  );
  const failed = result.outcomes.find(
    (outcome) => outcome.targetNodeKey === keys.second,
  );
  assert.deepEqual(failed, {
    targetNodeKey: keys.second,
    status: "schedule_failed",
    attempted: false,
    scheduled: false,
    errorCode: "ESCHEDULEFAILED",
  });
  const retained = result.record.teamDeliveries.find(
    (delivery) => delivery.targetNodeKey === keys.second,
  );
  assert.equal(retained.state, "prepared");
  assert.equal(retained.attempts.length, 0);
  assert.equal(
    result.record.teamDeliveries.find(
      (delivery) => delivery.targetNodeKey === keys.first,
    ).state,
    "turn_started",
  );
});

test("Project-role selector requires exact stable bindings", async () => {
  const result = await teams.resolveTeamCastPlan({
    senderNodeKey: keys.sender,
    selector: {
      kind: "project-role",
      projectId: ids.firstProject,
      role: "reviewer",
    },
    planId: ids.rolePlan,
  });
  assert.deepEqual(result.plan.recipientNodeKeys, [keys.first, keys.second].sort());
  assert.match(result.plan.selector.bindingSetSha256, /^[0-9a-f]{64}$/);

  registry.writeSessionRecord({
    name: "team-first-alias",
    threadId: ids.first,
    cwd: firstRoot,
  });
  routes.writeRouteBinding({
    sessionName: "team-first-alias",
    threadId: ids.first,
    projectId: "team-first",
    projectKey: ids.firstProject,
    nodeKey: keys.first,
    role: "reviewer",
  });
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: {
        kind: "project-role",
        projectId: ids.firstProject,
        role: "reviewer",
      },
    }),
    /ambiguous/,
  );
});

test("a plan id cannot be rebound to another selector", async () => {
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: { kind: "conversation", id: direct.conversation.conversationId },
      planId: ids.groupPlan,
    }),
    /idempotency conflict/,
  );
});

test("cross-Project and Tombstoned recipients fail before a plan is written", async () => {
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: { kind: "cluster", id: ids.crossCluster },
      planId: ids.crossPlan,
    }),
    /crosses the sender Project/,
  );
  assert.equal(teams.readTeamCastPlan(ids.crossPlan), null);

  await directory.tombstoneNode("codex", ids.second, { reason: "test" });
  await assert.rejects(
    teams.resolveTeamCastPlan({
      senderNodeKey: keys.sender,
      selector: { kind: "conversation", id: ids.group },
      planId: ids.tombstonePlan,
    }),
    /Tombstoned/,
  );
  assert.equal(teams.readTeamCastPlan(ids.tombstonePlan), null);
});
