import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  buildGraphProjection,
  graphConversationDetail,
  graphDeliveryDetail,
  graphNodeDetail,
} from "../src/graph-projection.js";

const ids = {
  project: "11345678-9234-4234-8234-123456789abc",
  codexA: "21345678-9234-4234-8234-123456789abc",
  codexB: "31345678-9234-4234-8234-123456789abc",
  claude: "41345678-9234-4234-8234-123456789abc",
  cluster: "51345678-9234-4234-8234-123456789abc",
  direct: "61345678-9234-4234-8234-123456789abc",
  group: "71345678-9234-4234-8234-123456789abc",
  recentMessage: "81345678-9234-4234-8234-123456789abc",
  oldMessage: "91345678-9234-4234-8234-123456789abc",
  groupMessage: "a1345678-9234-4234-8234-123456789abc",
};
const keys = {
  a: `codex:${ids.codexA}`,
  b: `codex:${ids.codexB}`,
  c: `claude:${ids.claude}`,
};
const now = Date.parse("2026-08-15T12:00:00.000Z");

function dependencies() {
  return {
    projects: () => [{
      projectId: ids.project,
      routingId: "graph-project",
      rootAliases: [{ path: "/private/project", lastSeenAt: "2026-08-15T11:00:00.000Z" }],
    }],
    nodes: () => [
      {
        nodeKey: keys.a,
        runtimeKind: "codex",
        projectId: ids.project,
        aliases: [{ value: "coordinator", lastSeenAt: "2026-08-15T11:00:00.000Z" }],
        selectedEndpoints: {
          app: { status: "reachable" },
        },
        updatedAt: "2026-08-15T11:00:00.000Z",
      },
      {
        nodeKey: keys.b,
        runtimeKind: "codex",
        projectId: ids.project,
        aliases: [{ value: "reviewer", lastSeenAt: "2026-08-15T11:00:00.000Z" }],
        selectedEndpoints: {
          app: { status: "unreachable", address: "/private/socket" },
        },
        updatedAt: "2026-08-15T11:00:00.000Z",
      },
    ],
    tombstones: () => [{
      nodeKey: keys.c,
      runtimeKind: "claude",
      projectId: ids.project,
      lastSafeLabel: "retired-reviewer",
      removedAt: "2026-08-15T10:00:00.000Z",
    }],
    clusters: () => [{
      clusterId: ids.cluster,
      routingId: "reviewers",
      members: [keys.a, keys.b],
      updatedAt: "2026-08-15T11:00:00.000Z",
    }],
    directConversations: () => [{
      kind: "direct",
      conversationId: ids.direct,
      currentMembers: [keys.a, keys.b],
      messages: [{
        sequence: 1,
        logicalMessageId: ids.recentMessage,
        senderNodeKey: keys.a,
        recipientNodeKey: keys.b,
        sourceKind: "delivery-ledger",
        replyToMessageId: null,
        bodyRef: "cxmsg-message:private-direct-body",
        recordedAt: "2026-08-15T11:31:00.000Z",
      }],
      updatedAt: "2026-08-15T11:00:00.000Z",
    }],
    groupConversations: () => [{
      kind: "group",
      conversationId: ids.group,
      projectId: ids.project,
      label: "audit-team",
      membershipSnapshots: [{ members: [keys.a, keys.b, keys.c] }],
      messages: [{
        sequence: 1,
        logicalMessageId: ids.groupMessage,
        senderNodeKey: keys.a,
        recipientNodeKeys: [keys.b, keys.c],
        membershipVersion: 1,
        replyToMessageId: null,
        hopCount: 0,
        expiry: "2026-08-15T13:00:00.000Z",
        bodyRef: "cxmsg-message:private-group-body",
        recordedAt: "2026-08-15T11:32:00.000Z",
      }],
      updatedAt: "2026-08-15T11:00:00.000Z",
    }],
    deliveries: () => [
      {
        logicalMessage: {
          messageId: ids.recentMessage,
          senderNodeKey: keys.a,
          createdAt: "2026-08-15T11:30:00.000Z",
          route: {
            kind: "direct",
            project_id: ids.project,
            conversation_id: ids.direct,
            payload_type: "coordination",
            wake_policy: "when-idle",
          },
          body: {
            bytes: 4096,
            sha256: "private-body-digest",
            contentRef: "cxmsg-message:redacted",
            privateText: "secret",
          },
        },
        delivery: {
          targetNodeKey: keys.b,
          admissionState: "admitted",
          state: "transport_delivered",
          wakePolicy: "when-idle",
          attempts: [{
            attemptId: "private-attempt-id",
            transport: "app-server",
            turnId: "private-turn-id",
            transportResult: { private: true },
          }],
          evidence: [{ state: "transport_delivered", detail: "private-evidence" }],
          replyHandle: { address: "/private/reply/socket" },
          updatedAt: "2026-08-15T11:31:00.000Z",
        },
      },
      {
        logicalMessage: {
          messageId: ids.oldMessage,
          senderNodeKey: keys.b,
          createdAt: "2026-08-15T09:00:00.000Z",
        },
        delivery: {
          targetNodeKey: keys.a,
          updatedAt: "2026-08-15T09:01:00.000Z",
        },
      },
    ],
    jobs: () => [{
      kind: "delegation",
      from: "coordinator",
      target: "reviewer",
      task: "private delegated task",
      updatedAt: "2026-08-15T11:45:00.000Z",
    }],
    sessions: () => [
      { name: "coordinator", threadId: ids.codexA },
      { name: "reviewer", threadId: ids.codexB, allowedDelegators: ["coordinator"], allowedClaudeRequesters: [{
        sessionId: ids.claude,
        permissions: ":read-only",
        token: "private-capability",
        grantedAt: "2026-08-15T11:40:00.000Z",
      }] },
    ],
    successors: () => [{
      predecessorNodeKey: keys.c,
      successorNodeKey: keys.b,
      linkedAt: "2026-08-15T10:30:00.000Z",
    }],
    claudeGrants: (record) => record.allowedClaudeRequesters || [],
  };
}

test("Graph Projection keeps relationship kinds separate and defaults to redaction", () => {
  const graph = buildGraphProjection({ range: "current", now }, dependencies());
  assert.equal(graph.summary.countsByKind["belongs-to-project"], 2);
  assert.equal(graph.summary.countsByKind["member-of-cluster"], 2);
  assert.equal(graph.summary.countsByKind["member-of-conversation"], 5);
  assert.equal(graph.summary.countsByKind["reachable-with"], 1);
  assert.equal(graph.summary.countsByKind["communicated-with"], 0);
  assert.equal(graph.summary.countsByKind["delegated-to"], 2);
  assert.equal(graph.summary.countsByKind["successor-of"], 1);
  const serialized = JSON.stringify(graph);
  assert.doesNotMatch(serialized, /private\/project|private\/socket|private delegated task/);
  assert.doesNotMatch(serialized, /private-capability|:read-only|cxmsg-message/);
  assert.ok(graph.edges.every((edge) => edge.ownerModule));
});

test("Graph Projection time and edge filters apply only to temporal evidence", () => {
  const oneHour = buildGraphProjection(
    { range: "1h", edgeKinds: ["communicated-with", "delegated-to"], now },
    dependencies(),
  );
  assert.equal(oneHour.summary.countsByKind["communicated-with"], 1);
  const communication = oneHour.edges.find(
    (edge) => edge.kind === "communicated-with",
  );
  assert.equal(communication.source, keys.a);
  assert.equal(communication.target, keys.b);
  const delegation = oneHour.edges.find(
    (edge) =>
      edge.kind === "delegated-to" && edge.source === keys.a && edge.target === keys.b,
  );
  assert.equal(delegation.count, 2);
  assert.deepEqual(delegation.evidenceKinds, ["codex-grant", "delegation-job"]);

  const all = buildGraphProjection(
    { range: "all", edgeKinds: ["communicated-with"], includePaths: true, now },
    dependencies(),
  );
  assert.equal(all.edges.length, 2);
  assert.deepEqual(
    all.entities.find((entity) => entity.kind === "project").paths,
    ["/private/project"],
  );
});

test("Graph Projection rejects unknown filters", () => {
  assert.throws(
    () => buildGraphProjection({ range: "week", now }, dependencies()),
    /Graph time range/,
  );
  assert.throws(
    () => buildGraphProjection({ edgeKinds: ["combined"], now }, dependencies()),
    /Graph edge kinds/,
  );
});

test("Graph Node detail composes only redacted incident relationships", () => {
  const detail = graphNodeDetail(keys.a, { range: "1h", now }, dependencies());
  assert.equal(detail.view, "node-detail");
  assert.equal(detail.node.id, keys.a);
  assert.equal(detail.project.id, `project:${ids.project}`);
  assert.deepEqual(detail.clusters.map((record) => record.id), [`cluster:${ids.cluster}`]);
  assert.equal(detail.conversations.length, 2);
  assert.ok(detail.relationships.every(
    (edge) => edge.source === keys.a || edge.target === keys.a,
  ));
  const serialized = JSON.stringify(detail);
  assert.doesNotMatch(serialized, /private\/project|private\/socket|private delegated task/);
  assert.doesNotMatch(serialized, /private-capability|:read-only|cxmsg-message/);
});

test("Graph Conversation detail returns bounded metadata without Message Body refs", () => {
  const detail = graphConversationDetail(
    ids.direct,
    { range: "current", limit: 1, now },
    dependencies(),
  );
  assert.equal(detail.view, "conversation-detail");
  assert.equal(detail.conversation.id, `conversation:${ids.direct}`);
  assert.deepEqual(detail.members, [keys.a, keys.b]);
  assert.equal(detail.messageCount, 1);
  assert.equal(detail.returnedMessageCount, 1);
  assert.equal(detail.messages[0].logicalMessageId, ids.recentMessage);
  assert.doesNotMatch(JSON.stringify(detail), /private-direct-body|bodyRef|contentRef/);
  assert.throws(
    () => graphConversationDetail(ids.direct, { limit: 201, now }, dependencies()),
    /between 1 and 200/,
  );

  const group = graphConversationDetail(
    ids.group,
    { range: "current", limit: 1, now },
    dependencies(),
  );
  assert.equal(group.conversation.conversationKind, "group");
  assert.equal(group.messages[0].recipientCount, 2);
  assert.doesNotMatch(JSON.stringify(group), /private-group-body|recipientNodeKeys/);
});

test("Graph Delivery detail separates recipient evidence and omits private attempt data", () => {
  const detail = graphDeliveryDetail(ids.recentMessage, dependencies());
  assert.equal(detail.view, "delivery-detail");
  assert.equal(detail.logicalMessage.logicalMessageId, ids.recentMessage);
  assert.equal(detail.logicalMessage.bodyBytes, 4096);
  assert.equal(detail.recipientCount, 1);
  assert.deepEqual(detail.statusCounts, { transport_delivered: 1 });
  assert.equal(detail.recipients[0].targetNodeKey, keys.b);
  assert.equal(detail.recipients[0].transport, "app-server");
  assert.deepEqual(detail.recipients[0].evidenceStates, ["transport_delivered"]);
  const serialized = JSON.stringify(detail);
  assert.doesNotMatch(
    serialized,
    /private-body-digest|cxmsg-message|private-attempt-id|private-turn-id|private-evidence|private\/reply\/socket/,
  );
  assert.doesNotMatch(serialized, /sha256|contentRef|privateText|attemptId|turnId|replyHandle/);
});

test("Graph Delivery detail redacts Inbound Policy identity and rule evidence", () => {
  const source = dependencies();
  const records = source.deliveries();
  records[0].delivery.inboundPolicy = {
    decision: "continue",
    reason: "no_match",
    targetNodeKey: keys.b,
    senderIdentityState: "verified",
    senderNodeKey: keys.a,
    senderProjectId: ids.project,
    policyRevision: 4,
    policySha256: "b".repeat(64),
    ruleId: null,
    selectorKind: null,
    failClosed: false,
  };
  records[0].delivery.evidence.push({
    state: "policy_denied",
    inboundPolicy: {
      decision: "deny",
      reason: "sender_denied",
      targetNodeKey: keys.b,
      senderIdentityState: "verified",
      senderNodeKey: keys.a,
      senderProjectId: ids.project,
      policyRevision: 5,
      policySha256: "c".repeat(64),
      ruleId: ids.cluster,
      selectorKind: "sender-node",
      failClosed: false,
    },
  });
  const detail = graphDeliveryDetail(ids.recentMessage, {
    ...source,
    deliveries: () => records,
  });
  assert.equal(detail.recipients[0].inboundPolicy.observationCount, 2);
  assert.equal(detail.recipients[0].inboundPolicy.latest.reason, "sender_denied");
  assert.equal(detail.recipients[0].inboundPolicy.latest.matchedRule, true);
  const policy = JSON.stringify(detail.recipients[0].inboundPolicy);
  assert.doesNotMatch(policy, new RegExp(ids.project.slice(0, 8)));
  assert.doesNotMatch(policy, new RegExp(ids.cluster.slice(0, 8)));
  assert.doesNotMatch(policy, /policySha256|senderNodeKey|senderProjectId|targetNodeKey|ruleId/);
});

test("graph CLI exposes the bounded read-only projection", () => {
  const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-graph-cli-"));
  try {
    const result = spawnSync(
      process.execPath,
      [
        path.resolve("bin/cxmsg.js"),
        "graph",
        "show",
        "--range",
        "1h",
        "--edge",
        "communicated-with",
        "--json",
      ],
      {
        encoding: "utf8",
        env: { ...process.env, CXMSG_STATE_DIR: stateDir },
      },
    );
    assert.equal(result.status, 0, result.stderr);
    const graph = JSON.parse(result.stdout);
    assert.equal(graph.range, "1h");
    assert.deepEqual(graph.edgeKinds, ["communicated-with"]);
    assert.equal(graph.summary.edgeCount, 0);
    assert.deepEqual(readdirSync(stateDir), []);

    const rejected = spawnSync(
      process.execPath,
      [
        path.resolve("bin/cxmsg.js"),
        "graph",
        "delivery",
        ids.recentMessage,
        "--range",
        "current",
      ],
      {
        encoding: "utf8",
        env: { ...process.env, CXMSG_STATE_DIR: stateDir },
      },
    );
    assert.equal(rejected.status, 1);
    assert.match(rejected.stderr, /accepts only --json/);
    assert.deepEqual(readdirSync(stateDir), []);
  } finally {
    rmSync(stateDir, { recursive: true, force: true });
  }
});
