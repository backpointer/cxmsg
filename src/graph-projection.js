import { listDirectConversations } from "./conversations.js";
import { listDeliveryLedgerReadOnly } from "./delivery-ledger.js";
import { listGroupConversations } from "./group-conversations.js";
import { listJobsReadOnly } from "./jobs.js";
import {
  listClusters,
  listNodes,
  listNodeTombstones,
  listProjects,
  listSuccessors,
} from "./node-directory.js";
import { listSessionRecords } from "./registry.js";
import { listClaudeRequestGrants } from "./claude-grants.js";

export const GRAPH_EDGE_KINDS = Object.freeze([
  "belongs-to-project",
  "member-of-cluster",
  "member-of-conversation",
  "reachable-with",
  "communicated-with",
  "delegated-to",
  "successor-of",
]);
export const GRAPH_TIME_RANGES = Object.freeze(["current", "1h", "24h", "all"]);

const RANGE_MS = Object.freeze({ "1h": 60 * 60 * 1_000, "24h": 24 * 60 * 60 * 1_000 });
const NODE_KEY_PATTERN = /^(codex|claude):[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

function normalizedKinds(edgeKinds) {
  const values = edgeKinds == null ? [...GRAPH_EDGE_KINDS] : [...edgeKinds];
  if (
    values.length === 0 ||
    values.some((value) => !GRAPH_EDGE_KINDS.includes(value))
  ) {
    throw new Error(`Graph edge kinds must be: ${GRAPH_EDGE_KINDS.join(", ")}`);
  }
  return [...new Set(values)].sort();
}

function cutoffFor(range, now) {
  if (!GRAPH_TIME_RANGES.includes(range)) {
    throw new Error(`Graph time range must be: ${GRAPH_TIME_RANGES.join(", ")}`);
  }
  if (!Number.isFinite(now)) throw new Error("Graph projection time is invalid");
  return RANGE_MS[range] ? now - RANGE_MS[range] : null;
}

function safeTimestamp(value) {
  return Number.isFinite(Date.parse(value || "")) ? value : null;
}

function latestAlias(record) {
  return [...(record.aliases || [])]
    .sort((left, right) =>
      String(left.lastSeenAt || "").localeCompare(String(right.lastSeenAt || "")),
    )
    .at(-1)?.value || record.nodeKey;
}

function ownerFor(kind) {
  if (["belongs-to-project", "member-of-cluster", "reachable-with", "successor-of"].includes(kind)) {
    return "node-directory";
  }
  if (kind === "member-of-conversation") return "conversation";
  if (kind === "communicated-with") return "delivery-ledger";
  return "grants-and-jobs";
}

export function buildGraphProjection(
  {
    range = "current",
    edgeKinds = GRAPH_EDGE_KINDS,
    includePaths = false,
    now = Date.now(),
  } = {},
  {
    projects = listProjects,
    nodes = listNodes,
    tombstones = listNodeTombstones,
    clusters = listClusters,
    directConversations = listDirectConversations,
    groupConversations = listGroupConversations,
    deliveries = listDeliveryLedgerReadOnly,
    jobs = listJobsReadOnly,
    sessions = listSessionRecords,
    successors = listSuccessors,
    claudeGrants = listClaudeRequestGrants,
  } = {},
) {
  const kinds = normalizedKinds(edgeKinds);
  const cutoff = cutoffFor(range, now);
  const entityMap = new Map();
  const edgeMap = new Map();
  let omittedReferences = 0;

  const addEntity = (entity) => {
    if (!entityMap.has(entity.id)) entityMap.set(entity.id, entity);
    return entityMap.get(entity.id);
  };
  const addEdge = ({ kind, source, target, at = null, current = false, evidenceKind }) => {
    if (!kinds.includes(kind)) return;
    if (!current) {
      if (range === "current") return;
      const timestamp = safeTimestamp(at);
      if (!timestamp || (cutoff !== null && Date.parse(timestamp) < cutoff)) return;
      at = timestamp;
    }
    if (!entityMap.has(source) || !entityMap.has(target)) {
      omittedReferences += 1;
      return;
    }
    const id = `${kind}|${source}|${target}`;
    const edge = edgeMap.get(id) || {
      id,
      kind,
      source,
      target,
      ownerModule: ownerFor(kind),
      current: false,
      count: 0,
      evidenceKinds: new Set(),
      firstObservedAt: null,
      latestObservedAt: null,
    };
    edge.current ||= current;
    edge.count += 1;
    edge.evidenceKinds.add(evidenceKind);
    if (at) {
      if (!edge.firstObservedAt || at < edge.firstObservedAt) edge.firstObservedAt = at;
      if (!edge.latestObservedAt || at > edge.latestObservedAt) edge.latestObservedAt = at;
    }
    edgeMap.set(id, edge);
  };
  const ensureUnresolvedNode = (identity, label = null) => {
    if (!NODE_KEY_PATTERN.test(identity || "")) return null;
    identity = identity.toLowerCase();
    if (!entityMap.has(identity)) {
      addEntity({
        id: identity,
        kind: "node",
        runtimeKind: identity.slice(0, identity.indexOf(":")),
        label: label || identity,
        projectId: null,
        lifecycle: "unresolved-directory",
      });
    }
    return identity;
  };
  const temporalIncluded = (at) => {
    if (range === "current") return false;
    const timestamp = safeTimestamp(at);
    return Boolean(
      timestamp && (cutoff === null || Date.parse(timestamp) >= cutoff),
    );
  };

  const projectRecords = projects();
  for (const project of projectRecords) {
    addEntity({
      id: `project:${project.projectId}`,
      kind: "project",
      label: project.routingId,
      projectId: project.projectId,
      ...(includePaths
        ? { paths: [...new Set((project.rootAliases || []).map((alias) => alias.path))].sort() }
        : {}),
    });
  }

  const nodeRecords = nodes();
  for (const node of nodeRecords) {
    addEntity({
      id: node.nodeKey,
      kind: "node",
      runtimeKind: node.runtimeKind,
      label: latestAlias(node),
      projectId: node.projectId,
      lifecycle: "live",
    });
  }
  for (const node of tombstones()) {
    addEntity({
      id: node.nodeKey,
      kind: "node",
      runtimeKind: node.runtimeKind,
      label: node.lastSafeLabel,
      projectId: node.projectId,
      lifecycle: "tombstoned",
    });
  }

  const sessionRecords = sessions();
  for (const session of sessionRecords) {
    ensureUnresolvedNode(
      `codex:${String(session.threadId).toLowerCase()}`,
      session.name,
    );
  }

  addEntity({ id: "observer:local", kind: "observer", label: "Local cxmsg owner" });
  for (const node of nodeRecords) {
    addEdge({
      kind: "belongs-to-project",
      source: node.nodeKey,
      target: `project:${node.projectId}`,
      current: true,
      evidenceKind: "directory-node",
    });
    if (
      Object.values(node.selectedEndpoints || {}).some(
        (endpoint) => endpoint.status === "reachable",
      )
    ) {
      addEdge({
        kind: "reachable-with",
        source: "observer:local",
        target: node.nodeKey,
        at: node.updatedAt,
        current: true,
        evidenceKind: "selected-endpoint",
      });
    }
  }

  for (const cluster of clusters()) {
    const clusterId = `cluster:${cluster.clusterId}`;
    addEntity({
      id: clusterId,
      kind: "cluster",
      label: cluster.routingId,
      projectIds: [...new Set(cluster.members.map((member) => entityMap.get(member)?.projectId).filter(Boolean))].sort(),
    });
    for (const member of cluster.members) {
      ensureUnresolvedNode(member);
      addEdge({
        kind: "member-of-cluster",
        source: member,
        target: clusterId,
        at: cluster.updatedAt,
        current: true,
        evidenceKind: "cluster-head",
      });
    }
  }

  for (const conversation of directConversations()) {
    const conversationId = `conversation:${conversation.conversationId}`;
    const memberProjectIds = [...new Set(
      conversation.currentMembers
        .map((member) => entityMap.get(member)?.projectId)
        .filter(Boolean),
    )];
    addEntity({
      id: conversationId,
      kind: "conversation",
      conversationKind: "direct",
      label: "Direct conversation",
      projectId: memberProjectIds.length === 1 ? memberProjectIds[0] : null,
    });
    for (const member of conversation.currentMembers) {
      ensureUnresolvedNode(member);
      addEdge({
        kind: "member-of-conversation",
        source: member,
        target: conversationId,
        at: conversation.updatedAt,
        current: true,
        evidenceKind: "direct-membership",
      });
    }
  }
  for (const conversation of groupConversations()) {
    const conversationId = `conversation:${conversation.conversationId}`;
    addEntity({
      id: conversationId,
      kind: "conversation",
      conversationKind: "group",
      label: conversation.label,
      projectId: conversation.projectId,
    });
    for (const member of conversation.membershipSnapshots.at(-1)?.members || []) {
      ensureUnresolvedNode(member);
      addEdge({
        kind: "member-of-conversation",
        source: member,
        target: conversationId,
        at: conversation.updatedAt,
        current: true,
        evidenceKind: "group-membership",
      });
    }
  }

  const nodeBySession = new Map(
    sessionRecords.map((record) => [record.name, `codex:${String(record.threadId).toLowerCase()}`]),
  );
  for (const record of deliveries()) {
    const sender = NODE_KEY_PATTERN.test(record.logicalMessage.senderNodeKey || "")
      ? record.logicalMessage.senderNodeKey.toLowerCase()
      : nodeBySession.get(record.logicalMessage.from);
    const recipients = record.teamDeliveries || record.groupDeliveries || [record.delivery];
    for (const delivery of recipients) {
      const target = NODE_KEY_PATTERN.test(delivery?.targetNodeKey || "")
        ? delivery.targetNodeKey.toLowerCase()
        : delivery?.targetThreadId
          ? `codex:${String(delivery.targetThreadId).toLowerCase()}`
          : nodeBySession.get(delivery?.target);
      const observedAt = delivery?.updatedAt || record.logicalMessage.createdAt;
      if (temporalIncluded(observedAt)) {
        ensureUnresolvedNode(sender);
        ensureUnresolvedNode(target);
      }
      addEdge({
        kind: "communicated-with",
        source: sender,
        target,
        at: observedAt,
        evidenceKind: "logical-message",
      });
    }
  }

  for (const session of sessionRecords) {
    const target = nodeBySession.get(session.name);
    for (const senderName of session.allowedDelegators || []) {
      ensureUnresolvedNode(nodeBySession.get(senderName), senderName);
      ensureUnresolvedNode(target, session.name);
      addEdge({
        kind: "delegated-to",
        source: nodeBySession.get(senderName),
        target,
        current: true,
        evidenceKind: "codex-grant",
      });
    }
    for (const grant of claudeGrants(session)) {
      const source = ensureUnresolvedNode(
        `claude:${String(grant.sessionId).toLowerCase()}`,
        grant.name,
      );
      ensureUnresolvedNode(target, session.name);
      addEdge({
        kind: "delegated-to",
        source,
        target,
        at: grant.grantedAt,
        current: true,
        evidenceKind: "claude-grant",
      });
    }
  }
  for (const job of jobs()) {
    if (job.kind !== "delegation") continue;
    const observedAt = job.updatedAt || job.createdAt;
    const source = nodeBySession.get(job.from);
    const target = nodeBySession.get(job.target);
    if (temporalIncluded(observedAt)) {
      ensureUnresolvedNode(source, job.from);
      ensureUnresolvedNode(target, job.target);
    }
    addEdge({
      kind: "delegated-to",
      source,
      target,
      at: observedAt,
      evidenceKind: "delegation-job",
    });
  }

  for (const relation of successors()) {
    ensureUnresolvedNode(relation.successorNodeKey);
    ensureUnresolvedNode(relation.predecessorNodeKey);
    addEdge({
      kind: "successor-of",
      source: relation.successorNodeKey,
      target: relation.predecessorNodeKey,
      at: relation.linkedAt,
      current: true,
      evidenceKind: "successor-record",
    });
  }

  const entities = [...entityMap.values()]
    .filter((entity) =>
      entity.id !== "observer:local" ||
      [...edgeMap.values()].some(
        (edge) => edge.source === entity.id || edge.target === entity.id,
      ),
    )
    .sort((left, right) => left.id.localeCompare(right.id));
  const edges = [...edgeMap.values()]
    .map((edge) => ({ ...edge, evidenceKinds: [...edge.evidenceKinds].sort() }))
    .sort((left, right) => left.id.localeCompare(right.id));
  return {
    version: 1,
    range,
    edgeKinds: kinds,
    generatedAt: new Date(now).toISOString(),
    entities,
    edges,
    summary: {
      entityCount: entities.length,
      edgeCount: edges.length,
      omittedReferences,
      unresolvedEntityCount: entities.filter(
        (entity) => entity.lifecycle === "unresolved-directory",
      ).length,
      countsByKind: Object.fromEntries(
        kinds.map((kind) => [kind, edges.filter((edge) => edge.kind === kind).length]),
      ),
    },
  };
}

function validateNodeKey(value) {
  if (!NODE_KEY_PATTERN.test(value || "")) {
    throw new Error("Graph Node detail requires a stable Node key");
  }
  return value.toLowerCase();
}

function validateUuid(label, value) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value || "")) {
    throw new Error(`${label} must be a UUID`);
  }
  return value.toLowerCase();
}

function incidentEdges(graph, entityId) {
  return graph.edges.filter(
    (edge) => edge.source === entityId || edge.target === entityId,
  );
}

export function graphNodeDetail(
  nodeKey,
  options = {},
  dependencies = {},
) {
  nodeKey = validateNodeKey(nodeKey);
  const graph = buildGraphProjection(options, dependencies);
  const node = graph.entities.find((entity) => entity.id === nodeKey);
  if (!node) throw new Error(`unknown Graph Node: ${nodeKey}`);
  const relationships = incidentEdges(graph, nodeKey);
  const targetEntities = new Map(graph.entities.map((entity) => [entity.id, entity]));
  const related = (kind) =>
    relationships
      .filter((edge) => edge.kind === kind)
      .map((edge) =>
        targetEntities.get(edge.source === nodeKey ? edge.target : edge.source),
      )
      .filter(Boolean)
      .sort((left, right) => left.id.localeCompare(right.id));
  return {
    version: 1,
    view: "node-detail",
    range: graph.range,
    generatedAt: graph.generatedAt,
    node,
    project: related("belongs-to-project")[0] || null,
    clusters: related("member-of-cluster"),
    conversations: related("member-of-conversation"),
    relationships,
    relationshipCounts: Object.fromEntries(
      graph.edgeKinds.map((kind) => [
        kind,
        relationships.filter((edge) => edge.kind === kind).length,
      ]),
    ),
  };
}

function boundedDetailLimit(limit) {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200) {
    throw new Error("Graph detail limit must be between 1 and 200");
  }
  return limit;
}

export function graphConversationDetail(
  conversationId,
  { limit = 50, ...options } = {},
  dependencies = {},
) {
  conversationId = validateUuid("Graph Conversation id", conversationId);
  limit = boundedDetailLimit(limit);
  const graph = buildGraphProjection(options, dependencies);
  const entityId = `conversation:${conversationId}`;
  const conversation = graph.entities.find((entity) => entity.id === entityId);
  if (!conversation) {
    throw new Error(`unknown Graph Conversation: ${conversationId}`);
  }
  const directRecords = (dependencies.directConversations || listDirectConversations)();
  const groupRecords = (dependencies.groupConversations || listGroupConversations)();
  const source =
    directRecords.find((record) => record.conversationId === conversationId) ||
    groupRecords.find((record) => record.conversationId === conversationId);
  if (!source) throw new Error(`Graph Conversation source is missing: ${conversationId}`);
  const sourceMessages = source.messages || [];
  const messages = sourceMessages.slice(-limit).map((message) =>
    source.kind === "direct"
      ? {
          sequence: message.sequence,
          logicalMessageId: message.logicalMessageId,
          senderNodeKey: message.senderNodeKey,
          recipientNodeKey: message.recipientNodeKey,
          sourceKind: message.sourceKind,
          replyToMessageId: message.replyToMessageId,
          recordedAt: message.recordedAt,
        }
      : {
          sequence: message.sequence,
          logicalMessageId: message.logicalMessageId,
          senderNodeKey: message.senderNodeKey,
          recipientCount: message.recipientNodeKeys.length,
          membershipVersion: message.membershipVersion,
          replyToMessageId: message.replyToMessageId,
          hopCount: message.hopCount,
          expiry: message.expiry,
          recordedAt: message.recordedAt,
        },
  );
  const membership = incidentEdges(graph, entityId).filter(
    (edge) => edge.kind === "member-of-conversation",
  );
  return {
    version: 1,
    view: "conversation-detail",
    range: graph.range,
    generatedAt: graph.generatedAt,
    conversation,
    members: membership.map((edge) => edge.source).sort(),
    membership,
    messageCount: sourceMessages.length,
    returnedMessageCount: messages.length,
    remainingMessageCount: Math.max(0, sourceMessages.length - messages.length),
    messages,
  };
}

function publicDelivery(delivery) {
  return {
    targetNodeKey: delivery.targetNodeKey ||
      (delivery.targetThreadId ? `codex:${String(delivery.targetThreadId).toLowerCase()}` : null),
    admissionState: delivery.admissionState,
    state: delivery.state,
    wakePolicy: delivery.wakePolicy,
    transport: delivery.attempts?.[0]?.transport || null,
    attemptCount: delivery.attempts?.length || 0,
    evidenceStates: [...new Set((delivery.evidence || []).map((item) => item.state))].sort(),
    errorCode: delivery.errorCode || null,
    updatedAt: delivery.updatedAt || null,
  };
}

export function graphDeliveryDetail(
  logicalMessageId,
  dependencies = {},
) {
  logicalMessageId = validateUuid("Graph Delivery Logical Message id", logicalMessageId);
  const records = (dependencies.deliveries || listDeliveryLedgerReadOnly)();
  const record = records.find(
    (candidate) => candidate.logicalMessage.messageId === logicalMessageId,
  );
  if (!record) throw new Error(`unknown Graph Delivery: ${logicalMessageId}`);
  const deliveries = record.teamDeliveries || record.groupDeliveries || [record.delivery];
  const recipients = deliveries.map(publicDelivery).sort((left, right) =>
    String(left.targetNodeKey).localeCompare(String(right.targetNodeKey)),
  );
  const route = record.logicalMessage.route || {};
  return {
    version: 1,
    view: "delivery-detail",
    logicalMessage: {
      logicalMessageId,
      senderNodeKey: record.logicalMessage.senderNodeKey || null,
      replyToMessageId: record.logicalMessage.replyToMessageId || null,
      routeKind: route.kind || null,
      projectId:
        route.project_id ||
        record.logicalMessage.teamCast?.projectId ||
        null,
      conversationId:
        route.conversation_id ||
        record.logicalMessage.group?.conversationId ||
        null,
      payloadType: route.payload_type || null,
      wakePolicy:
        route.wake_policy ||
        record.logicalMessage.teamCast?.wakePolicy ||
        null,
      bodyBytes: record.logicalMessage.body?.bytes || null,
      createdAt: record.logicalMessage.createdAt,
    },
    recipientCount: recipients.length,
    statusCounts: Object.fromEntries(
      [...new Set(recipients.map((recipient) => recipient.state))]
        .sort()
        .map((state) => [
          state,
          recipients.filter((recipient) => recipient.state === state).length,
        ]),
    ),
    recipients,
  };
}
