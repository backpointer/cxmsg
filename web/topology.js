const pendingStatuses = new Set(["dispatching", "queued", "running"]);

function projectName(value) {
  if (!value) return "Detached / historical";
  const parts = String(value).split("/").filter(Boolean);
  return parts.at(-1) || value;
}

export function buildTopology(snapshot) {
  const nodes = new Map();
  const codexByName = new Map();
  const claudeBySession = new Map();
  const claudeByName = new Map();

  function addNode(node) {
    if (!nodes.has(node.id)) nodes.set(node.id, node);
    return nodes.get(node.id);
  }

  for (const session of snapshot.codexSessions || []) {
    const node = addNode({
      id: `codex:${session.name}`,
      label: session.name,
      type: "codex",
      status: session.status,
      projectKey: session.cwd || "__detached__",
      projectLabel: projectName(session.cwd),
      projectPath: session.cwd || null,
    });
    codexByName.set(session.name, node);
  }

  for (const session of snapshot.claudeSessions || []) {
    const node = addNode({
      id: `claude:${session.sessionId || session.pid}`,
      label: session.name,
      type: "claude",
      status: session.status,
      projectKey: session.cwd || "__detached__",
      projectLabel: projectName(session.cwd),
      projectPath: session.cwd || null,
    });
    if (session.sessionId) claudeBySession.set(session.sessionId, node);
    claudeByName.set(session.name, node);
  }

  function historicalNode(label, side) {
    return addNode({
      id: `historical:${side}:${label || "unknown"}`,
      label: label || "unknown",
      type: "historical",
      status: "historical",
      projectKey: "__detached__",
      projectLabel: "Detached / historical",
      projectPath: null,
    });
  }

  const edges = new Map();
  for (const job of snapshot.jobs || []) {
    const source =
      codexByName.get(job.from) ||
      claudeBySession.get(job.sourceSessionId) ||
      claudeByName.get(job.sourceName) ||
      historicalNode(job.sourceName || job.from || "external", "source");
    const target = codexByName.get(job.target) || historicalNode(job.target, "target");
    const key = `${source.id}\u0000${target.id}`;
    const edge = edges.get(key) || {
      source,
      target,
      count: 0,
      statuses: new Set(),
      latestAt: null,
    };
    edge.count += 1;
    edge.statuses.add(job.status);
    if (!edge.latestAt || String(job.updatedAt) > String(edge.latestAt)) {
      edge.latestAt = job.updatedAt;
    }
    edges.set(key, edge);
  }

  const projects = new Map();
  for (const node of nodes.values()) {
    const project = projects.get(node.projectKey) || {
      key: node.projectKey,
      label: node.projectLabel,
      path: node.projectPath,
      nodes: [],
    };
    project.nodes.push(node);
    projects.set(node.projectKey, project);
  }
  const orderedProjects = [...projects.values()].sort((left, right) => {
    if (left.key === "__detached__") return 1;
    if (right.key === "__detached__") return -1;
    return left.label.localeCompare(right.label);
  });
  for (const project of orderedProjects) {
    project.nodes.sort((left, right) =>
      `${left.type}:${left.label}`.localeCompare(`${right.type}:${right.label}`),
    );
  }
  return { projects: orderedProjects, edges: [...edges.values()] };
}

export function edgeTone(edge) {
  if ([...edge.statuses].some((status) => ["failed", "unknown", "cancelled"].includes(status))) {
    return "failed";
  }
  if ([...edge.statuses].some((status) => pendingStatuses.has(status))) return "running";
  if (edge.statuses.has("completed")) return "completed";
  return "neutral";
}

export function focusProject(topology, projectKey) {
  const project =
    topology.projects.find((candidate) => candidate.key === projectKey) || topology.projects[0] || null;
  if (!project) return { project: null, localEdges: [], crossEdges: [] };
  const nodeIds = new Set(project.nodes.map((node) => node.id));
  const localEdges = topology.edges.filter(
    (edge) => nodeIds.has(edge.source.id) && nodeIds.has(edge.target.id),
  );
  const crossEdges = topology.edges.filter(
    (edge) => nodeIds.has(edge.source.id) !== nodeIds.has(edge.target.id),
  );
  return { project, localEdges, crossEdges };
}
