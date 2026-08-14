import { buildTopology, edgeTone, focusProject } from "./topology.js";

const POLL_INTERVAL_MS = 2_000;
const pendingStatuses = new Set(["dispatching", "queued", "running"]);

function element(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  const target = element(id);
  if (target) target.textContent = String(value);
}

function shortId(value, size = 8) {
  return value ? String(value).slice(0, size) : "—";
}

function projectName(value) {
  if (!value) return "—";
  const parts = String(value).split("/").filter(Boolean);
  return parts.at(-1) || value;
}

function formatTime(value) {
  if (!value) return "—";
  let parsed = typeof value === "number" ? value : Date.parse(value);
  if (typeof value === "number" && value < 1_000_000_000_000) parsed *= 1_000;
  if (!Number.isFinite(parsed)) return "—";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(parsed);
}

function badge(value, tone = value) {
  const span = document.createElement("span");
  span.className = `badge ${String(tone || "neutral").toLowerCase()}`;
  span.textContent = value || "—";
  return span;
}

function textCell(value, className = "") {
  const cell = document.createElement("td");
  cell.className = className;
  cell.textContent = value ?? "—";
  return cell;
}

function detail(primary, secondary, title = null) {
  const wrapper = document.createElement("div");
  wrapper.className = "detail";
  const strong = document.createElement("strong");
  strong.textContent = primary || "—";
  if (title) strong.title = title;
  const small = document.createElement("small");
  small.textContent = secondary || "";
  wrapper.append(strong, small);
  return wrapper;
}

function emptyRow(columns, message) {
  const row = document.createElement("tr");
  const cell = textCell(message, "empty");
  cell.colSpan = columns;
  row.append(cell);
  return row;
}

function renderAuthority(session) {
  const wrapper = document.createElement("div");
  wrapper.className = "chip-list";
  for (const delegator of session.delegators) {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = `${delegator} →`;
    chip.title = "Codex delegator";
    wrapper.append(chip);
  }
  for (const grant of session.claudeGrants) {
    const chip = document.createElement("span");
    chip.className = "chip claude";
    chip.textContent = `${grant.name || shortId(grant.sessionId)} · ${grant.permissions}`;
    chip.title = "Claude request grant";
    wrapper.append(chip);
  }
  for (const profile of session.permissionProfiles.filter((candidate) => candidate.allowed)) {
    const chip = document.createElement("span");
    chip.className = "chip profile";
    chip.textContent = profile.id;
    chip.title = profile.description || "Allowed permission profile";
    wrapper.append(chip);
  }
  if (!wrapper.childElementCount) wrapper.textContent = "None";
  return wrapper;
}

function renderDashboard(snapshot) {
  const codex = snapshot.codexSessions || [];
  const claude = snapshot.claudeSessions || [];
  const working = codex.filter((session) => session.status === "active").length;
  const busy = claude.filter((session) => session.status === "busy").length;
  const grants = codex.reduce(
    (total, session) => total + session.delegators.length + session.claudeGrants.length,
    0,
  );

  setText("server-state", snapshot.server.running ? "Running" : "Unavailable");
  setText("server-detail", `PID ${snapshot.server.pid || "—"} · ${snapshot.server.transport}`);
  setText("codex-count", codex.length);
  setText("codex-active", `${working} working`);
  setText("claude-count", claude.length);
  setText("claude-active", `${busy} busy`);
  setText("grant-count", grants);

  const codexBody = element("codex-table-body");
  codexBody.replaceChildren();
  if (!codex.length) codexBody.append(emptyRow(6, "No registered Codex sessions"));
  for (const session of codex) {
    const row = document.createElement("tr");
    const identity = document.createElement("td");
    identity.append(detail(session.name, shortId(session.threadId), session.threadId));
    const state = document.createElement("td");
    state.append(badge(session.status, session.status));
    const project = textCell(projectName(session.cwd));
    project.title = session.cwd || "";
    const connection = document.createElement("td");
    connection.append(
      detail(session.presentation, session.attachedPid ? `TUI PID ${session.attachedPid}` : "No live TUI"),
    );
    const authority = document.createElement("td");
    authority.append(renderAuthority(session));
    const bridge = document.createElement("td");
    bridge.append(badge(session.bridge.running ? "running" : "stopped", session.bridge.running ? "active" : "neutral"));
    row.append(identity, state, project, connection, authority, bridge);
    codexBody.append(row);
  }

  const claudeBody = element("claude-table-body");
  claudeBody.replaceChildren();
  if (!claude.length) claudeBody.append(emptyRow(5, "No live Claude sessions"));
  for (const peer of claude) {
    const row = document.createElement("tr");
    row.append(textCell(peer.name));
    const state = document.createElement("td");
    state.append(badge(peer.status, peer.status));
    const project = textCell(projectName(peer.cwd));
    project.title = peer.cwd || "";
    const sessionId = textCell(shortId(peer.sessionId), "mono");
    sessionId.title = peer.sessionId || "";
    row.append(state, project, sessionId, textCell(peer.pid, "mono"));
    claudeBody.append(row);
  }
}

function jobNeedsAttention(job) {
  return ["failed", "unknown", "cancelled"].includes(job.status) || job.replyStatus === "failed";
}

const SVG_NAMESPACE = "http://www.w3.org/2000/svg";
let latestTopologySnapshot = null;
let topologyResizeObserver = null;
let selectedProjectKey = null;

function svgElement(name, attributes = {}) {
  const node = document.createElementNS(SVG_NAMESPACE, name);
  for (const [key, value] of Object.entries(attributes)) {
    node.setAttribute(key, String(value));
  }
  return node;
}

function compactLabel(value, limit = 22) {
  const text = String(value || "unknown");
  return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
}

function nodeAnchor(node, other) {
  const centerX = node.x + node.width / 2;
  const centerY = node.y + node.height / 2;
  const otherX = other.x + other.width / 2;
  const otherY = other.y + other.height / 2;
  const deltaX = otherX - centerX;
  const deltaY = otherY - centerY;
  const scale = 1 / Math.max(Math.abs(deltaX) / (node.width / 2), Math.abs(deltaY) / (node.height / 2));
  return { x: centerX + deltaX * scale, y: centerY + deltaY * scale };
}

function drawTopology(snapshot) {
  const container = element("topology-graph");
  if (!container) return;
  const topology = buildTopology(snapshot);
  if (!topology.projects.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No sessions available for the graph";
    container.replaceChildren(empty);
    return;
  }

  const picker = element("project-filter");
  const availableKeys = new Set(topology.projects.map((project) => project.key));
  if (!selectedProjectKey || !availableKeys.has(selectedProjectKey)) {
    selectedProjectKey =
      topology.projects
        .filter((project) => project.key !== "__detached__")
        .map((project) => ({
          project,
          activity: topology.edges
            .filter(
              (edge) =>
                edge.source.projectKey === project.key || edge.target.projectKey === project.key,
            )
            .reduce((total, edge) => total + edge.count, 0),
        }))
        .sort((left, right) => right.activity - left.activity)[0]?.project.key ||
      topology.projects[0].key;
  }
  const optionsSignature = topology.projects.map((project) => project.key).join("\u0000");
  if (picker && picker.dataset.signature !== optionsSignature) {
    picker.replaceChildren(
      ...topology.projects.map((project) => {
        const option = document.createElement("option");
        option.value = project.key;
        option.textContent = project.label;
        return option;
      }),
    );
    picker.dataset.signature = optionsSignature;
  }
  if (picker) picker.value = selectedProjectKey;

  const focused = focusProject(topology, selectedProjectKey);
  const project = focused.project;
  const externalNodes = [
    ...new Map(
      focused.crossEdges.flatMap((edge) =>
        edge.source.projectKey === project.key
          ? [[edge.target.id, edge.target]]
          : [[edge.source.id, edge.source]],
      ),
    ).values(),
  ];
  const localJobCount = focused.localEdges.reduce((total, edge) => total + edge.count, 0);
  const crossJobCount = focused.crossEdges.reduce((total, edge) => total + edge.count, 0);
  setText(
    "project-summary",
    `${project.nodes.length} session${project.nodes.length === 1 ? "" : "s"} · ${localJobCount} internal · ${crossJobCount} cross-project`,
  );

  const width = Math.max(320, container.clientWidth || 900);
  const outerPadding = 22;
  const nodeGap = 14;
  const nodeHeight = 54;
  const zoneLabelHeight = 28;
  const hasExternal = externalNodes.length > 0;
  const horizontalZones = hasExternal && width >= 720;
  const selectedAreaWidth = horizontalZones ? width * 0.62 : width;
  const selectedColumns = selectedAreaWidth >= 680 ? 2 : 1;
  const selectedNodeWidth =
    (selectedAreaWidth - outerPadding * 2 - nodeGap * (selectedColumns - 1)) / selectedColumns;
  const selectedRows = Math.ceil(project.nodes.length / selectedColumns);
  const selectedHeight =
    zoneLabelHeight + selectedRows * nodeHeight + Math.max(0, selectedRows - 1) * nodeGap;
  const externalColumns = horizontalZones ? 1 : width >= 720 ? 3 : width >= 500 ? 2 : 1;
  const externalAreaWidth = horizontalZones ? width - selectedAreaWidth : width;
  const externalNodeWidth =
    (externalAreaWidth - outerPadding * 2 - nodeGap * (externalColumns - 1)) / externalColumns;
  const externalRows = Math.ceil(externalNodes.length / externalColumns);
  const externalHeight = hasExternal
    ? zoneLabelHeight + externalRows * nodeHeight + Math.max(0, externalRows - 1) * nodeGap
    : 0;
  const height = Math.max(
    170,
    outerPadding * 2 +
      (horizontalZones
        ? Math.max(selectedHeight, externalHeight)
        : selectedHeight + (hasExternal ? externalHeight + outerPadding : 0)),
  );
  project.nodes.forEach((node, nodeIndex) => {
    const nodeColumn = nodeIndex % selectedColumns;
    const nodeRow = Math.floor(nodeIndex / selectedColumns);
    node.x = outerPadding + nodeColumn * (selectedNodeWidth + nodeGap);
    node.y = outerPadding + zoneLabelHeight + nodeRow * (nodeHeight + nodeGap);
    node.width = selectedNodeWidth;
    node.height = nodeHeight;
  });
  externalNodes.forEach((node, nodeIndex) => {
    const nodeColumn = nodeIndex % externalColumns;
    const nodeRow = Math.floor(nodeIndex / externalColumns);
    node.x =
      (horizontalZones ? selectedAreaWidth : 0) +
      outerPadding +
      nodeColumn * (externalNodeWidth + nodeGap);
    node.y =
      outerPadding +
      (horizontalZones ? 0 : selectedHeight + outerPadding) +
      zoneLabelHeight +
      nodeRow * (nodeHeight + nodeGap);
    node.width = externalNodeWidth;
    node.height = nodeHeight;
  });
  const svg = svgElement("svg", {
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-label": `${project.label} project with ${project.nodes.length} sessions`,
  });
  const title = svgElement("title");
  title.textContent = `${project.label} session topology`;
  svg.append(title);

  const defs = svgElement("defs");
  for (const tone of ["neutral", "running", "completed", "failed"]) {
    const marker = svgElement("marker", {
      id: `arrow-${tone}`,
      viewBox: "0 0 10 10",
      refX: 9,
      refY: 5,
      markerWidth: 6,
      markerHeight: 6,
      orient: "auto-start-reverse",
    });
    marker.append(svgElement("path", { class: `edge-arrow ${tone}`, d: "M 0 0 L 10 5 L 0 10 z" }));
    defs.append(marker);
  }
  svg.append(defs);

  const guideLayer = svgElement("g", { class: "graph-guides" });
  const edgeLayer = svgElement("g", { class: "graph-edges" });
  const nodeLayer = svgElement("g", { class: "graph-nodes" });
  svg.append(guideLayer, edgeLayer, nodeLayer);

  const selectedLabel = svgElement("text", {
    class: "graph-zone-label",
    x: outerPadding,
    y: outerPadding + 12,
  });
  selectedLabel.textContent = project.label;
  guideLayer.append(selectedLabel);
  if (hasExternal) {
    const externalLabel = svgElement("text", {
      class: "graph-zone-label external",
      x: horizontalZones ? selectedAreaWidth + outerPadding : outerPadding,
      y: horizontalZones ? outerPadding + 12 : outerPadding + selectedHeight + outerPadding + 12,
    });
    externalLabel.textContent = "Connected projects";
    const divider = horizontalZones
      ? svgElement("line", {
          class: "graph-zone-divider",
          x1: selectedAreaWidth,
          y1: outerPadding,
          x2: selectedAreaWidth,
          y2: height - outerPadding,
        })
      : svgElement("line", {
          class: "graph-zone-divider",
          x1: outerPadding,
          y1: outerPadding + selectedHeight + outerPadding / 2,
          x2: width - outerPadding,
          y2: outerPadding + selectedHeight + outerPadding / 2,
        });
    guideLayer.append(divider, externalLabel);
  }

  for (const edge of [...focused.localEdges, ...focused.crossEdges]) {
    const selfEdge = edge.source.id === edge.target.id;
    const start = selfEdge
      ? { x: edge.source.x + edge.source.width * 0.68, y: edge.source.y }
      : nodeAnchor(edge.source, edge.target);
    const end = selfEdge
      ? { x: edge.target.x + edge.target.width * 0.86, y: edge.target.y }
      : nodeAnchor(edge.target, edge.source);
    const startX = start.x;
    const startY = start.y;
    const endX = end.x;
    const endY = end.y;
    const middleX = (startX + endX) / 2;
    const tone = edgeTone(edge);
    const crossProject = edge.source.projectKey !== edge.target.projectKey;
    const path = svgElement("path", {
      class: `graph-edge ${crossProject ? "cross" : "local"} ${tone}`,
      d: selfEdge
        ? `M ${startX} ${startY} C ${startX} ${startY - 34}, ${endX} ${endY - 34}, ${endX} ${endY}`
        : `M ${startX} ${startY} C ${middleX} ${startY}, ${middleX} ${endY}, ${endX} ${endY}`,
      "stroke-width": Math.min(4, 1.4 + Math.log2(edge.count + 1) * 0.55),
      "marker-end": `url(#arrow-${tone})`,
    });
    const edgeTitle = svgElement("title");
    edgeTitle.textContent = `${edge.source.label} → ${edge.target.label}: ${edge.count} job${edge.count === 1 ? "" : "s"}`;
    path.append(edgeTitle);
    edgeLayer.append(path);
  }

  for (const node of [...project.nodes, ...externalNodes]) {
    const group = svgElement("g", { class: `graph-node ${node.type}` });
    const nodeTitle = svgElement("title");
    nodeTitle.textContent = `${node.label} · ${node.type} · ${node.status} · ${project.path || project.label}`;
    group.append(nodeTitle);
    group.append(
      svgElement("rect", {
        x: node.x,
        y: node.y,
        width: node.width,
        height: node.height,
        rx: 9,
      }),
      svgElement("circle", {
        class: `node-status ${node.status}`,
        cx: node.x + 14,
        cy: node.y + 17,
        r: 4,
      }),
    );
    const type = svgElement("text", {
      class: "node-type",
      x: node.x + 25,
      y: node.y + 20,
    });
    type.textContent =
      node.projectKey === project.key ? node.type : `${node.projectLabel} · ${node.type}`;
    const label = svgElement("text", {
      class: "node-label",
      x: node.x + 14,
      y: node.y + 42,
    });
    label.textContent = compactLabel(node.label, Math.max(12, Math.floor(node.width / 8)));
    group.append(type, label);
    nodeLayer.append(group);
  }

  container.replaceChildren(svg);
}

function renderTopology(snapshot) {
  latestTopologySnapshot = snapshot;
  drawTopology(snapshot);
  const container = element("topology-graph");
  if (container && !topologyResizeObserver && typeof ResizeObserver === "function") {
    topologyResizeObserver = new ResizeObserver(() => {
      if (latestTopologySnapshot) drawTopology(latestTopologySnapshot);
    });
    topologyResizeObserver.observe(container);
  }
}

function renderOrchestration(snapshot) {
  const jobs = snapshot.jobs || [];
  const running = jobs.filter((job) => pendingStatuses.has(job.status)).length;
  const completed = jobs.filter((job) => job.status === "completed").length;
  const failed = jobs.filter(jobNeedsAttention).length;
  setText("job-total", jobs.length);
  setText("job-running", running);
  setText("job-completed", completed);
  setText("job-failed", failed);
  renderTopology(snapshot);

  const body = element("job-table-body");
  body.replaceChildren();
  if (!jobs.length) body.append(emptyRow(7, "No correlation jobs recorded"));
  const visibleJobs = jobs.slice(0, 20);
  setText(
    "job-table-caption",
    jobs.length > visibleJobs.length
      ? `Newest ${visibleJobs.length} of ${jobs.length} jobs. IDs are shortened visually.`
      : `Newest ${jobs.length} job${jobs.length === 1 ? "" : "s"}. IDs are shortened visually.`,
  );
  for (const job of visibleJobs) {
    const row = document.createElement("tr");
    const id = textCell(shortId(job.jobId), "mono");
    id.title = job.jobId;
    const route = document.createElement("td");
    route.append(detail(`${job.from || "external"} → ${job.target || "unknown"}`, shortId(job.turnId), job.turnId));
    const state = document.createElement("td");
    state.append(badge(job.status, job.status));
    const reply = document.createElement("td");
    reply.append(badge(job.replyStatus || "—", job.replyStatus || "neutral"));
    row.append(
      id,
      route,
      textCell(job.kind || "delegation"),
      textCell(job.permissions || "inherited"),
      state,
      reply,
      textCell(formatTime(job.updatedAt)),
    );
    body.append(row);
  }
}

const projectFilter = element("project-filter");
if (projectFilter) {
  projectFilter.addEventListener("change", () => {
    selectedProjectKey = projectFilter.value;
    if (latestTopologySnapshot) drawTopology(latestTopologySnapshot);
  });
}

function setConnection(ok, message) {
  setText("refresh-state", message);
  const dot = element("connection-dot");
  if (dot) dot.className = `dot ${ok ? "online" : "offline"}`;
}

async function refresh() {
  try {
    const response = await fetch("/api/snapshot", { cache: "no-store" });
    if (!response.ok) throw new Error(`snapshot unavailable (${response.status})`);
    const snapshot = await response.json();
    if (document.body.dataset.view === "dashboard") renderDashboard(snapshot);
    else renderOrchestration(snapshot);
    setConnection(true, `Updated ${formatTime(snapshot.generatedAt)}`);
  } catch (error) {
    setConnection(false, error.message);
  }
}

refresh();
setInterval(() => {
  if (!document.hidden) refresh();
}, POLL_INTERVAL_MS);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) refresh();
});
