# Domain language

This file defines the project terms used in architecture and improvement
documents. Code and documentation should preserve these distinctions because
they carry different authority and delivery semantics.

## Messaging

- **Peer Message**: untrusted coordination text delivered to a Codex or Claude
  peer. It carries information, never user authority.
- **Steering**: adding a Peer Message to a specific in-progress Codex turn.
  Steering does not create a new turn or expand that turn's permissions.
- **Scheduled Peer Message**: a durable Peer Message held until an explicit
  Trigger becomes eligible and the target can accept it.
- **Trigger**: the condition that makes a Scheduled Peer Message eligible.
  Initial trigger kinds are `when-idle`, `after-turn`, and `after-job`.
- **Delivery**: one durable recipient-specific record for a Logical Message.
  It exists before any transport attempt and records only provable scheduling,
  transport, turn, reply, failure, expiry, cancellation, or unknown evidence.
  A transport attempt is an event within a Delivery, not the Delivery itself.
- **Logical Message**: one user-visible message in a Conversation. A Logical
  Message can produce one Delivery for a direct recipient or multiple
  recipient-specific Deliveries for a group.
- **Delivery Ledger**: the durable evidence history for Logical Messages and
  their recipient-specific Deliveries. It records only states that cxmsg can
  prove.
- **Conversation**: a durable ordered context for Logical Messages and replies.
  A Conversation is either Direct or Group and never conveys user authority.
- **Direct Conversation**: the canonical Conversation between exactly two
  Nodes.
- **Group Conversation**: a Conversation with explicitly versioned membership
  and recipient-specific fan-out.

## Identity and topology

- **Node**: a registered, addressable Codex thread or Claude session. Its
  stable key is the composite `(runtime kind, native ID)`. Display names,
  process IDs, sockets, and terminal attachments are not Node identity.
- **Execution Thread**: a thread created to execute a Job, such as a
  Delegation fork. It is not a Node unless explicitly registered as one.
- **Endpoint**: volatile evidence describing how a Node can currently be
  reached or presented, such as a UDS address, process, bridge, or TUI
  attachment.
- **Project**: a stable private identity grouping one or more working roots.
  A path helps discover Project membership but is not Project identity.
- **Cluster**: an explicit logical grouping of Nodes independent of Project and
  Conversation membership.
- **Tombstone**: the retained identity metadata for a removed Node, Project,
  Cluster, or Conversation that is still referenced by history.
- **Topology Edge**: one typed relationship in the coordination graph. Project
  membership, Cluster membership, Conversation membership, reachability,
  communication history, and Delegation authority are separate edge kinds.

## Work and authority

- **Delegation**: a user-authorized, correlated job bounded by a stored grant,
  permission profile, execution mode, and approval policy.
- **Job**: a durable correlation record for Delegation or cross-runtime
  delivery. A Job has one stable ID and an explicit terminal state.
- **Busy**: a Codex thread has an in-progress turn. A bridge or worker process
  being alive does not by itself make the thread Busy.
- **Idle**: a Codex thread has no in-progress turn and can accept direct input.

## Lifecycle and diagnosis

- **Turn Lifecycle Module**: the Module that observes App Server turn state,
  reconciles missed events, and exposes a stable Busy/Idle view.
- **Scheduler Module**: the Module that durably stores Triggered work, claims it
  once, and dispatches it under per-target ordering rules.
- **Node Directory Module**: the Module that owns Node, Endpoint, Project,
  Cluster, alias, successor, and Tombstone identity rules.
- **Conversation Module**: the Module that owns Direct and Group Conversation
  membership, Logical Message ordering, replies, and fan-out policy.
- **Graph Projection Module**: the read-only Module that derives filtered
  topology views from the Node Directory, Delivery Ledger, Conversations,
  Jobs, and grants. It is never a source of identity or authority.
- **Doctor Module**: the read-only diagnostic Module that collects evidence
  through existing runtime Interfaces and reports health without changing
  local state or starting model work.
- **Repair**: an explicit, separately authorized mutation associated with one
  Doctor finding. Repair is not part of the default Doctor Interface.
- **Reconciliation**: comparing durable records with current App Server,
  process, socket, and worker evidence after a restart or missed event.
