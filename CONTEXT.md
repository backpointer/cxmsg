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
- **Team Recipient Trigger**: one exact Trigger persisted on one prepared Team
  recipient Delivery. It may pin that recipient's current turn or one existing
  Job, controls timing only, and cannot be rebound or treated as authority.
- **Delivery**: one durable recipient-specific record for a Logical Message.
  It exists before any transport attempt and records only provable scheduling,
  transport, turn, reply, failure, expiry, cancellation, or unknown evidence.
  A transport attempt is an event within a Delivery, not the Delivery itself.
- **Logical Message**: one user-visible message in a Conversation. A Logical
  Message can produce one Delivery for a direct recipient or multiple
  recipient-specific Deliveries for a group.
- **Message Body**: optional retained Peer Message content associated with one
  Logical Message. Its retention is independent from routing and Delivery
  evidence.
- **Content Reference**: an opaque local identifier plus byte count and digest
  used to retrieve and verify a retained Message Body. It is neither a path nor
  authority to execute work.
- **Reply Handle**: a short opaque identifier assigned to one admitted Logical
  Message within its recipient Node. It resolves only for that pinned recipient
  thread and maps to the Delivery Ledger's reverse route. It is neither global
  identity nor authority.
- **Route Admission**: the pre-injection decision that compares a typed Peer
  Message route with the target Node's externally owned Project and role
  binding. It is separate from Delivery evidence.
- **Inbound Peer Message Policy**: an owner-configured, recipient-Node-scoped
  deny-only policy evaluated before ordinary Peer Message context injection.
  It matches only verified stable sender Node or Project identity, or an
  explicit unknown-sender rule, and grants no authority.
- **Inbound Denial**: terminal metadata-only evidence that an ordinary Peer
  Message was rejected by Inbound Peer Message Policy before transport. It is
  distinct from Quarantine. Initial denial retains no Message Body; a later
  Scheduled or Explicit Retry denial never deletes a body retained by the
  earlier admission. It creates no retry, replay, reroute, wake, grant,
  approval, or task authority.
- **Quarantine**: owner-private durable storage for a Peer Message rejected by
  Route Admission. Quarantine creates no context injection, retry, reroute,
  wake, grant, approval, or task authority.
- **Delivery Ledger**: the durable evidence history for Logical Messages and
  their recipient-specific Deliveries. It records only states that cxmsg can
  prove.
- **Negative Acceptance**: version-pinned App Server evidence proving that one
  attempted input was rejected before any input-queue mutation. A timeout,
  disconnect, incomplete history search, or unrecognized server version is not
  Negative Acceptance.
- **Explicit Retry**: the one operator-requested second transport attempt
  allowed only after Negative Acceptance. It reuses the Logical Message ID,
  retained Message Body, recipient, and App Server client message ID. It is
  never automatic and never applies to `unknown` evidence.
- **Delivery Dedup Tombstone**: owner-private immutable evidence that a
  terminal Logical Message was explicitly selected for Retention purge. It
  permanently reserves that Logical Message ID and its immutable fingerprint
  so no retry, reply, or direct Ledger caller can wake it again. It contains no
  Message Body or Endpoint and is distinct from a Node Directory Tombstone.
- **Conversation**: a durable ordered context for Logical Messages and replies.
  A Conversation is either Direct or Group and never conveys user authority.
- **Direct Conversation**: the canonical owner-private Conversation for an
unordered pair of stable Node identities. It orders retained metadata and
replies but never injects history or conveys authority.
- **Recent Conversation Projection**: a bounded per-Node view of current
Conversation membership and durable Logical Message activity. It is discovery
metadata only; it never chooses a peer, follows a successor, or authorizes a
send.
- **Group Conversation**: a same-Project Conversation with explicitly versioned
  membership and a recipient set frozen for each Logical Message.
- **Store-only Delivery**: a recipient-specific Group Delivery retained for an
  explicit bounded inbox. It starts no model turn and acquires no Scheduler
  claim merely because it exists.
- **Inbox Cursor**: owner-private presentation state recording acknowledged
  Group sequences. It is not Delivery, read, processing, or completion
  evidence.
- **Inbox Digest Intent**: an owner-private, one-shot request to compose a
  bounded set of oldest unread Group messages only when cxmsg next starts a
  Codex Peer Message turn. It creates no turn and conveys no authority.
- **Inbox Digest**: the bounded, clearly untrusted model-context projection
  produced from an Inbox Digest Intent. It is never steered into Busy work, and
  its cursor advances only after App Server accepts the new turn.
- **Team Cast Plan**: an owner-private, short-lived resolution of one explicit
  Conversation, Cluster, or Project+role selector to a frozen same-Project
  recipient Node set. A Plan records selector evidence and a token-cost ceiling
  but starts no Delivery and conveys no authority.
- **Team Cast Mention Selection**: a bounded immutable subset of an unexpired
  Team Cast Plan chosen by exact stable Node keys. It estimates possible wake
  turns but is not itself a Delivery or wake operation.
- **Team Cast Wake-All Selection**: an explicit immutable copy of every frozen
  recipient in an unexpired Team Cast Plan. It is bounded to 64 Nodes and
  exposes the maximum model-turn count before any body or Delivery is created.
  It is not an implicit broadcast, authority, or Delivery operation.
- **Prepared Team Cast Delivery**: a recipient-specific entry committed with
  every sibling recipient in one Ledger batch after body persistence but before
  transport. Prepared state has no attempt, claim, receipt, or authority.
- **Scheduled Team Cast Fallback**: an explicit Codex recipient transition from
  `mention-wake`/`prepared` to `when-idle`/`scheduled`. It uses the shared
  Delivery Ledger and Scheduler claim protocol, never a second Team queue, and
  starts no attempt while the recipient is Busy.
- **Team Cast Recipient Evidence**: one recipient's durable post-attempt state,
  independently `turn_started`, `transport_delivered`, `failed`, or `unknown`.
  `turn_started` is Codex App Server acceptance; `transport_delivered` is
  Claude frame acceptance with a correlated Delivery Job. Neither is proof of
  model processing, reply, or task completion.

## Identity and topology

- **Node**: a registered, addressable Codex thread or Claude session. Its
  stable key is the composite `(runtime kind, native ID)`. Display names,
  process IDs, sockets, and terminal attachments are not Node identity.
- **Execution Thread**: a thread created to execute a Job, such as a
  Delegation fork or an explicitly fresh isolated execution. It is not a Node.
  Its provenance retains the source thread even when source history is not
  copied. Any future conversion to an addressable
  Node requires a separate explicit promotion lifecycle; ordinary registration
  or Directory synchronization is not promotion.
- **Endpoint**: volatile evidence describing how a Node can currently be
  reached or presented, such as a UDS address, process, bridge, or TUI
  attachment. Bounded Endpoint history records selection and rejection
  evidence but does not become Node identity.
- **Session Alias Consolidation**: an explicit registry operation that removes
  one duplicate display alias for the same Codex thread while preserving the
  canonical registration and, when unambiguous, moving only its TUI attachment
  metadata. It never deletes or Tombstones the stable Node and never transfers
  a grant, route role, bridge, pending Job, Conversation, or authority.
- **Project**: a stable private identity grouping one or more working roots.
  A path helps discover Project membership but is not Project identity.
- **Project Transition**: an explicit owner-private append-only move record
  connecting one Project discovery identity to the next while retaining the
  same stable Project ID. Worktree aliases are not transitions; merge and split
  are unsupported in the v1 lifecycle contract.
- **Successor Relation**: explicit continuity context between two stable Nodes.
  It transfers no role, grant, permission, membership, Delivery, Job,
  correlation, Endpoint, or authority.
- **Cluster**: an explicit logical grouping of Nodes independent of Project and
  Conversation membership.
- **Tombstone**: the retained identity metadata for a removed Node, Project,
  Cluster, or Conversation that is still referenced by history.
- **Topology Edge**: one typed relationship in the coordination graph. Project
  membership, Cluster membership, Conversation membership, reachability,
  communication history, and Delegation authority are separate edge kinds.

## Work and authority

- **Delegation**: a user-authorized, correlated job bounded by a stored grant,
  permission profile, execution mode, and approval policy. A large task may be
  held by a Job-bound owner-private Message Body reference; the reference does
  not change its authority.
- **Scheduled Delegation**: a durable Delegation Job held by the Scheduler until
  its pinned target is Idle. Its timing policy is not approval; cxmsg validates
  the grant, permission profile, approval mode, Node, and Project again before
  worker activation.
- **Job**: a durable correlation record for Delegation or cross-runtime
  delivery. A Job has one stable ID and an explicit terminal state.
- **Busy**: a Codex thread has an in-progress turn. A bridge or worker process
  being alive does not by itself make the thread Busy.
- **Idle**: a Codex thread has no in-progress turn and can accept direct input.

## Lifecycle and diagnosis

- **Turn Lifecycle Module**: the Module that observes App Server turn state,
  persists a metadata-only local observation sequence and connection epoch,
  reconciles missed events from one bounded recent-turn page, and exposes a
  stable Busy/Idle view. App Server notifications are ephemeral wake hints,
  not replayable evidence.
- **Scheduler Module**: the Module that durably stores Triggered work, claims it
  once, renews a still-owned lease before dispatch, and dispatches it under
  strict per-target FIFO ordering rules. Its desired-state record distinguishes
  an operator stop from a missing intended worker.
- **Message Body Store Module**: the owner-private Module that appends bounded
  Message Bodies, verifies their digest, quarantines partial segments, and
  exposes bounded range reads through opaque Content References.
- **Retention Module**: the Module that classifies owner-private Delivery
  metadata, Message Bodies, and Route Admission Quarantine as age-retained,
  evidence-protected, or purge-eligible. Its default Interface is read-only;
  any purge uses a separate explicit mutation with durable dedup Tombstones,
  recoverable backups, and an audit receipt.
- **Retention Mutation Barrier Module**: the Module that admits concurrent
  ordinary state writers through owner-private leases and grants one exclusive
  Retention mutation only after those writers drain. It is always acquired
  before Route, Job, Message Body, or Delivery Ledger locks and creates no
  deletion or task authority by itself.
- **Retention Transaction Module**: the Module that revalidates one exact
  Retention plan, stages private content generations, writes Delivery Dedup
  Tombstones, swaps active generations with retained backups, rebuilds derived
  indexes, and emits an audit receipt. After Tombstone creation it recovers by
  roll-forward; restore requires the current transaction head and unchanged
  active generation and never removes Tombstones.
- **Peer Message Context Projection Module**: the Module that converts an
  admitted Peer Message into the minimal model-visible untrusted marker,
  display alias, Message Body or preview, and Reply Handle. Routing, scheduling,
  identity, and Delivery evidence remain outside model context.
- **Cross-runtime Reply Adapter**: the Adapter that resolves a recipient-scoped
  Reply Handle to a stable Node of another runtime, revalidates that Node's
  current Endpoint, and creates correlated Delivery evidence without exposing
  the Endpoint in model context. It never turns an ACK into task completion.
- **Claude Native Receipt**: a bounded `peer_message_status` control receipt
  correlated by the exact outbound transport message ID. `held`, `denied`,
  `expired`, and `delivered` describe Claude's native message transport only;
  they are owner-local but unauthenticated advisory evidence, not a model ACK,
  reply, completion, approval, or authority. No lifecycle transition, retry,
  wake, routing, or permission decision may depend on this evidence.
- **Structured Reply Evidence**: an ordinary untrusted Claude Peer Message
  whose exact envelope `in-reply-to` ID and source both match one Claude
  Delivery Job. It proves correlation only and never changes the Job's ACK or
  completion state. Correlation text copied into the Message Body is ignored.
- **Node Directory Module**: the Module that owns Node, Endpoint, Project,
  Cluster, alias, successor, and Tombstone identity rules.
- **Conversation Module**: the Module that owns Direct and Group Conversation
  membership, Logical Message ordering, replies, and fan-out policy.
- **Graph Projection Module**: the read-only Module that derives filtered
  topology views from the Node Directory, Delivery Ledger, Conversations,
  Jobs, and grants. It is never a source of identity or authority.
- **Graph Detail Projection**: a bounded read-only view of one Node,
  Conversation, or Logical Message Delivery derived from the same owner
  Modules. It exposes correlation metadata, never Message Body or capability
  material, and cannot mutate or dispatch.
- **Local Graph Observer**: the owner-local observation point used as the
  source of `reachable-with` Edges. It prevents Endpoint evidence from being
  misrepresented as proof that one peer can reach another peer.
- **Doctor Module**: the read-only diagnostic Module that collects evidence
  through existing runtime Interfaces and reports health without changing
  local state or starting model work.
- **Conversation Consistency Inspector**: the Doctor component that compares
  Direct and Group metadata with Directory, Job, Delivery Ledger, Team Cast
  plan, selection, and per-recipient evidence. It reports mismatches but never
  replays, refans out, migrates, or repairs them.
- **Repair Plan**: a deterministic, read-only description of one currently
  allowlisted Doctor finding, its exact evidence digest, mutation category,
  and recoverability contract. It is not authority until the owner explicitly
  supplies the same digest to Repair Apply.
- **Repair Transaction Module**: the separately invoked mutation Module that
  revalidates one Repair Plan under a private Repair lease, writes a bounded
  backup and phase journal, calls one existing owner mutation, verifies the
  exact finding, and emits an audit receipt. It supports only Cluster head redo
  and rebuildable Delivery Ledger index repair.
- **Repair Retention Plan**: a deterministic, read-only projection of
  Doctor-consistent completed Repair transaction/receipt pairs older than a
  fixed cutoff. It carries no mutation authority; failed and incomplete Repair
  attempts remain blocked evidence.
- **Repair Archive Transaction**: an explicitly digest-confirmed, journaled
  move of completed Repair pairs out of the active Repair quota and into a
  bounded owner-private recoverable store. Recovery rolls interrupted moves
  forward; exact-ID restore returns the same evidence only when identities are
  empty and the active quota permits it.
- **Repair**: an explicit, separately authorized mutation associated with one
  Doctor finding. Repair is not part of the default Doctor Interface.
- **Reconciliation**: comparing durable records with current App Server,
  process, socket, and worker evidence after a restart or missed event.
