# Coordination graph and conversation plan

- Status: proposal
- Target release: to be determined
- Last updated: 2026-08-14

## Summary

cxmsg should let every registered session answer four questions without
guessing from process IDs or display names:

1. Who am I?
2. Which Project and logical Clusters contain me?
3. Which Nodes can I currently reach, and which Nodes have I communicated
   with?
4. Which Direct or Group Conversations contain my messages and replies?

This requires stable Node and Project identity, a Delivery Ledger for every
Peer Message, explicit Conversation membership, and a Graph Projection Module.
The graph remains a read-only view. It does not own identity, history, or
authority.

This plan extends the decisions in
[Busy delivery, scheduling, and doctor improvement plan](BUSY_SCHEDULING_DOCTOR.md).
The terms in [CONTEXT.md](../CONTEXT.md) are normative.

## Decision summary

- Every ordinary Peer Message receives a durable Logical Message record and at
  least one recipient-specific Delivery record before dispatch.
- Node identity is the composite `(runtime kind, native ID)` and is independent
  of display name, PID, UDS address, bridge, TUI attachment, and directory path.
- Codex Execution Threads are not Nodes unless explicitly registered.
- A removed identity becomes a Tombstone while referenced by retained history.
- Project identity is private and stable across Git worktrees; path is discovery
  evidence, not identity.
- Cluster, Conversation, Project, reachability, communication history, and
  Delegation authority remain different Topology Edge kinds.
- Conversation membership carries no user authority.
- Delivery states describe evidence, not inferred human or model behavior.
- Scheduler and Delivery Ledger reuse one durable claim and reconciliation
  Implementation. They must not maintain competing delivery truth.
- Claim ownership is represented by lease fields, never as Delivery evidence.
- Ledger appends use bounded deterministic hash shards, not one global lock.
- Group delivery defaults to store-only inbox presentation. Mention wake,
  wake-all, and digest composition are explicit.
- Graph Projection and Doctor are read-only consumers.

## Non-goals

- Treating a group as a Delegation grant or approval source.
- Inferring that a model read, understood, or processed a Peer Message.
- Automatically linking unrelated Claude sessions because their names or paths
  look similar.
- Turning every Delegation fork into a visible collaboration Node.
- Cascading identity deletion into historical message deletion.
- Automatically forwarding a received group message to another group.
- Making the local coordination graph a multi-host identity provider.
- Exposing private paths, message bodies, tokens, results, or approval text in
  web snapshots or ordinary logs.

## Current limitations

The current registry maps a Codex display name directly to a thread ID and cwd.
Claude discovery exposes a live session ID plus a volatile PID-based socket.
The web topology then:

- identifies Codex nodes by display name;
- groups nodes by the literal cwd string;
- derives communication edges only from Jobs;
- loses ordinary Codex Peer Message history;
- cannot distinguish configured relationships from observed communication;
- cannot retain a removed peer as historical context.

This is sufficient for a current operational snapshot but not for durable
Direct or Group Conversations.

## Relationship model

The graph has typed edges. Renderers may combine selected types visually, but
storage and policy never collapse them.

| Edge kind | Source of truth | Meaning |
| --- | --- | --- |
| `belongs-to-project` | Node Directory | Node working context belongs to Project |
| `member-of-cluster` | Node Directory | Explicit logical grouping |
| `member-of-conversation` | Conversation | Direct or versioned Group membership |
| `reachable-with` | Endpoint evidence | Current caller can attempt transport |
| `communicated-with` | Delivery Ledger | Retained message history exists |
| `delegated-to` | grants and Jobs | User-configured authority relationship |
| `successor-of` | explicit Node Directory record | New Node continues selected identity history |
| `forked-from` | App Server provenance | Execution Thread ancestry, not membership |

`reachable-with` is directional and time-sensitive. A valid registry record
does not imply reachability. Examples include sandbox-denied UDS, a standalone
TUI holding the rollout writer, a stale socket, and a thread whose metadata
cannot be read within bounded limits.

## Node Directory Module

### Node identity

The stable Node key is the composite `(runtime kind, native ID)`. Runtime kind
is part of the key even when both runtimes currently use UUID-shaped IDs; a
Codex thread and Claude session with the same native string are different
Nodes. Initial native IDs are:

- Codex: the registered App Server thread ID;
- Claude: the native Claude session ID.

Display names are mutable aliases. PID, socket path, bridge start nonce, and
TUI attachment belong to Endpoint evidence. A Node may have several historical
Endpoints, but at most one currently selected Endpoint per transport kind.

Endpoint selection is an identity-checked transition, not last-writer-wins. A
candidate must match the composite Node key and present a monotonic generation
or bridge start nonce. It may replace a reachable selected Endpoint only when
it proves the same owner generation or an explicit detach/adopt transition.
Older, ambiguous, and mismatched candidates remain historical evidence and
cannot overwrite the selected Endpoint. Strengthening bridge registry
ownership and compare-and-swap selection is a Phase 3 entry gate.

A Claude restart or new conversation can create a new session ID. cxmsg does
not automatically merge it into an older Node. An explicit `successor-of` edge
may connect the Nodes while retaining both histories and the time of the user
decision.

### Execution Threads

Delegation forks and other Job-specific App Server threads are Execution
Threads. They carry Job provenance but do not appear as addressable Nodes or
Conversation members. Any future conversion requires a separate explicit,
audited promotion lifecycle; ordinary registration and Directory
synchronization refuse a classified Execution Thread. This prevents the Node
Directory and communication graph from fragmenting on every fork.

`forked-from` remains available as provenance for Job inspection and Doctor.

### Removal and Tombstones

Removing a registered Node prevents future addressing but does not cascade into
the Delivery Ledger. A Tombstone retains only the stable ID, last safe display
label, runtime kind, removal time, and redacted Project reference needed to
render history.

Tombstones contain no socket, token, message body, permission profile, or live
process claim. Retention cleanup may remove a Tombstone only after no retained
record references it.

### Endpoint lifecycle

Endpoint state is evidence-based:

- `reachable`: the current caller completed the required identity handshake;
- `external-writer`: a standalone TUI owns the Codex rollout and cxmsg cannot
  safely attach or resume it;
- `unreachable`: credible identity exists but the caller is denied or timed
  out;
- `stale`: identity evidence proves that a saved Endpoint no longer has its
  expected listener or process;
- `unknown`: evidence is insufficient or contradictory;
- `mismatched`: identity verification failed.

Only `reachable` permits ordinary dispatch. Lifecycle mutation remains stricter
and follows existing safe-to-signal and safe-to-remove rules.

## Project identity

Project uses a private stable ID. The Node Directory discovers or confirms
membership using:

1. an explicit local Project declaration when present;
2. the canonical Git common directory for a repository and all of its
   worktrees;
3. a canonical non-Git root path as a fallback discovery key.

The discovery key is not the durable Project ID. This prevents a path rename or
worktree checkout from rewriting historical identity. The private Project
record retains root aliases and the time each alias was observed.

Automatic merging is conservative. Equal directory basenames, Git remote URLs,
or display labels are not enough to merge Projects. A manual merge records the
old Project as a Tombstone and preserves history.

Full paths remain owner-private. A local CLI may show them explicitly; web
snapshots default to a safe label and redacted root identifier.

## Cluster model

A Cluster is a named many-to-many logical grouping, such as `stock-qa` or
`release-reviewers`. Cluster membership is explicit and versioned.

- A Node can belong to multiple Clusters.
- A Cluster can span Projects.
- Cluster membership does not create a Conversation automatically.
- Cluster membership does not create reachability or authority.
- Removing a Cluster leaves a Tombstone while retained graph history refers to
  it.

Keeping Cluster and Conversation separate allows a group to organize the graph
without causing fan-out or model wake-ups.

The Phase 3 Directory foundation now implements this boundary. A Cluster owns a
private UUID, an independent human routing label, a sorted bounded current member set,
and immutable membership snapshots. Only explicit operations change membership;
idempotent repetition does not create a new version. New membership requires a
live Node, while retained snapshots may reference a Node Tombstone. A reduced
Cluster Tombstone preserves identity and the last membership version without
copying members. Doctor validates lifecycle conflicts, missing or orphaned
versions, exact one-member transitions, and Node references without repairing
records. Conversation fan-out and graph projection remain later modules.

The Delivery Ledger does not address a Cluster as a dynamic recipient set.
Conversation creation or membership change first pins an explicit Cluster
`membershipVersion`; recipient Deliveries then reference that immutable set.
This keeps Ledger dispatch dependent on Conversation evidence rather than on a
mutable Directory query.

## Delivery Ledger

### One truth for scheduling and delivery

The Delivery Ledger and Scheduler share durable record, claim, lease,
idempotency, expiry, and reconciliation rules. Scheduler Trigger state extends
the same Delivery record used for immediate dispatch. It is not copied into a
second status file.

The shared Implementation commits a Logical Message and all intended recipient
Deliveries before any transport attempt. A journal may represent this as one
batch entry or as begin/commit markers, but an incomplete batch is never
claimable. After commit, each recipient Delivery is independently claimable.
After restart, reconciliation uses the same message and Delivery IDs.

### Evidence-based states

Permitted Delivery states are:

```text
created
  -> scheduled
  -> transport_delivered
  -> turn_started
  -> replied
  -> failed
  -> expired
  -> cancelled
  -> unknown
```

Not every Delivery passes through every state. A store-only group Delivery can
remain `scheduled` until explicit dispatch or expiry; inbox presentation moves
only a separate cursor and does not invent Delivery evidence.

Claim is not evidence and is not a Delivery state. `claimOwner`, `claimNonce`,
and `leaseUntil` are exclusive-dispatch fields that may be set only while a
Delivery is dispatchable. An immediate send normally moves directly from
`created` to the strongest proven evidence state; it does not become
`scheduled` or acquire a claim unless dispatch is deferred.

The Ledger does not invent `received`, `read`, or `processed` states. Socket
acceptance proves only `transport_delivered`; App Server acceptance proves
`turn_started`; a correlated response proves `replied`. An ambiguous connection
loss produces `unknown`, not a retry that may duplicate work.

### Immediate steering race

An immediate Peer Message is recorded before cxmsg observes Busy or Idle state.
If `turn/steer` returns a deterministic expected-turn mismatch, cxmsg can
reconcile once. It may dispatch to the newly Idle thread using the same message
ID only when the pinned Codex version has a regression test proving that this
exact mismatch means the rejected steer performed no mutation. Without that
proof, the Delivery becomes `unknown` with bounded
`unverified_expected_turn_mismatch` evidence and is not replayed. On a verified
version, if bounded re-observation is Busy on a different turn, cxmsg does not
steer a second turn; the Delivery fails with bounded `turn_changed` evidence
and requires an explicit resend. If the transport closes before the result is
known, the Delivery becomes `unknown` and is not replayed automatically.

`clientUserMessageId` carries the Logical Message ID. Cross-method App Server
deduplication remains version-tested defense in depth, not the sole duplicate
guard.

### Storage and privacy

The proposed zero-dependency file Adapter uses append-only, bounded JSONL
segments plus a rebuildable index. Requirements:

- `schemaVersion` from the first record;
- owner-only directories and mode-`0600` files;
- a fixed, versioned hash-shard count keyed by Conversation ID, or by a stable
  direct-route key before a Conversation exists;
- one bounded append lock per active shard segment, with acquisition timeout
  and OS-released ownership on process exit;
- crash detection for partial final lines: quarantine the damaged segment,
  preserve complete preceding records, and open a new segment without
  truncating or silently repairing the old one;
- index rebuild from segments without message loss;
- bounded segment size and total storage quota;
- explicit retention and purge policy;
- no symlink following;
- stable hashes or shortened IDs in diagnostic output.

An append lock is held only for the bounded journal batch, never across
transport, model work, or a claim lease. Per-target dispatch-lane claims remain
independent of storage shards, so one crashed or slow sender cannot serialize
all cxmsg delivery.

One file per message is rejected because normal chat volume can cause inode and
directory-scan growth. A future database Adapter is possible only after a real
second runtime requirement exists; the file Adapter and an in-memory test
Adapter establish the initial Seam.

Message bodies are optional retained content associated with the Logical
Message. Metadata retention and body retention are separate policies. Purging a
body preserves redacted routing and delivery evidence when policy allows.

## Conversation Module

### Direct Conversation

One canonical Direct Conversation exists for an unordered pair of Node IDs.
Renaming a Node or changing its Endpoint does not create another Direct
Conversation. A successor Node starts a new Direct Conversation unless the user
explicitly links or migrates membership.

Replies reference the parent Logical Message ID, not a recipient Delivery ID.
This keeps threading stable when the original message had multiple transport
attempts. A successor Node may reply in the predecessor's Conversation only
after an explicit membership migration records both the `successor-of` relation
and Conversation continuation. Otherwise its reply starts or uses the new
Direct Conversation while retaining the parent Message as cross-Conversation
provenance.

### Group Conversation

A Group Conversation has a stable ID, mutable display label, and versioned
membership. Each Logical Message records the membership version and explicit
recipient set used for that send. Later membership changes do not rewrite prior
fan-out.

Crash-consistent fan-out order is:

1. acquire the one bounded lock for the Conversation's hash shard;
2. append the Logical Message and all intended recipient Delivery records as
   one batch entry, or with begin/commit markers under that same lock;
3. release the lock only after the committed batch is claimable;
4. dispatch and reconcile each Delivery independently.

A partially written batch is never claimable. Recovery either completes a
provably intact batch or marks it failed without dispatch.

Recipient failure never becomes silent success. The aggregate view reports
counts such as `3 replied, 1 transport_delivered, 1 failed` while preserving
each Delivery state.

### Wake and token policy

Group fan-out defaults to `store-only`. It does not start model turns merely
because a Node is a member.

Explicit policies are:

- `store-only`: retain the Delivery for explicit inbox or local presentation;
  it never injects text into a model turn by itself;
- `mention-wake`: wake only explicitly mentioned Nodes;
- `wake-all`: wake every reachable member after an explicit caller choice;
- scheduled variants: apply Trigger and expiry per recipient.

Unread is a presentation cursor, not proof that a model has read content. A
digest has a message-count and byte limit. Excess undisplayed content remains
in history and is not injected into one turn without a bounded selection.

Phase 6 first exposes `cxmsg inbox` as the store-only consumption Interface.
The Turn Lifecycle Module may observe activation for presentation, but an
externally started or already Busy turn is never steered with a digest. A later
explicit `digest-on-next-cxmsg-turn` policy may compose a clearly labelled
untrusted Peer Message digest only when cxmsg itself is creating the new turn;
the cursor advances only after App Server accepts that composition. This path
requires separate tests and cannot inherit Delegation authority.

For `wake-all`, each recipient has an independent wake outcome. An unreachable
recipient defaults to store-only fallback: its Delivery remains `scheduled`,
the failed wake attempt is retained as bounded evidence, and other recipients
continue. The aggregate result reports the fallback and never labels the whole
fan-out successful. A future fail-all policy requires an explicit caller
choice; it is not the default.

The recipient set is fixed at send time. Removing a member later does not erase
or reassign an already committed store-only Delivery; retention and explicit
inbox policy govern whether that former member can still retrieve it.

### Loop prevention

- Conversation-level Logical Message IDs are deduplicated.
- A bridge never automatically re-fans-out a received group message.
- Forwarding creates a new Logical Message with an explicit parent and bounded
  hop budget.
- An automatic status or failure notice cannot itself trigger another automatic
  status notice.
- Each recipient receives at most one automatic wake per Logical Message unless
  an explicit retry is authorized by its evidence state.

### Authority

Conversation and Cluster membership carry information only. A group cannot
grant, delegate, approve, change permission profiles, or select automatic
approval. Delegation remains a separate Job with a recipient-specific grant,
permission profile, execution mode, and approval policy.

Group Delegation, if ever added, expands into individually authorized Jobs. One
member's grant cannot authorize another member.

## Graph Projection Module

Graph Projection derives views from Node Directory, Endpoint evidence,
Conversations, Delivery Ledger, Jobs, and grants. It owns no writable graph
record.

Views filter by:

- Project and Cluster;
- Direct or Group Conversation;
- Topology Edge kind;
- current, one-hour, 24-hour, or retained-history time window;
- runtime kind and evidence state.

Project containers, logical Cluster overlays, and cross-Project edges are
rendered separately. The default view does not draw every retained edge. A Node
detail view can show its Project label, private path when explicitly requested,
Clusters, Conversations, current Endpoints, recent contacts, and redacted
Delivery counts.

The existing web view remains diagnostic and read-only. Sending, joining,
granting, and lifecycle mutation remain outside Graph Projection.

## Doctor integration

Doctor adds immutable checks for:

- duplicate or invalid Node IDs and aliases;
- volatile Endpoint fields incorrectly used as identity;
- Node-to-Project discovery contradictions;
- Git worktrees split across unintended Projects;
- orphaned Execution Threads incorrectly registered as Nodes;
- Tombstones still referenced after premature cleanup;
- malformed or partial Ledger segments;
- index rebuild consistency;
- validated Claude reply wake evidence whose original Delivery remains
  `ack_timeout`;
- retention and quota violations;
- incomplete or claimable partial fan-out batches;
- invalid membership versions and duplicate recipients;
- unverifiable states such as `read` or `processed` appearing in records;
- Graph Projection counts differing from source records.

Doctor never dispatches store-only messages, advances a cursor, repairs a
segment, merges Projects, links successor Nodes, or purges history.

## Reliability prerequisites

Before Directory and Conversation work begins, current transport blockers must
be removed:

1. replace full `thread/read(includeTurns:true)` activity reads with summary
   reads and bounded `thread/turns/list` queries;
2. record structured ingress and delivery logs with timestamp, correlation ID,
   target, attempt, phase, and redacted outcome;
3. separate empty health/probe connections from invalid peer frames;
4. instrument ACK ingress, parse, source validation, persistence, timeout, and
   late reconciliation;
5. route every validated Claude terminal ACK through one Reconciliation
   Interface that persists the original Delivery before emitting any Codex wake
   or reply copy;
6. reconcile stale Jobs such as `running` Delegations without a worker PID;
7. distinguish bridge handler activity from Codex thread Busy state.

Raising the WebSocket frame limit is not a substitute for bounded reads.

## Implementation sequence

### Phase 0: reliability prerequisites

- add a bounded thread activity Adapter;
- remove full-history reads from send, status, bridge wake, Job refresh, and
  mirror preflight;
- add structured delivery evidence and ACK phase logging;
- make terminal ACK persistence precede reply wake emission and deduplicate both
  with one receipt ID;
- reconcile stale Job and status metadata.

Initial implementation is complete against `codex-cli 0.147.0`. Automated
tests cover bounded metadata and turn-page calls, paginated Job result lookup,
late ACK evidence, redacted JSONL events, and missing-worker reconciliation. A
live large-thread status smoke test also completes without hydrating rollout
history. Compatibility remains pinned-version behavior and must be retested
when the experimental App Server protocol changes.

### Phase 1: immutable inspection and Doctor foundation

- split Inspector Interfaces from mutating refresh paths;
- ship read-only runtime, filesystem, Job, bridge, relay, and transport checks;
- establish the evidence vocabulary reused by Directory and Ledger.

Initial implementation is complete for the existing runtime state: bounded
Inspector Interfaces cover runtime, filesystem, Job, attachment, permission,
bridge, relay, App Server, and metadata-only registered-thread evidence.
`cxmsg doctor` exposes the stable redacted v1 report and exit codes without a
Repair path. Schedule, Directory, Ledger, Conversation, and projection checks
remain assigned to their later canonical phases.

### Phase 2: Turn Lifecycle and shared durable primitives

- surface App Server lifecycle notifications;
- add bounded reconciliation after missed events and restart;
- implement shared append, batch, claim, lease, expiry, and idempotency rules;
- verify crash recovery before adding ordinary message history.

The initial Message Body Store primitive is implemented as a bounded Phase 2
slice. It provides owner-only append segments, idempotency by Message ID and
digest, partial-segment quarantine, a fail-closed quota, and bounded UTF-8 range
reads. Codex Peer Messages over the inline limit use this primitive before
transport, but this does not claim that the Phase 4 Delivery Ledger, retention,
purge, index, claim, lease, or reconciliation work is complete.

The 64 MiB write quota and 256 MiB hard scan ceiling are separate. New writes
fail after the write quota, while retained bodies remain readable and
idempotent retries remain verifiable. One bounded retry tolerates an active
segment being atomically renamed into Quarantine during an unlocked read.

### Phase 2.5: Route Admission, Quarantine, and minimum deduplication

- parse the versioned routing envelope before model-context injection;
- compare `project_id` and `target_role` with an externally owned binding;
- keep `admissionState=pending|admitted|quarantined(reason)` separate from
  Delivery evidence;
- store rejected messages in owner-only Quarantine with no automatic wake,
  retry, reroute, or release;
- persist `logical_message_id` deduplication and permit at most one automatic
  wake before the full Delivery Ledger exists;
- evaluate immediate and scheduled Triggers only for admitted Deliveries.

This is a required deployment gate before the Hermes pilot and before Phases 3
and 4. It is not implemented by the Message Body Store slice. A Content
Reference proves body integrity only; it does not admit a route.

Implementation status: the immediate-only Phase 2.5 slice is now implemented.
It provides thread-pinned external route bindings, exact Project/role
admission, optional bound-sender role checking, pre-injection owner-only
Quarantine, redacted quarantine listing, and durable logical-message
idempotency with at most one automatic dispatch attempt. Unbound targets stay
legacy-compatible for migration. Scheduled admission, release, retention,
purge, and uncertain-attempt reconciliation remain in later phases; no
quarantined record is replayed automatically.

If `sender_role` is present, the sender must also have a binding pinned to its
current registered Codex thread; an unbound or stale sender is quarantined.
Admission and deduplication decisions emit only bounded redacted coordination
events. Ordinary Peer Messages are the scope of this gate. Delegation,
grant-validated Claude requests, and internal terminal-ACK wakes retain their
separate authorization and correlation paths.

Binding lookup is three-state. Only an absent path is `missing` and eligible
for migration compatibility. A present record that fails private metadata,
JSON, filename identity, Project key, Node key, or schema validation is
`invalid` and quarantined before transport; invalid state is never collapsed
into `legacy-unbound`.

### Phase 3: Node Directory and Project identity

- strengthen bridge registry ownership and identity-checked Endpoint selection
  before migrating live records;
- migrate registered Codex and live Claude identities into Nodes;
- classify existing forks as Execution Threads;
- map Git worktrees through their common directory;
- add Endpoint history, Tombstones, Clusters, and explicit successor edges.

Initial implementation status: explicit Project creation now assigns a private
random UUID to one exact canonical Git common directory or declared non-Git
root. Codex threads and Claude sessions synchronize as distinct composite Node
keys with mutable aliases and one generation-checked selected Endpoint per
transport. Default CLI and Doctor projections redact Project paths and Endpoint
addresses. Directory-aware Route Bindings pin the private Project UUID and
stable Codex Node key in addition to the routing label and thread ID.

The next lifecycle slice is also implemented. Explicit Node removal creates a
minimal Tombstone, automatic synchronization cannot resurrect that identity,
and an interrupted live-plus-Tombstone transition is left for read-only Doctor
inspection. Explicit same-Project `successor-of` records accept only one
predecessor per successor and reject cycles. A successor carries no automatic
role, grant, permission, Conversation, Delivery, or authority inheritance.

Execution Thread classification is now implemented for new fork Delegations
and their standalone start fallback. Classification and the Job's
`executionThreadId` link are persisted before delegated model input starts.
These records retain bounded source and Job provenance without becoming Nodes
or Conversation members. Historical classification is an explicit command and
requires retained fork, source/execution thread, and turn evidence; ambiguous
Jobs and arbitrary App Server threads are not inferred.

Bounded Endpoint history is now implemented inside owner-only Node records.
The current selected Endpoint remains a separate projection. Successful
selection, replacement, refresh, imported baseline, older rejection, and
equal-generation conflict decisions are retained without changing the existing
CAS result. Identical observations coalesce and bounded compaction preserves
the latest successful evidence for each selected transport. Default CLI and
Doctor output never render Endpoint addresses from history.

Phase 3 still has no automatic Project merge or move, successor inference, or
web projection. Cluster identity, immutable membership snapshots, Tombstones,
bounded recovery, and Doctor checks are implemented; Cluster membership still
creates no Conversation, fan-out, reachability, or authority. Execution Thread
classification remains separate from addressable Node synchronization. An
older Endpoint generation is rejected and an equal generation with conflicting
identity fails closed. The omitted lifecycle transitions require explicit
records and tests in the remaining Phase 3 work.

### Phase 4: Delivery Ledger and scheduling

- record every ordinary Peer Message before dispatch;
- unify immediate and Triggered Delivery evidence;
- add segmented storage, rebuildable index, retention, quota, and purge;
- migrate retained Jobs without inventing ordinary message history.

Initial implementation status: new ordinary Codex Peer Messages now commit one
Logical Message and one recipient Delivery atomically in an owner-only,
append-only segmented Ledger before transport. A separate immutable attempt
record precedes App Server access, and evidence records distinguish
`turn_started` from `unknown`; positive reconciliation may strengthen the
latter without replay. Long admitted bodies reference the Phase 2 Message Body
Store by digest and opaque Content Reference. Legacy `route-deliveries` remain
readable but are not written for new sends.

The initial slice supported immediate delivery only. The current Phase 4 slice
adds `when-idle` for one recipient while retaining one dispatch attempt and no
automatic retry, retention, or purge. A fail-closed 64 MiB metadata quota
reserves bounded space for the attempt and terminal evidence of every admitted
batch. Full rebuild remains bounded by a 256 MiB hard scan ceiling. A persistent
per-message index and digest-protected segment-manifest checkpoint now avoid a
full scan in each Scheduler poll; the Ledger remains truth and the cache
self-rebuilds when stale. Broader Trigger policies, retention policy, and Job
migration remain Phase 4 work.

The P0 diagnostic follow-up shares one 30-second grace between reconciliation
and Doctor. Doctor reports an aged attempt without evidence as a derived warn,
not as a Ledger transition, and the warning disappears after reconciliation
records `unknown` or `turn_started`. A Logical Message ID present in both the
Ledger and legacy storage is a required identity failure while runtime lookup
continues to prefer the Ledger without another dispatch. Complete invalid
records fail closed with bounded segment-and-line diagnostics; partial final
lines retain the existing uncommitted-tail quarantine rule. Metadata quota
usage is read from file sizes only, warns at 90 percent, and fails at 100
percent without automatic deletion or repair.

The first Scheduler slice is now implemented. A typed `when-idle` route stores
the full body by Content Reference, requires an explicit expiry within seven
days, and commits `scheduled` before returning. One managed worker uses FIFO
target lanes, a 256-record per-target bound, and immutable 30-second
claim/lease events. It observes Idle before and after claim acquisition,
releases an unused claim on a Busy race, records an attempt immediately before
`turn/start`, and never replays `unknown`. Expired claims are reclaimable after
worker or server restart. `cxmsg server start` owns Scheduler startup;
`cxmsg scheduler start|status|stop` exposes the lifecycle explicitly.

The operational-hardening slice adds a five-second worker heartbeat, a
15-second `stalled` threshold, redacted lifecycle audit events, explicit
`deliveries list|show|cancel|rebuild-index` commands, and read-only Doctor
checks for heartbeat and index consistency. Cancellation is terminal only
before an attempt and refuses an active lease. A live stalled worker is never
silently replaced, and the cache is capped at 4,096 Logical Messages until a
retention policy is selected.

The exact-Trigger slice adds `after-turn` and `after-job`. Both pin a UUID in
the immutable route, require that reference to exist before enqueue, retain the
body before scheduling, and recheck readiness after claim. A running Trigger
waits, a recognized terminal Trigger becomes eligible, and missing or
unverifiable evidence is observationally blocked without an attempt. Terminal
failed Jobs are eligible because this slice means "after any terminal Job",
not success-only. Bounded `status --json` turn IDs and read-only Doctor Job
reference checks expose no turn or message content. The current implementation
uses bounded reconciliation polling; durable App Server notification cursors
remain later Turn Lifecycle work.

This slice still has no scheduled Delegation, automatic retry, retention,
purge, or Conversation fan-out.
Those remain separate Phase 4/6 gates rather than inferred capabilities.

### Phase 5: Direct Conversation

- create canonical Node-pair Conversations;
- add Logical Message ordering and correlated replies;
- expose bounded local history without changing wake behavior.

### Phase 6: Group Conversation

- add versioned membership and crash-consistent fan-out;
- ship store-only with an explicit inbox first;
- add the separately gated `digest-on-next-cxmsg-turn` composition path;
- add mention wake, explicit wake-all with per-recipient fallback, digest
  bounds, expiry, and loop guards.

### Phase 7: Graph Projection and extended Doctor

- derive filtered Project, Cluster, Conversation, reachability, history, and
  Delegation edges;
- add Node detail and time-window views;
- add Directory, Ledger, Conversation, and projection consistency checks.

## Decisions required before implementation

The initial Message Body Store fixes these provisional Phase 2 values in code
and tests: 256 KiB maximum Message Body, 16 KiB inline threshold, 16 KiB
default read, 64 KiB maximum range read, 8 MiB append segment, and 64 MiB total
quota. Quota exhaustion rejects the new body and deletes nothing. Automatic
retention remains disabled. Phase 4 now provides a separately confirmed
explicit purge with an exact plan digest, durable Tombstones, recoverable
generation swaps, owner-private receipts, and generation-checked restore. The
temporary pre-index reader has a 256 MiB aggregate scan ceiling; the Phase 4
rebuildable index replaces its linear scan rather than silently raising that
ceiling.

- private Project ID creation and explicit declaration format;
- worktree discovery, Project move, merge, split, and Tombstone rules;
- Claude successor linking and Direct Conversation migration behavior;
- Ledger metadata retention, body retention, and terminal Delivery retention;
- default and maximum total storage quota;
- index-shard migration and reshard policy beyond the current 4,096-message cap;
- hash-shard count, shard-key version, and reshard migration behavior;
- partial-segment quarantine and operator recovery behavior;
- Route Admission Quarantine body retention, discard, expiry, and audit rules;
- automatic backup retention and expiry policy for explicit purge backups;
- Scheduler claim renewal and shutdown behavior beyond the fixed first slice;
- per-Node and per-Conversation queue depth;
- Group membership version retention;
- store-only expiry, digest count, digest byte limit, and cursor semantics;
- store-only inbox presentation and optional new-turn digest composition rules;
- mention syntax and explicit wake-all confirmation behavior;
- hop budget and forwarding provenance retention;
- migration treatment for existing Job history and absent ordinary send history.

For `dispatching` or `unknown` Route Delivery records, Phase 4 reconciliation
must first prove whether App Server accepted the stable client message ID.
Neither automatic nor operator-triggered retry may replay an uncertain wake
until that evidence contract and its crash tests exist.

The current pre-Ledger `cxmsg route reconcile` implements positive evidence
only. On the pinned Codex 0.147.0 protocol it searches a bounded summary window
for a `userMessage.clientId` equal to the Logical Message ID and may strengthen
the Delivery to `turn_started`. Absence, pagination exhaustion, legacy records
without a pinned target thread, and target replacement all authorize zero
replays. Actual retry remains a Phase 4 operation after durable body and
negative-acceptance evidence contracts exist.

No Implementation may silently choose these values.

## Acceptance tests

1. Renaming a Node preserves its identity, Direct Conversations, and history.
2. Restarting a Claude session creates a new Node until an explicit successor
   relation is recorded.
3. Multiple Git worktrees resolve to one Project while retaining distinct root
   aliases.
4. A Delegation fork does not appear as a Node unless registered.
5. Removing a Node leaves retained Conversations renderable through a
   Tombstone.
6. A standalone rollout writer reports `external-writer`, not reachable or
   stopped.
7. Every ordinary send has one Logical Message and the intended Delivery set
   before transport.
8. An ambiguous transport close becomes `unknown` and causes zero blind replay.
   A deterministic expected-turn mismatch may retry only on pinned versions
   whose tests prove that the rejected steer caused no mutation.
9. A version without expected-turn mismatch proof records `unknown` and makes
   zero retry attempts.
10. No record can claim `read` or `processed` without a newly defined proof.
11. A group batch crash before commit dispatches zero recipients.
12. A crash after commit reconciles each recipient without duplicate wake.
13. Replies attach to the Logical Message across retries and fan-out.
14. Store-only sends start zero model turns.
15. Mention wake reaches only mentioned Nodes; wake-all requires an explicit
    choice.
16. An unreachable wake-all recipient falls back to store-only without hiding
    the failed wake attempt.
17. One failed recipient remains visible without changing successful recipient
    states.
18. Dedup and hop budget prevent automatic reply and forwarding loops.
19. Segment index deletion is recoverable by bounded rebuild.
20. Retention and purge remove bodies without corrupting required metadata.
21. A partial final line quarantines its segment and never truncates it.
22. Web snapshots omit private paths and bodies at the final composition seam.
23. Graph counts are reproducible from Directory, Ledger, Conversations, Jobs,
    and grants.
24. A Codex thread and Claude session with the same native ID remain distinct
    Nodes.
25. A second Busy turn observed after expected-turn mismatch receives zero
    automatic steer attempts.
26. Removing a Group member does not remove that Node's previously committed
    store-only Delivery.
27. A late valid Claude terminal ACK reconciles `ack_timeout` to its terminal
    state and records that it arrived late.
28. Claude completion persistence occurs before the bridge emits a Codex reply
    wake, and duplicate receipts do not duplicate either transition.
29. A copied untrusted Peer Message containing a Delivery ID changes no
    Delivery state.
30. Doctor reports validated reply wake evidence without a reconciled Delivery
    while omitting reply text and private routing data.

## Recommended first slice

Implement only Phase 0 first. The current one-MiB receive limit and full-history
reads already make long-lived Nodes unreachable, so adding identity and chat
history before bounded activity reads would deepen the wrong behavior. After
Phase 0 passes large-thread integration tests, proceed through immutable Doctor
inspection and shared durable primitives before recording ordinary messages.
