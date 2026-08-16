# cxmsg implementation TODO

This is the execution checklist for the canonical Phase 0–7 roadmap in
[COORDINATION_GRAPH_CONVERSATIONS.md](COORDINATION_GRAPH_CONVERSATIONS.md).
It orders remaining work; it does not replace the design or change any
authority rule.

Baseline: cxmsg 0.23.0. Keep at most one item `IN PROGRESS`. A later item may
be researched early, but implementation does not begin until every listed
dependency is complete or an explicit design revision records why it is safe
to proceed.

## Priority rules

1. Preserve identity, retained content, and Delivery evidence before adding
   more history producers.
2. Prove recovery and idempotency before adding retry or fan-out.
3. Complete Direct Conversation invariants before Group Conversation.
4. Keep model wake policy separate from storage and recipient selection.
5. Keep Peer Messages untrusted; no roadmap item may create grant, approval,
   Delegation, or permission authority.

## Status legend

- `NEXT`: the only item that should be implemented next.
- `BLOCKED`: a named decision or dependency is still unresolved.
- `READY`: dependencies are complete, but a higher-priority item comes first.
- `LATER`: intentionally ordered after foundational work.
- `DEFERRED`: outside the current implementation sequence.

## Ordered checklist

### T0 — Freeze the 0.23.0 baseline

- Priority: P0
- Status: COMPLETE
- Scope: release gate

- [x] Review the minimal Peer Message projection and Cross-runtime Reply
      Adapter diff.
- [x] Confirm stable Claude session ID, changed Endpoint, legacy no-session,
      duplicate reply, wrong Node, and ACK separation tests.
- [x] Run the full test and syntax-check suites.
- [x] Commit and push 0.23.0 without the user-owned review notes or runtime
      state.
- [x] Confirm every restarted Claude bridge reports version 0.23.0 and
      implementation revision 13.

Done when the repository and running bridges point to one reviewed baseline
and no uncommitted implementation work is mixed into the next slice.

### T1 — Delivery retention and explicit purge

- Priority: P0
- Status: COMPLETE
- Canonical phase: Phase 4

- [x] Fix separate retention policies for Delivery metadata, Message Bodies,
      terminal evidence, and Quarantine.
- [x] Define quota ownership and behavior at warning and hard limits.
- [x] Add a read-only purge plan and dry-run output.
- [x] Add the shared Retention Mutation Barrier to Ledger, Message Body, Job,
      and Route Admission writers without holding it over model dispatch.
- [x] Add owner-private Delivery Dedup Tombstones that require the mutation
      barrier and prevent Logical Message ID or reply-target resurrection.
- [x] Add an explicit, recoverable purge mutation with exact selection and an
      audit receipt; automatic deletion remains disabled.
- [x] Preserve records referenced by active schedules, Jobs, replies,
      Conversations, Tombstones, or unresolved `unknown` evidence.
- [x] Add crash tests for segment rotation, index rebuild, and interrupted
      purge.

Done when storage growth is bounded without silently deleting live,
correlated, ambiguous, or authority-relevant evidence.

### T2 — Safe ordinary Peer Message retry

- Priority: P0
- Status: COMPLETE
- Canonical phase: Phase 4

- [x] Define the exact App Server evidence that proves an attempted input made
      zero mutation.
- [x] Keep connection loss, incomplete history, and protocol uncertainty as
      terminal `unknown` with zero automatic replay.
- [x] Add an explicit retry operation that reuses one Logical Message ID and
      stable client message ID.
- [x] Bound attempts, backoff, and expiry without copying the Message Body.
- [x] Test Busy-turn replacement, reconnect, duplicate commands, and process
      crashes at every write/dispatch seam.

Done when a rejected attempt may retry exactly once under proven
non-acceptance, while every ambiguous attempt produces zero replay.

### T3 — Durable Turn Lifecycle and Scheduler recovery

- Priority: P0
- Status: COMPLETE
- Canonical phases: Phase 2 and Phase 4

- [x] Record that App Server 0.147.0 has no notification replay cursor; persist
      a local observation sequence, connection epoch, and reconnect state.
- [x] Reconcile missed events without full-history reads or model work.
- [x] Distinguish an intentionally stopped Scheduler from a crashed worker.
- [x] Define claim renewal for bounded operations that may exceed the current
      lease.
- [x] Preserve FIFO per target while proving that lease loss stops the old
      dispatcher.
- [x] Add restart tests covering active claims, ready Triggers, and newly Busy
      targets.

Done when polling is only a bounded fallback and restart does not lose,
duplicate, or prematurely dispatch Scheduled Peer Messages.

### T4 — Explicit identity lifecycle

- Priority: P1
- Status: COMPLETE
- Canonical phase: Phase 3

- [x] Specify Project move, merge, split, and worktree alias transitions.
- [x] Specify Claude successor linking after restart or conversation change.
- [x] Keep successor relations from inheriting role, grant, permission,
      Conversation, Delivery, or authority implicitly.
- [x] Define explicit handling for schedules targeting a predecessor Node.
- [x] Extend Doctor to report ambiguous identity transitions without repair.

Done when later Conversations can reference stable Nodes and Projects without
silently following names, paths, sockets, or inferred successors.

### T5 — Scheduled Delegation and retained Job migration

- Priority: P1
- Status: COMPLETE
- Canonical phase: Phase 4

- [x] Schedule a Delegation through the Scheduler Module without treating its
      Trigger as approval.
- [x] Revalidate the stored grant, permission profile, approval policy, target
      Node, and Project immediately before execution.
- [x] Define migration for retained Jobs without inventing ordinary Peer
      Message history.
- [x] Preserve one Job correlation ID across queueing, execution, approval,
      result, and restart.
- [x] Test grant revocation, successor mismatch, expiry, duplicate enqueue,
      and crash recovery.

Done when a scheduled Delegation starts only under still-valid user authority
and cannot be confused with a Scheduled Peer Message.

### T6 — Direct Conversation

- Priority: P1
- Status: COMPLETE
- Canonical phase: Phase 5

- [x] Create one canonical Direct Conversation for an unordered pair of Node
      identities.
- [x] Assign durable per-Conversation Logical Message ordering.
- [x] Attach replies to the original Logical Message across retries and
      Cross-runtime Delivery Jobs.
- [x] Expose bounded owner-private history without injecting history into a
      model turn automatically.
- [x] Render Tombstoned members and define explicit successor migration.
- [x] Keep Conversation membership informational and non-authoritative.

Done when Codex↔Codex and Codex↔Claude use the same Conversation invariants
while Endpoint changes and retries do not fork history.

### T7 — Group Conversation, store-only first

- Priority: P2
- Status: COMPLETE
- Canonical phase: Phase 6

- [x] Add versioned Group Conversation membership independent from Cluster
      membership.
- [x] Freeze the recipient set and membership version at send time.
- [x] Commit one Logical Message and every recipient-specific Delivery in one
      crash-consistent fan-out batch before dispatch.
- [x] Implement `store-only` delivery and an explicit bounded inbox before any
      group wake policy.
- [x] Record partial failure per recipient; never collapse it into whole-group
      success or silently re-fan-out.
- [x] Add message deduplication, expiry, hop limits, and loop prevention.

Done when a Group message can be stored for a fixed recipient set with zero
model wake-ups and exact per-recipient evidence.

### T8 — Team Cast selection and wake policies

- Priority: P2
- Status: COMPLETE
- Canonical phase: Phase 6

- [x] Add explicit selectors for Project+role, Cluster, and Conversation; a
      selector resolves to a fixed recipient set before fan-out.
- [x] Reject ambiguous, cross-Project, Tombstoned, and Execution Thread
      recipients before committing the batch.
- [x] Resolve 1–16 explicit stable-Node mentions to an immutable subset of an
      unexpired plan without parsing display-name aliases or starting Delivery.
- [x] Persist the bounded Message Body and one atomic `prepared` Team Cast
      Ledger batch before any per-recipient transport attempt.
- [x] Dispatch prepared Codex recipients once with all-recipient preflight and
      independent `turn_started`, `failed`, or `unknown` evidence.
- [x] Add cross-runtime `mention-wake` with 1–16 exact stable-Node mentions,
      Codex `turn_started`, and Claude `transport_delivered` evidence linked to
      a separate Claude Delivery Job.
- [x] Add explicit `wake-all` over the complete frozen plan, bounded to 64
      recipients, with per-recipient fallback outcomes, wake-turn visibility,
      and a conservative fan-out payload-byte ceiling.
- [x] Add an explicit Codex `when-idle` Busy fallback per recipient by reusing
      the shared Delivery Ledger and Scheduler claim protocol.
- [x] Add `after-turn` and `after-job` per-recipient policies without creating
      a second scheduling truth.
- [x] Add bounded digest-on-next-cxmsg-turn composition; never steer an
      external or unrelated Busy turn with a digest.

Done when a Team Cast has deterministic membership, bounded token behavior,
and no quiet drop, implicit wake-all, or Project leakage.

### T9 — Graph Projection and extended Doctor

- Priority: P3
- Status: COMPLETE
- Canonical phase: Phase 7

- [x] Derive separate `belongs-to-project`, `member-of-cluster`,
      `member-of-conversation`, `reachable-with`, `communicated-with`, and
      `delegated-to` Topology Edges.
- [x] Add current, one-hour, 24-hour, and all-history filters.
- [x] Add Node, Conversation, and Delivery detail projections with redacted
      paths, Endpoints, bodies, and capability data by default.
- [x] Add Directory, Ledger, Conversation, Scheduler, fan-out, successor, and
      Tombstone consistency findings to Doctor.
- [x] Keep the graph and Doctor read-only and start zero model turns.

Done when every rendered edge is traceable to one owning Module and the graph
cannot mutate routing, membership, lifecycle, or authority.

### T10 — Optional Repair Interface

- Priority: P4
- Status: COMPLETE; explicitly allowlisted Repairs only

- [x] Consider only after extended Doctor findings and identity evidence are
      stable.
- [x] Require exact finding revalidation, a mutation lease, recoverability,
      and an audit receipt for each Repair.
- [x] Keep restart, signal, cleanup, grant, permission, and approval operations
      outside broad automatic repair.

Repair is not required for Direct Conversation, Team Cast, or Graph Projection.

### Post-roadmap maintenance — Repair retention

- Status: COMPLETE; recoverable archive only

- [x] Add a deterministic read-only plan for consistent completed Repair
      transaction/receipt pairs with a fixed 90-day minimum age.
- [x] Add an explicit digest-confirmed archive transaction outside the active
      Repair quota, with crash recovery and no automatic deletion.
- [x] Add exact restore, Doctor consistency checks, and bounded archive policy
      before enabling Repair retention mutation.

### Post-roadmap maintenance — Claude transport evidence

- Status: COMPLETE; transport and completion remain separate

- [x] Parse Claude native `peer_message_status` control receipts instead of
      rejecting them as ordinary invalid Peer Message frames.
      The frame shape is pinned by compatibility tests against the Claude Code
      2.1.232 protocol shape. Live external-peer probing confirms that receipt
      emission is optional, so missing evidence is never inferred as failure.
- [x] Correlate each native receipt to an exact outbound transport message ID
      without treating `delivered` as a model ACK or completion.
- [x] Return best-effort native `delivered` or `denied` status for exact
      Claude-originated message IDs after downstream routing resolves.
- [x] Retain exact envelope-level reply correlation as separate evidence after
      stable source validation; ignore correlation claims in Message Body text.
- [x] Preserve formal ACK state, wake behavior, permission, and approval
      invariants for native receipts and ordinary structured replies.
- [x] Give optional `accepted` ACKs a bounded completion lifecycle with
      `acceptedAt`, `completionDeadlineAt`, and `completion_timeout`.
- [x] Keep accepted/queued evidence distinct from `turn_started`, preserve the
      first deadline on duplicate ACKs, and allow only exact-source terminal
      reconciliation after timeout.
- [x] Surface overdue or malformed completion deadlines through Doctor and
      expose bounded lifecycle timestamps through CLI and MCP status.

### Post-roadmap maintenance — Session alias safety

- Status: COMPLETE; explicit same-thread consolidation only

- [x] Serialize registration, removal, and consolidation by stable Codex
      thread ID so concurrent aliases cannot bypass the uniqueness check.
- [x] Refuse ordinary removal while any other registered alias points to the
      same App Server thread.
- [x] Add explicit `cxmsg consolidate <canonical> <duplicate>` that preserves
      the canonical record and moves only matching TUI attachment metadata.
- [x] Fail closed instead of transferring route bindings, bridges, grants,
      pending Jobs, working-directory identity, or authority implicitly.
- [x] Make the post-`thread/delete` removal seam explicitly rerunnable on exact
      missing-thread evidence while the registry still pins identity.
- [x] Regress the post-registry-removal crash window through the existing
      read-only `ENODEUNREGISTERED` Doctor finding.
- [x] Regress opposite-argument concurrent consolidation with one winner, one
      explicit loser, and no lock-order deadlock.

### Post-roadmap maintenance — Reconciliation observability

- Status: COMPLETE; unknown remains non-replayable

- [x] Distinguish a never-reconciled Delivery from one carrying durable
      `EACCEPTANCEUNVERIFIED` reconciliation evidence.
- [x] Stop recommending repeated bounded scans after no positive acceptance
      evidence was observed.
- [x] Preserve `unknown`, retain the Delivery, and grant zero retry or wake
      authority through the new diagnostic distinction.

### Post-roadmap maintenance — Runtime version observability

- Status: COMPLETE; package identity and restart markers remain separate

- [x] Add `cxmsg version`, `cxmsg --version`, and a bounded JSON projection.
- [x] Stamp Scheduler and host relay state with package version and a
      module-specific implementation revision.
- [x] Diagnose current, legacy, and stale Scheduler, host relay, and Claude
      bridge implementations without restarting them.
- [x] Compare the Codex-owned App Server handshake version with the configured
      Codex CLI only during an explicit deep Doctor pass.
- [x] Keep package-version skew informational when the loaded module revision
      is still current.

### Post-roadmap maintenance — Claude denial observability

- Status: COMPLETE; native wire status remains unchanged

- [x] Preserve the reason-free native `denied` control frame for protocol
      compatibility.
- [x] Record a bounded local error code and denial origin after successful
      status return so Route Admission quarantine and downstream failure are
      distinguishable.
- [x] Keep denial evidence out of routing, retry, wake, permission, approval,
      and Delegation decisions.

## Explicit non-goals for this sequence

- Multi-host routing, Redis Streams, and remote brokers.
- Treating Peer Messages, Conversation membership, Triggers, ACKs, or Git
  pointers as user approval or Delegation authority.
- Automatically replaying `unknown` Delivery evidence.
- Automatically following names, paths, UDS addresses, PIDs, or successor
  relations as Node identity.
- Making the diagnostic web view a mutation or orchestration authority.

## Per-item completion gate

Before marking any item complete:

1. Update `CONTEXT.md` if a new domain term or invariant is introduced.
2. Record unresolved policy choices in the canonical design document; do not
   choose silent defaults in code.
3. Add focused failure, idempotency, crash, privacy, and authority tests.
4. Run `npm run check`, the focused tests, and the full suite.
5. Update README, SECURITY, Doctor schema, and migration notes as applicable.
6. Restart only the workers or bridges whose loaded implementation changed.
7. Commit one coherent slice without runtime state, credentials, local paths,
   session rollouts, or unrelated user files.
