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
- Status: NEXT
- Canonical phase: Phase 6

- [ ] Add explicit selectors for Project+role, Cluster, and Conversation; a
      selector resolves to a fixed recipient set before fan-out.
- [ ] Reject ambiguous, cross-Project, Tombstoned, and Execution Thread
      recipients before committing the batch.
- [ ] Add `mention-wake` with bounded mention syntax.
- [ ] Add explicit `wake-all` with per-recipient fallback and token-cost
      visibility.
- [ ] Add `when-idle`, `after-turn`, and `after-job` per-recipient policies
      without creating a second scheduling truth.
- [ ] Add bounded digest-on-next-cxmsg-turn composition; never steer an
      external or unrelated Busy turn with a digest.

Done when a Team Cast has deterministic membership, bounded token behavior,
and no quiet drop, implicit wake-all, or Project leakage.

### T9 — Graph Projection and extended Doctor

- Priority: P3
- Status: BLOCKED on T6; Group edges also require T7
- Canonical phase: Phase 7

- [ ] Derive separate `belongs-to-project`, `member-of-cluster`,
      `member-of-conversation`, `reachable-with`, `communicated-with`, and
      `delegated-to` Topology Edges.
- [ ] Add current, one-hour, 24-hour, and all-history filters.
- [ ] Add Node, Conversation, and Delivery detail projections with redacted
      paths, Endpoints, bodies, and capability data by default.
- [ ] Add Directory, Ledger, Conversation, Scheduler, fan-out, successor, and
      Tombstone consistency findings to Doctor.
- [ ] Keep the graph and Doctor read-only and start zero model turns.

Done when every rendered edge is traceable to one owning Module and the graph
cannot mutate routing, membership, lifecycle, or authority.

### T10 — Optional Repair Interface

- Priority: P4
- Status: DEFERRED

- [ ] Consider only after extended Doctor findings and identity evidence are
      stable.
- [ ] Require exact finding revalidation, a mutation lease, recoverability,
      and an audit receipt for each Repair.
- [ ] Keep restart, signal, cleanup, grant, permission, and approval operations
      outside broad automatic repair.

Repair is not required for Direct Conversation, Team Cast, or Graph Projection.

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
