# Codex session messaging

`cxmsg` adds same-computer discovery and text messaging between independent
Codex sessions. It runs Codex `app-server` as the durable thread manager and
uses its Unix-domain socket as the transport.

This is an integration layer, not a built-in Codex `SendMessage` feature.
It is an independent community project and is not affiliated with or endorsed
by OpenAI or Anthropic.

## Requirements

- Codex CLI with `app-server --listen unix://` and `--remote unix://`
- Node.js 20 or newer
- macOS or Linux
- Claude Code 2.1.224 or newer for the optional Claude bridge

The prototype was developed against `codex-cli 0.147.0`. App Server is still
experimental, so pin and retest the Codex CLI version before relying on it.

## Quick start

Clone the repository and optionally expose the command globally:

```bash
git clone https://github.com/backpointer/cxmsg.git
cd cxmsg
npm link
```

Or replace `cxmsg` below with `./bin/cxmsg`.

Start the local App Server:

```bash
cxmsg server start
```

Adopt an existing thread under one unique display alias. If an older cxmsg
version left the same thread registered twice, keep the intended canonical
name and explicitly consolidate the duplicate:

```bash
cxmsg register worker <thread-id>
cxmsg consolidate worker old-worker --json
```

`consolidate` does not delete the App Server thread or Tombstone its stable
Node. It moves only unambiguous foreground attachment metadata and refuses to
infer or transfer route bindings, bridges, grants, pending Jobs, or authority.
Ordinary `remove` also refuses a shared thread, preventing one alias from
deleting the conversation behind another alias.

Session removal is restart-safe when App Server deletion succeeded but the
registry record still exists: a repeated remove accepts exact missing-thread
evidence and finishes registry and Node Tombstone cleanup. If a crash occurs
after registry removal but before the Tombstone, Doctor reports the retained
live Node as `ENODEUNREGISTERED`; it never repairs or deletes that Node
automatically. Confirm retirement before using the explicit Directory Node
tombstone command.

Open two terminals in the desired workspace:

```bash
# Terminal A
cxmsg open coordinator

# Terminal B
cxmsg open worker
```

List peers and send a message from a shell owned by a named session:

```bash
cxmsg peers
cxmsg send worker "Migration finished; rebase is safe."
```

`cxmsg open` sets `CODEX_SESSION_NAME`, so the sender is automatic. From a
different shell, specify it explicitly:

```bash
cxmsg send --from coordinator worker "ping"
```

For an explicitly isolated target, bind its current registered thread to one
Project and role, then use a typed routed send:

```bash
cxmsg route bind coordinator --project hermes --role coordinator
cxmsg route bind worker --project hermes --role auditor
cxmsg send \
  --from coordinator \
  --project hermes \
  --target-role auditor \
  --sender-role coordinator \
  --logical-message-id <uuid> \
  worker \
  "Review handoff <id> at commit <sha>."
```

Each newly admitted message with pinned sender and recipient threads receives a
short recipient-scoped Reply Handle. Use it, or the Logical Message ID for
backward compatibility, for a strictly correlated response:

```bash
cxmsg reply <reply-handle|logical-message-id> "Review passed at commit <sha>."
```

`reply` derives its destination from the original Ledger record instead of
accepting a target name. New sends pin both the sender and recipient thread IDs;
the reply is rejected before context injection if either name now resolves to a
different thread. A routed reply also requires both current bindings and
inverts the original `sender_role` and `target_role` within the same Project and
task. Therefore a routed request intended to receive a strict reply must include
`--sender-role`. Records created before sender-thread pinning remain readable but
cannot be used as strict reply targets. `reply` is still an untrusted Peer
Message and creates no authority or approval.

The Codex App Server model-visible Peer Message projection is intentionally small: a leading
`[untrusted-peer]` marker, sender display alias, optional Reply Handle, and the
body or bounded preview. Protocol, stable `from`/`to` identity, Project and role
route, timestamps, wake policy, triggers, expiry, claims, attempts, digests, and
Delivery outcomes stay outside model context. `additionalContext` is not used
as a token-free metadata channel because it is still part of model input.
Retained large bodies can be read with `cxmsg message show <reply-handle>` from
the recipient session.
Scheduled records created before Reply Handles existed expose their Logical
Message UUID only when both legacy thread identities are pinned, preserving
strict reply compatibility without restoring the full metadata envelope.

Claude-to-Codex ingress with a valid native Claude session ID now stores the
stable `claude:<session-id>` Node identity in the owner-private Delivery Ledger
and projects only the recipient-scoped Reply Handle. `cxmsg reply` resolves the
same Claude session ID against its current live endpoint, so a socket change
does not reinterpret a display name or reuse the old address. Frames from older
Claude peers that omit `from-session` receive no Reply Handle and temporarily
retain the exact reply address in model context; this fail-safe compatibility
path can be removed only after those peers supply stable Node identity.
For a Project-routed message, the Claude Node must also be present in that
Project's Node Directory (for example via `cxmsg directory sync --project
<routing-id> --claude-only`); otherwise the reverse route fails closed instead
of crossing a Project based on a claimed role.

Cross-runtime replies create a Claude Delivery Job whose owner-private
correlation record links the new Logical Message ID to the original message.
The Claude transport ACK remains separate evidence: a queued or
`transport_delivered` reply is not reported as model completion.

Once a target is bound, an untyped, expired, wrong-Project, wrong-role, or
stale-thread message is stored in owner-only Quarantine before any App Server
turn is started or steered. Inspect redacted metadata only with:

```bash
cxmsg route show worker --json
cxmsg route list --json
cxmsg route reconcile <logical-message-id> --json
cxmsg route retry <logical-message-id> --json
cxmsg quarantine list --json
```

There is intentionally no automatic quarantine release, replay, reroute, or
permission effect. `route reconcile` starts no model turn: it scans a bounded
App Server turn window for a `userMessage.clientId` exactly matching the
Logical Message ID. Positive evidence strengthens `dispatching` or `unknown`
to `turn_started`. Missing or truncated evidence remains `unknown` and is never
permission to retry. Reusing the same `logical_message_id` with identical
sender, target, route, and body is a no-op after its first dispatch attempt;
reusing it with different content fails as an idempotency conflict. Targets
without an explicit binding retain legacy unscoped-send compatibility during
migration.

Inbound Peer Message Policy v1 currently has an internal owner-private record
Adapter, pure evaluator, schema inspection, and Doctor foundation only. Public
policy mutation and message enforcement are deliberately unavailable until
direct, Explicit Retry, Scheduler, Group, and Team paths pass the cross-path
integration gate. If policy records appear before activation, Doctor reports
`EINBOUNDPOLICYINACTIVE`; do not treat those records as enforced blocking.
Removing the final internal rule removes its empty configuration record, while
an invalid record requires exact file-digest confirmation to purge. Neither
operation removes Delivery Ledger denial evidence or retained message data.

After a scan records no positive evidence, Doctor reports
`EROUTERECONCILEDUNKNOWN` instead of requesting the same scan again. The
Delivery remains retained and non-replayable.

One narrow explicit retry is available only when the pinned App Server
contract proves that `turn/steer` rejected the input before queue mutation.
cxmsg 0.27 recognizes the audited `codex-app-server/0.147.0` no-active-turn,
expected-turn-mismatch, and non-steerable-turn rejections. It stores the first
result as `retryable`, retains every admitted ordinary Message Body, enforces a
one-second backoff and ten-minute retry window, and reuses the exact Logical
Message ID and body. A second proven rejection is `failed`. A timeout,
disconnect, unsupported App Server version, incomplete reconciliation, or any
other ambiguous result is `unknown` and permits zero retry. Retry is never
automatic:

```bash
cxmsg route retry <logical-message-id> --json
```

The complete fail-closed contract is documented in
[`docs/PEER_RETRY_POLICY_V1.md`](docs/PEER_RETRY_POLICY_V1.md).

New ordinary Codex Peer Messages are committed to the owner-only Delivery
Ledger before App Server dispatch. One append record atomically contains the
Logical Message metadata and its single recipient Delivery; later append-only
records distinguish the dispatch attempt from `turn_started` or `unknown`
evidence. The Ledger stores body byte count, digest, and an optional opaque
Content Reference, never the raw body. Legacy `route-deliveries` records remain
readable for reconciliation but new sends do not create them.

The Ledger supports `when-idle`, `after-turn`, and `after-job` scheduled
delivery. It retains every scheduled body before enqueue, requires an explicit
expiry no more than seven days away, and starts no turn while the target is
Busy:

```bash
cxmsg send \
  --from coordinator \
  --project hermes \
  --target-role auditor \
  --logical-message-id <uuid> \
  --wake-policy when-idle \
  --expiry 2026-08-16T12:00:00Z \
  worker \
  "Review handoff <id> after the current turn."
```

Exact Trigger examples use the same route and expiry:

```bash
cxmsg send --from coordinator --project hermes --target-role auditor \
  --after-turn <turn-id> --expiry 2026-08-16T12:00:00Z \
  worker "Continue after that exact turn reaches a terminal state."

cxmsg send --from coordinator --project hermes --target-role auditor \
  --after-job <job-id> --expiry 2026-08-16T12:00:00Z \
  worker "Use the result after that exact Job reaches any terminal state."
```

The referenced turn or Job must exist at enqueue time. `after-turn` accepts a
running or recognized terminal turn pinned to the target thread; `after-job`
fires for success or failure. A pending Trigger remains `waiting-trigger`; a
missing, unavailable, unknown, or unsupported state is observationally
`blocked` and never becomes permission to wake. Trigger readiness is checked
again after claim acquisition. If it regresses, the unused claim is released
without recording a dispatch attempt. A blocked earlier Trigger holds its
target lane so a later message cannot violate FIFO. `cxmsg status <target> --json` exposes only
the bounded active and recent terminal turn IDs, never turn contents.

`cxmsg server start` starts the Scheduler with App Server. It can also be
managed with `cxmsg scheduler start|status|stop`. The worker checks the target
again after acquiring a 30-second claim, releases the claim if the target has
become Busy, renews the still-owned claim immediately before dispatch, and
records one attempt immediately before `turn/start`. A failed renewal stops the
old dispatcher with zero attempts; expired claims are recoverable after
restart. Each target lane is FIFO and accepts at
most 256 pending scheduled Deliveries. A transport result whose mutation is
uncertain becomes `unknown` and is never replayed automatically.

The [Turn Lifecycle and Scheduler Recovery contract](docs/TURN_LIFECYCLE_SCHEDULER_V1.md)
consumes App Server 0.147.0 `thread/status/changed`,
`turn/started`, and `turn/completed` notifications as low-latency wake signals.
That protocol has no replay cursor, so cxmsg persists its own monotonic
observation sequence and connection epoch, then reconciles each pending target
with `thread/read` and one recent eight-turn metadata-only page after reconnect.
Polling remains a bounded fallback; notifications are never treated as durable
delivery, completion, approval, or authority evidence. The lifecycle store
retains no turn items or message text.

The Scheduler reads a rebuildable per-message index instead of rescanning the
whole Ledger on every poll. The append-only Ledger remains the source of truth;
owner-only index shards and their checkpoint are cache evidence and rebuild
automatically when their bounded segment manifest is stale. Operators can
inspect or cancel an unclaimed scheduled Delivery without exposing its body:

```bash
cxmsg deliveries list --status scheduled --json
cxmsg deliveries show <logical-message-id> --json
cxmsg deliveries cancel <logical-message-id> --json
cxmsg deliveries rebuild-index --json
```

Two deterministic Doctor findings have an explicit Repair path. Planning is
read-only and writes no Repair state. Apply requires the exact digest, repeats
the finding and evidence checks under a private Repair lease, preserves an
owner-only backup, calls the existing owner mutation once, verifies the same
finding, and writes a terminal receipt:

```bash
cxmsg repair plan directory-cluster-memberships.history.<short-id> --json
cxmsg repair apply directory-cluster-memberships.history.<short-id> \
  --confirm <plan-digest> --json

cxmsg repair plan delivery-ledger.index.consistency --json
cxmsg repair apply delivery-ledger.index.consistency \
  --confirm <plan-digest> --json

cxmsg repair retention plan --before <ISO-timestamp> --json
cxmsg repair retention archive --before <same-ISO-timestamp> \
  --confirm <plan-digest> --json
cxmsg repair retention recover --json
cxmsg repair retention restore <archive-id> \
  --confirm <archive-id> --json
```

Only `ECLUSTERMEMBERSHIPREDO` and `ELEDGERINDEXSTALE` are allowlisted. A stale
digest, changed owner evidence, ambiguous Cluster prefix, unsafe backup,
unverified result, or exhausted 256 MiB/1,024-transaction Repair retention
bound fails closed. Repair Apply never resends a message, follows a successor,
changes identity or membership intent, signals or restarts a process, grants
authority, changes permissions, or answers an approval. Doctor
reports incomplete Repair journals but never resumes or rolls them back.

Repair retention planning is a separate, read-only maintenance interface. It
preserves at least 90 days, selects only consistent `completed` transaction and
receipt pairs, reports failed or incomplete attempts as blocked, and emits no
backup contents or storage paths. Archive requires the exact plan digest under
the same Repair mutation lease, then atomically moves each selected pair into a
separate owner-private 1 GiB/1,024-archive bounded store. It does not erase the
backup. Interrupted pair moves remain journaled and `recover` rolls them
forward; Doctor reports them without mutation. Restore requires the exact
archive ID, rechecks the active Repair quota and every content digest, and can
run only once. Archive and restore never start a model turn or create authority.
Archive revalidates its selection after first rolling forward any older
incomplete Repair-archive journal under the same lease. Thus a stale archive
request never moves one of its own candidates, although it may finish a
previously committed interrupted operation before returning the stale error.

Cancellation is terminal and refuses an active, unexpired claim. Scheduler
status includes a heartbeat and reports a live identity with a stale heartbeat
as `stalled`, rather than as stopped. A separate desired-state marker makes a
missing worker `crashed` when it was intended to run and `stopped` only after an
operator stop. Claim, renewal, release, expiry, cancellation,
dispatch, and bounded failure outcomes are written to the existing redacted
coordination event log; retained bodies and endpoint paths are never included.
`cxmsg doctor` validates the index/checkpoint and heartbeat without rebuilding,
restarting, claiming, cancelling, or dispatching anything.

This slice does not implement automatic retry or task-completion inference.
Store-only Group fan-out is recorded without model wake, while explicit Team
Cast mention, wake-all, when-idle, after-turn, and after-job policies use their
separately verified paths. Scheduled Delegation is implemented as a separate
durable Job path and never creates ordinary Peer Message history. The index is
bounded to 4,096 Logical Messages and is not a retention mechanism. An ambiguous
dispatch remains `unknown`, and only positive App Server acceptance evidence
may strengthen it to `turn_started`. The sole explicit retry path requires
version-pinned Negative Acceptance and never replays `unknown`. Storage uses
private 8 MiB JSONL
segments, a 64 MiB fail-closed quota with bounded terminal-evidence reserve,
and a 256 MiB hard scan ceiling. Automatic retention remains disabled. Explicit
purge requires an exact read-only plan digest and preserves a restorable backup.

Doctor derives stale dispatch observation from the same 30-second grace used
by `route reconcile`. An attempt with no evidence remains `created` in the
Ledger; after the grace Doctor reports `ELEDGERATTEMPTSTALE` without changing
state. A successful or negative reconciliation appends evidence and removes
that derived stale condition. A Logical Message ID found in both the Ledger
and legacy storage is an unhealthy `ELEDGERDUPLICATEIDENTITY`; runtime lookup
still uses the Ledger first and starts no duplicate turn.

Doctor measures active and quarantined Ledger segment bytes without reading
record bodies. Usage at 90 percent is a warning and usage at or above the
quota is a required failure because new sends are rejected. Evidence reserves
can make writes fail before raw bytes reach 100 percent. At exhaustion, stop
new sends and inspect an explicit retention plan. Do not edit, partially
delete, or move segments manually. The supported purge path revalidates the
plan under an exclusive mutation barrier and retains its backup. Monitor the
Message Body Store on the same volume as a separate quota consumer.

Retention is always operator initiated:

```bash
cxmsg retention plan --before <ISO-timestamp> --scope all --json
cxmsg retention purge --before <same-ISO-timestamp> --scope all \
  --confirm <plan-digest> --json
cxmsg retention restore <backup-id> --confirm <backup-id> --json
cxmsg retention recover --json
```

The plan emits no Message Body text. Purge rejects changed candidates,
nonterminal or `unknown` evidence, active claims, reply/Job references, unsafe
files, partial segments, and a mismatched digest. A rejected Route Admission
record and its quarantined Ledger record may be removed only together with
`scope=all`. Tombstones are durable before generation replacement. A crash
after that point rolls forward from content-addressed generation evidence.
Restore succeeds only for the current transaction head when every active
generation is unchanged; it restores retained data but deliberately keeps
Tombstones, so the old Logical Message ID and reply target cannot wake again.
Backups and abandoned staging generations have no automatic expiry.

Legacy compatibility applies only when the binding file is genuinely absent.
If a binding path exists but its file type, owner, mode, link count, JSON, or
identity schema is invalid, cxmsg quarantines the message as
`binding_invalid`; it never interprets damage as an unbound target. An explicit
`route bind` may replace the damaged record after the operator verifies the
intended session, Project, and role.

Supplying `--sender-role` is an assertion that must match a binding for the
currently registered sender thread. An unbound, missing, or replacement sender
is quarantined rather than trusted. Use `--` after send options when an
operator wants an explicit end-of-options delimiter.

`--payload-type` belongs to the typed Route Envelope and therefore requires
both `--project` and `--target-role`. It does not silently decorate an ordinary
direct message. Omit it for an unscoped direct send; cxmsg reports this exact
compatibility requirement before creating a Logical Message.

Project, task, role, and payload-type identifiers are 1–128 ASCII safe
characters and may contain letters, digits, `.`, `_`, `:`, or `-`. Codex
session names remain the narrower 1–64 character namespace without `:`.

## Stable Node and Project Directory

Create one explicit local Project identity before synchronizing addressable
sessions. The routing label remains human-readable, while cxmsg generates and
retains a separate private UUID:

```bash
cxmsg directory project ensure hermes /path/to/hermes
cxmsg directory project move hermes /new/path/to/hermes --json
cxmsg directory project-transitions --project hermes --json
cxmsg directory sync --project hermes --codex-only
cxmsg directory sync --project hermes --claude-only
```

Omit the runtime-only flag to synchronize both. Git repositories and their
worktrees are matched by canonical Git common directory; non-Git projects use
the explicitly declared canonical root. Equal basenames, remote URLs, or
ancestor paths do not authorize cxmsg to choose a Project silently during
`cxmsg create`. A scheduled Delegation to an unsynchronized target fails before
Job creation with `ETARGETNODE`; when exactly one Project contains the target,
the error includes the exact bounded `directory sync --project ... --codex-only`
command. Similar paths never cause an automatic merge.

An explicit Project move retains the stable Project UUID, appends an
owner-private transition, preserves prior root aliases, and changes only the
discovery head. Repeating the same move is idempotent. Project merge and split
are intentionally unsupported; neither path nor Git metadata may infer them.
See the [Identity Lifecycle contract](docs/IDENTITY_LIFECYCLE_V1.md).

Inspect redacted identity metadata:

```bash
cxmsg directory projects --json
cxmsg directory nodes --json
cxmsg directory node show codex <thread-id> --json
cxmsg directory node tombstone codex <thread-id> --reason session-retired
cxmsg directory successor add codex <old-thread-id> codex <new-thread-id>
cxmsg directory tombstones --json
cxmsg directory successors --json
cxmsg directory execution sync --json
cxmsg directory execution-threads --json
cxmsg directory execution-thread show <thread-id> --json
cxmsg directory node show codex <thread-id> --history --json
cxmsg directory cluster ensure release-reviewers --json
cxmsg directory cluster member add release-reviewers codex <thread-id>
cxmsg directory cluster show release-reviewers --history --json
cxmsg directory cluster recover release-reviewers --json
cxmsg directory clusters --json
cxmsg directory cluster tombstone release-reviewers --reason group-retired
cxmsg directory cluster-tombstones --json
```

Project paths are omitted by default. `directory projects --paths`,
`directory node show ... --endpoints`, and `directory node show ... --history`
are explicit local disclosure options.
The stable Node key is `(runtime kind, native ID)`, encoded as
`codex:<thread-id>` or `claude:<session-id>`; names are mutable aliases.
Endpoint PID, UDS address, App Server presentation name, status, and generation
remain volatile evidence and are not Node identity.

Each Node retains at most 64 Endpoint observations across at most 16 transport
kinds. Selection decisions distinguish `selected`, `replaced`, `refreshed`,
`older-rejected`, and `conflict-rejected`; an imported pre-history selection is
marked `baseline-imported`. Repeated identical observations are coalesced with
a count and first/last timestamps. Compaction preserves the latest successful
evidence for every currently selected transport. A rejected observation never
overwrites the selected Endpoint. Default Node output exposes only the history
count; `--history` may reveal owner-private addresses and session aliases.

Removing a registered Codex session creates a reduced Node Tombstone when that
Node is present in the Directory. Operators can also Tombstone a retired Node
explicitly. A Tombstone retains only stable identity, Project identity, last
safe label, reason, and removal time; it prevents `directory sync` from
automatically recreating that Node. Interrupted transitions that leave both a
live Node and Tombstone are reported by `cxmsg doctor` and are never repaired
automatically. Ordinary routed and legacy-unbound sends to a Tombstoned Node
are quarantined before model-context injection.

A successor relation is an explicit one-way link from an old Node to a live
same-Project Node. Each successor has at most one predecessor and cycles are
rejected. The link retains lifecycle provenance only: it does not transfer a
grant, permission profile, role, Conversation membership, pending message, or
authority. cxmsg never infers successors from a reused name, path, PID, socket,
or restart.

A Scheduled Peer Message never follows a successor relation. Once its pinned
thread is a predecessor, dispatch remains blocked with
`ETARGETPREDECESSOR`; if the relation appears after claim, the unused claim is
released with zero attempts. The operator must cancel the old schedule and
enqueue a new Logical Message for the intended successor.

Fork Delegations, explicit fresh executions, and standalone `thread/start`
fallbacks are classified as
non-addressable Execution Threads before their first delegated turn starts.
The record contains only the execution thread ID, Job ID, source thread/Node
reference when available, creation mode, and classification time. It contains
no task, result, permission, approval, or message body. Execution Threads are
not synchronized as Nodes and cannot become Conversation members merely by
being classified.

`directory execution sync` is an explicit migration command for retained Jobs.
It classifies only fork Jobs with distinct UUID-shaped source and execution
threads, a retained turn ID, and a non-startup Job state. Ambiguous historical
Jobs are skipped, existing classifications are reused, and Job records are not
rewritten. cxmsg never infers an Execution Thread from an arbitrary unregistered
App Server thread.

A Cluster has its own private UUID and a human routing label. Membership is an
explicit many-to-many relation: one Node may join several Clusters and one
Cluster may span Projects. Each real change appends an immutable, ordered
membership snapshot; duplicate add/remove operations are idempotent and do not
advance the version. A live Cluster can add only live Nodes, while existing
history may continue to reference Node Tombstones. Removing a Cluster creates
a reduced Tombstone and retains its membership snapshots for later graph
history.

Snapshot-first writes have one recoverable crash window: an immutable next
version can exist before the current Cluster head advances. Cluster mutation
and Tombstoning deterministically redo exactly one valid next snapshot while
holding the Cluster lock; `directory cluster recover` exposes the same bounded
operation explicitly. Multiple, malformed, non-contiguous, or identity-broken
snapshots remain fail-closed. Doctor stays read-only and reports the single
redo case separately. At 1,024 retained versions Doctor warns for operator
retention review, but cxmsg never purges immutable membership automatically.

A Cluster Tombstone permanently reserves both its stable UUID and routing
label. Creating another Cluster with the retired label is intentionally
rejected rather than interpreted as identity reuse.

Cluster membership is organizational metadata only. It does not create a
Conversation, route, transport reachability, wake, grant, permission, approval,
or fan-out. Default CLI output exposes only member counts; `--members` is an
explicit local disclosure option, and `--history` reads retained versions.

## Direct Conversations

cxmsg records one owner-private Direct Conversation for each unordered pair of
known stable Node identities. Codex-to-Codex Ledger messages and correlated
Codex-to-Claude Jobs share the same ordering and reply invariants. Retries reuse
the existing sequence; names, sockets, PIDs, paths, and Endpoint changes do not
fork history.

Inspect the metadata-only projection locally:

```bash
cxmsg conversation list --json
cxmsg conversation recent codex:<thread-id> --limit 50 --json
cxmsg conversation show <conversation-id> --json
cxmsg conversation history <conversation-id> --limit 50 --json
```

History includes Node keys, Logical Message IDs, reply links, source kind, and
current Ledger or Job status. It never includes message bodies and is never
injected into a model turn automatically. Conversation membership creates no
role, permission, approval, Delegation, wake, or cross-Project authority.

`conversation recent` is a messenger-style, per-Node metadata projection. JSON
output contains `{ conversations, diagnostics, complete, hasMore }`. An
incomplete projection is printed for diagnosis but exits nonzero, so automation
cannot silently select an older peer after suppressed evidence. It combines
current Direct and Group membership, orders durable Logical Message
activity by parsed timestamp descending and Conversation ID ascending, and
shows the stable peer Node for Direct Conversations. Alias values are display
only. A bounded owner-private summary index keeps the healthy path from loading
retained message arrays. Missing-summary diagnosis inspects at most 32
unindexed Conversation records to scope the finding to the requested Node;
excess records remain explicitly unscoped. Doctor reports missing, stale, or orphan summaries
without repairing them automatically. Each summary is bound to the private
Conversation file generation; a crash-stale generation is omitted rather than
used for peer discovery. Reads share the Conversation mutation lock so member
migration cannot race the projection. The command never selects a replacement peer, follows an implicit
successor, authorizes a send, or exposes a body, Endpoint, path, task, result,
grant, or token. Unread state is `null` in this first projection slice and is
not inferred from Delivery or message counts.

Only messages whose retained Ledger or Claude Job source matches the
Conversation are projected. A metadata entry left by a crash before its source
commit remains recoverable by exact retry but is not presented as a recent
conversation until that source exists.

Create an empty Direct Conversation only after both Nodes are present in the
Directory, or explicitly continue one after recording the exact successor
relation:

```bash
cxmsg conversation direct ensure codex <thread-id> claude <session-id>
cxmsg conversation migrate <conversation-id> codex <old-id> codex <new-id>
```

Original members remain visible when Tombstoned; migration changes only the
current member projection. Retention protects Ledger and Message Body records
referenced by Conversation history. See
[Direct Conversation v1](docs/DIRECT_CONVERSATION_V1.md).

Inbound Claude `from-session` values are wire claims, not Directory evidence.
cxmsg therefore never creates a Node from an inbound frame merely to populate
recent conversations. Synchronize locally observed Nodes explicitly; a missing
Node causes Conversation recording to be skipped without weakening delivery or
Route Admission.

## Group Conversations and store-only inbox

Group membership is explicitly versioned and independent from Clusters. The
initial Group implementation requires 3–65 live Nodes in one exact Project.
Every send freezes the current membership version and recipient set, stores the
body by Content Reference, and commits all recipient-specific Deliveries in one
Ledger batch.

```bash
cxmsg conversation group ensure review-team \
  codex:<uuid> codex:<uuid> claude:<uuid>
cxmsg conversation group list --json
cxmsg conversation group show <conversation-id> --members --history --json
cxmsg conversation group member add <conversation-id> codex:<uuid>
```

The v1 send policy is always `store-only`: it creates zero model turns, zero
Scheduler claims, and no automatic forwarding. An explicit expiry within seven
days is required.

```bash
cxmsg conversation group send <conversation-id> \
  --from codex:<uuid> \
  --expiry <ISO-within-7-days> \
  -- "Review handoff pointer abc123"
```

Inspect a recipient's bounded metadata-only inbox and acknowledge presentation
separately:

```bash
cxmsg inbox list claude:<uuid> --json
cxmsg inbox ack claude:<uuid> <conversation-id> <sequence>
cxmsg inbox digest-next codex:<uuid> --limit 8 --max-bytes 4096
cxmsg inbox digest-status codex:<uuid> --json
cxmsg inbox digest-cancel codex:<uuid>
```

Inbox output contains a Content Reference and per-recipient Delivery status,
not body text. Acknowledgement is only a local presentation cursor; it is not
proof of model read, processing, reply, or task completion. Partial recipient
failure remains visible per Delivery and never silently re-fans out. See
[Group Conversation v1](docs/GROUP_CONVERSATION_V1.md).

`digest-next` is an explicit one-shot Codex presentation intent. The next
cxmsg Peer Message that starts a new turn may append at most 16 oldest unread
messages and 8 KiB of clearly marked untrusted previews. It is never attached
to `turn/steer`, Delegation, or an externally started turn. The selected inbox
cursor advances only after App Server accepts that new turn; a failed start
keeps both the intent and unread state for later inspection.

## Team Cast recipient plans

Team Cast first resolves a Conversation, Cluster, or exact Project+role
selector to a fixed same-Project recipient set. Resolution is deliberately
separate from delivery: it starts no model turn and writes no Delivery.

```bash
cxmsg team resolve --from codex:<uuid> --conversation <uuid> --json
cxmsg team resolve --from codex:<uuid> --cluster review-team --json
cxmsg team resolve --from codex:<uuid> \
  --project <project-uuid> --role reviewer --json
cxmsg team plan <plan-uuid> --json
cxmsg team select-mentions --plan <plan-uuid> \
  --from codex:<uuid> --mention codex:<uuid> --json
cxmsg team select-all --plan <plan-uuid> \
  --from codex:<uuid> --json
cxmsg team prepare --selection <selection-uuid> \
  --from codex:<uuid> --logical-message-id <uuid> -- \
  "Review handoff pointer abc123"
cxmsg team dispatch <logical-message-id> --json
cxmsg team dispatch <logical-message-id> --when-busy when-idle --json
cxmsg team dispatch <logical-message-id> --when-busy after-turn --json
cxmsg team dispatch <logical-message-id> --after-job <job-uuid> --json
```

Default output exposes only the recipient count and set digest. Use
`--recipients` for an explicit owner-local identity listing. Plans expire after
15 minutes and cannot be rebound to a changed selector. Explicit mentions use
stable Node keys only and produce another zero-delivery fixed subset; they do
not parse names from prose. `select-all` is a separate explicit operation that
copies the plan's complete frozen recipient set, up to 64 Nodes. Both selection
commands start zero Deliveries and expose `estimatedWakeTurns` before body
persistence. Preparation stores the body and one exact
per-recipient Ledger batch. Explicit dispatch supports Codex and Claude
recipients, performs an all-recipient identity/transport preflight, and then
records independent per-recipient
outcomes. It never steers a Busy Codex turn or redrives an existing attempt.
Codex acceptance records `turn_started`; Claude frame acceptance records
`transport_delivered` plus a correlated Claude Delivery Job ID. That job, not
the Team Cast state, tracks ACK, overload retry, and later completion. By
default a Busy Codex recipient rejects the
whole preflight with zero attempts. `--when-busy when-idle` instead moves only
that explicit recipient into the existing Delivery Ledger/Scheduler claim
protocol; it never steers the active turn, creates a second queue, or schedules
Claude recipients. `--when-busy after-turn` pins each Busy Codex recipient's
exact active turn, while idle siblings still dispatch immediately.
`--after-job <job-uuid>` instead schedules every pending Codex recipient against
one exact existing Job and rejects a mixed Claude fan-out before changing any
Delivery. These Triggers control timing only; they are not approval, authority,
or task-completion evidence. The original 15-minute Team Cast expiry remains
the fallback deadline. See
[Team Cast selector plan v1](docs/TEAM_CAST_SELECTOR_V1.md).

Preparation also reports `estimatedFanoutPayloadBytes`, calculated as Message
Body bytes multiplied by the fixed recipient count. This is a conservative
payload ceiling, not an exact token forecast: stored-body references, envelope
metadata, tokenizer behavior, and each recipient's existing context change the
actual token cost. A failed per-recipient Busy schedule is reported as
`schedule_failed` and remains durably `prepared`; siblings are not hidden or
rolled back, and retry remains explicit.

When a Directory Project exists, `cxmsg route bind` also pins the binding to
the private Project UUID and stable Codex Node key. Reusing the same routing
label for another Project or replacing the registered thread cannot inherit
that admission. Existing bindings created before Directory adoption remain
supported until explicitly re-bound.

Codex Peer Messages accept up to 256 KiB. Bodies through 16 KiB are delivered
inline. Larger bodies are stored locally before transport and the receiver gets
only a bounded preview plus an opaque Content Reference. Read only the range
needed for the current task:

```bash
cxmsg message info cxmsg-message:<message-id> --json
cxmsg message show <message-id> --offset 0 --limit 16384 --json
```

The default read is 16 KiB and a single read is capped at 64 KiB. Repeat with
the returned `nextOffset`; do not infer that fetching a range means the model
read, understood, or completed the message.

Grant a coordinator permission to delegate user-authorized jobs to a worker:

```bash
cxmsg grant coordinator worker
```

Inspect named permission profiles available for the worker's project:

```bash
cxmsg permissions worker
```

Delegate a job and capture its printed job ID:

```bash
cxmsg delegate \
  --from coordinator \
  --permissions :workspace \
  --execution fork \
  --approval relay \
  worker \
  "Run the focused tests and fix the scoped failure."
```

Wait for completion or retrieve the same correlated result later:

```bash
cxmsg wait <job-id> --timeout 300
cxmsg result <job-id>
```

If a relayed approval is requested, inspect and resolve it without losing the
job or its App Server turn:

```bash
cxmsg approvals <job-id>
cxmsg approve <job-id> <approval-id>
# or: cxmsg deny <job-id> <approval-id>
```

Use `--json` with `permissions`, `wait`, or `result` for machine-readable
output. Revoke a delegation relationship with:

```bash
cxmsg revoke coordinator worker
```

Stop the local server when no attached Codex session needs it:

```bash
cxmsg server stop
```

## Read-only diagnostics

Inspect the installed package and the expected revision of each cxmsg-loaded
module without contacting a service:

```bash
cxmsg version
cxmsg --version
cxmsg version --json
```

The package version identifies the invoked installation. Implementation
revisions are separate restart markers for long-running Scheduler, host relay,
and Claude bridge workers. The App Server is owned by the configured Codex
installation, so it is reported as `external-codex` rather than assigned a
cxmsg implementation revision.

Run the bounded Doctor before changing runtime state:

```bash
cxmsg doctor
cxmsg doctor --json
cxmsg doctor --target worker
cxmsg doctor --deep --target worker
```

The default pass uses only passive process, registry, file, socket metadata,
Job, grant, Node Directory, Conversation, Team Cast fan-out, Delivery Ledger,
Scheduler, route-binding, Quarantine, bridge, and relay evidence. It verifies
that Direct and Group message metadata resolve to retained owner records and
that every prepared Team Cast recipient matches its immutable plan and
selection. `--deep` additionally performs
non-mutating App Server, Claude bridge, and host relay handshakes; resolves
registered threads with `thread/read(includeTurns:false)`; and checks stored
permission profile references. It also compares the App Server handshake
version with the configured Codex CLI version. Passive Doctor checks compare
the revision stamped by each Scheduler, host relay, and Claude bridge against
the currently invoked cxmsg code. A package-version difference alone does not
make an unchanged service implementation stale. Neither mode sends a peer message, starts or
steers a model turn, answers an approval request, changes a grant, signals a
process, removes a record, or repairs state.

Text output is intended for operators. Automation should use `--json` and the
versioned [Doctor schema](docs/DOCTOR_SCHEMA_V1.md). Exit code `0` means
`healthy`, `1` means `degraded` or `unhealthy`, and `2` means an invalid
invocation or failure to construct a report. Doctor deliberately has no
`--fix` option.

`--target` excludes unrelated global historical findings and keeps only the
selected Session's Jobs, attachments, bridge, thread, and sender/recipient
Route Deliveries alongside current service health. Stable duplicate finding
IDs are collapsed to the strongest result. Run unscoped Doctor when auditing
global Directory, Conversation, Message Body, policy, or Repair state.

Create a durable named session without attaching a TUI:

```bash
cxmsg create worker
```

Remove a named session and its saved Codex thread:

```bash
cxmsg remove worker
```

Adopt an existing App Server thread under a peer name:

```bash
cxmsg register worker <thread-id>
```

Peer names are 1-64 characters, start with an ASCII letter or digit, and may
otherwise contain letters, digits, `.`, `_`, or `-`.

The existing thread must be addressable by the same App Server. A regular
standalone `codex` TUI keeps an active-writer lock; close it before resuming the
thread through `cxmsg attach worker`. A register-only thread that is
`notLoaded` remains `stored-or-external`: peer delivery and Claude terminal
notification refuse to auto-resume it with `EEXTERNALWRITERUNVERIFIED`, rather
than competing with a possible standalone rollout writer.

## Local web views

Inspect the canonical read-only coordination graph from owner-private state:

```bash
cxmsg graph show --range current --json
cxmsg graph show --range 1h --edge communicated-with --edge delegated-to --json
cxmsg graph show --range all --paths --json
cxmsg graph node codex:<thread-id> --range 24h --json
cxmsg graph conversation <conversation-id> --limit 50 --json
cxmsg graph delivery <logical-message-id> --json
```

The projection keeps Project, Cluster, Conversation, reachability,
communication, Delegation, and successor relationships as separate edge
kinds. `current` includes current Directory, membership, Endpoint, and grant
evidence; `1h`, `24h`, and `all` additionally include temporal Ledger and Job
evidence. Paths require explicit `--paths`. Endpoint addresses, Message Bodies,
tasks, results, permission profiles, approvals, and grant tokens are never
projected. `reachable-with` means the local cxmsg owner currently has a selected
identity-verified reachable Endpoint for that Node; it does not infer that one
peer can directly reach another.
Registered threads or retained Ledger identities that have not yet been synced
into the Node Directory appear as `unresolved-directory` placeholders. They do
not receive Project or reachability Edges, and the summary reports their count
instead of silently inventing identity metadata.

The Node detail view composes only that Node's incident relationships. The
Conversation detail view returns current membership plus at most 200 bounded
message metadata records. The Delivery detail view separates each recipient's
admission, transport, evidence-state, and attempt-count summary. These detail
views expose stable Node, Conversation, Project, and Logical Message IDs needed
for diagnosis, but omit Message Body text and references, body digests,
Endpoint and reply addresses, grant capabilities, Job content, and native
attempt or turn identifiers. `--paths` remains the only explicit exception and
applies to Project paths in Graph and Node views only.

Start the loopback-only web server:

```bash
cxmsg web
```

Then open the two local views:

- `http://127.0.0.1:4173/dashboard` shows App Server health, registered Codex
  sessions, live Claude peers, bridges, delegation relationships, redacted
  Claude grants, and available permission profiles.
- `http://127.0.0.1:4173/orchestration` groups Codex and Claude nodes into
  project clusters, draws aggregated same-project and cross-project job routes,
  and shows correlation status, permission profiles, and reply delivery state.

Select another port with `cxmsg web --port <number>`. The server always binds to
`127.0.0.1`; it does not expose a LAN listener. Version 1 is deliberately
read-only. It does not send messages, create delegated jobs, alter grants, or
start model turns, so refreshing either page consumes no model tokens.

Task text, final result text, error bodies, Claude socket addresses, and grant
tokens are omitted from the web snapshot. The browser receives only operational
metadata. Keep using the CLI and agent-side tools for messaging and delegation.

## Foreground and background TUI

App Server owns each cxmsg thread continuously. The terminal UI is an
attachable client, so closing or detaching that UI does not remove the thread
or stop later `send` and `delegate` wake-ups.

Create a thread if needed and attach its TUI in the foreground:

```bash
cxmsg open worker
```

For an already registered thread, require it to exist and attach without
creating a replacement:

```bash
cxmsg attach worker
```

From another shell, inspect whether the cxmsg-managed TUI is attached and
whether the App Server thread is working:

```bash
cxmsg status worker
cxmsg status worker --json
```

Move the session to the background by terminating only its verified remote TUI
process. The foreground `open`/`attach` command then returns, while App Server
and the thread remain running:

```bash
cxmsg detach worker
```

Run `cxmsg attach worker` later to restore the TUI. `cxmsg session` remains a
compatibility alias for `cxmsg open`; `cxmsg background` is an alias for
`cxmsg detach`.

`status` reports `foreground` only when a live PID is verified as the exact
remote Codex TUI for that thread. It reports `background` for a thread known to
be managed by cxmsg without an attached TUI. An adopted thread that has not yet
completed a cxmsg attach reports `stored-or-external`, because a standalone
Codex TUI may still own its rollout lock. cxmsg never signals that unverified
process.

Only one writer can own a rollout. Before the first `cxmsg attach` of a thread
registered from a regular `codex` TUI, exit that standalone TUI. Restarting the
same thread with ordinary `codex resume` while cxmsg manages it is unsupported;
use `cxmsg attach` instead. `cxmsg server stop` and `cxmsg remove` refuse to run
while a verified remote TUI attachment would be orphaned.

## Claude Code bridge

Claude Code 2.1.224 and newer advertises each live local session through
`~/.claude/sessions/` and receives cross-session JSONL messages on an
owner-only socket under `/tmp/cc-socks/`. `cxmsg` can use that native transport
without invoking a Claude model for discovery.

List live Claude sessions:

```bash
cxmsg claude peers
```

Start one persistent bridge for the Codex peer that should receive Claude
replies:

```bash
cxmsg claude bridge start coordinator
cxmsg claude bridge status coordinator
```

If that Codex target has a Route Admission binding, an ordinary Claude message
must carry the same versioned routing envelope or it is quarantined before
context injection. The Claude message body is JSON in this form:

```json
{
  "protocol": "cxmsg-route/1",
  "schema_version": 1,
  "project_id": "hermes",
  "target_role": "coordinator",
  "logical_message_id": "<uuid>",
  "payload_type": "coordination",
  "wake_policy": "immediate",
  "message": "Review result is available at commit <sha>."
}
```

The bridge checks this envelope only for ordinary peer context. A validated
Claude grant request and an internal terminal-delivery wake keep their separate
authorization and correlation paths; routing fields cannot manufacture either
one. Until Claude Nodes join the Phase 3 Directory, an ordinary Claude envelope
must omit `sender_role` because no thread-pinned Codex sender binding can verify
that claim.

If a managed sandbox can read the registry but cannot connect to local Unix
sockets, start the authenticated host relay from an ordinary host terminal:

```bash
cxmsg relay start
cxmsg relay status
```

The relay binds only to `127.0.0.1`, keeps its bearer capability in the
mode-`0600` cxmsg state directory, and performs the same session, bridge,
socket-owner, and target checks as direct delivery. `cxmsg claude send`
automatically uses it only when direct UDS access is denied. If the sandbox
also blocks loopback TCP, run the command through an allowed host tool; cxmsg
does not weaken the sandbox or request broader agent permissions.

### Codex host-side MCP

When a Codex managed sandbox blocks both filesystem UDS and loopback TCP,
configure cxmsg as a local stdio MCP server. Codex launches the MCP process as
a host integration, so the model does not need to invoke the network-blocked
shell CLI:

```bash
npm link
codex mcp add cxmsg -- cxmsg-mcp
```

Restart the Codex client after changing MCP configuration. The server exposes
three bounded tools:

- `cxmsg_peers_list`: list Claude peers and their transport state without
  invoking a model.
- `cxmsg_send_peer`: send ordinary, user-authorized coordination text and
  return a durable delivery ID.
- `cxmsg_delivery_status`: read redacted transport, ACK, retry, and completion
  metadata for that delivery ID.

The MCP server uses stdio and opens no additional network listener. It reuses
the existing cxmsg bridge, peer validation, message limits, durable delivery
records, ACK handling, and retry policy. Its result distinguishes transport
acceptance from model completion and reports whether the destination was
actually attempted.

These tools deliberately do not expose `grant`, `delegate`, approval, retry,
or lifecycle operations. An MCP message is transport, not user authority, and
cannot expand a target's permissions. Existing same-user trust boundaries
still apply, so verify the source name, target session ID, cwd, and address
before sending sensitive text.

Use `codex mcp get cxmsg` to inspect the registration and
`codex mcp remove cxmsg` to remove it. The loopback relay remains useful for
callers that can reach `127.0.0.1`; the MCP path is intended for profiles that
cannot reach either local socket transport.

Claude's `ListPeers` now shows that bridge as `codex-coordinator`. Send a
message from the Codex identity to a uniquely named live Claude session:

```bash
cxmsg claude send \
  --from coordinator \
  claude-reviewer \
  "The migration is ready for review."
```

The command now prints a durable delivery ID. Inspect or retry it with:

```bash
cxmsg claude delivery <delivery-id> --json
cxmsg claude retry <delivery-id>
```

`transport_delivered` means the Claude UDS accepted the frame; it does not mean
the Claude API completed a model turn. cxmsg wraps correlated deliveries with
an ACK request. A cooperating Claude session replies to the message's `from`
address with one of these envelopes:

```text
<cxmsg-ack in-reply-to="<delivery-id>" status="accepted">
queued or started
</cxmsg-ack>
```

An optional `accepted` ACK is followed later by exactly one terminal ACK:

```text
<cxmsg-ack in-reply-to="<delivery-id>" status="completed">
brief result
</cxmsg-ack>
```

For transient overloads it can report a bounded retry:

```text
<cxmsg-ack in-reply-to="<delivery-id>" status="retryable_error" code="529" retry-after="30">
Overloaded
</cxmsg-ack>
```

Statuses are `transport_delivered`, `acknowledged`, `completed`,
`retry_scheduled`, `failed`, `ack_rejected`, `transport_error`, `unreachable`,
`ack_timeout`, and `completion_timeout`. An `accepted` ACK records
`acknowledged`, `acceptedAt`, and a bounded `completionDeadlineAt`. It does not
claim that a Claude model turn started because Claude may only have queued the
request. If no terminal ACK arrives before that deadline, the delivery becomes
`completion_timeout`; a later exact-source terminal ACK can still reconcile it.
A `429` or `529` retryable ACK schedules exponential backoff with a maximum
delay and attempt budget. Retries preserve the delivery correlation ID and
record each transport message ID so the receiver can avoid duplicating work.
After `accepted`, only `completed` or `failed` is valid; a later retryable ACK
is rejected so cxmsg cannot automatically duplicate work Claude already
accepted.

ACK instructions are embedded in each outbound frame. An in-flight delivery
created before an upgrade therefore retains its earlier instruction shape.
Restart every running Claude bridge before relying on a new ACK lifecycle;
`cxmsg doctor` reports bridges whose implementation revision is stale.

Claude Code may also return a native `peer_message_status` control receipt for
an individual transport message ID. Receipt emission is optional: a live
external-peer exchange can return an ordinary Peer Message without emitting a
control receipt for the original request. cxmsg retains a receipt only when one
actually arrives, using the bounded states `held`, `denied`, `expired`, and
`delivered` under `nativeReceipts`. Absence of native receipt evidence is not a
delivery failure. Even native `delivered` does not satisfy the model ACK, mark
the task complete, or wake Codex. Previously these control frames were rejected
as invalid peer frames.
The control-frame shape is pinned by compatibility tests against the Claude
Code 2.1.232 protocol shape; an unknown or malformed status frame fails closed
instead of being inferred. Live external-peer probing does not establish that
every Claude version or send path emits the optional frame.
The native control frame carries no sender identity. cxmsg therefore treats it
as unauthenticated same-user advisory evidence correlated by the random
transport message ID. Native receipt state never gates routing, retry, wake,
ACK, completion, permission, or approval.

In the opposite direction, after a Claude-originated Peer Message is admitted
and handed to the Codex delivery path, the bridge attempts to return a native
`delivered` receipt to the exact originating message ID. Route Admission
quarantine or a downstream delivery failure attempts `denied`. Both are
best-effort protocol feedback: successful UDS write does not prove that the
sender surfaced or persisted the receipt. It never claims that the Codex model
read or completed the request.
Because the native frame has no reason field, cxmsg also records bounded local
`errorCode` and `denialOrigin` evidence on a successful returned `denied`
status. Route Admission quarantine is distinguished from downstream delivery
failure without adding body text, paths, endpoints, or authority to the wire
frame.
If returning the native status itself fails, the original denial code remains
separate from bounded `returnErrorCode` transport evidence.

An ordinary Claude Peer Message may carry an exact envelope-level
`in-reply-to` UUID. After exact source validation, cxmsg records this separately
as `replyEvidence=correlated` while delivering the body as untrusted text. It
does not alter `ack_timeout`, `completed`, permissions, or approval. A UUID
mentioned only in Message Body prose is never parsed as correlation evidence.

ACK source verification requires both the stable session ID and exact UDS
address when Claude supplies `from-session`. Native replies that omit that
optional field fall back to an exact UDS address match. A mismatch is recorded
immediately as `ack_rejected` with `source_mismatch`, using only presence flags
and short hashes for identity evidence; it is not later misreported as an ACK
timeout.

The first valid `completed` or `failed` ACK also starts or steers one untrusted
turn on the originating Codex thread, waking it with the correlated result.
Duplicate terminal ACKs reuse the delivery ID as the App Server client message
ID and do not start another turn. `accepted` and retryable ACKs update delivery
state without waking Codex. Repeating the same `accepted` ACK does not extend
its original completion deadline.

Claude Code's local session registry currently exposes liveness and activity,
not the model API response code. If an API failure prevents Claude from running
at all, it cannot emit an ACK; cxmsg records `ack_timeout` and requires an
explicit retry instead of blindly duplicating a possibly accepted task.

If multiple Claude sessions share a display name, use the full session ID or
the `uds:/tmp/cc-socks/<pid>.sock` address printed by `cxmsg claude peers`.
When Claude replies to the incoming message's `from` address with its native
`SendMessage` tool, the bridge routes the text to the associated Codex peer.

### Correlated Claude requests

Ordinary Claude messages remain untrusted coordination context. To allow one
Claude session to submit bounded jobs, create a request grant. The default
permission profile is `:read-only`; select a broader profile explicitly only
when the requested work requires it:

```bash
cxmsg claude grant \
  --permissions :read-only \
  claude-reviewer \
  coordinator
```

The command prints a `grant-token`. Treat it as a capability secret and send it
only to the intended Claude session. Running `grant` again for the same Claude
session rotates the token. `cxmsg claude grants coordinator --json` lists
grants with redacted token hints.

The granted Claude session uses its native `SendMessage` tool to send the
following exact envelope to `codex-coordinator`:

```text
<cxmsg-request grant="<grant-token>">
Review the migration report and return the three highest-risk findings.
</cxmsg-request>
```

The bridge creates a correlation job using the native Claude message ID,
waits for the target Codex session to become idle, and runs the request in a
persistent fork. The fork inherits the Codex conversation context but uses the
grant's named permission profile and `approvalPolicy: "never"`. It never steers
the request into an unrelated active turn.

After the turn completes, the bridge sends exactly one response to the
request's original UDS reply address:

```text
<cxmsg-response in-reply-to="<request-id>" status="completed">
...Codex final answer...
</cxmsg-response>
```

The same job is available through the normal correlation commands:

```bash
cxmsg wait <request-id> --timeout 300 --json
cxmsg result <request-id> --json
```

If Claude exits before the response is delivered, the full result remains in
the mode-`0600` job record. After that Claude session is reachable again, retry
the stored response with:

```bash
cxmsg claude retry <request-id>
```

Revoke future requests with the stable Claude session ID printed by
`cxmsg claude peers`:

```bash
cxmsg claude revoke \
  <claude-session-id> \
  coordinator
```

Stop the bridge before removing its Codex peer:

```bash
cxmsg claude bridge stop coordinator
```

Each exposed Codex peer has a separate bridge process and socket, which makes
the reply address itself select the Codex destination. Bridge state and logs
are stored under `~/.codex/cxmsg/claude-bridges/`; the Claude-visible record is
removed when the bridge stops.

Thread activity checks use metadata-only `thread/read` plus bounded
`thread/turns/list` pages; cxmsg does not hydrate the full rollout merely to
check status, steer a message, refresh a Job, or run mirror preflight. Redacted
transport, ACK, timeout, and wake evidence is appended to the owner-only
`~/.codex/cxmsg/events.jsonl`. These JSONL records contain correlation IDs and
bounded state codes, not message bodies, full socket paths, or error text. The
active file is limited to 1 MiB and four owner-only archives are retained as
`events.jsonl.1` through `events.jsonl.4`, bounding this evidence set to about
5 MiB. Rotation and append share an owner-only lock; a logging failure never
turns a successful message operation into a delivery failure.

Status and ordinary delivery use an owner-only UDS health check. The cxmsg
bridge health response binds its target, thread, PID, start time, and a fresh
nonce to the registry record. New bridge records and health responses also
bind the cxmsg package version and a separately incremented bridge
implementation revision. `cxmsg doctor` warns when a running bridge predates
that stamp or reports another revision. Updating files does not replace code
already loaded by a long-running bridge; restart that bridge explicitly from
an allowed host context and rerun Doctor. Doctor never restarts it.

A sandbox may deny `kill(pid, 0)` or `ps` with `EPERM`; that makes process
identity unverified rather than stopped when the socket check succeeds.
Destructive lifecycle operations remain stricter:
`server stop`, `claude bridge stop`, and stale cleanup refuse to signal or
remove an identity they cannot verify.

Socket probes preserve their cause instead of returning a boolean. CLI and web
state distinguish `running`, `unreachable`, `stopped`, `stale`, `unknown`, and
`mismatched`. `EPERM` or a probe timeout with a valid registry and socket is
reported as `unreachable` (`verification=sandbox-denied` or `timeout`), never
as `stopped`. Unreachable peers remain visible with their session status and
error code. Start, stop, signal, and stale cleanup remain fail-closed.

The bridge transports text only. Incoming Claude text is converted to the same
untrusted `additionalContext` used by `cxmsg send`, and an idle Codex turn still
uses `approvalPolicy: "never"`. It cannot convey a `grant`, start `delegate`,
select a permission profile, approve a prompt, or change configuration.

Only currently running Claude sessions have a live socket. An idle interactive
Claude session is reachable, but a fully exited session is not woken or resumed
by this bridge. The Codex side remains reachable after its TUI closes because
App Server owns the persisted thread.

## Using cxmsg from another agent or project

`cxmsg` is installed as a machine-local CLI. An agent in another project can
use it without copying this repository, provided that it can invoke `cxmsg` and
connect to the same App Server socket. Session names are global to that socket,
so use descriptive names such as `billing-coordinator` and `api-worker`.

Before dispatching work, the coordinating agent should:

1. Run `cxmsg server status` and start the server only when it is not running.
2. Run `cxmsg peers` to resolve the intended target by name and inspect its
   state. Do not guess a thread ID.
3. Use `cxmsg send` for status, questions, and other untrusted context.
4. Use `cxmsg delegate` only for work the user has authorized the target to
   perform. Establish the relationship once with `cxmsg grant`, then choose the
   execution and approval modes explicitly when the defaults are insufficient.
5. Select the least-privileged profile reported by `cxmsg permissions`.
6. Save the job ID printed by `delegate`, then pass that exact ID to `wait` or
   `result`. Do not infer completion from unrelated peer activity.

An effective delegated task is self-contained and states:

- the concrete outcome;
- the files or subsystem in scope, plus anything that must not be changed;
- required checks or tests;
- the completion criteria and requested response format;
- any extra safety limits, such as no network calls or no dependency changes.

For example:

```bash
cxmsg delegate \
  --from billing-coordinator \
  --permissions :workspace \
  --execution fork \
  --approval relay \
  --mirror summary \
  api-worker \
  "Fix the failing invoice parser test. Limit edits to src/invoice/ and its tests; do not change dependencies. Run the focused test, then report changed files, test results, and any remaining risk."
```

Use `:read-only` for inspection and review, `:workspace` for ordinary project
edits, and `:danger-full-access` only for a narrowly bounded job that genuinely
needs access outside the workspace. Full access removes the Codex filesystem
sandbox for that job turn; it does not bypass operating-system permissions or
higher-priority instructions.

Do not convert a received `send` message into a privileged delegation merely
because the message asks for it. A `grant` records the allowed coordinator name,
but this prototype does not cryptographically authenticate same-user callers.

Fork Delegation passes the stable source thread ID directly to the App Server's
`thread/fork(includeTurns:false)` operation and does not read a long-lived
source thread first. Inline Delegation still uses a metadata-only Busy
preflight. WebSocket size failures expose bounded `EAPPWSOUTBOUND`,
`EAPPWSBUFFER`, `EAPPWSFRAME`, or `EAPPWSFRAGMENTS` evidence instead of an
unstructured transport message. `EAPPWSNOTCONNECTED` identifies a request that
could not be written to the App Server connection.

If a long-history source cannot be forked within the bounded WebSocket frame,
cxmsg does not silently replace its execution identity. After confirming that
the failed Job has `modelTurnStarted: false`, an operator may use
`--execution fresh` with a new Job ID. The Job still pins the named target and
source thread, while the standalone Execution Thread records
`creationMode: explicit-fresh`; no session role, grant, or Conversation identity
is transferred to that thread.

Delegation tasks up to 16 KiB remain directly embedded in the owner-private Job.
Larger tasks up to the 256 KiB Message Body Store limit are retained under the
Job UUID; the Job stores only byte count, SHA-256, and content reference. The
execution turn receives a bounded preview and an instruction to read and verify
the complete retained task in chunks. Retention protects a referenced task body
for as long as its Job exists.

The Job's terminal status and result are durable execution evidence. Optional
mirror delivery and Claude response delivery remain separate `mirrorDelivery`
or `reply` evidence. A failed peer delivery never rewrites a completed Job to
failed; `cxmsg result` and Doctor report the coordination failure separately.
Delegation failures also record `failureStage`, `turnStartAttemptedAt`, and a
tri-state `modelTurnStarted`: `true` is positive turn evidence, `false` proves
failure before model execution, and `null` means acceptance is ambiguous and
must not be retried automatically. `rerouteGuidance` is bounded operational
advice and never grants authority.

Idle persisted threads remain addressable even when their original TUI is
closed. A newly opened Codex session that has never received a first turn has no
rollout file yet and cannot be resumed after it exits; send one initial prompt
before adopting such a session.

## Waiting from an orchestration tool

`cxmsg wait` stays silent while a job is running or waiting for relayed approval
and writes the final result to
stdout only after the job reaches a terminal state. A long-running shell API may
yield control first and return an execution handle such as `session_id` without
terminating the process. Treat that response as running, not as an empty
successful result.

Use a blocking wait when the orchestrator can retain and poll a shell execution
handle:

```bash
cxmsg wait <job-id> --timeout 300 --json
```

The orchestration loop should follow this state machine:

```text
invoke wait
  -> exit_code present: process ended; inspect stdout, stderr, and the code
  -> session_id present without exit_code: save it and poll that same process
  -> neither: treat the runner response as incomplete, not as job completion
```

Do not discard the execution handle or start another `wait` merely because the
first shell response has empty output. The shell runner's yield interval and
`cxmsg wait --timeout` are separate timers. A `wait` timeout exits with code 1
and writes its error to stderr; it does not cancel the delegated job.

Use non-blocking polling when the orchestrator cannot preserve a live shell
handle:

```bash
cxmsg result <job-id> --json
```

Poll the same correlation ID at a bounded interval until `status` is no longer
`running`, `queued`, `dispatching`, or `awaiting_approval`. Preserve both stdout
and stderr, and require an explicit process exit code or a terminal job status
before declaring success or failure. Empty output alone is never a completion
signal.

## Delivery behavior

- New named sessions are stored as durable App Server threads prefixed with
  `cxmsg:`. Existing threads can instead be adopted with `cxmsg register`.
  A user-private registry under `~/.codex/cxmsg/sessions/` maps peer names to
  thread IDs; unregistered Codex threads are not exposed as peers.
- If the target has an active turn, the message is appended with `turn/steer`.
- If the target is idle, a new turn is started with `approvalPolicy: "never"`.
- A routed `--wake-policy when-idle` message is retained instead of steered.
  The Scheduler starts it only after two bounded Idle observations surrounding
  claim acquisition. It still uses `approvalPolicy: "never"`.
- The message body is supplied as `additionalContext` with kind `untrusted`.
- Inline Message Bodies larger than 2 KiB are split on UTF-8 boundaries into ordered
  `additionalContext` fragments. One common envelope carries the sender,
  recipient, route, total byte count, fragment count, and SHA-256 digest; each
  fragment carries only its sequence number and body slice. This avoids repeating
  token-heavy routing metadata while still exposing missing or reordered fragments.
- Message Bodies over 16 KiB and through 256 KiB are written first to the
  owner-only Message Body Store. The injected envelope contains a 2 KiB preview,
  opaque Content Reference, total byte count, and SHA-256 digest. It never
  contains the storage path. Storage uses 8 MiB append-only segments, a 64 MiB
  fail-closed write quota, and no automatic deletion. Existing bodies remain
  readable above the write quota through a separate 256 MiB bounded scan
  ceiling, so quota exhaustion does not strand retained content.
- Each new ordinary Codex Peer Message commits one Logical Message and one
  recipient Delivery to the append-only Delivery Ledger before transport. Its
  body is represented only by byte count, digest, and optional Content
  Reference. Dispatch-attempt and evidence records remain distinct; neither
  `turn_started` nor a peer reply proves durable task completion.
- New Logical Messages pin the sender thread as well as the recipient thread.
  `cxmsg reply` records `replyTo` in the untrusted model envelope and
  `replyToMessageId` in the Ledger, and delivers only when the current sender
  and target invert the original identities exactly.
- Scheduled claim ownership and lease timestamps are concurrency fields, not
  delivery evidence. A missing or expired claim never authorizes manual replay.
- Peer-triggered turns cannot open an approval path. Operations outside the
  target's existing sandbox and permissions fail normally.
- Offline cxmsg-managed threads are resumed before delivery. A register-only
  `stored-or-external` thread is never auto-resumed; attach it explicitly first.

## Delegated jobs

`cxmsg delegate` is intentionally separate from `cxmsg send`:

- `send` is untrusted coordination context and cannot convey user authority.
- `delegate` requires an explicit `grant <sender> <target>` relationship.
- Every job receives a UUID correlation ID stored under
  `~/.codex/cxmsg/jobs/` with mode `0600`.
- Job and registry updates use owner-checked lease locks so concurrent status,
  approval, and completion writes do not overwrite one another.
- The job record binds the logical target thread, execution thread, App Server
  turn ID, task, permission profile, status, error, and final result.
- `wait` polls only the correlated turn until it reaches a terminal state.
- `result` refreshes a running job once or returns its cached terminal result.
- If a delegation worker exits before recording a terminal result, later
  `status`, `wait`, or `result` calls mark the job failed with
  `failureCode: "worker_exited"` instead of leaving it running indefinitely.

### Scheduled Delegation

Use an explicit bounded expiry to queue authorized work until the pinned target
is Idle:

```bash
cxmsg delegate \
  --from coordinator \
  --permissions :workspace \
  --when-idle \
  --expiry <ISO-within-7-days> \
  worker \
  "Run the bounded review after the current turn."
```

The expiry must be in the future and no more than seven days away. `--job-id`
may supply a UUID idempotency key: repeating the exact enqueue is deduplicated,
while changed task or policy data with the same ID is rejected. The same Job ID
then correlates queueing, claim, worker activation, approval, result, and
restart recovery.

Queueing validates the current grant, named permission profile, approval mode,
exact Codex Node, and private Project identity. The Scheduler validates them
again before and after its lease claim, and the Delegation worker performs the
final validation before creating an execution turn. Revocation, a blocked or
missing permission profile, Project mismatch, successor link, or expiry fails
the Job with zero model turns. A Busy target keeps the Job scheduled. An
expired claim can be reclaimed, but only the worker holding the exact current
claim can activate the Job.

Scheduled Delegation remains distinct from a Scheduled Peer Message: it stores
the task only in the owner-private Job, creates no Logical Message or Delivery
Ledger entry, and obtains authority only from the still-valid user-created
grant. `directory execution sync` remains the explicit migration for retained
fork Jobs; it classifies strong Execution Thread evidence without fabricating
ordinary communication history. See
[Scheduled Delegation v1](docs/SCHEDULED_DELEGATION_V1.md).

By default, each delegated job runs in a persistent fork of the target worker
thread. The fork retains the worker's conversation context while keeping
job-specific permission changes and approval policy away from the original
worker. Empty workers without a rollout use a fresh job thread in the same
project instead.

### Execution modes and TUI visibility

The default `--execution fork` keeps the isolation described above. The target
thread remains idle while the fork works, so its ordinary Codex TUI does not
show the delegated turn. `cxmsg status <target>` compensates by reporting
`delegated-working` or `awaiting-approval`, plus active job counts.

Use `--execution fresh` only when the operator explicitly chooses to omit a
long or unavailable source history. It starts an isolated Execution Thread in
the target Project while retaining the named target and source thread as Job
provenance. It does not inherit the target's conversation context and never
promotes the new thread to an addressable peer.

Use `--execution inline` when the delegated request and response must appear in
the target's original TUI and remain in that conversation context:

```bash
cxmsg delegate \
  --from coordinator \
  --permissions :workspace \
  --execution inline \
  --approval relay \
  worker \
  "Implement the agreed change and report the verification."
```

Inline execution requires the original target thread to be idle and does not
support `--mirror`, because the turn already lives in the original context. It
also serializes work on that thread, while fork execution can isolate multiple
job histories.

For fork execution, `--mirror summary` or `--mirror full` starts a separate,
untrusted synchronization turn on the original thread after the job completes.
Mirroring consumes model tokens. It fails rather than steering into unrelated
active work; the correlated job result remains available even if mirroring
fails.

### Approval modes

Delegated CLI jobs run in a detached worker that keeps the initiating App
Server connection alive through turn completion. This is required because App
Server sends approval requests back to the client connection that started the
turn.

- `--approval never` is the backward-compatible default. The turn uses
  `approvalPolicy: "never"`; actions outside its permission profile fail closed.
- `--approval relay` uses `approvalPolicy: "on-request"`, records the job as
  `awaiting_approval`, and waits for `cxmsg approve` or `cxmsg deny`. Set the
  bounded wait with `--approval-timeout <seconds>`; the default is 600 seconds.
- `--approval auto` automatically accepts supported command, file-change, and
  requested-permission prompts for that one explicitly authorized job. Every
  decision is still recorded in the private job file.

Use `auto` only when the user explicitly pre-authorized the complete bounded
task. A delegated agent must not select or upgrade itself to `auto`. Prompt
injection inside repository content can influence which actions the model asks
to run, so prefer `relay` for destructive, external, costly, or unclear work.

Relay and auto also handle selectable `request_user_input` approval prompts
when they expose an unambiguous accept or decline option. Prompts requiring a
typed answer fail explicitly instead of guessing user input.

Claude request grants can select the same approval behavior independently:

```bash
cxmsg claude grant \
  --permissions :danger-full-access \
  --approval relay \
  <claude-session> \
  worker
```

Reissuing a Claude grant rotates its capability token. The new token must be
delivered only to the intended live Claude session.

Omit `--permissions` to inherit the target fork's current permissions. When it
is present, `cxmsg` validates the named profile through
`permissionProfile/list` before dispatch. Common built-in profiles include
`:read-only`, `:workspace`, and `:danger-full-access`, but availability can be
restricted by managed requirements.

Codex Peer Messages are limited to 256 KiB. Bodies through 16 KiB use inline
delivery; larger bodies use a 2 KiB preview and Content Reference. Delegation
tasks and outbound Claude messages remain limited to 16 KiB. Session names are
limited to 64 characters and may contain letters, numbers, `.`, `_`, and `-`.

## Why App Server

OpenAI describes App Server as the long-running owner of Codex Core threads,
thread persistence, streamed events, and bidirectional approval requests. That
makes it a better foundation than an MCP polling mailbox: MCP can expose a send
tool, but it cannot independently wake an idle Codex thread.

Redis Streams remains possible, but on one computer it would duplicate App
Server's durable thread and wake-up responsibilities. It is better reserved for
multi-host fan-out, audit retention, or non-Codex consumers.

The transport remains local. The server listens at
`~/.codex/cxmsg/app-server.sock`, and this client speaks App Server's JSON-RPC
protocol using the required WebSocket upgrade and frames over that socket.
Runtime PID and logs are stored beside the socket with user-only permissions.

## Safety boundary

A peer message carries information, not user authority. `cxmsg` labels the body
as untrusted context and disables approval prompts for peer-started idle turns.
The receiving session still applies its own instructions, sandbox, and tool
policy. Do not use peer messages to relay secrets or bypass a denied action.

Delegation grants are a cooperative policy boundary for processes owned by the
same Unix user, not protection against a malicious local process. Any process
with the same filesystem and socket access can invoke the CLI. Keep the UDS,
registry, and job files user-private, grant only named coordinators, and prefer
`:read-only` or `:workspace` over `:danger-full-access`.

Claude discovery and delivery have the same Unix-user boundary. A same-user
process can create a plausible Claude session record and socket, so verify the
target name, session ID, cwd, and address before sending sensitive text.
Correlated Claude requests additionally require the capability token created by
an explicit grant; do not paste that token into logs, repositories, or unrelated
sessions. A grant cannot exceed its stored permission profile or open an
approval prompt. The bridge emits at most one automatic response per request ID
and does not treat ordinary replies as new requests.

The host relay token is also a same-user capability secret. Do not expose the
relay beyond loopback, copy its state record into a repository, or pass its
token on a command line. Stop it with `cxmsg relay stop` when host fallback is
not needed.

## Development proposals

- [Busy delivery, scheduling, and doctor improvement plan](docs/BUSY_SCHEDULING_DOCTOR.md)
- [Coordination graph and conversation plan](docs/COORDINATION_GRAPH_CONVERSATIONS.md)
- [Prioritized implementation TODO](docs/IMPLEMENTATION_TODO.md)
- [Scheduled Delegation v1](docs/SCHEDULED_DELEGATION_V1.md)
- [Direct Conversation v1](docs/DIRECT_CONVERSATION_V1.md)
- [Group Conversation v1](docs/GROUP_CONVERSATION_V1.md)
- [Team Cast selector plan v1](docs/TEAM_CAST_SELECTOR_V1.md)
- [Retention policy v1](docs/RETENTION_POLICY_V1.md)
- [Doctor JSON schema v1](docs/DOCTOR_SCHEMA_V1.md)
- [Domain language](CONTEXT.md)

## References

- [Unlocking the Codex harness](https://openai.com/ko-KR/index/unlocking-the-codex-harness/)
- [Codex App Server documentation](https://developers.openai.com/codex/app-server/)
- [Codex MCP documentation](https://developers.openai.com/codex/mcp/)
- [Codex App Server protocol README](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md)
- [Claude Code cross-session messaging](https://blakecrosley.com/ko/blog/claude-code-cross-session-messaging)
- [Claude Code session documentation](https://code.claude.com/docs/en/sessions)
