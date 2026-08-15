# Security policy

## Supported versions

This project is experimental. Security fixes are applied to the latest version
on the default branch; older versions are not maintained separately.

## Trust boundary

`cxmsg` is designed for cooperating processes owned by the same local operating
system user. Its Unix sockets, registries, attachment records, grants, and job
records are user-private, but they are not a security boundary against another
malicious process running as that user.

- A peer message is untrusted coordination context, not user authority.
- A delegation grant is a local cooperative policy, not cryptographic caller
  authentication.
- Permission profiles bound what a delegated job can access; they do not bypass
  operating-system permissions or higher-priority instructions.
- Claude request grant tokens are capability secrets. Do not store them in a
  repository, log, issue, or ordinary coordination message.
- Session rollouts and `~/.codex/cxmsg/` runtime state can contain sensitive
  prompts, results, paths, and tokens. Never commit or publish them.

For Codex-to-Codex delivery, the Peer Message Context Projection Module emits a leading `[untrusted-peer]`
header, a bounded display alias, an optional recipient-scoped Reply Handle, and
the body or bounded preview. The marker is descriptive defense in depth, not an
authorization check; only a marker assembled in the header position by cxmsg
has this meaning. Peer-controlled occurrences in a Message Body are ordinary
text. Project, role, stable Node identity, schedule, trigger, expiry, claim,
attempt, digest, and Delivery evidence remain in Route Admission, the Delivery
Ledger, and other owner-private Modules rather than model context.

Reply Handles use 10 Crockford Base32 characters and are unique within a pinned
recipient thread. They are not capability tokens. Resolution requires the
current registered session name and exact recipient thread identity, then uses
the original Ledger record's pinned reverse route. Handles are never silently
reassigned; missing, ambiguous, stale-thread, quarantined, and unpinned records
fail closed. Existing Logical Message UUID replies remain supported for
backward compatibility.

Claude-to-Codex ingress with a valid native session ID binds a Reply Handle to
the stable Claude Node and exact recipient Codex thread. The reverse route
resolves only the same Claude native session ID against a current live endpoint;
it never reinterprets a display name or trusts a retained socket address.
Malformed session IDs are rejected. Legacy frames with no `from-session` get no
Reply Handle and retain their exact reply address as a temporary compatibility
field. That field must not be removed until the sender supplies stable Node
identity. Reply correlation is owner-private and does not collapse transport
ACK, model reply, or task completion into one state.

The Doctor Module is read-only. Default and `--deep` diagnosis must not signal
processes, delete or rewrite records, start model turns, grant authority,
change permission profiles, or answer approvals. An `unknown` or
`sandbox-denied` finding is not permission to weaken a sandbox or perform
cleanup. Doctor output omits message, task, result, error-body, approval-body,
capability-token, and full socket-path data.

The Retention Module remains read-only by default. Its plan uses explicit
cutoffs, fixed minimum ages, bounded reason codes, and owner-private metadata;
it never emits Message Body text or starts model work. An eligible candidate is
not deletion authority. Explicit purge additionally requires the exact plan
digest supplied through `--confirm`, then revalidates all evidence under the
exclusive mutation barrier. Quota exhaustion, Peer Messages, ACKs, Triggers,
replies, and Doctor findings cannot authorize deletion.

Retention-sensitive writers use owner-private leases behind one Retention
Mutation Barrier. Purge acquires its exclusive mutation lease and
drain those writers before replanning. The barrier is ordered before Route,
Job, Message Body, and Delivery Ledger locks; it is released during App Server
dispatch and model work. A writer cannot upgrade itself into a mutation.
Delivery Dedup Tombstones require the exclusive mutation context, accept only
terminal admitted Deliveries without active claims, contain no Message Body,
and permanently reject reuse as either a Logical Message ID or reply target.
Only the Retention Transaction Module creates them after all new generations
are staged and fsynced.

The Retention Transaction Module validates regular-file identity, ownership,
mode, link count, complete JSONL records, record schema, and content digests.
Its generation identity is a SHA-256 over sorted filename, size, and content
digest entries; timestamps and local paths are excluded. Active generations
are moved to an opaque owner-private backup and are never automatically
expired. Audit receipts contain plan and generation digests, counts, backup
identity, and outcome but no body, task, capability, Endpoint, or storage path.

Before Tombstones, an interrupted prepared transaction is marked abandoned and
changes no active generation. From Tombstone creation onward, recovery is
roll-forward and infers state from actual active, staged, and backup generation
digests rather than trusting a phase label alone. Restore verifies the receipt
digest, current transaction head, backup generation, and unchanged active
generation. It preserves the displaced post-purge generation and never removes
Delivery Dedup Tombstones. Thus restored evidence is readable, but its purged
Logical Message IDs and reply targets remain permanently non-wakeable.

Redacted coordination events are stored in an owner-only segmented JSONL set:
one 1 MiB active segment and four retained archives. Rotation uses an
owner-only lock. The event set is operational evidence rather than a complete
conversation history and must not be copied into a repository.

Long Codex Peer Message bodies are stored separately under the owner-only
Message Body Store. Content References expose a message UUID, byte count, and
SHA-256 digest, never a filesystem path. Reads are bounded and digest-verified.
The initial store rejects writes above its quota and does not automatically
purge data. Reads use a separate bounded scan ceiling so write-quota exhaustion
does not make existing content inaccessible. Doctor checks only directory and
segment metadata, quarantine counts, and quota usage; it never parses Message
Bodies. Store segments may contain private coordination text and must never be
committed, published, exposed in web snapshots, or treated as authority.

Route Admission bindings and records are also owner-only runtime state. A
binding is pinned to the currently registered Codex thread and compares a
typed Project and role before context injection; it is routing policy, never a
delegation grant or user approval. Rejected messages retain their full body in
owner-only Quarantine, while CLI and Doctor output expose metadata and digest
only. Quarantine has no automatic release, retry, reroute, wake, cleanup, or
authority path. Logical-message deduplication prevents a second automatic wake
after an attempt is durably marked, including when the first outcome is
uncertain.

New ordinary Codex deliveries use an owner-only append-only Delivery Ledger.
The atomic batch contains redacted Logical Message metadata and one recipient
Delivery before transport; it does not contain the raw Message Body. Attempt
and evidence records are separate, and an uncertain transport result is never
permission to replay. Private segment metadata, bounded record validation,
`O_NOFOLLOW`, fsync, a fail-closed quota, and reserved terminal-evidence space
protect the initial file Adapter. Partial active tails are quarantined before
another append. There is no automatic retention, purge, retry, or repair.
Explicit purge is a separate confirmed Retention transaction and never follows
from quota pressure.
Delivery Ledger files and their quarantine are runtime state and must never be
committed, published, or copied into a web snapshot.

The Delivery Ledger index is an owner-only, rebuildable cache, not delivery or
authorization evidence. One digest-protected shard projects each Logical
Message, while a digest-protected checkpoint pins the bounded active and
quarantine segment manifest. Missing, stale, or incomplete cache evidence
causes a full bounded rebuild from the Ledger; malformed metadata, unsafe file
identity, symlinks, or more than 4,096 projected messages fail closed. Doctor
only compares the cache with Ledger truth and never repairs it implicitly.

An ordinary Peer Message retry is allowed exactly once and only after a
version-pinned App Server error proves the attempted `turn/steer` made no input
queue mutation. The durable evidence records the bounded rejection code and
contract identifier, not the raw server message. The retry reuses the same
Logical Message ID, retained body digest, pinned recipient thread, and
`clientUserMessageId`; it has a one-second minimum delay and expires after ten
minutes. A second proven rejection becomes `failed`. Connection loss, timeout,
unsupported versions, incomplete history, and all other uncertainty become
`unknown`, for which both automatic and explicit retry fail closed.

Scheduled delivery retains the full Message Body in the separate owner-only
Body Store before the Delivery becomes claimable. `when-idle`, `after-turn`,
and `after-job` routes require an explicit expiry no more than seven days away.
Exact turn and Job identities are validated at enqueue; trigger readiness is
checked again before and after claim. Missing or unverifiable evidence is
blocked, not eligible, and an unused claim is released without an attempt.
Trigger completion creates no authority and cannot infer task completion.
Claims use random worker and claim IDs with a 30-second lease; they are
concurrency metadata, never delivery or authorization evidence. The Scheduler
rechecks the pinned target thread after claim acquisition and renews the exact
still-live claim immediately before recording an attempt and calling
`turn/start`. An expired, replaced, or mismatched claim stops the old dispatcher
with zero attempts. A Busy race releases the unused claim. An uncertain result
becomes `unknown` and is not retried automatically. Scheduler stop and stale
worker replacement remain fail-closed when process identity cannot be verified.

Project and Node lifecycle metadata never transfers authority. A Project move
retains one stable Project ID and records the old and new discovery evidence in
owner-private state; default output redacts those paths. Worktree aliases must
share the exact canonical Git common directory. Project merge and split are
unsupported and cannot be inferred from paths, labels, or Git remotes.
Successor relations transfer no role, grant, permission, approval,
Conversation/Cluster membership, Delivery, Job, reply, Endpoint, or
correlation. A scheduled predecessor target is blocked before target access and
again after claim; cxmsg never rewrites it to the successor. Malformed
successor storage also blocks scheduled dispatch rather than being treated as
an empty relation set.

App Server lifecycle notifications have no replay cursor in the pinned 0.147.0
contract. They reduce latency but do not prove delivery, reading, processing,
completion, approval, or authority. The owner-private lifecycle projection
stores only thread state, bounded turn IDs, a local observation sequence, and a
connection epoch. Reconnect catch-up reads one bounded metadata-only page and
never stores turn items or message text. Polling remains the recovery fallback.

The worker heartbeat proves recent loop progress only; it does not prove
process identity, delivery, completion, or authority. A stale heartbeat on an
identity-matched live worker is `stalled` and is never replaced automatically.
Cancellation is allowed only before an attempt and refuses an active unexpired
claim. Scheduler audit events contain bounded IDs, phases, outcomes, and error
codes only—never message bodies, credentials, full paths, worker IDs, or claim
nonces.

A complete malformed Ledger line fails the whole Ledger closed. Doctor may
identify only its segment number and line number; it never emits the record.
An incomplete final line is not committed evidence and is ignored until the
next writer moves the whole partial segment to Ledger quarantine. Doctor never
performs either operation. If corruption is reported, first back up the entire
Ledger; never edit or partially delete evidence in place.

Ledger quota usage is the actual active-plus-quarantine segment size. At 90
percent Doctor warns; at or above 100 percent it fails because new sends are
blocked, while existing evidence remains. Reserved attempt and terminal
evidence reduce effective headroom. There is no supported manual quota reset:
stop new sends, retain a complete backup, and wait for an audited retention or
purge operation rather than deleting or moving segments. Message Body Store
usage is a separate quota on the same filesystem and should be monitored with
the Ledger.

Migration compatibility is explicitly fail-open only for a target with no
Route Admission binding: it accepts legacy unscoped Peer Messages as untrusted
context. Removing a binding restores that compatibility behavior, so a Hermes
or other isolated deployment must inventory and monitor its expected bindings;
Doctor cannot infer a binding that no longer exists. This is not a boundary
against another malicious process running as the same OS user. If a routed
message supplies `sender_role`, the sender must have a matching binding pinned
to its current registered thread or the message is quarantined.

An existing binding path is never equivalent to a missing binding. cxmsg
requires a private owner-controlled regular file with one link and a valid
filename-bound identity schema. Symlinks, broad modes, malformed JSON, stale
Node keys, and incomplete records are `binding_invalid` and fail closed before
App Server access. The same rule yields `sender_binding_invalid` when a routed
sender-role assertion references an invalid sender binding.

Route Admission covers ordinary Codex Peer Messages and ordinary Claude bridge
ingress. User-authorized Delegation, a Claude request validated by a capability
grant, and the bridge's internal correlated terminal-ACK wake are distinct
paths with their own authorization or correlation checks. Their bypass of
ordinary Route Admission does not make routing metadata authoritative.

Node Directory state is owner-only and may contain canonical Project roots,
native Codex thread or Claude session identifiers, mutable aliases, and current
Endpoint addresses. Default list and Doctor output omit Project paths and
Endpoint details; local CLI callers must explicitly request `--paths` or
`--endpoints`. These records must never be committed, published, copied into
web snapshots, or treated as authentication. A private Project UUID prevents a
routing label from silently changing identity, but it remains cooperative
same-user state rather than a boundary against another malicious same-user
process.

Project creation is explicit. Discovery reuses an identity only for the exact
canonical Git common directory or declared non-Git root. cxmsg does not merge
Projects by basename, remote URL, or path similarity and does not infer Node
successors. Endpoint selection accepts a newer generation or refresh of the
same generation and exact Endpoint identity; conflicting equal generations do
not overwrite the selected Endpoint.

Endpoint history is owner-only, bounded to 64 observations and 16 selected
transport kinds per Node, and excluded from default CLI and Doctor output.
Identical repeated observations are coalesced. Compaction retains the latest
successful evidence for each selected transport rather than silently severing
selection provenance. Older and equal-generation conflicting observations are
recorded as rejected evidence while the selected Endpoint remains unchanged.
`--history` is an explicit local disclosure because observations may contain
socket addresses and presentation aliases.

Node removal writes an owner-only reduced Tombstone and removes the live Node
record. Tombstones contain no Endpoint, PID, socket, address, token, message
body, permission profile, or process claim. Their presence blocks automatic
Node resurrection. If a crash leaves live and Tombstone records together,
Doctor reports the conflict but never deletes or selects either record.

Successor links are explicit, same-Project, single-predecessor, and acyclic.
They preserve lifecycle provenance only. They never migrate grants,
permissions, approvals, route roles, Conversation membership, queued work, or
message authority. A new runtime Node must receive each of those relationships
through its owning subsystem and normal validation path.

Execution Thread records are provenance, not identity or authority. New fork
Delegations are classified before model input is delivered, and the Job stores
the classified execution thread ID separately from its addressable target
thread. An Execution Thread cannot collide with a live or Tombstoned Node and
must not be added to the addressable session registry. Its record contains no
task, result, permission, approval, credential, or message body. Explicit
legacy synchronization requires strong retained Job evidence and does not scan
or promote arbitrary App Server threads.

Scheduled Delegation stores timing and claim evidence in the existing private
Job; it does not convert a Trigger into approval or create Peer Message
authority. Enqueue and execution both require the user-created grant. The
Scheduler checks the pinned target thread, Node, Project, permission profile,
execution mode, approval mode, expiry, and successor state before and after
claim, and the worker checks them once more before model input. Revocation or
identity/policy drift fails closed with zero delegated turns.

Only the exact live claim can activate a scheduled Job. A Scheduler crash before
worker activation leaves an expiring claim that may be reclaimed; a stale
worker cannot consume its old claim. After activation the Job is never
automatically re-enqueued, so ambiguous worker startup cannot duplicate model
work. Scheduled Jobs retain the same owner-only task and result protections as
immediate Delegations and never write an ordinary Delivery Ledger record.

Direct Conversation records are owner-private metadata projections. They retain
stable Node keys, Logical Message IDs, ordering, reply references, source kind,
and timestamps, but never copy message bodies, tasks, results, Endpoints,
paths, grants, approvals, or capability tokens. Membership is informational
and cannot authorize a send, bypass Route Admission, wake a Node, grant a role,
or continue a predecessor identity automatically. History commands are bounded
local reads and never inject retained content into a model turn.

An explicit member migration requires the exact stored `successor-of`
relation. It transfers only Conversation continuity; every permission, role,
grant, route, schedule, and Project check remains owned by its original
subsystem. Malformed, linked, non-owner, overly permissive, oversized, or
identity-inconsistent Conversation storage fails closed. Retention preserves
Ledger and body records referenced by Conversation history.

Group Conversation v1 is same-Project and store-only. It validates every
member as a live stable Node before a new membership version or Logical Message
is created, freezes recipients at send time, and starts no model turn. One
owner-private Ledger batch contains all recipient Deliveries. A recipient
failure changes only that Delivery and cannot become whole-group success,
automatic retry, or automatic re-fan-out.

Group records and inbox cursors are owner-only local state. Inbox output is
bounded and metadata-only; it exposes Content References rather than message
bodies and never includes paths, Endpoints, grants, or capability data. Cursor
acknowledgement is presentation state, not evidence that a model read,
processed, replied to, or completed anything. Group membership and messages
remain untrusted coordination and cannot grant, delegate, approve, alter a
permission profile, or cross a Project boundary.

Team Cast selector plans are also non-authoritative owner-private metadata.
Resolution rejects ambiguous identity, cross-Project recipients, Tombstoned
Nodes, Execution Threads, stale route bindings, and Cluster head/snapshot
disagreement before writing a plan. It starts no Delivery or model turn.
Default output redacts recipient Node keys, while each plan retains only stable
identities, selector evidence, digests, timestamps, and a wake-cost ceiling.
Possessing a plan cannot grant, approve, delegate, dispatch, or bypass Route
Admission. See [Team Cast selector plan v1](docs/TEAM_CAST_SELECTOR_V1.md).

Mention selection is explicit metadata, not an `@display-name` parser. It
accepts at most 16 exact stable Node keys already present in an unexpired plan,
rechecks lifecycle and Project identity, and rejects duplicates. The selection
remains a zero-delivery intent: its `mention-wake` label cannot itself wake a
model, invoke a transport, or establish authority.

Team Cast preparation persists the Message Body and the complete recipient
batch before dispatch. Its `prepared` Delivery state is intentionally rejected
by the generic immediate API and ignored by the Scheduler, so batch creation
cannot accidentally become wake-all. Retention treats every prepared recipient
as nonterminal and protects both Ledger and body evidence. Preparation still
conveys no grant, approval, Delegation, permission, read, or completion claim.

Team Cast dispatch requires an explicit command and uses exact stable
Node/session identity. Every pending recipient must pass preflight before the
first attempt; Busy Codex preflight never steers another turn. Each Codex turn
uses approval policy `never` and an untrusted peer envelope. Claude targets are
resolved by exact native session ID, and the sender must map to exactly one
usable bridge even when the Codex thread has multiple display aliases. Direct
UDS denial may use the existing authenticated host relay; it never changes
Route Admission or authority.

A durable attempt prevents retry after a caller crash, while per-recipient
evidence prevents one success from hiding a sibling failure or unknown outcome.
The Ledger rejects a transport that does not match the recipient runtime.
Claude `transport_delivered` records only native frame acceptance and a
correlated Delivery Job ID; ACK, overload retry, model completion, reply, and
task completion remain separate evidence.

Codex App Server and the Claude Code session transport used by this project are
version-sensitive integrations. Pin compatible client versions and retest after
upgrades.

## Reporting a vulnerability

Do not open a public issue containing credentials, tokens, private prompts,
session data, or a working exploit. Use the repository host's private security
advisory feature when available. Include the affected version, impact, minimal
reproduction steps, and any suggested mitigation.
