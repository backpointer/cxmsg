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

Once a target is bound, an untyped, expired, wrong-Project, wrong-role, or
stale-thread message is stored in owner-only Quarantine before any App Server
turn is started or steered. Inspect redacted metadata only with:

```bash
cxmsg route show worker --json
cxmsg route list --json
cxmsg route reconcile <logical-message-id> --json
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

New ordinary Codex Peer Messages are committed to the owner-only Delivery
Ledger before App Server dispatch. One append record atomically contains the
Logical Message metadata and its single recipient Delivery; later append-only
records distinguish the dispatch attempt from `turn_started` or `unknown`
evidence. The Ledger stores body byte count, digest, and an optional opaque
Content Reference, never the raw body. Legacy `route-deliveries` records remain
readable for reconciliation but new sends do not create them.

The Ledger also supports the first scheduled-delivery policy, `when-idle`.
It retains every scheduled body before enqueue, requires an explicit expiry no
more than seven days away, and starts no turn while the target is Busy:

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

`cxmsg server start` starts the Scheduler with App Server. It can also be
managed with `cxmsg scheduler start|status|stop`. The worker checks the target
again after acquiring a 30-second claim, releases the claim if the target has
become Busy, and records one attempt immediately before `turn/start`. Expired
claims are recoverable after restart. Each target lane is FIFO and accepts at
most 256 pending scheduled Deliveries. A transport result whose mutation is
uncertain becomes `unknown` and is never replayed automatically.

This slice does not yet implement `after-turn`, `after-job`, automatic retry,
retention, purge, group fan-out, or task-completion inference. An ambiguous
dispatch remains `unknown`, and only positive App Server acceptance evidence
may strengthen it to `turn_started`. Storage uses private 8 MiB JSONL
segments, a 64 MiB fail-closed quota with bounded terminal-evidence reserve,
and a 256 MiB hard scan ceiling. Automatic retention and purge remain disabled
until their policy is explicitly selected.

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
new sends and make a complete backup. Do not edit, partially delete, or move
segments; this release has no supported quota recovery until retention and
purge tooling ships. Monitor the Message Body Store on the same volume as a
separate quota consumer.

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

Project, task, role, and payload-type identifiers are 1–128 ASCII safe
characters and may contain letters, digits, `.`, `_`, `:`, or `-`. Codex
session names remain the narrower 1–64 character namespace without `:`.

## Stable Node and Project Directory

Create one explicit local Project identity before synchronizing addressable
sessions. The routing label remains human-readable, while cxmsg generates and
retains a separate private UUID:

```bash
cxmsg directory project ensure hermes /path/to/hermes
cxmsg directory sync --project hermes --codex-only
cxmsg directory sync --project hermes --claude-only
```

Omit the runtime-only flag to synchronize both. Git repositories and their
worktrees are matched by canonical Git common directory; non-Git projects use
the explicitly declared canonical root. Equal basenames, remote URLs, or
similar paths never cause an automatic merge.

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

Fork Delegations and their standalone `thread/start` fallback are classified as
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

Run the bounded Doctor before changing runtime state:

```bash
cxmsg doctor
cxmsg doctor --json
cxmsg doctor --target worker
cxmsg doctor --deep --target worker
```

The default pass uses only passive process, registry, file, socket metadata,
Job, grant, Node Directory, route-binding, Quarantine, bridge, and relay evidence. `--deep`
additionally performs
non-mutating App Server, Claude bridge, and host relay handshakes; resolves
registered threads with `thread/read(includeTurns:false)`; and checks stored
permission profile references. Neither mode sends a peer message, starts or
steers a model turn, answers an approval request, changes a grant, signals a
process, removes a record, or repairs state.

Text output is intended for operators. Automation should use `--json` and the
versioned [Doctor schema](docs/DOCTOR_SCHEMA_V1.md). Exit code `0` means
`healthy`, `1` means `degraded` or `unhealthy`, and `2` means an invalid
invocation or failure to construct a report. Doctor deliberately has no
`--fix` option.

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
thread through `cxmsg attach worker`.

## Local web views

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
`retry_scheduled`, `retryable_error`, `failed`, `ack_rejected`,
`transport_error`, `unreachable`, and `ack_timeout`. A `429` or `529` ACK
schedules exponential backoff with a maximum delay and attempt budget. Retries
preserve the delivery correlation ID and record each transport message ID so
the receiver can avoid duplicating work.

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
state without waking Codex.

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
  `additionalContext` fragments. Every fragment carries the same message ID,
  total byte count, and SHA-256 digest, so a receiver can detect a missing or
  reordered fragment instead of silently accepting a TUI-truncated message.
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
- Scheduled claim ownership and lease timestamps are concurrency fields, not
  delivery evidence. A missing or expired claim never authorizes manual replay.
- Peer-triggered turns cannot open an approval path. Operations outside the
  target's existing sandbox and permissions fail normally.
- Offline stored threads are resumed before delivery.

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

Each delegated job runs in a persistent fork of the target worker thread. The
fork retains the worker's conversation context while keeping job-specific
permission changes and approval policy away from the original worker. Empty
workers without a rollout use a fresh job thread in the same project instead.

### Execution modes and TUI visibility

The default `--execution fork` keeps the isolation described above. The target
thread remains idle while the fork works, so its ordinary Codex TUI does not
show the delegated turn. `cxmsg status <target>` compensates by reporting
`delegated-working` or `awaiting-approval`, plus active job counts.

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
- [Doctor JSON schema v1](docs/DOCTOR_SCHEMA_V1.md)
- [Domain language](CONTEXT.md)

## References

- [Unlocking the Codex harness](https://openai.com/ko-KR/index/unlocking-the-codex-harness/)
- [Codex App Server documentation](https://developers.openai.com/codex/app-server/)
- [Codex MCP documentation](https://developers.openai.com/codex/mcp/)
- [Codex App Server protocol README](https://github.com/openai/codex/blob/main/codex-rs/app-server/README.md)
- [Claude Code cross-session messaging](https://blakecrosley.com/ko/blog/claude-code-cross-session-messaging)
- [Claude Code session documentation](https://code.claude.com/docs/en/sessions)
