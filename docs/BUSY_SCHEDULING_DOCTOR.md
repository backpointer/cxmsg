# Busy delivery, scheduling, and doctor improvement plan

- Status: proposal
- Target release: to be determined
- Last updated: 2026-08-14

## Summary

cxmsg already supports two useful but different busy-target behaviors:

- an ordinary Peer Message steers an active Codex turn;
- an authorized Claude request waits for the target Codex thread to become
  idle before starting isolated work.

The remaining gap is a durable, explicit scheduling path shared by Codex and
Claude coordination. A caller should be able to choose immediate steering,
delivery after the current busy period, delivery after a known turn, or
delivery after a correlated Job. The choice must not blur Peer Message and
Delegation authority.

This plan also adds a read-only `doctor` command. Doctor will diagnose App
Server, sockets, bridges, relay, registries, Jobs, permissions, and the future
schedule without starting a model turn or silently repairing state.

The terms in [CONTEXT.md](../CONTEXT.md) are normative for this proposal.

## Goals

1. Preserve immediate steering for ordinary Peer Messages.
2. Add durable delivery after idle, turn completion, or Job completion.
3. Keep Delegation out of unrelated active turns.
4. Recover safely after process restart, connection loss, or missed App Server
   notification.
5. Serialize eligible work per target and make every claim idempotent.
6. Distinguish transport acceptance, task acceptance, and task completion.
7. Provide actionable, privacy-preserving diagnostics through `cxmsg doctor`.
8. Keep diagnosis read-only and Repair explicit.

## Non-goals

- Treating a Peer Message as user consent or a permission grant.
- Interrupting, cancelling, or reprioritizing an active turn by default.
- Replaying `turn/start` after an uncertain connection failure.
- Automatically restarting processes, deleting records, changing permission
  profiles, granting authority, or approving prompts.
- Waking a fully exited Claude process that no longer exposes its native
  session socket.
- Providing a multi-host broker. This design remains same-computer first.

## Current behavior

| Source and operation | Idle target | Busy target | Durable wait |
| --- | --- | --- | --- |
| Codex `send` to Codex | `turn/start` | `turn/steer` | No |
| Claude ordinary message to Codex | `turn/start` | `turn/steer` | No |
| Codex inline `delegate` | Starts authorized turn | Fails | Job records failure |
| Codex fork `delegate` | Starts work in fork | Preflight currently rejects busy source | Job records failure |
| Claude authorized request to Codex | Forks and starts | Polls until idle | Yes, bounded by one shared timeout |
| Codex delivery to Claude | Sends immediately | Sends immediately; Claude controls consumption | Delivery Job only |

The current App Server client handles requests and server approval requests,
but drops ID-less notifications. Therefore busy waiting relies on repeated
`thread/read` calls. Connection closure also fails pending requests without a
reconnect-and-reconcile path.

## Delivery policy

The policy is explicit and operation-specific:

```text
busy + ordinary immediate Peer Message  -> steer the observed active turn
busy + Scheduled Peer Message           -> remain queued until eligible and idle
busy + authorized Delegation            -> queue only when explicitly requested,
                                           otherwise fail; never implicitly steer
```

### Immediate Peer Message

Existing `cxmsg send` behavior remains compatible:

```bash
cxmsg send worker "Use the revised schema when you reach the migration step."
```

If `worker` is Busy, cxmsg steers the message into the observed active turn. If
the turn completes between `thread/read` and `turn/steer`, the first version
returns an explicit delivery error. It does not retry as `turn/start` because
current immediate sends have no durable cxmsg delivery record.

`clientUserMessageId` carries one correlation ID, but App Server deduplication
must be verified against every supported Codex version before cxmsg relies on
it for a cross-method retry. Adding a durable record to immediate sends, or
enabling reconcile-and-start after a steering race, is outside this proposal's
first implementation. This keeps current immediate delivery at one mutating
attempt and avoids an unverified duplicate path.

### Scheduled Peer Message

The initial command forms are:

```bash
cxmsg send --when-idle worker "Review this after your current work."
cxmsg send --after-turn <turn-id> worker "The follow-up input is ready."
# This fires after either successful or failed terminal Job completion.
cxmsg send --after-job <job-id> worker "Use the correlated result."

cxmsg schedule list
cxmsg schedule show <schedule-id>
cxmsg schedule cancel <schedule-id>
```

Semantics:

- `--when-idle` becomes eligible whenever the target is Idle. If the target is
  already Idle, dispatch can begin immediately.
- `--after-turn` requires the exact turn ID to reach a terminal state and then
  waits for the target to be Idle. Before this Trigger ships,
  `cxmsg status <target> --json` must expose the active turn ID and a bounded
  list of recent terminal turn IDs without exposing turn contents.
- `--after-job` requires the exact Job ID to reach any terminal state and then
  waits for the target to be Idle. This includes failed Jobs. A later extension
  may add success-only conditions; the first version does not infer one.
- A missing turn or Job is rejected at enqueue time. If it later becomes
  unverifiable, the schedule becomes `blocked`, not silently eligible.
- Reaching a Trigger never converts delivery into steering. A newly Busy target
  leaves the record queued for its next Idle state.
- Every schedule supports an expiry and explicit cancellation. The default
  expiry should be documented and bounded before release.

### Scheduled Delegation

Delegation keeps a separate command and authority path:

```bash
cxmsg delegate --when-idle --from coordinator worker "Run the scoped review."
```

The existing default remains fail-fast on a Busy source thread. The explicit
`--when-idle` form creates a durable queued Job. Grant, permission profile,
execution mode, and approval policy are validated both when queued and again
immediately before execution. Revocation or policy change while queued fails
the Job without starting a turn.

Fork execution may eventually stop requiring the source thread to remain Idle
after a safe, version-tested App Server fork contract is established. Until
then, the conservative Idle precondition remains.

## Scheduled record and state machine

A schedule record is owner-only, mode `0600`, and includes at least:

- stable schedule/message ID and idempotency key;
- source identity, target identity, and target thread ID;
- operation kind: Peer Message or Delegation reference;
- Trigger kind and exact referenced turn or Job ID;
- status, timestamps, expiry, attempt count, and last bounded error;
- claim owner, claim nonce, and lease expiry;
- final delivery thread/turn/message IDs when known.

Task text remains in the private record and must never appear in web snapshots,
default Doctor output, logs, or error telemetry.

```text
queued
  -> blocked            trigger or target cannot currently be verified
  -> eligible           trigger is satisfied
  -> claimed            one dispatcher owns a bounded lease
  -> delivering
       -> delivered
       -> queued         transient failure or target became Busy
       -> failed         permanent validation or policy failure
  -> expired
  -> cancelled
```

`blocked` is observational and recoverable. Reconciliation can return it to
`queued` or `eligible`. `delivered`, `failed`, `expired`, and `cancelled` are
terminal. A dispatcher that loses its lease must stop; a later dispatcher may
claim the same record using the same idempotency key.

## Ordering and concurrency

- A target has one dispatch lane with FIFO order by `createdAt`, then schedule
  ID as the stable tie-breaker.
- The first version permits one active scheduled dispatch per target.
- Enqueue enforces a fixed per-target queue-depth limit. Exceeding it fails
  before a schedule record is written; expiry is not a substitute for this
  admission limit.
- An earlier record blocked on an unsatisfied explicit Trigger does not block a
  later `when-idle` record. Among records that are eligible together, FIFO is
  preserved.
- Delegation concurrency is separate from ordinary Peer Message delivery. A
  configurable concurrency option is deferred until real workloads establish
  a need.
- Multiple bridge workers may observe the same record, but only one may hold
  its claim lease.

## Turn Lifecycle Module

Scheduling requires a deeper Turn Lifecycle Module rather than more polling in
each command. Its Interface exposes observed Busy/Idle state and terminal turn
events while hiding App Server protocol details from the Scheduler Module.

Primary evidence:

- `turn/started`;
- `turn/completed`;
- `thread/status/changed`.

Fallback evidence:

- bounded `thread/read` reconciliation;
- durable Job state;
- registry and attachment records.

The App Server Adapter must surface ID-less notifications instead of dropping
them. Consumers subscribe through a bounded local Interface; they do not parse
raw JSON-RPC messages independently.

On connection loss, the Module reconnects and reconciles known thread and turn
IDs. It never replays an uncertain `turn/start` or `turn/steer`. If the outcome
cannot be established, the record becomes `blocked` or `unknown` and Doctor
reports it.

Notification support is version-sensitive. The Adapter must capability-check
the pinned Codex CLI and keep reconciliation available when notifications are
absent or missed.

## Scheduler Module

The Scheduler Module owns durable eligibility, claims, ordering, and dispatch.
It uses four narrow Interfaces:

1. a schedule store Interface for atomic create, claim, renew, transition, and
   cancel operations;
2. the Turn Lifecycle Interface for Busy/Idle and terminal-turn evidence;
3. the Job Interface for correlated terminal state;
4. delivery Adapters for Peer Message and Delegation dispatch.

This produces leverage: commands, Claude bridges, a future web view, and Doctor
all share one set of scheduling invariants. It also improves locality because
claim and transition rules are not duplicated across bridge and CLI workers.

The scheduler starts with the managed App Server lifecycle or as a separately
verified worker. Startup always scans nonterminal records and reconciles them
before accepting a new claim. The lifecycle owner and stop behavior are an
explicit pre-implementation decision, not an Implementation detail.

## Claude delivery completion

Codex-to-Claude delivery already distinguishes UDS acceptance from model
completion, but `accepted` needs a complete lifecycle. The recommended contract
is two-stage:

```text
transport_delivered
  -> acknowledged       Claude accepted or queued the request
  -> completed | failed
```

Required changes:

- delivery instructions explicitly permit an optional `accepted` envelope
  followed by exactly one terminal `completed` or `failed` envelope;
- `accepted` establishes a separate bounded completion deadline;
- expiry after `accepted` becomes `completion_timeout`, not an indefinitely
  pending `acknowledged` record;
- source verification applies to both envelopes;
- retries preserve the delivery ID and do not duplicate accepted work;
- a late valid terminal envelope may reconcile a timeout when its source and
  correlation ID still match, while recording that it arrived late.

A Claude peer's Busy state is advisory because cxmsg does not own Claude's turn
lifecycle. Direct delivery may be accepted by its native session socket while
Claude queues or processes it. `transport_delivered` must never be displayed as
task completion.

## Doctor Module

### Commands

```bash
cxmsg doctor
cxmsg doctor --json
cxmsg doctor --target <session-name>
cxmsg doctor --deep
```

Default Doctor is bounded and read-only. `--deep` performs active but
non-mutating handshakes and consistency reads. Neither form sends a Peer
Message, starts or steers a model turn, consumes model tokens, changes a grant,
answers an approval request, signals a process, removes a file, or restarts a
managed process.

### Report model

Text output is concise and redacted. JSON output follows a stable structure:

```json
{
  "schemaVersion": 1,
  "overall": "degraded",
  "checks": [
    {
      "id": "app-server.socket.connect",
      "scope": "app-server",
      "status": "unknown",
      "summary": "Socket exists but this caller cannot connect",
      "verification": "sandbox-denied",
      "errorCode": "EPERM",
      "repairable": false,
      "remediation": "Run doctor from an allowed host context"
    }
  ]
}
```

Check status is one of `pass`, `warn`, `fail`, `unknown`, or `skipped`.
Overall status is:

- `healthy`: all required checks pass; optional checks may be skipped;
- `degraded`: one or more warnings or unknown results, with no confirmed
  functional failure;
- `unhealthy`: at least one confirmed required failure.

Exit codes are stable for automation:

- `0`: healthy;
- `1`: degraded or unhealthy; inspect JSON `overall` and check statuses;
- `2`: invalid invocation or Doctor itself could not construct a report.

Automation must use `--json` and branch on `overall`; exit code 1 intentionally
groups operational warnings with confirmed failures.

### Check catalog

#### Runtime

- Node.js minimum version;
- cxmsg version and package integrity metadata available to the running CLI;
- Codex CLI version against the tested compatibility range;
- required App Server methods and notification capability when `--deep`.

#### State filesystem

- state directories, files, locks, and sockets have expected type, owner, and
  restrictive mode;
- no symlink is followed for a security-sensitive record;
- registry, attachment, grant, delivery, Job, and schedule JSON validates;
- filename identity matches the ID stored in the record;
- malformed records are reported by hashed or shortened identity, never by
  task text or capability token.

#### App Server and Turn lifecycle

- managed PID record, process state, and process identity evidence;
- socket existence, type, owner, mode, parent permissions, and handshake;
- distinction among `running`, `unreachable`, `stopped`, `stale`, `unknown`,
  and `mismatched`;
- supervisor PID and actual listener PID are not assumed equal unless the
  managed-process Interface documents that relationship;
- thread registry entries resolve through `thread/read`;
- duplicate names, duplicate thread IDs, invalid cwd, and
  `canAcceptDirectInput=false` are reported;
- notification loss or reconnect reconciliation failures are visible.

#### Managed processes and transports

- TUI attachment, Claude bridge, host relay, scheduler worker, and delegation
  worker records match their verified identity where observable;
- socket probes preserve `EPERM`, `ENOENT`, `ECONNREFUSED`, and timeout rather
  than reducing all failures to false;
- EPERM with credible socket and registry evidence is `unknown` or
  `unreachable`, never `stopped`;
- bridge target/thread identity and health nonce match the registry;
- relay listener evidence and caller reachability are reported separately;
- relay capability presence and file mode are checked without reading it into
  output.

#### Jobs and approvals

- nonterminal Jobs have a live or reconcilable worker, thread, and turn;
- a missing worker is distinguished from an unverified worker;
- approval waits show age and deadline without approval prompt contents;
- overdue, orphaned, or contradictory Job states are reported;
- Claude `acknowledged` deliveries past their completion deadline are reported;
- terminal Job result and error bodies are never emitted by Doctor.

#### Permissions and grants

- referenced permission profiles exist and are allowed;
- Delegation relationships and Claude grants reference valid targets;
- revoked or rotated grants are distinguishable without printing capability
  tokens;
- Doctor never recommends `:danger-full-access` as a generic repair.

#### Schedules

- Trigger references exist and terminal evidence is consistent;
- expired records are not still claimable;
- claim leases have a valid owner, nonce, and bounded expiry;
- duplicate idempotency keys and contradictory terminal state are detected;
- per-target lanes report the oldest eligible and blocked records;
- Doctor does not dispatch an eligible record as part of diagnosis.

### Privacy and evidence

Default output may include stable public names and shortened IDs needed for
action. It omits:

- task, prompt, message, result, error-body, and approval-body text;
- grant and relay capability tokens or token hints that aid guessing;
- full Claude UDS addresses unless an explicit verbose host-local mode is later
  designed;
- unrelated absolute paths and environment variables.

Evidence that could identify a secret or local address is represented as
`present`, `missing`, a safe state enum, or a short one-way hash. JSON and text
renderers consume the same redacted finding model.

### Repair separation

The first Doctor release has no Repair command. A later release may add
individually named operations such as:

```bash
cxmsg doctor repair <check-id>
```

There will be no broad `--fix` switch. Every Repair must:

1. name one current finding and revalidate it immediately;
2. print the exact mutation category before execution;
3. refuse when process identity, socket identity, ownership, or target is
   unknown;
4. preserve a recoverable backup when practical;
5. never grant authority, approve a prompt, change a permission profile, or
   enable full access;
6. report whether the mutation occurred and rerun the associated check.

Unreachable and sandbox-denied findings are not repairable from inside the
restricted caller. Doctor must recommend an allowed host context rather than
asking the agent to weaken its sandbox.

## Doctor architecture

Doctor is a deep Module: a small read-only Interface hides the complexity of
process evidence, socket probes, App Server handshakes, registry validation,
Job reconciliation, and schedule validation.

Each check Adapter returns a redacted finding. The Doctor Module combines
findings and computes overall state. Text and JSON are renderer Adapters; they
contain no health policy. Existing process-state, socket-probe, registry, Job,
bridge, and relay Implementations remain the source of truth and are deepened
where necessary instead of copied into CLI conditionals.

Doctor must use immutable Inspector Interfaces. It must not call convenience
paths such as Job refresh or stale cleanup when those paths can transition a
record, acquire a mutation lease, rewrite a timestamp, or remove state. When a
current Interface mixes inspection and mutation, it is split before being used
by Doctor.

Repair, if later implemented, uses a separate Interface and separate command
dispatch. This separation prevents a future renderer or `--deep` option from
acquiring mutation authority accidentally.

## Security invariants

1. Peer Message content is always untrusted, whether immediate or scheduled.
2. Scheduling cannot create or expand a grant.
3. Delegation is reauthorized at execution time.
4. A Trigger records timing, not user approval.
5. Unknown identity is never treated as permission to signal, delete, or
   replace a process or record.
6. EPERM is not process absence and not proof of identity.
7. Delivery IDs, claim nonces, and App Server client message IDs are stable
   across safe retries.
8. Connection loss causes reconciliation, not blind replay.
9. Doctor is read-only; Repair is explicit and fail-closed.
10. No diagnostic path emits capability secrets or private conversation text.

## Implementation phases

### Phase 1: read-only Doctor foundation

- split pure Job and Claude-delivery Inspector Interfaces from mutating refresh
  paths, including worker-exit, timeout, and ACK-deadline state transitions;
  the Inspector returns the same evidence without rewriting a record;
- add the finding schema, check registry, text renderer, and JSON renderer;
- reuse existing process and socket evidence;
- cover runtime, state filesystem, App Server, bridge, relay, registry, Job,
  permission, and grant checks;
- document stable exit codes;
- ship without Repair.

### Phase 2: Turn Lifecycle Module

- surface App Server notifications;
- add bounded subscription and reconciliation Interfaces;
- make closed transports reconnectable without replaying a mutation;
- test with the pinned Codex CLI and with notifications disabled.

### Phase 3: durable Scheduler Module

- add mode-`0600` records and atomic claim leases;
- implement `when-idle`, `after-turn`, and `after-job`;
- add list, show, cancel, expiry, restart recovery, and per-target FIFO;
- include schedule checks in Doctor.

### Phase 4: scheduled Delegation and Claude lifecycle

- add explicit `delegate --when-idle` with execution-time reauthorization;
- separate idle-wait deadline from execution deadline for Claude requests;
- formalize `accepted` then terminal ACK and `completion_timeout`;
- add one per-target Claude request lane or another explicitly documented
  concurrency rule.

### Phase 5: selected Repairs

- add only Repairs with strong identity evidence and recoverable behavior;
- require an exact finding ID and revalidation;
- keep restart, signal, record removal, grant, permission, and approval actions
  outside broad automation.

## Acceptance tests

### Busy and scheduling

1. Immediate Peer Message to a Busy Codex target steers the exact observed
   turn.
2. Turn completion racing with steering produces either one successful steer
   or an explicit error; it never falls back to an unverified second mutation.
3. `--when-idle` never steers and delivers once after Idle.
4. `--after-turn` does not deliver for another turn's completion.
5. Status JSON exposes bounded active/recent turn IDs without turn contents.
6. `--after-job` waits for the exact correlation ID and triggers for any
   terminal Job status, including failure.
7. A target becoming Busy after eligibility leaves the schedule queued.
8. Restart during `claimed` delivery causes lease reconciliation and no
   duplicate model turn.
9. FIFO holds among simultaneously eligible records for one target.
10. Enqueue above the per-target depth limit fails without writing a record.
11. Expired and cancelled records cannot be claimed.
12. A queued Delegation with a revoked grant fails before a turn starts.
13. Peer Message scheduling never inherits Delegation permissions.
14. Missed App Server notifications are recovered by bounded reconciliation.
15. Connection loss after uncertain `turn/start` never causes blind replay.

### Claude completion

1. `accepted` followed by `completed` reaches a terminal completed state.
2. `accepted` without terminal ACK reaches `completion_timeout`.
3. Wrong-source accepted or terminal ACK is `ack_rejected`.
4. Retryable 429/529 behavior keeps one correlation ID and a bounded budget.
5. `transport_delivered` is never rendered as model completion.

### Doctor

1. Healthy isolated fixtures return exit code 0 and no private body text.
2. UDS `EPERM` with credible listener evidence reports unreachable/unknown,
   not stopped.
3. `ENOENT`, refused stale socket, timeout, and identity mismatch remain
   distinct findings.
4. Default and `--deep` start zero model turns and mutate zero files.
5. A missing worker and an unverified worker produce different findings.
6. Malformed records are reported without parsing arbitrary paths or following
   symlinks.
7. Capability files are mode-checked without emitting their contents.
8. JSON output conforms to the versioned schema and stable exit-code contract.
9. Eligible schedules are reported but not dispatched.
10. Future Repair refuses unknown identity and reruns its exact check after a
    successful mutation.

## Documentation changes required at implementation time

- add command examples and delivery policy to README;
- add schedule and Doctor state schemas to a versioned reference document;
- update SECURITY with schedule record and Repair trust rules;
- add a migration note for any new runtime directories;
- state the tested Codex and Claude versions for notification and ACK behavior;
- mark every web view as diagnostic only unless a separately authorized
  orchestration Interface is implemented.

## Decisions required before implementation

The following values must be fixed in code, tests, and reference documentation
before Phase 3 begins:

- default and maximum schedule expiry;
- terminal schedule retention and cleanup ownership;
- claim lease duration and renewal interval;
- scheduler start, supervision, and stop ownership, including whether
  `cxmsg server stop` releases active leases while preserving queued records and
  how Doctor reports a missing or intentionally stopped scheduler;
- per-target queue-depth default and maximum, with enqueue-time rejection when
  the configured limit is reached;
- per-target dispatch-lane identity when names are renamed;
- supported Codex CLI version range for notification behavior.

Until these are decided, implementations must not choose silent defaults.

## Recommended first slice

Implement Phase 1 without Repair, then Phase 2 before the Scheduler Module.
Doctor immediately improves supportability using existing evidence, while the
Turn Lifecycle Module removes the polling and reconnect weaknesses that would
otherwise be copied into scheduling. This sequence creates depth and leverage
without changing existing `send` or `delegate` behavior prematurely.
