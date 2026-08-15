# Turn Lifecycle and Scheduler Recovery v1

## Scope

This contract defines cxmsg's metadata-only Turn Lifecycle projection and the
recovery rules for Scheduled Peer Messages. It does not create task authority,
approval, completion evidence, or a durable copy of Codex conversation content.

The audited Codex App Server contract is 0.147.0. It emits
`thread/status/changed`, `turn/started`, and `turn/completed`, but those
notifications contain no replay cursor. The cursors returned by
`thread/turns/list` are pagination cursors, not notification offsets.

## Lifecycle projection

The owner-private `turn-lifecycle.json` projection stores only:

- a local monotonic observation sequence;
- one random connection epoch and connected/disconnected timestamps;
- the initialized App Server semantic version;
- per thread: status, active turn ID, at most eight recent terminal turn IDs,
  observation time, source, and local sequence.

It never stores notification payloads, turn items, prompts, responses, diffs,
paths, endpoints, credentials, or Message Bodies. A malformed existing file is
not silently replaced.

Notifications wake the Scheduler early. They are ephemeral optimization hints,
not durable evidence. After every connection, the Scheduler enumerates at most
256 distinct threads with pending Scheduled Deliveries, reads thread metadata,
and requests exactly one descending page of at most eight turns with
`itemsView=notLoaded`. Failure leaves the prior projection intact. The normal
bounded polling loop remains the correctness fallback.

## Desired state and crash classification

`scheduler.intent.json` records only `running` or `stopped` plus the change
time. The CLI writes `running` before spawning a worker and writes `stopped`
only for an explicit stop. Therefore:

- a verified worker with a fresh heartbeat is `running`;
- a verified worker with a stale heartbeat is `stalled`;
- an absent worker whose desired state is `running` is `crashed`;
- an absent worker whose desired state is `stopped`, or has no legacy intent
  record, is `stopped`;
- an unverifiable process remains `unreachable` and is never signalled or
  replaced automatically.

An unexpected worker exit leaves its worker record and running intent for
diagnosis. Graceful test completion and signal-driven shutdown may remove the
worker record; the controlling CLI records the final stopped intent.

## Queue ordering and claim renewal

Pending records are ordered by Delivery creation time and then Logical Message
ID. Only the earliest record in each pinned target-thread lane may be evaluated
for dispatch. An unready Trigger, live claim, or Busy target blocks later
records in that lane; another target lane remains independent.

A claim uses a random claim ID and worker ID with a 30-second lease. Bounded
metadata reads use the original lease. Immediately before the App Server start
request, the worker must append a renewal event for the same live owner and
then append the Delivery attempt. Renewal:

- fails after expiry or ownership replacement;
- extends the prior lease rather than creating a new claim;
- does not increment the claim acquisition count;
- creates no Delivery, completion, approval, or authority evidence.

If renewal fails, the old dispatcher stops before an App Server request and
records zero attempts. Once an attempt is durable, the record is no longer
claimable; any uncertain transport result follows the existing `unknown`
reconciliation policy and is never automatically replayed.

## Recovery invariants

1. An expired unused claim may be acquired by a new worker after restart.
2. A live claim cannot be renewed, released, or dispatched by another worker.
3. Trigger readiness and target identity are checked both before and after
   claim acquisition.
4. A newly Busy target releases the unused claim with zero attempts.
5. Lifecycle reconnect catch-up starts no model turn and reads no full history.
6. Notification loss may delay dispatch only until the next bounded poll.
7. ACK, model reply, Git result commit, and task completion remain separate
   evidence outside this lifecycle projection.
