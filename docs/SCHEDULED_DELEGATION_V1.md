# Scheduled Delegation v1

This document defines the first durable Scheduled Delegation contract. The
domain terms in [CONTEXT.md](../CONTEXT.md) are normative.

## Scope

The v1 command supports `--when-idle` with an explicit ISO expiry no more than
seven days in the future. Immediate Delegation keeps its existing fail-fast Busy
behavior. `after-turn`, `after-job`, recurring schedules, Group Delegation, and
automatic retry after worker activation are outside this version.

A Scheduled Delegation is one owner-private Job. It is not a Peer Message,
Logical Message, Delivery, Conversation message, grant, or approval. Its task
text is retained only in the Job and is excluded from Doctor, web snapshots,
and ordinary coordination logs.

## Durable identity and idempotency

One Job UUID correlates enqueue, schedule claim, worker activation, approval,
execution turn, result, and restart recovery. `--job-id` permits an exact retry
of enqueue. cxmsg hashes the immutable sender, target, pinned thread, task,
permission, execution, approval, mirror, expiry, Node, and Project fields.

- same Job ID and fingerprint: return the retained Job without another enqueue;
- same Job ID and different fingerprint: fail with an idempotency conflict;
- no Job ID: create one random UUID.

The Job pins the target Codex thread, stable Node key, and private Project ID.
A successor relation never transfers the Job.

## Authority validation

cxmsg validates all of the following at enqueue, before claim, after claim, and
once more in the worker before model input:

- the exact target session still maps to the pinned thread;
- the sender remains in the target's user-created Delegation grant list;
- the execution, approval, and mirror modes remain valid;
- the named permission profile still exists and is allowed;
- the pinned Codex Node is live and belongs to the pinned Project;
- the target cwd still resolves inside that Project;
- the Node has no explicit successor;
- the expiry has not passed.

Any policy or identity mismatch is terminal and starts zero delegated turns.
Transient App Server read failures remain blocked for a later Scheduler pass.

## Claim and crash behavior

The Scheduler selects the oldest Scheduled Delegation per target, waits for an
Idle observation, and writes a bounded lease claim. It revalidates authority and
Idle state after claim before spawning the worker. The worker must atomically
consume that exact claim before it can change the Job from `scheduled` to
`queued`.

- crash before worker spawn: the claim expires and another Scheduler may claim;
- stale or delayed worker: activation fails when its claim no longer matches;
- Busy after claim: release the unused claim with zero attempts;
- successful activation: increment attempt count once and never automatically
  re-enqueue that Job;
- crash after activation: normal Job worker reconciliation marks the Job failed
  rather than starting another turn.

This is at-most-once model execution under startup ambiguity. It intentionally
prefers a visible failed Job over a duplicate delegated turn.

## Retained Job migration

Existing Job files remain Jobs and do not produce Delivery Ledger entries.
`cxmsg directory execution sync` may classify a retained fork only when the Job
contains strong source thread, execution thread, turn, and non-startup state
evidence. It leaves the Job unchanged, skips ambiguous records, and creates no
ordinary message history.
