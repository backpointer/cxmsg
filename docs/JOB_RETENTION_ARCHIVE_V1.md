# Job retention archive v1

The Job retention archive reduces the active Job working set without deleting
durable evidence. It is an owner-invoked maintenance Interface, not a Scheduler,
retry, completion, permission, approval, or Delegation Interface.

## Selection contract

`plan` is read-only and deterministic. The cutoff must preserve seven days.
Only Jobs in `completed`, `failed`, `expired`, or `cancelled` are candidates.
The following evidence blocks selection:

- nonterminal, `unknown`, ACK-timeout, completion-timeout, or otherwise
  reconcilable lifecycle state;
- unresolved Claude request response delivery;
- an `after-job` Delivery or Team recipient Trigger;
- an active Job's reply correlation;
- an Execution Thread record;
- a Direct or Group Conversation message source.

The plan never contains task or result text, paths, Endpoints, approval bodies,
tokens, grants, or permission contents. Its digest binds the cutoff and the
ordered eligible Job identities, lifecycle metadata, byte sizes, and exact file
digests.

## Mutation and recovery

`archive` requires the exact plan digest before and after acquiring the shared
Retention Mutation Barrier. Ordinary Job, Ledger, Message Body, and Route
writers drain first. Each Job is moved to an owner-private transaction item and
the manifest is durably updated after every move. A terminal receipt commits
the archive. A crash before commit is recovered by roll-forward from the actual
active/archive file location and exact content digest.

The archive store is bounded to 1 GiB and 1,024 transactions. It contains exact
Job records, manifests, and receipts; it is runtime state and must never be
committed. There is no automatic expiry or deletion.

`restore` requires the exact archive ID, rejects an occupied active Job
identity, moves every item back, and records a one-time restore receipt. A
crash during restore also rolls forward. Restore does not replay, wake, retry,
or resume a Job.

## Cross-retention invariant

An archived Job is absent from active runtime lookup. The ordinary Retention
planner nevertheless reads its exact task-body and reply-correlation metadata.
Consequently, archiving a Job cannot make its stored Delegation task or related
Logical Message purge-eligible. Restore is required before active `result`,
`wait`, Scheduler, approval, receipt, or reconciliation Interfaces can address
that Job again.
