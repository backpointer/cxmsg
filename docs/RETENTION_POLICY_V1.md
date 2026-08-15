# Retention policy v1

## Purpose

The Retention Module classifies owner-private Delivery Ledger metadata,
Message Bodies, and Route Admission Quarantine without reading Message Body
text into its plan or starting model work. Retention never creates authority,
changes a Delivery state, retries a message, or resolves Quarantine.

Version 1 first shipped a read-only plan. The Retention Mutation Barrier and
Delivery Dedup Tombstone foundations are now implemented, but automatic
deletion and the purge command remain disabled until segment replacement,
backup restoration, receipts, and crash tests described below are implemented
together.

## Fixed minimum ages

| Category | Minimum retained age | Current mutation policy |
| --- | ---: | --- |
| Delivery Ledger metadata and evidence | 90 days | disabled |
| Message Bodies | 30 days | disabled |
| Route Admission Quarantine | 30 days | disabled |

The caller must always provide an absolute ISO cutoff. There is no implicit
"delete old data" default. Planning `scope=all` uses the longest minimum age,
90 days, so one broad request cannot apply the shorter body policy to Ledger
metadata.

The fixed 64 MiB Ledger and Message Body quotas remain fail-closed. Reaching a
warning or hard limit does not authorize automatic purge; new writes continue
to warn or fail according to the existing store policy, and retained reads
remain available.

## Evidence protection

Age alone never makes a record purgeable. The read-only plan protects:

- every nonterminal Delivery, including `created`, `scheduled`, and `unknown`;
- every Delivery rejected into Route Admission Quarantine until its separate
  Quarantine record is handled by one transaction;
- both sides of a reply chain;
- Logical Message IDs referenced by a retained cross-runtime Delivery Job;
- any future Conversation or Group fan-out reference before those Modules add
  their retention adapters.

`unknown` is ambiguous transport evidence, not an old terminal result. It is
never made purgeable by time and never authorizes replay.

An orphan Message Body with no Ledger or Job reference may be listed as a body
candidate after the body minimum age. Planning does not delete it.

## Read-only Interface

```bash
cxmsg retention plan \
  --before 2026-01-01T00:00:00Z \
  --scope all \
  --json
```

Scopes are `all`, `ledger`, `bodies`, and `quarantine`. Text output contains
only counts and estimated bytes. JSON output may contain owner-private Logical
Message IDs and bounded reason codes, but never Message Body text, task text,
credentials, capability tokens, full environment data, or storage paths.

The plan reports three outcomes per category:

- `eligible`: old enough and not protected by known evidence;
- `blocked`: old enough but protected, with bounded reason codes;
- `retainedByAge`: newer than the explicit cutoff.

`eligible` means "candidate for a future explicit purge transaction", not
"safe to delete with filesystem commands".

## Required purge transaction

Mutation remains disabled until one implementation provides all of the
following as one coherent slice:

1. Acquire one Retention mutation lock and re-run the exact plan.
2. Reject changed candidate sets, active claims, new reply/Job references,
   malformed segments, symlinks, broad modes, and non-owner files.
3. Write durable Delivery dedup Tombstones before removing Ledger batches so a
   purged Logical Message ID can never wake again.
4. Rewrite append-only segments into a private staging directory and fsync the
   files and transaction manifest.
5. Atomically swap the staging and active segment directories while retaining
   the previous directories under an opaque backup ID.
6. Rebuild the Ledger and Message Body indexes from the new truth.
7. Write an owner-only audit receipt containing policy version, cutoff,
   category counts, candidate-set digests, backup ID, and outcome. It contains
   no bodies or credentials.
8. Provide an explicit restore command that verifies the receipt, backup
   identity, and unchanged active generation before swapping back.
9. Keep automatic backup expiry disabled until backup retention has its own
   explicit policy.

A partial implementation must not expose a `purge` command. In particular,
deleting Message Body or Ledger segment files directly is not an acceptable
Retention Module implementation.

## Mutation barrier and Tombstone foundation

Every Delivery Ledger truth mutation, Message Body append, Job write, and
Route Admission prepare transaction enters the Retention Mutation Barrier
before acquiring its existing store locks. Ordinary writers may run
concurrently. An exclusive Retention mutation first blocks new writers and
then drains existing owner-private writer leases. A writer may not upgrade its
lease to a mutation, and malformed or unverifiable leases fail closed.

The lock order is Retention barrier, Route message lock, Job lock, Message Body
lock, then Delivery Ledger lock. App Server dispatch and model work occur
outside the barrier; only durable prepare and evidence mutations enter it.

Delivery Dedup Tombstones live outside swappable Ledger segment directories.
They require an exclusive Retention mutation, accept only admitted terminal
Deliveries without an active claim, and permanently block reuse of their
Logical Message IDs and reply targets. They contain a digest rather than a
Message Body. Creating Tombstones alone does not delete Ledger records and is
not exposed as a CLI operation.

## Privacy and authority

- Retention plans and receipts are mode `0600` owner-private state.
- Default text output is aggregate and path-free.
- Retention does not inspect or emit Message Body text.
- A Peer Message, ACK, Trigger, reply, Conversation membership, or quota event
  cannot authorize purge.
- Doctor may inspect retention evidence but remains read-only.
- The web view may display redacted plan summaries but cannot execute purge or
  restore.
