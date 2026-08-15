# Team Cast selector plan v1

Team Cast selector v1 resolves an explicit selector to one bounded, immutable
recipient plan before any fan-out. It is a resolution gate, not a send command:
creating or reading a plan starts zero model turns, creates no Delivery, and
does not call the Scheduler.

## Selectors

A plan accepts exactly one selector:

- a Direct or Group Conversation UUID;
- a Cluster UUID or routing ID; or
- an exact private Project UUID plus a route role.

The sender must be a live stable Node and a current member of a Conversation or
Cluster selector. Project+role v1 uses only current Codex session records with
an exact stable Node-bound Route Binding. Missing, stale, duplicate, malformed,
or legacy name-only bindings fail closed.

The resolver removes the sender, sorts and deduplicates recipients, and then
requires 1–64 live Nodes in the sender's exact Project. Ambiguous Conversation
or Cluster identity, cross-Project members, Node Tombstones, Execution Threads,
and inconsistent immutable Cluster membership snapshots are rejected before a
plan file is written.

## Frozen evidence

Each owner-private plan records:

- a random or caller-supplied idempotency UUID;
- sender Node and Project identities;
- the normalized selector and its membership version or binding-set digest;
- the exact sorted recipient Node keys and their SHA-256 set digest;
- an upper-bound wake-turn estimate equal to the recipient count; and
- a 15-minute resolution expiry.

Reusing a plan ID with another selector or recipient set is an idempotency
conflict. Expiry does not start delivery and does not silently re-resolve the
selector. A later wake implementation must consume an unexpired exact plan or
create a new explicit plan.

Plan files are mode `0600` under mode `0700` owner-controlled directories.
Normal output redacts recipient Node keys; `--recipients` is an explicit local
inspection option. Plans contain no message body, Endpoint, PID, path, grant,
approval, credential, or capability token.

## CLI

Resolve without delivery:

```bash
cxmsg team resolve --from codex:<uuid> --conversation <uuid> --json
cxmsg team resolve --from codex:<uuid> --cluster <uuid-or-routing-id> --json
cxmsg team resolve --from codex:<uuid> \
  --project <project-uuid> --role reviewer --json
```

Inspect a retained plan:

```bash
cxmsg team plan <plan-uuid> --json
cxmsg team plan <plan-uuid> --recipients --json
```

Resolve explicit mention metadata to a bounded subset of an unexpired plan:

```bash
cxmsg team select-mentions --plan <plan-uuid> \
  --from codex:<uuid> \
  --mention codex:<uuid> \
  --mention claude:<uuid> \
  --json
cxmsg team selection <selection-uuid> --json
```

Mention selection accepts 1–16 exact stable Node keys, rejects duplicates and
identities outside the frozen plan, and rechecks each selected Node's lifecycle
and Project. It stores only an immutable recipient subset and digest. It does
not parse prose or infer `@name` aliases, so a display-name collision cannot
change the recipient. It also starts zero Deliveries and zero turns; the
subsequent multi-recipient Ledger and dispatch gates are not implemented by
this command.

Prepare the Message Body and all recipient-specific Ledger entries without
dispatching them:

```bash
cxmsg team prepare --selection <selection-uuid> \
  --from codex:<uuid> \
  --logical-message-id <uuid> \
  -- "Review handoff pointer abc123"
```

Preparation stores the bounded body by Content Reference, then commits one
`teamCast` Ledger batch containing the exact selected recipients. Every
recipient starts in `prepared` state with zero attempts, evidence, or Scheduler
claims. The generic immediate Delivery API and the Scheduler cannot consume
this state. Retrying the same Logical Message ID and content is idempotent;
changed content, routing evidence, or recipients is a conflict.

This crash-safe preparation seam reserves Ledger quota for later per-recipient
evidence but still reports `deliveryStarted: false`. It is not transport
delivery, model receipt, or task completion.

The JSON result includes `deliveryStarted: false` when resolving. Membership,
role, and plan possession are coordination metadata only; none authorizes work,
grants a permission, approves a prompt, or expands a peer's authority.

Per-recipient mention dispatch, explicit wake-all, scheduled policies, and
digest composition remain separately gated future work.
