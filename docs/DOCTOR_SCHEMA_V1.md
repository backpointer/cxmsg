# Doctor report schema v1

`cxmsg doctor --json` emits one redacted, read-only diagnostic report. This
document fixes the version 1 automation contract.

## Report

```json
{
  "schemaVersion": 1,
  "overall": "degraded",
  "deep": false,
  "target": null,
  "checks": []
}
```

- `schemaVersion` is the integer `1`.
- `overall` is `healthy`, `degraded`, or `unhealthy`.
- `deep` records whether active non-mutating handshakes were requested.
- `target` is the requested public cxmsg session name or `null`.
- `checks` is an ordered array of redacted findings.

`healthy` means every required check passed and optional checks were at most
skipped. `degraded` means at least one warning, unknown result, or optional
failure exists without a confirmed required failure. `unhealthy` means at
least one required check confirmed a failure.

## Check

Every check contains:

```json
{
  "id": "app-server.socket.connect",
  "scope": "app-server",
  "status": "unknown",
  "summary": "App Server socket exists but this caller cannot connect",
  "verification": "sandbox-denied",
  "errorCode": "EPERM",
  "repairable": false,
  "remediation": "Run doctor from an allowed host context",
  "required": true
}
```

- `id` is a stable machine-oriented check identifier.
- `scope` groups checks such as `runtime`, `state`, `sessions`, `jobs`,
  `permissions`, `app-server`, `attachments`, `bridges`, `relay`,
  `message-bodies`, `route-bindings`, `route-deliveries`, `delivery-ledger`,
  `quarantine`,
  `directory-projects`, `directory-nodes`, `directory-node-tombstones`,
  `directory-successors`, `directory-execution-threads`, `directory-clusters`,
  `directory-cluster-memberships`, `directory-cluster-tombstones`, or
  `schedules`.
- `status` is `pass`, `warn`, `fail`, `unknown`, or `skipped`.
- `summary` is bounded operator text with no private body data.
- `verification` is bounded evidence such as `metadata`, `registry`,
  `identity`, `handshake`, `app-server`, `sandbox-denied`, or
  `not-requested`. It may be omitted when unavailable.
- `errorCode` is a bounded system or cxmsg code and may be omitted.
- `repairable` is always `false` in the initial read-only release.
- `remediation` may be omitted. It never recommends full access as a generic
  fix.
- `required` controls the `overall` policy. Optional failures still make a
  report degraded; only required failures make it unhealthy.

Bridge implementation findings use `EBRIDGEVERSIONUNKNOWN` when a legacy
running bridge has no implementation revision and `EBRIDGESTALECODE` when its
recorded revision differs from the current executable. Both are warnings:
Doctor reports the need for an explicit bridge restart but never performs it.
The implementation revision is independent of the package version so local or
unreleased code changes cannot be hidden behind an unchanged semantic version.
Maintainers increment the implementation revision whenever the long-running
bridge worker's loaded behavior or health contract changes; changing only the
package version is not a substitute.

Message Body Store findings inspect owner, mode, type, segment size,
Quarantine count, and write-quota usage without parsing Message Body text.
`EMESSAGEBODYPARTIAL` and `EMESSAGEBODYQUOTA` are retained operational
warnings, not permission for Doctor to purge or rewrite a segment.

Route findings validate owner-only metadata, bounded JSON schemas, the binding
filename and registered-thread identity, delivery records, and Quarantine
records. They never emit a quarantined body in the report. `EROUTEIDENTITY`
requires an explicit re-bind of the intended session; `EQUARANTINED` is an
operator-review warning and never authorizes automatic release.

Node Directory findings validate bounded owner-only Project, Project transition,
Node, Tombstone, and successor records; unique routing/discovery identity; Node-to-Project
references; Endpoint generation schemas; and whether an addressable Codex Node
still has a registry record. Tombstones must contain only reduced identity,
Project, safe-label, reason, and time fields. Successor relations must reference
same-Project live or tombstoned Nodes, give each successor at most one
predecessor, and remain acyclic. Findings omit canonical roots, Endpoint
addresses, and native private routing details. `ENODEUNREGISTERED` recommends
an explicit operator lifecycle action but never permits Doctor to delete or
Tombstone a Node. `ENODELIFECYCLE` reports interrupted transitions where live
and tombstoned records coexist; Doctor never chooses either record as truth.
Project transitions must form one unbranched, acyclic chain whose sink matches
the current Project discovery identity. `EPROJECTMOVEINCOMPLETE` warns that a
durable transition exists but the Project head still names its source;
`EPROJECTTRANSITIONAMBIGUOUS` fails on a branch, cycle, disconnected chain, or
head mismatch. Doctor never completes, rolls back, merges, or splits a Project.

Endpoint history findings validate the 64-observation and 16-transport bounds,
schema, chronological ordering, monotonic successful generations, justified
older/conflict rejections, and agreement between the latest successful history
and every selected Endpoint. Legacy Nodes with selected Endpoints but no
history receive `EENDPOINTHISTORYLEGACY`; one subsequent explicit sync imports
their selections as baseline evidence. `EENDPOINTHISTORY` is fail-closed
diagnostic evidence only. Doctor never prints an Endpoint address or rewrites
history.

Execution Thread findings validate bounded Job provenance without reading task
or result bodies. An Execution Thread must not collide with a Node or
Tombstone, appear in the addressable session registry, or share its Job with
another Execution Thread. Its retained fork Job must identify the same source
and execution thread. Optional source Node and Project references must resolve
together. Doctor reports inconsistencies but never registers, deletes, resumes,
or converts an Execution Thread.

Cluster findings validate unique stable and routing identities, mutually
exclusive live/Tombstone lifecycle, bounded sorted Node membership, and a
complete immutable sequence of membership snapshots. Every transition changes
exactly the recorded Node and current membership must match the latest version.
Clusters may intentionally span Projects. Retained snapshots may reference a
live or Tombstoned Node, but an orphaned Cluster snapshot or missing Node
identity fails inspection. Cluster Tombstones contain no members, paths,
endpoints, Conversation state, wake policy, or authority. Doctor never fills a
version gap, changes membership, resurrects a Cluster, or deletes history.
One valid next snapshot beyond a matching live head is reported as optional
`ECLUSTERMEMBERSHIPREDO` with an explicit `directory cluster recover`
remediation; Doctor does not perform the redo. At 1,024 retained membership
versions, `ECLUSTERMEMBERSHIPRETENTION` requests operator policy review without
purging files.

Route Delivery findings pin new records to the registered target thread.
`dispatching` and `unknown` records produce optional `EROUTEUNCONFIRMED`
findings and recommend positive reconciliation, never replay. A legacy record
without the pinned thread reports `EROUTELEGACYIDENTITY`; a replacement target
reports `EROUTETARGETIDENTITY`. Doctor reads no message body and does not call
the App Server reconciliation path itself.

Delivery Ledger findings use the same 30-second reconciliation grace as the
route command. An older `created` Delivery with an attempt and no evidence is
reported as optional `ELEDGERATTEMPTSTALE`; this is a derived observation, not
a new Delivery state. `ELEDGERDUPLICATEIDENTITY` is a required failure when one
Logical Message ID exists in both the Ledger and legacy Route Delivery
storage. `ELEDGERSCHEMA` identifies a complete invalid record by safe segment
number and line number without record contents. `ELEDGERQUOTAWARN` starts at
90 percent of actual segment bytes and required `ELEDGERQUOTA` starts at 100
percent. Doctor performs no reconciliation, retry, archive, purge, or repair.

The rebuildable Delivery Ledger index has separate optional findings.
`ELEDGERINDEXMISSING` means Ledger messages exist without cache evidence;
`ELEDGERINDEXSTALE` covers an invalid checkpoint, a segment-manifest mismatch,
an invalid or missing digest-protected shard, or a message-set mismatch. Doctor
does not rebuild the cache. The explicit remediation is
`cxmsg deliveries rebuild-index` after the underlying Ledger finding has been
reviewed.

Scheduled Delegation Jobs do not require a worker before claim activation.
Doctor validates their bounded schedule and claim schema; it reports
`EDELEGATIONEXPIRED` for an unreconciled expired Job and
`EDELEGATIONCLAIMEXPIRED` for a reclaimable lease. Both are read-only warnings:
Doctor does not claim, activate, expire, fail, or dispatch the Job.

Scheduled Delivery findings remain read-only. `ESCHEDULERDOWN` reports queued
Deliveries without a registered worker, `ESCHEDULERUNVERIFIED` preserves an
`EPERM` process result, and `ESCHEDULECLAIMEXPIRED` identifies a reclaimable
lease without claiming or dispatching it. Invalid scheduler metadata is
`ESCHEDULERSCHEMA`. A live legacy record without heartbeat evidence reports
`ESCHEDULERLEGACY`; a live worker whose heartbeat is older than 15 seconds
reports `ESCHEDULERSTALLED`; a bounded pass failure reports `ESCHEDULERPASS`.
`ETARGETPREDECESSOR` warns that a queued Delivery still targets a Node with an
explicit successor and must be cancelled and recreated for the intended Node.
Doctor never starts or restarts the Scheduler, acquires or releases a claim,
cancels a Delivery, rebuilds the index, reads the retained body, or starts a
model turn.

Exact Trigger findings are also read-only. An `after-turn` record receives a
schema-only check because default Doctor does not call App Server turn history.
For `after-job`, `ETRIGGERJOBMISSING` means the exact referenced owner-only Job
record disappeared, `ETRIGGERJOBSCHEMA` means its bounded metadata is invalid,
and `ETRIGGERBLOCKED` means its durable status is `unknown`. None of these
findings permits manual dispatch or changes the Delivery state.

Consumers must ignore unknown fields. A future incompatible change increments
`schemaVersion`.

## Exit codes

- `0`: `overall=healthy`.
- `1`: `overall=degraded` or `overall=unhealthy`; inspect the JSON fields.
- `2`: invalid invocation or Doctor could not construct a report.

Automation must not infer health from exit code `1` alone.

## Read-only and privacy contract

Default Doctor reads bounded local metadata. `--deep` may connect to the App
Server, Claude bridge, and host relay, issue health handshakes, resolve
permission profiles, and call metadata-only `thread/read`. Neither mode sends
a Peer Message, starts or steers a turn, creates a Job, dispatches scheduled
work, answers an approval, changes a grant, signals a process, deletes a file,
or rewrites runtime state.

The report never contains task, prompt, message, result, error-body, or
approval-body text; capability or relay tokens; full Claude socket addresses;
or unrelated absolute paths. Malformed identities are shortened or hashed.
