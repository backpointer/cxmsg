# Inbound Peer Message Policy v1

Status: Slices 1 through 3 implemented behind the inactive cross-path feature
gate; Group, Team, and public mutation remain unavailable

## Purpose

An operator may deny ordinary inbound Peer Messages to one stable recipient
Node from:

- one exact verified sender Node;
- every verified sender Node in one stable Project; or
- senders for which cxmsg cannot prove a stable Node identity.

The policy prevents model-context injection. It is not a permission system,
grant, approval, Delegation control, network firewall, or substitute for Route
Admission identity checks.

## Identity and ownership

The policy owner is one stable recipient Node key. A CLI session alias is only
an input used to resolve and pin that Node key at mutation time. PID, UDS path,
display name, current working directory, runtime endpoint, and role are not
policy identity.

Sender rules use either:

- an exact stable `codex:<thread-id>` or `claude:<session-id>` Node key; or
- an exact Node Directory Project UUID.

A typed envelope's claimed Project or sender role is not proof for a Project
rule. Project matching requires a valid live sender Node whose Directory
record carries that Project UUID. Unknown identity has two closed reason
classes:

- `sender_unidentified`: no stable sender identity was supplied or associated
  with the legacy input; and
- `sender_unverifiable`: a stable identity was supplied but its Directory
  record is missing, unsafe, invalid, unreadable, or tombstoned.

Both classes match the v1 unknown-sender selector, but evidence and Doctor keep
their reasons distinct. If a recipient has any sender-Project rule, an unsafe,
invalid, unreadable, missing, or tombstoned claimed sender identity is denied
fail-closed as `identity_unverifiable`; it cannot bypass Project policy by
becoming ordinary unknown. A valid sender Node in a different Project simply
does not match that Project rule. Names are never deny selectors.

Node successors do not inherit sender or recipient policy. Project Transition
retains the same stable Project UUID and therefore retains Project matching.
Tombstoned selectors remain historical policy data until explicitly removed;
Doctor may report them but must not follow an alias or successor automatically.

## Evaluation order

For every ordinary recipient Delivery, evaluation is deterministic:

1. Resolve the exact pinned recipient Node.
2. Read and validate its owner-private policy.
3. If the policy path exists but is unsafe or invalid, deny fail-closed with
   `policy_invalid`.
4. Apply exact sender-Node deny.
5. Apply verified sender-Project deny.
6. When sender identity is claimed but unverifiable and a sender-Project rule
   exists, deny fail-closed as `identity_unverifiable`.
7. Apply the unknown-sender deny when sender identity is not provable.
8. If no deny matches, continue the existing Route Admission decision.

Thus `deny > Route Admission binding match > legacy-unbound`. v1 has no allow
rule, wildcard name, role selector, path selector, regex, precedence override,
or implicit same-Project exception.

## Covered and excluded paths

The policy applies to ordinary Peer Message content regardless of transport:

- direct Codex send and reply;
- Claude-to-Codex ordinary ingress;
- immediate, steering, when-idle, after-turn, and after-job delivery;
- the one operator-requested Explicit Retry after proven Negative Acceptance;
- Direct Conversation messages;
- Group Conversation store-only recipients; and
- Team Cast recipient Deliveries.

Transport fallback, host relay, MCP, or UDS does not bypass destination-side
evaluation. Scheduled work is evaluated when created and revalidated under its
pinned recipient identity after Scheduler lease renewal and before an attempt
is created. The atomic policy-record read is the dispatch policy linearization
point; its exact revision and digest are retained with the decision. A deny
committed after that snapshot is not retroactive to the already-linearized
attempt and applies to later Logical Messages or later eligible attempts. A
removed deny never revives an existing denied Delivery.

Explicit Retry evaluates policy inside the existing Route mutation critical
section immediately before `beginRetryDelivery`. A matching deny terminally
ends the admitted Delivery without consuming the retry attempt or starting a
turn. This is not deduplication to an earlier denial; it is an explicit
`admitted -> policy_denied` evidence transition.

A crash after policy snapshot but before attempt creation leaves no attempt;
the next claim must renew its lease and evaluate the then-current policy again.
A crash after attempt creation follows the existing unknown-evidence rule and
is never automatically replayed. Reconciliation may strengthen evidence but
cannot dispatch or bypass a new eligible-path policy evaluation.

The Scheduler stores a closed, metadata-only snapshot only when a continuing
decision becomes an actual attempt. A matching deny instead records terminal
`policy_denied` evidence, clears the current claim, preserves the already
retained body, and consumes zero attempts. Expired pre-attempt claims do not
carry policy authority: a replacement worker must evaluate current policy
again.

The following are not ordinary Peer Message admission and remain governed by
their existing separate contracts:

- a correlated user-granted Delegation;
- a correlated Claude request validated against a user-created Claude grant;
- native transport receipts and formal Delivery ACKs; and
- the local correlated wake summarizing a terminal outbound Claude Delivery.

An ordinary message from a sender that also holds a grant is still ordinary
and can be denied. The grant affects only the separately validated authorized
request path. Inbound policy can never create, expand, revoke, or approve a
grant or permission profile.

## Denial evidence and privacy

A rule matched during initial admission creates one recipient-specific
terminal Inbound Denial before transport, steering, scheduling claim, or model
wake. It records only bounded metadata required for diagnosis and
deduplication:

- Logical Message ID and immutable route/fingerprint digest;
- sender and recipient stable Node keys when verified;
- matched selector kind and opaque rule ID;
- Project UUID only when it was independently verified;
- body byte count and SHA-256, but no content reference or body text;
- `admissionState=denied`, `status=denied`, attempt count zero;
- bounded reason code and timestamps.

Inbound Denial is not Route Admission Quarantine. No quarantine file or Message
Body is written solely for an initially denied recipient. If one Group or Team
Logical Message has both admitted and denied recipients, a shared body may
exist for the admitted recipients, while initially denied recipient evidence
exposes no content reference and cannot project it into that recipient's model
context. This isolation is against recipient model context, not against the
owner of the owner-private state.

Policy may also deny an already admitted Scheduled Delivery or Explicit Retry.
That transition preserves the original `admissionState=admitted`, Logical
Message content reference, retained Message Body, and earlier attempt evidence.
It appends terminal `status=policy_denied` evidence containing no new content
reference, consumes no new attempt, and performs no deletion. Existing content
remains governed only by Retention; policy evaluation never removes it.

The exact same Logical Message retry is deduplicated to the prior denial with
zero wake. A changed fingerprint under the same ID is an idempotency conflict.
Removing a rule does not release, retry, replay, reroute, or wake old denials;
a sender must create a new Logical Message ID after policy removal.

Codex sender-visible output is indistinguishable from the existing generic
Route Admission rejection; it does not reveal whether a rule, identity error,
or Project selector matched. Owner-private coordination events may expose the
bounded rule ID, selector kind, `EINBOUNDDENIED`, and
`denialOrigin=inbound-policy`. They expose no body, full path, endpoint, grant
token, or environment. Claude native status remains the compatible reason-free
`denied` frame; local evidence carries the cause.

## Storage and mutation contract

Policies live outside the repository in owner-private cxmsg state. Each record
contains a schema version, exact target Node key, monotonically increasing
revision, bounded rule set, and created/updated timestamps. Limits:

- at most 256 rules per recipient Node;
- at most 1,024 recipient policy records and 4,096 total rules;
- exact selector kinds only: `sender-node`, `sender-project`, `unknown-sender`;
- one generated UUID rule ID per rule;
- duplicate selector addition is idempotent and returns the existing rule;
- removal requires the exact target and rule ID;
- removing the final rule removes the now-empty policy record;
- an invalid owner-private record can be removed only by an internal operation
  that confirms the exact SHA-256 of its current file bytes.

Writers use an owner-only lock, no-follow/type/owner/link/mode validation, an
O_EXCL temporary file, file fsync, atomic rename, and parent-directory fsync.
They re-read the current revision under the lock and compare the canonical,
key-sorted previous-record digest immediately before replacement; a changed
record fails stale instead of overwriting it. The lock serializes cooperative
writers. The digest comparison is defense against a non-cooperative same-user
writer and is not the serialization mechanism.
The mutation lock lives outside the record directory. Recognized temporary and
deleting names do not count toward record quota or block a writer; Doctor
reports one that remains beyond a bounded grace period. Unexpected names and
unsafe metadata remain fail-closed.
Mutation is unavailable through Peer Message, ordinary reply, Claude frame,
host relay, or the cxmsg messaging MCP surface. A peer request to change policy
is coordination text only and supplies no authority.

Initial CLI shape:

```text
cxmsg inbound deny add <target-session> --sender-node <node-key> [--json]
cxmsg inbound deny add <target-session> --sender-project <project-id-or-label> [--json]
cxmsg inbound deny add <target-session> --unknown-sender [--json]
cxmsg inbound deny remove <target-session> <rule-id> [--json]
cxmsg inbound deny list [<target-session>] [--json]
cxmsg inbound denials list [--target <target-session>] [--json]
```

Project labels are resolved once to a stable UUID. Default output is redacted
and bounded. No command displays a Message Body.

## Group, Team, and scheduling invariants

- A fan-out freezes its original recipient set; denied recipients are not
  silently removed.
- Each recipient receives an independent admitted or denied Delivery outcome.
- Partial denial is never collapsed into overall success.
- A message body is persisted only if at least one recipient needs retained
  content; an all-denied fan-out stores metadata only.
- Policy revalidation cannot change the pinned recipient or Trigger.
- A queued Delivery denied before dispatch releases no attempt and consumes no
  model turn.
- After its recorded policy snapshot, a concurrent rule addition is not
  retroactive to that already-linearized attempt.
- A policy read error is not permission to deliver.
- No denial can be converted into Store-only, scheduled, retryable, or unknown.

## Doctor and retention

Doctor is read-only and reports:

- unsafe, invalid, duplicate, over-quota, or target-mismatched policy records;
- rules referencing missing or tombstoned Node or Project identity;
- admitted or attempted Deliveries that contradict durable denial evidence;
- initially denied Deliveries with a body content reference, attempt, claim,
  turn, wake, or quarantine body;
- post-admission denials that add an attempt, delete prior content, or carry a
  content reference inside the denial evidence;
- pending scheduled work now blocked by current policy, without dispatching it;
- policy-bearing recipient Nodes that have a Successor while the Successor has
  no explicit policy, without transferring the policy;
- global policy-record or total-rule quota violations.

Doctor never repairs policy, removes a rule, reads Message Body text, retries a
Delivery, or starts a model turn. Any future Repair remains explicitly
allowlisted and cannot add or remove deny rules.

Terminal denial metadata follows the Delivery Ledger retention contract. An
explicit Retention purge must first reserve the Logical Message ID with a
durable Delivery Dedup Tombstone. There is no automatic denial deletion, rule
expiry, quarantine release, or body cleanup.

Removing a policy configuration record is not deletion of Inbound Denial
Evidence. It changes future admission only. Existing Delivery Ledger evidence,
retained Message Bodies, tombstones, and audit events keep their independent
retention contracts.

## Migration and implementation slices

Missing policy means no new deny and preserves current behavior. An existing
unsafe policy fails closed only for its pinned recipient. No route binding,
grant, Conversation membership, Cluster membership, or Project identity is
migrated or inferred.

Implementation should proceed in independently verifiable slices. The public
mutation CLI and feature activation remain unavailable until every ordinary
path in slices 2 through 4 is integrated, so an intermediate release cannot
create a policy that only some paths enforce:

1. internal owner-private policy Adapter, pure evaluator, schema inspection,
   and Doctor foundations, without public mutation or activation;
2. direct Codex and Claude ordinary Route Admission integration, Explicit
   Retry evaluation, and both initial and post-admission denial evidence;
3. Scheduler revalidation, policy snapshot evidence, and crash/idempotency
   tests;
4. Group store-only and Team Cast per-recipient integration, then public CLI
   mutation and feature activation only after the cross-path gate passes;
5. trace, graph, and web redacted projections.

Intermediate slices are internal foundations, not partial feature completion.
Doctor must report any integration gap. No release may expose policy mutation
or claim feature completion while an ordinary delivery path can bypass the
policy silently.

## Acceptance tests

1. Exact stable sender Node deny produces zero context injection and attempt.
2. Verified Project deny matches Directory Project UUID, not envelope claim.
3. Name, endpoint, PID, cwd, or role spoofing cannot satisfy or evade a rule.
4. Unknown sender is admitted by default and denied only by the explicit rule.
5. Unsafe or malformed policy fails closed without retaining the body.
6. Deny takes precedence for a legacy-unbound recipient.
7. Exact resend of an initially denied Logical Message deduplicates to terminal
   denied; a changed fingerprint conflicts.
8. Rule removal never revives or replays an old denied Logical Message.
9. A rule added after scheduling prevents dispatch with attempt count zero.
10. Direct reply and Claude ingress cannot bypass policy.
11. Correlated ACK wake and valid granted request retain their separate paths.
12. A granted sender's ordinary message remains subject to deny.
13. Group and Team partial denial records every recipient independently.
14. All-denied fan-out persists no Message Body.
15. Policy or denial output contains no body, path, endpoint, token, or raw
    environment data.
16. Peer and MCP messaging cannot mutate policy.
17. Node successor creation does not transfer a rule.
18. Doctor detects invalid initial or post-admission denial evidence and
    performs zero mutation.
19. Initial admit followed by a new deny makes Explicit Retry terminally
    `policy_denied` without consuming its retry attempt or starting a turn.
20. Retry or Scheduled denial preserves an existing retained body, while the
    new denial evidence carries no content reference and performs no deletion.
21. With a sender-Project rule, sender Directory failure denies fail-closed;
    a valid sender in a different Project continues admission evaluation.
22. Unidentified and unverifiable sender evidence use distinct reason codes.
23. A crash after policy snapshot cannot recover by dispatching without a new
    claim and policy evaluation; reconciliation never dispatches.
24. Steering is tested independently from direct start, reply, and Claude
    ingress.
25. Group partial denial does not alter admitted recipients' content or timing.
26. Fan-out policy changes do not alter the frozen recipient set and produce
    recipient-specific decisions at their defined linearization points.
27. A 257th per-recipient rule, 1,025th policy record, or 4,097th global rule
    is rejected; duplicate selector addition returns the same rule ID.
28. Codex sender-visible rejection does not reveal policy match details.
29. Doctor reports recipient Successor policy gaps and never transfers policy.
30. A tombstoned claimed sender does not match sender-Node policy and is
    classified as `sender_unverifiable`.

## Explicit v1 non-goals

- allow rules or allow-over-deny precedence;
- deny-everyone-except selectors;
- wildcard, regex, display-name, role, path, PID, or Endpoint selectors;
- automatic rule transfer, expiry, replay, release, or deletion; and
- recipient-model privacy against the owner of the local cxmsg state.
