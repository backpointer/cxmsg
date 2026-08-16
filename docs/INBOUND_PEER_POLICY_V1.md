# Inbound Peer Message Policy v1

Status: design contract; implementation requires independent review

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
record carries that Project UUID. An absent or invalid sender identity is
`unknown`; it matches only an explicit unknown-sender rule. Names are never
deny selectors.

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
6. Apply the unknown-sender deny when sender identity is not provable.
7. If no deny matches, continue the existing Route Admission decision.

Thus `deny > Route Admission binding match > legacy-unbound`. v1 has no allow
rule, wildcard name, role selector, path selector, regex, precedence override,
or implicit same-Project exception.

## Covered and excluded paths

The policy applies to ordinary Peer Message content regardless of transport:

- direct Codex send and reply;
- Claude-to-Codex ordinary ingress;
- immediate, steering, when-idle, after-turn, and after-job delivery;
- Direct Conversation messages;
- Group Conversation store-only recipients; and
- Team Cast recipient Deliveries.

Transport fallback, host relay, MCP, or UDS does not bypass destination-side
evaluation. Scheduled work is evaluated when created and revalidated under its
pinned recipient identity immediately before dispatch. A newly added deny may
terminally deny queued work; a removed deny never revives an existing denied
Delivery.

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

A matched rule creates one recipient-specific terminal Inbound Denial before
transport, steering, scheduling claim, or model wake. It records only bounded
metadata required for diagnosis and deduplication:

- Logical Message ID and immutable route/fingerprint digest;
- sender and recipient stable Node keys when verified;
- matched selector kind and opaque rule ID;
- Project UUID only when it was independently verified;
- body byte count and SHA-256, but no content reference or body text;
- `admissionState=denied`, `status=denied`, attempt count zero;
- bounded reason code and timestamps.

Inbound Denial is not Route Admission Quarantine. No quarantine file or Message
Body is written solely for a denied recipient. If one Group or Team Logical
Message has both admitted and denied recipients, a shared body may exist for
the admitted recipients, while denied recipient evidence exposes no content
reference and cannot read it.

The exact same Logical Message retry is deduplicated to the prior denial with
zero wake. A changed fingerprint under the same ID is an idempotency conflict.
Removing a rule does not release, retry, replay, reroute, or wake old denials;
a sender must create a new Logical Message ID after policy removal.

Owner-private coordination events may expose the bounded rule ID, selector
kind, `EINBOUNDDENIED`, and `denialOrigin=inbound-policy`. They expose no body,
full path, endpoint, grant token, or environment. Claude native status remains
the compatible reason-free `denied` frame; local evidence carries the cause.

## Storage and mutation contract

Policies live outside the repository in owner-private cxmsg state. Each record
contains a schema version, exact target Node key, monotonically increasing
revision, bounded rule set, and created/updated timestamps. Limits:

- at most 256 rules per recipient Node;
- exact selector kinds only: `sender-node`, `sender-project`, `unknown-sender`;
- one generated UUID rule ID per rule;
- duplicate selector addition is idempotent and returns the existing rule;
- removal requires the exact target and rule ID.

Writers use an owner-only lock, no-follow/type/owner/link/mode validation, an
O_EXCL temporary file, file fsync, atomic rename, and parent-directory fsync.
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
cxmsg inbound denied list [--target <target-session>] [--json]
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
- A policy read error is not permission to deliver.
- No denial can be converted into Store-only, scheduled, retryable, or unknown.

## Doctor and retention

Doctor is read-only and reports:

- unsafe, invalid, duplicate, over-quota, or target-mismatched policy records;
- rules referencing missing or tombstoned Node or Project identity;
- admitted or attempted Deliveries that contradict durable denial evidence;
- denied Deliveries with a body content reference, attempt, claim, turn, wake,
  or quarantine body;
- pending scheduled work now blocked by current policy, without dispatching it.

Doctor never repairs policy, removes a rule, reads Message Body text, retries a
Delivery, or starts a model turn. Any future Repair remains explicitly
allowlisted and cannot add or remove deny rules.

Terminal denial metadata follows the Delivery Ledger retention contract. An
explicit Retention purge must first reserve the Logical Message ID with a
durable Delivery Dedup Tombstone. There is no automatic denial deletion, rule
expiry, quarantine release, or body cleanup.

## Migration and implementation slices

Missing policy means no new deny and preserves current behavior. An existing
unsafe policy fails closed only for its pinned recipient. No route binding,
grant, Conversation membership, Cluster membership, or Project identity is
migrated or inferred.

Implementation should proceed in independently verifiable slices:

1. owner-private policy Adapter, pure evaluator, CLI mutation, and Doctor;
2. direct Codex and Claude ordinary Route Admission integration plus terminal
   metadata-only Ledger denial;
3. Scheduler revalidation and crash/idempotency tests;
4. Group store-only and Team Cast per-recipient integration;
5. trace, graph, and web redacted projections.

No slice may claim completion while an ordinary delivery path can bypass the
policy silently.

## Acceptance tests

1. Exact stable sender Node deny produces zero context injection and attempt.
2. Verified Project deny matches Directory Project UUID, not envelope claim.
3. Name, endpoint, PID, cwd, or role spoofing cannot satisfy or evade a rule.
4. Unknown sender is admitted by default and denied only by the explicit rule.
5. Unsafe or malformed policy fails closed without retaining the body.
6. Deny takes precedence for a legacy-unbound recipient.
7. Exact retry deduplicates to terminal denied; changed fingerprint conflicts.
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
18. Doctor detects denial evidence carrying any attempt, wake, or content ref
    and performs zero mutation.
