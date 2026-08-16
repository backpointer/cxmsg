# Direct Conversation v1

Direct Conversation v1 is an owner-private ordering and history projection for
ordinary cxmsg communication. It does not replace the Delivery Ledger or Job
records, inject retained history into a model, or create authority.

## Identity and membership

- A Direct Conversation is canonical for one unordered pair of stable Node
  keys. Display names, PIDs, paths, sockets, and Endpoint generations do not
  participate in its identity.
- The Conversation ID is deterministically derived from the two original Node
  keys. Reversing sender and recipient therefore selects the same record.
- Original members remain immutable. `currentMembers` may change only through
  an explicit migration backed by the exact `successor-of` relation.
- Membership is coordination metadata. It grants no role, permission,
  approval, Delegation, wake entitlement, or cross-Project access.

## Messages and replies

Each retained entry contains a Logical Message ID, sender and recipient Node
keys, source kind, per-Conversation sequence, optional parent Logical Message
ID, and timestamp. Message bodies, tasks, results, Endpoints, paths, grants,
and capability tokens are not copied into Conversation storage.

Retries reuse the same Logical Message ID and sequence. Reusing an ID with a
different sender, recipient, reply parent, or source kind fails as an
idempotency conflict. Replies reference the parent Logical Message rather than
a transport attempt or recipient Delivery. If the replying pair differs from
the parent's Conversation, the new entry records bounded cross-Conversation
provenance without merging the Conversations.

The Delivery Ledger remains delivery truth for Codex routes. The correlated
Claude Job remains delivery truth for Codex-to-Claude sends. Conversation
history resolves only their current redacted status and never upgrades an ACK,
model reply, or text such as “completed” into durable task completion.

`lastActivityAt`, `lastMessageId`, and `lastSenderNodeKey` are bounded summary
metadata. Activity advances only when a new Logical Message receives a durable
Conversation sequence. Deduplication, retry, dispatch, ACK, receipt, and member
migration do not refresh it. Timestamps are normalized to UTC and advance
monotonically; deterministic presentation sorts parsed epochs descending and
then Conversation IDs ascending. A scheduled message therefore appears at its
durable enqueue time, not its later wake time.

## Storage and failure behavior

Conversation records live in owner-only local state with a single bounded
mutation lock. Writes use a private temporary file, file `fsync`, atomic
rename, and directory `fsync`. Readers reject links, non-owner records,
world/group-accessible records, malformed schemas, non-contiguous sequences,
invalid migration chains, duplicate Logical Message IDs, and storage beyond
the configured bounds.

The Conversation mutation occurs before a new Ledger batch or Claude Job so
the sequence can be embedded in that source record. All participating writers
share the Retention Mutation Barrier. A crash in the narrow interval may leave
a metadata-only Conversation entry whose source status is `missing-source`;
repeating the exact operation reuses that entry and completes the source write.
It never starts a model turn from Conversation storage.

Retention protects Ledger and Message Body records referenced by retained
Conversation entries. Automatic purge is still disabled. Conversation
retention and removal are intentionally not introduced in v1.

## CLI

```bash
cxmsg conversation direct ensure codex <thread-id> claude <session-id>
cxmsg conversation list --json
cxmsg conversation recent codex:<thread-id> --limit 50 --json
cxmsg conversation show <conversation-id> --json
cxmsg conversation history <conversation-id> --limit 50 --json
cxmsg conversation migrate <conversation-id> codex <old-id> codex <new-id>
```

`history` is owner-local, metadata-only, capped at 200 entries per request,
and uses an exclusive `--before <sequence>` cursor. No command wakes a Node or
loads history into a model turn.

`recent` accepts one stable Node key, filters only `currentMembers`, combines
Direct and Group Conversations, and returns at most 200 metadata records. A
Direct record includes its stable peer Node plus a presentation-only current
alias and liveness state. It does not resolve a send target, migrate a member,
or select a replacement reviewer. Unread state is `null` in this first slice
and is not inferred from Delivery counts. A rebuildable owner-private summary
record keeps current membership and the latest activity pointer so the healthy
projection does not load full retained message arrays. Doctor reports missing,
stale, and orphan summaries without silently repairing them. A private file
generation binding and the shared mutation lock make stale summaries fail
closed at query time. Entries whose latest Conversation message lacks its
matching Ledger or Claude Job source are omitted until exact retry completes
the source commit.

JSON output is an envelope containing `conversations`, bounded `diagnostics`,
`complete`, and `hasMore`. Missing-summary diagnosis inspects at most 32
otherwise unindexed records to determine whether they include the requested
Node. Invalid, stale, missing, or source-unverified evidence makes
`complete=false` and the CLI exits nonzero after printing the bounded result;
an older partial list is never presented as complete reviewer continuity.

Inbound Claude frame identities are wire claims. They are never promoted to
Directory Nodes as a side effect of delivery or Conversation recording. Local
Directory synchronization remains the trust boundary for addressable Node
identity.

## Fixed v1 bounds

- 2,048 Direct Conversations
- 4,096 Logical Messages per Conversation
- 32 explicit member migrations per Conversation
- 4 MiB maximum serialized Conversation record
- 200 history entries per CLI request
- 200 recent Conversation entries per CLI request

Group Conversation, inbox presentation, fan-out, Team Cast selection, and
Graph Projection remain separate later phases.
