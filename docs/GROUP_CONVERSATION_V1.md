# Group Conversation v1

Group Conversation v1 provides explicit versioned membership, crash-consistent
recipient fan-out, and an owner-private store-only inbox. It deliberately adds
no group wake policy and no authority.

## Membership

- A Group Conversation has a random stable UUID, a bounded routing label, one
  private Project ID, and 3–65 stable Node members.
- Membership is independent from Cluster membership. Every real add or remove
  appends an ordered immutable snapshot; a duplicate change is idempotent.
- New membership requires live Nodes in the same exact Project. A retained
  Logical Message keeps the membership version and recipient set captured at
  send time, so later changes never rewrite or re-fan-out old messages.
- Membership grants no role, route, transport, wake, approval, permission, or
  Delegation authority.

## Store-only fan-out

A sender must be a live current member. cxmsg records the Conversation sequence
and fixed recipients, stores the body by Content Reference, then appends one
Delivery Ledger batch containing the Logical Message and every recipient
Delivery. The shared Retention Mutation Barrier and Conversation/Ledger lock
order prevent purge from interleaving with preparation. There is no dispatch
step in v1, so all recipient Deliveries start as `scheduled` with
`wakePolicy=store-only`, zero attempts, and no claim.

A crash after Conversation preparation but before the Ledger append can leave
one metadata-only entry with a missing source. Repeating the exact Logical
Message ID reuses its sequence, body digest, membership version, and recipients
and completes the Ledger batch. Changed content or routing is an idempotency
conflict. No recovery path starts a model turn.

Recipient evidence is independent. A bounded terminal `failed`, `expired`, or
`cancelled` event changes only that recipient Delivery. Aggregate inspection
shows per-state counts and the recipient records; it never turns partial
failure into whole-group success or triggers another fan-out.

## Inbox

`cxmsg inbox` is an explicit metadata-only local view. It returns at most 200
entries and fails closed after scanning 16,384 retained Group messages. Each
entry contains the Conversation, sequence, Logical Message ID, sender Node,
membership version, reply/hop metadata, expiry, recipient Delivery state, and
Content Reference—but not body text, paths, Endpoints, or capability data.

Acknowledgement advances a separate per-Node, per-Conversation presentation
cursor. It does not claim that a model read, understood, processed, or completed
the message and does not change Delivery evidence.

```bash
cxmsg conversation group ensure review-team \
  codex:<uuid> codex:<uuid> claude:<uuid>
cxmsg conversation group member add <conversation-id> codex:<uuid>
cxmsg conversation group send <conversation-id> \
  --from codex:<uuid> --expiry <ISO-within-7-days> -- "Review pointer abc123"
cxmsg inbox list claude:<uuid> --json
cxmsg inbox ack claude:<uuid> <conversation-id> <sequence>
```

Group message bodies use the existing 256 KiB maximum. Every message requires
an explicit future expiry no more than seven days away. Replies must reference
an earlier message in the same Group Conversation; the hop count is derived
from that parent and capped at eight. cxmsg never automatically forwards a
Group message or emits an automatic group reply.

## Fixed v1 bounds

- 512 Group Conversations
- 3–65 members and at most 64 recipients per send
- 256 retained membership versions per Group
- 4,096 messages per Group
- 4 MiB maximum Group record
- 16,384 messages scanned and 200 entries returned per inbox query
- 512 Conversation cursors per Node

Mention wake, wake-all, scheduled per-recipient wake, Team Cast selectors,
digest composition, and automatic inbox presentation remain later, separately
gated features.
