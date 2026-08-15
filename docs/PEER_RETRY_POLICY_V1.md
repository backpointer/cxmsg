# Ordinary Peer Message retry policy v1

## Scope

This policy covers only one recipient, immediate Codex-to-Codex Peer Message
Delivery. It does not cover Scheduled Peer Messages, Delegation, Claude
delivery, Quarantine, Group fan-out, or any `unknown` transport result.

Retry is an explicit operator action, never an automatic consequence of an
error, ACK, Peer Message, quota condition, or model response.

## Negative Acceptance contract

The first Delivery attempt becomes `retryable` only when all of these facts are
present:

1. The response is an App Server JSON-RPC invalid-request error.
2. The initialized App Server user agent identifies an audited version.
3. The exact error is known to occur before input-queue mutation in that
   version.
4. The attempt and bounded contract identifier are durably appended to the
   Delivery Ledger.

Policy v1 supports `codex-app-server/0.147.0` for the exact `turn/steer`
rejections `no_active_turn`, `expected_turn_mismatch`,
`non_steerable_review`, and `non_steerable_compact`. The implementation stores
only bounded error codes and the contract identifier, not the raw server error.

Any different version, error code, message shape, timeout, disconnect,
incomplete turn search, or missing response is `unknown`. Negative observation
is not Negative Acceptance.

## Retry invariants

- One Logical Message has at most two Delivery attempts.
- The retry reuses the original Logical Message ID, recipient Delivery,
  recipient thread, route, reply correlation, Message Body digest, Content
  Reference, and App Server `clientUserMessageId`.
- Every admitted ordinary body is retained before the first attempt. Retry
  reads and verifies that body; it does not copy it into another store record.
- Retry has a one-second minimum delay and expires ten minutes after the first
  Negative Acceptance observation.
- Target thread replacement, missing body, digest mismatch, legacy Delivery,
  or changed admission evidence rejects before another attempt.
- A second proven rejection is terminal `failed`.
- An ambiguous second attempt is terminal `unknown` and cannot retry.
- A crash after the second attempt record permits reconciliation only, never a
  third attempt.
- Positive reconciliation may strengthen `dispatching` or `unknown` to
  `turn_started`; a negative or incomplete search never authorizes retry.

## Interface

```bash
cxmsg route retry <logical-message-id> --json
```

The command returns success only for `turn_started`. `failed`, `expired`, and
`unknown` return a nonzero exit status. Backoff and eligibility failures do not
append another attempt.

## Upgrade gate

Supporting another App Server version requires reviewing that version's source
ordering, adding the exact contract to the allowlist, and passing regression
tests proving zero input-queue mutation for every accepted rejection. Similar
text or a matching JSON-RPC code alone is insufficient.
