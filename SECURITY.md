# Security policy

## Supported versions

This project is experimental. Security fixes are applied to the latest version
on the default branch; older versions are not maintained separately.

## Trust boundary

`cxmsg` is designed for cooperating processes owned by the same local operating
system user. Its Unix sockets, registries, attachment records, grants, and job
records are user-private, but they are not a security boundary against another
malicious process running as that user.

- A peer message is untrusted coordination context, not user authority.
- A delegation grant is a local cooperative policy, not cryptographic caller
  authentication.
- Permission profiles bound what a delegated job can access; they do not bypass
  operating-system permissions or higher-priority instructions.
- Claude request grant tokens are capability secrets. Do not store them in a
  repository, log, issue, or ordinary coordination message.
- Session rollouts and `~/.codex/cxmsg/` runtime state can contain sensitive
  prompts, results, paths, and tokens. Never commit or publish them.

The Doctor Module is read-only. Default and `--deep` diagnosis must not signal
processes, delete or rewrite records, start model turns, grant authority,
change permission profiles, or answer approvals. An `unknown` or
`sandbox-denied` finding is not permission to weaken a sandbox or perform
cleanup. Doctor output omits message, task, result, error-body, approval-body,
capability-token, and full socket-path data.

Redacted coordination events are stored in an owner-only segmented JSONL set:
one 1 MiB active segment and four retained archives. Rotation uses an
owner-only lock. The event set is operational evidence rather than a complete
conversation history and must not be copied into a repository.

Long Codex Peer Message bodies are stored separately under the owner-only
Message Body Store. Content References expose a message UUID, byte count, and
SHA-256 digest, never a filesystem path. Reads are bounded and digest-verified.
The initial store rejects writes above its quota and does not automatically
purge data. Reads use a separate bounded scan ceiling so write-quota exhaustion
does not make existing content inaccessible. Doctor checks only directory and
segment metadata, quarantine counts, and quota usage; it never parses Message
Bodies. Store segments may contain private coordination text and must never be
committed, published, exposed in web snapshots, or treated as authority.

Route Admission bindings and records are also owner-only runtime state. A
binding is pinned to the currently registered Codex thread and compares a
typed Project and role before context injection; it is routing policy, never a
delegation grant or user approval. Rejected messages retain their full body in
owner-only Quarantine, while CLI and Doctor output expose metadata and digest
only. Quarantine has no automatic release, retry, reroute, wake, cleanup, or
authority path. Logical-message deduplication prevents a second automatic wake
after an attempt is durably marked, including when the first outcome is
uncertain.

Migration compatibility is explicitly fail-open only for a target with no
Route Admission binding: it accepts legacy unscoped Peer Messages as untrusted
context. Removing a binding restores that compatibility behavior, so a Hermes
or other isolated deployment must inventory and monitor its expected bindings;
Doctor cannot infer a binding that no longer exists. This is not a boundary
against another malicious process running as the same OS user. If a routed
message supplies `sender_role`, the sender must have a matching binding pinned
to its current registered thread or the message is quarantined.

Route Admission covers ordinary Codex Peer Messages and ordinary Claude bridge
ingress. User-authorized Delegation, a Claude request validated by a capability
grant, and the bridge's internal correlated terminal-ACK wake are distinct
paths with their own authorization or correlation checks. Their bypass of
ordinary Route Admission does not make routing metadata authoritative.

Codex App Server and the Claude Code session transport used by this project are
version-sensitive integrations. Pin compatible client versions and retest after
upgrades.

## Reporting a vulnerability

Do not open a public issue containing credentials, tokens, private prompts,
session data, or a working exploit. Use the repository host's private security
advisory feature when available. Include the affected version, impact, minimal
reproduction steps, and any suggested mitigation.
