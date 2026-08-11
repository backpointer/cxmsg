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

Codex App Server and the Claude Code session transport used by this project are
version-sensitive integrations. Pin compatible client versions and retest after
upgrades.

## Reporting a vulnerability

Do not open a public issue containing credentials, tokens, private prompts,
session data, or a working exploit. Use the repository host's private security
advisory feature when available. Include the affected version, impact, minimal
reproduction steps, and any suggested mitigation.
