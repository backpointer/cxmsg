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
  `message-bodies`, or `schedules`.
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
