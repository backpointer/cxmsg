# Doctor report schema v2

Version 2 extends the redacted, read-only v1 contract. Consumers must continue
to ignore unknown fields.

```json
{
  "schemaVersion": 2,
  "overall": "degraded",
  "operationalOverall": "healthy",
  "historicalOverall": "degraded",
  "deep": false,
  "target": null,
  "checks": []
}
```

- `overall` evaluates every retained check and preserves the v1 meaning.
- `operationalOverall` excludes checks with `historical: true`; CLI exit status
  follows this field.
- `historicalOverall` evaluates only retained historical incidents and is
  `healthy` when none exist.
- A check may include `historical: true`, `observedBytes`, and `limitBytes`.
  Historical evidence remains visible and immutable; this classification does
  not permit replay, retry, deletion, or Repair.
- Runtime log findings use scope `runtime-logs` and inspect only owner, mode,
  type, link count, segment count, and size.
- Healthy Execution Thread checks may be represented by aggregate summary IDs.
  Record-specific finding IDs use digest-derived labels and do not expose the
  native identity.
- `EJOBRETENTION` is an owner-review threshold, not deletion authority.
- Job archive consistency uses scope `job-retention`. A nonterminal archive is
  reported as `EJOBARCHIVEINCOMPLETE`; malformed, unsafe, duplicated, or
  missing pair evidence is a required failure and is never repaired by Doctor.

All check fields, status meanings, privacy boundaries, `--target` behavior, and
read-only guarantees not changed above remain as specified by
[Doctor schema v1](DOCTOR_SCHEMA_V1.md).

Exit codes are:

- `0`: `operationalOverall=healthy`.
- `1`: operational health is `degraded` or `unhealthy`.
- `2`: invalid invocation or report construction failure.
