import assert from "node:assert/strict";
import test from "node:test";

const retention = await import(`../src/retention.js?test=${Date.now()}`);

const NOW = Date.parse("2026-08-15T00:00:00.000Z");
const BEFORE = "2026-05-01T00:00:00.000Z";
const OLD = "2026-04-01T00:00:00.000Z";
const RECENT = "2026-08-01T00:00:00.000Z";

const ids = {
  eligible: "11345678-1234-4234-8234-123456789abc",
  unknown: "21345678-1234-4234-8234-123456789abc",
  replyTarget: "31345678-1234-4234-8234-123456789abc",
  reply: "41345678-1234-4234-8234-123456789abc",
  job: "51345678-1234-4234-8234-123456789abc",
  recent: "61345678-1234-4234-8234-123456789abc",
  orphanBody: "71345678-1234-4234-8234-123456789abc",
  quarantine: "81345678-1234-4234-8234-123456789abc",
};

function delivery(messageId, state, updatedAt = OLD, replyToMessageId = null) {
  return {
    logicalMessage: {
      messageId,
      from: "sender",
      ...(replyToMessageId ? { replyToMessageId } : {}),
      body: { bytes: 100, sha256: "0".repeat(64), contentRef: null },
      route: null,
      createdAt: updatedAt,
    },
    delivery: {
      target: "target",
      state,
      admissionState: "admitted",
      updatedAt,
      attempts: [],
      evidence: [],
    },
  };
}

function body(messageId, createdAt = OLD, bodyBytes = 100) {
  return {
    contentRef: `cxmsg-message:${messageId}`,
    messageId,
    bodyBytes,
    bodySha256: "1".repeat(64),
    createdAt,
  };
}

test("retention planning protects live, ambiguous, reply, and Job-correlated evidence", () => {
  const ledger = [
    delivery(ids.eligible, "turn_started"),
    delivery(ids.unknown, "unknown"),
    delivery(ids.replyTarget, "turn_started"),
    delivery(ids.reply, "turn_started", OLD, ids.replyTarget),
    delivery(ids.job, "turn_started"),
    delivery(ids.recent, "turn_started", RECENT),
  ];
  const plan = retention.planRetention(
    {
      before: BEFORE,
      scope: "all",
      ledger,
      bodies: [
        body(ids.eligible, OLD, 200),
        body(ids.unknown),
        body(ids.replyTarget),
        body(ids.job),
        body(ids.recent, RECENT),
        body(ids.orphanBody, OLD, 300),
      ],
      quarantine: [
        {
          logicalMessageId: ids.quarantine,
          quarantinedAt: OLD,
          reason: "project_mismatch",
          messageBytes: 400,
        },
        {
          logicalMessageId: "91345678-1234-4234-8234-123456789abc",
          quarantinedAt: RECENT,
          reason: "role_mismatch",
          messageBytes: 500,
        },
      ],
      jobs: [
        {
          correlation: {
            kind: "peer-reply",
            logicalMessageId: ids.job,
            replyToMessageId: ids.replyTarget,
          },
        },
      ],
    },
    { now: NOW },
  );

  assert.equal(plan.policy.automaticDeletion, false);
  assert.equal(plan.policy.mutationEnabled, false);
  assert.deepEqual(
    plan.categories.ledger.eligible.map((candidate) => candidate.messageId),
    [ids.eligible],
  );
  assert.match(
    JSON.stringify(plan.categories.ledger.blocked),
    /nonterminal_unknown|reply_chain|job_correlation/,
  );
  assert.equal(plan.categories.ledger.retainedByAge, 1);
  assert.deepEqual(
    plan.categories.bodies.eligible.map((candidate) => candidate.messageId),
    [ids.eligible, ids.orphanBody],
  );
  assert.equal(plan.categories.bodies.estimatedBytes, 500);
  assert.deepEqual(
    plan.categories.quarantine.eligible.map((candidate) => candidate.messageId),
    [ids.quarantine],
  );
  assert.equal(plan.categories.quarantine.retainedByAge, 1);
  assert.doesNotMatch(JSON.stringify(plan), /private body|socket|credential/);
});

test("retention planning enforces minimum ages and has no mutation path", async () => {
  assert.throws(
    () =>
      retention.planRetention(
        { before: "2026-08-01T00:00:00.000Z", scope: "all" },
        { now: NOW },
      ),
    /at least 90 days/,
  );
  assert.throws(
    () => retention.planRetention({ before: BEFORE, scope: "unknown" }, { now: NOW }),
    /scope must be/,
  );

  const plan = await retention.buildRetentionPlan(
    { before: BEFORE, scope: "bodies" },
    {
      ledgerReader: async () => [],
      bodyReader: () => [body(ids.orphanBody)],
      quarantineReader: () => {
        throw new Error("unselected private Quarantine should still be metadata-read safe");
      },
      jobReader: () => [],
      now: NOW,
    },
  );
  assert.deepEqual(
    plan.categories.bodies.eligible.map((candidate) => candidate.messageId),
    [ids.orphanBody],
  );
  assert.equal("purge" in retention, false);
});
