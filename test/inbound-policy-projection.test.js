import assert from "node:assert/strict";
import test from "node:test";

import {
  latestInboundPolicyEvidence,
  summarizeInboundPolicyEvidence,
  traceInboundPolicyEvidence,
  unavailableInboundPolicySummary,
} from "../src/inbound-policy-projection.js";

const ids = {
  message: "11345678-9234-4234-8234-123456789abc",
  delivery: "21345678-9234-4234-8234-123456789abc",
  target: "31345678-9234-4234-8234-123456789abc",
  sender: "41345678-9234-4234-8234-123456789abc",
  project: "51345678-9234-4234-8234-123456789abc",
  rule: "61345678-9234-4234-8234-123456789abc",
};

function policy(overrides = {}) {
  return {
    decision: "continue",
    reason: "no_match",
    targetNodeKey: `codex:${ids.target}`,
    senderIdentityState: "verified",
    senderNodeKey: `claude:${ids.sender}`,
    senderProjectId: ids.project,
    policyRevision: 7,
    policySha256: "a".repeat(64),
    ruleId: null,
    selectorKind: null,
    failClosed: false,
    ...overrides,
  };
}

test("Inbound Policy trace projects ordered bounded evidence without identity material", () => {
  const delivery = {
    inboundPolicy: policy({ reason: "no_policy", policyRevision: null, policySha256: null }),
    attempts: [{ inboundPolicySnapshot: policy() }],
    evidence: [{
      inboundPolicy: policy({
        decision: "deny",
        reason: "sender_denied",
        ruleId: ids.rule,
        selectorKind: "sender-node",
      }),
    }],
  };
  const trace = traceInboundPolicyEvidence(delivery);
  assert.equal(trace.observationCount, 3);
  assert.deepEqual(
    trace.observations.map((item) => item.evidenceSource),
    ["initial-admission", "delivery-attempt", "terminal-evidence"],
  );
  assert.equal(trace.latest.reason, "sender_denied");
  assert.equal(trace.latest.matchedRule, true);
  const encoded = JSON.stringify(trace);
  assert.doesNotMatch(encoded, new RegExp(ids.target.slice(0, 8)));
  assert.doesNotMatch(encoded, new RegExp(ids.sender.slice(0, 8)));
  assert.doesNotMatch(encoded, new RegExp(ids.project.slice(0, 8)));
  assert.doesNotMatch(encoded, new RegExp(ids.rule.slice(0, 8)));
  assert.doesNotMatch(encoded, /policySha256|senderNodeKey|senderProjectId|targetNodeKey|ruleId/);
});

test("Inbound Policy projection does not pass malformed strings through", () => {
  const malicious = policy({
    reason: "secret/path/token",
    senderIdentityState: "claimed-secret",
    selectorKind: "secret-selector",
  });
  const projected = latestInboundPolicyEvidence({ inboundPolicy: malicious });
  assert.deepEqual(projected, {
    projectionState: "invalid",
    evidenceSource: "initial-admission",
    decision: null,
    reason: null,
    senderIdentityState: null,
    selectorKind: null,
    failClosed: null,
    policyRevision: null,
    matchedRule: null,
  });
  assert.doesNotMatch(JSON.stringify(projected), /secret/);
});

test("Inbound Policy web summary aggregates latest recipient outcomes only", () => {
  const records = [{
    logicalMessage: { messageId: ids.message },
    teamDeliveries: [
      {
        inboundPolicy: policy(),
        evidence: [{
          inboundPolicy: policy({
            decision: "deny",
            reason: "identity_unverifiable",
            senderIdentityState: "unverifiable",
            senderNodeKey: null,
            senderProjectId: null,
            ruleId: null,
            selectorKind: null,
            failClosed: true,
          }),
        }],
      },
      { inboundPolicy: policy() },
      { inboundPolicy: policy({ reason: "not-a-reason" }) },
    ],
  }];
  const summary = summarizeInboundPolicyEvidence(records, [{ rules: [{}, {}] }]);
  assert.deepEqual(summary, {
    status: "available",
    configuredTargetCount: 1,
    configuredRuleCount: 2,
    evaluatedRecipientCount: 3,
    deniedRecipientCount: 1,
    continuedRecipientCount: 1,
    invalidEvidenceCount: 1,
    failClosedDenialCount: 1,
    countsByReason: { identity_unverifiable: 1, no_match: 1 },
  });
  assert.equal(unavailableInboundPolicySummary().status, "unavailable");
});
