import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test, { after } from "node:test";

const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-inbound-policy-"));
process.env.CXMSG_STATE_DIR = root;

const {
  INBOUND_POLICIES_DIR,
  INBOUND_POLICY_LOCK_PATH,
  INBOUND_POLICY_TRANSIENT_GRACE_MS,
  evaluateInboundPolicyRecord,
  evaluateInboundPolicy,
  inboundPolicyFilename,
  inboundPolicyState,
  listInboundPoliciesStrict,
  purgeInboundPolicyRecord,
  removeInboundDenyRule,
  upsertInboundDenyRule,
  validInboundPolicyRecord,
} = await import("../src/inbound-policy.js");
const { inspectInboundPolicies } = await import("../src/inspectors.js");

const TARGET = "codex:12345678-1234-4234-8234-123456789abc";
const SECOND_TARGET = "codex:22345678-1234-4234-8234-123456789abc";
const SENDER = "claude:32345678-1234-4234-8234-123456789abc";
const OTHER_SENDER = "codex:42345678-1234-4234-8234-123456789abc";
const PROJECT = "52345678-1234-4234-8234-123456789abc";
const OTHER_PROJECT = "62345678-1234-4234-8234-123456789abc";

after(async () => {
  delete process.env.CXMSG_STATE_DIR;
  await fs.rm(root, { recursive: true, force: true });
});

test("Inbound policy Adapter persists private idempotent bounded rules", async () => {
  const first = await upsertInboundDenyRule({
    targetNodeKey: TARGET,
    selectorKind: "sender-node",
    selectorValue: SENDER,
    now: "2026-08-16T00:00:00.000Z",
  });
  assert.equal(first.changed, true);
  assert.equal(first.policy.revision, 1);

  const duplicate = await upsertInboundDenyRule({
    targetNodeKey: TARGET,
    selectorKind: "sender-node",
    selectorValue: SENDER,
    now: "2026-08-16T00:00:01.000Z",
  });
  assert.equal(duplicate.changed, false);
  assert.equal(duplicate.rule.ruleId, first.rule.ruleId);
  assert.equal(duplicate.policy.revision, 1);

  const [project, unknown] = await Promise.all([
    upsertInboundDenyRule({
      targetNodeKey: TARGET,
      selectorKind: "sender-project",
      selectorValue: PROJECT,
      now: "2026-08-16T00:00:02.000Z",
    }),
    upsertInboundDenyRule({
      targetNodeKey: TARGET,
      selectorKind: "unknown-sender",
      now: "2026-08-16T00:00:02.000Z",
    }),
  ]);
  assert.equal(project.changed, true);
  assert.equal(unknown.changed, true);
  const state = inboundPolicyState(TARGET);
  assert.equal(state.state, "valid");
  assert.equal(state.record.revision, 3);
  assert.equal(state.record.rules.length, 3);
  assert.equal(listInboundPoliciesStrict().length, 1);

  const filename = path.join(INBOUND_POLICIES_DIR, inboundPolicyFilename(TARGET));
  assert.equal(path.basename(filename).includes("12345678"), false);
  assert.equal(
    path.basename(filename),
    `${createHash("sha256").update(TARGET).digest("hex")}.json`,
  );
  assert.equal((await fs.stat(filename)).mode & 0o077, 0);
  assert.equal(path.dirname(INBOUND_POLICY_LOCK_PATH), root);
  assert.notEqual(path.dirname(INBOUND_POLICY_LOCK_PATH), INBOUND_POLICIES_DIR);

  const removed = await removeInboundDenyRule({
    targetNodeKey: TARGET,
    ruleId: first.rule.ruleId,
    now: "2026-08-16T00:00:04.000Z",
  });
  assert.equal(removed.removedRule.ruleId, first.rule.ruleId);
  assert.equal(removed.policy.revision, 4);
  assert.equal(removed.policy.rules.length, 2);
});

test("Inbound policy evaluator separates verified and unknown sender evidence", async () => {
  await upsertInboundDenyRule({
    targetNodeKey: SECOND_TARGET,
    selectorKind: "sender-node",
    selectorValue: SENDER,
    now: "2026-08-16T01:00:00.000Z",
  });
  await upsertInboundDenyRule({
    targetNodeKey: SECOND_TARGET,
    selectorKind: "sender-project",
    selectorValue: PROJECT,
    now: "2026-08-16T01:00:01.000Z",
  });
  const policyState = inboundPolicyState(SECOND_TARGET);

  const nodeDenied = evaluateInboundPolicyRecord({
    targetNodeKey: SECOND_TARGET,
    policyState,
    senderIdentity: { state: "verified", nodeKey: SENDER, projectId: OTHER_PROJECT },
  });
  assert.equal(nodeDenied.decision, "deny");
  assert.equal(nodeDenied.reason, "sender_denied");
  assert.equal(nodeDenied.selectorKind, "sender-node");

  const projectDenied = evaluateInboundPolicyRecord({
    targetNodeKey: SECOND_TARGET,
    policyState,
    senderIdentity: { state: "verified", nodeKey: OTHER_SENDER, projectId: PROJECT },
  });
  assert.equal(projectDenied.decision, "deny");
  assert.equal(projectDenied.reason, "project_denied");

  const differentProject = evaluateInboundPolicyRecord({
    targetNodeKey: SECOND_TARGET,
    policyState,
    senderIdentity: {
      state: "verified",
      nodeKey: OTHER_SENDER,
      projectId: OTHER_PROJECT,
    },
  });
  assert.equal(differentProject.decision, "continue");
  assert.equal(differentProject.reason, "no_match");

  const unverifiable = evaluateInboundPolicyRecord({
    targetNodeKey: SECOND_TARGET,
    policyState,
    senderIdentity: { state: "unverifiable", nodeKey: SENDER },
  });
  assert.equal(unverifiable.decision, "deny");
  assert.equal(unverifiable.reason, "identity_unverifiable");
  assert.equal(unverifiable.ruleId, null);
  assert.equal(unverifiable.selectorKind, null);
  assert.equal(unverifiable.senderNodeKey, null);
  assert.equal(unverifiable.senderProjectId, null);
  assert.equal(unverifiable.failClosed, true);

  const unidentified = evaluateInboundPolicyRecord({
    targetNodeKey: SECOND_TARGET,
    policyState,
    senderIdentity: { state: "unidentified" },
  });
  assert.equal(unidentified.decision, "continue");
  assert.equal(unidentified.reason, "sender_unidentified");
});

test("unknown-sender rule preserves distinct identity reason codes", async () => {
  const target = "codex:72345678-1234-4234-8234-123456789abc";
  await upsertInboundDenyRule({
    targetNodeKey: target,
    selectorKind: "unknown-sender",
    now: "2026-08-16T02:00:00.000Z",
  });
  const policyState = inboundPolicyState(target);
  const unidentified = evaluateInboundPolicyRecord({
    targetNodeKey: target,
    policyState,
    senderIdentity: { state: "unidentified" },
  });
  const unverifiable = evaluateInboundPolicyRecord({
    targetNodeKey: target,
    policyState,
    senderIdentity: { state: "unverifiable", nodeKey: SENDER },
  });
  assert.equal(unidentified.decision, "deny");
  assert.equal(unidentified.reason, "sender_unidentified");
  assert.equal(unverifiable.decision, "deny");
  assert.equal(unverifiable.reason, "sender_unverifiable");
  assert.throws(
    () =>
      evaluateInboundPolicyRecord({
        targetNodeKey: target,
        policyState,
        senderIdentity: { state: "unidentified", nodeKey: SENDER },
      }),
    /cannot claim a Node/,
  );
});

test("runtime evaluator matches the pure record evaluator", () => {
  const senderIdentity = {
    state: "verified",
    nodeKey: SENDER,
    projectId: OTHER_PROJECT,
  };
  assert.deepEqual(
    evaluateInboundPolicy({
      targetNodeKey: SECOND_TARGET,
      senderIdentity,
    }),
    evaluateInboundPolicyRecord({
      targetNodeKey: SECOND_TARGET,
      policyState: inboundPolicyState(SECOND_TARGET),
      senderIdentity,
    }),
  );
});

test("invalid policy evidence denies fail-closed and missing policy continues", () => {
  const missing = evaluateInboundPolicyRecord({
    targetNodeKey: TARGET,
    policyState: { state: "missing", record: null },
    senderIdentity: { state: "unidentified" },
  });
  assert.equal(missing.decision, "continue");
  assert.equal(missing.reason, "no_policy");
  assert.equal(missing.senderNodeKey, null);
  assert.equal(missing.senderProjectId, null);
  assert.equal(missing.failClosed, false);

  const invalid = evaluateInboundPolicyRecord({
    targetNodeKey: TARGET,
    policyState: { state: "invalid", record: null },
    senderIdentity: { state: "verified", nodeKey: SENDER, projectId: PROJECT },
  });
  assert.equal(invalid.decision, "deny");
  assert.equal(invalid.reason, "policy_invalid");
  assert.equal(invalid.senderNodeKey, SENDER);
  assert.equal(invalid.senderProjectId, PROJECT);
  assert.equal(invalid.failClosed, true);
});

test("removing the final rule removes the inactive policy record", async () => {
  const target = "codex:a2345678-1234-4234-8234-123456789abc";
  const added = await upsertInboundDenyRule({
    targetNodeKey: target,
    selectorKind: "unknown-sender",
    now: "2026-08-16T02:30:00.000Z",
  });
  const removed = await removeInboundDenyRule({
    targetNodeKey: target,
    ruleId: added.rule.ruleId,
    now: "2026-08-16T02:30:01.000Z",
  });
  assert.equal(removed.deleted, true);
  assert.equal(removed.policy, null);
  assert.equal(inboundPolicyState(target).state, "missing");
  assert.equal(
    listInboundPoliciesStrict().some((record) => record.targetNodeKey === target),
    false,
  );
});

test("invalid policy records require an exact digest for supported purge", async () => {
  const validFilename = path.join(
    INBOUND_POLICIES_DIR,
    inboundPolicyFilename(TARGET),
  );
  const validDigest = createHash("sha256")
    .update(await fs.readFile(validFilename))
    .digest("hex");
  await assert.rejects(
    purgeInboundPolicyRecord({
      targetNodeKey: TARGET,
      confirmSha256: validDigest,
    }),
    (error) => error?.code === "EINBOUNDPOLICYVALID",
  );

  const target = "codex:b2345678-1234-4234-8234-123456789abc";
  await fs.mkdir(INBOUND_POLICIES_DIR, { recursive: true, mode: 0o700 });
  const filename = path.join(INBOUND_POLICIES_DIR, inboundPolicyFilename(target));
  const bytes = Buffer.from('{"invalid":true}\n');
  await fs.writeFile(filename, bytes, { mode: 0o600 });
  const digest = createHash("sha256").update(bytes).digest("hex");
  await assert.rejects(
    purgeInboundPolicyRecord({
      targetNodeKey: target,
      confirmSha256: "0".repeat(64),
    }),
    (error) => error?.code === "EINBOUNDPOLICYSTALE",
  );
  assert.equal(inboundPolicyState(target).state, "invalid");
  const purged = await purgeInboundPolicyRecord({
    targetNodeKey: target,
    confirmSha256: digest,
    now: "2026-08-16T02:40:00.000Z",
  });
  assert.equal(purged.recordSha256, digest);
  assert.equal(inboundPolicyState(target).state, "missing");
});

test("policy schema rejects duplicate selectors and a 257th rule", () => {
  const createdAt = "2026-08-16T03:00:00.000Z";
  const rules = Array.from({ length: 256 }, (_, index) => ({
    ruleId: `a0000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`,
    selectorKind: "sender-node",
    selectorNodeKey: `codex:b0000000-0000-4000-8000-${index
      .toString(16)
      .padStart(12, "0")}`,
    createdAt,
  }));
  const record = {
    schemaVersion: 1,
    recordType: "inbound-peer-policy",
    targetNodeKey: TARGET,
    revision: 1,
    rules,
    createdAt,
    updatedAt: createdAt,
  };
  assert.equal(validInboundPolicyRecord(record), true);
  assert.equal(
    validInboundPolicyRecord({ ...record, rules: [...rules, {
      ...rules[0],
      ruleId: "c0000000-0000-4000-8000-000000000000",
      selectorNodeKey: "codex:d0000000-0000-4000-8000-000000000000",
    }] }),
    false,
  );
  assert.equal(
    validInboundPolicyRecord({ ...record, rules: [rules[0], { ...rules[0], ruleId: rules[1].ruleId }] }),
    false,
  );
});

test("policy state rejects symlink replacement without reading its target", async () => {
  const outside = path.join(os.tmpdir(), `cxmsg-inbound-outside-${process.pid}`);
  await fs.writeFile(outside, "not policy\n", { mode: 0o600 });
  const filename = path.join(INBOUND_POLICIES_DIR, inboundPolicyFilename(TARGET));
  await fs.rm(filename, { force: true });
  await fs.symlink(outside, filename);
  try {
    const state = inboundPolicyState(TARGET);
    assert.equal(state.state, "invalid");
    assert.equal(await fs.readFile(outside, "utf8"), "not policy\n");
  } finally {
    await fs.rm(filename, { force: true });
    await fs.rm(outside, { force: true });
  }
});

test("policy state rejects hard-linked records", async () => {
  const target = "codex:c2345678-1234-4234-8234-123456789abc";
  const outside = path.join(os.tmpdir(), `cxmsg-inbound-hardlink-${process.pid}`);
  const filename = path.join(INBOUND_POLICIES_DIR, inboundPolicyFilename(target));
  await fs.writeFile(outside, '{"invalid":true}\n', { mode: 0o600 });
  await fs.link(outside, filename);
  try {
    assert.equal(inboundPolicyState(target).state, "invalid");
    assert.equal(await fs.readFile(outside, "utf8"), '{"invalid":true}\n');
  } finally {
    await fs.rm(filename, { force: true });
    await fs.rm(outside, { force: true });
  }
});

test("policy writer rejects a symlink mutation lock without touching its target", async () => {
  const outside = path.join(os.tmpdir(), `cxmsg-inbound-lock-${process.pid}`);
  await fs.writeFile(outside, "unchanged\n", { mode: 0o600 });
  await fs.symlink(outside, INBOUND_POLICY_LOCK_PATH);
  try {
    await assert.rejects(
      upsertInboundDenyRule({
        targetNodeKey: "codex:92345678-1234-4234-8234-123456789abc",
        selectorKind: "unknown-sender",
      }),
      /not owner-private/,
    );
    assert.equal(await fs.readFile(outside, "utf8"), "unchanged\n");
  } finally {
    await fs.rm(INBOUND_POLICY_LOCK_PATH, { force: true });
    await fs.rm(outside, { force: true });
  }
});

test("Doctor foundation reports activation state and redacted identity gaps", async () => {
  const target = "codex:82345678-1234-4234-8234-123456789abc";
  await upsertInboundDenyRule({
    targetNodeKey: target,
    selectorKind: "sender-project",
    selectorValue: PROJECT,
    now: "2026-08-16T04:00:00.000Z",
  });
  const policyFilename = path.join(
    INBOUND_POLICIES_DIR,
    inboundPolicyFilename(target),
  );
  const before = createHash("sha256")
    .update(await fs.readFile(policyFilename))
    .digest("hex");
  const checks = inspectInboundPolicies({ stateDir: root, featureActive: false });
  assert.equal(
    checks.find((check) => check.id === "inbound-policies.activation").errorCode,
    "EINBOUNDPOLICYINACTIVE",
  );
  assert.ok(checks.some((check) => check.errorCode === "EINBOUNDPOLICYTARGET"));
  assert.ok(
    checks.some((check) => check.errorCode === "EINBOUNDPOLICYSENDERPROJECT"),
  );
  const rendered = JSON.stringify(checks);
  assert.doesNotMatch(rendered, /82345678-1234|52345678-1234/);
  assert.equal(
    createHash("sha256").update(await fs.readFile(policyFilename)).digest("hex"),
    before,
  );

  const activeChecks = inspectInboundPolicies({ stateDir: root });
  assert.equal(
    activeChecks.find((check) => check.id === "inbound-policies.activation").status,
    "pass",
  );

  const unexpected = path.join(INBOUND_POLICIES_DIR, "abandoned.tmp");
  await fs.writeFile(unexpected, "partial\n", { mode: 0o600 });
  try {
    assert.ok(
      inspectInboundPolicies({ stateDir: root }).some(
        (check) => check.errorCode === "EINBOUNDPOLICYUNEXPECTED",
      ),
    );
  } finally {
    await fs.rm(unexpected, { force: true });
  }
});

test("policy scans tolerate active artifacts and Doctor reports stale artifacts", async () => {
  const name = `${inboundPolicyFilename(TARGET)}.${randomUUID()}.tmp`;
  const artifact = path.join(INBOUND_POLICIES_DIR, name);
  await fs.writeFile(artifact, "partial\n", { mode: 0o600 });
  try {
    assert.doesNotThrow(() => listInboundPoliciesStrict());
    assert.equal(
      inspectInboundPolicies({ stateDir: root })
        .find((check) => check.id === "inbound-policies.entries")
        .status,
      "pass",
    );
    const stale = new Date(Date.now() - INBOUND_POLICY_TRANSIENT_GRACE_MS - 1_000);
    await fs.utimes(artifact, stale, stale);
    assert.doesNotThrow(() => listInboundPoliciesStrict());
    assert.equal(
      inspectInboundPolicies({ stateDir: root })
        .find((check) => check.id === "inbound-policies.entries")
        .errorCode,
      "EINBOUNDPOLICYTRANSIENTSTALE",
    );
  } finally {
    await fs.rm(artifact, { force: true });
  }
});

test("Doctor inspection honors an explicit stateDir instead of runtime state", async () => {
  const isolated = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-inbound-inspect-"));
  try {
    const checks = inspectInboundPolicies({ stateDir: isolated });
    assert.equal(
      checks.find((check) => check.id === "inbound-policies.activation").status,
      "pass",
    );
  } finally {
    await fs.rm(isolated, { recursive: true, force: true });
  }
});
