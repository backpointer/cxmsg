import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function writeJson(filename, value) {
  writeFileSync(filename, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
}

function runCxmsg(root, args) {
  return spawnSync(process.execPath, [path.resolve("bin/cxmsg.js"), ...args], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_STATE_DIR: root },
  });
}

function stateDigest(directory) {
  const records = [];
  const visit = (current, relative = "") => {
    for (const name of readdirSync(current).sort()) {
      const filename = path.join(current, name);
      const child = relative ? `${relative}/${name}` : name;
      const metadata = statSync(filename);
      if (metadata.isDirectory()) visit(filename, child);
      else records.push({ child, sha256: sha256(readFileSync(filename)) });
    }
  };
  visit(directory);
  return sha256(JSON.stringify(records));
}

function repairRecord(root, {
  transactionId,
  phase,
  completedAt,
  repairKind = "delivery-ledger-index-rebuild",
}) {
  const transaction = path.join(root, "repairs", "transactions", transactionId);
  const receipts = path.join(root, "repairs", "receipts");
  mkdirSync(transaction, { recursive: true, mode: 0o700 });
  mkdirSync(receipts, { recursive: true, mode: 0o700 });
  const planBase = {
    schemaVersion: 1,
    findingId: "delivery-ledger.index.consistency",
    errorCode: "ELEDGERINDEXSTALE",
    repairKind,
    mutationCategory: "rebuildable-delivery-index",
    target: { kind: "delivery-ledger-index" },
    evidenceSha256: "1".repeat(64),
    recoverability: "ledger-truth-and-owner-private-index-backup",
    automatic: false,
  };
  const plan = {
    ...planBase,
    planDigest: sha256(JSON.stringify(planBase)),
  };
  const receipt = {
    schemaVersion: 1,
    transactionId,
    findingId: plan.findingId,
    repairKind,
    planDigest: plan.planDigest,
    status: phase,
    ...(phase === "failed" ? { errorCode: "EREPAIRFAILED" } : {}),
    startedAt: "2025-01-01T00:00:00.000Z",
    completedAt,
  };
  writeJson(path.join(receipts, `${transactionId}.json`), receipt);
  const manifest = {
    schemaVersion: 1,
    transactionId,
    phase,
    plan,
    startedAt: receipt.startedAt,
    updatedAt: completedAt,
  };
  if (phase === "completed") {
    mkdirSync(path.join(transaction, "index"), { mode: 0o700 });
    manifest.backup = { kind: "delivery-ledger-index", files: [] };
    manifest.receiptSha256 = sha256(JSON.stringify(receipt));
  }
  writeJson(path.join(transaction, "manifest.json"), manifest);
}

test("Repair retention plan is deterministic, read-only, and completion-only", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-retention-"));
  try {
    repairRecord(root, {
      transactionId: "12345678-1234-4234-8234-123456789abc",
      phase: "completed",
      completedAt: "2025-01-02T00:00:00.000Z",
    });
    repairRecord(root, {
      transactionId: "22345678-1234-4234-8234-123456789abc",
      phase: "failed",
      completedAt: "2025-01-03T00:00:00.000Z",
    });
    repairRecord(root, {
      transactionId: "32345678-1234-4234-8234-123456789abc",
      phase: "completed",
      completedAt: "2026-02-01T00:00:00.000Z",
    });
    const repairs = path.join(root, "repairs");
    const before = stateDigest(repairs);
    const args = [
      "repair",
      "retention",
      "plan",
      "--before",
      "2026-01-01T00:00:00.000Z",
      "--json",
    ];
    const first = runCxmsg(root, args);
    const second = runCxmsg(root, args);
    assert.equal(first.status, 0, first.stderr);
    assert.equal(second.status, 0, second.stderr);
    const firstPlan = JSON.parse(first.stdout);
    const secondPlan = JSON.parse(second.stdout);
    assert.equal(firstPlan.planDigest, secondPlan.planDigest);
    assert.equal(firstPlan.policy.automaticDeletion, false);
    assert.equal(firstPlan.policy.mutationEnabled, false);
    assert.deepEqual(
      firstPlan.category.eligible.map((candidate) => candidate.transactionId),
      ["12345678-1234-4234-8234-123456789abc"],
    );
    assert.deepEqual(firstPlan.category.blocked, [{
      transactionId: "22345678-1234-4234-8234-123456789abc",
      phase: "failed",
      reason: "noncompleted_repair",
    }]);
    assert.equal(firstPlan.category.retainedByAge, 1);
    assert.ok(firstPlan.category.estimatedBytes > 0);
    assert.equal(stateDigest(repairs), before);
    assert.doesNotMatch(first.stdout, new RegExp(root));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair retention plan rejects a recent cutoff and inconsistent state", () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-retention-bad-"));
  try {
    repairRecord(root, {
      transactionId: "42345678-1234-4234-8234-123456789abc",
      phase: "completed",
      completedAt: "2025-01-02T00:00:00.000Z",
    });
    const recent = runCxmsg(root, [
      "repair",
      "retention",
      "plan",
      "--before",
      new Date().toISOString(),
      "--json",
    ]);
    assert.equal(recent.status, 1);
    assert.match(recent.stderr, /preserve at least 90 days/);

    writeFileSync(
      path.join(root, "repairs", "receipts", "unexpected.tmp"),
      "invalid\n",
      { mode: 0o600 },
    );
    const inconsistent = runCxmsg(root, [
      "repair",
      "retention",
      "plan",
      "--before",
      "2026-01-01T00:00:00.000Z",
      "--json",
    ]);
    assert.equal(inconsistent.status, 1);
    assert.match(inconsistent.stderr, /consistent Repair state/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
