import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdtempSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  truncateSync,
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
    assert.equal(firstPlan.policy.mutationEnabled, true);
    assert.equal(firstPlan.policy.mutationKind, "recoverable-archive");
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

    const archived = runCxmsg(root, [
      "repair",
      "retention",
      "archive",
      "--before",
      "2026-01-01T00:00:00.000Z",
      "--confirm",
      firstPlan.planDigest,
      "--json",
    ]);
    assert.equal(archived.status, 0, archived.stderr);
    const archiveReceipt = JSON.parse(archived.stdout);
    assert.equal(archiveReceipt.outcome, "archived");
    assert.equal(archiveReceipt.itemCount, 1);
    const archivedId = "12345678-1234-4234-8234-123456789abc";
    assert.equal(
      existsSync(path.join(root, "repairs", "transactions", archivedId)),
      false,
    );
    assert.equal(
      existsSync(path.join(root, "repairs", "receipts", `${archivedId}.json`)),
      false,
    );
    const archiveDirectory = path.join(
      root,
      "repair-retention",
      "transactions",
      archiveReceipt.archiveId,
    );
    assert.ok(existsSync(path.join(archiveDirectory, "items", archivedId, "transaction")));
    assert.ok(existsSync(path.join(archiveDirectory, "items", archivedId, "receipt.json")));
    assert.equal(statSync(archiveDirectory).mode & 0o077, 0);
    assert.doesNotMatch(archived.stdout, new RegExp(root));
    const doctor = runCxmsg(root, ["doctor", "--json"]);
    const doctorReport = JSON.parse(doctor.stdout);
    assert.ok(doctorReport.checks.some((check) =>
      check.scope === "repair-retention" && check.status === "pass" &&
      check.id.includes(archiveReceipt.archiveId.slice(0, 8))
    ));

    const replay = runCxmsg(root, [
      "repair",
      "retention",
      "archive",
      "--before",
      "2026-01-01T00:00:00.000Z",
      "--confirm",
      firstPlan.planDigest,
      "--json",
    ]);
    assert.equal(replay.status, 1);
    assert.match(replay.stderr, /plan changed/);
    assert.equal(
      readdirSync(path.join(root, "repair-retention", "transactions")).length,
      1,
    );

    cpSync(
      path.join(archiveDirectory, "items", archivedId, "transaction"),
      path.join(root, "repairs", "transactions", archivedId),
      { recursive: true },
    );
    chmodSync(path.join(root, "repairs", "transactions", archivedId), 0o700);
    chmodSync(
      path.join(root, "repairs", "transactions", archivedId, "index"),
      0o700,
    );
    chmodSync(
      path.join(root, "repairs", "transactions", archivedId, "manifest.json"),
      0o600,
    );
    cpSync(
      path.join(archiveDirectory, "items", archivedId, "receipt.json"),
      path.join(root, "repairs", "receipts", `${archivedId}.json`),
    );
    chmodSync(
      path.join(root, "repairs", "receipts", `${archivedId}.json`),
      0o600,
    );
    const duplicateRestore = runCxmsg(root, [
      "repair",
      "retention",
      "restore",
      archiveReceipt.archiveId,
      "--confirm",
      archiveReceipt.archiveId,
      "--json",
    ]);
    assert.equal(duplicateRestore.status, 1);
    assert.match(duplicateRestore.stderr, /cannot be recovered safely/);
    rmSync(path.join(root, "repairs", "transactions", archivedId), {
      recursive: true,
      force: true,
    });
    rmSync(path.join(root, "repairs", "receipts", `${archivedId}.json`), {
      force: true,
    });

    const wrongRestore = runCxmsg(root, [
      "repair",
      "retention",
      "restore",
      archiveReceipt.archiveId,
      "--confirm",
      "00000000-0000-4000-8000-000000000000",
      "--json",
    ]);
    assert.equal(wrongRestore.status, 1);
    assert.match(wrongRestore.stderr, /exact archive id/);

    const restored = runCxmsg(root, [
      "repair",
      "retention",
      "restore",
      archiveReceipt.archiveId,
      "--confirm",
      archiveReceipt.archiveId,
      "--json",
    ]);
    assert.equal(restored.status, 0, restored.stderr);
    assert.equal(JSON.parse(restored.stdout).outcome, "restored");
    assert.ok(existsSync(path.join(root, "repairs", "transactions", archivedId)));
    assert.ok(existsSync(path.join(root, "repairs", "receipts", `${archivedId}.json`)));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair archive and restore recover interrupted pair moves", async () => {
  const root = mkdtempSync(path.join(os.tmpdir(), "cxmsg-repair-retention-crash-"));
  const previousStateDir = process.env.CXMSG_STATE_DIR;
  try {
    repairRecord(root, {
      transactionId: "52345678-1234-4234-8234-123456789abc",
      phase: "completed",
      completedAt: "2025-01-02T00:00:00.000Z",
    });
    process.env.CXMSG_STATE_DIR = root;
    const moduleUrl = new URL("../src/repair-retention.js", import.meta.url);
    moduleUrl.searchParams.set("crash-test", String(Date.now()));
    const retention = await import(moduleUrl.href);
    const before = "2026-01-01T00:00:00.000Z";
    const now = Date.parse("2026-08-16T00:00:00.000Z");
    const plan = retention.buildRepairRetentionPlan({ before }, { now });
    await assert.rejects(
      retention.archiveRepairRetention(
        { before, expectedPlanDigest: plan.planDigest },
        {
          now,
          fault(phase) {
            if (phase === "after-transaction-move") {
              throw new Error("simulated archive crash");
            }
          },
        },
      ),
      /simulated archive crash/,
    );
    const archiveState = path.join(root, "repair-retention");
    const beforeDoctor = stateDigest(archiveState);
    const doctor = runCxmsg(root, ["doctor", "--json"]);
    const doctorReport = JSON.parse(doctor.stdout);
    assert.ok(doctorReport.checks.some((check) =>
      check.scope === "repair-retention" &&
      check.errorCode === "EREPAIRARCHIVEINCOMPLETE" &&
      check.status === "warn"
    ));
    assert.equal(stateDigest(archiveState), beforeDoctor);
    let recovered = await retention.recoverRepairRetention();
    assert.equal(recovered.length, 1);
    assert.equal(recovered[0].outcome, "archived");
    const [archiveId] = readdirSync(
      path.join(root, "repair-retention", "transactions"),
    );
    await assert.rejects(
      retention.restoreRepairRetention(
        { archiveId },
        {
          fault(phase) {
            if (phase === "after-transaction-restore") {
              throw new Error("simulated restore crash");
            }
          },
        },
      ),
      /simulated restore crash/,
    );
    recovered = await retention.recoverRepairRetention();
    assert.equal(recovered.length, 1);
    assert.equal(recovered[0].outcome, "restored");
    assert.ok(existsSync(path.join(
      root,
      "repairs",
      "transactions",
      "52345678-1234-4234-8234-123456789abc",
    )));

    const secondPlan = retention.buildRepairRetentionPlan({ before }, { now });
    await assert.rejects(
      retention.archiveRepairRetention(
        { before, expectedPlanDigest: secondPlan.planDigest },
        {
          now,
          fault(phase) {
            if (phase === "after-archive-receipt") {
              throw new Error("simulated receipt boundary crash");
            }
          },
        },
      ),
      /simulated receipt boundary crash/,
    );
    const archiveRoot = path.join(root, "repair-retention", "transactions");
    const interruptedId = readdirSync(archiveRoot).find((candidate) => {
      const manifest = JSON.parse(readFileSync(
        path.join(archiveRoot, candidate, "manifest.json"),
        "utf8",
      ));
      return manifest.status === "archiving";
    });
    assert.ok(interruptedId);
    const receiptPath = path.join(
      root,
      "repair-retention",
      "receipts",
      `${interruptedId}.json`,
    );
    const receiptBeforeRecovery = readFileSync(receiptPath, "utf8");
    recovered = await retention.recoverRepairRetention();
    assert.equal(recovered.length, 1);
    assert.equal(recovered[0].outcome, "archived");
    assert.equal(readFileSync(receiptPath, "utf8"), receiptBeforeRecovery);
  } finally {
    if (previousStateDir === undefined) delete process.env.CXMSG_STATE_DIR;
    else process.env.CXMSG_STATE_DIR = previousStateDir;
    rmSync(root, { recursive: true, force: true });
  }
});

test("Repair restore rejects archived tampering and active quota exhaustion", () => {
  for (const scenario of ["tamper", "quota"]) {
    const root = mkdtempSync(
      path.join(os.tmpdir(), `cxmsg-repair-retention-${scenario}-`),
    );
    try {
      const transactionId = scenario === "tamper"
        ? "62345678-1234-4234-8234-123456789abc"
        : "72345678-1234-4234-8234-123456789abc";
      repairRecord(root, {
        transactionId,
        phase: "completed",
        completedAt: "2025-01-02T00:00:00.000Z",
      });
      const args = [
        "repair",
        "retention",
        "plan",
        "--before",
        "2026-01-01T00:00:00.000Z",
        "--json",
      ];
      const planned = runCxmsg(root, args);
      assert.equal(planned.status, 0, planned.stderr);
      const plan = JSON.parse(planned.stdout);
      const archived = runCxmsg(root, [
        "repair",
        "retention",
        "archive",
        "--before",
        "2026-01-01T00:00:00.000Z",
        "--confirm",
        plan.planDigest,
        "--json",
      ]);
      assert.equal(archived.status, 0, archived.stderr);
      const archiveId = JSON.parse(archived.stdout).archiveId;
      if (scenario === "tamper") {
        const archivedManifest = path.join(
          root,
          "repair-retention",
          "transactions",
          archiveId,
          "items",
          transactionId,
          "transaction",
          "manifest.json",
        );
        const changed = JSON.parse(readFileSync(archivedManifest, "utf8"));
        changed.updatedAt = "2025-01-05T00:00:00.000Z";
        writeJson(archivedManifest, changed);
      } else {
        const quota = path.join(root, "repairs", "quota.bin");
        writeFileSync(quota, "", { mode: 0o600 });
        truncateSync(quota, 256 * 1024 * 1024);
      }
      const restored = runCxmsg(root, [
        "repair",
        "retention",
        "restore",
        archiveId,
        "--confirm",
        archiveId,
        "--json",
      ]);
      assert.equal(restored.status, 1);
      assert.match(
        restored.stderr,
        scenario === "tamper" ? /evidence changed/ : /bounded retention limit/,
      );
      assert.equal(
        existsSync(path.join(root, "repairs", "transactions", transactionId)),
        false,
      );
      assert.equal(
        existsSync(path.join(root, "repairs", "receipts", `${transactionId}.json`)),
        false,
      );
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
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

    const planResult = runCxmsg(root, [
      "repair",
      "retention",
      "plan",
      "--before",
      "2026-01-01T00:00:00.000Z",
      "--json",
    ]);
    assert.equal(planResult.status, 0, planResult.stderr);
    const plan = JSON.parse(planResult.stdout);
    const transactionId = "42345678-1234-4234-8234-123456789abc";
    const receiptPath = path.join(
      root,
      "repairs",
      "receipts",
      `${transactionId}.json`,
    );
    const manifestPath = path.join(
      root,
      "repairs",
      "transactions",
      transactionId,
      "manifest.json",
    );
    const receipt = JSON.parse(readFileSync(receiptPath, "utf8"));
    receipt.completedAt = "2025-01-04T00:00:00.000Z";
    writeJson(receiptPath, receipt);
    const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    manifest.updatedAt = receipt.completedAt;
    manifest.receiptSha256 = sha256(JSON.stringify(receipt));
    writeJson(manifestPath, manifest);
    const stale = runCxmsg(root, [
      "repair",
      "retention",
      "archive",
      "--before",
      "2026-01-01T00:00:00.000Z",
      "--confirm",
      plan.planDigest,
      "--json",
    ]);
    assert.equal(stale.status, 1);
    assert.match(stale.stderr, /plan changed/);
    assert.equal(existsSync(path.join(root, "repair-retention")), false);

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
