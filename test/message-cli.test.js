import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-message-cli-"));
process.env.CXMSG_STATE_DIR = stateDir;
const bodies = await import(`../src/message-bodies.js?cli=${Date.now()}`);
const messageId = "82345678-1234-4234-8234-123456789abc";
const body = `first line\n${"나".repeat(20_000)}\nlast line`;
const stored = await bodies.storeMessageBody({ messageId, body });
const registry = await import(`../src/registry.js?cli=${Date.now()}`);
const ledger = await import(`../src/delivery-ledger.js?cli=${Date.now()}`);
const senderThreadId = "92345678-1234-4234-8234-123456789abc";
const targetThreadId = "a2345678-1234-4234-8234-123456789abc";
registry.writeSessionRecord({ name: "sender", threadId: senderThreadId, cwd: path.resolve(".") });
registry.writeSessionRecord({ name: "reader", threadId: targetThreadId, cwd: path.resolve(".") });
await ledger.commitSingleRecipientDelivery(
  {
    logicalMessage: {
      messageId,
      from: "sender",
      senderThreadId,
      body: {
        messageId,
        bytes: stored.bodyBytes,
        sha256: stored.bodySha256,
        contentRef: stored.contentRef,
      },
      route: null,
      routeFingerprint: createHash("sha256").update("null").digest("hex"),
      createdAt: "2026-08-15T00:00:00.000Z",
    },
    target: "reader",
    targetThreadId,
    admissionState: "admitted",
    admissionReason: "legacy-unbound",
    now: "2026-08-15T00:00:00.000Z",
  },
  { replyHandleFactory: () => "m:23456789AB" },
);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

function cxmsg(...args) {
  return spawnSync(process.execPath, ["bin/cxmsg.js", ...args], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CXMSG_STATE_DIR: stateDir,
      CODEX_SESSION_NAME: "reader",
    },
    encoding: "utf8",
  });
}

function readOnlyCxmsgAt(readStateDir, ...args) {
  return spawnSync(
    process.execPath,
    [
      "--permission",
      `--allow-fs-read=${path.resolve(".")}`,
      `--allow-fs-read=${readStateDir}`,
      "bin/cxmsg.js",
      ...args,
    ],
    {
      cwd: path.resolve("."),
      env: {
        ...process.env,
        CXMSG_STATE_DIR: readStateDir,
        CODEX_SESSION_NAME: "reader",
      },
      encoding: "utf8",
    },
  );
}

function readOnlyCxmsg(...args) {
  return readOnlyCxmsgAt(stateDir, ...args);
}

test("message info exposes bounded metadata without a storage path", () => {
  const result = cxmsg("message", "info", `cxmsg-message:${messageId}`, "--json");
  assert.equal(result.status, 0, result.stderr);
  const info = JSON.parse(result.stdout);
  assert.equal(info.messageId, messageId);
  assert.equal(info.bodyBytes, Buffer.byteLength(body, "utf8"));
  assert.equal("path" in info, false);
  assert.equal("body" in info, false);
});

test("message info and show require no filesystem write permission", () => {
  const info = readOnlyCxmsg("message", "info", messageId, "--json");
  assert.equal(info.status, 0, info.stderr);
  const metadata = JSON.parse(info.stdout);
  assert.equal(metadata.messageId, messageId);
  assert.equal("path" in metadata, false);

  const shown = readOnlyCxmsg(
    "message",
    "show",
    metadata.contentRef,
    "--limit",
    "64",
    "--json",
  );
  assert.equal(shown.status, 0, shown.stderr);
  assert.equal(JSON.parse(shown.stdout).messageId, messageId);
});

test("an absent body store stays absent during a read-only lookup", () => {
  const emptyState = mkdtempSync(path.join(os.tmpdir(), "cxmsg-message-empty-"));
  try {
    const result = readOnlyCxmsgAt(
      emptyState,
      "message",
      "info",
      "72345678-1234-4234-8234-123456789abc",
      "--json",
    );
    assert.equal(result.status, 1);
    assert.match(result.stderr, /unknown message body/);
    assert.doesNotMatch(result.stderr, /ERR_ACCESS_DENIED/);
    assert.equal(existsSync(path.join(emptyState, "message-bodies")), false);
  } finally {
    rmSync(emptyState, { recursive: true, force: true });
  }
});

test("message show reads resumable bounded ranges", () => {
  const first = cxmsg(
    "message",
    "show",
    messageId,
    "--offset",
    "0",
    "--limit",
    "4096",
    "--json",
  );
  assert.equal(first.status, 0, first.stderr);
  const firstRange = JSON.parse(first.stdout);
  assert.equal(firstRange.complete, false);
  assert.ok(firstRange.nextOffset >= 4096);
  assert.doesNotMatch(firstRange.text, /\uFFFD/);

  const second = cxmsg(
    "message",
    "show",
    messageId,
    "--offset",
    String(firstRange.nextOffset),
    "--limit",
    String(64 * 1024),
    "--json",
  );
  assert.equal(second.status, 0, second.stderr);
  const secondRange = JSON.parse(second.stdout);
  assert.equal(secondRange.offset, firstRange.nextOffset);
  assert.equal(secondRange.complete, true);
  assert.equal(firstRange.text + secondRange.text, body);

  const byHandle = cxmsg("message", "show", "m:23456789AB", "--limit", "64", "--json");
  assert.equal(byHandle.status, 0, byHandle.stderr);
  assert.equal(JSON.parse(byHandle.stdout).messageId, messageId);
});
