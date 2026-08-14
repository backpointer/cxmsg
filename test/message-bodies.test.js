import assert from "node:assert/strict";
import {
  appendFileSync,
  lstatSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  statSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const stateDir = mkdtempSync(path.join(os.tmpdir(), "cxmsg-message-bodies-"));
process.env.CXMSG_STATE_DIR = stateDir;
const bodies = await import(`../src/message-bodies.js?test=${Date.now()}`);

test.after(() => {
  rmSync(stateDir, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

const firstId = "12345678-1234-4234-8234-123456789abc";
const secondId = "22345678-1234-4234-8234-123456789abc";
const largeId = "32345678-1234-4234-8234-123456789abc";

test("message bodies are owner-only, idempotent, and digest verified", async () => {
  const body = `heading\n${"가나다라마바사".repeat(1_000)}\ntrailer`;
  const stored = await bodies.storeMessageBody({ messageId: firstId, body });
  const repeated = await bodies.storeMessageBody({ messageId: firstId, body });

  assert.deepEqual(repeated, stored);
  assert.equal(stored.contentRef, `cxmsg-message:${firstId}`);
  assert.equal(stored.bodyBytes, Buffer.byteLength(body, "utf8"));
  assert.match(stored.bodySha256, /^[0-9a-f]{64}$/);
  assert.equal(lstatSync(bodies.MESSAGE_BODIES_DIR).mode & 0o777, 0o700);
  const segment = path.join(
    bodies.MESSAGE_BODY_SEGMENTS_DIR,
    readdirSync(bodies.MESSAGE_BODY_SEGMENTS_DIR)[0],
  );
  assert.equal(statSync(segment).mode & 0o777, 0o600);
  assert.throws(
    () => bodies.messageBodyInfo("cxmsg-message:not-a-uuid"),
    /message-id must be a UUID/,
  );
  await assert.rejects(
    bodies.storeMessageBody({ messageId: firstId, body: `${body}changed` }),
    /idempotency conflict/,
  );
});

test("message body reads are bounded, UTF-8 safe, and resumable", () => {
  const first = bodies.readMessageBody(firstId, { offset: 9, limit: 17 });
  assert.equal(first.offset, 11);
  assert.doesNotMatch(first.text, /\uFFFD/);
  assert.ok(first.nextOffset > first.offset);
  assert.equal(first.complete, false);

  const second = bodies.readMessageBody(firstId, {
    offset: first.nextOffset,
    limit: 64 * 1024,
  });
  assert.equal(second.offset, first.nextOffset);
  assert.equal(second.complete, true);
  assert.equal(
    Buffer.byteLength(first.text + second.text, "utf8"),
    first.bodyBytes - first.offset,
  );
});

test("partial active segments are quarantined without losing complete records", async () => {
  const active = path.join(
    bodies.MESSAGE_BODY_SEGMENTS_DIR,
    readdirSync(bodies.MESSAGE_BODY_SEGMENTS_DIR)[0],
  );
  appendFileSync(active, "{partial", { mode: 0o600 });
  const body = "second body remains readable";
  await bodies.storeMessageBody({ messageId: secondId, body });

  assert.equal(readdirSync(bodies.MESSAGE_BODY_QUARANTINE_DIR).length, 1);
  assert.equal(bodies.readMessageBody(firstId, { limit: 64 * 1024 }).bodyBytes > 0, true);
  assert.equal(bodies.readMessageBody(secondId).text, body);
});

test("stored body limit is 256 KiB and quota failure deletes nothing", async () => {
  const maximum = "a".repeat(256 * 1024);
  await bodies.storeMessageBody(
    { messageId: largeId, body: maximum },
    { quotaBytes: 2 * 1024 * 1024, segmentBytes: 1024 * 1024 },
  );
  assert.equal(bodies.messageBodyInfo(largeId).bodyBytes, 256 * 1024);
  await assert.rejects(
    bodies.storeMessageBody({
      messageId: "42345678-1234-4234-8234-123456789abc",
      body: `${maximum}x`,
    }),
    /stored body limit/,
  );

  const before = readdirSync(bodies.MESSAGE_BODY_SEGMENTS_DIR);
  await assert.rejects(
    bodies.storeMessageBody(
      {
        messageId: "52345678-1234-4234-8234-123456789abc",
        body: "b".repeat(220 * 1024),
      },
      { quotaBytes: 300 * 1024, segmentBytes: 1024 * 1024 },
    ),
    /quota exceeded/,
  );
  assert.deepEqual(readdirSync(bodies.MESSAGE_BODY_SEGMENTS_DIR), before);
});
