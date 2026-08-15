import assert from "node:assert/strict";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-relay-version-"));
process.env.CXMSG_STATE_DIR = root;
const relay = await import(`../src/host-relay.js?test=${Date.now()}`);
const version = await import("../src/version.js");

test.after(async () => {
  await fs.rm(root, { recursive: true, force: true });
  delete process.env.CXMSG_STATE_DIR;
});

test("host relay binds package and implementation versions in its health identity", async () => {
  const running = await relay.runHostRelay({ port: 0 });
  try {
    const record = relay.readHostRelayRecord();
    assert.equal(record.cxmsgVersion, version.CXMSG_VERSION);
    assert.equal(
      record.implementationRevision,
      relay.HOST_RELAY_IMPLEMENTATION_REVISION,
    );
    const health = await relay.hostRelayRequest("/health", { record });
    assert.equal(health.cxmsgVersion, record.cxmsgVersion);
    assert.equal(
      health.implementationRevision,
      record.implementationRevision,
    );
  } finally {
    await new Promise((resolve, reject) =>
      running.server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
