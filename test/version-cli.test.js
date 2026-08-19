import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import test from "node:test";
import {
  CXMSG_IMPLEMENTATION_REVISIONS,
  CXMSG_VERSION,
} from "../src/version.js";
import { MAX_CLAUDE_FRAME_BYTES } from "../src/claude-messaging.js";
import { MAX_STORED_MESSAGE_BYTES } from "../src/message-bodies.js";
import { MAX_MESSAGE_BYTES } from "../src/messaging.js";
import {
  DEFAULT_APP_SERVER_FRAME_BYTES,
  MAX_APP_SERVER_FRAME_BYTES,
} from "../src/unix-websocket.js";

const cli = path.resolve("bin/cxmsg.js");

function run(...args) {
  return spawnSync(process.execPath, [cli, ...args], { encoding: "utf8" });
}

test("version and --version expose the installed package and implementation revisions", () => {
  for (const args of [["version"], ["--version"]]) {
    const result = run(...args);
    assert.equal(result.status, 0);
    assert.match(result.stdout, new RegExp(`^cxmsg ${CXMSG_VERSION}\\n`));
    assert.match(
      result.stdout,
      new RegExp(`scheduler=${CXMSG_IMPLEMENTATION_REVISIONS.scheduler}`),
    );
    assert.match(result.stdout, /app-server external-codex\n/);
    assert.match(
      result.stdout,
      new RegExp(`transport app-server-frame=${DEFAULT_APP_SERVER_FRAME_BYTES}`),
    );
    assert.equal(result.stderr, "");
  }
});

test("version JSON has a stable bounded schema without runtime paths", () => {
  const result = run("version", "--json");
  assert.equal(result.status, 0);
  assert.equal(result.stderr, "");
  assert.deepEqual(JSON.parse(result.stdout), {
    schemaVersion: 2,
    packageVersion: CXMSG_VERSION,
    implementationRevisions: CXMSG_IMPLEMENTATION_REVISIONS,
    appServer: {
      owner: "codex",
      revision: null,
    },
    transportLimits: {
      appServerFrameBytes: DEFAULT_APP_SERVER_FRAME_BYTES,
      appServerFrameMaximumBytes: MAX_APP_SERVER_FRAME_BYTES,
      claudeFrameBytes: MAX_CLAUDE_FRAME_BYTES,
      peerMessageBytes: MAX_MESSAGE_BYTES,
      storedMessageBytes: MAX_STORED_MESSAGE_BYTES,
    },
  });
  assert.doesNotMatch(result.stdout, /\/Users\/|\/Volumes\/|\.codex\/cxmsg/);
});

test("version validates bounded App Server frame overrides", () => {
  const accepted = spawnSync(process.execPath, [cli, "version", "--json"], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_APP_SERVER_FRAME_BYTES: "6291456" },
  });
  assert.equal(accepted.status, 0);
  assert.equal(JSON.parse(accepted.stdout).transportLimits.appServerFrameBytes, 6_291_456);

  const rejected = spawnSync(process.execPath, [cli, "version", "--json"], {
    encoding: "utf8",
    env: { ...process.env, CXMSG_APP_SERVER_FRAME_BYTES: "999999999" },
  });
  assert.equal(rejected.status, 1);
  assert.match(rejected.stderr, /CXMSG_APP_SERVER_FRAME_BYTES/);
});

test("version rejects unknown options with the invalid-invocation exit code", () => {
  const result = run("version", "--unknown");
  assert.equal(result.status, 2);
  assert.match(result.stderr, /unknown version option/);
});
