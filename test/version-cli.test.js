import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import test from "node:test";
import {
  CXMSG_IMPLEMENTATION_REVISIONS,
  CXMSG_VERSION,
} from "../src/version.js";

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
    assert.match(result.stdout, /app-server external-codex\n$/);
    assert.equal(result.stderr, "");
  }
});

test("version JSON has a stable bounded schema without runtime paths", () => {
  const result = run("version", "--json");
  assert.equal(result.status, 0);
  assert.equal(result.stderr, "");
  assert.deepEqual(JSON.parse(result.stdout), {
    schemaVersion: 1,
    packageVersion: CXMSG_VERSION,
    implementationRevisions: CXMSG_IMPLEMENTATION_REVISIONS,
    appServer: {
      owner: "codex",
      revision: null,
    },
  });
  assert.doesNotMatch(result.stdout, /\/Users\/|\/Volumes\/|\.codex\/cxmsg/);
});

test("version rejects unknown options with the invalid-invocation exit code", () => {
  const result = run("version", "--unknown");
  assert.equal(result.status, 2);
  assert.match(result.stderr, /unknown version option/);
});
