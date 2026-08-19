import assert from "node:assert/strict";
import { closeSync, readFileSync, writeFileSync } from "node:fs";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {
  appServerLogEnvironment,
  openBoundedRuntimeLog,
} from "../src/runtime-logs.js";

test("runtime logs rotate before daemon start and remain owner-private", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-runtime-log-"));
  const target = path.join(root, "app-server.log");
  try {
    writeFileSync(target, "a".repeat(1024), { mode: 0o600 });
    const descriptor = openBoundedRuntimeLog(target, {
      stateDir: root,
      maxBytes: 1024,
      archives: 2,
    });
    closeSync(descriptor);
    assert.equal(readFileSync(`${target}.1`, "utf8").length, 1024);
    assert.equal(readFileSync(target, "utf8"), "");
    assert.equal((await fs.stat(target)).mode & 0o777, 0o600);

    await fs.rm(target);
    await fs.symlink(path.join(root, "outside.log"), target);
    assert.throws(
      () => openBoundedRuntimeLog(target, { stateDir: root }),
      (error) => error?.code === "ERUNTIMELOGIDENTITY",
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("App Server logging suppresses raw tool router context by default", () => {
  const environment = appServerLogEnvironment({ PATH: "/bin" });
  assert.equal(environment.NO_COLOR, "1");
  assert.match(environment.RUST_LOG, /codex_core::tools::router=off/);
  assert.equal(environment.PATH, "/bin");
  assert.equal(
    appServerLogEnvironment({ CXMSG_APP_SERVER_RUST_LOG: "error" }).RUST_LOG,
    "error",
  );
});
