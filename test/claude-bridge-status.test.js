import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

test("Claude bridge status explicitly warns about stale loaded code", async () => {
  const stateDir = await fs.mkdtemp(path.join(os.tmpdir(), "cxmsg-bridge-status-"));
  try {
    const bridges = path.join(stateDir, "claude-bridges");
    await fs.mkdir(bridges, { recursive: true, mode: 0o700 });
    await fs.writeFile(
      path.join(bridges, "worker.json"),
      `${JSON.stringify({
        version: 1,
        target: "worker",
        targetThreadId: "12345678-1234-4234-8234-123456789abc",
        pid: 999_999,
        socketPath: "/tmp/cc-socks/999999.sock",
        startedAt: Date.now(),
        cxmsgVersion: "0.49.1",
        implementationRevision: 25,
      })}\n`,
      { mode: 0o600 },
    );
    const result = spawnSync(
      process.execPath,
      ["bin/cxmsg.js", "claude", "bridge", "status", "worker"],
      {
        cwd: path.resolve("."),
        env: { ...process.env, CXMSG_STATE_DIR: stateDir },
        encoding: "utf8",
      },
    );
    assert.equal(result.status, 0, result.stderr);
    assert.match(result.stdout, /implementation=stale/);
    assert.match(result.stderr, /revision 25; current=28/);
    assert.match(result.stderr, /bridge stop worker/);
  } finally {
    await fs.rm(stateDir, { recursive: true, force: true });
  }
});
