import assert from "node:assert/strict";
import test from "node:test";
import { serviceEvidence } from "../src/process-state.js";
import { failedProbe, timeoutProbe } from "../src/socket-probe.js";

function error(code) {
  return Object.assign(new Error(code), { code });
}

test("socket probes preserve denied, missing, refused, and timeout causes", () => {
  assert.deepEqual(failedProbe(error("EPERM")), {
    state: "denied",
    errorCode: "EPERM",
    error: "EPERM",
  });
  assert.equal(failedProbe(error("ENOENT")).state, "missing");
  assert.equal(failedProbe(error("ECONNREFUSED")).state, "refused");
  assert.equal(timeoutProbe().state, "timeout");
});

test("service evidence distinguishes unreachable, stale, and mismatched safely", () => {
  const unreachable = serviceEvidence({
    process: "unverified",
    identity: "unavailable",
    socketProbe: failedProbe(error("EPERM")),
    socketPresent: true,
  });
  assert.equal(unreachable.status, "unreachable");
  assert.equal(unreachable.safeToSignal, false);
  assert.equal(unreachable.safeToRemove, false);

  const verifiedButDenied = serviceEvidence({
    process: "alive",
    identity: "matched",
    socketProbe: failedProbe(error("EPERM")),
    socketPresent: true,
  });
  assert.equal(verifiedButDenied.status, "unreachable");
  assert.equal(verifiedButDenied.safeToSignal, false);

  const stale = serviceEvidence({
    process: "missing",
    identity: "unavailable",
    socketProbe: failedProbe(error("ECONNREFUSED")),
    socketPresent: true,
  });
  assert.equal(stale.status, "stale");
  assert.equal(stale.safeToRemove, true);

  const mismatched = serviceEvidence({
    process: "alive",
    identity: "mismatched",
    socketProbe: { state: "healthy" },
    socketPresent: true,
  });
  assert.equal(mismatched.status, "mismatched");
  assert.equal(mismatched.safeToSignal, false);
  assert.equal(mismatched.safeToRemove, false);
});
