import assert from "node:assert/strict";
import test from "node:test";
import {
  processIdentity,
  processState,
  serviceEvidence,
} from "../src/process-state.js";

function systemError(code) {
  return Object.assign(new Error(code), { code });
}

test("process evidence distinguishes EPERM, ESRCH, and unavailable ps", () => {
  assert.equal(
    processState(42, () => {
      throw systemError("EPERM");
    }),
    "unverified",
  );
  assert.equal(
    processState(42, () => {
      throw systemError("ESRCH");
    }),
    "missing",
  );
  assert.equal(
    processIdentity(42, ["worker"], () => ({
      status: 1,
      stdout: "",
      stderr: "ps: Operation not permitted",
    })).state,
    "unavailable",
  );
});

test("healthy UDS permits status but not signaling or stale cleanup when PID is unverified", () => {
  assert.deepEqual(
    serviceEvidence({
      process: "unverified",
      identity: "unavailable",
      socketHealthy: true,
    }),
    { running: true, safeToSignal: false, safeToRemove: false },
  );
  assert.deepEqual(
    serviceEvidence({
      process: "missing",
      identity: "unavailable",
      socketHealthy: false,
    }),
    { running: false, safeToSignal: false, safeToRemove: true },
  );
});
