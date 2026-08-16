import assert from "node:assert/strict";
import { constants } from "node:fs";
import test from "node:test";
import { requireNoFollowFlag } from "../src/file-safety.js";

test("owner-private storage requires a real O_NOFOLLOW flag", () => {
  assert.equal(requireNoFollowFlag(), constants.O_NOFOLLOW);
  for (const unavailable of [null, 0, -1, "O_NOFOLLOW"]) {
    assert.throws(
      () => requireNoFollowFlag(unavailable),
      (error) => error?.code === "ESTORAGENONOFOLLOW",
    );
  }
});
