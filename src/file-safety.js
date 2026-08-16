import { constants } from "node:fs";

export function requireNoFollowFlag(value = constants.O_NOFOLLOW) {
  if (!Number.isInteger(value) || value <= 0) {
    const error = new Error("owner-private storage requires O_NOFOLLOW");
    error.code = "ESTORAGENONOFOLLOW";
    throw error;
  }
  return value;
}
