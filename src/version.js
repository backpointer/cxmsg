import { readFileSync } from "node:fs";

export const CXMSG_VERSION = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
).version;

export const CXMSG_IMPLEMENTATION_REVISIONS = Object.freeze({
  cli: 13,
  scheduler: 7,
  hostRelay: 1,
  claudeBridge: 33,
});
