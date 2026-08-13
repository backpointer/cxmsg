#!/usr/bin/env node

import { runHostRelay } from "../src/host-relay.js";

const port = Number(process.argv[2] || 4174);
runHostRelay({ port }).catch((error) => {
  process.stderr.write(`cxmsg host relay: ${error.message}\n`);
  process.exitCode = 1;
});
