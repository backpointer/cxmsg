#!/usr/bin/env node

import { runDelegationWorker } from "../src/delegation-worker.js";

runDelegationWorker(process.argv[2]).catch((error) => {
  process.stderr.write(`cxmsg delegation worker: ${error.message}\n`);
  process.exitCode = 1;
});
