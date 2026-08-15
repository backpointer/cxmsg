#!/usr/bin/env node

import { runDelegationWorker } from "../src/delegation-worker.js";

const jobId = process.argv[2];
let scheduleClaim = null;
if (process.argv[3] === "--claim") {
  scheduleClaim = {
    claimId: process.argv[4],
    workerId: process.argv[5],
  };
}

runDelegationWorker(jobId, { scheduleClaim }).catch((error) => {
  process.stderr.write(`cxmsg delegation worker: ${error.message}\n`);
  process.exitCode = 1;
});
