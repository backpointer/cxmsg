#!/usr/bin/env node

import { runSchedulerWorker } from "../src/scheduler.js";

runSchedulerWorker().catch((error) => {
  process.stderr.write(`cxmsg scheduler worker: ${error.message}\n`);
  process.exitCode = 1;
});
