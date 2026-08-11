#!/usr/bin/env node

import { runClaudeBridge } from "../src/claude-bridge.js";

runClaudeBridge(process.argv[2]).catch((error) => {
  process.stderr.write(`cxmsg Claude bridge: ${error.message}\n`);
  process.exitCode = 1;
});
