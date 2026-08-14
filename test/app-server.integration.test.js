import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { withAppServer } from "../src/app-server-client.js";
import {
  listRecentTurns,
  readThreadMetadata,
} from "../src/thread-activity.js";

const integrationEnabled = process.env.CXMSG_INTEGRATION === "1";

test(
  "real UDS app-server creates, names, reads, and deletes a thread",
  { skip: !integrationEnabled },
  async () => {
    await withAppServer(async (client) => {
      const name = `cxmsg:integration-${randomUUID().slice(0, 8)}`;
      const started = await client.request("thread/start", {
        cwd: process.cwd(),
        serviceName: "cxmsg-integration-test",
      });
      const threadId = started.thread.id;

      try {
        await client.request("thread/name/set", { threadId, name });
        const thread = await readThreadMetadata(client, threadId);
        const turns = await listRecentTurns(client, threadId, {
          itemsView: "summary",
        });
        assert.equal(thread.id, threadId);
        assert.equal(thread.name, name);
        assert.equal(thread.ephemeral, false);
        assert.deepEqual(turns.data, []);
      } finally {
        await client.request("thread/delete", { threadId });
      }
    });
  },
);
