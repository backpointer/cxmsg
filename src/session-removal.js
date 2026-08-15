import { removeSessionRecord } from "./registry.js";
import { tombstoneNode } from "./node-directory.js";

const MISSING_THREAD_PATTERN =
  /not found|does not exist|not persisted|thread not loaded|no rollout/i;

export async function finalizeSessionRemovalLocked({
  name,
  threadId,
  deleteThread,
  fault = null,
}) {
  if (typeof deleteThread !== "function") {
    throw new Error("session removal requires a thread deletion adapter");
  }
  try {
    await deleteThread(threadId);
  } catch (error) {
    if (!MISSING_THREAD_PATTERN.test(error?.message || "")) throw error;
  }
  if (fault) await fault("after-thread-delete");
  removeSessionRecord(name);
  if (fault) await fault("after-session-record-remove");
  await tombstoneNode("codex", threadId, {
    reason: "session-removed",
    missingOk: true,
  });
  return { name, threadId };
}
