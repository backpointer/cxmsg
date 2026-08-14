import os from "node:os";
import path from "node:path";

export const CXMSG_STATE_DIR = process.env.CXMSG_STATE_DIR
  ? path.resolve(process.env.CXMSG_STATE_DIR)
  : path.join(os.homedir(), ".codex", "cxmsg");
export const DEFAULT_SOCKET_PATH = path.join(CXMSG_STATE_DIR, "app-server.sock");
export const PID_PATH = path.join(CXMSG_STATE_DIR, "app-server.pid");
export const LOG_PATH = path.join(CXMSG_STATE_DIR, "app-server.log");
export const EVENT_LOG_PATH = path.join(CXMSG_STATE_DIR, "events.jsonl");
export function socketPath() {
  return process.env.CXMSG_SOCKET || DEFAULT_SOCKET_PATH;
}

export function socketUrl() {
  return `unix://${socketPath()}`;
}
