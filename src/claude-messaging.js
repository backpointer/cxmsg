import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { processState } from "./process-state.js";
import {
  failedProbe,
  healthyProbe,
  isUnreachableProbe,
  timeoutProbe,
} from "./socket-probe.js";

export const CLAUDE_SESSIONS_DIR = path.join(os.homedir(), ".claude", "sessions");
export const CLAUDE_SOCKETS_DIR = path.join(path.sep, "tmp", "cc-socks");
export function claudeSocketsDir() {
  return process.env.CXMSG_CLAUDE_SOCKETS_DIR
    ? path.resolve(process.env.CXMSG_CLAUDE_SOCKETS_DIR)
    : CLAUDE_SOCKETS_DIR;
}
export const CLAUDE_PEER_PROTOCOL = 1;
export const MAX_CLAUDE_FRAME_BYTES = 64 * 1024;
const CLAUDE_REQUEST_CLOSE = "</cxmsg-request>";

function isUuid(value) {
  return /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value || "");
}

function xmlEscapeAttribute(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function xmlUnescape(value) {
  return value.replace(
    /&(?:amp|lt|gt|quot|apos);/g,
    (entity) =>
      ({
        "&amp;": "&",
        "&lt;": "<",
        "&gt;": ">",
        "&quot;": '"',
        "&apos;": "'",
      })[entity] || entity,
  );
}

function safeClaudeName(value) {
  return value
    .replace(/[\p{Cf}\p{Cc}\p{Cs}\p{Zl}\p{Zp}]/gu, "")
    .replace(/["<>]/g, "")
    .slice(0, 64);
}

function protectClosingTag(value) {
  return value.replace(/<\/(?=cross-session-message(?:[>\s/]|$))/gi, "<\\/");
}

export function claudePeerAddress(socketPath) {
  return `uds:${socketPath}`;
}

export function buildClaudePeerFrame({
  fromSocket,
  fromName,
  fromSession,
  message,
  messageId = randomUUID(),
}) {
  if (!isUuid(messageId)) throw new Error("Claude message id must be a UUID");
  if (!message?.trim()) throw new Error("message must not be empty");
  if (Buffer.byteLength(message, "utf8") > MAX_CLAUDE_FRAME_BYTES / 2) {
    throw new Error("message is too large for the Claude peer protocol");
  }

  const from = claudePeerAddress(fromSocket);
  const attributes = [`from="${xmlEscapeAttribute(from)}"`];
  if (isUuid(fromSession)) attributes.push(`from-session="${fromSession}"`);
  const name = safeClaudeName(fromName || "");
  if (name) attributes.push(`from-name="${xmlEscapeAttribute(name)}"`);
  const content =
    `<cross-session-message ${attributes.join(" ")}>\n` +
    `${protectClosingTag(message)}\n` +
    "</cross-session-message>";

  return {
    msgV: CLAUDE_PEER_PROTOCOL,
    msg_id: messageId,
    type: "user",
    message: { role: "user", content },
    priority: "next",
    from,
  };
}

export function buildClaudeRequestBody(task, grantToken) {
  if (!task?.trim()) throw new Error("Claude request task must not be empty");
  if (!isUuid(grantToken)) throw new Error("Claude request grant token must be a UUID");
  if (task.includes(CLAUDE_REQUEST_CLOSE)) {
    throw new Error("Claude request task contains a reserved closing tag");
  }
  return `<cxmsg-request grant="${grantToken}">\n${task}\n${CLAUDE_REQUEST_CLOSE}`;
}

export function parseClaudeRequestBody(body) {
  if (typeof body !== "string") return null;
  const match = body.match(
    /^<cxmsg-request grant="([0-9a-f-]+)">\n([\s\S]+)\n<\/cxmsg-request>$/i,
  );
  if (!match?.[2]?.trim() || !isUuid(match[1])) return null;
  return { grantToken: match[1], task: match[2] };
}

export function redactClaudeRequestCapabilities(body) {
  if (typeof body !== "string" || !/^\s*<cxmsg-request\b/i.test(body)) {
    return body;
  }
  const openingEnd = body.indexOf(">");
  if (openingEnd === -1) return "<cxmsg-request grant=\"[redacted]\">";
  const opening = body.slice(0, openingEnd + 1).replace(
    /\bgrant\s*=\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s>]+)/gi,
    'grant="[redacted]"',
  );
  return `${opening}${body.slice(openingEnd + 1)}`;
}

export function buildClaudeResponseBody({ requestId, status, result, error }) {
  if (!isUuid(requestId)) throw new Error("Claude request id must be a UUID");
  if (!/^[a-z][a-zA-Z0-9_-]{0,31}$/.test(status || "")) {
    throw new Error("invalid Claude response status");
  }
  const text = (result || error || "Codex completed without a final response.")
    .replaceAll("</cxmsg-response>", "<\\/cxmsg-response>");
  return (
    `<cxmsg-response in-reply-to="${requestId}" status="${status}">\n` +
    `${text}\n` +
    "</cxmsg-response>"
  );
}

export function parseClaudePeerFrame(frame) {
  if (
    !frame ||
    frame.msgV !== CLAUDE_PEER_PROTOCOL ||
    !isUuid(frame.msg_id) ||
    frame.type !== "user" ||
    frame.message?.role !== "user" ||
    typeof frame.message.content !== "string"
  ) {
    throw new Error("invalid Claude peer frame");
  }
  if (Buffer.byteLength(frame.message.content, "utf8") > MAX_CLAUDE_FRAME_BYTES) {
    throw new Error("Claude peer frame exceeds the size limit");
  }

  const match = frame.message.content.match(
    /^<cross-session-message([^>]*)>\n([\s\S]*)\n<\/cross-session-message>$/,
  );
  if (!match) throw new Error("invalid Claude cross-session envelope");
  const attributes = {};
  for (const attribute of match[1].matchAll(/\s+([a-z-]+)="([^"]*)"/g)) {
    attributes[attribute[1]] = xmlUnescape(attribute[2]);
  }
  const fromAddress = attributes.from || frame.from;
  if (!fromAddress?.startsWith("uds:") || (frame.from && frame.from !== fromAddress)) {
    throw new Error("Claude peer sender address mismatch");
  }

  return {
    messageId: frame.msg_id,
    fromAddress,
    fromSocket: fromAddress.slice(4),
    fromName: attributes["from-name"] || null,
    fromSession: attributes["from-session"] || null,
    body: match[2].replaceAll("<\\/cross-session-message", "</cross-session-message"),
  };
}

export async function validateClaudeSocketPath(socketPath) {
  const resolved = path.resolve(socketPath || "");
  if (
    path.dirname(resolved) !== claudeSocketsDir() ||
    !/^\d+\.sock$/.test(path.basename(resolved))
  ) {
    throw new Error(`refusing non-Claude UDS path: ${socketPath}`);
  }
  const metadata = await fs.lstat(resolved);
  if (!metadata.isSocket()) throw new Error(`not a Unix socket: ${resolved}`);
  if (typeof process.getuid === "function" && metadata.uid !== process.getuid()) {
    throw new Error(`Claude socket is owned by another user: ${resolved}`);
  }
  if ((metadata.mode & 0o077) !== 0) {
    throw new Error(`Claude socket permissions are too broad: ${resolved}`);
  }
  return resolved;
}

export async function probeClaudeSocket(socketPath, timeoutMs = 250) {
  let resolved;
  try {
    resolved = await validateClaudeSocketPath(socketPath);
  } catch (error) {
    return failedProbe(error);
  }
  return new Promise((resolve) => {
    const socket = net.createConnection({ path: resolved });
    const finish = (probe) => {
      socket.destroy();
      resolve(probe);
    };
    socket.once("connect", () => finish(healthyProbe()));
    socket.once("error", (error) =>
      finish(error.code === "EBUSY" ? healthyProbe() : failedProbe(error)),
    );
    socket.setTimeout(timeoutMs, () => finish(timeoutProbe()));
  });
}

export async function listClaudePeers({
  sessionsDir = CLAUDE_SESSIONS_DIR,
  processStateFn = processState,
  probeSocket = probeClaudeSocket,
} = {}) {
  let filenames;
  try {
    filenames = await fs.readdir(sessionsDir);
  } catch {
    return [];
  }

  const records = await Promise.all(
    filenames
      .filter((filename) => /^\d+\.json$/.test(filename))
      .map(async (filename) => {
        try {
          const pid = Number.parseInt(filename.slice(0, -5), 10);
          const record = JSON.parse(await fs.readFile(path.join(sessionsDir, filename), "utf8"));
          const process = processStateFn(pid);
          if (
            record.pid !== pid ||
            process === "missing" ||
            record.entrypoint === "cxmsg" ||
            typeof record.messagingSocketPath !== "string" ||
            path.basename(record.messagingSocketPath) !== `${pid}.sock`
          ) {
            return null;
          }
          const probe = await probeSocket(record.messagingSocketPath);
          if (!probe || typeof probe === "boolean") {
            if (!probe) return null;
          } else if (!isUnreachableProbe(probe) && probe.state !== "healthy") {
            return null;
          }
          const unreachable =
            typeof probe === "object" && isUnreachableProbe(probe);
          return {
            pid,
            name: typeof record.name === "string" ? record.name : `claude-${pid}`,
            sessionId: typeof record.sessionId === "string" ? record.sessionId : null,
            cwd: typeof record.cwd === "string" ? record.cwd : null,
            status: unreachable
              ? "unreachable"
              : typeof record.status === "string"
                ? record.status
                : null,
            sessionStatus:
              typeof record.status === "string" ? record.status : null,
            verification: unreachable
              ? probe.state === "denied"
                ? "sandbox-denied"
                : "timeout"
              : "socket",
            errorCode: unreachable ? probe.errorCode : null,
            kind: typeof record.kind === "string" ? record.kind : null,
            peerProtocol: record.peerProtocol,
            socketPath: record.messagingSocketPath,
            address: claudePeerAddress(record.messagingSocketPath),
          };
        } catch {
          return null;
        }
      }),
  );
  return records.filter(Boolean).sort((left, right) => left.name.localeCompare(right.name));
}

export function resolveClaudePeer(peers, target) {
  if (!target?.trim()) throw new Error("Claude target must not be empty");
  const matches = peers.filter(
    (peer) =>
      peer.address === target ||
      peer.sessionId === target ||
      peer.name === target,
  );
  if (matches.length === 0) throw new Error(`unknown live Claude session: ${target}`);
  if (matches.length > 1) {
    throw new Error(`multiple live Claude sessions are named ${target}; use a session id or uds address`);
  }
  return matches[0];
}

export async function sendClaudePeerFrame(socketPath, frame) {
  const resolved = await validateClaudeSocketPath(socketPath);
  const encoded = `${JSON.stringify(frame)}\n`;
  if (Buffer.byteLength(encoded, "utf8") > MAX_CLAUDE_FRAME_BYTES) {
    throw new Error("Claude peer frame exceeds the size limit");
  }

  await new Promise((resolve, reject) => {
    const socket = net.createConnection({ path: resolved });
    let settled = false;
    const fail = (error) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(error);
    };
    socket.setTimeout(5_000, () => fail(new Error(`timed out sending to ${resolved}`)));
    socket.once("error", fail);
    socket.once("close", () => {
      if (settled) return;
      settled = true;
      resolve();
    });
    socket.once("connect", () => {
      socket.write(encoded, () => {
        if (process.platform === "darwin") setTimeout(() => socket.end(), 150);
        else socket.end();
      });
    });
  });
  return { messageId: frame.msg_id, socketPath: resolved };
}
