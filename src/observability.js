import { appendFileSync, chmodSync, mkdirSync } from "node:fs";
import { CXMSG_STATE_DIR, EVENT_LOG_PATH } from "./runtime.js";

const SAFE_VALUE_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

function safeValue(value) {
  if (value === null || value === undefined || value === "") return null;
  const text = String(value);
  return SAFE_VALUE_PATTERN.test(text) ? text : "redacted";
}

export function coordinationEvent(fields) {
  const event = {
    protocol: "cxmsg-event/1",
    timestamp: new Date().toISOString(),
    kind: safeValue(fields.kind),
    phase: safeValue(fields.phase),
    correlationId: safeValue(fields.correlationId),
    target: safeValue(fields.target),
    attempt: Number.isSafeInteger(fields.attempt) ? fields.attempt : null,
    outcome: safeValue(fields.outcome),
    errorCode: safeValue(fields.errorCode),
    late: fields.late === true,
  };
  return Object.fromEntries(
    Object.entries(event).filter(([, value]) => value !== null),
  );
}

export function writeCoordinationEvent(fields) {
  try {
    mkdirSync(CXMSG_STATE_DIR, { recursive: true, mode: 0o700 });
    chmodSync(CXMSG_STATE_DIR, 0o700);
    const event = coordinationEvent(fields);
    appendFileSync(EVENT_LOG_PATH, `${JSON.stringify(event)}\n`, {
      encoding: "utf8",
      mode: 0o600,
    });
    chmodSync(EVENT_LOG_PATH, 0o600);
    return event;
  } catch {
    return null;
  }
}
