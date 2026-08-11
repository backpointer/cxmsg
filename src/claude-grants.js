import { randomUUID } from "node:crypto";

export const DEFAULT_CLAUDE_PERMISSION_PROFILE = ":read-only";

export function validateClaudeSessionId(sessionId) {
  if (!/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(sessionId || "")) {
    throw new Error("Claude session-id must be a UUID");
  }
  return sessionId;
}

export function listClaudeRequestGrants(record) {
  return Array.isArray(record?.allowedClaudeRequesters)
    ? record.allowedClaudeRequesters.filter(
        (grant) =>
          grant &&
          typeof grant.sessionId === "string" &&
          typeof grant.permissions === "string" &&
          typeof grant.token === "string",
      )
    : [];
}

export function findClaudeRequestGrant(record, source) {
  if (!source?.grantToken) return null;
  return (
    listClaudeRequestGrants(record).find(
      (grant) =>
        grant.token === source.grantToken &&
        (!source.fromSession || grant.sessionId === source.fromSession),
    ) || null
  );
}

export function upsertClaudeRequestGrant(
  record,
  peer,
  permissions,
  token = randomUUID(),
) {
  const sessionId = validateClaudeSessionId(peer?.sessionId);
  validateClaudeSessionId(token);
  if (!permissions?.trim()) throw new Error("permission profile is required");
  const grants = listClaudeRequestGrants(record).filter(
    (grant) => grant.sessionId !== sessionId,
  );
  grants.push({
    sessionId,
    name: peer.name || null,
    address: peer.address || null,
    permissions,
    token,
    grantedAt: new Date().toISOString(),
  });
  return {
    ...record,
    allowedClaudeRequesters: grants.sort((left, right) =>
      left.sessionId.localeCompare(right.sessionId),
    ),
  };
}

export function publicClaudeRequestGrant(grant) {
  return {
    sessionId: grant.sessionId,
    name: grant.name,
    address: grant.address,
    permissions: grant.permissions,
    tokenHint: `${grant.token.slice(0, 8)}…`,
    grantedAt: grant.grantedAt,
  };
}

export function removeClaudeRequestGrant(record, sessionId) {
  validateClaudeSessionId(sessionId);
  return {
    ...record,
    allowedClaudeRequesters: listClaudeRequestGrants(record).filter(
      (grant) => grant.sessionId !== sessionId,
    ),
  };
}
