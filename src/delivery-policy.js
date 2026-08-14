export const ROUTE_RECONCILE_GRACE_MS = 30_000;
export const MAX_WHEN_IDLE_DELAY_MS = 7 * 24 * 60 * 60 * 1_000;
export const SCHEDULED_WAKE_POLICIES = Object.freeze([
  "when-idle",
  "after-turn",
  "after-job",
]);
export const SCHEDULER_CLAIM_LEASE_MS = 30_000;
export const SCHEDULER_POLL_MS = 500;
export const SCHEDULER_HEARTBEAT_MS = 5_000;
export const SCHEDULER_HEARTBEAT_STALE_MS = 15_000;
export const SCHEDULED_DELIVERY_PER_TARGET_LIMIT = 256;
