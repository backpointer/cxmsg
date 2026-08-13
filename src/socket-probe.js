export function healthyProbe() {
  return { state: "healthy", errorCode: null, error: null };
}

export function failedProbe(error, fallback = "invalid") {
  const errorCode = error?.code || null;
  const state =
    errorCode === "EPERM" || errorCode === "EACCES"
      ? "denied"
      : errorCode === "ENOENT"
        ? "missing"
        : errorCode === "ECONNREFUSED" || errorCode === "ECONNRESET"
          ? "refused"
          : errorCode === "ETIMEDOUT"
            ? "timeout"
            : fallback;
  return {
    state,
    errorCode,
    error: error?.message || String(error || state),
  };
}

export function timeoutProbe(message = "Unix socket probe timed out") {
  return failedProbe(Object.assign(new Error(message), { code: "ETIMEDOUT" }));
}

export function isUnreachableProbe(probe) {
  return probe?.state === "denied" || probe?.state === "timeout";
}
