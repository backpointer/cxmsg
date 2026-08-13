import { spawnSync } from "node:child_process";

export function processState(pid, kill = process.kill.bind(process)) {
  if (!Number.isSafeInteger(pid) || pid <= 1) return "missing";
  try {
    kill(pid, 0);
    return "alive";
  } catch (error) {
    if (error?.code === "ESRCH") return "missing";
    if (error?.code === "EPERM") return "unverified";
    throw error;
  }
}

export function processIdentity(
  pid,
  expectedFragments,
  run = spawnSync,
) {
  const result = run("ps", ["-p", String(pid), "-o", "command="], {
    encoding: "utf8",
  });
  if (result.status !== 0) return { state: "unavailable", command: null };
  const command = result.stdout.trim();
  return {
    state: expectedFragments.every((fragment) => command.includes(fragment))
      ? "matched"
      : "mismatched",
    command,
  };
}

export function serviceEvidence({ process, identity, socketHealthy }) {
  return {
    running: Boolean(socketHealthy),
    safeToSignal: process === "alive" && identity === "matched",
    safeToRemove: process === "missing" && !socketHealthy,
  };
}
