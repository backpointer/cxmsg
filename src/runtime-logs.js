import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fstatSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  renameSync,
  unlinkSync,
} from "node:fs";
import path from "node:path";
import { requireNoFollowFlag } from "./file-safety.js";
import { CXMSG_STATE_DIR } from "./runtime.js";

export const RUNTIME_LOG_MAX_BYTES = 1024 * 1024;
export const RUNTIME_LOG_ARCHIVES = 2;

function insideStateDirectory(target, stateDir) {
  const relative = path.relative(path.resolve(stateDir), path.resolve(target));
  return relative !== "" && relative !== ".." && !relative.startsWith(`..${path.sep}`);
}

function validatePrivateFile(target) {
  let metadata;
  try {
    metadata = lstatSync(target);
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
  if (
    !metadata.isFile() ||
    metadata.isSymbolicLink() ||
    metadata.nlink !== 1 ||
    (typeof process.getuid === "function" && metadata.uid !== process.getuid())
  ) {
    const error = new Error("runtime log failed owner-private identity validation");
    error.code = "ERUNTIMELOGIDENTITY";
    throw error;
  }
  return metadata;
}

function syncDirectory(directory) {
  const descriptor = openSync(directory, constants.O_RDONLY);
  try {
    fsyncSync(descriptor);
  } finally {
    closeSync(descriptor);
  }
}

export function openBoundedRuntimeLog(
  target,
  {
    stateDir = CXMSG_STATE_DIR,
    maxBytes = RUNTIME_LOG_MAX_BYTES,
    archives = RUNTIME_LOG_ARCHIVES,
  } = {},
) {
  if (!insideStateDirectory(target, stateDir)) {
    const error = new Error("runtime log must remain inside the cxmsg state directory");
    error.code = "ERUNTIMELOGPATH";
    throw error;
  }
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1024 || maxBytes > 16 * 1024 * 1024) {
    throw new Error("runtime log maxBytes must be 1024-16777216");
  }
  if (!Number.isSafeInteger(archives) || archives < 1 || archives > 8) {
    throw new Error("runtime log archives must be 1-8");
  }
  const directory = path.dirname(target);
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const directoryMetadata = lstatSync(directory);
  if (
    !directoryMetadata.isDirectory() ||
    directoryMetadata.isSymbolicLink() ||
    (typeof process.getuid === "function" && directoryMetadata.uid !== process.getuid())
  ) {
    const error = new Error("runtime log directory failed owner-private identity validation");
    error.code = "ERUNTIMELOGDIRECTORY";
    throw error;
  }
  chmodSync(directory, 0o700);
  const targetMetadata = validatePrivateFile(target);
  for (let index = 1; index <= archives; index += 1) {
    validatePrivateFile(`${target}.${index}`);
  }
  if (targetMetadata && targetMetadata.size >= maxBytes) {
    const oldest = `${target}.${archives}`;
    if (existsSync(oldest)) unlinkSync(oldest);
    for (let index = archives - 1; index >= 1; index -= 1) {
      const source = `${target}.${index}`;
      if (existsSync(source)) renameSync(source, `${target}.${index + 1}`);
    }
    renameSync(target, `${target}.1`);
    syncDirectory(directory);
  }
  const descriptor = openSync(
    target,
    constants.O_CREAT |
      constants.O_APPEND |
      constants.O_WRONLY |
      requireNoFollowFlag(),
    0o600,
  );
  try {
    const metadata = fstatSync(descriptor);
    if (
      !metadata.isFile() ||
      metadata.nlink !== 1 ||
      (typeof process.getuid === "function" && metadata.uid !== process.getuid())
    ) {
      const error = new Error("opened runtime log failed owner-private identity validation");
      error.code = "ERUNTIMELOGIDENTITY";
      throw error;
    }
    chmodSync(target, 0o600);
    syncDirectory(directory);
    return descriptor;
  } catch (error) {
    closeSync(descriptor);
    throw error;
  }
}

export function appServerLogEnvironment(environment = process.env) {
  return {
    ...environment,
    NO_COLOR: "1",
    RUST_LOG:
      environment.CXMSG_APP_SERVER_RUST_LOG ||
      "warn,codex_core::tools::router=off",
  };
}
