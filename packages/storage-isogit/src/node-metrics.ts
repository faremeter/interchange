import fs from "node:fs";
import path from "node:path";

import { hasCode } from "@intx/types";
import type { RepoDiskUsage, RepoObjectCounts } from "./repo-disk";

// Synchronous Node counterparts to the runtime-backed helpers in repo-disk.ts.
// Keep their counting and absence semantics aligned.

function countDirEntries(dir: string): number {
  try {
    return fs.readdirSync(dir).length;
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return 0;
    throw cause;
  }
}

export function countLooseObjects(repoDir: string): number {
  const objectsDir = path.join(repoDir, ".git", "objects");
  let fanoutDirs: string[];
  try {
    fanoutDirs = fs.readdirSync(objectsDir);
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return 0;
    throw cause;
  }
  let total = 0;
  for (const name of fanoutDirs) {
    if (name === "pack" || name === "info") continue;
    if (!/^[0-9a-f]{2}$/.test(name)) continue;
    total += countDirEntries(path.join(objectsDir, name));
  }
  return total;
}

export function countPackFiles(repoDir: string): number {
  const packDir = path.join(repoDir, ".git", "objects", "pack");
  try {
    return fs.readdirSync(packDir).filter((name) => name.endsWith(".pack"))
      .length;
  } catch (cause) {
    if (hasCode(cause) && cause.code === "ENOENT") return 0;
    throw cause;
  }
}

export function gitBytes(repoDir: string): number {
  let total = 0;
  const stack = [path.join(repoDir, ".git")];
  while (stack.length > 0) {
    const current = stack.pop();
    if (current === undefined) break;
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(current);
    } catch (cause) {
      if (hasCode(cause) && cause.code === "ENOENT") continue;
      throw cause;
    }
    if (stat.isDirectory()) {
      for (const child of fs.readdirSync(current)) {
        stack.push(path.join(current, child));
      }
    } else if (stat.isFile()) {
      total += stat.size;
    }
  }
  return total;
}

export function repoObjectCounts(dir: string): RepoObjectCounts {
  return {
    packCount: countPackFiles(dir),
    looseObjectCount: countLooseObjects(dir),
  };
}

export function repoDiskUsage(dir: string): RepoDiskUsage {
  return {
    gitBytes: gitBytes(dir),
    ...repoObjectCounts(dir),
  };
}
