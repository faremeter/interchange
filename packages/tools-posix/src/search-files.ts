import type { Dirent } from "node:fs";
import { readdir, stat } from "node:fs/promises";
import { resolve, join } from "node:path";

import { hasCode } from "./errors";
import { globToRegex, shouldSkip } from "./glob-match";

export type SearchFilesArgs = {
  pattern: string;
  path?: string;
  max_results?: number;
};

const DEFAULT_MAX_RESULTS = 1000;

export async function runSearchFiles(
  args: SearchFilesArgs,
  signal: AbortSignal,
): Promise<string> {
  signal.throwIfAborted();

  const basePath = resolve(args.path ?? process.cwd());
  const maxResults = args.max_results ?? DEFAULT_MAX_RESULTS;
  const regex = globToRegex(args.pattern);

  let info;
  try {
    info = await stat(basePath);
  } catch (err) {
    if (hasCode(err)) {
      if (err.code === "ENOENT") {
        throw new Error(`directory not found: ${basePath}`, { cause: err });
      }
      if (err.code === "EACCES") {
        throw new Error(`permission denied: ${basePath}`, { cause: err });
      }
    }
    throw err;
  }

  if (!info.isDirectory()) {
    throw new Error(`path is not a directory: ${basePath}`);
  }

  signal.throwIfAborted();

  let entries: Dirent[];
  try {
    entries = await readdir(basePath, { recursive: true, withFileTypes: true });
  } catch (err) {
    if (hasCode(err) && err.code === "EACCES") {
      throw new Error(`permission denied reading directory: ${basePath}`, {
        cause: err,
      });
    }
    throw err;
  }

  const matches: string[] = [];
  let totalMatches = 0;

  for (const entry of entries) {
    signal.throwIfAborted();
    if (!entry.isFile() && !entry.isSymbolicLink()) continue;
    const relativePath = join(entry.parentPath, entry.name).slice(
      basePath.length + 1,
    );
    if (shouldSkip(relativePath)) continue;
    if (regex.test(relativePath)) {
      totalMatches++;
      if (matches.length < maxResults) {
        matches.push(relativePath);
      }
    }
  }

  if (totalMatches === 0) {
    return `no files matching "${args.pattern}"`;
  }

  let result = matches.join("\n");
  if (totalMatches > maxResults) {
    result += `\n... (${maxResults} of ${totalMatches} matches shown)`;
  }
  return result;
}
