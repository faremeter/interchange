// This module is Node-bound: it reads the filesystem through node:fs and is
// not portable to environments without that API.

import type { Dirent } from "node:fs";
import { readFile, readdir, stat } from "node:fs/promises";
import { resolve, relative, join } from "node:path";

import { hasCode } from "@intx/types";

import { globToRegex, shouldSkip } from "./glob-match";

export type GrepArgs = {
  pattern: string;
  path?: string;
  glob?: string;
  context?: number;
  max_results?: number;
};

const DEFAULT_MAX_RESULTS = 500;

type Match = {
  file: string;
  lineNumber: number;
  line: string;
};

type FileSearchResult = {
  matches: Match[];
  lines: string[];
};

function isBinary(buf: Uint8Array): boolean {
  return buf.includes(0);
}

async function searchFile(
  filePath: string,
  regex: RegExp,
  signal: AbortSignal,
): Promise<FileSearchResult | null> {
  signal.throwIfAborted();

  let buf: Uint8Array;
  try {
    buf = await readFile(filePath, { signal });
  } catch (err) {
    if (
      hasCode(err) &&
      (err.code === "EISDIR" || err.code === "EACCES" || err.code === "ENOENT")
    ) {
      return null;
    }
    throw err;
  }

  if (isBinary(buf)) return null;

  const lines = new TextDecoder().decode(buf).split("\n");
  const matches: Match[] = [];

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? "";
    if (regex.test(line)) {
      matches.push({ file: filePath, lineNumber: i + 1, line });
    }
  }

  if (matches.length === 0) return null;

  return { matches, lines };
}

/**
 * Format matches with optional context lines. When context is requested,
 * we derive context from the cached file lines to avoid duplicating
 * lines when matches are adjacent.
 */
function formatMatches(
  matches: Match[],
  contextLines: number,
  fileLines: Map<string, string[]>,
): string {
  if (contextLines === 0) {
    return matches.map((m) => `${m.file}:${m.lineNumber}:${m.line}`).join("\n");
  }

  // Group matches by file to deduplicate context across adjacent matches
  const groups = new Map<string, Match[]>();
  for (const m of matches) {
    const list = groups.get(m.file) ?? [];
    list.push(m);
    groups.set(m.file, list);
  }

  const parts: string[] = [];
  let firstGroup = true;

  for (const [file, fileMatches] of groups) {
    const lines = fileLines.get(file);
    if (lines === undefined) continue;

    const matchLineNums = new Set(fileMatches.map((m) => m.lineNumber));

    // Build ranges of lines to print (match lines + context), merged
    const ranges: { start: number; end: number }[] = [];
    for (const m of fileMatches) {
      const start = Math.max(1, m.lineNumber - contextLines);
      const end = Math.min(lines.length, m.lineNumber + contextLines);
      const prev = ranges[ranges.length - 1];
      if (prev !== undefined && start <= prev.end + 1) {
        prev.end = end;
      } else {
        ranges.push({ start, end });
      }
    }

    for (const [ri, range] of ranges.entries()) {
      if (!firstGroup || ri > 0) {
        parts.push("--");
      }
      firstGroup = false;
      for (let ln = range.start; ln <= range.end; ln++) {
        const lineContent = lines[ln - 1] ?? "";
        const sep = matchLineNums.has(ln) ? ":" : "-";
        parts.push(`${file}${sep}${ln}${sep}${lineContent}`);
      }
    }
  }

  return parts.join("\n");
}

export async function runGrep(
  args: GrepArgs,
  signal: AbortSignal,
): Promise<string> {
  signal.throwIfAborted();

  let regex: RegExp;
  try {
    regex = new RegExp(args.pattern);
  } catch (err) {
    throw new Error(
      `invalid regex: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  const basePath = resolve(args.path ?? process.cwd());
  const contextLines = args.context ?? 0;
  const maxResults = args.max_results ?? DEFAULT_MAX_RESULTS;
  const globFilter = args.glob !== undefined ? globToRegex(args.glob) : null;

  let info;
  try {
    info = await stat(basePath);
  } catch (err) {
    if (hasCode(err)) {
      if (err.code === "ENOENT") {
        throw new Error(`path not found: ${basePath}`, { cause: err });
      }
      if (err.code === "EACCES") {
        throw new Error(`permission denied: ${basePath}`, { cause: err });
      }
    }
    throw err;
  }

  const isDir = info.isDirectory();
  let filePaths: string[];

  if (info.isFile()) {
    filePaths = [basePath];
  } else if (isDir) {
    let entries: Dirent[];
    try {
      entries = await readdir(basePath, {
        recursive: true,
        withFileTypes: true,
      });
    } catch (err) {
      if (hasCode(err) && err.code === "EACCES") {
        throw new Error(`permission denied reading directory: ${basePath}`, {
          cause: err,
        });
      }
      throw err;
    }
    filePaths = [];
    for (const entry of entries) {
      if (!entry.isFile() && !entry.isSymbolicLink()) continue;
      const relativePath = join(entry.parentPath, entry.name).slice(
        basePath.length + 1,
      );
      if (shouldSkip(relativePath)) continue;
      if (globFilter !== null && !globFilter.test(relativePath)) continue;
      filePaths.push(resolve(basePath, relativePath));
    }
  } else {
    throw new Error(`path is not a file or directory: ${basePath}`);
  }

  const allMatches: Match[] = [];
  const fileLinesCache = new Map<string, string[]>();
  let totalMatches = 0;

  for (const fp of filePaths) {
    signal.throwIfAborted();

    const result = await searchFile(fp, regex, signal);
    if (result === null) continue;

    totalMatches += result.matches.length;

    const displayPath = isDir ? relative(basePath, fp) : fp;

    for (const m of result.matches) {
      if (allMatches.length < maxResults) {
        allMatches.push({ ...m, file: displayPath });
      }
    }

    // Cache lines from the single read for context rendering
    if (contextLines > 0 && allMatches.length <= maxResults) {
      fileLinesCache.set(displayPath, result.lines);
    }
  }

  if (totalMatches === 0) {
    return `no matches for /${args.pattern}/`;
  }

  let output = formatMatches(allMatches, contextLines, fileLinesCache);
  if (totalMatches > maxResults) {
    output += `\n... (${maxResults} of ${totalMatches} matches shown)`;
  }
  return output;
}
