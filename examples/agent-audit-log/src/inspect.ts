// Walk an agent's contextDir as a git repository and report what is
// in each commit. `isomorphic-git` is the same library the agent's
// isogit store uses, so the audit data the agent wrote is readable
// with no extra format-translation step.

import * as fs from "node:fs";

import { type } from "arktype";
import git from "isomorphic-git";

const ManifestRecord = type({
  strategy: "string",
  "+": "delete",
});

const NotFoundError = type({
  code: "'NotFoundError'",
  "+": "delete",
});

export type AuditCommitSummary = {
  hash: string;
  message: string;
  /** Commit time as ms-since-epoch (matches `ContextCommit.timestamp`). */
  timestamp: number;
  /** File names at the commit's tree root. */
  files: string[];
  /** Strategy names captured in manifest.jsonl, in order. */
  manifestStrategies: string[];
};

/**
 * Read up to `depth` commits from `contextDir` (newest first) and
 * return a structured summary per commit. Throws if the directory
 * is not a git repository.
 */
export async function summarizeAuditLog(
  contextDir: string,
  depth = 10,
): Promise<AuditCommitSummary[]> {
  const commits = await git.log({ fs, dir: contextDir, depth });
  const summaries: AuditCommitSummary[] = [];
  for (const entry of commits) {
    const tree = await git.readTree({ fs, dir: contextDir, oid: entry.oid });
    const files = tree.tree.map((t) => t.path);
    const manifestStrategies = await readManifestStrategies(
      contextDir,
      entry.oid,
    );
    summaries.push({
      hash: entry.oid,
      message: entry.commit.message.trim(),
      timestamp: entry.commit.author.timestamp * 1000,
      files,
      manifestStrategies,
    });
  }
  return summaries;
}

async function readManifestStrategies(
  contextDir: string,
  commitOid: string,
): Promise<string[]> {
  // Narrow the try/catch to git.readBlob — that is the only call
  // whose absence we want to translate into "no strategies". A bad
  // manifest.jsonl (parse failure, schema drift) is a real audit
  // corruption signal and must surface, not get swallowed alongside
  // the missing-file case.
  let blob: Uint8Array;
  try {
    const result = await git.readBlob({
      fs,
      dir: contextDir,
      oid: commitOid,
      filepath: "manifest.jsonl",
    });
    blob = result.blob;
  } catch (err) {
    // The very first commit (init) does not carry manifest.jsonl;
    // isomorphic-git tags that with `code: "NotFoundError"`. Anything
    // else propagates.
    if (isNotFoundError(err)) return [];
    throw err;
  }

  const text = new TextDecoder().decode(blob);
  const strategies: string[] = [];
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i] ?? "";
    if (line.length === 0) continue;
    // The manifest is structured audit data; treat malformed lines as
    // corruption rather than silently dropping them. The error names
    // the file, commit, line number, and the offending content so an
    // operator can locate it without grepping the repo.
    const raw: unknown = JSON.parse(line);
    const validated = ManifestRecord(raw);
    if (validated instanceof type.errors) {
      const preview = line.length > 120 ? line.slice(0, 117) + "..." : line;
      throw new Error(
        `manifest.jsonl in commit ${commitOid} line ${String(i + 1)} is not a \`{ strategy: string }\` record (${validated.summary}); line content: ${preview}`,
      );
    }
    strategies.push(validated.strategy);
  }
  return strategies;
}

function isNotFoundError(err: unknown): boolean {
  return !(NotFoundError(err) instanceof type.errors);
}
