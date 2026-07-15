#!/usr/bin/env bun
/* eslint-disable no-console */

// Refresh the workspace member versions recorded in `bun.lock` to a release
// version. `bin/release` bumps every workspace package.json version, but bun
// does not rewrite the version it records for each workspace member in
// `bun.lock` — plain `bun install`, `--force`, and `--lockfile-only` all
// leave it stale. `bun pm pack` derives the `workspace:*` -> concrete
// dependency rewrite from that recorded version, so a stale lockfile would
// publish internal dependencies pinned to the previous version: an
// unresolvable graph on npm, the exact failure this project fixes.
//
// Deleting and regenerating the lockfile refreshes those versions but
// re-resolves the whole third-party graph, drifting hundreds of lines
// within semver ranges — unreviewed churn in a release commit. Instead this
// edits only the `version` values inside the top-level `workspaces` section
// (the sole place workspace versions live; third-party packages use a
// different array syntax under `packages`), preserving bun's exact format
// everywhere else. The section is located by brace-matching and the result
// is re-parsed to confirm every workspace version is the release version.

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { type } from "arktype";

/** The character-level string-scan state: whether the scanner is inside a
 *  JSON string, and whether the previous character escaped the next one. */
interface StringScan {
  inString: boolean;
  escaped: boolean;
}

/** Advance the string-scan state by one character, so a structural scan can
 *  tell whether a brace or comma sits inside a string value (where it must be
 *  ignored) or outside it. */
function stepStringScan(c: string | undefined, s: StringScan): StringScan {
  if (s.inString) {
    if (s.escaped) return { inString: true, escaped: false };
    if (c === "\\") return { inString: true, escaped: true };
    if (c === '"') return { inString: false, escaped: false };
    return { inString: true, escaped: false };
  }
  if (c === '"') return { inString: true, escaped: false };
  return { inString: false, escaped: false };
}

/** Drop the trailing commas (before `}`/`]`) that appear outside strings —
 *  the only JSON5 feature bun.lock uses — so the result parses as JSON. A
 *  naive regex would also strip a comma inside a string value; tracking
 *  string state keeps such a value intact. */
function stripTrailingCommas(text: string): string {
  let out = "";
  let scan: StringScan = { inString: false, escaped: false };
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    let drop = false;
    if (!scan.inString && c === ",") {
      let j = i + 1;
      while (
        j < text.length &&
        (text[j] === " " ||
          text[j] === "\t" ||
          text[j] === "\n" ||
          text[j] === "\r")
      ) {
        j++;
      }
      drop = text[j] === "}" || text[j] === "]";
    }
    if (!drop) out += c;
    scan = stepStringScan(c, scan);
  }
  return out;
}

/** Parse `bun.lock`, which is JSONC (trailing commas before `}`/`]`). */
function parseLockfile(text: string): unknown {
  return JSON.parse(stripTrailingCommas(text));
}

const lockfileSchema = type({
  workspaces: { "[string]": { "version?": "string" } },
});

/** The version recorded for each workspace member that declares one. */
export function workspaceVersions(lockText: string): Record<string, string> {
  const parsed = lockfileSchema(parseLockfile(lockText));
  if (parsed instanceof type.errors) {
    throw new Error(
      `sync-workspace-lockfile: bun.lock is not shaped as expected: ${parsed.summary}`,
    );
  }
  const versions: Record<string, string> = {};
  for (const [path, entry] of Object.entries(parsed.workspaces)) {
    if (entry.version !== undefined) versions[path] = entry.version;
  }
  return versions;
}

/** Index of the `}` matching the `{` at `open`, skipping brace characters
 *  inside strings. */
function matchingBrace(text: string, open: number): number {
  let depth = 0;
  let scan: StringScan = { inString: false, escaped: false };
  for (let i = open; i < text.length; i++) {
    const c = text[i];
    if (!scan.inString) {
      if (c === "{") depth += 1;
      else if (c === "}" && (depth -= 1) === 0) return i;
    }
    scan = stepStringScan(c, scan);
  }
  throw new Error(
    "sync-workspace-lockfile: unbalanced braces in the workspaces section",
  );
}

/** Set every workspace member's own `version` to `version`, editing only
 *  each member's version field inside the `workspaces` section and leaving
 *  the rest of the lockfile byte-identical. Throws unless the result
 *  re-parses with every workspace version equal to `version`. */
export function rewriteWorkspaceVersions(
  lockText: string,
  version: string,
): string {
  const keyIndex = lockText.indexOf('"workspaces":');
  if (keyIndex === -1) {
    throw new Error(
      "sync-workspace-lockfile: no workspaces section in bun.lock",
    );
  }
  const sectionOpen = lockText.indexOf("{", keyIndex);
  if (sectionOpen === -1) {
    throw new Error("sync-workspace-lockfile: malformed workspaces section");
  }
  const sectionClose = matchingBrace(lockText, sectionOpen);
  const members = Object.keys(workspaceVersions(lockText));
  let section = lockText.slice(sectionOpen, sectionClose + 1);

  // Edit each member's own `version` — the first `version` in its block,
  // which precedes any `dependencies` object. Scoping to the block (rather
  // than a section-wide replace) leaves a dependency that happens to be
  // named `version` untouched.
  for (const path of members) {
    const marker = section.indexOf(`"${path}": {`);
    if (marker === -1) {
      throw new Error(
        `sync-workspace-lockfile: workspace member ${path} not found`,
      );
    }
    const blockOpen = section.indexOf("{", marker);
    const blockClose = matchingBrace(section, blockOpen);
    const block = section.slice(blockOpen, blockClose + 1);
    const rewritten = block.replace(
      /("version": ")[^"]*/,
      (_match, prefix: string) => prefix + version,
    );
    section =
      section.slice(0, blockOpen) + rewritten + section.slice(blockClose + 1);
  }
  const text =
    lockText.slice(0, sectionOpen) + section + lockText.slice(sectionClose + 1);

  // Re-parse (round-trip) and confirm every workspace version was updated.
  const stale = Object.entries(workspaceVersions(text)).filter(
    ([, v]) => v !== version,
  );
  if (stale.length > 0) {
    throw new Error(
      `sync-workspace-lockfile: ${stale.length} workspace version(s) not set ` +
        `to ${version}: ${stale.map(([path]) => path).join(", ")}`,
    );
  }
  return text;
}

if (import.meta.main) {
  const version = process.argv[2];
  if (
    version === undefined ||
    !/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)
  ) {
    throw new Error("usage: sync-workspace-lockfile <semver-version>");
  }
  if (import.meta.dirname === undefined) {
    throw new Error(
      "sync-workspace-lockfile: import.meta.dirname is undefined; cannot locate bun.lock",
    );
  }
  const lockPath = join(import.meta.dirname, "..", "bun.lock");
  const before = readFileSync(lockPath, "utf8");
  const count = Object.keys(workspaceVersions(before)).length;
  writeFileSync(lockPath, rewriteWorkspaceVersions(before, version));
  console.log(
    `sync-workspace-lockfile: set ${count} workspace versions to ${version}`,
  );
}
