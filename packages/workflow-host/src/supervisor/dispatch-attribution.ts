// D2 per-leg substrate-attribution instrumentation (measurement-only).
//
// This module is the off-by-default observability + A/B surface the D2
// latency re-attribution drives. NONE of it runs in production: the
// supervisor only reaches these helpers when a `onDispatchTiming`
// observer is wired (the same env-gated seam the 4.7 latency gate added)
// and only consults the repack toggle when the boot edge supplies a
// non-zero repack interval. With the observer unwired and the toggle
// absent, the supervisor samples no clock, reads no directory, and forks
// no `git gc` -- the dispatch path is byte-for-byte unchanged.
//
// The counters are deliberately filesystem-level reads against the
// workflow-run repo's on-disk working tree (resolved via
// `RepoStore.getRepoDir`, a pure path computation), NOT reaches into
// isogit internals. That keeps the attribution honest about what it can
// observe cheaply and avoids coupling the measurement to the storage
// layer's private object model. `runs/` and `addresses/<addr>/consumed/`
// fan-out is exactly the never-pruned tree growth the design (§9)
// implicates; loose-object count + `.git` byte size are the pack-growth
// proxies the §10c repack A/B discriminates against.

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import type { DispatchStructuralCounters } from "./types";

const RUNS_DIR = "runs";
const ADDRESSES_DIR = "addresses";
const CONSUMED_DIR = "consumed";

/**
 * Count the immediate child entries of `dir`. Returns 0 when the
 * directory does not exist yet (the repo's first commit has not created
 * the subtree) -- absence is a real "fan-out is zero", not an error to
 * surface, because the sampler runs on every leg including ones that fire
 * before the subtree exists.
 */
function countEntries(dir: string): number {
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch (cause) {
    if (
      cause instanceof Error &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return 0;
    }
    throw cause;
  }
  return entries.length;
}

/**
 * Total entries under every `addresses/<segment>/consumed/`. The address
 * segment is `urlEncoded(address)` on disk; rather than re-deriving the
 * exact encoding (and risking drift from the kind handler), enumerate the
 * address segments present and sum each one's `consumed/` fan-out. For a
 * single warm deployment there is exactly one segment, but the sum is
 * correct for any number.
 */
function countConsumed(repoDir: string): number {
  const addressesDir = path.join(repoDir, ADDRESSES_DIR);
  let segments: string[];
  try {
    segments = fs.readdirSync(addressesDir);
  } catch (cause) {
    if (
      cause instanceof Error &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return 0;
    }
    throw cause;
  }
  let total = 0;
  for (const segment of segments) {
    total += countEntries(path.join(addressesDir, segment, CONSUMED_DIR));
  }
  return total;
}

/**
 * Count loose git objects under `.git/objects/<xx>/`. Loose objects are
 * the un-packed per-commit objects isogit writes on each commit; their
 * count rising with message index (and collapsing after a repack) is the
 * pack-growth signature the §10c A/B looks for. The two-hex-char fan-out
 * dirs plus `pack`/`info` are the only children of `objects/`; the latter
 * two are skipped.
 */
function countLooseObjects(repoDir: string): number {
  const objectsDir = path.join(repoDir, ".git", "objects");
  let fanoutDirs: string[];
  try {
    fanoutDirs = fs.readdirSync(objectsDir);
  } catch (cause) {
    if (
      cause instanceof Error &&
      (cause as NodeJS.ErrnoException).code === "ENOENT"
    ) {
      return 0;
    }
    throw cause;
  }
  let total = 0;
  for (const name of fanoutDirs) {
    if (name === "pack" || name === "info") continue;
    // Loose-object fan-out dirs are exactly two lowercase hex chars.
    if (!/^[0-9a-f]{2}$/.test(name)) continue;
    total += countEntries(path.join(objectsDir, name));
  }
  return total;
}

/**
 * Total byte size of the repo's `.git` directory (loose + pack + refs +
 * logs). A coarse repo-size proxy: if a leg's time tracks this
 * sublinearly-but-growing while loose-object count stays bounded, the
 * cost is pack growth; if it tracks `runs/`/`consumed/` fan-out linearly,
 * it is tree-rewrite. Walks the tree with `fs.statSync`; bounded by the
 * repo size, which is the thing being measured.
 */
function gitBytes(repoDir: string): number {
  const gitDir = path.join(repoDir, ".git");
  let total = 0;
  const stack: string[] = [gitDir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (current === undefined) break;
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(current);
    } catch (cause) {
      if (
        cause instanceof Error &&
        (cause as NodeJS.ErrnoException).code === "ENOENT"
      ) {
        continue;
      }
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

/**
 * Sample the four structural counters against the workflow-run repo's
 * on-disk working tree. Cheap filesystem reads; invoked only when the
 * D2 observer is wired, at each leg's `"end"` mark, so the per-leg slope
 * can be correlated with the grower that explains it.
 */
export function sampleStructuralCounters(
  repoDir: string,
): DispatchStructuralCounters {
  return {
    runsFanOut: countEntries(path.join(repoDir, RUNS_DIR)),
    consumedFanOut: countConsumed(repoDir),
    looseObjects: countLooseObjects(repoDir),
    gitBytes: gitBytes(repoDir),
  };
}

/**
 * Measurement-only forced-repack toggle for the §10c A/B. When the boot
 * edge supplies a non-zero `everyMessages`, the supervisor invokes
 * `maybeRepack` once per dispatched message (after `markConsumed`, still
 * under the single-writer discipline -- the dispatch loop processes one
 * message at a time and no concurrent commit is in flight at that point),
 * and every `everyMessages`-th message forces a `git gc`/repack of the
 * workflow-run repo. If forcing a repack flattens the per-leg slope, the
 * cost is loose-object/pack growth (cheap pack/gc fix); if it does not,
 * the cost is the per-commit root-tree rewrite scaling with `runs/` +
 * `consumed/` fan-out (run-model change). Absent toggle => never repacks.
 */
export type RepackToggle = {
  everyMessages: number;
};

/**
 * Force a synchronous `git gc` of the repo at `repoDir`. Synchronous
 * (`spawnSync`) so the supervisor's single-writer invariant is trivially
 * preserved: the dispatch loop is the sole caller and blocks here, so no
 * `writeTreePreservingPrefix` commit can interleave with the repack.
 * Returns the wall-clock duration in ms and whether the gc succeeded;
 * a non-zero exit is surfaced (not swallowed) so a broken A/B run does
 * not silently report "repack had no effect".
 */
export function forceRepack(repoDir: string): {
  ok: boolean;
  durationMs: number;
  detail: string;
} {
  const t0 = performance.now();
  const result = spawnSync(
    "git",
    ["-C", repoDir, "gc", "--quiet", "--prune=now"],
    { encoding: "utf8" },
  );
  const durationMs = performance.now() - t0;
  if (result.error !== undefined) {
    return { ok: false, durationMs, detail: result.error.message };
  }
  if (result.status !== 0) {
    return {
      ok: false,
      durationMs,
      detail: `git gc exited ${String(result.status)}: ${result.stderr}`,
    };
  }
  return { ok: true, durationMs, detail: "" };
}
