import path from "node:path";
import { createNodeIsogitRuntime } from "./node-runtime";
import { normalizeRuntime } from "./runtime";

/**
 * A Node runtime that records each rename and sync in `steps`, as
 * `rename <from> -> <to>` and `sync <path>` with paths relative to `dir`,
 * and fails each sync for which `failing.sync` returns true.
 */
export function recordingRuntime(dir: string) {
  const node = createNodeIsogitRuntime();
  const steps: string[] = [];
  const failing: { sync: (p: string) => boolean } = { sync: () => false };
  const name = (p: string) => path.relative(dir, p);
  const runtime = normalizeRuntime({
    ...node,
    rename: async (from, to) => {
      steps.push(`rename ${name(from)} -> ${name(to)}`);
      await node.rename(from, to);
    },
    sync: async (p) => {
      if (failing.sync(p)) throw new Error("sync failed");
      steps.push(`sync ${name(p)}`);
      await node.sync?.(p);
    },
  });
  return { runtime, steps, failing };
}
