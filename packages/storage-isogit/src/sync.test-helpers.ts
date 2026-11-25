import fs from "node:fs";
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

/** `root` and every file and directory under it, relative to `dir`. */
export async function treeOf(dir: string, root: string): Promise<string[]> {
  const entries = [path.relative(dir, root)];
  for (const entry of await fs.promises.readdir(root, {
    withFileTypes: true,
  })) {
    const full = path.join(root, entry.name);
    if (entry.isDirectory()) entries.push(...(await treeOf(dir, full)));
    else entries.push(path.relative(dir, full));
  }
  return entries;
}
