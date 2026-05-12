import path from "node:path";
import { stat } from "node:fs/promises";
import { createRequire } from "node:module";
import whichPkg from "which";

// Intentional: any stat failure means "does not exist" for our purposes.
export async function exists(p: string): Promise<boolean> {
  try {
    await stat(p);
    return true;
  } catch {
    return false;
  }
}

export async function which(cmd: string): Promise<string | undefined> {
  const r = await whichPkg(cmd, { nothrow: true });
  return typeof r === "string" ? r : undefined;
}

export function resolveModuleFrom(id: string, dir: string): string | undefined {
  try {
    return createRequire(path.join(dir, "package.json")).resolve(id);
  } catch {
    return undefined;
  }
}

export interface NearestRootContext {
  directory: string;
}

export function nearestRoot(
  includes: string[],
  excludes?: string[],
): (file: string, ctx: NearestRootContext) => Promise<string | undefined> {
  return async (file, ctx) => {
    const start = path.dirname(file);
    const stop = ctx.directory;

    if (excludes && (await findUpward(excludes, start, stop)) !== undefined) {
      return undefined;
    }
    const hit = await findUpward(includes, start, stop);
    if (hit === undefined) return ctx.directory;
    return path.dirname(hit);
  };
}

async function findUpward(
  targets: string[],
  start: string,
  stop: string,
): Promise<string | undefined> {
  let dir = path.resolve(start);
  const root = path.resolve(stop);
  while (true) {
    for (const t of targets) {
      const candidate = path.join(dir, t);
      if (await exists(candidate)) return candidate;
    }
    if (dir === root) return undefined;
    const parent = path.dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
}
