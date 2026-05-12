import path from "node:path";

export interface LSPContext {
  cwd: string;
  worktree: string;
}

export function containsPath(filepath: string, ctx: LSPContext): boolean {
  if (isWithin(ctx.cwd, filepath)) return true;
  if (ctx.worktree === "/") return false;
  return isWithin(ctx.worktree, filepath);
}

function isWithin(parent: string, child: string): boolean {
  const rel = path.relative(path.resolve(parent), path.resolve(child));
  if (rel === "") return true;
  if (rel.startsWith("..")) return false;
  if (path.isAbsolute(rel)) return false;
  return true;
}
