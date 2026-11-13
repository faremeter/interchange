import path from "node:path";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { spawn } from "./launch";
import { nearestRoot, exists, which, resolveModuleFrom } from "./fs";

export interface ServerContext {
  directory: string;
  worktree: string;
  /** A tsserver script to run instead of the workspace TypeScript's own.
   *  TypeScript 7 ships none, so its workspaces need one from an older
   *  install. The plugin checks that it exists. */
  tsserverPath?: string;
}

export interface ServerHandle {
  process: ChildProcessWithoutNullStreams;
  initialization?: Record<string, unknown>;
}

export interface ServerInfo {
  id: string;
  extensions: string[];
  seedsInitialDiagnostics?: boolean;
  root: (file: string, ctx: ServerContext) => Promise<string | undefined>;
  spawn: (
    root: string,
    ctx: ServerContext,
  ) => Promise<ServerHandle | undefined>;
}

export const Typescript: ServerInfo = {
  id: "typescript",
  extensions: [".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".mts", ".cts"],
  seedsInitialDiagnostics: true,
  root: nearestRoot(
    [
      "package-lock.json",
      "bun.lockb",
      "bun.lock",
      "pnpm-lock.yaml",
      "yarn.lock",
    ],
    ["deno.json", "deno.jsonc"],
  ),
  async spawn(root, ctx) {
    const tsserver =
      ctx.tsserverPath ??
      resolveModuleFrom("typescript/lib/tsserver.js", ctx.directory);
    if (tsserver === undefined) return undefined;

    const localBin = path.join(
      root,
      "node_modules",
      ".bin",
      "typescript-language-server",
    );
    const bin = (await exists(localBin))
      ? localBin
      : await which("typescript-language-server");
    if (bin === undefined) return undefined;

    return {
      process: spawn(bin, ["--stdio"], { cwd: root }),
      initialization: { tsserver: { path: tsserver } },
    };
  },
};
