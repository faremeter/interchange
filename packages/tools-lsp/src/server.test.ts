import { afterEach, describe, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Typescript, type ServerHandle } from "./server";

const roots: string[] = [];

afterEach(async () => {
  for (const root of roots.splice(0)) {
    await rm(root, { recursive: true, force: true });
  }
});

// A workspace with typescript-language-server on its bin path but a
// TypeScript that ships no tsserver script, as TypeScript 7 does.
async function workspaceWithoutTsserver(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "lsp-server-test-"));
  roots.push(root);
  await writeFile(join(root, "package.json"), '{"private":true}');
  const bin = join(root, "node_modules", ".bin");
  await mkdir(bin, { recursive: true });
  const server = join(bin, "typescript-language-server");
  await writeFile(server, "#!/bin/sh\nexit 0\n");
  await chmod(server, 0o755);
  return root;
}

async function settled(handle: ServerHandle): Promise<void> {
  if (handle.process.exitCode === null) await once(handle.process, "exit");
}

describe("Typescript server", () => {
  test("a workspace without a tsserver script gets no server", async () => {
    const root = await workspaceWithoutTsserver();
    expect(
      await Typescript.spawn(root, { directory: root, worktree: root }),
    ).toBeUndefined();
  });

  test("a configured tsserverPath replaces the workspace's own", async () => {
    const root = await workspaceWithoutTsserver();
    const tsserverPath = join(root, "typescript5", "lib", "tsserver.js");
    const handle = await Typescript.spawn(root, {
      directory: root,
      worktree: root,
      tsserverPath,
    });
    if (handle === undefined) throw new Error("expected a server handle");
    await settled(handle);

    expect(handle.process.spawnargs).toEqual([
      join(root, "node_modules", ".bin", "typescript-language-server"),
      "--stdio",
    ]);
    expect(handle.initialization).toEqual({ tsserver: { path: tsserverPath } });
  });
});
