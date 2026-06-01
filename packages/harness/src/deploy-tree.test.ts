import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { readDeployTree } from "./deploy-tree";

const tempDirs: string[] = [];

async function tempDir(): Promise<string> {
  const d = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "deploy-tree-test-"),
  );
  tempDirs.push(d);
  return d;
}

afterEach(async () => {
  const dirs = tempDirs.splice(0);
  await Promise.all(
    dirs.map((d) => fs.promises.rm(d, { recursive: true, force: true })),
  );
});

describe("readDeployTree", () => {
  test("returns undefined prompt when no deploy dir exists", async () => {
    const dir = await tempDir();
    const result = await readDeployTree(dir);
    expect(result.systemPrompt).toBeUndefined();
  });

  test("reads prompt.md from deploy directory", async () => {
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    await fs.promises.writeFile(
      path.join(dir, "deploy", "prompt.md"),
      "You are a test agent.",
    );

    const result = await readDeployTree(dir);
    expect(result.systemPrompt).toBe("You are a test agent.");
  });

  test("treats empty prompt.md as undefined", async () => {
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    await fs.promises.writeFile(path.join(dir, "deploy", "prompt.md"), "");

    const result = await readDeployTree(dir);
    expect(result.systemPrompt).toBeUndefined();
  });
});
