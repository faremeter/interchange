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
  test("returns undefined prompt and empty tools when no deploy dir exists", async () => {
    const dir = await tempDir();
    const result = await readDeployTree(dir);
    expect(result.systemPrompt).toBeUndefined();
    expect(result.tools).toEqual([]);
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
    expect(result.tools).toEqual([]);
  });

  test("reads tool definitions from skills directory", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "read_file");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "tool.json"),
      JSON.stringify({
        name: "read_file",
        description: "Read a file from disk",
        inputSchema: {
          type: "object",
          properties: { path: { type: "string" } },
        },
      }),
    );

    const result = await readDeployTree(dir);
    expect(result.tools).toHaveLength(1);
    const tool = result.tools[0];
    if (tool === undefined) throw new Error("unreachable");
    expect(tool.definition.name).toBe("read_file");
    expect(tool.definition.description).toBe("Read a file from disk");
    expect(tool.hasHandler).toBe(false);
  });

  test("detects handler.ts in skill directory", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "custom");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "tool.json"),
      JSON.stringify({
        name: "custom_tool",
        description: "A custom tool",
        inputSchema: { type: "object", properties: {} },
      }),
    );
    await fs.promises.writeFile(
      path.join(skillDir, "handler.ts"),
      "export default function() {}",
    );

    const result = await readDeployTree(dir);
    expect(result.tools).toHaveLength(1);
    const tool = result.tools[0];
    if (tool === undefined) throw new Error("unreachable");
    expect(tool.definition.name).toBe("custom_tool");
    expect(tool.hasHandler).toBe(true);
  });

  test("ignores directory named handler.ts", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "dirhandler");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "tool.json"),
      JSON.stringify({
        name: "dir_handler_tool",
        description: "Tool with directory named handler.ts",
        inputSchema: { type: "object", properties: {} },
      }),
    );
    await fs.promises.mkdir(path.join(skillDir, "handler.ts"));

    const result = await readDeployTree(dir);
    expect(result.tools).toHaveLength(1);
    const tool = result.tools[0];
    if (tool === undefined) throw new Error("unreachable");
    expect(tool.hasHandler).toBe(false);
  });

  test("throws on malformed tool.json", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "bad");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(path.join(skillDir, "tool.json"), "not json");

    await expect(readDeployTree(dir)).rejects.toThrow("Malformed tool.json");
  });

  test("throws on missing required fields in tool.json", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "incomplete");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "tool.json"),
      JSON.stringify({ name: "x" }),
    );

    await expect(readDeployTree(dir)).rejects.toThrow(
      'missing or empty "description"',
    );
  });

  test("skips skill directories without tool.json", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "no-tool");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "README.md"),
      "no tool here",
    );

    const result = await readDeployTree(dir);
    expect(result.tools).toEqual([]);
  });

  test("treats empty prompt.md as undefined", async () => {
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    await fs.promises.writeFile(path.join(dir, "deploy", "prompt.md"), "");

    const result = await readDeployTree(dir);
    expect(result.systemPrompt).toBeUndefined();
  });

  test("skips non-directory entries in skills folder", async () => {
    const dir = await tempDir();
    const skillsDir = path.join(dir, "deploy", "skills");
    await fs.promises.mkdir(skillsDir, { recursive: true });
    await fs.promises.writeFile(path.join(skillsDir, ".gitkeep"), "");

    const result = await readDeployTree(dir);
    expect(result.tools).toEqual([]);
  });

  test("throws on array tool.json with clear message", async () => {
    const dir = await tempDir();
    const skillDir = path.join(dir, "deploy", "skills", "array-tool");
    await fs.promises.mkdir(skillDir, { recursive: true });
    await fs.promises.writeFile(
      path.join(skillDir, "tool.json"),
      JSON.stringify([{ name: "x" }]),
    );

    await expect(readDeployTree(dir)).rejects.toThrow("expected an object");
  });
});
