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

  test("returns undefined toolPackageManifestRaw when no manifest file exists", async () => {
    const dir = await tempDir();
    const result = await readDeployTree(dir);
    expect(result.toolPackageManifestRaw).toBeUndefined();
  });

  test("returns tool-packages-manifest.json bytes as a raw string", async () => {
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    // The fixture is shaped to round-trip through the
    // ToolPackageManifest arktype validator (post-INTR-108: asset
    // sources carry both `assetId` and `path`). The reader itself
    // does no validation, but a fixture that the schema would reject
    // is a misleading example for future readers.
    const manifestBytes = JSON.stringify({
      schemaVersion: "1",
      topLevel: [{ name: "@intx/tools-posix", version: "1.2.3" }],
      entries: [
        {
          name: "@intx/tools-posix",
          version: "1.2.3",
          integrity: "sha512-AAAA",
          source: {
            kind: "asset",
            assetId: "ast_x",
            path: "packages/intx-tools-posix-1.2.3.tgz",
          },
        },
      ],
    });
    await fs.promises.writeFile(
      path.join(dir, "deploy", "tool-packages-manifest.json"),
      manifestBytes,
    );
    const result = await readDeployTree(dir);
    expect(result.toolPackageManifestRaw).toBe(manifestBytes);
  });

  test("returns an empty assetMounts map when no asset-mounts.json file exists", async () => {
    const dir = await tempDir();
    const result = await readDeployTree(dir);
    expect(result.assetMounts.size).toBe(0);
  });

  test("parses asset-mounts.json into a ReadonlyMap of assetId to mount path", async () => {
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    await fs.promises.writeFile(
      path.join(dir, "deploy", "asset-mounts.json"),
      JSON.stringify({
        assetMounts: {
          ast_a: "skills/skill-a",
          ast_b: "packages/builtins",
        },
      }),
    );
    const result = await readDeployTree(dir);
    expect(result.assetMounts.size).toBe(2);
    expect(result.assetMounts.get("ast_a")).toBe("skills/skill-a");
    expect(result.assetMounts.get("ast_b")).toBe("packages/builtins");
  });

  test("throws when asset-mounts.json fails schema validation", async () => {
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    await fs.promises.writeFile(
      path.join(dir, "deploy", "asset-mounts.json"),
      JSON.stringify({ assetMounts: { ast_a: 42 } }),
    );
    let caught: unknown = null;
    try {
      await readDeployTree(dir);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    const msg = caught instanceof Error ? caught.message : String(caught);
    expect(msg).toContain("asset-mounts.json failed validation");
  });

  test("returns corrupt-JSON bytes as-is for the caller to handle", async () => {
    // The reader does not parse; the caller's loader gate produces
    // the manifest.invalid failure and routes it through the same
    // deploy.apply.error frame channel as every other category.
    const dir = await tempDir();
    await fs.promises.mkdir(path.join(dir, "deploy"), { recursive: true });
    const garbage = "{this is not valid json";
    await fs.promises.writeFile(
      path.join(dir, "deploy", "tool-packages-manifest.json"),
      garbage,
    );
    const result = await readDeployTree(dir);
    expect(result.toolPackageManifestRaw).toBe(garbage);
  });
});
