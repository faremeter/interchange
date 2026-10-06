import { describe, test, expect, afterEach } from "bun:test";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";

import { base64Encode } from "@intx/types";
import type { ToolPackageManifest } from "@intx/types/tool-packages";
import { collectReachableObjects } from "@intx/storage-isogit/node";

import {
  assetReferenceFormats,
  indexAssetPackIntoGitDir,
  materializeWorkflowAssets,
  sourceAssetGitDir,
} from "./source-asset-delivery";

// The subtree checkout inside `materializeWorkflowAssets` is exercised
// end-to-end by the source-workflow e2e. These cover the pure classification
// and path helpers, `indexAssetPackIntoGitDir`'s atomic-publish semantics,
// and the payload-cap and duplicate-delivery guards, in isolation.

function manifest(
  entries: ToolPackageManifest["entries"],
): ToolPackageManifest {
  return { schemaVersion: "1", topLevel: [], entries };
}

describe("assetReferenceFormats", () => {
  test("classifies each asset by the formats it is referenced with", () => {
    const formats = assetReferenceFormats(
      manifest([
        {
          name: "@x/a",
          version: "1.0.0",
          source: {
            kind: "asset",
            assetId: "asset_src",
            package: {
              format: "source",
              commitSha: "c1",
              packageDir: ".",
              treeOid: "t1",
            },
          },
        },
        {
          name: "@x/b",
          version: "1.0.0",
          source: {
            kind: "asset",
            assetId: "asset_src",
            package: {
              format: "source",
              commitSha: "c1",
              packageDir: "packages/b",
              treeOid: "t2",
            },
          },
        },
        {
          name: "@x/c",
          version: "1.0.0",
          source: {
            kind: "asset",
            assetId: "asset_tar",
            package: {
              format: "tarball",
              path: "tarballs/c.tgz",
              integrity: "sha512-c",
            },
          },
        },
        {
          name: "left-pad",
          version: "1.3.0",
          source: { kind: "registry", registry: "npm", integrity: "sha512-lp" },
        },
      ]),
    );

    expect(formats.get("asset_src")).toEqual({ tarball: false, source: true });
    expect(formats.get("asset_tar")).toEqual({ tarball: true, source: false });
    // A registry entry backs no asset.
    expect(formats.has("npm")).toBe(false);
    expect(formats.size).toBe(2);
  });

  test("marks an asset referenced both ways as needing both checkouts", () => {
    const formats = assetReferenceFormats(
      manifest([
        {
          name: "@x/a",
          version: "1.0.0",
          source: {
            kind: "asset",
            assetId: "asset_dual",
            package: {
              format: "source",
              commitSha: "c1",
              packageDir: ".",
              treeOid: "t1",
            },
          },
        },
        {
          name: "@x/b",
          version: "1.0.0",
          source: {
            kind: "asset",
            assetId: "asset_dual",
            package: {
              format: "tarball",
              path: "tarballs/b.tgz",
              integrity: "sha512-b",
            },
          },
        },
      ]),
    );

    expect(formats.get("asset_dual")).toEqual({ tarball: true, source: true });
  });
});

describe("sourceAssetGitDir", () => {
  test("joins a safe assetId under the gitDir root", () => {
    expect(sourceAssetGitDir("/root/gits", "ast_wf-1.2")).toBe(
      path.join("/root/gits", "ast_wf-1.2"),
    );
  });

  test.each([
    ["a/b", "slash"],
    ["../escape", "parent traversal"],
    ["with space", "space"],
    ["", "empty"],
    // All-dots ids satisfy SAFE_ASSET_ID (which permits ".") but escape or
    // collapse the per-asset dir, so they must be rejected explicitly.
    ["..", "bare parent"],
    [".", "current dir"],
    ["...", "triple dot"],
  ])("rejects an unsafe assetId (%s)", (assetId) => {
    expect(() => sourceAssetGitDir("/root/gits", assetId)).toThrow(
      /unsafe assetId/,
    );
  });
});

describe("indexAssetPackIntoGitDir", () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    const dirs = tempDirs.splice(0);
    await Promise.all(
      dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
    );
  });

  async function tempDir(): Promise<string> {
    const d = await fsp.mkdtemp(
      path.join(os.tmpdir(), "source-delivery-test-"),
    );
    tempDirs.push(d);
    return d;
  }

  // Pack a one-commit repo and return its commit sha plus a pack of every
  // object reachable from it.
  async function buildPack(): Promise<{ commitSha: string; pack: Uint8Array }> {
    const dir = await tempDir();
    await git.init({ fs, dir, defaultBranch: "main" });
    await fsp.writeFile(path.join(dir, "package.json"), '{"name":"x"}\n');
    await git.add({ fs, dir, filepath: "package.json" });
    const commitSha = await git.commit({
      fs,
      dir,
      message: "t",
      author: { name: "t", email: "t@t.dev" },
    });
    const oids = await collectReachableObjects(dir, commitSha);
    const { packfile } = await git.packObjects({ fs, dir, oids });
    if (packfile === undefined) {
      throw new Error("source-delivery test: packObjects returned no packfile");
    }
    return { commitSha, pack: packfile };
  }

  test("retains the durable gitDir on success", async () => {
    const root = await tempDir();
    const gitDir = path.join(root, "asset-ok");
    const { commitSha, pack } = await buildPack();

    await indexAssetPackIntoGitDir({ pack, commitSha, gitDir });

    expect(fs.existsSync(path.join(gitDir, ".git"))).toBe(true);
  });

  test("leaves no durable gitDir when indexing fails", async () => {
    // The pin is absent from the pack, so indexing throws. The atomic
    // temp+rename build means the final gitDir is never created -- restore's
    // dir-exists gate must not find a partial store.
    const root = await tempDir();
    const gitDir = path.join(root, "asset-bad");
    const { pack } = await buildPack();

    await expect(
      indexAssetPackIntoGitDir({ pack, commitSha: "0".repeat(40), gitDir }),
    ).rejects.toThrow(/not found in the pack/);

    expect(fs.existsSync(gitDir)).toBe(false);
  });
});

describe("materializeWorkflowAssets", () => {
  const tempDirs: string[] = [];

  afterEach(async () => {
    const dirs = tempDirs.splice(0);
    await Promise.all(
      dirs.map((d) => fsp.rm(d, { recursive: true, force: true })),
    );
  });

  async function tempDir(): Promise<string> {
    const d = await fsp.mkdtemp(
      path.join(os.tmpdir(), "source-delivery-test-"),
    );
    tempDirs.push(d);
    return d;
  }

  async function buildPack(): Promise<{ commitSha: string; pack: Uint8Array }> {
    const dir = await tempDir();
    await git.init({ fs, dir, defaultBranch: "main" });
    await fsp.writeFile(path.join(dir, "package.json"), '{"name":"x"}\n');
    await git.add({ fs, dir, filepath: "package.json" });
    const commitSha = await git.commit({
      fs,
      dir,
      message: "t",
      author: { name: "t", email: "t@t.dev" },
    });
    const oids = await collectReachableObjects(dir, commitSha);
    const { packfile } = await git.packObjects({ fs, dir, oids });
    if (packfile === undefined) {
      throw new Error("source-delivery test: packObjects returned no packfile");
    }
    return { commitSha, pack: packfile };
  }

  test("rejects an inline payload over the cap before decoding it", async () => {
    await expect(
      materializeWorkflowAssets({
        assets: [
          {
            assetId: "asset_a",
            mountPath: "m/",
            pack: "x".repeat(17),
            ref: "refs/heads/main",
            commitSha: "c",
          },
        ],
        closure: manifest([]),
        assetRoot: "/unused",
        gitDirRoot: "/unused",
        maxAssetPayloadBytes: 16,
      }),
    ).rejects.toThrow(/inline asset payload exceeds the 16-byte cap/);
  });

  test("rejects a second delivery of the same assetId", async () => {
    const root = await tempDir();
    const { commitSha, pack } = await buildPack();
    const encoded = base64Encode(pack);
    const asset = {
      assetId: "asset_src",
      mountPath: "m/",
      pack: encoded,
      ref: "refs/heads/main",
      commitSha,
    };
    await expect(
      materializeWorkflowAssets({
        assets: [asset, { ...asset, mountPath: "n/" }],
        closure: manifest([
          {
            name: "@x/a",
            version: "1.0.0",
            source: {
              kind: "asset",
              assetId: "asset_src",
              package: {
                format: "source",
                commitSha,
                packageDir: ".",
                treeOid: "t1",
              },
            },
          },
        ]),
        assetRoot: path.join(root, "workspace"),
        gitDirRoot: path.join(root, "gitdirs"),
        maxAssetPayloadBytes: 50_000_000,
      }),
    ).rejects.toThrow(/asset "asset_src" is delivered more than once/);
  });
});
