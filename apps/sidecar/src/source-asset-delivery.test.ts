import { describe, test, expect } from "bun:test";
import path from "node:path";

import type { ToolPackageManifest } from "@intx/types/tool-packages";

import {
  assetReferenceFormats,
  sourceAssetGitDir,
} from "./source-asset-delivery";

// The git-index + subtree-checkout path (`indexAssetPackIntoGitDir`,
// `materializeWorkflowAssets`) is exercised end-to-end by the source-workflow
// e2e; these cover the pure classification and path helpers in isolation.

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
