import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import type { SourceRefPin } from "@intx/types/sidecar";

import { resolveDeploymentAssetMounts } from "./workflow-host-wiring";

// The durable source-asset store is what makes an asset-sourced deployment
// survive a restart: the deploy checks the assets out once, and both the deploy
// apply and the boot-time restore re-materialize from the store using mounts
// derived from the pin ALONE (restore has no re-delivery). These tests pin that
// derivation and its fail-loud presence check without spinning up a deployment.

const DEPLOYMENT_ID = "dep-1";

function assetPin(assetIds: readonly string[]): SourceRefPin {
  return {
    source: {
      kind: "asset",
      assetId: assetIds[0] ?? "asset_top",
      package: { format: "tarball" },
    },
    closure: {
      schemaVersion: "1",
      topLevel: [{ name: "@x/wf", version: "1.0.0" }],
      entries: assetIds.map((assetId, i) => ({
        name: i === 0 ? "@x/wf" : `@x/dep-${String(i)}`,
        version: "1.0.0",
        source: {
          kind: "asset",
          assetId,
          package: {
            format: "tarball",
            path: "tarballs/x.tgz",
            integrity: "sha512-placeholder",
          },
        },
      })),
    },
  };
}

let dataDir: string;

beforeEach(async () => {
  dataDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "wf-deploy-source-assets-"),
  );
});

afterEach(async () => {
  await fs.rm(dataDir, { recursive: true, force: true });
});

describe("resolveDeploymentAssetMounts", () => {
  test("derives the pin's mount map when the store is populated", async () => {
    const assetId = "asset_top";
    // Populate the durable store exactly as a deploy checkout would; restore
    // reads only this, deriving the mount from the pin.
    await fs.mkdir(
      path.join(
        dataDir,
        "workflow-definition-sources",
        DEPLOYMENT_ID,
        "source-assets",
        assetId,
      ),
      { recursive: true },
    );

    const { assetRoot, assetMounts } = await resolveDeploymentAssetMounts(
      dataDir,
      DEPLOYMENT_ID,
      assetPin([assetId]),
    );

    expect(assetRoot).toBe(
      path.join(dataDir, "workflow-definition-sources", DEPLOYMENT_ID),
    );
    expect([...assetMounts]).toEqual([[assetId, `source-assets/${assetId}/`]]);
  });

  test("dedupes many entries backed by one asset into a single mount", async () => {
    const assetId = "asset_top";
    await fs.mkdir(
      path.join(
        dataDir,
        "workflow-definition-sources",
        DEPLOYMENT_ID,
        "source-assets",
        assetId,
      ),
      { recursive: true },
    );
    // Two closure entries, same backing asset.
    const { assetMounts } = await resolveDeploymentAssetMounts(
      dataDir,
      DEPLOYMENT_ID,
      assetPin([assetId, assetId]),
    );
    expect(assetMounts.size).toBe(1);
  });

  test("fails loud when a pinned asset is absent from the store", async () => {
    await expect(
      resolveDeploymentAssetMounts(
        dataDir,
        DEPLOYMENT_ID,
        assetPin(["asset_missing"]),
      ),
    ).rejects.toThrow(/is not present in the durable store/);
  });

  test("returns an empty mount map for a registry-sourced pin", async () => {
    const registryPin: SourceRefPin = {
      source: { kind: "registry", registry: "npmjs" },
      closure: {
        schemaVersion: "1",
        topLevel: [{ name: "@x/wf", version: "1.0.0" }],
        entries: [
          {
            name: "@x/wf",
            version: "1.0.0",
            source: {
              kind: "registry",
              registry: "npmjs",
              integrity: "sha512-placeholder",
            },
          },
        ],
      },
    };

    const { assetMounts } = await resolveDeploymentAssetMounts(
      dataDir,
      DEPLOYMENT_ID,
      registryPin,
    );
    expect(assetMounts.size).toBe(0);
  });
});
