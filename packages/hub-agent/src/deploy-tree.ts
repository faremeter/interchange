// Deploy tree reader: extracts the system prompt, the tool-package
// manifest, and the asset-mounts map from the deploy directory of an
// agent's git repository.
//
// The deploy tree is written by `applyDeployPack` and contains:
//   deploy/prompt.md                       — system prompt for inference
//   deploy/tool-packages-manifest.json     — optional, full pinned closure
//                                             of NPM-distributed tool
//                                             packages
//   deploy/asset-mounts.json               — optional, assetId → mount
//                                             path map covering every
//                                             `kind: "asset"` entry in
//                                             the manifest

import fs from "node:fs";
import path from "node:path";
import { type } from "arktype";

import { hasCode } from "@intx/types";

const AssetMountsFile = type({
  assetMounts: type({ "[string]": "string" }),
});

export type DeployTree = {
  systemPrompt: string | undefined;
  /**
   * Raw, un-parsed bytes of `deploy/tool-packages-manifest.json`.
   * JSON parsing and arktype validation are the caller's
   * responsibility — the sidecar's harness builder does both inside
   * `materializeToolPackages` so a corrupt or schema-invalid manifest
   * fails the apply loudly (category `manifest.invalid`) the same way
   * as every other apply-time failure.
   *
   * Undefined means the manifest file is not present.
   */
  toolPackageManifestRaw: string | undefined;
  /**
   * Parsed `deploy/asset-mounts.json`, validated by arktype. The map
   * is empty when the file is absent — that is the legitimate shape
   * for a deploy with no asset-sourced tool packages, and the loader's
   * own gating raises if a manifest entry asks for a missing assetId.
   */
  assetMounts: ReadonlyMap<string, string>;
};

/**
 * Read the system prompt and tool-package manifest bytes from the
 * deploy directory. Each field is independently optional — undefined
 * means the corresponding file is not present in the materialized
 * deploy. An agent that has not yet received a deploy pack returns
 * both as undefined.
 *
 * No parsing or validation of the manifest happens here; the caller
 * runs both inside the loader boundary so parse errors and schema
 * errors land on the same failure path.
 */
export async function readDeployTree(dir: string): Promise<DeployTree> {
  const promptPath = path.join(dir, "deploy", "prompt.md");
  const manifestPath = path.join(dir, "deploy", "tool-packages-manifest.json");
  const assetMountsPath = path.join(dir, "deploy", "asset-mounts.json");

  let systemPrompt: string | undefined;
  try {
    const raw = await fs.promises.readFile(promptPath, "utf-8");
    systemPrompt = raw.trim() === "" ? undefined : raw;
  } catch (e) {
    if (hasCode(e) && e.code === "ENOENT") {
      systemPrompt = undefined;
    } else {
      throw e;
    }
  }

  let toolPackageManifestRaw: string | undefined;
  try {
    toolPackageManifestRaw = await fs.promises.readFile(manifestPath, "utf-8");
  } catch (e) {
    if (hasCode(e) && e.code === "ENOENT") {
      toolPackageManifestRaw = undefined;
    } else {
      throw e;
    }
  }

  let assetMounts: ReadonlyMap<string, string> = new Map();
  try {
    const raw = await fs.promises.readFile(assetMountsPath, "utf-8");
    const parsed: unknown = JSON.parse(raw);
    const validated = AssetMountsFile(parsed);
    if (validated instanceof type.errors) {
      throw new Error(
        `deploy/asset-mounts.json failed validation: ${validated.summary}`,
      );
    }
    assetMounts = new Map(Object.entries(validated.assetMounts));
  } catch (e) {
    if (hasCode(e) && e.code === "ENOENT") {
      assetMounts = new Map();
    } else {
      throw e;
    }
  }

  return { systemPrompt, toolPackageManifestRaw, assetMounts };
}
