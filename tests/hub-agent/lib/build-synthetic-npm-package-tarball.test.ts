// The packer stages under an internal mkdtemp so two packs cannot clobber a
// shared scratch dir. These tests pack both former call-site shapes in
// parallel and read the tarballs back.

import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { describe, expect, test } from "bun:test";
import * as tar from "tar";

import { buildSyntheticNpmPackageTarball } from "./synthetic-npm-package-tarball";

async function readPackedFile(
  bytes: Uint8Array,
  relativePath: string,
): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "npm-pkg-assert-"));
  try {
    const tarballPath = path.join(dir, "in.tgz");
    await fs.writeFile(tarballPath, bytes);
    await tar.x({ file: tarballPath, cwd: dir });
    return await fs.readFile(path.join(dir, relativePath), "utf8");
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}

describe("buildSyntheticNpmPackageTarball", () => {
  test("packs a workflow tarball and an external dep tarball without clobbering", async () => {
    const [workflow, ext] = await Promise.all([
      buildSyntheticNpmPackageTarball({
        packageName: "@wf/asset-skeleton",
        version: "1.0.0",
        moduleFilename: "workflow.mjs",
        moduleSource: "export const workflow = {};\n",
        manifest: { interchange: { workflow: "./workflow.mjs" } },
      }),
      buildSyntheticNpmPackageTarball({
        packageName: "wf-ext-dep",
        version: "2.0.0",
        moduleFilename: "index.mjs",
        moduleSource: "export const value = 1;\n",
        manifest: { type: "module", exports: "./index.mjs" },
      }),
    ]);

    expect(workflow.tarballFilename).toBe("asset-skeleton-1.0.0.tgz");
    expect(ext.tarballFilename).toBe("wf-ext-dep-2.0.0.tgz");

    expect(
      JSON.parse(await readPackedFile(workflow.bytes, "package/package.json")),
    ).toEqual({
      name: "@wf/asset-skeleton",
      version: "1.0.0",
      interchange: { workflow: "./workflow.mjs" },
    });
    expect(await readPackedFile(workflow.bytes, "package/workflow.mjs")).toBe(
      "export const workflow = {};\n",
    );

    expect(
      JSON.parse(await readPackedFile(ext.bytes, "package/package.json")),
    ).toEqual({
      name: "wf-ext-dep",
      version: "2.0.0",
      type: "module",
      exports: "./index.mjs",
    });
    expect(await readPackedFile(ext.bytes, "package/index.mjs")).toBe(
      "export const value = 1;\n",
    );
  });
});
