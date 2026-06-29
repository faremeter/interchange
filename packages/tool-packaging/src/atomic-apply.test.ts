import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { type AnnotatedPluginFactory, definePlugin } from "@intx/agent";
import type { DeployApplyErrorCategory } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

import { applyAtomic } from "./atomic-apply";
import {
  type LoadedToolFactory,
  type LoadedToolPackage,
  type ToolLoader,
  ToolLoaderError,
} from "./loader";

let scratch: string;
let instanceDir: string;
let assetRoot: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "tool-packaging-atomic-"));
  instanceDir = path.join(scratch, "instance");
  assetRoot = path.join(scratch, "asset");
  await fs.mkdir(instanceDir, { recursive: true });
  await fs.mkdir(assetRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

function deployDir(id: string): string {
  return path.join(instanceDir, "packages", id);
}

async function seedDeploy(id: string, marker: string): Promise<string> {
  const dir = deployDir(id);
  await fs.mkdir(dir, { recursive: true });
  const markerPath = path.join(dir, "marker");
  await fs.writeFile(markerPath, marker);
  return markerPath;
}

function fakeFactory(id: string): LoadedToolFactory {
  const fn = () => ({
    definitions: [],
    run: async () => ({ callId: "stub", content: "ok" }),
  });
  return Object.assign(fn, { id, requires: [] as readonly string[] });
}

function fakePlugin(id: string): AnnotatedPluginFactory {
  return definePlugin({ id, factory: () => ({}) });
}

function makeStubLoader(
  impl: (args: { instanceScratchDir: string }) => Promise<LoadedToolPackage[]>,
): ToolLoader {
  return {
    loadManifest: async (args) => {
      // Write a sentinel file into the scratch dir so the test can
      // observe what staged before the loader returned.
      await fs.writeFile(
        path.join(args.instanceScratchDir, "loader-sentinel"),
        "ran",
      );
      return impl({ instanceScratchDir: args.instanceScratchDir });
    },
  };
}

const minimalManifest: ToolPackageManifest = {
  schemaVersion: "1",
  topLevel: [],
  entries: [],
};

async function dirExists(p: string): Promise<boolean> {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

describe("applyAtomic success", () => {
  test("stages a per-deploy-id directory and returns its path and the new id", async () => {
    const loader = makeStubLoader(async () => [
      {
        name: "foo",
        version: "1.0.0",
        plugins: [],
        factories: [fakeFactory("@foo/a")],
        directors: [],
      },
    ]);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    expect(result.status).toBe("ok");
    if (result.status !== "ok") return;
    expect(result.activeDeployId).toBe("dpl_1");
    expect(result.deployDir).toBe(deployDir("dpl_1"));
    expect(result.loaded).toHaveLength(1);
    expect(await dirExists(deployDir("dpl_1"))).toBe(true);
    // The loader staged into the deploy dir, not a swappable path.
    expect(
      await dirExists(path.join(deployDir("dpl_1"), "loader-sentinel")),
    ).toBe(true);
    // The protocol no longer renames; there is no active/pending tree.
    expect(await dirExists(path.join(instanceDir, "active"))).toBe(false);
    expect(await dirExists(path.join(instanceDir, "pending"))).toBe(false);
  });

  test("retains the previous deploy directory across the next apply", async () => {
    const loader = makeStubLoader(async () => []);
    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_1",
      previousDeployId: "dpl_0",
      newDeployId: "dpl_1",
    });
    // Marker so we can confirm the dpl_1 tree survives the next apply.
    await fs.writeFile(path.join(deployDir("dpl_1"), "marker"), "first");

    await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_2",
      previousDeployId: "dpl_1",
      newDeployId: "dpl_2",
    });
    expect(await dirExists(deployDir("dpl_2"))).toBe(true);
    // dpl_1 is the previous deploy and is retained untouched.
    expect(
      await fs.readFile(path.join(deployDir("dpl_1"), "marker"), "utf8"),
    ).toBe("first");
  });

  test("prelude sweep reaps every deploy except current and previous", async () => {
    // Seed a stale deploy (two generations back) and the immediately
    // prior deploy. The apply must reap the stale one and keep the
    // prior one.
    const stalePath = await seedDeploy("dpl_stale", "two-back");
    const prevPath = await seedDeploy("dpl_prev", "one-back");

    const loader = makeStubLoader(async () => []);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_S",
      previousDeployId: "dpl_prev",
      newDeployId: "dpl_new",
    });
    expect(result.status).toBe("ok");
    expect(await dirExists(deployDir("dpl_stale"))).toBe(false);
    expect(await dirExists(stalePath)).toBe(false);
    // previous and current survive.
    expect(await fs.readFile(prevPath, "utf8")).toBe("one-back");
    expect(await dirExists(deployDir("dpl_new"))).toBe(true);
  });
});

describe("applyAtomic failure: every loader error category", () => {
  const categories: DeployApplyErrorCategory[] = [
    "tarball.missing",
    "integrity.mismatch",
    "registry.fetch.failed",
    "registry.unknown",
    "registry.auth.failed",
    "tarball.extract.failed",
    "manifest.invalid",
    "package.entry.missing",
    "package.entry.invalid",
    "factory.construct.failed",
  ];

  for (const category of categories) {
    test(`category ${category}: deploy dir discarded, previous untouched, error carries previousDeployId`, async () => {
      // Seed a prior deploy to confirm it is preserved.
      const priorMarker = await seedDeploy("dpl_prior", "untouched");

      const loader: ToolLoader = {
        loadManifest: async () => {
          throw new ToolLoaderError({
            category,
            message: `induced failure: ${category}`,
            package: { name: "p", version: "1.0.0" },
          });
        },
      };
      const result = await applyAtomic({
        manifest: minimalManifest,
        loader,
        instanceDir,
        assetRoot,
        assetMounts: new Map(),
        attemptId: "atp_X",
        previousDeployId: "dpl_prior",
        newDeployId: "dpl_attempted",
      });
      expect(result.status).toBe("failed");
      if (result.status !== "failed") return;
      expect(result.category).toBe(category);
      expect(result.previousDeployId).toBe("dpl_prior");
      expect(result.attemptId).toBe("atp_X");
      expect(result.package).toEqual({ name: "p", version: "1.0.0" });

      // Atomicity invariant: the prior deploy is unchanged.
      expect(await fs.readFile(priorMarker, "utf8")).toBe("untouched");
      // The attempted deploy dir has been removed.
      expect(await dirExists(deployDir("dpl_attempted"))).toBe(false);
    });
  }
});

describe("applyAtomic failure: tool.name.duplicate", () => {
  test("two factories with the same id across packages → tool.name.duplicate", async () => {
    const priorMarker = await seedDeploy("dpl_prior", "untouched");
    const loader = makeStubLoader(async () => [
      {
        name: "a",
        version: "1.0.0",
        plugins: [],
        factories: [fakeFactory("@dup/x")],
        directors: [],
      },
      {
        name: "b",
        version: "1.0.0",
        plugins: [],
        factories: [fakeFactory("@dup/x")],
        directors: [],
      },
    ]);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_D",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_attempted",
    });
    expect(result.status).toBe("failed");
    if (result.status !== "failed") return;
    expect(result.category).toBe("tool.name.duplicate");
    expect(result.previousDeployId).toBe("dpl_prior");
    expect(await dirExists(deployDir("dpl_attempted"))).toBe(false);
    expect(await fs.readFile(priorMarker, "utf8")).toBe("untouched");
  });

  test("two plugins with the same id across packages → tool.name.duplicate", async () => {
    const priorMarker = await seedDeploy("dpl_prior", "untouched");
    const loader = makeStubLoader(async () => [
      {
        name: "a",
        version: "1.0.0",
        plugins: [fakePlugin("dup/plug")],
        factories: [],
        directors: [],
      },
      {
        name: "b",
        version: "2.0.0",
        plugins: [fakePlugin("dup/plug")],
        factories: [],
        directors: [],
      },
    ]);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_DP",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_attempted",
    });
    expect(result.status).toBe("failed");
    if (result.status !== "failed") return;
    expect(result.category).toBe("tool.name.duplicate");
    expect(result.message).toContain("plugin factory id dup/plug");
    // The collision is attributed to the second package that carried
    // the already-seen plugin id.
    expect(result.package).toEqual({ name: "b", version: "2.0.0" });
    expect(result.previousDeployId).toBe("dpl_prior");
    expect(await dirExists(deployDir("dpl_attempted"))).toBe(false);
    expect(await fs.readFile(priorMarker, "utf8")).toBe("untouched");
  });
});

describe("applyAtomic failure: unexpected error shape", () => {
  test("loader throwing a plain Error becomes factory.construct.failed", async () => {
    const loader: ToolLoader = {
      loadManifest: async () => {
        throw new Error("something exploded");
      },
    };
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_U",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_attempted",
    });
    expect(result.status).toBe("failed");
    if (result.status !== "failed") return;
    expect(result.category).toBe("factory.construct.failed");
    expect(result.message).toContain("something exploded");
    expect(await dirExists(deployDir("dpl_attempted"))).toBe(false);
  });
});

describe("applyAtomic clears a stale deploy dir before staging", () => {
  test("a leftover directory under the new deploy id does not leak into the staged tree", async () => {
    // A crash mid-build (or uuid reuse) can leave a partial tree under
    // the exact id this apply is about to build. The prelude must clear
    // it so the loader stages into a clean directory.
    await fs.mkdir(deployDir("dpl_new"), { recursive: true });
    await fs.writeFile(path.join(deployDir("dpl_new"), "stale"), "old");
    const loader = makeStubLoader(async () => []);
    const result = await applyAtomic({
      manifest: minimalManifest,
      loader,
      instanceDir,
      assetRoot,
      assetMounts: new Map(),
      attemptId: "atp_L",
      previousDeployId: "dpl_prior",
      newDeployId: "dpl_new",
    });
    expect(result.status).toBe("ok");
    expect(await dirExists(path.join(deployDir("dpl_new"), "stale"))).toBe(
      false,
    );
    expect(
      await dirExists(path.join(deployDir("dpl_new"), "loader-sentinel")),
    ).toBe(true);
  });
});
