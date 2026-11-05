// End-to-end integration of the tool-package pipeline.
//
// Exercises the gluing between the hub-side resolver and the sidecar-side
// closure materialization, in-process against synthetic fixtures.
//
// What the test demonstrates per scenario:
//
//   - happy path: resolver walks pins → manifest round-trips through disk →
//     materializeClosure lays the closure out.
//
//   - integrity.mismatch: bytes served by the mock registry do not
//     match the resolver's pinned integrity → materializeClosure rejects
//     with the right category.
//
//   - registry.unknown: manifest references a registry the sidecar
//     was not told about → materializeClosure rejects with registry.unknown.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import ssri from "ssri";
import * as tar from "tar";

import {
  type Packument,
  type PackumentFetcher,
  type RegistryConfig,
  type RegistrySource,
  AssetRegistrySource,
  HttpRegistrySource,
  createClosureResolver,
  createTarballCache,
  createTarballFetcher,
  materializeClosure,
  storeEntryDir,
} from "@intx/tool-packaging";
import type {
  ToolPackageManifest,
  ToolPackagePin,
} from "@intx/types/tool-packages";
import { getToolPackageSourceContentIdentity } from "@intx/types/tool-packages";

let scratch: string;
let fixtureRoot: string;
let cacheDir: string;
let instanceDir: string;
let assetRoot: string;

beforeEach(async () => {
  scratch = await fs.mkdtemp(path.join(os.tmpdir(), "intr108-e2e-"));
  fixtureRoot = path.join(scratch, "fixtures");
  cacheDir = path.join(scratch, "cache");
  instanceDir = path.join(scratch, "instance");
  assetRoot = path.join(scratch, "asset");
  await fs.mkdir(fixtureRoot, { recursive: true });
  await fs.mkdir(cacheDir, { recursive: true });
  await fs.mkdir(instanceDir, { recursive: true });
  await fs.mkdir(assetRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratch, { recursive: true, force: true });
});

interface Fixture {
  name: string;
  version: string;
  bytes: Uint8Array;
  integrity: string;
  tarballUrl: string;
}

async function buildFixture(spec: {
  name: string;
  version: string;
  exportName: string;
}): Promise<Fixture> {
  const stagingDir = path.join(
    fixtureRoot,
    `${spec.name.replace("/", "_")}-${spec.version}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });

  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify({
      name: spec.name,
      version: spec.version,
      interchange: { tools: "./tools.js" },
    }),
  );
  await fs.writeFile(path.join(packageDir, "tools.js"), "export {};\n");

  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
    "package",
  ]);
  const bytes = await fs.readFile(tarballPath);
  const integrity = ssri.fromData(bytes, { algorithms: ["sha512"] }).toString();
  const tarballUrl = `https://registry.test/${spec.name}/-/${
    spec.name.startsWith("@") ? spec.name.split("/")[1] : spec.name
  }-${spec.version}.tgz`;
  return {
    name: spec.name,
    version: spec.version,
    bytes,
    integrity,
    tarballUrl,
  };
}

function makePackumentFromFixtures(fixtures: readonly Fixture[]): Packument[] {
  return fixtures.map((f) => ({
    name: f.name,
    versions: {
      [f.version]: {
        name: f.name,
        version: f.version,
        dist: {
          tarball: f.tarballUrl,
          integrity: f.integrity,
        },
      },
    },
  }));
}

async function storedPackageJSON(
  storeDir: string,
  fixture: { name: string; version: string },
): Promise<unknown> {
  return JSON.parse(
    await fs.readFile(
      path.join(
        storeEntryDir(storeDir, fixture.name, fixture.version),
        "package.json",
      ),
      "utf8",
    ),
  );
}

describe("hub→sidecar pipeline (happy path)", () => {
  test("resolved manifest round-trips through disk and lays out cleanly", async () => {
    const fixture = await buildFixture({
      name: "tools-fixture",
      version: "1.0.0",
      exportName: "main",
    });

    // Hub side: build the resolver against a synthetic packument.
    const fetchPackument: PackumentFetcher = async (name) => {
      const packuments = makePackumentFromFixtures([fixture]);
      const p = packuments.find((q) => q.name === name);
      if (p === undefined) throw new Error(`no packument: ${name}`);
      return p;
    };
    const registryName = "fixture-registry";
    const registryConfig: RegistryConfig = {
      url: "https://registry.test",
    };
    const registries = new Map<string, RegistryConfig>([
      [registryName, registryConfig],
    ]);
    const source = new HttpRegistrySource({
      name: registryName,
      config: registryConfig,
      fetchPackument,
    });
    const resolver = createClosureResolver({
      registries: new Map([[source.name, source]]),
      defaultRegistry: source.name,
    });
    const pins: ToolPackagePin[] = [
      { name: fixture.name, version: fixture.version },
    ];
    const manifest = await resolver.resolveClosure(pins);
    expect(manifest.entries).toHaveLength(1);
    const materializedEntry = manifest.entries[0];
    if (materializedEntry === undefined) throw new Error("missing entry");
    expect(getToolPackageSourceContentIdentity(materializedEntry.source)).toBe(
      fixture.integrity,
    );

    // Persist the manifest; round-trip it.
    const deployDir = path.join(instanceDir, "deploy");
    await fs.mkdir(deployDir, { recursive: true });
    await fs.writeFile(
      path.join(deployDir, "tool-packages-manifest.json"),
      JSON.stringify(manifest),
    );
    const readBack: ToolPackageManifest = JSON.parse(
      await fs.readFile(
        path.join(deployDir, "tool-packages-manifest.json"),
        "utf-8",
      ),
    );
    expect(readBack).toEqual(manifest);

    // Sidecar side: instantiate cache + loader against the same
    // registries, fetch via a stub that hands back the fixture bytes.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 100_000_000,
    });
    const { storeDir, entries } = await materializeClosure({
      manifest: readBack,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
      host: { os: "linux", cpu: "x64" },
      cache,
      registries,
      fetchTarball: async () => fixture.bytes,
    });
    expect(entries).toHaveLength(1);
    expect(await storedPackageJSON(storeDir, fixture)).toMatchObject({
      name: fixture.name,
      version: fixture.version,
    });
  });
});

describe("hub→sidecar pipeline (failure paths)", () => {
  test("integrity.mismatch when registry-served bytes diverge from the pin", async () => {
    const fixture = await buildFixture({
      name: "tools-mismatched",
      version: "1.0.0",
      exportName: "main",
    });
    const registries = new Map<string, RegistryConfig>([
      ["fixture-registry", { url: "https://registry.test" }],
    ]);
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: fixture.name, version: fixture.version }],
      entries: [
        {
          name: fixture.name,
          version: fixture.version,
          source: {
            kind: "registry",
            registry: "fixture-registry",
            integrity: fixture.integrity,
          },
        },
      ],
    };

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 100_000_000,
    });
    await expect(
      materializeClosure({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
        host: { os: "linux", cpu: "x64" },
        cache,
        registries,
        // Hand back bytes that do not match the pinned integrity.
        fetchTarball: async () =>
          new TextEncoder().encode("definitely the wrong bytes"),
      }),
    ).rejects.toMatchObject({ category: "integrity.mismatch" });
  });

  test("registry.unknown when manifest names a registry not in the sidecar config", async () => {
    const fixture = await buildFixture({
      name: "tools-unknown-reg",
      version: "1.0.0",
      exportName: "main",
    });
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: fixture.name, version: fixture.version }],
      entries: [
        {
          name: fixture.name,
          version: fixture.version,
          source: {
            kind: "registry",
            registry: "ghost-registry",
            integrity: fixture.integrity,
          },
        },
      ],
    };

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 100_000_000,
    });
    await expect(
      materializeClosure({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
        host: { os: "linux", cpu: "x64" },
        cache,
        // Sidecar only knows about a different registry.
        registries: new Map([
          ["fixture-registry", { url: "https://registry.test" }],
        ]),
        fetchTarball: async () => fixture.bytes,
      }),
    ).rejects.toMatchObject({ category: "registry.unknown" });
  });

  test("ManifestInvalidError when resolver detects unsatisfied peer deps", async () => {
    const fetchPackument: PackumentFetcher = async (name) => {
      if (name === "needs-react") {
        return {
          name: "needs-react",
          versions: {
            "1.0.0": {
              name: "needs-react",
              version: "1.0.0",
              dist: {
                tarball: "https://r.test/x.tgz",
                integrity: "sha512-AAAA",
              },
              peerDependencies: { react: "^19" },
            },
          },
        };
      }
      throw new Error(`no packument: ${name}`);
    };
    const source = new HttpRegistrySource({
      name: "fixture-registry",
      config: { url: "https://r.test" },
      fetchPackument,
    });
    const resolver = createClosureResolver({
      registries: new Map([[source.name, source]]),
      defaultRegistry: source.name,
    });
    let caught: unknown;
    try {
      await resolver.resolveClosure([
        { name: "needs-react", version: "1.0.0" },
      ]);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).toMatch(/needs-react/);
    expect(String(caught)).toMatch(/react/);
  });
});

describe("hub→sidecar pipeline (cache behavior)", () => {
  test("cache hit on second apply avoids re-fetching", async () => {
    const fixture = await buildFixture({
      name: "tools-cached",
      version: "1.0.0",
      exportName: "main",
    });
    const registries = new Map<string, RegistryConfig>([
      ["fixture-registry", { url: "https://registry.test" }],
    ]);
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: fixture.name, version: fixture.version }],
      entries: [
        {
          name: fixture.name,
          version: fixture.version,
          source: {
            kind: "registry",
            registry: "fixture-registry",
            integrity: fixture.integrity,
          },
        },
      ],
    };
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 100_000_000,
    });
    let fetchCount = 0;
    const materialize = (instanceScratchDir: string) =>
      materializeClosure({
        manifest,
        instanceScratchDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
        host: { os: "linux", cpu: "x64" },
        cache,
        registries,
        fetchTarball: async () => {
          fetchCount += 1;
          return fixture.bytes;
        },
      });

    // First apply: fetch happens.
    await materialize(path.join(instanceDir, "first"));
    expect(fetchCount).toBe(1);

    // Second apply against a fresh instance dir but the SAME cache.
    await materialize(path.join(instanceDir, "second"));
    // Cache hit; fetcher was not called again.
    expect(fetchCount).toBe(1);
  });
});

describe("hub→sidecar pipeline (asset-backed registry)", () => {
  /**
   * Synthesize an in-memory asset that mirrors what the asset service
   * would expose for a `package-registry` asset: a `tarballs/` tree
   * holding npm-style tarballs whose `package.json`s validate.
   *
   * The fixture returns the bound `readBlob`/`listBlobs` pair the
   * resolver consumes, plus a writeMount helper that lays the same
   * tarball bytes onto disk at the mount path the mount map will
   * point the loader at. End-to-end: the resolver picks an entry,
   * the mount map records `(assetId, mount)`, the loader joins
   * `assetRoot + mount + path` and reads the same bytes back.
   */
  async function buildAssetRegistryFixture(
    pkgs: readonly {
      name: string;
      version: string;
      exportName: string;
    }[],
  ): Promise<{
    readBlob: (path: string) => Promise<Uint8Array>;
    listBlobs: (dir: string) => Promise<string[]>;
    writeMount: (mountAbsPath: string) => Promise<void>;
    tarballs: { name: string; version: string; filename: string }[];
  }> {
    const stagingRoot = path.join(scratch, "asset-fixture");
    await fs.mkdir(stagingRoot, { recursive: true });
    const byPath = new Map<string, Uint8Array>();
    const tarballs: { name: string; version: string; filename: string }[] = [];
    for (const pkg of pkgs) {
      const stagingDir = path.join(
        stagingRoot,
        `${pkg.name.replace("/", "_")}-${pkg.version}`,
      );
      const packageDir = path.join(stagingDir, "package");
      await fs.mkdir(packageDir, { recursive: true });
      await fs.writeFile(
        path.join(packageDir, "package.json"),
        JSON.stringify({
          name: pkg.name,
          version: pkg.version,
          interchange: { tools: "./tools.js" },
        }),
      );
      await fs.writeFile(
        path.join(packageDir, "tools.js"),
        "// stub; importer is faked in this test\n",
      );
      const tarballPath = path.join(stagingDir, "out.tgz");
      await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
        "package",
      ]);
      const bytes = await fs.readFile(tarballPath);
      const basename = pkg.name.startsWith("@")
        ? pkg.name.split("/")[1]
        : pkg.name;
      const filename = `${String(basename)}-${pkg.version}.tgz`;
      byPath.set(`tarballs/${filename}`, bytes);
      tarballs.push({ name: pkg.name, version: pkg.version, filename });
    }
    return {
      readBlob: async (p) => {
        const b = byPath.get(p);
        if (b === undefined) throw new Error(`no blob at ${p}`);
        return b;
      },
      listBlobs: async (d) => {
        if (d !== "tarballs") throw new Error(`unexpected list dir: ${d}`);
        return Array.from(byPath.keys()).map((p) =>
          p.slice("tarballs/".length),
        );
      },
      writeMount: async (mountAbsPath) => {
        const tarballsDir = path.join(mountAbsPath, "tarballs");
        await fs.mkdir(tarballsDir, { recursive: true });
        for (const [key, bytes] of byPath) {
          const filename = key.slice("tarballs/".length);
          await fs.writeFile(path.join(tarballsDir, filename), bytes);
        }
      },
      tarballs,
    };
  }

  test("resolver picks asset entries, materialization reads them through the mount", async () => {
    const fixture = await buildAssetRegistryFixture([
      { name: "asset-tool", version: "1.2.3", exportName: "main" },
    ]);
    const assetId = "asset_workspace_builtins";

    const assetSource = new AssetRegistrySource({
      name: "workspace-builtins",
      assetId,
      readBlob: fixture.readBlob,
      listBlobs: fixture.listBlobs,
    });
    const registries = new Map<string, RegistrySource>([
      [assetSource.name, assetSource],
    ]);
    const resolver = createClosureResolver({
      registries,
      defaultRegistry: assetSource.name,
    });
    const pins: ToolPackagePin[] = [{ name: "asset-tool", version: "1.2.3" }];
    const manifest = await resolver.resolveClosure(pins);
    expect(manifest.entries).toHaveLength(1);
    const resolverEntry = manifest.entries[0];
    if (resolverEntry === undefined) throw new Error("no entry");
    if (resolverEntry.source.kind !== "asset") {
      throw new Error(
        `expected asset source, got ${resolverEntry.source.kind}`,
      );
    }
    if (resolverEntry.source.package.format !== "tarball") {
      throw new Error(
        `expected tarball package, got ${resolverEntry.source.package.format}`,
      );
    }
    expect(resolverEntry.source.assetId).toBe(assetId);
    expect(resolverEntry.source.package.path).toBe(
      `tarballs/${fixture.tarballs[0]?.filename ?? ""}`,
    );

    const mountPath = `package-registries/${assetSource.name}/`;

    // Materialize the asset tarballs onto disk at the mount. The loader's default tarball fetcher
    // resolves `kind: "asset"` entries against `assetRoot + mount + path`.
    await fixture.writeMount(path.join(assetRoot, mountPath));

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 100_000_000,
    });
    const { storeDir, entries } = await materializeClosure({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map([[assetId, mountPath]]),
      gitDirs: new Map(),
      host: { os: "linux", cpu: "x64" },
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      fetchTarball: createTarballFetcher(),
    });
    expect(entries).toHaveLength(1);
    expect(
      await storedPackageJSON(storeDir, {
        name: "asset-tool",
        version: "1.2.3",
      }),
    ).toMatchObject({ name: "asset-tool", version: "1.2.3" });
  });
});
