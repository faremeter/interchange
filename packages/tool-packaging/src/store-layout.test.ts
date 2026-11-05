import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import { pathToFileURL } from "node:url";
import * as tar from "tar";
import ssri from "ssri";
import npmRegistryFetch from "npm-registry-fetch";

import { createTarballCache } from "./cache";
import {
  type ClosureMaterializationErrorCategory,
  ClosureMaterializationError,
} from "./materialization-internal";
import {
  buildRegistryFetchOpts,
  readResponseWithLimit,
} from "./registry-fetch";
import {
  type MaterializeClosureArgs,
  materializeClosure,
  storeEntryDir,
} from "./store-layout";
import { type TarballFetcher, createTarballFetcher } from "./tarball-fetcher";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

let scratchRoot: string;
let cacheDir: string;
let instanceDir: string;
let assetRoot: string;
let fixtureSourceRoot: string;

beforeEach(async () => {
  scratchRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "tool-packaging-store-layout-"),
  );
  cacheDir = path.join(scratchRoot, "cache");
  instanceDir = path.join(scratchRoot, "instance");
  assetRoot = path.join(scratchRoot, "asset");
  fixtureSourceRoot = path.join(scratchRoot, "fixture-source");
  await fs.mkdir(cacheDir, { recursive: true });
  await fs.mkdir(instanceDir, { recursive: true });
  await fs.mkdir(assetRoot, { recursive: true });
  await fs.mkdir(fixtureSourceRoot, { recursive: true });
});

afterEach(async () => {
  await fs.rm(scratchRoot, { recursive: true, force: true });
});

interface FixtureSpec {
  name: string;
  version: string;
  interchangeToolsRelPath?: string | null; // null means omit the field
  entryModuleSource?: string; // emitted at the interchange.tools path
  extraFiles?: Record<string, string>;
  dependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
  main?: string;
  type?: "module" | "commonjs";
}

/**
 * Pack a fixture into an npm-style tarball (with a "package/" prefix).
 * Returns the raw bytes and the SRI integrity.
 */
async function packFixture(
  spec: FixtureSpec,
): Promise<{ bytes: Uint8Array; integrity: string }> {
  const stagingDir = path.join(
    fixtureSourceRoot,
    `${spec.name.replace("/", "_")}-${spec.version}`,
  );
  const packageDir = path.join(stagingDir, "package");
  await fs.mkdir(packageDir, { recursive: true });

  const pkgJson: Record<string, unknown> = {
    name: spec.name,
    version: spec.version,
  };
  const interchangeField: Record<string, unknown> = {};
  if (spec.interchangeToolsRelPath !== null) {
    interchangeField.tools = spec.interchangeToolsRelPath ?? "./tools.js";
  }
  if (Object.keys(interchangeField).length > 0) {
    pkgJson.interchange = interchangeField;
  }
  if (spec.dependencies !== undefined) {
    pkgJson.dependencies = spec.dependencies;
  }
  if (spec.optionalDependencies !== undefined) {
    pkgJson.optionalDependencies = spec.optionalDependencies;
  }
  if (spec.main !== undefined) pkgJson.main = spec.main;
  if (spec.type !== undefined) pkgJson.type = spec.type;
  await fs.writeFile(
    path.join(packageDir, "package.json"),
    JSON.stringify(pkgJson),
  );

  if (spec.entryModuleSource !== undefined) {
    const entryRel = spec.interchangeToolsRelPath ?? "./tools.js";
    const entryAbs = path.resolve(packageDir, entryRel);
    await fs.mkdir(path.dirname(entryAbs), { recursive: true });
    await fs.writeFile(entryAbs, spec.entryModuleSource);
  }

  for (const [rel, contents] of Object.entries(spec.extraFiles ?? {})) {
    const abs = path.join(packageDir, rel);
    await fs.mkdir(path.dirname(abs), { recursive: true });
    await fs.writeFile(abs, contents);
  }

  const tarballPath = path.join(stagingDir, "out.tgz");
  await tar.create(
    {
      cwd: stagingDir,
      gzip: true,
      file: tarballPath,
    },
    ["package"],
  );
  const bytes = await fs.readFile(tarballPath);
  const integrity = ssri.fromData(bytes, { algorithms: ["sha512"] }).toString();
  return { bytes, integrity };
}

async function isLaidOut(
  storeDir: string,
  name: string,
  version: string,
): Promise<boolean> {
  const manifest = path.join(
    storeEntryDir(storeDir, name, version),
    "package.json",
  );
  return fs.access(manifest).then(
    () => true,
    () => false,
  );
}

// Binds what a test holds fixed around `materializeClosure`'s per-call layout
// arguments, and gives it the production fetcher unless it supplies its own.
function createMaterializer(
  config: Pick<MaterializeClosureArgs, "cache" | "registries" | "host"> &
    Partial<Pick<MaterializeClosureArgs, "fetchTarball">>,
) {
  const fetchTarball = config.fetchTarball ?? createTarballFetcher();
  return {
    materialize: (
      args: Pick<
        MaterializeClosureArgs,
        | "manifest"
        | "instanceScratchDir"
        | "assetRoot"
        | "assetMounts"
        | "gitDirs"
      >,
    ) => materializeClosure({ ...args, ...config, fetchTarball }),
  };
}

describe("materializeClosure layout", () => {
  test("lays out a workspace: protocol dependency against its closure member", async () => {
    // A source-workspace member depends on another member via `workspace:*`.
    // That protocol is not semver, so before it is normalized the range
    // resolver rejects the layout with "no satisfying version"; materialization must
    // instead match the one closure entry of that name.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const app = await packFixture({
      name: "@wf/app",
      version: "1.0.0",
      entryModuleSource: "// stub",
      dependencies: { "@wf/lib": "workspace:*" },
    });
    const lib = await packFixture({
      name: "@wf/lib",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      extraFiles: { "index.mjs": "export const x = 1;\n" },
    });

    const fetchTarball: TarballFetcher = async (entry) => {
      if (entry.name === "@wf/app") return app.bytes;
      if (entry.name === "@wf/lib") return lib.bytes;
      throw new Error(`unexpected fetch ${entry.name}`);
    };
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "@wf/app", version: "1.0.0" }],
      entries: [
        {
          name: "@wf/app",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: app.integrity,
          },
        },
        {
          name: "@wf/lib",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: lib.integrity,
          },
        },
      ],
    };

    // Before the workspace: normalization this rejected with
    // `package.entry.invalid`; it now lays out cleanly.
    const { storeDir } = await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    expect(await isLaidOut(storeDir, "@wf/app", "1.0.0")).toBe(true);
    expect(await isLaidOut(storeDir, "@wf/lib", "1.0.0")).toBe(true);
  });

  test("lays out a catalog: protocol dependency against its closure member", async () => {
    // A monorepo member's on-disk package.json keeps a bare `catalog:` range;
    // the hub already expanded it and pinned the external at a concrete version
    // in the closure. Like `workspace:`, `catalog:` is not semver, so the layout
    // must match the one closure entry of that name rather than reject it.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const app = await packFixture({
      name: "@wf/catalog-app",
      version: "1.0.0",
      entryModuleSource: "// stub",
      dependencies: { "left-pad": "catalog:" },
    });
    const leftPad = await packFixture({
      name: "left-pad",
      version: "1.3.0",
      interchangeToolsRelPath: null,
      extraFiles: { "index.mjs": "export const x = 1;\n" },
    });

    const fetchTarball: TarballFetcher = async (entry) => {
      if (entry.name === "@wf/catalog-app") return app.bytes;
      if (entry.name === "left-pad") return leftPad.bytes;
      throw new Error(`unexpected fetch ${entry.name}`);
    };
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "@wf/catalog-app", version: "1.0.0" }],
      entries: [
        {
          name: "@wf/catalog-app",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: app.integrity,
          },
        },
        {
          name: "left-pad",
          version: "1.3.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: leftPad.integrity,
          },
        },
      ],
    };

    const { storeDir } = await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    expect(await isLaidOut(storeDir, "@wf/catalog-app", "1.0.0")).toBe(true);
  });

  test("materializes package files independently from the cache extraction", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "copied-tool",
      version: "1.0.0",
      entryModuleSource: "original contents",
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });

    await materializer.materialize({
      manifest: {
        schemaVersion: "1",
        topLevel: [{ name: "copied-tool", version: "1.0.0" }],
        entries: [
          {
            name: "copied-tool",
            version: "1.0.0",
            source: {
              kind: "registry",
              registry: "npmjs",
              integrity: fixture.integrity,
            },
          },
        ],
      },
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });

    const extraction = await cache.extractTarball(fixture.integrity);
    try {
      const cachedFile = path.join(extraction.dir, "tools.js");
      const materializedFile = path.join(
        instanceDir,
        "store",
        "copied-tool",
        "1.0.0",
        "tools.js",
      );
      expect(await fs.readFile(materializedFile, "utf8")).toBe(
        "original contents",
      );

      await fs.writeFile(materializedFile, "materialized change");
      expect(await fs.readFile(cachedFile, "utf8")).toBe("original contents");
    } finally {
      extraction.release();
    }
  });

  test("rejects a non-positive registryFetchTimeoutMs", () => {
    expect(() => createTarballFetcher({ registryFetchTimeoutMs: 0 })).toThrow(
      /registryFetchTimeoutMs must be a positive finite number/,
    );
  });

  test("skips entries whose os does not match the host", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const matching = await packFixture({
      name: "watcher-linux",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const nonMatching = await packFixture({
      name: "watcher-darwin",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const fetched: string[] = [];
    const fetchTarball: TarballFetcher = async (entry) => {
      fetched.push(`${entry.name}@${entry.version}`);
      return entry.name === "watcher-linux"
        ? matching.bytes
        : nonMatching.bytes;
    };
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: "watcher-linux", version: "1.0.0" },
        { name: "watcher-darwin", version: "1.0.0" },
      ],
      entries: [
        {
          name: "watcher-linux",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: matching.integrity,
          },
          os: ["linux"],
        },
        {
          name: "watcher-darwin",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: nonMatching.integrity,
          },
          os: ["darwin"],
        },
      ],
    };

    const { storeDir } = await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    expect(await isLaidOut(storeDir, "watcher-linux", "1.0.0")).toBe(true);
    expect(await isLaidOut(storeDir, "watcher-darwin", "1.0.0")).toBe(false);
    expect(fetched).toEqual(["watcher-linux@1.0.0"]);
  });

  test("honors npm's os/cpu negation syntax", async () => {
    // npm's os/cpu field accepts `!host` tokens to mean "anything
    // except". A naive `entries.includes(host)` treats `!win32` as a
    // literal allow-list token and would skip a package on linux.
    // Exercise each shape: bare allow, bare deny, negated allow,
    // negated deny.
    const fixtures = await Promise.all([
      packFixture({
        name: "allow-bare",
        version: "1.0.0",
        entryModuleSource: "",
      }),
      packFixture({
        name: "deny-bare",
        version: "1.0.0",
        entryModuleSource: "",
      }),
      packFixture({
        name: "allow-negate",
        version: "1.0.0",
        entryModuleSource: "",
      }),
      packFixture({
        name: "deny-negate",
        version: "1.0.0",
        entryModuleSource: "",
      }),
    ]);
    const byName = new Map(
      fixtures.map((f, i) => [
        ["allow-bare", "deny-bare", "allow-negate", "deny-negate"][i] ?? "",
        f,
      ]),
    );
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async (entry) => {
        const f = byName.get(entry.name);
        if (f === undefined) throw new Error(`no fixture: ${entry.name}`);
        return f.bytes;
      },
    });
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [
        { name: "allow-bare", version: "1.0.0" },
        { name: "deny-bare", version: "1.0.0" },
        { name: "allow-negate", version: "1.0.0" },
        { name: "deny-negate", version: "1.0.0" },
      ],
      entries: [
        {
          name: "allow-bare",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: byName.get("allow-bare")?.integrity ?? "",
          },
          os: ["linux"],
        },
        {
          name: "deny-bare",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: byName.get("deny-bare")?.integrity ?? "",
          },
          os: ["win32"],
        },
        {
          name: "allow-negate",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: byName.get("allow-negate")?.integrity ?? "",
          },
          os: ["!win32"],
        },
        {
          name: "deny-negate",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: byName.get("deny-negate")?.integrity ?? "",
          },
          os: ["!linux"],
        },
      ],
    };

    const { storeDir } = await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    expect(await isLaidOut(storeDir, "allow-bare", "1.0.0")).toBe(true);
    expect(await isLaidOut(storeDir, "allow-negate", "1.0.0")).toBe(true);
    expect(await isLaidOut(storeDir, "deny-bare", "1.0.0")).toBe(false);
    expect(await isLaidOut(storeDir, "deny-negate", "1.0.0")).toBe(false);
  });

  test("uses cache hit and skips the fetcher", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "cached",
      version: "1.0.0",
      entryModuleSource: "",
    });
    await cache.put(fixture.integrity, fixture.bytes);
    let fetcherCalls = 0;
    const fetchTarball: TarballFetcher = async () => {
      fetcherCalls += 1;
      throw new Error("fetcher should not be called on cache hit");
    };
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
    });
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "cached", version: "1.0.0" }],
      entries: [
        {
          name: "cached",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: fixture.integrity,
          },
        },
      ],
    };
    await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    expect(fetcherCalls).toBe(0);
  });
});

describe("materialization error categories", () => {
  async function expectCategory(
    args: {
      manifest: ToolPackageManifest;
      fetchTarball: TarballFetcher;
    },
    category: ClosureMaterializationErrorCategory,
  ): Promise<void> {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: args.fetchTarball,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: args.manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${caught === undefined ? "no error" : String(caught)}`,
      );
    }
    expect(caught.category).toBe(category);
  }

  test("integrity.mismatch when fetched bytes do not match", async () => {
    const fixture = await packFixture({
      name: "i",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const wrongBytes = new TextEncoder().encode("not the right bytes");
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "i", version: "1.0.0" }],
          entries: [
            {
              name: "i",
              version: "1.0.0",
              source: {
                kind: "registry",
                registry: "npmjs",
                integrity: fixture.integrity,
              },
            },
          ],
        },
        fetchTarball: async () => wrongBytes,
      },
      "integrity.mismatch",
    );
  });

  test("git.materialization.failed when a git-sourced entry has no gitDir", async () => {
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "g", version: "1.0.0" }],
          entries: [
            {
              name: "g",
              version: "1.0.0",
              source: {
                kind: "asset",
                assetId: "asset_wf",
                package: {
                  format: "source",
                  commitSha: "abc123",
                  packageDir: ".",
                  treeOid: "def456",
                },
              },
            },
          ],
        },
        // The store rejects the git entry before any tarball I/O, so this
        // fetcher must never run.
        fetchTarball: async () => {
          throw new Error("fetchTarball must not run for a git-sourced entry");
        },
      },
      "git.materialization.failed",
    );
  });

  test("tarball.extract.failed on garbage bytes leaves the cache entry intact", async () => {
    // We can't easily produce bytes that match an integrity but are
    // not a valid tarball; instead, build a real cache entry whose
    // bytes we control. Stuff the cache directly via a put with
    // matched integrity, then point the manifest at it. Extraction
    // fails inside `tar.extract` (not at the integrity re-check), so
    // materialization must NOT evict — a sibling agent in the same process
    // may be copying from the same extraction tree at this moment.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const garbage = new TextEncoder().encode("definitely not a tarball");
    const integrity = ssri
      .fromData(garbage, { algorithms: ["sha512"] })
      .toString();
    await cache.put(integrity, garbage);
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => garbage,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "g", version: "1.0.0" }],
          entries: [
            {
              name: "g",
              version: "1.0.0",
              source: { kind: "registry", registry: "npmjs", integrity },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${String(caught)}`,
      );
    }
    expect(caught.category).toBe("tarball.extract.failed");
    // The integrity check at extract time passed; the failure was a
    // tar parse error, so the cached bytes must remain.
    expect(await cache.get(integrity)).not.toBeNull();
  });

  test("transient FS errors during extract do not evict the cache entry", async () => {
    const fixture = await packFixture({
      name: "t",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const realCache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await realCache.put(fixture.integrity, fixture.bytes);
    const evictCalls: string[] = [];
    const wrappedCache = {
      get: realCache.get.bind(realCache),
      has: realCache.has.bind(realCache),
      pin: realCache.pin.bind(realCache),
      put: realCache.put.bind(realCache),
      size: realCache.size.bind(realCache),
      sweepOrphans: realCache.sweepOrphans.bind(realCache),
      extractTarball: async (
        _integrity: string,
      ): Promise<{ dir: string; release: () => void }> => {
        const err: NodeJS.ErrnoException = new Error("simulated EIO");
        err.code = "EIO";
        throw err;
      },
      evict: async (integrity: string): Promise<void> => {
        evictCalls.push(integrity);
        await realCache.evict(integrity);
      },
    };
    const materializer = createMaterializer({
      cache: wrappedCache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "t", version: "1.0.0" }],
          entries: [
            {
              name: "t",
              version: "1.0.0",
              source: {
                kind: "registry",
                registry: "npmjs",
                integrity: fixture.integrity,
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${String(caught)}`,
      );
    }
    expect(caught.category).toBe("tarball.extract.failed");
    expect(evictCalls).toEqual([]);
    expect(await realCache.get(fixture.integrity)).not.toBeNull();
  });

  test("integrity mismatch during extract evicts the cache entry", async () => {
    const fixture = await packFixture({
      name: "m",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const realCache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await realCache.put(fixture.integrity, fixture.bytes);
    const evictCalls: string[] = [];
    const { TarballIntegrityMismatchError } = await import("./cache");
    const wrappedCache = {
      get: realCache.get.bind(realCache),
      has: realCache.has.bind(realCache),
      pin: realCache.pin.bind(realCache),
      put: realCache.put.bind(realCache),
      size: realCache.size.bind(realCache),
      sweepOrphans: realCache.sweepOrphans.bind(realCache),
      extractTarball: async (
        integrity: string,
      ): Promise<{ dir: string; release: () => void }> => {
        throw new TarballIntegrityMismatchError(integrity);
      },
      evict: async (integrity: string): Promise<void> => {
        evictCalls.push(integrity);
        await realCache.evict(integrity);
      },
    };
    const materializer = createMaterializer({
      cache: wrappedCache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              source: {
                kind: "registry",
                registry: "npmjs",
                integrity: fixture.integrity,
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${String(caught)}`,
      );
    }
    expect(caught.category).toBe("tarball.extract.failed");
    expect(evictCalls).toEqual([fixture.integrity]);
    expect(await realCache.get(fixture.integrity)).toBeNull();
  });

  test("registry.unknown when manifest names a registry not in config", async () => {
    const fixture = await packFixture({
      name: "u",
      version: "1.0.0",
      entryModuleSource: "",
    });
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "u", version: "1.0.0" }],
          entries: [
            {
              name: "u",
              version: "1.0.0",
              source: {
                kind: "registry",
                registry: "ghost",
                integrity: fixture.integrity,
              },
            },
          ],
        },
        // Default fetcher used; no override; materialization resolves
        // registry "ghost" against config and fails before fetching.
        fetchTarball: async () => {
          throw new Error("unreachable");
        },
      },
      "registry.unknown",
    );
  });

  test("package.entry.invalid when the package.json bytes are malformed JSON", async () => {
    // Hand-pack a tarball whose package.json contains invalid JSON so
    // materialization's JSON.parse throws. The wrapper translates the
    // SyntaxError into the package.entry.invalid category rather than
    // letting it bubble out as a raw SyntaxError.
    const stagingDir = path.join(scratchRoot, "bad-json-staging");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    await fs.writeFile(path.join(pkgDir, "package.json"), "{ not valid json");
    const tarballPath = path.join(stagingDir, "bad.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const bytes = await fs.readFile(tarballPath);
    const integrity = ssri
      .fromData(bytes, { algorithms: ["sha512"] })
      .toString();

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => bytes,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "bad-json", version: "1.0.0" }],
          entries: [
            {
              name: "bad-json",
              version: "1.0.0",
              source: { kind: "registry", registry: "npmjs", integrity },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/malformed package\.json/);
    }
  });

  test("package.entry.invalid when dependencies is an array, not an object map", async () => {
    // `Object.entries(["foo"])` yields `[["0", "foo"]]`, which would
    // feed nonsense package names into the closure resolver and fail
    // far downstream with a misleading message. The package-json read
    // must reject the wrong-shape value at the boundary with a clear,
    // structured failure.
    const stagingDir = path.join(scratchRoot, "deps-array-staging");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({
        name: "deps-array",
        version: "1.0.0",
        dependencies: ["nothing-good"],
      }),
    );
    const tarballPath = path.join(stagingDir, "bad.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const bytes = await fs.readFile(tarballPath);
    const integrity = ssri
      .fromData(bytes, { algorithms: ["sha512"] })
      .toString();

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => bytes,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "deps-array", version: "1.0.0" }],
          entries: [
            {
              name: "deps-array",
              version: "1.0.0",
              source: { kind: "registry", registry: "npmjs", integrity },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /package\.json#dependencies.*must be an object map/,
      );
    }
  });

  test("package.entry.invalid when optionalDependencies is an array, not an object map", async () => {
    const stagingDir = path.join(scratchRoot, "opt-deps-array-staging");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({
        name: "opt-deps-array",
        version: "1.0.0",
        optionalDependencies: ["nothing-good"],
      }),
    );
    const tarballPath = path.join(stagingDir, "bad.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const bytes = await fs.readFile(tarballPath);
    const integrity = ssri
      .fromData(bytes, { algorithms: ["sha512"] })
      .toString();

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => bytes,
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "opt-deps-array", version: "1.0.0" }],
          entries: [
            {
              name: "opt-deps-array",
              version: "1.0.0",
              source: { kind: "registry", registry: "npmjs", integrity },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /package\.json#optionalDependencies.*must be an object map/,
      );
    }
  });

  test("tarball.missing when asset-sourced file is absent", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              source: {
                kind: "asset",
                assetId: "asset_one",
                package: {
                  format: "tarball",
                  path: "does/not/exist.tgz",
                  integrity: "sha512-AAAA",
                },
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["asset_one", "package-registries/one"]]),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${String(caught)}`,
      );
    }
    expect(caught.category).toBe("tarball.missing");
  });

  test("package.entry.invalid when an asset entry's path escapes the asset root", async () => {
    // A `..`-bearing entry.source.package.path would let a malicious
    // manifest read any file the sidecar process can open. The default fetcher
    // must reject this at the boundary rather than handing the
    // escaped absolute path to fs.readFile.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              source: {
                kind: "asset",
                assetId: "asset_one",
                package: {
                  format: "tarball",
                  path: "../../../../etc/passwd",
                  integrity: "sha512-AAAA",
                },
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["asset_one", "package-registries/one"]]),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${String(caught)}`,
      );
    }
    expect(caught.category).toBe("package.entry.invalid");
    expect(caught.message).toMatch(/escapes the declared mount/);
  });

  test("asset.mount.missing when the entry's assetId is absent from assetMounts", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      // The fetcher must never be reached; the mount check runs before
      // any I/O and reaching the fetcher would mean the gate is wrong.
      fetchTarball: async () => {
        throw new Error("unreachable");
      },
    });
    let caught: unknown;
    try {
      await materializer.materialize({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              source: {
                kind: "asset",
                assetId: "asset_missing",
                package: {
                  format: "tarball",
                  path: "tarballs/m-1.0.0.tgz",
                  integrity: "sha512-AAAA",
                },
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["asset_other", "package-registries/other"]]),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ClosureMaterializationError)) {
      throw new Error(
        `expected ClosureMaterializationError, got ${String(caught)}`,
      );
    }
    expect(caught.category).toBe("asset.mount.missing");
    expect(caught.message).toContain("m@1.0.0");
  });
});

describe("transitive resolution contract", () => {
  // Locks the closure-satisfies-itself contract: a top-level package
  // whose body imports a transitive that lives in the manifest closure
  // resolves through the per-instance node_modules layout materialization
  // builds, without help from the sidecar host's own node_modules.
  test("scoped packages land under <store>/@scope/<name>/<version>", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const scopedPkg = await packFixture({
      name: "@scope/only",
      version: "2.3.4",
      interchangeToolsRelPath: "./entry.js",
      entryModuleSource: `
export const factory = Object.assign(
  () => ({ definitions: [], run: async () => ({ callId: "x", content: "" }) }),
  { id: "@scope/only", requires: [], definitions: [] },
);
`,
    });
    await cache.put(scopedPkg.integrity, scopedPkg.bytes);

    const materializer = createMaterializer({
      cache,
      registries: new Map([["test", { url: "https://example.test" }]]),
      host: { os: process.platform, cpu: process.arch },
      fetchTarball: async () => scopedPkg.bytes,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "@scope/only", version: "2.3.4" }],
      entries: [
        {
          name: "@scope/only",
          version: "2.3.4",
          source: {
            kind: "registry",
            registry: "test",
            integrity: scopedPkg.integrity,
          },
        },
      ],
    };

    await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });

    // The store dir keeps npm's `@scope/name` shape so the scope
    // segment is a real directory, not a slash baked into the name.
    const storeDir = path.join(instanceDir, "store", "@scope", "only", "2.3.4");
    const pkg = await fs.readFile(path.join(storeDir, "package.json"), "utf8");
    expect(JSON.parse(pkg)).toMatchObject({
      name: "@scope/only",
      version: "2.3.4",
    });
  });

  test("a top-level pinned at v3 with a transitive needing v4 materializes both at their nested positions", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });

    // libA lives in node_modules under top, depends on lodash^4.
    // The top-level itself depends on lodash^3 (a hypothetical
    // direct downgrade). Both lodash versions must be in the store
    // tree, each at the position its requirer needs.
    const lodash3 = await packFixture({
      name: "lodash",
      version: "3.10.1",
      interchangeToolsRelPath: null,
      type: "module",
      main: "./lib.js",
      extraFiles: {
        "lib.js": `export const major = 3;`,
      },
    });
    const lodash4 = await packFixture({
      name: "lodash",
      version: "4.17.21",
      interchangeToolsRelPath: null,
      type: "module",
      main: "./lib.js",
      extraFiles: {
        "lib.js": `export const major = 4;`,
      },
    });
    const libA = await packFixture({
      name: "lib-a",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      type: "module",
      main: "./lib.js",
      extraFiles: {
        "lib.js": `
import { major } from "lodash";
export const seenLodash = major;
`,
      },
      dependencies: { lodash: "^4.0.0" },
    });
    const topPkg = await packFixture({
      name: "version-top",
      version: "1.0.0",
      interchangeToolsRelPath: "./index.js",
      type: "module",
      entryModuleSource: `
import { major as topSawLodash } from "lodash";
import { seenLodash } from "lib-a";
export const seen = { top: topSawLodash, libA: seenLodash };
`,
      dependencies: { lodash: "^3.0.0", "lib-a": "^1.0.0" },
    });
    await cache.put(lodash3.integrity, lodash3.bytes);
    await cache.put(lodash4.integrity, lodash4.bytes);
    await cache.put(libA.integrity, libA.bytes);
    await cache.put(topPkg.integrity, topPkg.bytes);

    const materializer = createMaterializer({
      cache,
      registries: new Map([["test", { url: "https://example.test" }]]),
      host: { os: process.platform, cpu: process.arch },
      fetchTarball: async (entry) => {
        if (entry.name === "version-top") return topPkg.bytes;
        if (entry.name === "lib-a") return libA.bytes;
        if (entry.name === "lodash" && entry.version === "3.10.1")
          return lodash3.bytes;
        if (entry.name === "lodash" && entry.version === "4.17.21")
          return lodash4.bytes;
        throw new Error(`unexpected fetch ${entry.name}@${entry.version}`);
      },
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "version-top", version: "1.0.0" }],
      entries: [
        {
          name: "version-top",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: topPkg.integrity,
          },
        },
        {
          name: "lib-a",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: libA.integrity,
          },
        },
        {
          name: "lodash",
          version: "3.10.1",
          source: {
            kind: "registry",
            registry: "test",
            integrity: lodash3.integrity,
          },
        },
        {
          name: "lodash",
          version: "4.17.21",
          source: {
            kind: "registry",
            registry: "test",
            integrity: lodash4.integrity,
          },
        },
      ],
    };

    const { storeDir } = await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    const entryUrl = pathToFileURL(
      path.join(storeEntryDir(storeDir, "version-top", "1.0.0"), "index.js"),
    ).href;
    const entry: unknown = await import(entryUrl);
    // The top saw lodash major 3 (the version satisfying its own
    // range); lib-a saw lodash major 4 (the version satisfying its
    // own range). Both must be true simultaneously, which is only
    // possible with nested node_modules entries.
    expect(entry).toMatchObject({ seen: { top: 3, libA: 4 } });

    // Verify both lodash versions actually exist in the store at
    // their nested positions, not hoisted under a single one.
    const topLodash = path.join(
      instanceDir,
      "store",
      "version-top",
      "1.0.0",
      "node_modules",
      "lodash",
    );
    const libALodash = path.join(
      instanceDir,
      "store",
      "lib-a",
      "1.0.0",
      "node_modules",
      "lodash",
    );
    const topLodashReal = await fs.realpath(topLodash);
    const libALodashReal = await fs.realpath(libALodash);
    expect(topLodashReal.endsWith(path.join("lodash", "3.10.1"))).toBe(true);
    expect(libALodashReal.endsWith(path.join("lodash", "4.17.21"))).toBe(true);
  });

  test("three requirers with overlapping ranges land on the first-arrival pick", async () => {
    // Closure carries three versions of `dep` (1.0.0, 1.5.0, 2.0.0)
    // and three requirers, each declaring a range that more than one
    // closure version satisfies. Materialization's first-arrival BFS from
    // the top-level pin records its pick for each (name, range), so
    // later requirers see the same answer even though semver alone
    // would return a different "max satisfying" version. This locks
    // the requirer-vs-requirer consistency the resolver assumes when
    // it stops fetching once a (name, range) is recorded.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });

    async function packDep(version: string) {
      return packFixture({
        name: "dep",
        version,
        interchangeToolsRelPath: null,
        type: "module",
        main: "./lib.js",
        extraFiles: {
          "lib.js": `export const v = ${JSON.stringify(version)};`,
        },
      });
    }

    const dep1 = await packDep("1.0.0");
    const dep1_5 = await packDep("1.5.0");
    const dep2 = await packDep("2.0.0");

    // Three requirers under the top-level package. The top requires
    // them in declaration order so materialization's BFS visits them in
    // that order; rA arrives first with range "^1" and records
    // dep->1.5.0 in the resolution map (max-satisfying inside the
    // closure for "^1" is 1.5.0). rB ("^1.0") and rC ("^1.5") both
    // also have 1.5.0 as their max-satisfying answer, but the point
    // here is to lock the first-arrival behavior: every subsequent
    // requirer that uses one of those ranges receives the recorded
    // pick rather than re-deriving against the current closure shape.
    const rA = await packFixture({
      name: "req-a",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      type: "module",
      main: "./lib.js",
      extraFiles: { "lib.js": `export {};` },
      dependencies: { dep: "^1" },
    });
    const rB = await packFixture({
      name: "req-b",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      type: "module",
      main: "./lib.js",
      extraFiles: { "lib.js": `export {};` },
      dependencies: { dep: "^1.0" },
    });
    const rC = await packFixture({
      name: "req-c",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      type: "module",
      main: "./lib.js",
      extraFiles: { "lib.js": `export {};` },
      dependencies: { dep: "^1.5" },
    });
    const top = await packFixture({
      name: "first-arrival-top",
      version: "1.0.0",
      interchangeToolsRelPath: "./index.js",
      type: "module",
      entryModuleSource: `
export const factory = Object.assign(
  () => ({
    definitions: [{ name: "echo", description: "", inputSchema: {} }],
    run: async (call) => ({ callId: call.id, content: "" }),
  }),
  { id: "fa/top", requires: [], definitions: [{ name: "echo" }] },
);
`,
      dependencies: {
        "req-a": "1.0.0",
        "req-b": "1.0.0",
        "req-c": "1.0.0",
      },
    });

    for (const fx of [dep1, dep1_5, dep2, rA, rB, rC, top]) {
      await cache.put(fx.integrity, fx.bytes);
    }

    const materializer = createMaterializer({
      cache,
      registries: new Map([["test", { url: "https://example.test" }]]),
      host: { os: process.platform, cpu: process.arch },
      fetchTarball: async (entry) => {
        if (entry.name === "first-arrival-top") return top.bytes;
        if (entry.name === "req-a") return rA.bytes;
        if (entry.name === "req-b") return rB.bytes;
        if (entry.name === "req-c") return rC.bytes;
        if (entry.name === "dep") {
          if (entry.version === "1.0.0") return dep1.bytes;
          if (entry.version === "1.5.0") return dep1_5.bytes;
          if (entry.version === "2.0.0") return dep2.bytes;
        }
        throw new Error(`unexpected fetch ${entry.name}@${entry.version}`);
      },
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "first-arrival-top", version: "1.0.0" }],
      entries: [
        {
          name: "first-arrival-top",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: top.integrity,
          },
        },
        {
          name: "req-a",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: rA.integrity,
          },
        },
        {
          name: "req-b",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: rB.integrity,
          },
        },
        {
          name: "req-c",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: rC.integrity,
          },
        },
        {
          name: "dep",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: dep1.integrity,
          },
        },
        {
          name: "dep",
          version: "1.5.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: dep1_5.integrity,
          },
        },
        {
          name: "dep",
          version: "2.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: dep2.integrity,
          },
        },
      ],
    };

    await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });

    // Every requirer's node_modules/dep should resolve to the same
    // store slot — the first-arrival pick of 1.5.0 for ranges that
    // all satisfy it. If materialization re-ran maxSatisfying per requirer
    // without the recording layer, all three would still pick 1.5.0
    // in this exact scenario, but the test's point is the
    // recorded-pick path: a future change that drifts requirer C onto
    // 2.0.0 (say by extending its range) should not leak across to
    // requirer A or B, and the test would surface that drift the
    // moment one of the recorded slots resolved differently.
    const reqAOnDep = await fs.realpath(
      path.join(instanceDir, "store", "req-a", "1.0.0", "node_modules", "dep"),
    );
    const reqBOnDep = await fs.realpath(
      path.join(instanceDir, "store", "req-b", "1.0.0", "node_modules", "dep"),
    );
    const reqCOnDep = await fs.realpath(
      path.join(instanceDir, "store", "req-c", "1.0.0", "node_modules", "dep"),
    );
    expect(reqAOnDep.endsWith(path.join("dep", "1.5.0"))).toBe(true);
    expect(reqBOnDep.endsWith(path.join("dep", "1.5.0"))).toBe(true);
    expect(reqCOnDep.endsWith(path.join("dep", "1.5.0"))).toBe(true);
  });
});

describe("entry-path containment", () => {
  test("rejects a tarball symlink whose target escapes the extraction root", async () => {
    // node-tar strips escape-pointing symlinks at extraction time, so
    // the layout-build guard is defense-in-depth: corrupt cache state
    // or a future swap to a different extractor must not surface a
    // symlink into the per-instance layout that points outside the
    // package's extracted bytes. The test pre-populates a cache
    // extraction directory directly to drive that guard.
    const sinkPath = path.join(scratchRoot, "symlink-sink.txt");
    await fs.writeFile(sinkPath, "secret");

    const fixture = await packFixture({
      name: "evil-symlink",
      version: "1.0.0",
      entryModuleSource: "export const main = null;",
    });

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await cache.put(fixture.integrity, fixture.bytes);
    // Force the extraction to materialize once, then plant an
    // escape-pointing symlink inside it before materialization's
    // layout pass picks it up.
    const extractedHandle = await cache.extractTarball(fixture.integrity);
    const extractedRoot = extractedHandle.dir;
    await fs.symlink(
      path.relative(extractedRoot, sinkPath),
      path.join(extractedRoot, "evil-link"),
    );
    extractedHandle.release();

    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-symlink", version: "1.0.0" }],
      entries: [
        {
          name: "evil-symlink",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: fixture.integrity,
          },
        },
      ],
    };

    let caught: unknown;
    try {
      await materializer.materialize({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /escapes the package extraction directory/,
      );
      expect(caught.message).toContain("evil-symlink@1.0.0");
    }
  });

  test("rejects a chained symlink whose realpath escapes the extraction root", async () => {
    // A single-hop check (`path.resolve(dirname(src), target)`) sees
    // only the literal target string. A symlink whose literal target
    // string stays inside the extraction root, but whose canonical
    // realpath chains through another symlink to land outside, would
    // slip past. Plant exactly that shape: `jump` → `inner/escape`
    // where `escape` is itself a symlink that escapes. The literal
    // resolution of `jump`'s target stays inside; the realpath does
    // not.
    const sinkPath = path.join(scratchRoot, "chained-sink.txt");
    await fs.writeFile(sinkPath, "secret");

    const fixture = await packFixture({
      name: "evil-chain",
      version: "1.0.0",
      entryModuleSource: "export const main = null;",
    });

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await cache.put(fixture.integrity, fixture.bytes);
    const extractedHandle = await cache.extractTarball(fixture.integrity);
    const extractedRoot = extractedHandle.dir;
    extractedHandle.release();

    // `aaa-jump` is named to sort before the inner directory so
    // copyTree's walk hits it before recursing into the inner
    // tree. Its literal target string `inner/escape` resolves to a
    // path inside the extraction root, so the literal-target check
    // accepts it; only the realpath sees through `escape` to the
    // sink outside.
    const innerDir = path.join(extractedRoot, "inner");
    await fs.mkdir(innerDir, { recursive: true });
    await fs.symlink(
      path.relative(innerDir, sinkPath),
      path.join(innerDir, "escape"),
    );
    await fs.symlink("inner/escape", path.join(extractedRoot, "aaa-jump"));

    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-chain", version: "1.0.0" }],
      entries: [
        {
          name: "evil-chain",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: fixture.integrity,
          },
        },
      ],
    };

    let caught: unknown;
    try {
      await materializer.materialize({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /escapes the package extraction directory/,
      );
    }
  });

  test("accepts a dangling relative symlink whose literal target stays inside the extraction root", async () => {
    // The realpath-based containment check must not reject a symlink
    // whose target chain ENOENTs before the final inode — a dangling
    // relative symlink within the extraction root cannot escape (the
    // target name is fixed; even if the target is later created, it
    // would land at the same inside-the-root path). Plant a
    // dangling-but-contained symlink and verify the layout succeeds.
    const fixture = await packFixture({
      name: "ok-dangling",
      version: "1.0.0",
      entryModuleSource: `
export const main = Object.assign(
  () => ({ definitions: [], run: async () => ({ callId: "x", content: "" }) }),
  { id: "ok-dangling/main", requires: [], definitions: [] },
);
`,
    });

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await cache.put(fixture.integrity, fixture.bytes);
    const extractedHandle = await cache.extractTarball(fixture.integrity);
    const extractedRoot = extractedHandle.dir;

    // Relative symlink whose literal target string resolves to a path
    // inside the extraction root, but the target does not exist on
    // disk. realpath ENOENTs; materialization should fall back to the
    // literal-path containment check and accept.
    await fs.symlink(
      "missing-sibling.txt",
      path.join(extractedRoot, "dangling-link"),
    );
    extractedHandle.release();

    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "ok-dangling", version: "1.0.0" }],
      entries: [
        {
          name: "ok-dangling",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: fixture.integrity,
          },
        },
      ],
    };

    const { storeDir } = await materializer.materialize({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
    });
    expect(await isLaidOut(storeDir, "ok-dangling", "1.0.0")).toBe(true);
  });

  test("rejects a dangling symlink whose literal target escapes the extraction root", async () => {
    // The dangling-symlink fallback only excuses ENOENT chains whose
    // literal resolved path stays inside the extraction root. A
    // dangling link pointing outside is still an escape: when (and
    // if) the target is later created, the link would resolve to a
    // path outside the package tree.
    const fixture = await packFixture({
      name: "evil-dangling",
      version: "1.0.0",
      entryModuleSource: "export const main = null;",
    });

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await cache.put(fixture.integrity, fixture.bytes);
    const extractedHandle = await cache.extractTarball(fixture.integrity);
    const extractedRoot = extractedHandle.dir;

    // Symlink target string resolves outside the extraction root;
    // target does not exist, so realpath ENOENTs and the fallback
    // checks the literal path — which escapes — and rejects.
    await fs.symlink(
      "../missing-outside.txt",
      path.join(extractedRoot, "escape-dangling"),
    );
    extractedHandle.release();

    const materializer = createMaterializer({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-dangling", version: "1.0.0" }],
      entries: [
        {
          name: "evil-dangling",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "npmjs",
            integrity: fixture.integrity,
          },
        },
      ],
    };

    let caught: unknown;
    try {
      await materializer.materialize({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /escapes the package extraction directory/,
      );
    }
  });

  test("rejects an asset source.package.path that traverses out of its declared mount", async () => {
    // Two sibling mounts live under the same assetRoot. The manifest
    // entry names mountB but its source.package.path walks `..` up to mountA's
    // tarball. assetRoot-only containment would accept this; per-mount
    // containment must reject it as a cross-mount traversal.
    const mountARel = "package-registries/a/";
    const mountBRel = "package-registries/b/";
    const mountAAbs = path.join(assetRoot, mountARel);
    const mountBAbs = path.join(assetRoot, mountBRel);
    const tarballsA = path.join(mountAAbs, "tarballs");
    await fs.mkdir(tarballsA, { recursive: true });
    await fs.mkdir(mountBAbs, { recursive: true });

    const fixture = await packFixture({
      name: "pkg",
      version: "1.0.0",
      entryModuleSource: "module.exports = {};",
    });
    await fs.writeFile(
      path.join(tarballsA, "pkg-1.0.0.tgz"),
      new Uint8Array(fixture.bytes),
    );

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const materializer = createMaterializer({
      cache,
      registries: new Map(),
      host: { os: process.platform, cpu: process.arch },
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "pkg", version: "1.0.0" }],
      entries: [
        {
          name: "pkg",
          version: "1.0.0",
          source: {
            kind: "asset",
            assetId: "ast_b",
            package: {
              format: "tarball",
              path: "../a/tarballs/pkg-1.0.0.tgz",
              integrity: fixture.integrity,
            },
          },
        },
      ],
    };

    let caught: unknown;
    try {
      await materializer.materialize({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["ast_b", mountBRel]]),
        gitDirs: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/cross-mount traversal/);
    }
  });
});

describe("buildRegistryFetchOpts", () => {
  test("passes the basic-auth password verbatim to forceAuth", () => {
    // `npm-registry-fetch` base64-encodes `<user>:<pass>` itself when
    // it builds the `Authorization: Basic` header, so the password
    // component handed to `forceAuth` must be plaintext. Pre-encoding
    // would land `base64(plaintext)` in the on-wire credential, and
    // the registry would reject the request as authentication failure.
    const opts = buildRegistryFetchOpts({
      url: "https://r.test",
      auth: { basic: { user: "alice", pass: "s3cret" } },
    });
    expect(opts.forceAuth).toEqual({ username: "alice", password: "s3cret" });
  });

  test("forwards bearer tokens as `token`", () => {
    const opts = buildRegistryFetchOpts({
      url: "https://r.test",
      auth: { token: "tok_123" },
    });
    expect(opts.token).toBe("tok_123");
    expect("forceAuth" in opts).toBe(false);
  });
});

describe("readResponseWithLimit", () => {
  const stubCtx = {
    registry: "npmjs",
    name: "evil-pkg",
    version: "1.0.0",
  } as const;

  test("rejects a Content-Length that overshoots the cap", async () => {
    const cap = 1024;
    const res = new Response(new Uint8Array(0), {
      headers: { "content-length": String(cap + 1) },
    });
    let caught: unknown;
    try {
      await readResponseWithLimit(res, cap, stubCtx);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/exceeds the/);
    }
  });

  test("rejects a Content-Length whose shape is not digit-only", async () => {
    // `1e9` reads as 1e9 against Number() but only matters as a cap
    // smuggle if a downstream check assumes RFC 9110's digit shape.
    const res = new Response(new Uint8Array(0), {
      headers: { "content-length": "1e9" },
    });
    let caught: unknown;
    try {
      await readResponseWithLimit(res, 10 * 1024 * 1024, stubCtx);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/non-digit Content-Length/);
    }
  });

  test("aborts a streamed body once the cap is crossed (header missing)", async () => {
    const cap = 1024;
    // Stream chunks past the cap with no Content-Length so the first
    // guard is bypassed and the streaming tally is the one that fires.
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let i = 0; i < 4; i += 1) {
          controller.enqueue(new Uint8Array(512));
        }
        controller.close();
      },
    });
    const res = new Response(stream);
    let caught: unknown;
    try {
      await readResponseWithLimit(res, cap, stubCtx);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/streamed past/);
    }
  });

  test("accepts a body that lands under the cap", async () => {
    const cap = 1024;
    const payload = new Uint8Array(512).fill(0x7f);
    const res = new Response(payload, {
      headers: { "content-length": String(payload.byteLength) },
    });
    const buf = await readResponseWithLimit(res, cap, stubCtx);
    expect(buf.byteLength).toBe(payload.byteLength);
    expect(buf[0]).toBe(0x7f);
  });

  test("aborts a stalled body when the fetch deadline signal fires", async () => {
    // A body that never produces a chunk and never closes: reader.read()
    // stays pending until the deadline cancels it. Without the signal
    // guard this would block forever under the byte cap.
    const stream = new ReadableStream<Uint8Array>({
      start() {
        // never enqueue, never close
      },
    });
    const res = new Response(stream);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20);
    let caught: unknown;
    try {
      await readResponseWithLimit(res, 1024, stubCtx, controller.signal);
    } catch (err) {
      caught = err;
    } finally {
      clearTimeout(timer);
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/exceeded the registry fetch timeout/);
    }
  });

  // The cases above feed a web `ReadableStream` (getReader). The default
  // fetcher pairs readResponseWithLimit with npm-registry-fetch, whose
  // `Response.body` is a Minipass/Node stream — async-iterable, Buffer
  // chunks, no getReader. These cases pin that Node-stream body path:
  // correct reads, the byte cap, and the abort deadline must all hold on
  // the shape the real fetch returns.
  function nodeBodyResponse(
    body: Readable,
    headers: Record<string, string> = {},
  ): Response {
    // npm-registry-fetch's result is typed `Response` but its `.body` is
    // a Node stream, not a web ReadableStream. readResponseWithLimit only
    // reads `.headers.get` and `.body`, so a structural stand-in with a
    // Node body faithfully models that runtime shape.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- deliberately modeling npm-registry-fetch's Response-typed, Node-bodied result; the function under test only touches headers.get and body.
    return { headers: new Headers(headers), body } as unknown as Response;
  }

  test("reads a real npm-registry-fetch (Minipass) response body end to end", async () => {
    // The regression the seam-fed cases could not catch: the default
    // fetcher's real npm-registry-fetch body is a Minipass with no
    // getReader. Drive an actual fetch against a local tarball server so
    // the exact production body shape flows through readResponseWithLimit.
    const payload = new Uint8Array(2048);
    for (let i = 0; i < payload.length; i += 1) payload[i] = i % 251;
    const server = Bun.serve({
      port: 0,
      fetch: () => new Response(payload),
    });
    try {
      const url = `http://localhost:${server.port}/pkg/-/pkg-1.0.0.tgz`;
      const res = await npmRegistryFetch(url, {});
      const buf = await readResponseWithLimit(res, 10 * 1024 * 1024, stubCtx);
      expect(buf.byteLength).toBe(payload.byteLength);
      expect(Buffer.from(buf).equals(Buffer.from(payload))).toBeTruthy();
    } finally {
      await server.stop(true);
    }
  });

  test("concatenates a multi-chunk Node-stream body under the cap", async () => {
    const parts = [
      Buffer.from([1, 2, 3]),
      Buffer.from([4, 5]),
      Buffer.from([6, 7, 8, 9]),
    ];
    const res = nodeBodyResponse(Readable.from(parts));
    const buf = await readResponseWithLimit(res, 1024, stubCtx);
    expect(Array.from(buf)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  test("enforces the byte cap on a Node-stream body with no Content-Length", async () => {
    const cap = 1024;
    // Four 512-byte chunks, no Content-Length, so the streaming tally is
    // the guard that must fire on the Node body path.
    const chunks = Array.from({ length: 4 }, () => Buffer.alloc(512));
    const res = nodeBodyResponse(Readable.from(chunks));
    let caught: unknown;
    try {
      await readResponseWithLimit(res, cap, stubCtx);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/streamed past/);
    }
  });

  test("honors the fetch deadline on a stalled Node-stream body", async () => {
    // A Node stream that never pushes and never ends: the for-await parks
    // until the deadline signal destroys it. Without the destroy-driven
    // abort this would block forever under the byte cap.
    const body = new Readable({
      read() {
        // never push, never end — a stalled body the deadline must break
      },
    });
    const res = nodeBodyResponse(body);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20);
    let caught: unknown;
    try {
      await readResponseWithLimit(res, 1024, stubCtx, controller.signal);
    } catch (err) {
      caught = err;
    } finally {
      clearTimeout(timer);
    }
    expect(caught).toBeInstanceOf(ClosureMaterializationError);
    if (caught instanceof ClosureMaterializationError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/exceeded the registry fetch timeout/);
    }
  });
});

describe("materializeClosure", () => {
  test("lays out a resolvable store dir without importing any author code", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });

    // The top-level entry's module has an import-time side effect: if it
    // were ever imported, it would write a sentinel file. materializeClosure
    // is the eval-free primitive — it must lay out the store without
    // running this module, so the sentinel must not exist afterward.
    const sentinelPath = path.join(scratchRoot, "author-code-ran.marker");

    const depPkg = await packFixture({
      name: "@closure-scope/transitive-dep",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      main: "./index.js",
      type: "module",
      extraFiles: {
        "index.js": `export const greeting = "hello from transitive";`,
      },
    });

    const topEntry = `
import { writeFileSync } from "node:fs";
import { greeting } from "@closure-scope/transitive-dep";
writeFileSync(${JSON.stringify(sentinelPath)}, "author code executed");
export const factory = Object.assign(
  () => ({
    definitions: [{ name: "echo", description: "", inputSchema: {} }],
    run: async (call) => ({ callId: call.id, content: greeting }),
  }),
  { id: "closure/top", requires: [], definitions: [{ name: "echo" }] },
);
`;
    const topPkg = await packFixture({
      name: "@closure-scope/top",
      version: "1.0.0",
      interchangeToolsRelPath: "./index.js",
      entryModuleSource: topEntry,
      type: "module",
      dependencies: { "@closure-scope/transitive-dep": "^1.0.0" },
    });

    await cache.put(topPkg.integrity, topPkg.bytes);
    await cache.put(depPkg.integrity, depPkg.bytes);

    const fetchTarball: TarballFetcher = async (entry) => {
      if (entry.name === "@closure-scope/top") return topPkg.bytes;
      if (entry.name === "@closure-scope/transitive-dep") return depPkg.bytes;
      throw new Error(`unexpected fetch: ${entry.name}`);
    };

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "@closure-scope/top", version: "1.0.0" }],
      entries: [
        {
          name: "@closure-scope/top",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: topPkg.integrity,
          },
        },
        {
          name: "@closure-scope/transitive-dep",
          version: "1.0.0",
          source: {
            kind: "registry",
            registry: "test",
            integrity: depPkg.integrity,
          },
        },
      ],
    };

    const { storeDir } = await materializeClosure({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
      gitDirs: new Map(),
      host: { os: process.platform, cpu: process.arch },
      cache,
      registries: new Map([["test", { url: "https://example.test" }]]),
      fetchTarball,
    });

    // The returned storeDir is `<instanceScratchDir>/store`.
    expect(storeDir).toBe(path.join(instanceDir, "store"));

    // The top-level package dir exists with its package.json, and the
    // dependency resolves through the layout's node_modules symlink to
    // the dep's own store entry.
    const topDir = storeEntryDir(storeDir, "@closure-scope/top", "1.0.0");
    const topPkgJson = JSON.parse(
      await fs.readFile(path.join(topDir, "package.json"), "utf8"),
    );
    expect(topPkgJson).toMatchObject({
      name: "@closure-scope/top",
      version: "1.0.0",
    });

    const depLink = path.join(
      topDir,
      "node_modules",
      "@closure-scope",
      "transitive-dep",
    );
    const depReal = await fs.realpath(depLink);
    const depStoreDir = storeEntryDir(
      storeDir,
      "@closure-scope/transitive-dep",
      "1.0.0",
    );
    expect(depReal).toBe(await fs.realpath(depStoreDir));
    const depPkgJson = JSON.parse(
      await fs.readFile(path.join(depReal, "package.json"), "utf8"),
    );
    expect(depPkgJson).toMatchObject({
      name: "@closure-scope/transitive-dep",
      version: "1.0.0",
    });

    // No author code ran: the entry module's import-time side effect
    // never fired because materializeClosure never imported it.
    let sentinelExists = true;
    try {
      await fs.stat(sentinelPath);
    } catch {
      sentinelExists = false;
    }
    expect(sentinelExists).toBe(false);
  });
});
