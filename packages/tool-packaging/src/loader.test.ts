import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import * as tar from "tar";
import ssri from "ssri";

import { createTarballCache } from "./cache";
import {
  type LoadedDirectorFactory,
  type LoadedToolFactory,
  type TarballFetcher,
  ToolLoaderError,
  buildRegistryFetchOpts,
  createToolLoader,
  readResponseWithLimit,
} from "./loader";
import type { DeployApplyErrorCategory } from "@intx/types/sidecar";
import type { ToolPackageManifest } from "@intx/types/tool-packages";

let scratchRoot: string;
let cacheDir: string;
let instanceDir: string;
let assetRoot: string;
let fixtureSourceRoot: string;

beforeEach(async () => {
  scratchRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "tool-packaging-loader-"),
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
  interchangeDirectorsRelPath?: string | null; // null means omit the field
  directorsModuleSource?: string; // emitted at the interchange.directors path
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
): Promise<{ bytes: Buffer; integrity: string }> {
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
  const interchangeField: Record<string, string> = {};
  if (spec.interchangeToolsRelPath !== null) {
    interchangeField.tools = spec.interchangeToolsRelPath ?? "./tools.js";
  }
  if (
    spec.interchangeDirectorsRelPath !== null &&
    spec.interchangeDirectorsRelPath !== undefined
  ) {
    interchangeField.directors = spec.interchangeDirectorsRelPath;
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

  if (
    spec.directorsModuleSource !== undefined &&
    spec.interchangeDirectorsRelPath !== undefined &&
    spec.interchangeDirectorsRelPath !== null
  ) {
    const directorsAbs = path.resolve(
      packageDir,
      spec.interchangeDirectorsRelPath,
    );
    await fs.mkdir(path.dirname(directorsAbs), { recursive: true });
    await fs.writeFile(directorsAbs, spec.directorsModuleSource);
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

function makeFakeFactory(
  id: string,
  requires: readonly string[] = [],
): LoadedToolFactory {
  const fn = () => ({
    definitions: [],
    run: async () => ({
      callId: "stub",
      content: "ok",
    }),
  });
  return Object.assign(fn, { id, requires });
}

/**
 * Build a director-factory-shaped stub the loader's structural check
 * accepts. `configSchema` is a callable arktype-shaped validator (the
 * real `defineDirector` requires this); the body returns a director
 * stub adequate for the loader's structural narrow without exercising
 * the runtime director protocol.
 */
function makeFakeDirectorFactory(
  id: string,
  requires: readonly string[] = [],
): LoadedDirectorFactory {
  const fn = () => {
    throw new Error(
      `stub director factory ${id} should not be invoked in loader tests`,
    );
  };
  const configSchema = (value: unknown) => value;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- LoadedDirectorFactory carries an erased Config (`unknown`); the structural fields the loader's predicate checks (id, requires, configSchema) are satisfied below, and the loader does not invoke the factory body in these tests.
  return Object.assign(fn, {
    id,
    requires,
    configSchema,
  }) as unknown as LoadedDirectorFactory;
}

describe("createToolLoader", () => {
  test("loads a single top-level package via injected fetcher and importer", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-leaf",
      version: "1.0.0",
      entryModuleSource: "// stub; importer is faked",
    });

    const fakeFactory = makeFakeFactory("@vendor/leaf/main", ["env.thing"]);
    const fetchTarball: TarballFetcher = async (entry) => {
      expect(entry.name).toBe("tools-leaf");
      return fixture.bytes;
    };
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
      importModule: async () => ({ main: fakeFactory }),
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "tools-leaf", version: "1.0.0" }],
      entries: [
        {
          name: "tools-leaf",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.name).toBe("tools-leaf");
    expect(loaded[0]?.factories).toHaveLength(1);
    expect(loaded[0]?.factories[0]?.id).toBe("@vendor/leaf/main");
  });

  test("rejects a non-positive registryFetchTimeoutMs", () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    expect(() =>
      createToolLoader({
        cache,
        registries: new Map([["npmjs", { url: "https://r.test" }]]),
        host: { os: "linux", cpu: "x64" },
        registryFetchTimeoutMs: 0,
      }),
    ).toThrow(/registryFetchTimeoutMs must be a positive finite number/);
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
      importModule: async () => ({
        main: makeFakeFactory("@watcher/linux/main"),
      }),
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
          integrity: matching.integrity,
          source: { kind: "registry", registry: "npmjs" },
          os: ["linux"],
        },
        {
          name: "watcher-darwin",
          version: "1.0.0",
          integrity: nonMatching.integrity,
          source: { kind: "registry", registry: "npmjs" },
          os: ["darwin"],
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.name).toBe("watcher-linux");
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async (entry) => {
        const f = byName.get(entry.name);
        if (f === undefined) throw new Error(`no fixture: ${entry.name}`);
        return f.bytes;
      },
      importModule: async () => ({ main: makeFakeFactory("@vendor/x/main") }),
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
          integrity: byName.get("allow-bare")?.integrity ?? "",
          source: { kind: "registry", registry: "npmjs" },
          os: ["linux"],
        },
        {
          name: "deny-bare",
          version: "1.0.0",
          integrity: byName.get("deny-bare")?.integrity ?? "",
          source: { kind: "registry", registry: "npmjs" },
          os: ["win32"],
        },
        {
          name: "allow-negate",
          version: "1.0.0",
          integrity: byName.get("allow-negate")?.integrity ?? "",
          source: { kind: "registry", registry: "npmjs" },
          os: ["!win32"],
        },
        {
          name: "deny-negate",
          version: "1.0.0",
          integrity: byName.get("deny-negate")?.integrity ?? "",
          source: { kind: "registry", registry: "npmjs" },
          os: ["!linux"],
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    const names = loaded.map((p) => p.name).sort();
    expect(names).toEqual(["allow-bare", "allow-negate"]);
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball,
      importModule: async () => ({ main: makeFakeFactory("@c/main") }),
    });
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "cached", version: "1.0.0" }],
      entries: [
        {
          name: "cached",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };
    await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    expect(fetcherCalls).toBe(0);
  });

  test("cache-busts the import URL with the entry integrity so reapplies with new bytes import distinct modules", async () => {
    // Same (name, version), different bytes — Node's ESM cache keys
    // by resolved URL, so without the integrity query string both
    // applies would share the first apply's module. Track the URLs
    // the loader hands to import(): they must (a) carry the
    // `integrity=<sri>` query string and (b) differ between the two
    // applies whose integrities differ.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixtureA = await packFixture({
      name: "swappable",
      version: "1.0.0",
      entryModuleSource: "// bytes A",
    });
    const fixtureB = await packFixture({
      name: "swappable",
      version: "1.0.0",
      entryModuleSource: "// bytes B (distinct from A)",
    });
    expect(fixtureA.integrity).not.toBe(fixtureB.integrity);

    const importUrls: string[] = [];
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async (entry) =>
        entry.integrity === fixtureA.integrity
          ? fixtureA.bytes
          : fixtureB.bytes,
      importModule: async (importUrl) => {
        importUrls.push(importUrl);
        return { main: makeFakeFactory("@swappable/main") };
      },
    });

    const makeManifest = (integrity: string): ToolPackageManifest => ({
      schemaVersion: "1",
      topLevel: [{ name: "swappable", version: "1.0.0" }],
      entries: [
        {
          name: "swappable",
          version: "1.0.0",
          integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    });

    const instanceDirA = path.join(scratchRoot, "instance-a");
    const instanceDirB = path.join(scratchRoot, "instance-b");
    await fs.mkdir(instanceDirA, { recursive: true });
    await fs.mkdir(instanceDirB, { recursive: true });

    await loader.loadManifest({
      manifest: makeManifest(fixtureA.integrity),
      instanceScratchDir: instanceDirA,
      assetRoot,
      assetMounts: new Map(),
    });
    await loader.loadManifest({
      manifest: makeManifest(fixtureB.integrity),
      instanceScratchDir: instanceDirB,
      assetRoot,
      assetMounts: new Map(),
    });

    expect(importUrls).toHaveLength(2);
    const [urlA, urlB] = importUrls;
    if (urlA === undefined || urlB === undefined) {
      throw new Error("expected two import URLs");
    }
    expect(urlA.startsWith("file://")).toBeTruthy();
    expect(urlB.startsWith("file://")).toBeTruthy();
    expect(urlA).toContain(
      `integrity=${encodeURIComponent(fixtureA.integrity)}`,
    );
    expect(urlB).toContain(
      `integrity=${encodeURIComponent(fixtureB.integrity)}`,
    );
    expect(urlA).not.toBe(urlB);
  });

  test("loader prefixes tool definitions with the bundle id", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-prefix",
      version: "1.0.0",
      entryModuleSource: "",
    });
    // The factory exports a definition named "search". The loader
    // should expose it as "@vendor/prefix/main:search" to the model.
    const factory = Object.assign(
      () => ({
        definitions: [{ name: "search", description: "test", inputSchema: {} }],
        run: async (call: { id: string; name: string }) => ({
          callId: call.id,
          content: `ran ${call.name}`,
        }),
      }),
      { id: "@vendor/prefix/main", requires: [] as readonly string[] },
    );
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => ({ main: factory }),
    });
    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "tools-prefix", version: "1.0.0" }],
      entries: [
        {
          name: "tools-prefix",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };
    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    const loadedFactory = loaded[0]?.factories[0];
    if (loadedFactory === undefined) {
      throw new Error("expected at least one factory");
    }
    // Stub env; the factory does not read anything from it in this
    // test, but BaseEnv must satisfy the type at the call site.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- BaseEnv satisfied by the {} cast for this stub; the factory does not read env at runtime.
    const bundle = loadedFactory({} as Parameters<typeof loadedFactory>[0]);
    expect(bundle.definitions[0]?.name).toBe("@vendor/prefix/main:search");
    // Invoking via the prefixed name should round-trip to the
    // original handler.
    const result = await bundle.run(
      {
        id: "c1",
        name: "@vendor/prefix/main:search",
        arguments: {},
      },
      new AbortController().signal,
    );
    expect(result.content).toBe("ran search");
  });

  test("loader rejects bundles that export duplicate-named tool definitions", async () => {
    // Two definitions with the same name in the same bundle would
    // silently collapse to one entry in the wrapper's name map and
    // leave one tool unreachable. The wrapper surfaces the collision
    // as tool.name.duplicate when the factory is invoked.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-dup",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const factory = Object.assign(
      () => ({
        definitions: [
          { name: "search", description: "first", inputSchema: {} },
          { name: "search", description: "second", inputSchema: {} },
        ],
        run: async (call: { id: string; name: string }) => ({
          callId: call.id,
          content: `ran ${call.name}`,
        }),
      }),
      { id: "@vendor/dup/main", requires: [] as readonly string[] },
    );
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => ({ main: factory }),
    });
    const loaded = await loader.loadManifest({
      manifest: {
        schemaVersion: "1",
        topLevel: [{ name: "tools-dup", version: "1.0.0" }],
        entries: [
          {
            name: "tools-dup",
            version: "1.0.0",
            integrity: fixture.integrity,
            source: { kind: "registry", registry: "npmjs" },
          },
        ],
      },
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    const loadedFactory = loaded[0]?.factories[0];
    if (loadedFactory === undefined) {
      throw new Error("expected at least one factory");
    }
    let caught: unknown;
    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- BaseEnv satisfied by the {} cast for this stub; the factory does not read env at runtime.
      loadedFactory({} as Parameters<typeof loadedFactory>[0]);
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
    }
    expect(caught.category).toBe("tool.name.duplicate");
    expect(caught.message).toMatch(/two tool definitions named/);
  });

  test("loader rejects bundles whose `definitions` field is not an array", async () => {
    // A factory that returns `definitions: null` (or omits the field
    // entirely) would otherwise yield a bare TypeError when the
    // wrapper iterates `bundle.definitions`. The apply pipeline would
    // route that through the unknown-shape catch-all
    // (`factory.construct.failed`), which is wrong: the shape of the
    // returned bundle is the violated invariant, so the failure
    // belongs in `package.entry.invalid`. The wrapper shape-checks
    // before iterating to keep the operator-facing taxonomy honest.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-malformed",
      version: "1.0.0",
      entryModuleSource: "",
    });
    const factory = Object.assign(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- the test exercises the wrapper's defensive shape-check, which catches malformed bundles whose runtime shape disagrees with the type. Suppressing the type system on the violating field is the point.
      (): any => ({
        // Deliberately violate the bundle shape — `definitions` must
        // be an array per the `AnnotatedToolFactory` contract.
        definitions: null,
        run: async (call: { id: string; name: string }) => ({
          callId: call.id,
          content: `ran ${call.name}`,
        }),
      }),
      { id: "@vendor/malformed/main", requires: [] as readonly string[] },
    );
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => ({ main: factory }),
    });
    const loaded = await loader.loadManifest({
      manifest: {
        schemaVersion: "1",
        topLevel: [{ name: "tools-malformed", version: "1.0.0" }],
        entries: [
          {
            name: "tools-malformed",
            version: "1.0.0",
            integrity: fixture.integrity,
            source: { kind: "registry", registry: "npmjs" },
          },
        ],
      },
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    const loadedFactory = loaded[0]?.factories[0];
    if (loadedFactory === undefined) {
      throw new Error("expected at least one factory");
    }
    let caught: unknown;
    try {
      // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- BaseEnv satisfied by the {} cast for this stub; the factory does not read env at runtime.
      loadedFactory({} as Parameters<typeof loadedFactory>[0]);
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
    }
    expect(caught.category).toBe("package.entry.invalid");
    expect(caught.message).toMatch(/non-array `definitions`/);
  });
});

describe("loader error categories", () => {
  async function expectCategory(
    args: {
      manifest: ToolPackageManifest;
      fetchTarball: TarballFetcher;
      importModule?: (p: string) => Promise<unknown>;
    },
    category: DeployApplyErrorCategory,
  ): Promise<void> {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: args.fetchTarball,
      ...(args.importModule !== undefined
        ? { importModule: args.importModule }
        : {}),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: args.manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(
        `expected ToolLoaderError, got ${caught === undefined ? "no error" : String(caught)}`,
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
    const wrongBytes = Buffer.from("not the right bytes");
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "i", version: "1.0.0" }],
          entries: [
            {
              name: "i",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        fetchTarball: async () => wrongBytes,
      },
      "integrity.mismatch",
    );
  });

  test("tarball.extract.failed on garbage bytes leaves the cache entry intact", async () => {
    // We can't easily produce bytes that match an integrity but are
    // not a valid tarball; instead, build a real cache entry whose
    // bytes we control. Stuff the cache directly via a put with
    // matched integrity, then point the manifest at it. Extraction
    // fails inside `tar.extract` (not at the integrity re-check), so
    // the loader must NOT evict — a sibling agent in the same process
    // may be hardlinking from the same extraction tree at this moment.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const garbage = Buffer.from("definitely not a tarball");
    const integrity = ssri
      .fromData(garbage, { algorithms: ["sha512"] })
      .toString();
    await cache.put(integrity, garbage);
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => garbage,
      importModule: async () => ({}),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "g", version: "1.0.0" }],
          entries: [
            {
              name: "g",
              version: "1.0.0",
              integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
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
    const loader = createToolLoader({
      cache: wrappedCache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => ({}),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "t", version: "1.0.0" }],
          entries: [
            {
              name: "t",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
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
    const loader = createToolLoader({
      cache: wrappedCache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => ({}),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
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
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "ghost" },
            },
          ],
        },
        // Default fetcher used; no override; the loader resolves
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
    // the loader's JSON.parse throws. The wrapper translates the
    // SyntaxError into the package.entry.invalid category rather than
    // letting it bubble out as the catch-all factory.construct.failed.
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(bytes),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "bad-json", version: "1.0.0" }],
          entries: [
            {
              name: "bad-json",
              version: "1.0.0",
              integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(bytes),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "deps-array", version: "1.0.0" }],
          entries: [
            {
              name: "deps-array",
              version: "1.0.0",
              integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(bytes),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "opt-deps-array", version: "1.0.0" }],
          entries: [
            {
              name: "opt-deps-array",
              version: "1.0.0",
              integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              integrity: "sha512-AAAA",
              source: {
                kind: "asset",
                assetId: "asset_one",
                path: "does/not/exist.tgz",
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["asset_one", "package-registries/one"]]),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
    }
    expect(caught.category).toBe("tarball.missing");
  });

  test("package.entry.invalid when an asset entry's path escapes the asset root", async () => {
    // A `..`-bearing entry.source.path would let a malicious manifest
    // read any file the sidecar process can open. The default fetcher
    // must reject this at the boundary rather than handing the
    // escaped absolute path to fs.readFile.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              integrity: "sha512-AAAA",
              source: {
                kind: "asset",
                assetId: "asset_one",
                path: "../../../../etc/passwd",
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["asset_one", "package-registries/one"]]),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
    }
    expect(caught.category).toBe("package.entry.invalid");
    expect(caught.message).toMatch(/escapes the declared mount/);
  });

  test("asset.mount.missing when the entry's assetId is absent from assetMounts", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const loader = createToolLoader({
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
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "m", version: "1.0.0" }],
          entries: [
            {
              name: "m",
              version: "1.0.0",
              integrity: "sha512-AAAA",
              source: {
                kind: "asset",
                assetId: "asset_missing",
                path: "tarballs/m-1.0.0.tgz",
              },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["asset_other", "package-registries/other"]]),
      });
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ToolLoaderError)) {
      throw new Error(`expected ToolLoaderError, got ${String(caught)}`);
    }
    expect(caught.category).toBe("asset.mount.missing");
  });

  test("package.entry.missing when package.json has no interchange.tools field", async () => {
    const fixture = await packFixture({
      name: "pem",
      version: "1.0.0",
      interchangeToolsRelPath: null,
    });
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "pem", version: "1.0.0" }],
          entries: [
            {
              name: "pem",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        fetchTarball: async () => fixture.bytes,
      },
      "package.entry.missing",
    );
  });

  test("package.entry.invalid when import throws", async () => {
    const fixture = await packFixture({
      name: "pi",
      version: "1.0.0",
      entryModuleSource: "",
    });
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "pi", version: "1.0.0" }],
          entries: [
            {
              name: "pi",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        fetchTarball: async () => fixture.bytes,
        importModule: async () => {
          throw new Error("boom");
        },
      },
      "package.entry.invalid",
    );
  });

  test("package.entry.invalid when entry exports nothing factory-shaped", async () => {
    const fixture = await packFixture({
      name: "ps",
      version: "1.0.0",
      entryModuleSource: "",
    });
    await expectCategory(
      {
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "ps", version: "1.0.0" }],
          entries: [
            {
              name: "ps",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        fetchTarball: async () => fixture.bytes,
        importModule: async () => ({ random: 42, alsoRandom: "hello" }),
      },
      "package.entry.invalid",
    );
  });
});

describe("transitive resolution contract", () => {
  // Locks the closure-satisfies-itself contract: a top-level package
  // whose body imports a transitive that lives in the manifest closure
  // resolves through the per-instance node_modules layout the loader
  // builds, without help from the sidecar host's own node_modules.
  test("a top-level package importing a closure transitive resolves through the loader's node_modules layout", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });

    // The transitive dep ships a JS module the top-level can import by
    // its bare specifier. The dep is a fully-formed npm package: it
    // has a `main` so Node knows where to load from. The loader is
    // expected to materialize it under
    // `<scratch>/store/@closure-scope/transitive-dep/1.0.0/` and to
    // place a symlink at `.../top/1.0.0/node_modules/@closure-scope/transitive-dep`
    // pointing at it.
    const depEntry = `export const greeting = "hello from transitive";`;
    const depPkg = await packFixture({
      name: "@closure-scope/transitive-dep",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      main: "./index.js",
      type: "module",
      extraFiles: {
        "index.js": depEntry,
      },
    });

    // The top-level imports the dep by name. The loaded module both
    // re-exports a tool factory (so the loader accepts it as a valid
    // entry) and proves the import succeeded by carrying the dep's
    // greeting in a marker the test can read back.
    const topEntry = `
import { greeting } from "@closure-scope/transitive-dep";
export const factory = Object.assign(
  () => ({
    definitions: [{ name: "echo", description: "", inputSchema: {} }],
    run: async (call) => ({ callId: call.id, content: greeting }),
  }),
  { id: "closure/top", requires: [] },
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

    const loader = createToolLoader({
      cache,
      registries: new Map([["test", { url: "https://example.test" }]]),
      host: { os: process.platform, cpu: process.arch },
      fetchTarball: async (entry) => {
        if (entry.name === "@closure-scope/top") return topPkg.bytes;
        if (entry.name === "@closure-scope/transitive-dep") return depPkg.bytes;
        throw new Error(`unexpected fetch: ${entry.name}`);
      },
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "@closure-scope/top", version: "1.0.0" }],
      entries: [
        {
          name: "@closure-scope/top",
          version: "1.0.0",
          integrity: topPkg.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "@closure-scope/transitive-dep",
          version: "1.0.0",
          integrity: depPkg.integrity,
          source: { kind: "registry", registry: "test" },
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    expect(loaded).toHaveLength(1);
    const factory = loaded[0]?.factories[0];
    if (factory === undefined) throw new Error("expected one factory");
    // The factory's body returns the transitive's greeting, proving
    // the bare-specifier import composed end-to-end.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- BaseEnv is satisfied by an empty object for this stub; the factory does not read env at runtime.
    const bundle = factory({} as Parameters<typeof factory>[0]);
    const result = await bundle.run(
      { id: "c1", name: "closure/top:echo", arguments: {} },
      new AbortController().signal,
    );
    expect(result.content).toBe("hello from transitive");
  });

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
  { id: "@scope/only", requires: [] },
);
`,
    });
    await cache.put(scopedPkg.integrity, scopedPkg.bytes);

    const loader = createToolLoader({
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
          integrity: scopedPkg.integrity,
          source: { kind: "registry", registry: "test" },
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.name).toBe("@scope/only");

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
export const factory = Object.assign(
  () => ({
    definitions: [{ name: "echo", description: "", inputSchema: {} }],
    run: async (call) => ({
      callId: call.id,
      content: JSON.stringify({ top: topSawLodash, libA: seenLodash }),
    }),
  }),
  { id: "vt/main", requires: [] },
);
`,
      dependencies: { lodash: "^3.0.0", "lib-a": "^1.0.0" },
    });
    await cache.put(lodash3.integrity, lodash3.bytes);
    await cache.put(lodash4.integrity, lodash4.bytes);
    await cache.put(libA.integrity, libA.bytes);
    await cache.put(topPkg.integrity, topPkg.bytes);

    const loader = createToolLoader({
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
          integrity: topPkg.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "lib-a",
          version: "1.0.0",
          integrity: libA.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "lodash",
          version: "3.10.1",
          integrity: lodash3.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "lodash",
          version: "4.17.21",
          integrity: lodash4.integrity,
          source: { kind: "registry", registry: "test" },
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    const factory = loaded[0]?.factories[0];
    if (factory === undefined) throw new Error("expected one factory");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- BaseEnv stub
    const bundle = factory({} as Parameters<typeof factory>[0]);
    const result = await bundle.run(
      { id: "c1", name: "vt/main:echo", arguments: {} },
      new AbortController().signal,
    );
    // The top saw lodash major 3 (the version satisfying its own
    // range); lib-a saw lodash major 4 (the version satisfying its
    // own range). Both must be true simultaneously, which is only
    // possible with nested node_modules entries.
    if (typeof result.content !== "string") {
      throw new Error("expected stringified content");
    }
    const parsed: unknown = JSON.parse(result.content);
    expect(parsed).toEqual({ top: 3, libA: 4 });

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
    // closure version satisfies. The loader's first-arrival BFS from
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
    // them in declaration order so the loader's BFS visits them in
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
  { id: "fa/top", requires: [] },
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

    const loader = createToolLoader({
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
          integrity: top.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "req-a",
          version: "1.0.0",
          integrity: rA.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "req-b",
          version: "1.0.0",
          integrity: rB.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "req-c",
          version: "1.0.0",
          integrity: rC.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "dep",
          version: "1.0.0",
          integrity: dep1.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "dep",
          version: "1.5.0",
          integrity: dep1_5.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "dep",
          version: "2.0.0",
          integrity: dep2.integrity,
          source: { kind: "registry", registry: "test" },
        },
      ],
    };

    await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });

    // Every requirer's node_modules/dep should resolve to the same
    // store slot — the first-arrival pick of 1.5.0 for ranges that
    // all satisfy it. If the loader re-ran maxSatisfying per requirer
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
  test("rejects an interchange.tools entry that escapes the package dir", async () => {
    // Stage a tarball whose package.json points interchange.tools at
    // a file outside the extracted package directory. The loader's
    // path-resolve step must reject the escape rather than letting
    // the dynamic import target arbitrary sidecar-readable files.
    const sinkPath = path.join(scratchRoot, "loaded-from-outside.mjs");
    await fs.writeFile(
      sinkPath,
      `export const mod = Object.assign(() => ({
        definitions: [],
        run: async () => ({ callId: "x", content: "ok" }),
      }), { id: "external/sink", requires: [] });`,
    );

    const stagingDir = path.join(scratchRoot, "evil-staging");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    const layoutDir = path.join(instanceDir, "store", "evil", "1.0.0");
    const escapeRel = path.relative(layoutDir, sinkPath);
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({
        name: "evil",
        version: "1.0.0",
        interchange: { tools: escapeRel },
      }),
    );
    const tarballPath = path.join(stagingDir, "evil.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const finalBytes = await fs.readFile(tarballPath);
    const integrity = ssri
      .fromData(finalBytes, { algorithms: ["sha512"] })
      .toString();

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(finalBytes),
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil", version: "1.0.0" }],
      entries: [
        {
          name: "evil",
          version: "1.0.0",
          integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/escapes the package directory/);
    }
  });

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
    // escape-pointing symlink inside it before the loader's
    // layout pass picks it up.
    const extractedHandle = await cache.extractTarball(fixture.integrity);
    const extractedRoot = extractedHandle.dir;
    await fs.symlink(
      path.relative(extractedRoot, sinkPath),
      path.join(extractedRoot, "evil-link"),
    );
    extractedHandle.release();

    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(fixture.bytes),
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-symlink", version: "1.0.0" }],
      entries: [
        {
          name: "evil-symlink",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /escapes the package extraction directory/,
      );
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
    // hardlinkTree's walk hits it before recursing into the inner
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

    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(fixture.bytes),
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-chain", version: "1.0.0" }],
      entries: [
        {
          name: "evil-chain",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    // dangling-but-contained symlink and verify the load succeeds.
    const fixture = await packFixture({
      name: "ok-dangling",
      version: "1.0.0",
      entryModuleSource: `
export const main = Object.assign(
  () => ({ definitions: [], run: async () => ({ callId: "x", content: "" }) }),
  { id: "ok-dangling/main", requires: [] },
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
    // disk. realpath ENOENTs; the loader should fall back to the
    // literal-path containment check and accept.
    await fs.symlink(
      "missing-sibling.txt",
      path.join(extractedRoot, "dangling-link"),
    );
    extractedHandle.release();

    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(fixture.bytes),
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "ok-dangling", version: "1.0.0" }],
      entries: [
        {
          name: "ok-dangling",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };

    const loaded = await loader.loadManifest({
      manifest,
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });
    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.name).toBe("ok-dangling");
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

    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(fixture.bytes),
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-dangling", version: "1.0.0" }],
      entries: [
        {
          name: "evil-dangling",
          version: "1.0.0",
          integrity: fixture.integrity,
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    };

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /escapes the package extraction directory/,
      );
    }
  });

  test("rejects an asset source.path that traverses out of its declared mount", async () => {
    // Two sibling mounts live under the same assetRoot. The manifest
    // entry names mountB but its source.path walks `..` up to mountA's
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
    const loader = createToolLoader({
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
          integrity: fixture.integrity,
          source: {
            kind: "asset",
            assetId: "ast_b",
            path: "../a/tarballs/pkg-1.0.0.tgz",
          },
        },
      ],
    };

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map([["ast_b", mountBRel]]),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/cross-mount traversal/);
    }
  });
});

describe("interchange.tools containment under symlink graph", () => {
  test("rejects an interchange.tools entry that traverses a node_modules symlink to another package", async () => {
    // The per-instance scratch tree contains a `node_modules/` symlink
    // graph the loader builds for nested resolution. An
    // interchange.tools entry that string-contains inside the top
    // package's directory but realpath-resolves into a transitive
    // dependency's bytes (or anywhere else the symlink target points)
    // would bypass the string-level containment check entirely.
    //
    // Build a real two-package closure (top + transitive dep), then
    // point the top's interchange.tools at a path that walks through
    // its own `node_modules/<dep>/...` symlink. The string check
    // accepts the path because it starts with the package directory;
    // the realpath check must reject it because the resolved target
    // lives under the dep's store entry.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });

    const depEntry = `export const x = 1;`;
    const depPkg = await packFixture({
      name: "evil-dep",
      version: "1.0.0",
      interchangeToolsRelPath: null,
      main: "./index.js",
      type: "module",
      extraFiles: { "index.js": depEntry },
    });

    const topPkg = await packFixture({
      name: "evil-top",
      version: "1.0.0",
      // The entry path strings-inside the top directory but
      // realpath-walks through `node_modules/evil-dep` into the dep's
      // store entry. Leave entryModuleSource undefined so the tarball
      // ships no `node_modules/` of its own; the loader's layout pass
      // will then build `node_modules/evil-dep` as a symlink to the
      // dep's store entry, and the realpath check should fire when
      // the entry path is resolved.
      interchangeToolsRelPath: "./node_modules/evil-dep/index.js",
      type: "module",
      dependencies: { "evil-dep": "^1.0.0" },
    });

    await cache.put(topPkg.integrity, topPkg.bytes);
    await cache.put(depPkg.integrity, depPkg.bytes);

    const loader = createToolLoader({
      cache,
      registries: new Map([["test", { url: "https://example.test" }]]),
      host: { os: process.platform, cpu: process.arch },
      fetchTarball: async (entry) => {
        if (entry.name === "evil-top") return topPkg.bytes;
        if (entry.name === "evil-dep") return depPkg.bytes;
        throw new Error(`unexpected fetch: ${entry.name}`);
      },
    });

    const manifest: ToolPackageManifest = {
      schemaVersion: "1",
      topLevel: [{ name: "evil-top", version: "1.0.0" }],
      entries: [
        {
          name: "evil-top",
          version: "1.0.0",
          integrity: topPkg.integrity,
          source: { kind: "registry", registry: "test" },
        },
        {
          name: "evil-dep",
          version: "1.0.0",
          integrity: depPkg.integrity,
          source: { kind: "registry", registry: "test" },
        },
      ],
    };

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest,
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/symlink/);
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
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
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
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("registry.fetch.failed");
      expect(caught.message).toMatch(/exceeded the registry fetch timeout/);
    }
  });
});

describe("interchange.directors walker", () => {
  test("loads director factories from an interchange.directors entry alongside tool factories", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-and-directors",
      version: "1.0.0",
      entryModuleSource: "// stub; importer is faked",
      interchangeDirectorsRelPath: "./directors.js",
      directorsModuleSource: "// stub; importer is faked",
    });

    const toolsModule = { main: makeFakeFactory("@vendor/main") };
    const directorsModule = {
      planner: makeFakeDirectorFactory("@vendor/main/planner"),
    };
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async (importUrl) => {
        // Match on the entry basename rather than the substring
        // "directors" because the per-instance scratch tree's mkdtemp
        // suffix can happen to contain that substring.
        if (importUrl.includes("directors.js")) return directorsModule;
        return toolsModule;
      },
    });

    const loaded = await loader.loadManifest({
      manifest: {
        schemaVersion: "1",
        topLevel: [{ name: "tools-and-directors", version: "1.0.0" }],
        entries: [
          {
            name: "tools-and-directors",
            version: "1.0.0",
            integrity: fixture.integrity,
            source: { kind: "registry", registry: "npmjs" },
          },
        ],
      },
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });

    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.factories).toHaveLength(1);
    expect(loaded[0]?.factories[0]?.id).toBe("@vendor/main");
    expect(loaded[0]?.directors).toHaveLength(1);
    expect(loaded[0]?.directors[0]?.id).toBe("@vendor/main/planner");
  });

  test("ignores a director-shaped export in the interchange.tools entry", async () => {
    // The tools walker's predicate must reject director-shaped values so
    // a director placed in `interchange.tools` is not silently
    // classified as a tool and namespace-prefixed. Mirrors the
    // discriminator the directors walker uses against tool shapes.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-with-stray-director",
      version: "1.0.0",
      entryModuleSource: "// stub; importer is faked",
    });

    const toolsModule = {
      tool: makeFakeFactory("@vendor/tool"),
      strayDirector: makeFakeDirectorFactory("@vendor/director"),
    };
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => toolsModule,
    });

    const loaded = await loader.loadManifest({
      manifest: {
        schemaVersion: "1",
        topLevel: [{ name: "tools-with-stray-director", version: "1.0.0" }],
        entries: [
          {
            name: "tools-with-stray-director",
            version: "1.0.0",
            integrity: fixture.integrity,
            source: { kind: "registry", registry: "npmjs" },
          },
        ],
      },
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });

    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.factories).toHaveLength(1);
    expect(loaded[0]?.factories[0]?.id).toBe("@vendor/tool");
    expect(loaded[0]?.directors).toEqual([]);
  });

  test("treats a missing interchange.directors field as a no-op", async () => {
    // Locks the absence-is-fine contract: a tools-only package must
    // still load. The directors walker contributes an empty list.
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "tools-only",
      version: "1.0.0",
      entryModuleSource: "// stub",
    });
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async () => ({
        main: makeFakeFactory("@vendor/tools-only"),
      }),
    });

    const loaded = await loader.loadManifest({
      manifest: {
        schemaVersion: "1",
        topLevel: [{ name: "tools-only", version: "1.0.0" }],
        entries: [
          {
            name: "tools-only",
            version: "1.0.0",
            integrity: fixture.integrity,
            source: { kind: "registry", registry: "npmjs" },
          },
        ],
      },
      instanceScratchDir: instanceDir,
      assetRoot,
      assetMounts: new Map(),
    });

    expect(loaded).toHaveLength(1);
    expect(loaded[0]?.directors).toEqual([]);
  });

  test("package.entry.invalid when the package.json bytes are malformed JSON", async () => {
    // Cross-cuts the directors walker: the package-json read happens
    // once for both walkers, so a malformed package.json blocks both.
    // Pinned here to document the shared failure surface.
    const stagingDir = path.join(scratchRoot, "bad-json-dir-staging");
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
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(bytes),
    });
    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "bad-json-dir", version: "1.0.0" }],
          entries: [
            {
              name: "bad-json-dir",
              version: "1.0.0",
              integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/malformed package\.json/);
    }
  });

  test("rejects an interchange.directors entry path that escapes the package dir", async () => {
    const sinkPath = path.join(scratchRoot, "loaded-directors-outside.mjs");
    await fs.writeFile(sinkPath, "export const x = 1;");

    const stagingDir = path.join(scratchRoot, "evil-dir-staging");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    const layoutDir = path.join(instanceDir, "store", "evil-dir", "1.0.0");
    const escapeRel = path.relative(layoutDir, sinkPath);
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({
        name: "evil-dir",
        version: "1.0.0",
        interchange: {
          tools: "./tools.js",
          directors: escapeRel,
        },
      }),
    );
    await fs.writeFile(path.join(pkgDir, "tools.js"), "");
    const tarballPath = path.join(stagingDir, "evil-dir.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const finalBytes = await fs.readFile(tarballPath);
    const integrity = ssri
      .fromData(finalBytes, { algorithms: ["sha512"] })
      .toString();

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(finalBytes),
      importModule: async () => ({
        main: makeFakeFactory("@vendor/tools-stub"),
      }),
    });

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "evil-dir", version: "1.0.0" }],
          entries: [
            {
              name: "evil-dir",
              version: "1.0.0",
              integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(/interchange\.directors/);
      expect(caught.message).toMatch(/escapes the package directory/);
    }
  });

  test("rejects an interchange.directors target reached via a symlink that escapes the extraction root", async () => {
    const sinkPath = path.join(scratchRoot, "directors-symlink-sink.mjs");
    await fs.writeFile(sinkPath, "export const x = 1;");

    const fixture = await packFixture({
      name: "evil-dir-symlink",
      version: "1.0.0",
      entryModuleSource: `
export const main = Object.assign(
  () => ({ definitions: [], run: async () => ({ callId: "x", content: "" }) }),
  { id: "@vendor/evil-dir-symlink", requires: [] },
);
`,
      interchangeDirectorsRelPath: "./directors-link.js",
    });

    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    await cache.put(fixture.integrity, fixture.bytes);
    const extractedHandle = await cache.extractTarball(fixture.integrity);
    const extractedRoot = extractedHandle.dir;
    // Plant a symlink at the directors-entry name whose realpath
    // resolves outside the package extraction directory.
    await fs.symlink(
      path.relative(extractedRoot, sinkPath),
      path.join(extractedRoot, "directors-link.js"),
    );
    extractedHandle.release();

    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => Buffer.from(fixture.bytes),
      importModule: async () => ({
        main: makeFakeFactory("@vendor/evil-dir-symlink"),
      }),
    });

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "evil-dir-symlink", version: "1.0.0" }],
          entries: [
            {
              name: "evil-dir-symlink",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    // The symlink fires `hardlinkTree`'s containment guard at layout
    // time, before either walker runs. The directors walker's own
    // realpath containment check is the second line of defense and
    // shares the same operator-facing category; the test asserts the
    // taxonomy mapping, not which layer produced the message.
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /escapes the package extraction directory/,
      );
    }
  });

  test("package.entry.invalid when the directors module import throws", async () => {
    const cache = createTarballCache({
      rootDir: cacheDir,
      maxBytes: 10_000_000,
    });
    const fixture = await packFixture({
      name: "dir-import-fail",
      version: "1.0.0",
      entryModuleSource: "// stub",
      interchangeDirectorsRelPath: "./directors.js",
      directorsModuleSource: "// stub",
    });
    const loader = createToolLoader({
      cache,
      registries: new Map([["npmjs", { url: "https://r.test" }]]),
      host: { os: "linux", cpu: "x64" },
      fetchTarball: async () => fixture.bytes,
      importModule: async (importUrl) => {
        if (importUrl.includes("directors.js")) {
          throw new Error("director module boom");
        }
        return { main: makeFakeFactory("@vendor/dir-import-fail") };
      },
    });

    let caught: unknown;
    try {
      await loader.loadManifest({
        manifest: {
          schemaVersion: "1",
          topLevel: [{ name: "dir-import-fail", version: "1.0.0" }],
          entries: [
            {
              name: "dir-import-fail",
              version: "1.0.0",
              integrity: fixture.integrity,
              source: { kind: "registry", registry: "npmjs" },
            },
          ],
        },
        instanceScratchDir: instanceDir,
        assetRoot,
        assetMounts: new Map(),
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ToolLoaderError);
    if (caught instanceof ToolLoaderError) {
      expect(caught.category).toBe("package.entry.invalid");
      expect(caught.message).toMatch(
        /dynamic import of dir-import-fail@1\.0\.0 interchange\.directors failed/,
      );
    }
  });
});
