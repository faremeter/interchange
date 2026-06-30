import { describe, test, expect } from "bun:test";
import ssri from "ssri";
import * as tar from "tar";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import {
  type Packument,
  type PackumentFetcher,
  type RegistryConfig,
  type RegistrySource,
  AssetRegistrySource,
  HttpRegistrySource,
  ManifestInvalidError,
  createClosureResolver,
  parsePin,
} from "./index";

function packument(
  name: string,
  versions: Record<
    string,
    {
      dependencies?: Record<string, string>;
      optionalDependencies?: Record<string, string>;
      peerDependencies?: Record<string, string>;
      os?: string[];
      cpu?: string[];
      integrity?: string;
    }
  >,
): Packument {
  return {
    name,
    versions: Object.fromEntries(
      Object.entries(versions).map(([v, body]) => [
        v,
        {
          name,
          version: v,
          dist: {
            tarball: `https://example.test/${name}/-/${name}-${v}.tgz`,
            integrity: body.integrity ?? `sha512-${name}-${v}`,
          },
          ...(body.dependencies !== undefined
            ? { dependencies: body.dependencies }
            : {}),
          ...(body.optionalDependencies !== undefined
            ? { optionalDependencies: body.optionalDependencies }
            : {}),
          ...(body.peerDependencies !== undefined
            ? { peerDependencies: body.peerDependencies }
            : {}),
          ...(body.os !== undefined ? { os: body.os } : {}),
          ...(body.cpu !== undefined ? { cpu: body.cpu } : {}),
        },
      ]),
    ),
  };
}

function httpSource(
  named: { name: string } & RegistryConfig,
  packuments: Record<string, Packument>,
): HttpRegistrySource {
  const fetchPackument: PackumentFetcher = async (pkgName) => {
    const p = packuments[pkgName];
    if (p === undefined) {
      throw new Error(`mock fetcher: no packument for ${pkgName}`);
    }
    return p;
  };
  const { name, ...config } = named;
  return new HttpRegistrySource({ name, config, fetchPackument });
}

function registriesOf(
  ...sources: readonly RegistrySource[]
): Map<string, RegistrySource> {
  const m = new Map<string, RegistrySource>();
  for (const src of sources) {
    m.set(src.name, src);
  }
  return m;
}

describe("createResolver construction", () => {
  test("throws if registries map is empty", () => {
    expect(() =>
      createClosureResolver({
        registries: new Map(),
        defaultRegistry: "any",
      }),
    ).toThrow(/registries cannot be empty/);
  });

  test("throws if defaultRegistry is not present in the map", () => {
    expect(() =>
      createClosureResolver({
        registries: registriesOf(
          httpSource({ name: "a", url: "https://a.test" }, {}),
        ),
        defaultRegistry: "missing",
      }),
    ).toThrow(/defaultRegistry "missing" not present/);
  });

  test("throws if a scopeRouting entry omits the leading @", () => {
    // A bare scope like "intx" never matches any package name's scope
    // (parseScope only returns strings that start with "@"), so the
    // route would silently never fire and the package would fall
    // through to the default registry. createClosureResolver must
    // reject this at config-load time rather than swallow the typo.
    expect(() =>
      createClosureResolver({
        registries: registriesOf(
          httpSource({ name: "npmjs", url: "https://npmjs.test" }, {}),
        ),
        defaultRegistry: "npmjs",
        scopeRouting: [{ scope: "intx", registry: "npmjs" }],
      }),
    ).toThrow(/scopeRouting entry has invalid scope/);
  });

  test("throws if a scopeRouting entry uses uppercase characters", () => {
    expect(() =>
      createClosureResolver({
        registries: registriesOf(
          httpSource({ name: "npmjs", url: "https://npmjs.test" }, {}),
        ),
        defaultRegistry: "npmjs",
        scopeRouting: [{ scope: "@INTX", registry: "npmjs" }],
      }),
    ).toThrow(/scopeRouting entry has invalid scope/);
  });

  test("accepts a well-formed npm scope", () => {
    expect(() =>
      createClosureResolver({
        registries: registriesOf(
          httpSource({ name: "npmjs", url: "https://npmjs.test" }, {}),
        ),
        defaultRegistry: "npmjs",
        scopeRouting: [{ scope: "@intx", registry: "npmjs" }],
      }),
    ).not.toThrow();
  });
});

describe("resolveClosure", () => {
  test("resolves a single pin with no transitive deps", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      { "tools-leaf": packument("tools-leaf", { "1.0.0": {} }) },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    const manifest = await resolver.resolveClosure([
      { name: "tools-leaf", version: "1.0.0" },
    ]);
    expect(manifest.entries).toHaveLength(1);
    expect(manifest.entries[0]).toMatchObject({
      name: "tools-leaf",
      version: "1.0.0",
      integrity: "sha512-tools-leaf-1.0.0",
      source: { kind: "registry", registry: "npmjs" },
      tarballUrl: "https://example.test/tools-leaf/-/tools-leaf-1.0.0.tgz",
    });
    expect(manifest.topLevel).toEqual([
      { name: "tools-leaf", version: "1.0.0" },
    ]);
  });

  test("walks transitive dependencies", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        "tools-with-deps": packument("tools-with-deps", {
          "1.0.0": { dependencies: { "left-pad": "^1.3.0" } },
        }),
        "left-pad": packument("left-pad", { "1.3.0": {} }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    const manifest = await resolver.resolveClosure([
      { name: "tools-with-deps", version: "1.0.0" },
    ]);
    const names = manifest.entries.map((e) => e.name).sort();
    expect(names).toEqual(["left-pad", "tools-with-deps"]);
  });

  test("dedups multiple paths to the same package@version", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        a: packument("a", { "1.0.0": { dependencies: { shared: "1.0.0" } } }),
        b: packument("b", { "1.0.0": { dependencies: { shared: "1.0.0" } } }),
        shared: packument("shared", { "1.0.0": {} }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    const manifest = await resolver.resolveClosure([
      { name: "a", version: "1.0.0" },
      { name: "b", version: "1.0.0" },
    ]);
    const sharedEntries = manifest.entries.filter((e) => e.name === "shared");
    expect(sharedEntries).toHaveLength(1);
  });

  test("includes optionalDependencies with os/cpu metadata", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        watcher: packument("watcher", {
          "1.0.0": {
            optionalDependencies: {
              "watcher-darwin": "1.0.0",
              "watcher-linux": "1.0.0",
            },
          },
        }),
        "watcher-darwin": packument("watcher-darwin", {
          "1.0.0": { os: ["darwin"] },
        }),
        "watcher-linux": packument("watcher-linux", {
          "1.0.0": { os: ["linux"] },
        }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    const manifest = await resolver.resolveClosure([
      { name: "watcher", version: "1.0.0" },
    ]);
    const darwin = manifest.entries.find((e) => e.name === "watcher-darwin");
    const linux = manifest.entries.find((e) => e.name === "watcher-linux");
    expect(darwin?.os).toEqual(["darwin"]);
    expect(linux?.os).toEqual(["linux"]);
  });

  test("routes scoped packages to the matching registry", async () => {
    const calls: string[] = [];
    const npmjs = new HttpRegistrySource({
      name: "npmjs",
      config: { url: "https://npmjs.test" },
      fetchPackument: async (name) => {
        calls.push(`${name}@npmjs`);
        if (name === "left-pad") {
          return packument("left-pad", { "1.0.0": {} });
        }
        throw new Error(`unexpected fetch: ${name}`);
      },
    });
    const intxPrivate = new HttpRegistrySource({
      name: "intx-private",
      config: { url: "https://intx-private.test" },
      fetchPackument: async (name) => {
        calls.push(`${name}@intx-private`);
        if (name === "@intx/tools-mail") {
          return packument("@intx/tools-mail", { "1.0.0": {} });
        }
        throw new Error(`unexpected fetch: ${name}`);
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(npmjs, intxPrivate),
      defaultRegistry: "npmjs",
      scopeRouting: [{ scope: "@intx", registry: "intx-private" }],
    });
    await resolver.resolveClosure([
      { name: "@intx/tools-mail", version: "1.0.0" },
      { name: "left-pad", version: "1.0.0" },
    ]);
    expect(calls).toContain("@intx/tools-mail@intx-private");
    expect(calls).toContain("left-pad@npmjs");
  });

  test("keeps separate packument caches for registries whose names share a substring", async () => {
    // Length-prefixed cache keys: regression for `${name}::${pkg}` that
    // collided when one registry's name contained the separator.
    const ambiguousCalls: string[] = [];
    const baseCalls: string[] = [];
    const ambiguous = new HttpRegistrySource({
      name: "intx::a",
      config: { url: "https://ambiguous.test" },
      fetchPackument: async (name) => {
        ambiguousCalls.push(name);
        return packument(name, { "1.0.0": {} });
      },
    });
    const base = new HttpRegistrySource({
      name: "intx",
      config: { url: "https://base.test" },
      fetchPackument: async (name) => {
        baseCalls.push(name);
        return packument(name, { "1.0.0": {} });
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(ambiguous, base),
      defaultRegistry: "intx",
      scopeRouting: [{ scope: "@a", registry: "intx::a" }],
    });
    await resolver.resolveClosure([
      { name: "@a/foo", version: "1.0.0" },
      { name: "foo", version: "1.0.0" },
    ]);
    expect(ambiguousCalls).toContain("@a/foo");
    expect(baseCalls).toContain("foo");
  });

  test("throws if scopeRouting references unknown registry", async () => {
    const src = httpSource({ name: "npmjs", url: "https://r.test" }, {});
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
      scopeRouting: [{ scope: "@intx", registry: "does-not-exist" }],
    });
    await expect(
      resolver.resolveClosure([{ name: "@intx/foo", version: "1.0.0" }]),
    ).rejects.toThrow(/scopeRouting references unknown registry/);
  });

  test("throws if a packument entry lacks dist.integrity", async () => {
    const src = new HttpRegistrySource({
      name: "npmjs",
      config: { url: "https://r.test" },
      fetchPackument: async () => ({
        name: "broken",
        versions: {
          "1.0.0": {
            name: "broken",
            version: "1.0.0",
            dist: { tarball: "https://example.test/broken-1.0.0.tgz" },
          },
        },
      }),
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    await expect(
      resolver.resolveClosure([{ name: "broken", version: "1.0.0" }]),
    ).rejects.toThrow(/no dist.integrity/);
  });
});

describe("resolveClosure peer-dependency validation", () => {
  test("satisfied peers are accepted", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        "react-tools": packument("react-tools", {
          "1.0.0": {
            dependencies: { react: "^17.0.0" },
            peerDependencies: { react: "^17.0.0" },
          },
        }),
        react: packument("react", { "17.0.2": {} }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    const manifest = await resolver.resolveClosure([
      { name: "react-tools", version: "1.0.0" },
    ]);
    const reactEntry = manifest.entries.find((e) => e.name === "react");
    expect(reactEntry?.version).toBe("17.0.2");
  });

  test("unsatisfied peers throw ManifestInvalidError", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        "react-tools": packument("react-tools", {
          "1.0.0": { peerDependencies: { react: "^17.0.0" } },
        }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    await expect(
      resolver.resolveClosure([{ name: "react-tools", version: "1.0.0" }]),
    ).rejects.toBeInstanceOf(ManifestInvalidError);
  });

  test("ManifestInvalidError carries violation details", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        "react-tools": packument("react-tools", {
          "1.0.0": { peerDependencies: { react: "^17.0.0" } },
        }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    let caught: unknown;
    try {
      await resolver.resolveClosure([
        { name: "react-tools", version: "1.0.0" },
      ]);
    } catch (err) {
      caught = err;
    }
    if (!(caught instanceof ManifestInvalidError)) {
      throw new Error("expected ManifestInvalidError, got something else");
    }
    expect(caught.violations).toHaveLength(1);
    expect(caught.violations[0]).toMatchObject({
      dependent: { name: "react-tools", version: "1.0.0" },
      peer: { name: "react", range: "^17.0.0" },
      satisfiedBy: null,
    });
  });
});

describe("resolveClosure top-level versions", () => {
  test("topLevel carries the resolved concrete version, not the pin's range", async () => {
    const src = new HttpRegistrySource({
      name: "r",
      config: { url: "https://r.test" },
      fetchPackument: async (name) => {
        if (name !== "@intx/widget") throw new Error(`no packument: ${name}`);
        return {
          name: "@intx/widget",
          versions: {
            "1.2.3": {
              name: "@intx/widget",
              version: "1.2.3",
              dist: {
                tarball: "https://r.test/widget-1.2.3.tgz",
                integrity: "sha512-AAAA",
              },
            },
          },
        };
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "r",
    });

    const manifest = await resolver.resolveClosure([
      { name: "@intx/widget", version: "^1.0.0" },
    ]);

    expect(manifest.topLevel).toEqual([
      { name: "@intx/widget", version: "1.2.3" },
    ]);
    expect(manifest.entries[0]?.version).toBe("1.2.3");
    const topLevelKeys = new Set(
      manifest.topLevel.map((p) => `${p.name}@${p.version}`),
    );
    const entryKeys = manifest.entries.map((e) => `${e.name}@${e.version}`);
    for (const key of entryKeys) {
      expect(topLevelKeys.has(key)).toBe(true);
    }
  });

  test("top-level pin's resolved version wins over a transitive that shares its name", async () => {
    // The BFS walk records each top-level pin's resolved version on
    // first arrival. The invariant the resolver depends on is that a
    // top-level pin's resolution lands in `topLevelResolved` BEFORE
    // any transitive dep walker reaches the same name — otherwise a
    // transitive that satisfies a wider range would silently overwrite
    // the top-level pin's chosen version, and the manifest's
    // `topLevel` list would reference a version the operator did not
    // pin. The property is enforced by seeding the queue with all pins
    // before any fan-out and by FIFO queue semantics; a future
    // refactor that swaps the queue for a priority list or interleaves
    // seed-vs-transitive would silently invert first-arrival, and
    // wrong-version manifest entries would pass type checking and the
    // apply pipeline. Lock the property with a test so the regression
    // surfaces as a failing assertion, not a 3am page.
    //
    // Fixture: pin `shared@^1.0.0` (resolves to 1.2.0) and pin
    // `other@^1.0.0` whose transitive dep `shared@^1.5.0` would
    // resolve to 1.6.0 if it arrived first. The manifest's
    // `topLevel` must carry shared@1.2.0 (the top-level pin's
    // resolution), regardless of which BFS walker reaches `shared`
    // first internally.
    const fetchPackument: PackumentFetcher = async (name) => {
      if (name === "shared") {
        return {
          name: "shared",
          versions: {
            "1.2.0": {
              name: "shared",
              version: "1.2.0",
              dist: {
                tarball: "https://r.test/shared-1.2.0.tgz",
                integrity: "sha512-AAAA",
              },
            },
            "1.6.0": {
              name: "shared",
              version: "1.6.0",
              dist: {
                tarball: "https://r.test/shared-1.6.0.tgz",
                integrity: "sha512-BBBB",
              },
            },
          },
        };
      }
      if (name === "other") {
        return {
          name: "other",
          versions: {
            "1.0.0": {
              name: "other",
              version: "1.0.0",
              dist: {
                tarball: "https://r.test/other-1.0.0.tgz",
                integrity: "sha512-CCCC",
              },
              dependencies: { shared: "^1.5.0" },
            },
          },
        };
      }
      throw new Error(`no packument: ${name}`);
    };
    const resolver = createClosureResolver({
      registries: registriesOf(
        new HttpRegistrySource({
          name: "r",
          config: { url: "https://r.test" },
          fetchPackument,
        }),
      ),
      defaultRegistry: "r",
    });

    // Pin `shared` to the narrow `~1.2.0` range (>=1.2.0, <1.3.0) so
    // the top-level resolution is forced to 1.2.0. `other`'s
    // transitive depends on `shared@^1.0.0`, which alone would let
    // npm-pick-manifest choose 1.6.0 — but the first-arrival
    // invariant must use the top-level's 1.2.0 (still satisfies
    // ^1.0.0) instead of overwriting the top-level slot with 1.6.0.
    const manifest = await resolver.resolveClosure([
      { name: "shared", version: "~1.2.0" },
      { name: "other", version: "^1.0.0" },
    ]);

    // The top-level pin's resolved version is 1.2.0; the transitive's
    // wider range (^1.0.0) is satisfied by 1.2.0 so no second
    // `shared` entry is minted, and the top-level slot is unchanged.
    expect(manifest.topLevel).toContainEqual({
      name: "shared",
      version: "1.2.0",
    });
    expect(manifest.topLevel).not.toContainEqual({
      name: "shared",
      version: "1.6.0",
    });
  });
});

describe("resolveClosure duplicate-pin gate", () => {
  test("rejects two pins with the same name but different ranges", async () => {
    const src = httpSource(
      { name: "npmjs", url: "https://r.test" },
      {
        foo: packument("foo", {
          "1.5.0": {},
          "2.0.0": {},
        }),
      },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "npmjs",
    });
    await expect(
      resolver.resolveClosure([
        { name: "foo", version: "^1" },
        { name: "foo", version: "^2" },
      ]),
    ).rejects.toThrow(ManifestInvalidError);
  });
});

describe("resolveClosure optionalDependencies", () => {
  test("optionalDependencies missing from the registry are skipped, not failed", async () => {
    const src = new HttpRegistrySource({
      name: "r",
      config: { url: "https://r.test" },
      fetchPackument: async (name) => {
        if (name === "root-pkg") {
          return {
            name: "root-pkg",
            versions: {
              "1.0.0": {
                name: "root-pkg",
                version: "1.0.0",
                dist: {
                  tarball: "https://r.test/root.tgz",
                  integrity: "sha512-AAAA",
                },
                optionalDependencies: { "missing-from-registry": "^1.0.0" },
              },
            },
          };
        }
        throw new Error(`no packument: ${name}`);
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "r",
    });

    const manifest = await resolver.resolveClosure([
      { name: "root-pkg", version: "1.0.0" },
    ]);
    expect(manifest.entries).toHaveLength(1);
    expect(manifest.entries[0]?.name).toBe("root-pkg");
  });

  test("missing non-optional dependency still aborts the closure walk", async () => {
    const src = new HttpRegistrySource({
      name: "r",
      config: { url: "https://r.test" },
      fetchPackument: async (name) => {
        if (name === "root-pkg") {
          return {
            name: "root-pkg",
            versions: {
              "1.0.0": {
                name: "root-pkg",
                version: "1.0.0",
                dist: {
                  tarball: "https://r.test/root.tgz",
                  integrity: "sha512-AAAA",
                },
                dependencies: { "missing-from-registry": "^1.0.0" },
              },
            },
          };
        }
        throw new Error(`no packument: ${name}`);
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "r",
    });

    let caught: unknown;
    try {
      await resolver.resolveClosure([{ name: "root-pkg", version: "1.0.0" }]);
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);
    expect(String(caught)).toMatch(/missing-from-registry/);
  });

  test("a failure inside an optional subtree drops the whole subtree", async () => {
    // root-pkg has an optional dep `opt-parent` whose own hard dep
    // `opt-child-missing` is not in the registry. The whole optional
    // subtree (parent + would-be child) must be absent from the
    // resulting manifest; root-pkg survives.
    const src = new HttpRegistrySource({
      name: "r",
      config: { url: "https://r.test" },
      fetchPackument: async (name) => {
        if (name === "root-pkg") {
          return {
            name: "root-pkg",
            versions: {
              "1.0.0": {
                name: "root-pkg",
                version: "1.0.0",
                dist: {
                  tarball: "https://r.test/root.tgz",
                  integrity: "sha512-AAAA",
                },
                optionalDependencies: { "opt-parent": "^1.0.0" },
              },
            },
          };
        }
        if (name === "opt-parent") {
          return {
            name: "opt-parent",
            versions: {
              "1.0.0": {
                name: "opt-parent",
                version: "1.0.0",
                dist: {
                  tarball: "https://r.test/opt-parent.tgz",
                  integrity: "sha512-BBBB",
                },
                dependencies: { "opt-child-missing": "^1.0.0" },
              },
            },
          };
        }
        throw new Error(`no packument: ${name}`);
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "r",
    });

    const manifest = await resolver.resolveClosure([
      { name: "root-pkg", version: "1.0.0" },
    ]);
    const names = manifest.entries.map((e) => e.name).sort();
    expect(names).toEqual(["root-pkg"]);
  });

  test("a dep first seen in a poisoned subtree still lands in a sibling subtree", async () => {
    // root has two optional subtrees A and B that both depend on
    // `shared@1`. Subtree A also depends on `missing` (unresolvable),
    // which poisons A. The walker visits A's `shared` edge before B's
    // due to BFS declaration order, so without a subtree-aware dedup
    // the second arrival of `shared` from B is dropped and B silently
    // loses its hard dependency when A's stash is discarded.
    const src = new HttpRegistrySource({
      name: "r",
      config: { url: "https://r.test" },
      fetchPackument: async (name) => {
        if (name === "root") {
          return packument("root", {
            "1.0.0": {
              optionalDependencies: {
                "a-parent": "^1.0.0",
                "b-parent": "^1.0.0",
              },
            },
          });
        }
        if (name === "a-parent") {
          return packument("a-parent", {
            "1.0.0": {
              dependencies: { shared: "^1.0.0", missing: "^1.0.0" },
            },
          });
        }
        if (name === "b-parent") {
          return packument("b-parent", {
            "1.0.0": { dependencies: { shared: "^1.0.0" } },
          });
        }
        if (name === "shared") {
          return packument("shared", { "1.0.0": {} });
        }
        throw new Error(`no packument: ${name}`);
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "r",
    });
    const manifest = await resolver.resolveClosure([
      { name: "root", version: "1.0.0" },
    ]);
    const names = manifest.entries.map((e) => e.name).sort();
    expect(names).toContain("root");
    expect(names).toContain("b-parent");
    expect(names).toContain("shared");
    expect(names).not.toContain("a-parent");
  });

  test("poisoning an outer optional subtree cascades to nested optional descendants", async () => {
    // root depends on A (regular). A opens optional OuterOpt.
    // OuterOpt opens optional InnerOpt (nested under OuterOpt's
    // subtree) and depends-on hard OuterHard, whose packument is
    // missing — poisoning OuterOpt's subtree. InnerOpt's own subtree
    // would otherwise complete cleanly and merge its entries into the
    // top-level closure, leaving InnerOpt and InnerDep as orphans
    // whose only requirer chain runs through the dropped OuterOpt.
    // The cascade must drop InnerOpt's subtree too.
    const src = new HttpRegistrySource({
      name: "r",
      config: { url: "https://r.test" },
      fetchPackument: async (name) => {
        if (name === "A") {
          return packument("A", {
            "1.0.0": { optionalDependencies: { OuterOpt: "^1.0.0" } },
          });
        }
        if (name === "OuterOpt") {
          return packument("OuterOpt", {
            "1.0.0": {
              optionalDependencies: { InnerOpt: "^1.0.0" },
              dependencies: { OuterHard: "^1.0.0" },
            },
          });
        }
        if (name === "InnerOpt") {
          return packument("InnerOpt", {
            "1.0.0": { dependencies: { InnerDep: "^1.0.0" } },
          });
        }
        if (name === "InnerDep") {
          return packument("InnerDep", { "1.0.0": {} });
        }
        throw new Error(`no packument: ${name}`);
      },
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "r",
    });
    const manifest = await resolver.resolveClosure([
      { name: "A", version: "1.0.0" },
    ]);
    const names = manifest.entries.map((e) => e.name).sort();
    expect(names).toContain("A");
    expect(names).not.toContain("OuterOpt");
    expect(names).not.toContain("OuterHard");
    expect(names).not.toContain("InnerOpt");
    expect(names).not.toContain("InnerDep");
  });
});

describe("AssetRegistrySource", () => {
  // Build tarballs in a scratch dir, then expose them via an in-memory
  // readBlob / listBlobs pair so the source sees the same byte stream
  // it would get from the asset service in production.
  async function buildAssetFixture(
    pkgs: readonly {
      name: string;
      version: string;
      pkgJsonExtras?: Record<string, unknown>;
    }[],
  ): Promise<{
    readBlob: (path: string) => Promise<Uint8Array>;
    listBlobs: (dir: string) => Promise<string[]>;
    tarballNames: string[];
  }> {
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "asset-reg-src-"));
    const tarballNames: string[] = [];
    const byPath = new Map<string, Uint8Array>();
    for (const pkg of pkgs) {
      const stagingDir = path.join(
        dir,
        `${pkg.name.replace("/", "_")}-${pkg.version}`,
      );
      const pkgDir = path.join(stagingDir, "package");
      await fs.mkdir(pkgDir, { recursive: true });
      await fs.writeFile(
        path.join(pkgDir, "package.json"),
        JSON.stringify({
          name: pkg.name,
          version: pkg.version,
          ...(pkg.pkgJsonExtras ?? {}),
        }),
      );
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
      const basename = pkg.name.startsWith("@")
        ? pkg.name.split("/")[1]
        : pkg.name;
      const filename = `${String(basename)}-${pkg.version}.tgz`;
      tarballNames.push(filename);
      byPath.set(`tarballs/${filename}`, bytes);
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
      tarballNames,
    };
  }

  test("emits an asset-tagged entry for every pin sourced from the asset", async () => {
    const fixture = await buildAssetFixture([
      { name: "tools-alpha", version: "1.0.0" },
    ]);
    const src = new AssetRegistrySource({
      name: "workspace-builtins",
      assetId: "asset_workspace_builtins",
      readBlob: fixture.readBlob,
      listBlobs: fixture.listBlobs,
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "workspace-builtins",
    });
    const manifest = await resolver.resolveClosure([
      { name: "tools-alpha", version: "1.0.0" },
    ]);
    expect(manifest.entries).toHaveLength(1);
    const entry = manifest.entries[0];
    expect(entry?.source).toEqual({
      kind: "asset",
      assetId: "asset_workspace_builtins",
      path: `tarballs/${fixture.tarballNames[0]}`,
    });
    expect(entry?.tarballUrl).toBeUndefined();
    expect(entry?.integrity).toMatch(/^sha512-/);
  });

  test("computes per-tarball SRI from bytes — divergent contents yield divergent integrities", async () => {
    const fixture = await buildAssetFixture([
      { name: "p", version: "1.0.0" },
      { name: "q", version: "1.0.0" },
    ]);
    const src = new AssetRegistrySource({
      name: "wb",
      assetId: "asset_x",
      readBlob: fixture.readBlob,
      listBlobs: fixture.listBlobs,
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "wb",
    });
    const manifest = await resolver.resolveClosure([
      { name: "p", version: "1.0.0" },
      { name: "q", version: "1.0.0" },
    ]);
    const integrities = manifest.entries.map((e) => e.integrity);
    expect(new Set(integrities).size).toBe(integrities.length);
    for (const integrity of integrities) {
      expect(integrity).toMatch(/^sha512-/);
    }
    // Cross-check: re-derive SRI from the on-disk bytes.
    for (const entry of manifest.entries) {
      if (entry.source.kind !== "asset") {
        throw new Error(`expected asset entry, got ${entry.source.kind}`);
      }
      const bytes = await fixture.readBlob(entry.source.path);
      const expected = ssri
        .fromData(bytes, { algorithms: ["sha512"] })
        .toString();
      expect(entry.integrity).toBe(expected);
    }
  });

  test("mixes with an HTTP source for transitive deps the asset does not publish", async () => {
    // Top-level pins live in the asset; transitive `left-pad` lives in
    // the HTTP registry. The asset registry is the default so unscoped
    // top-level pins resolve through it; the transitive dep then
    // bounces to the HTTP registry because the asset does not publish
    // it. This is the asset-publishes-its-own-closure shape: the asset
    // is authoritative for the names it carries and the http registry
    // covers any name the asset does not.
    //
    // The two-step fallback is not implemented at the resolver layer —
    // the walker queries the source the routing rule names and treats
    // a miss as a fatal closure-walk failure. To make `left-pad` route
    // to the http registry, exercise the scopeRouting feature: `@np`
    // is a scope that routes to npmjs, but unscoped names that the
    // asset does not publish need an alternative. For this test we
    // exercise the cleaner shape: the asset-registry version of
    // `tools-mixed` declares the transitive dep via a scoped name so
    // scope routing carries it to the http registry.
    const fixture = await buildAssetFixture([
      {
        name: "tools-mixed",
        version: "1.0.0",
        pkgJsonExtras: { dependencies: { "@np/left-pad": "^1.3.0" } },
      },
    ]);
    const assetSrc = new AssetRegistrySource({
      name: "asset-reg",
      assetId: "asset_mixed",
      readBlob: fixture.readBlob,
      listBlobs: fixture.listBlobs,
    });
    const httpSrc = httpSource(
      { name: "npmjs", url: "https://r.test" },
      { "@np/left-pad": packument("@np/left-pad", { "1.3.0": {} }) },
    );
    const resolver = createClosureResolver({
      registries: registriesOf(assetSrc, httpSrc),
      defaultRegistry: "asset-reg",
      scopeRouting: [{ scope: "@np", registry: "npmjs" }],
    });
    const manifest = await resolver.resolveClosure([
      { name: "tools-mixed", version: "1.0.0" },
    ]);
    expect(manifest.entries).toHaveLength(2);
    const byName = new Map(manifest.entries.map((e) => [e.name, e]));
    expect(byName.get("tools-mixed")?.source.kind).toBe("asset");
    expect(byName.get("@np/left-pad")?.source.kind).toBe("registry");
  });

  test("inner-tenancy asset shadows http registry of the same name", async () => {
    // The resolver does not itself implement tenant inheritance — the
    // session service walks the chain and decides which source claims
    // each registry name. The collision behavior at the resolver layer
    // is simpler: the map only carries one entry per name, so whichever
    // source the session service inserted under "shared" wins. This
    // test locks in that the resolver respects that single-entry
    // contract end-to-end.
    const fixture = await buildAssetFixture([
      { name: "shadowed", version: "1.0.0" },
    ]);
    const inner = new AssetRegistrySource({
      name: "shared",
      assetId: "asset_inner",
      readBlob: fixture.readBlob,
      listBlobs: fixture.listBlobs,
    });
    // Build a map directly so the test calls out that the asset source
    // occupies the slot the http source would otherwise hold.
    const registries = new Map<string, RegistrySource>([[inner.name, inner]]);
    const resolver = createClosureResolver({
      registries,
      defaultRegistry: "shared",
    });
    const manifest = await resolver.resolveClosure([
      { name: "shadowed", version: "1.0.0" },
    ]);
    expect(manifest.entries).toHaveLength(1);
    expect(manifest.entries[0]?.source.kind).toBe("asset");
  });

  test("asset registry wins over an http registry of the same name", async () => {
    // Lock in the collision-resolution contract the session service
    // enforces: when both an AssetRegistrySource and an
    // HttpRegistrySource carry the same registry name, the asset
    // entry is what the resolver consults. The session service
    // populates its registry map by inserting assets first and
    // skipping HTTP entries on name collision; this test replays the
    // same composition and asserts the asset path produced the
    // manifest entry.
    const NAME = "shared-registry";
    const fixture = await buildAssetFixture([
      { name: "claimed", version: "1.0.0" },
    ]);
    const assetSrc = new AssetRegistrySource({
      name: NAME,
      assetId: "asset_winner",
      readBlob: fixture.readBlob,
      listBlobs: fixture.listBlobs,
    });
    const httpSrc = new HttpRegistrySource({
      name: NAME,
      config: { url: "https://shadowed.test" },
      fetchPackument: async () => {
        throw new Error(
          "http registry was consulted but the asset registry should have shadowed it",
        );
      },
    });

    // Replay the session-service composition: asset first, then HTTP
    // only when the name is not already claimed. The Map is the
    // resolver's view of the world; the policy lives in how this
    // map is built.
    const registries = new Map<string, RegistrySource>();
    for (const src of [assetSrc, httpSrc]) {
      if (registries.has(src.name)) continue;
      registries.set(src.name, src);
    }
    expect(registries.get(NAME)).toBe(assetSrc);

    const resolver = createClosureResolver({
      registries,
      defaultRegistry: NAME,
    });
    const manifest = await resolver.resolveClosure([
      { name: "claimed", version: "1.0.0" },
    ]);
    expect(manifest.entries).toHaveLength(1);
    const entry = manifest.entries[0];
    expect(entry?.source).toEqual({
      kind: "asset",
      assetId: "asset_winner",
      path: `tarballs/${fixture.tarballNames[0]}`,
    });
  });

  test("rejects the closure when a tarball's package.json fails validation", async () => {
    // Construct a tarball whose package.json is missing the required
    // `name` field; AssetRegistrySource must surface this via
    // ManifestInvalidError instead of silently skipping the tarball.
    const dir = await fs.mkdtemp(path.join(os.tmpdir(), "asset-reg-bad-"));
    const stagingDir = path.join(dir, "broken");
    const pkgDir = path.join(stagingDir, "package");
    await fs.mkdir(pkgDir, { recursive: true });
    await fs.writeFile(
      path.join(pkgDir, "package.json"),
      JSON.stringify({ version: "1.0.0" }),
    );
    const tarballPath = path.join(stagingDir, "out.tgz");
    await tar.create({ cwd: stagingDir, gzip: true, file: tarballPath }, [
      "package",
    ]);
    const bytes = await fs.readFile(tarballPath);
    const byPath = new Map<string, Uint8Array>([
      ["tarballs/broken-1.0.0.tgz", bytes],
    ]);
    const src = new AssetRegistrySource({
      name: "broken",
      assetId: "asset_broken",
      readBlob: async (p) => {
        const b = byPath.get(p);
        if (b === undefined) throw new Error(`no blob at ${p}`);
        return b;
      },
      listBlobs: async () =>
        Array.from(byPath.keys()).map((p) => p.slice("tarballs/".length)),
    });
    const resolver = createClosureResolver({
      registries: registriesOf(src),
      defaultRegistry: "broken",
    });
    await expect(
      resolver.resolveClosure([{ name: "anything", version: "1.0.0" }]),
    ).rejects.toBeInstanceOf(ManifestInvalidError);
  });
});

describe("parsePin", () => {
  test("parses a scoped name with caret range", () => {
    expect(parsePin("@intx/tools-mail@^1.2.0")).toEqual({
      name: "@intx/tools-mail",
      version: "^1.2.0",
    });
  });

  test("parses a bare name with exact version", () => {
    expect(parsePin("left-pad@1.3.0")).toEqual({
      name: "left-pad",
      version: "1.3.0",
    });
  });

  test("throws on an invalid range", () => {
    expect(() => parsePin("foo@not-a-range")).toThrow(/invalid version range/);
  });

  test("rejects a mixed-case package name", () => {
    // npm itself forbids uppercase in package names; the sidecar
    // loader matches entries by their lowercase packument name, so a
    // mixed-case pin would silently fall off the matcher's key set.
    expect(() => parsePin("@INTX/tools-mail@1.0.0")).toThrow(
      /pin name must be lowercase/,
    );
  });
});
