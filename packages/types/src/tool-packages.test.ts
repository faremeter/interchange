import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import {
  ToolPackagePin,
  ToolPackagePinArray,
  ToolPackageSource,
  ToolPackageManifestEntry,
  ToolPackageManifest,
} from "./tool-packages";

describe("ToolPackagePin", () => {
  test("accepts a name and version", () => {
    const result = ToolPackagePin({
      name: "@intx/tools-posix",
      version: "1.2.3",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects missing name", () => {
    const result = ToolPackagePin({ version: "1.2.3" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing version", () => {
    const result = ToolPackagePin({ name: "@intx/tools-posix" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects non-string name", () => {
    const result = ToolPackagePin({ name: 42, version: "1.2.3" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an uppercase name", () => {
    const result = ToolPackagePin({ name: "Tools-Posix", version: "1.2.3" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an uppercase scope", () => {
    const result = ToolPackagePin({ name: "@INTX/tools", version: "1.2.3" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a leading-dot name", () => {
    const result = ToolPackagePin({ name: ".hidden", version: "1.2.3" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts a star range at the pin level", () => {
    const result = ToolPackagePin({ name: "left-pad", version: "*" });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a caret range at the pin level", () => {
    const result = ToolPackagePin({ name: "left-pad", version: "^1.2.3" });
    expect(result instanceof type.errors).toBe(false);
  });
});

describe("ToolPackagePinArray", () => {
  test("accepts an array with distinct names", () => {
    const result = ToolPackagePinArray([
      { name: "@intx/tools-posix", version: "1.2.3" },
      { name: "@intx/tools-mail", version: "1.2.3" },
    ]);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an empty array", () => {
    const result = ToolPackagePinArray([]);
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an array with duplicate names", () => {
    const result = ToolPackagePinArray([
      { name: "@intx/tools-posix", version: "1.2.3" },
      { name: "@intx/tools-posix", version: "1.3.0" },
    ]);
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an array containing an invalid pin name", () => {
    const result = ToolPackagePinArray([{ name: "BadName", version: "1.2.3" }]);
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an array containing an unparseable semver range", () => {
    const result = ToolPackagePinArray([
      { name: "left-pad", version: "not-a-range" },
    ]);
    expect(result instanceof type.errors).toBe(true);
  });

  test("accepts a star range", () => {
    const result = ToolPackagePinArray([{ name: "left-pad", version: "*" }]);
    expect(result instanceof type.errors).toBe(false);
  });
});

describe("ToolPackageSource", () => {
  test("accepts an asset source", () => {
    const result = ToolPackageSource({
      kind: "asset",
      assetId: "asset_abc",
      path: "tarballs/foo-1.2.3.tgz",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a registry source", () => {
    const result = ToolPackageSource({
      kind: "registry",
      registry: "npmjs",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown kind", () => {
    const result = ToolPackageSource({
      kind: "ftp",
      path: "foo",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an asset source missing path", () => {
    const result = ToolPackageSource({ kind: "asset", assetId: "asset_abc" });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an asset source missing assetId", () => {
    const result = ToolPackageSource({
      kind: "asset",
      path: "tarballs/foo.tgz",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects a registry source missing registry", () => {
    const result = ToolPackageSource({ kind: "registry" });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("ToolPackageManifestEntry", () => {
  const validEntry = {
    name: "@intx/tools-posix",
    version: "1.2.3",
    integrity: "sha512-AAAA",
    source: {
      kind: "asset",
      assetId: "asset_workspace_builtins",
      path: "tarballs/intx-tools-posix-1.2.3.tgz",
    },
  } as const;

  test("accepts a minimal entry", () => {
    const result = ToolPackageManifestEntry(validEntry);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts os and cpu metadata", () => {
    const result = ToolPackageManifestEntry({
      ...validEntry,
      os: ["darwin", "linux"],
      cpu: ["arm64", "x64"],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a tarballUrl on a registry entry", () => {
    const result = ToolPackageManifestEntry({
      name: "left-pad",
      version: "1.3.0",
      integrity: "sha512-BBBB",
      source: { kind: "registry", registry: "npmjs" },
      tarballUrl: "https://registry.npmjs.org/left-pad/-/left-pad-1.3.0.tgz",
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects missing integrity", () => {
    const result = ToolPackageManifestEntry({
      name: validEntry.name,
      version: validEntry.version,
      source: validEntry.source,
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an unknown source kind", () => {
    const result = ToolPackageManifestEntry({
      ...validEntry,
      source: { kind: "ftp", url: "ftp://example.com/foo" },
    });
    expect(result instanceof type.errors).toBe(true);
  });
});

describe("ToolPackageManifest", () => {
  const validManifest = {
    schemaVersion: "1",
    topLevel: [{ name: "@intx/tools-posix", version: "1.2.3" }],
    entries: [
      {
        name: "@intx/tools-posix",
        version: "1.2.3",
        integrity: "sha512-AAAA",
        source: {
          kind: "asset",
          assetId: "asset_workspace_builtins",
          path: "tarballs/intx-tools-posix-1.2.3.tgz",
        },
      },
    ],
  } as const;

  test("accepts a minimal valid manifest", () => {
    const result = ToolPackageManifest(validManifest);
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts an empty topLevel and entries", () => {
    const result = ToolPackageManifest({
      schemaVersion: "1",
      topLevel: [],
      entries: [],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("accepts a closure with transitive entries", () => {
    const result = ToolPackageManifest({
      schemaVersion: "1",
      topLevel: [{ name: "tools-with-deps", version: "1.0.0" }],
      entries: [
        {
          name: "tools-with-deps",
          version: "1.0.0",
          integrity: "sha512-CCCC",
          source: {
            kind: "asset",
            assetId: "asset_workspace_builtins",
            path: "tarballs/tools-with-deps-1.0.0.tgz",
          },
        },
        {
          name: "left-pad",
          version: "1.3.0",
          integrity: "sha512-DDDD",
          source: { kind: "registry", registry: "npmjs" },
        },
      ],
    });
    expect(result instanceof type.errors).toBe(false);
  });

  test("rejects an unknown schemaVersion", () => {
    const result = ToolPackageManifest({
      ...validManifest,
      schemaVersion: "2",
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing topLevel", () => {
    const result = ToolPackageManifest({
      schemaVersion: "1",
      entries: [],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects missing entries", () => {
    const result = ToolPackageManifest({
      schemaVersion: "1",
      topLevel: [],
    });
    expect(result instanceof type.errors).toBe(true);
  });

  test("rejects an entry with a malformed source", () => {
    const result = ToolPackageManifest({
      schemaVersion: "1",
      topLevel: [{ name: "foo", version: "1.0.0" }],
      entries: [
        {
          name: "foo",
          version: "1.0.0",
          integrity: "sha512-EEEE",
          source: { kind: "asset" },
        },
      ],
    });
    expect(result instanceof type.errors).toBe(true);
  });
});
