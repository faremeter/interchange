import { describe, expect, test } from "bun:test";

import type { AdapterManifest } from "@intx/types";

import { adapterManifestDigest } from "./adapter-manifest-digest";

const first = {
  provider: "acme",
  specifier: "@acme/adapter",
  export: "createAcme",
};
const second = {
  provider: "zenith",
  specifier: "./zenith.js",
  export: "default",
};

describe("adapterManifestDigest", () => {
  test("is a sha256 hex digest", async () => {
    expect(await adapterManifestDigest([first])).toMatch(/^[0-9a-f]{64}$/);
  });

  test("does not depend on the key order of an entry", async () => {
    const reordered: AdapterManifest = [
      {
        export: first.export,
        specifier: first.specifier,
        provider: first.provider,
      },
    ];
    expect(await adapterManifestDigest(reordered)).toBe(
      await adapterManifestDigest([first]),
    );
  });

  test("changes with the order of the entries", async () => {
    expect(await adapterManifestDigest([first, second])).not.toBe(
      await adapterManifestDigest([second, first]),
    );
  });

  test("changes with an entry's value", async () => {
    expect(
      await adapterManifestDigest([{ ...first, export: "createOther" }]),
    ).not.toBe(await adapterManifestDigest([first]));
  });

  test("treats an absent manifest as an empty one", async () => {
    expect(await adapterManifestDigest(undefined)).toBe(
      await adapterManifestDigest([]),
    );
    expect(await adapterManifestDigest([])).not.toBe(
      await adapterManifestDigest([first]),
    );
  });
});
