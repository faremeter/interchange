import { describe, expect, test } from "bun:test";

import { parseAdapterManifestEnv } from "./adapter-manifest";

describe("parseAdapterManifestEnv", () => {
  test("treats an absent or blank value as no declaration", () => {
    expect(parseAdapterManifestEnv(undefined)).toBeUndefined();
    expect(parseAdapterManifestEnv("")).toBeUndefined();
    expect(parseAdapterManifestEnv("   ")).toBeUndefined();
  });

  test("returns an empty manifest for an empty array", () => {
    expect(parseAdapterManifestEnv("[]")).toEqual([]);
  });

  test("returns the entries of a valid manifest", () => {
    const entries = [
      { provider: "openai-responses", specifier: "./adapter", export: "make" },
    ];
    expect(parseAdapterManifestEnv(JSON.stringify(entries))).toEqual(entries);
  });

  test("rejects malformed JSON", () => {
    expect(() => parseAdapterManifestEnv("{not json")).toThrow(
      /SIDECAR_ADAPTER_MANIFEST is not valid JSON/,
    );
  });

  test("rejects a value of the wrong shape", () => {
    for (const raw of [
      '{"provider":"x"}',
      '[{"provider":1}]',
      '[{"provider":"a:b","specifier":"x","export":"y"}]',
    ]) {
      expect(() => parseAdapterManifestEnv(raw)).toThrow(
        /SIDECAR_ADAPTER_MANIFEST failed validation/,
      );
    }
  });
});
