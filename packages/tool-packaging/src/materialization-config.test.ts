import { describe, expect, test } from "bun:test";

import { hostPlatform, parseToolRegistries } from "./materialization-config";

const EMPTY_REGISTRY_MESSAGE =
  "SIDECAR_TOOL_REGISTRIES is set but empty — unset the variable to use the default npmjs registry";

describe("parseToolRegistries", () => {
  test("undefined returns a fresh npmjs map", () => {
    const first = parseToolRegistries(undefined);
    const second = parseToolRegistries(undefined);
    expect(first).not.toBe(second);
    expect([...first.entries()]).toEqual([
      ["npmjs", { url: "https://registry.npmjs.org" }],
    ]);
    expect([...second.entries()]).toEqual([
      ["npmjs", { url: "https://registry.npmjs.org" }],
    ]);
  });

  test("empty or whitespace throws", () => {
    expect(() => parseToolRegistries("")).toThrow(EMPTY_REGISTRY_MESSAGE);
    expect(() => parseToolRegistries("   ")).toThrow(EMPTY_REGISTRY_MESSAGE);
  });

  test("invalid JSON throws", () => {
    expect(() => parseToolRegistries("{")).toThrow(
      /^SIDECAR_TOOL_REGISTRIES is not valid JSON: /,
    );
  });

  test("schema failure throws", () => {
    expect(() => parseToolRegistries("[{}]")).toThrow(
      /^SIDECAR_TOOL_REGISTRIES failed validation: /,
    );
  });

  test("a duplicate name throws", () => {
    const raw = JSON.stringify([
      { name: "npmjs", url: "https://registry.npmjs.org" },
      { name: "npmjs", url: "https://example.test" },
    ]);
    expect(() => parseToolRegistries(raw)).toThrow(
      'SIDECAR_TOOL_REGISTRIES has duplicate registry name "npmjs"',
    );
  });

  test("omitted auth stays omitted and a present auth is kept", () => {
    const raw = JSON.stringify([
      { name: "plain", url: "https://plain.test" },
      {
        name: "tokened",
        url: "https://tokened.test",
        auth: { token: "secret" },
      },
    ]);
    const registries = parseToolRegistries(raw);
    const plain = registries.get("plain");
    const tokened = registries.get("tokened");
    expect(plain).toEqual({ url: "https://plain.test" });
    expect(plain !== undefined && Object.hasOwn(plain, "auth")).toBe(false);
    expect(tokened).toEqual({
      url: "https://tokened.test",
      auth: { token: "secret" },
    });
  });
});

describe("hostPlatform", () => {
  const platforms = [
    "aix",
    "android",
    "darwin",
    "freebsd",
    "haiku",
    "linux",
    "openbsd",
    "sunos",
    "win32",
    "cygwin",
    "netbsd",
  ];
  const archs = [
    "arm",
    "arm64",
    "ia32",
    "loong64",
    "mips",
    "mipsel",
    "ppc64",
    "riscv64",
    "s390x",
    "x64",
  ];

  test("a known pair is returned as os and cpu", () => {
    for (const os of platforms) {
      expect(hostPlatform(os, "x64")).toEqual({ os, cpu: "x64" });
    }
    for (const cpu of archs) {
      expect(hostPlatform("linux", cpu)).toEqual({ os: "linux", cpu });
    }
  });

  test("an unknown os or cpu throws", () => {
    expect(() => hostPlatform("plan9", "x64")).toThrow(
      'sidecar boot: process.platform "plan9" is not a recognized npm `os` token; tool-package platform filtering would be unreliable',
    );
    expect(() => hostPlatform("linux", "sparc")).toThrow(
      'sidecar boot: process.arch "sparc" is not a recognized npm `cpu` token; tool-package platform filtering would be unreliable',
    );
  });
});
