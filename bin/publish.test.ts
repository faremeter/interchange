import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";

import {
  type ObservedMatrix,
  type Target,
  assertMatrix,
  checkInternalDepExpressions,
  checkPackedManifest,
  checkVersionSync,
  readTargets,
  topoSortLeafFirst,
} from "./publish";

function target(
  name: string,
  version: string,
  deps: Record<string, string> = {},
  field = "dependencies",
): Target {
  const intxSpecs = Object.entries(deps)
    .filter(([n]) => n.startsWith("@intx/"))
    .map(([n, spec]) => ({ field, name: n, spec }));
  return {
    name,
    dir: `/repo/packages/${name.replace("@intx/", "")}`,
    version,
    internalDeps: [...new Set(intxSpecs.map((s) => s.name))],
    intxSpecs,
  };
}

describe("checkVersionSync", () => {
  test("passes when every package is at the release version", () => {
    const targets = [target("@intx/a", "0.2.0"), target("@intx/b", "0.2.0")];
    expect(checkVersionSync(targets, "0.2.0")).toEqual([]);
  });

  test("flags each package whose version lags the release", () => {
    const targets = [target("@intx/a", "0.2.0"), target("@intx/b", "0.1.2")];
    const violations = checkVersionSync(targets, "0.2.0");
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("@intx/b");
    expect(violations[0]).toContain("0.1.2");
  });
});

describe("checkInternalDepExpressions", () => {
  test("accepts workspace: and catalog: specifiers", () => {
    const targets = [
      target("@intx/a", "0.2.0", { "@intx/b": "workspace:*" }),
      target("@intx/b", "0.2.0", { left: "catalog:", other: "^1.0.0" }),
    ];
    expect(checkInternalDepExpressions(targets)).toEqual([]);
  });

  test("rejects an internal dependency pinned to a literal version", () => {
    const targets = [target("@intx/a", "0.2.0", { "@intx/b": "0.1.2" })];
    const violations = checkInternalDepExpressions(targets);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("@intx/b");
    expect(violations[0]).toContain("0.1.2");
  });

  test("ignores third-party dependencies pinned to literals", () => {
    const targets = [target("@intx/a", "0.2.0", { "left-pad": "^1.3.0" })];
    expect(checkInternalDepExpressions(targets)).toEqual([]);
  });
});

describe("topoSortLeafFirst", () => {
  test("orders each package after its internal dependencies", () => {
    const targets = [
      target("@intx/app", "0.2.0", { "@intx/lib": "workspace:*" }),
      target("@intx/lib", "0.2.0", { "@intx/core": "workspace:*" }),
      target("@intx/core", "0.2.0"),
    ];
    const ordered = topoSortLeafFirst(targets).map((t) => t.name);
    expect(ordered.indexOf("@intx/core")).toBeLessThan(
      ordered.indexOf("@intx/lib"),
    );
    expect(ordered.indexOf("@intx/lib")).toBeLessThan(
      ordered.indexOf("@intx/app"),
    );
  });

  test("throws on a dependency cycle", () => {
    const targets = [
      target("@intx/a", "0.2.0", { "@intx/b": "workspace:*" }),
      target("@intx/b", "0.2.0", { "@intx/a": "workspace:*" }),
    ];
    expect(() => topoSortLeafFirst(targets)).toThrow(/cycle/);
  });

  test("ignores external @intx-looking deps not in the target set", () => {
    const targets = [
      target("@intx/a", "0.2.0", { "@intx/not-a-target": "workspace:*" }),
    ];
    expect(topoSortLeafFirst(targets).map((t) => t.name)).toEqual(["@intx/a"]);
  });

  test("orders after an internal dependency in a non-dependencies field", () => {
    const targets = [
      target(
        "@intx/app",
        "0.2.0",
        { "@intx/lib": "workspace:*" },
        "peerDependencies",
      ),
      target("@intx/lib", "0.2.0"),
    ];
    const ordered = topoSortLeafFirst(targets).map((t) => t.name);
    expect(ordered.indexOf("@intx/lib")).toBeLessThan(
      ordered.indexOf("@intx/app"),
    );
  });
});

describe("checkPackedManifest", () => {
  test("accepts internal deps rewritten to the release version", () => {
    const manifest = {
      name: "@intx/mime",
      version: "0.2.0",
      dependencies: { "@intx/crypto": "0.2.0", arktype: "^2.1.29" },
    };
    expect(checkPackedManifest("@intx/mime", manifest, "0.2.0")).toEqual([]);
  });

  test("flags an internal dep left at a stale version by a stale lockfile", () => {
    const manifest = {
      name: "@intx/mime",
      version: "0.2.0",
      dependencies: { "@intx/crypto": "0.1.2", "@intx/types": "0.2.0" },
    };
    const violations = checkPackedManifest("@intx/mime", manifest, "0.2.0");
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("@intx/crypto");
    expect(violations[0]).toContain("0.1.2");
  });

  test("accepts a caret range from a workspace:^ specifier", () => {
    const manifest = {
      name: "@intx/mime",
      version: "0.2.0",
      dependencies: { "@intx/crypto": "^0.2.0", "@intx/types": "~0.2.0" },
    };
    expect(checkPackedManifest("@intx/mime", manifest, "0.2.0")).toEqual([]);
  });

  test("ignores third-party dep versions", () => {
    const manifest = {
      name: "@intx/mime",
      version: "0.2.0",
      dependencies: { arktype: "^2.1.29", "left-pad": "1.3.0" },
    };
    expect(checkPackedManifest("@intx/mime", manifest, "0.2.0")).toEqual([]);
  });
});

describe("assertMatrix", () => {
  test("passes when every asserted runtime loads as expected", () => {
    const observed: ObservedMatrix = {
      "@intx/a": { node: "load", bun: "load" },
    };
    expect(assertMatrix(observed)).toEqual([]);
  });

  test("accepts a documented incompatibility failing as expected", () => {
    const observed: ObservedMatrix = {
      "@intx/tools-lsp": { node: "fail", bun: "load", deno: "fail" },
    };
    expect(assertMatrix(observed)).toEqual([]);
  });

  test("flags a documented incompatibility that unexpectedly loads", () => {
    const observed: ObservedMatrix = {
      "@intx/tools-lsp": { node: "load", bun: "load" },
    };
    const violations = assertMatrix(observed);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("remove the exception");
  });

  test("flags a package that fails a runtime it should load under", () => {
    const observed: ObservedMatrix = {
      "@intx/a": { node: "fail", bun: "load" },
    };
    const violations = assertMatrix(observed);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("expected to load");
  });

  test("skips a runtime that was unavailable (no observed result)", () => {
    const observed: ObservedMatrix = {
      "@intx/a": { node: "load", bun: "load" }, // deno not on PATH -> absent
    };
    expect(assertMatrix(observed)).toEqual([]);
  });

  test("flags a package that fails deno when it should load", () => {
    const observed: ObservedMatrix = {
      "@intx/a": { node: "load", bun: "load", deno: "fail" },
    };
    const violations = assertMatrix(observed);
    expect(violations).toHaveLength(1);
    expect(violations[0]).toContain("deno");
  });
});

describe("readTargets", () => {
  test("includes non-private packages, skips private and manifest-less dirs", () => {
    const repo = mkdtempSync(join(tmpdir(), "readtargets-"));
    const pkgs = join(repo, "packages");
    mkdirSync(pkgs);
    const writePkg = (dir: string, manifest: unknown): void => {
      const d = join(pkgs, dir);
      mkdirSync(d);
      if (manifest !== undefined) {
        writeFileSync(join(d, "package.json"), JSON.stringify(manifest));
      }
    };
    writePkg("a", {
      name: "@intx/a",
      version: "0.2.0",
      dependencies: { "@intx/b": "workspace:*" },
    });
    writePkg("b", { name: "@intx/b", version: "0.2.0" });
    writePkg("secret", {
      name: "@intx/secret",
      version: "0.2.0",
      private: true,
    });
    writePkg("empty", undefined); // no package.json -> ENOENT -> skipped
    try {
      const targets = readTargets(repo);
      expect(targets.map((t) => t.name)).toEqual(["@intx/a", "@intx/b"]);
      expect(targets.find((t) => t.name === "@intx/a")?.internalDeps).toEqual([
        "@intx/b",
      ]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });
});
