import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";

import { readWorkspacePackages } from "./packages";

function repoWith(pkgs: Record<string, unknown | undefined>): string {
  const repo = mkdtempSync(join(tmpdir(), "packages-"));
  const pkgsDir = join(repo, "packages");
  mkdirSync(pkgsDir);
  for (const [dir, manifest] of Object.entries(pkgs)) {
    const d = join(pkgsDir, dir);
    mkdirSync(d);
    if (manifest !== undefined) {
      writeFileSync(join(d, "package.json"), JSON.stringify(manifest));
    }
  }
  return repo;
}

describe("readWorkspacePackages", () => {
  test("includes non-private packages, skips private and manifest-less dirs", () => {
    const repo = repoWith({
      a: {
        name: "@intx/a",
        version: "0.2.0",
        dependencies: { "@intx/b": "workspace:*" },
      },
      b: { name: "@intx/b", version: "0.2.0" },
      secret: { name: "@intx/secret", version: "0.2.0", private: true },
      empty: undefined, // directory without a package.json
    });
    try {
      const pkgs = readWorkspacePackages(repo);
      expect(pkgs.map((p) => p.name)).toEqual(["@intx/a", "@intx/b"]);
      expect(pkgs.find((p) => p.name === "@intx/a")?.internalDeps).toEqual([
        "@intx/b",
      ]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("unions @intx deps across dependencies, peer, and optional", () => {
    const repo = repoWith({
      a: {
        name: "@intx/a",
        version: "0.2.0",
        dependencies: { "@intx/b": "workspace:*", left: "^1.0.0" },
        peerDependencies: { "@intx/c": "workspace:*" },
        optionalDependencies: { "@intx/d": "workspace:*" },
      },
    });
    try {
      const [a] = readWorkspacePackages(repo);
      expect(a?.internalDeps.sort()).toEqual(["@intx/b", "@intx/c", "@intx/d"]);
      expect(a?.intxSpecs.map((s) => s.field).sort()).toEqual([
        "dependencies",
        "optionalDependencies",
        "peerDependencies",
      ]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("throws on a malformed manifest", () => {
    const repo = repoWith({ a: { version: "0.2.0" } }); // missing name
    try {
      expect(() => readWorkspacePackages(repo)).toThrow(/well-formed/);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });
});
