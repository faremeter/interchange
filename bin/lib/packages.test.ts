import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";

import { readWorkspaceManifestPaths, readWorkspacePackages } from "./packages";

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

  test("derives import specifiers and optional peers from the manifest", () => {
    const repo = repoWith({
      log: {
        name: "@intx/log",
        version: "0.2.0",
        exports: {
          ".": { default: "./dist/index.js" },
          "./hono": { default: "./dist/hono.js" },
        },
        peerDependencies: { hono: "^4.0.0" },
        peerDependenciesMeta: { hono: { optional: true } },
      },
    });
    try {
      const log = readWorkspacePackages(repo).find(
        (p) => p.name === "@intx/log",
      );
      expect(log?.importSpecifiers).toEqual(["@intx/log", "@intx/log/hono"]);
      expect(log?.optionalPeers).toEqual([{ name: "hono", range: "^4.0.0" }]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("skips wildcard exports keys, which are not importable specifiers", () => {
    const repo = repoWith({
      a: {
        name: "@intx/a",
        version: "0.2.0",
        exports: {
          ".": { default: "./dist/index.js" },
          "./feat/*": { default: "./dist/feat/*.js" },
        },
      },
    });
    try {
      const a = readWorkspacePackages(repo).find((p) => p.name === "@intx/a");
      expect(a?.importSpecifiers).toEqual(["@intx/a"]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("a package with no exports has only its root specifier", () => {
    const repo = repoWith({ a: { name: "@intx/a", version: "0.2.0" } });
    try {
      const a = readWorkspacePackages(repo).find((p) => p.name === "@intx/a");
      expect(a?.importSpecifiers).toEqual(["@intx/a"]);
      expect(a?.optionalPeers).toEqual([]);
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

/** A repo with a root manifest declaring `workspaces`, plus a manifest at
 *  each `dir`. A `dir` mapped to `undefined` is created without a
 *  `package.json` (a directory that is not a member). */
function repoWithWorkspaces(
  workspaces: string[],
  members: Record<string, Record<string, unknown> | undefined>,
): string {
  const repo = mkdtempSync(join(tmpdir(), "manifest-paths-"));
  writeFileSync(
    join(repo, "package.json"),
    JSON.stringify({ name: "root", private: true, workspaces }),
  );
  for (const [dir, manifest] of Object.entries(members)) {
    const d = join(repo, dir);
    mkdirSync(d, { recursive: true });
    if (manifest !== undefined) {
      writeFileSync(join(d, "package.json"), JSON.stringify(manifest));
    }
  }
  return repo;
}

describe("readWorkspaceManifestPaths", () => {
  test("enumerates every member across all globs, private and tests/lib included", () => {
    const repo = repoWithWorkspaces(
      ["packages/*", "apps/*", "examples/*", "tests/lib"],
      {
        "packages/a": { name: "@intx/a" },
        "packages/secret": { name: "@intx/secret", private: true },
        "apps/ui": { name: "@intx/ui", private: true },
        "examples/demo": { name: "@intx/demo", private: true },
        "tests/lib": { name: "@intx/test-harness", private: true },
      },
    );
    try {
      expect(readWorkspaceManifestPaths(repo)).toEqual(
        [
          join(repo, "packages/a/package.json"),
          join(repo, "packages/secret/package.json"),
          join(repo, "apps/ui/package.json"),
          join(repo, "examples/demo/package.json"),
          join(repo, "tests/lib/package.json"),
        ].sort(),
      );
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("skips a glob-matched directory that has no package.json", () => {
    const repo = repoWithWorkspaces(["packages/*"], {
      "packages/a": { name: "@intx/a" },
      "packages/empty": undefined,
    });
    try {
      expect(readWorkspaceManifestPaths(repo)).toEqual([
        join(repo, "packages/a/package.json"),
      ]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("de-duplicates a member matched by overlapping globs", () => {
    const repo = repoWithWorkspaces(["packages/*", "packages/a"], {
      "packages/a": { name: "@intx/a" },
    });
    try {
      expect(readWorkspaceManifestPaths(repo)).toEqual([
        join(repo, "packages/a/package.json"),
      ]);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });

  test("throws when the root manifest has no workspaces array", () => {
    const repo = mkdtempSync(join(tmpdir(), "manifest-paths-"));
    writeFileSync(
      join(repo, "package.json"),
      JSON.stringify({ name: "root", private: true }),
    );
    try {
      expect(() => readWorkspaceManifestPaths(repo)).toThrow(/workspaces/);
    } finally {
      rmSync(repo, { recursive: true, force: true });
    }
  });
});
