import { afterEach, expect, test } from "bun:test";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import {
  checkWorkspaceDescriptions,
  checkWorkspaceMetadata,
  expectedFiles,
  fixWorkspaceMetadata,
} from "./publish-metadata";

type PackageSpec = Record<string, unknown>;

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

// The `packages` array is placed under `packages/pN`; `extra` places a
// manifest at an explicit workspace-relative dir (e.g. `apps/ui`,
// `tests/lib`) so the description check — which enumerates every member the
// root `workspaces` globs declare — can be exercised across all member
// kinds. A root manifest with those globs is always written, since
// `checkWorkspaceDescriptions` derives its member set from it.
function makeWorkspace(
  packages: PackageSpec[],
  extra: Record<string, PackageSpec> = {},
): string {
  const root = mkdtempSync(join(tmpdir(), "publish-metadata-"));
  roots.push(root);
  writeFileSync(
    join(root, "package.json"),
    JSON.stringify({
      name: "root",
      private: true,
      workspaces: ["packages/*", "apps/*", "examples/*", "tests/lib"],
    }),
  );
  for (const [i, pkg] of packages.entries()) {
    const path = join(root, "packages", `p${i}`, "package.json");
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, JSON.stringify(pkg));
  }
  for (const [dir, pkg] of Object.entries(extra)) {
    const path = join(root, dir, "package.json");
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, JSON.stringify(pkg));
  }
  return root;
}

// Create empty files under `packages/pN` so a `sideEffects` glob has
// something to match on disk.
function seedPackageFiles(root: string, index: number, relPaths: string[]) {
  for (const rel of relPaths) {
    const path = join(root, "packages", `p${index}`, rel);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, "");
  }
}

const canonical = (
  name: string,
  sideEffects: false | string[] = false,
): Record<string, unknown> => ({
  name,
  files: expectedFiles(name),
  sideEffects,
  publishConfig: { access: "public" },
});

test("expectedFiles adds package-root data dirs for db and inference-discovery", () => {
  expect(expectedFiles("@intx/mime")).toEqual(["dist", "README.md", "LICENSE"]);
  expect(expectedFiles("@intx/db")).toEqual([
    "dist",
    "migrations",
    "README.md",
    "LICENSE",
  ]);
  expect(expectedFiles("@intx/inference-discovery")).toEqual([
    "dist",
    "media",
    "README.md",
    "LICENSE",
  ]);
});

test("a fully-set package produces no violations", async () => {
  const root = makeWorkspace([
    canonical("@x/a"),
    canonical("@x/side", ["./src/index.ts", "./dist/index.js"]),
  ]);
  seedPackageFiles(root, 1, ["src/index.ts"]);
  const { violations } = await checkWorkspaceMetadata(root);
  expect(violations).toEqual([]);
});

test("a missing files allowlist is flagged", async () => {
  const pkg = canonical("@x/a");
  delete pkg["files"];
  const { violations } = await checkWorkspaceMetadata(makeWorkspace([pkg]));
  expect(violations.some((v) => v.includes("files"))).toBe(true);
});

test("a missing or wrong publishConfig.access is flagged", async () => {
  const pkg = canonical("@x/a");
  pkg["publishConfig"] = { access: "restricted" };
  const { violations } = await checkWorkspaceMetadata(makeWorkspace([pkg]));
  expect(violations.some((v) => v.includes("publishConfig"))).toBe(true);
});

test("any package may declare its own non-false sideEffects", async () => {
  const root = makeWorkspace([
    canonical("@x/registry", ["./src/register.ts", "./dist/register.js"]),
  ]);
  seedPackageFiles(root, 0, ["src/register.ts"]);
  const { violations } = await checkWorkspaceMetadata(root);
  expect(violations).toEqual([]);
});

test("a sideEffects array of only unshipped paths is flagged", async () => {
  // `./src/*` matches on disk at lint time but is absent from the published
  // tarball (files ships `dist`), so a source-only declaration would be
  // silently tree-shaken. At least one entry must be under a shipped path.
  const root = makeWorkspace([canonical("@x/a", ["./src/index.ts"])]);
  seedPackageFiles(root, 0, ["src/index.ts"]);
  const { violations } = await checkWorkspaceMetadata(root);
  expect(violations.some((v) => v.includes("only unshipped modules"))).toBe(
    true,
  );
});

test("a sideEffects glob matching no file and no shipped path is flagged", async () => {
  const root = makeWorkspace([canonical("@x/a", ["./src/nope.ts"])]);
  const { violations } = await checkWorkspaceMetadata(root);
  expect(violations.some((v) => v.includes("./src/nope.ts"))).toBe(true);
});

test("a log-shaped src-and-dist array passes with dist unbuilt", async () => {
  const root = makeWorkspace([
    canonical("@x/logish", [
      "./src/index.ts",
      "./src/hono.ts",
      "./src/default-sink.ts",
      "./dist/index.js",
      "./dist/hono.js",
      "./dist/default-sink.js",
    ]),
  ]);
  // Source present, `dist` shipped via `files` but unbuilt — as at lint time.
  seedPackageFiles(root, 0, [
    "src/index.ts",
    "src/hono.ts",
    "src/default-sink.ts",
  ]);
  expect((await checkWorkspaceMetadata(root)).violations).toEqual([]);
});

test("a dist glob is flagged when the package does not ship dist", async () => {
  const { violations } = await checkWorkspaceMetadata(
    makeWorkspace([
      {
        name: "@x/a",
        files: ["README.md", "LICENSE"],
        sideEffects: ["./dist/index.js"],
        publishConfig: { access: "public" },
      },
    ]),
  );
  expect(violations.some((v) => v.includes("./dist/index.js"))).toBe(true);
});

test("a glob is flagged when files is absent, shipping nothing", async () => {
  const { violations } = await checkWorkspaceMetadata(
    makeWorkspace([
      {
        name: "@x/a",
        sideEffects: ["./dist/index.js"],
        publishConfig: { access: "public" },
      },
    ]),
  );
  expect(violations.some((v) => v.includes("./dist/index.js"))).toBe(true);
});

test("an absent sideEffects is flagged", async () => {
  const pkg = canonical("@x/a");
  delete pkg["sideEffects"];
  const { violations } = await checkWorkspaceMetadata(makeWorkspace([pkg]));
  expect(violations.some((v) => v.includes("sideEffects"))).toBe(true);
});

test("a malformed sideEffects is flagged", async () => {
  for (const bad of [true, [], [123], "false"]) {
    const pkg = canonical("@x/a");
    pkg["sideEffects"] = bad;
    const { violations } = await checkWorkspaceMetadata(makeWorkspace([pkg]));
    expect(violations.some((v) => v.includes("sideEffects"))).toBe(true);
  }
});

test("a private package is not checked", async () => {
  const { violations, packageCount } = await checkWorkspaceMetadata(
    makeWorkspace([{ name: "@x/private", private: true }]),
  );
  expect(violations).toEqual([]);
  expect(packageCount).toBe(0);
});

test("fix sets mechanical fields, seeds sideEffects false, then check passes", async () => {
  const root = makeWorkspace([
    { name: "@x/a" },
    { name: "@x/private", private: true },
  ]);
  const changed = await fixWorkspaceMetadata(root);
  expect(changed).toEqual(["@x/a"]);
  const a = JSON.parse(
    readFileSync(join(root, "packages", "p0", "package.json"), "utf8"),
  );
  expect(a.files).toEqual(expectedFiles("@x/a"));
  expect(a.sideEffects).toBe(false);
  expect(a.publishConfig).toEqual({ access: "public" });
  // Private package untouched.
  const priv = JSON.parse(
    readFileSync(join(root, "packages", "p1", "package.json"), "utf8"),
  );
  expect(priv.files).toBeUndefined();
  expect((await checkWorkspaceMetadata(root)).violations).toEqual([]);
});

test("fix never overwrites an existing sideEffects glob list", async () => {
  const globs = ["./src/register.ts", "./dist/register.js"];
  const root = makeWorkspace([{ name: "@x/registry", sideEffects: globs }]);
  const changed = await fixWorkspaceMetadata(root);
  expect(changed).toEqual(["@x/registry"]);
  const m = JSON.parse(
    readFileSync(join(root, "packages", "p0", "package.json"), "utf8"),
  );
  // The hand-authored glob list survives; the mechanical fields are set.
  expect(m.sideEffects).toEqual(globs);
  expect(m.files).toEqual(expectedFiles("@x/registry"));
  expect(m.publishConfig).toEqual({ access: "public" });
});

test("fix is idempotent on an already-canonical workspace", async () => {
  const root = makeWorkspace([canonical("@x/a")]);
  expect(await fixWorkspaceMetadata(root)).toEqual([]);
});

test("a member with a non-empty description passes", async () => {
  const { violations, manifestCount } = await checkWorkspaceDescriptions(
    makeWorkspace([{ name: "@x/a", description: "does a thing" }]),
  );
  expect(violations).toEqual([]);
  expect(manifestCount).toBe(1);
});

test("a missing description is flagged", async () => {
  const { violations } = await checkWorkspaceDescriptions(
    makeWorkspace([{ name: "@x/a" }]),
  );
  expect(violations).toEqual([
    '@x/a: "description" must be a non-empty string',
  ]);
});

test("an empty or whitespace-only description is flagged", async () => {
  const { violations } = await checkWorkspaceDescriptions(
    makeWorkspace([
      { name: "@x/empty", description: "" },
      { name: "@x/blank", description: "   " },
    ]),
  );
  expect(violations.some((v) => v.startsWith("@x/empty:"))).toBe(true);
  expect(violations.some((v) => v.startsWith("@x/blank:"))).toBe(true);
});

test("a non-string description is flagged", async () => {
  const { violations } = await checkWorkspaceDescriptions(
    makeWorkspace([{ name: "@x/a", description: 123 }]),
  );
  expect(violations).toEqual([
    '@x/a: "description" must be a non-empty string',
  ]);
});

test("the description check covers private members and every workspace dir", async () => {
  const privateMember = (name: string): PackageSpec => ({
    name,
    private: true,
  });
  const { violations, manifestCount } = await checkWorkspaceDescriptions(
    makeWorkspace([{ name: "@x/pkg", private: true }], {
      "apps/ui": privateMember("@x/app"),
      "examples/e": privateMember("@x/example"),
      "tests/lib": privateMember("@x/harness"),
    }),
  );
  // Four members, all private, none with a description — the description
  // check does not skip private members the way the tarball-field check does.
  expect(manifestCount).toBe(4);
  expect(violations.map((v) => v.split(":")[0]).sort()).toEqual([
    "@x/app",
    "@x/example",
    "@x/harness",
    "@x/pkg",
  ]);
});

test("a private member is skipped by the metadata check but not the description check", async () => {
  const root = makeWorkspace([
    { name: "@x/private", private: true, description: "a private member" },
  ]);
  // The tarball-field check ignores the private package entirely.
  expect((await checkWorkspaceMetadata(root)).packageCount).toBe(0);
  // The description check still counts it, and it passes because it has one.
  const { violations, manifestCount } = await checkWorkspaceDescriptions(root);
  expect(manifestCount).toBe(1);
  expect(violations).toEqual([]);
});
