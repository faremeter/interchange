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
  checkWorkspaceExports,
  expectedConditions,
  fixWorkspaceExports,
} from "./exports-shape";

// Each fixture is a throwaway workspace: packages/<name>/package.json with
// an exports map. checkWorkspaceExports / fixWorkspaceExports run against it.

type PackageSpec = { name: string; private?: boolean; exports?: unknown };

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

function makeWorkspace(packages: PackageSpec[]): string {
  const root = mkdtempSync(join(tmpdir(), "exports-shape-"));
  roots.push(root);
  for (const [i, pkg] of packages.entries()) {
    const path = join(root, "packages", `p${i}`, "package.json");
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, JSON.stringify(pkg));
  }
  return root;
}

test("expectedConditions builds the three-condition shape for a file stem", () => {
  expect(expectedConditions("./src/index.ts")).toEqual({
    "intx-src": "./src/index.ts",
    types: "./dist/index.d.ts",
    default: "./dist/index.js",
  });
});

test("expectedConditions resolves a directory-barrel stem", () => {
  expect(expectedConditions("./src/runtime/index.ts")).toEqual({
    "intx-src": "./src/runtime/index.ts",
    types: "./dist/runtime/index.d.ts",
    default: "./dist/runtime/index.js",
  });
});

test("expectedConditions keeps intx-src, types, default in resolution order", () => {
  expect(Object.keys(expectedConditions("./src/index.ts"))).toEqual([
    "intx-src",
    "types",
    "default",
  ]);
});

test("expectedConditions rejects a non-source target", () => {
  expect(() => expectedConditions("./dist/index.js")).toThrow("./src/");
});

test("a canonical package produces no violations", async () => {
  const { violations } = await checkWorkspaceExports(
    makeWorkspace([
      {
        name: "@x/a",
        exports: {
          ".": {
            "intx-src": "./src/index.ts",
            types: "./dist/index.d.ts",
            default: "./dist/index.js",
          },
        },
      },
    ]),
  );
  expect(violations).toEqual([]);
});

test("a pre-flip two-key exports map is flagged", async () => {
  const { violations } = await checkWorkspaceExports(
    makeWorkspace([
      {
        name: "@x/a",
        exports: {
          ".": { types: "./src/index.ts", default: "./src/index.ts" },
        },
      },
    ]),
  );
  expect(violations.length).toBe(1);
  expect(violations[0]).toContain("@x/a");
});

test("a correctly-keyed but mis-ordered exports map is flagged", async () => {
  const { violations } = await checkWorkspaceExports(
    makeWorkspace([
      {
        name: "@x/a",
        exports: {
          ".": {
            default: "./dist/index.js",
            types: "./dist/index.d.ts",
            "intx-src": "./src/index.ts",
          },
        },
      },
    ]),
  );
  expect(violations.length).toBe(1);
});

test("a private package is not checked", async () => {
  const { violations, packageCount } = await checkWorkspaceExports(
    makeWorkspace([
      {
        name: "@x/private",
        private: true,
        exports: {
          ".": { types: "./src/index.ts", default: "./src/index.ts" },
        },
      },
    ]),
  );
  expect(violations).toEqual([]);
  expect(packageCount).toBe(0);
});

test("a subpath with no source target is flagged", async () => {
  const { violations } = await checkWorkspaceExports(
    makeWorkspace([
      {
        name: "@x/a",
        exports: {
          ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
        },
      },
    ]),
  );
  expect(violations.length).toBe(1);
  expect(violations[0]).toContain("no");
});

test("fix rewrites pre-flip exports to the canonical shape, then check passes", async () => {
  const root = makeWorkspace([
    {
      name: "@x/a",
      exports: {
        ".": { types: "./src/index.ts", default: "./src/index.ts" },
        "./sub": { types: "./src/sub/index.ts", default: "./src/sub/index.ts" },
      },
    },
    {
      name: "@x/private",
      private: true,
      exports: { ".": { types: "./src/index.ts", default: "./src/index.ts" } },
    },
  ]);
  const changed = await fixWorkspaceExports(root);
  expect(changed).toEqual(["@x/a"]);
  const rewritten = JSON.parse(
    readFileSync(join(root, "packages", "p0", "package.json"), "utf8"),
  );
  expect(rewritten.exports["."]).toEqual({
    "intx-src": "./src/index.ts",
    types: "./dist/index.d.ts",
    default: "./dist/index.js",
  });
  expect(rewritten.exports["./sub"]).toEqual({
    "intx-src": "./src/sub/index.ts",
    types: "./dist/sub/index.d.ts",
    default: "./dist/sub/index.js",
  });
  // The private package is untouched.
  const priv = JSON.parse(
    readFileSync(join(root, "packages", "p1", "package.json"), "utf8"),
  );
  expect(priv.exports["."]).toEqual({
    types: "./src/index.ts",
    default: "./src/index.ts",
  });
  const { violations } = await checkWorkspaceExports(root);
  expect(violations).toEqual([]);
});

test("fix is idempotent on an already-canonical workspace", async () => {
  const root = makeWorkspace([
    {
      name: "@x/a",
      exports: {
        ".": {
          "intx-src": "./src/index.ts",
          types: "./dist/index.d.ts",
          default: "./dist/index.js",
        },
      },
    },
  ]);
  expect(await fixWorkspaceExports(root)).toEqual([]);
});
