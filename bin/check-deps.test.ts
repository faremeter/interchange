import { afterEach, expect, test } from "bun:test";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { checkWorkspace } from "./check-deps";

// Each fixture is a throwaway workspace on disk: a root package.json carrying
// the catalog and a `workspaces` array derived from the members it declares,
// plus those members with their manifests, source files, and optional
// tsconfig. checkWorkspace enumerates members through the root `workspaces`
// globs and runs the static checks (phantom imports + catalog convergence)
// against them; files under `bin`/`tests` that are not members are checked
// against the root manifest.

type MemberSpec = {
  manifest: Record<string, unknown>;
  files?: Record<string, string>;
  tsconfig?: Record<string, unknown>;
};
type WorkspaceSpec = {
  catalog?: Record<string, string>;
  rootDeps?: Record<string, string>;
  members?: Record<string, MemberSpec>;
  // Files written relative to the workspace root, e.g. "bin/x.ts",
  // "tests/y.test.ts" — the non-member trees the guard checks against root.
  extraFiles?: Record<string, string>;
};

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

function writeFile(root: string, rel: string, content: string): void {
  const path = join(root, rel);
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, content);
}

function makeWorkspace(spec: WorkspaceSpec): string {
  const root = mkdtempSync(join(tmpdir(), "check-deps-"));
  roots.push(root);
  // The root manifest declares exactly the members the fixture writes, as
  // literal `workspaces` entries — the same single source of truth
  // `checkWorkspace` reads via `readWorkspaceManifestPaths`. Literal entries
  // (rather than `<dir>/*` globs) mirror how the real root lists `tests/lib`
  // and let a fixture place a non-member directory beside a member — e.g. a
  // non-member `tests/db` next to member `tests/lib` — the shape that
  // matters for the root-scripts exclusion. A memberless fixture gets `[]`,
  // which the enumerator accepts (it just finds no members).
  const workspaces = Object.keys(spec.members ?? {});
  writeFile(
    root,
    "package.json",
    JSON.stringify({
      name: "@fixture/root",
      workspaces,
      catalog: spec.catalog ?? {},
      devDependencies: spec.rootDeps ?? {},
    }),
  );
  for (const [memberPath, member] of Object.entries(spec.members ?? {})) {
    writeFile(
      root,
      `${memberPath}/package.json`,
      JSON.stringify(member.manifest),
    );
    if (member.tsconfig)
      writeFile(
        root,
        `${memberPath}/tsconfig.json`,
        JSON.stringify(member.tsconfig),
      );
    for (const [rel, content] of Object.entries(member.files ?? {}))
      writeFile(root, `${memberPath}/${rel}`, content);
  }
  for (const [rel, content] of Object.entries(spec.extraFiles ?? {}))
    writeFile(root, rel, content);
  return root;
}

async function violationsFor(spec: WorkspaceSpec): Promise<string[]> {
  return (await checkWorkspace(makeWorkspace(spec))).violations;
}

test("a clean workspace produces no violations", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a", dependencies: { lodash: "^4" } },
        files: { "src/i.ts": 'import { merge } from "lodash";\n' },
      },
    },
  });
  expect(v).toEqual([]);
});

test("a source parse failure identifies the workspace member", async () => {
  const root = makeWorkspace({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: { "src/index.ts": "export {};" },
      },
      "packages/b": {
        manifest: { name: "@x/b" },
        files: { "src/index.ts": "import {" },
      },
    },
  });
  await expect(checkWorkspace(root)).rejects.toThrow(
    "Cannot parse packages/b/src/index.ts:",
  );
});

test("an undeclared value import is flagged", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: { "src/i.ts": 'import { x } from "undeclared-pkg";\n' },
      },
    },
  });
  expect(v.some((m) => m.includes("undeclared-pkg"))).toBe(true);
});

test("an undeclared inline type-only import is flagged", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: { "src/i.ts": 'import { type T } from "undeclared-type";\n' },
      },
    },
  });
  expect(v.some((m) => m.includes("undeclared-type"))).toBe(true);
});

test("an undeclared `typeof import()` reference is flagged", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: {
          "src/i.ts": 'export type S = typeof import("undeclared-typeof");\n',
        },
      },
    },
  });
  expect(v.some((m) => m.includes("undeclared-typeof"))).toBe(true);
});

test("a declared import is not flagged", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a", devDependencies: { tar: "^7" } },
        files: { "src/i.test.ts": 'import { x } from "tar";\n' },
      },
    },
  });
  expect(v).toEqual([]);
});

test("an augmentation target must be declared by the member even when root declares it", async () => {
  const v = await violationsFor({
    rootDeps: { "http-core": "^1" },
    members: {
      "packages/plugin": {
        manifest: { name: "@x/plugin", dependencies: { framework: "^1" } },
        files: {
          "src/index.ts": `import type { Request } from "framework";
export type PluginRequest = Request;
declare module "http-core" { interface Request { plugin?: string } }
`,
        },
      },
    },
  });
  expect(v).toEqual([
    '@x/plugin: imports "http-core" (packages/plugin/src/index.ts) but does not declare it in package.json',
  ]);
});

test("inherited forced module detection counts source augmentations but not ambient definitions", async () => {
  const v = await violationsFor({
    members: {
      "packages/plugin": {
        manifest: { name: "@x/plugin" },
        tsconfig: { extends: "../../tsconfig.base.json" },
        files: {
          "src/plugin.ts": 'declare module "http-core" {}',
          "src/ambient.d.ts": 'declare module "ambient" {}',
        },
      },
    },
    extraFiles: {
      "tsconfig.base.json": JSON.stringify({
        compilerOptions: { moduleDetection: "force" },
      }),
    },
  });
  expect(v).toEqual([
    '@x/plugin: imports "http-core" (packages/plugin/src/plugin.ts) but does not declare it in package.json',
  ]);
});

test("a bodyless ambient declaration does not abort dependency scanning", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: {
          "src/types.d.ts": 'declare module "untyped-lib";',
          "src/index.ts": 'import "undeclared";',
        },
      },
    },
  });
  expect(v).toEqual([
    '@x/a: imports "undeclared" (packages/a/src/index.ts) but does not declare it in package.json',
  ]);
});

test("a subpath import resolves to its package name", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a", dependencies: { lodash: "^4" } },
        files: { "src/i.ts": 'import merge from "lodash/merge";\n' },
      },
    },
  });
  expect(v).toEqual([]);
});

test("a wildcard tsconfig path alias is excluded", async () => {
  const v = await violationsFor({
    members: {
      "apps/ui": {
        manifest: { name: "@x/ui" },
        tsconfig: { compilerOptions: { paths: { "@/*": ["./src/*"] } } },
        files: { "src/i.ts": 'import { x } from "@/widgets";\n' },
      },
    },
  });
  expect(v).toEqual([]);
});

test("an exact (non-wildcard) alias does not suppress a longer package name", async () => {
  const v = await violationsFor({
    members: {
      "apps/ui": {
        manifest: { name: "@x/ui" },
        tsconfig: { compilerOptions: { paths: { react: ["./src/react.ts"] } } },
        files: {
          "src/exact.ts": 'import x from "react";\n',
          "src/longer.ts": 'import x from "react-dom";\n',
        },
      },
    },
  });
  // "react" matches the exact alias and is excluded; "react-dom" does not.
  expect(v.some((m) => m.includes('"react-dom"'))).toBe(true);
  expect(v.some((m) => m.includes('"react"'))).toBe(false);
});

test("emitted output under dist/ and the tsconfig outDir is not scanned", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        tsconfig: { compilerOptions: { outDir: "./build" } },
        files: {
          "dist/gen.ts": 'import x from "phantom-dist";\n',
          "build/gen.ts": 'import x from "phantom-outdir";\n',
          "src/redist/keep.ts": 'import x from "phantom-redist";\n',
        },
      },
    },
  });
  // dist/ and the outDir (build/) are skipped; redist/ is real source.
  expect(v.some((m) => m.includes("phantom-dist"))).toBe(false);
  expect(v.some((m) => m.includes("phantom-outdir"))).toBe(false);
  expect(v.some((m) => m.includes("phantom-redist"))).toBe(true);
});

test("JSONC extends preserves aliases and resolves inherited output relative to its config", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        tsconfig: { extends: "../../tsconfig.base.json" },
        files: {
          "src/index.ts": 'import { value } from "@/value";\n',
          "build/index.js": 'import value from "emitted-only";\n',
          "src/real.ts": 'import value from "undeclared";\n',
        },
      },
    },
    extraFiles: {
      "tsconfig.base.json": `{
        // The paths below are relative to the base config.
        "compilerOptions": {
          "outDir": "./packages/a/build",
          "paths": { "@/*": ["./packages/a/src/*"] },
        },
      }`,
    },
  });
  expect(v).toEqual([
    '@x/a: imports "undeclared" (packages/a/src/real.ts) but does not declare it in package.json',
  ]);
});

test("an unresolved tsconfig extends target fails the dependency scan", async () => {
  const root = makeWorkspace({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        tsconfig: { extends: "./missing.json" },
      },
    },
  });
  await expect(checkWorkspace(root)).rejects.toThrow("cannot resolve");
});

test("a backtick dynamic import of an undeclared package is a violation", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: { "src/index.ts": "await import(`undeclared`);\n" },
      },
    },
  });
  expect(v).toEqual([
    '@x/a: imports "undeclared" (packages/a/src/index.ts) but does not declare it in package.json',
  ]);
});

test("import-like text inside a template literal is not a real import", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a" },
        files: {
          "src/i.ts":
            'const s = `import x from "tmpl-only";`;\nexport default s;\n',
        },
      },
    },
  });
  expect(v.some((m) => m.includes("tmpl-only"))).toBe(false);
});

test("an external dependency used by two runtime members must be catalog:", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a", dependencies: { shared: "^1" } },
      },
      "packages/b": {
        manifest: { name: "@x/b", dependencies: { shared: "^1" } },
      },
    },
  });
  expect(v.some((m) => m.includes('"shared"') && m.includes("catalog:"))).toBe(
    true,
  );
});

test("a shared dependency referenced as catalog: is accepted", async () => {
  const v = await violationsFor({
    catalog: { shared: "^1" },
    members: {
      "packages/a": {
        manifest: { name: "@x/a", dependencies: { shared: "catalog:" } },
      },
      "packages/b": {
        manifest: { name: "@x/b", dependencies: { shared: "catalog:" } },
      },
    },
  });
  expect(v).toEqual([]);
});

test("a named catalog reference is accepted", async () => {
  const v = await violationsFor({
    catalog: { shared: "^1" },
    members: {
      "packages/a": {
        manifest: { name: "@x/a", dependencies: { shared: "catalog:named" } },
      },
      "packages/b": {
        manifest: { name: "@x/b", dependencies: { shared: "catalog:named" } },
      },
    },
  });
  expect(v).toEqual([]);
});

test("a peer-only shared dependency is not forced onto the catalog", async () => {
  const v = await violationsFor({
    members: {
      "packages/a": {
        manifest: { name: "@x/a", peerDependencies: { wide: "^4" } },
      },
      "packages/b": {
        manifest: { name: "@x/b", peerDependencies: { wide: "^4" } },
      },
    },
  });
  expect(v).toEqual([]);
});

test("a catalog entry with fewer than two consumers is flagged", async () => {
  const v = await violationsFor({
    catalog: { orphan: "^1" },
    members: {
      "packages/a": { manifest: { name: "@x/a" } },
    },
  });
  expect(v.some((m) => m.includes("orphan") && m.includes("at least 2"))).toBe(
    true,
  );
});

test("a malformed manifest raises rather than misbehaving", async () => {
  const root = makeWorkspace({
    members: {
      "packages/a": { manifest: { name: "@x/a", dependencies: ["oops"] } },
    },
  });
  let caught: unknown;
  try {
    await checkWorkspace(root);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(Error);
  expect(String(caught)).toContain("well-formed manifest");
});

test("a bin/ script importing an external undeclared in root is flagged", async () => {
  const v = await violationsFor({
    extraFiles: { "bin/tool.ts": 'import { x } from "undeclared-tool-dep";\n' },
  });
  expect(
    v.some(
      (m) =>
        m.includes("undeclared-tool-dep") && m.includes("root package.json"),
    ),
  ).toBe(true);
});

test("a tests/ file importing an external undeclared in root is flagged", async () => {
  const v = await violationsFor({
    extraFiles: {
      "tests/x.test.ts": 'import { x } from "undeclared-spec-dep";\n',
    },
  });
  expect(v.some((m) => m.includes("undeclared-spec-dep"))).toBe(true);
});

test("root-script augmentations use the source directory's module detection setting", async () => {
  const v = await violationsFor({
    extraFiles: {
      "tests/plugin/tsconfig.json": JSON.stringify({
        compilerOptions: { moduleDetection: "force" },
      }),
      "tests/plugin/augment.ts": 'declare module "http-core" {}',
      "tests/plugin/ambient.d.ts": 'declare module "ambient" {}',
    },
  });
  expect(v).toEqual([
    'tests/plugin/augment.ts: imports "http-core" but the root package.json does not declare it',
  ]);
});

test("a bin/ import declared in the root manifest is accepted", async () => {
  const v = await violationsFor({
    rootDeps: { "some-tool": "^1" },
    extraFiles: { "bin/tool.ts": 'import { x } from "some-tool";\n' },
  });
  expect(v).toEqual([]);
});

test("a bin/ or tests/ import of a workspace package is accepted", async () => {
  const v = await violationsFor({
    members: { "packages/a": { manifest: { name: "@x/a" } } },
    extraFiles: {
      "bin/tool.ts": 'import { x } from "@x/a";\n',
      "tests/x.test.ts": 'import { y } from "@x/a/sub";\n',
    },
  });
  expect(v).toEqual([]);
});

test("a bin/ import of the bun runtime module is accepted", async () => {
  const v = await violationsFor({
    extraFiles: { "bin/tool.ts": 'import { Glob } from "bun";\n' },
  });
  expect(v).toEqual([]);
});

test("a workspace member under tests/ is checked against its own manifest, not twice against root", async () => {
  const v = await violationsFor({
    members: {
      "tests/lib": {
        manifest: { name: "@x/harness" },
        files: { "h.ts": 'import { x } from "member-undeclared";\n' },
      },
    },
  });
  // Exactly one violation, in the member shape. Without the member-subtree
  // exclusion the root-scripts scan would add a second, root-shaped
  // violation for the same import.
  expect(v).toEqual([
    '@x/harness: imports "member-undeclared" (tests/lib/h.ts) but does not declare it in package.json',
  ]);
});

test("a member under tests/ importing an external it declares is not re-flagged against root", async () => {
  const v = await violationsFor({
    members: {
      "tests/lib": {
        manifest: {
          name: "@x/harness",
          dependencies: { "only-in-member": "^1" },
        },
        files: { "h.ts": 'import { x } from "only-in-member";\n' },
      },
    },
  });
  // "only-in-member" is declared in the member's own manifest and absent
  // from root. As a member it is accepted; without the exclusion the
  // root-scripts pass would wrongly flag it as undeclared in root.
  expect(v).toEqual([]);
});

test("a non-member directory whose name is a prefix of a member is still checked against root", async () => {
  const v = await violationsFor({
    members: {
      "tests/lib": { manifest: { name: "@x/harness" } },
    },
    extraFiles: {
      "tests/library/x.ts": 'import { x } from "undeclared-sibling-dep";\n',
    },
  });
  // tests/library is not tests/lib. A boundary-matched exclusion must not
  // swallow it, so its import is validated against the root manifest. A
  // naive prefix check (without the trailing slash) would wrongly exclude it.
  expect(
    v.some(
      (m) =>
        m.includes("undeclared-sibling-dep") && m.includes("root package.json"),
    ),
  ).toBe(true);
});

test("a non-member directory with its own package.json under tests/ is checked against root", async () => {
  const v = await violationsFor({
    members: {
      "tests/lib": { manifest: { name: "@x/harness" } },
    },
    // tests/db has a manifest but is not in `workspaces`, mirroring the real
    // repo where tests/lib is the only member under tests/. It is a
    // non-member, so its files resolve against the root manifest.
    extraFiles: {
      "tests/db/package.json": JSON.stringify({ name: "@x/db-tests" }),
      "tests/db/x.test.ts": 'import { x } from "undeclared-nonmember-dep";\n',
    },
  });
  expect(
    v.some(
      (m) =>
        m.includes("undeclared-nonmember-dep") &&
        m.includes("root package.json"),
    ),
  ).toBe(true);
});
