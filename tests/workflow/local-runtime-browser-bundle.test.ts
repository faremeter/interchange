// A browser caller can bundle the local runtime: `runLocal`, the real step
// invoker, the harness credential capabilities and browser storage. This is
// also the browser check for `@intx/storage-isogit/browser`.
//
// Bun's browser target does not fail on a Node builtin: it polyfills some
// (`node:path`), silently stubs others (`node:fs`, `node:child_process`), and
// errors only on a missing named export. So the test records every builtin
// specifier the bundle resolves, through an `onResolve` hook, rather than
// searching the output for import statements that the target never emits.
//
// Every builtin is refused except the reviewed pairs in `ALLOWED`, which Bun
// serves with a complete browser polyfill. The hook sees static, CommonJS and
// literal dynamic imports. It does not see a dynamic import of a computed
// specifier, or a use of the `process` or `Buffer` globals. Code the entry
// reaches guards `process`; MIME header encoding in `@intx/mime` uses
// `Buffer`, so a browser caller installs one, as
// `@intx/storage-isogit/browser` does.

import { builtinModules } from "node:module";
import path from "node:path";

import { expect, test } from "bun:test";

const NODE_BUILTINS = new Set(builtinModules);

// Builtin imports the bundle accepts, by importing module and specifier.
// `@intx/storage-isogit`'s browser runtime and `safe-buffer` (an
// isomorphic-git dependency) import `buffer`, which Bun serves from its
// browser polyfill. `inherits` resolves to its browser file when this test
// runs alone, but when the test process has already loaded `inherits` under
// the Node conditions, Bun's resolver returns its Node file, which requires
// `util` inside a try/catch; Bun serves that from its polyfill too.
const ALLOWED: readonly { importer: string; specifier: string }[] = [
  {
    importer: "packages/storage-isogit/src/browser-runtime.ts",
    specifier: "buffer",
  },
  { importer: "node_modules/safe-buffer/index.js", specifier: "buffer" },
  { importer: "node_modules/inherits/inherits.js", specifier: "util" },
];

function isRuntimeBuiltin(specifier: string): boolean {
  return (
    specifier.startsWith("node:") ||
    specifier.startsWith("bun:") ||
    specifier === "bun" ||
    NODE_BUILTINS.has(specifier)
  );
}

function isAllowed(specifier: string, importer: string): boolean {
  return ALLOWED.some(
    (entry) =>
      entry.specifier === specifier &&
      importer.endsWith(
        `${path.sep}${entry.importer.split("/").join(path.sep)}`,
      ),
  );
}

// Bundle `entry` for a browser and return each builtin import it resolves
// that `ALLOWED` does not name.
async function refusedBuiltins(
  entry: string,
): Promise<{ specifier: string; importer: string }[]> {
  const refused: { specifier: string; importer: string }[] = [];
  const result = await Bun.build({
    entrypoints: [path.join(import.meta.dir, "fixtures", entry)],
    target: "browser",
    format: "esm",
    conditions: ["intx-src"],
    plugins: [
      {
        name: "record-runtime-builtins",
        setup(build) {
          build.onResolve({ filter: /.*/ }, (args) => {
            if (
              isRuntimeBuiltin(args.path) &&
              !isAllowed(args.path, args.importer)
            ) {
              refused.push({ specifier: args.path, importer: args.importer });
            }
            return undefined;
          });
        },
      },
    ],
  });
  expect(result.outputs).toHaveLength(1);
  return refused;
}

test("the builtin recorder sees static, CommonJS and literal dynamic Node imports", async () => {
  const refused = [
    ...(await refusedBuiltins("browser-builtin-imports.ts")),
    ...(await refusedBuiltins("browser-builtin-require.ts")),
  ];
  expect(refused.map((entry) => entry.specifier).sort()).toEqual([
    "node:fs",
    "node:fs/promises",
    "node:os",
    "node:path",
  ]);
});

test("the local runtime bundles for a browser with no Node or Bun builtin", async () => {
  expect(await refusedBuiltins("browser-local-runtime.ts")).toEqual([]);
});
