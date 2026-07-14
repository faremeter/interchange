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

import { rewriteDistTree, rewriteSpecifiers } from "./dist-rewrite";

// The pure core is exercised with an injected resolver that names the
// resolution it would make; the tree walker is exercised against a
// throwaway emitted tree on disk so the file-vs-directory resolution runs
// against real files.

/** A resolver that appends `.js` to every specifier it is asked about,
 *  standing in for the filesystem probe in the pure-core tests. */
const appendJs = (spec: string): string => `${spec}.js`;

test("a static import specifier is rewritten", () => {
  const out = rewriteSpecifiers(
    'import { a } from "./foo";\n',
    "x.js",
    appendJs,
  );
  expect(out).toBe('import { a } from "./foo.js";\n');
});

test("an `export * from` re-export is rewritten", () => {
  const out = rewriteSpecifiers('export * from "./foo";\n', "x.js", appendJs);
  expect(out).toBe('export * from "./foo.js";\n');
});

test("a named `export { a } from` re-export is rewritten", () => {
  const out = rewriteSpecifiers(
    'export { a } from "./foo";\n',
    "x.js",
    appendJs,
  );
  expect(out).toBe('export { a } from "./foo.js";\n');
});

test("a dynamic import() specifier is rewritten", () => {
  const out = rewriteSpecifiers(
    'const m = import("./foo");\n',
    "x.js",
    appendJs,
  );
  expect(out).toBe('const m = import("./foo.js");\n');
});

test("a .d.ts import() type node specifier is rewritten", () => {
  const out = rewriteSpecifiers(
    'export type T = import("./foo").Bar;\n',
    "x.d.ts",
    appendJs,
  );
  expect(out).toBe('export type T = import("./foo.js").Bar;\n');
});

test("a parent-directory relative specifier is rewritten", () => {
  const out = rewriteSpecifiers(
    'import { a } from "../foo";\n',
    "x.js",
    appendJs,
  );
  expect(out).toBe('import { a } from "../foo.js";\n');
});

test("a bare (external) specifier is left untouched", () => {
  const src =
    'import { a } from "@intx/agent";\nimport { b } from "arktype";\n';
  expect(rewriteSpecifiers(src, "x.js", appendJs)).toBe(src);
});

test("a node: builtin specifier is left untouched", () => {
  const src = 'import { readFileSync } from "node:fs";\n';
  expect(rewriteSpecifiers(src, "x.js", appendJs)).toBe(src);
});

test("an already-extensioned relative specifier is left untouched", () => {
  const src = 'import { a } from "./foo.js";\nimport data from "./d.json";\n';
  expect(rewriteSpecifiers(src, "x.js", appendJs)).toBe(src);
});

test("import-like text inside a string literal is not rewritten", () => {
  const src = "const s = 'import x from \"./tmpl\"';\nexport default s;\n";
  expect(rewriteSpecifiers(src, "x.js", appendJs)).toBe(src);
});

test("import-like text inside a comment is not rewritten", () => {
  const src = '// import x from "./cmt"\nexport const a = 1;\n';
  expect(rewriteSpecifiers(src, "x.js", appendJs)).toBe(src);
});

test("the original quote character is preserved", () => {
  const out = rewriteSpecifiers(
    "import { a } from './foo';\n",
    "x.js",
    appendJs,
  );
  expect(out).toBe("import { a } from './foo.js';\n");
});

test("a null resolution leaves the specifier in place", () => {
  const src = 'import { a } from "./foo";\n';
  expect(rewriteSpecifiers(src, "x.js", () => null)).toBe(src);
});

test("an `import x = require(...)` specifier is rewritten", () => {
  const out = rewriteSpecifiers(
    'import x = require("./foo");\n',
    "x.d.ts",
    appendJs,
  );
  expect(out).toBe('import x = require("./foo.js");\n');
});

test("multiple specifiers on one line are each rewritten", () => {
  const out = rewriteSpecifiers(
    'export { a } from "./foo"; export { b } from "./bar";\n',
    "x.js",
    appendJs,
  );
  expect(out).toBe(
    'export { a } from "./foo.js"; export { b } from "./bar.js";\n',
  );
});

// --- tree walker against a real emitted tree ---

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0))
    rmSync(root, { recursive: true, force: true });
});

function makeTree(files: Record<string, string>): string {
  const root = mkdtempSync(join(tmpdir(), "dist-rewrite-"));
  roots.push(root);
  for (const [rel, content] of Object.entries(files)) {
    const path = join(root, rel);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, content);
  }
  return root;
}

test("a sibling file resolves to `.js` and a directory barrel to `/index.js`", () => {
  const dir = makeTree({
    "index.js": 'export * from "./foo";\nimport { b } from "./bar";\n',
    "foo.js": "export const a = 1;\n",
    "bar/index.js": "export const b = 2;\n",
  });
  const report = rewriteDistTree(dir);
  expect(report.unresolved).toEqual([]);
  const out = readFileSync(join(dir, "index.js"), "utf8");
  expect(out).toContain('export * from "./foo.js";');
  expect(out).toContain('import { b } from "./bar/index.js";');
});

test("an unresolvable relative specifier is reported and left in place", () => {
  const dir = makeTree({
    "index.js": 'import { c } from "./missing";\n',
  });
  const report = rewriteDistTree(dir);
  expect(report.unresolved.length).toBe(1);
  expect(report.unresolved[0]).toContain("./missing");
  expect(readFileSync(join(dir, "index.js"), "utf8")).toBe(
    'import { c } from "./missing";\n',
  );
});

test("declaration files are rewritten alongside javascript", () => {
  const dir = makeTree({
    "index.d.ts":
      'export type T = import("./foo").Bar;\nexport * from "./foo";\n',
    "foo.js": "export const a = 1;\n",
    "foo.d.ts": "export type Bar = number;\n",
  });
  const report = rewriteDistTree(dir);
  expect(report.unresolved).toEqual([]);
  const out = readFileSync(join(dir, "index.d.ts"), "utf8");
  expect(out).toContain('import("./foo.js").Bar');
  expect(out).toContain('export * from "./foo.js";');
});
