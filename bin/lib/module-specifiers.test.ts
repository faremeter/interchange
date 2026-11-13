import { expect, test } from "bun:test";
import { collectModuleSpecifiers } from "./module-specifiers";

test("dependency scans retain runtime and type-only module references", () => {
  const code = `
import value from "default-import";
import type { T } from "type-import";
import { type U } from "inline-type-import";
export * as ns from "namespace-export";
export type { V } from "type-export";
const dynamic = import("dynamic-import");
const common = require("commonjs-import");
import legacy = require("import-equals");
type Imported = import("import-type").T;
type Module = typeof import("typeof-import");
declare module "augmented-module" {}
// import "comment";
const text = 'require("string")';
const template = \`import "template"\`;
`;
  expect(
    collectModuleSpecifiers(code, "source.ts", "auto").map((ref) => ref.text),
  ).toEqual([
    "default-import",
    "type-import",
    "inline-type-import",
    "namespace-export",
    "type-export",
    "dynamic-import",
    "commonjs-import",
    "import-equals",
    "import-type",
    "typeof-import",
    "augmented-module",
  ]);
});

test.each(["types.d.ts", "types.d.mts", "types.d.cts"])(
  "ambient module definitions in %s stay excluded under forced module detection",
  (fileName) => {
    expect(
      collectModuleSpecifiers(
        'declare module "ambient" { export interface T {} }',
        fileName,
        "force",
      ),
    ).toEqual([]);
  },
);

test("augmentation targets are distinguished from imports", () => {
  const refs = collectModuleSpecifiers(
    'import framework = require("framework"); declare module "http-core" {}',
    "plugin.ts",
    "auto",
  );
  expect(refs.map((ref) => [ref.text, ref.kind])).toEqual([
    ["framework", "import"],
    ["http-core", "augmentation"],
  ]);
});

test("module detection controls declarations in files without import or export syntax", () => {
  const source = 'declare module "http-core" {}';
  expect(collectModuleSpecifiers(source, "plugin.ts", "auto")).toEqual([]);
  expect(collectModuleSpecifiers(source, "plugin.ts", "force")).toMatchObject([
    { text: "http-core", kind: "augmentation" },
  ]);
  for (const fileName of ["plugin.mts", "plugin.cts"]) {
    expect(collectModuleSpecifiers(source, fileName, "auto")).toMatchObject([
      { text: "http-core", kind: "augmentation" },
    ]);
    expect(collectModuleSpecifiers(source, fileName, "legacy")).toEqual([]);
  }
});

test("a module nested in an ambient definition is an augmentation", () => {
  expect(
    collectModuleSpecifiers(
      'declare module "framework" { module "http-core" {} }',
      "types.d.ts",
      "auto",
    ).map((ref) => [ref.text, ref.kind]),
  ).toEqual([["http-core", "augmentation"]]);
});

test("TSX dependency scans ignore JSX text but inspect expressions", () => {
  const code = `export const view = <div>require("text"){import("real")}</div>;`;
  expect(
    collectModuleSpecifiers(code, "view.tsx", "auto").map((ref) => ref.text),
  ).toEqual(["real"]);
});

test("an invalid source fails the scan instead of losing dependencies", () => {
  expect(() =>
    collectModuleSpecifiers("import {", "invalid.ts", "auto"),
  ).toThrow("Cannot parse invalid.ts");
});

test("backtick specifiers without substitutions are module references", () => {
  const code =
    "const a = import(`template-import`);\n" +
    "const b = require(`template-require`);\n" +
    "const c = import(`computed-${name}`);\n";
  const refs = collectModuleSpecifiers(code, "source.ts", "auto");
  expect(refs.map((ref) => [ref.text, ref.kind])).toEqual([
    ["template-import", "import"],
    ["template-require", "require"],
  ]);
  const first = refs[0];
  if (first === undefined) throw new Error("expected a reference");
  expect(code.slice(first.start, first.end)).toBe("`template-import`");
});
