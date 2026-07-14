#!/usr/bin/env bun
/* eslint-disable no-console */

// Rewrite relative module specifiers in emitted ESM output to carry
// explicit `.js` (or `/index.js`) extensions.
//
// The workspace source uses extensionless relative imports (`./foo`,
// `../bar`, and directory barrels like `./providers`) under
// `moduleResolution: "bundler"`. `tsc` copies those specifiers verbatim
// into the emitted `.js` and `.d.ts`, so the compiled output does not
// run on Node — whose ESM loader requires a real file path — until every
// relative specifier names one. This module resolves each relative
// specifier against the emitted tree (a sibling file `foo.js`, or a
// directory barrel `foo/index.js`) and rewrites it in place. The `.d.ts`
// declarations get the same `.js` specifiers; TypeScript maps `./foo.js`
// to the sibling `./foo.d.ts` when resolving types. External specifiers
// (`@intx/*`, `arktype`, `node:*`) and already-extensioned ones are left
// untouched.
//
// `rewriteSpecifiers` is the pure core — an AST walk plus surgical text
// splices, with the filesystem injected as a resolver — and is exported
// for tests. `rewriteDistTree` binds a filesystem resolver and rewrites a
// whole `dist` tree. The CLI gate runs only when this file is the entry
// point. An unresolved relative specifier is left in place (and reported)
// rather than mangled, so a genuine gap fails loudly at load time.

import {
  existsSync,
  readdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import ts from "typescript";

// Extensions Node's ESM loader accepts as-is; a specifier already ending
// in one of these is left alone.
const RESOLVED_EXTENSION = /\.(js|mjs|cjs|json|node)$/;

/** A relative specifier (starts with `.`) that carries no module
 *  extension — the only kind this rewriter touches. */
function isRewritable(spec: string): boolean {
  return spec.startsWith(".") && !RESOLVED_EXTENSION.test(spec);
}

/** One string-literal module specifier located in the source text. */
type SpecifierRef = { start: number; end: number; text: string };

/** Collect the module-specifier string literals of a parsed source file:
 *  static `import`/`export ... from` clauses, `import x = require("...")`,
 *  dynamic `import("...")`, and `.d.ts` `import("...")` type nodes. A
 *  `declare module "name"` declaration names a module rather than
 *  importing one, so its name literal is deliberately not collected. */
function collectSpecifiers(source: ts.SourceFile): SpecifierRef[] {
  const refs: SpecifierRef[] = [];
  const record = (node: ts.Node | undefined): void => {
    if (node !== undefined && ts.isStringLiteral(node)) {
      refs.push({
        start: node.getStart(source),
        end: node.getEnd(),
        text: node.text,
      });
    }
  };
  const visit = (node: ts.Node): void => {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      record(node.moduleSpecifier);
    } else if (
      ts.isImportEqualsDeclaration(node) &&
      ts.isExternalModuleReference(node.moduleReference)
    ) {
      record(node.moduleReference.expression);
    } else if (
      ts.isCallExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ImportKeyword
    ) {
      record(node.arguments[0]);
    } else if (
      ts.isImportTypeNode(node) &&
      ts.isLiteralTypeNode(node.argument)
    ) {
      record(node.argument.literal);
    }
    ts.forEachChild(node, visit);
  };
  visit(source);
  return refs;
}

/** Rewrite the relative module specifiers of `sourceText` using
 *  `resolve`, which maps a relative, extensionless specifier to its
 *  extensioned form (or returns null to leave it in place). Pure: the
 *  filesystem lives entirely in `resolve`. Edits are applied to the
 *  original text right-to-left, so formatting outside the specifiers is
 *  preserved exactly. */
export function rewriteSpecifiers(
  sourceText: string,
  fileName: string,
  resolve: (spec: string) => string | null,
): string {
  const source = ts.createSourceFile(
    fileName,
    sourceText,
    ts.ScriptTarget.Latest,
    true,
  );
  const edits: { start: number; end: number; replacement: string }[] = [];
  for (const ref of collectSpecifiers(source)) {
    if (!isRewritable(ref.text)) continue;
    const resolved = resolve(ref.text);
    if (resolved === null || resolved === ref.text) continue;
    // Preserve the original quote character rather than assuming one.
    const quote = sourceText[ref.start] ?? '"';
    edits.push({
      start: ref.start,
      end: ref.end,
      replacement: `${quote}${resolved}${quote}`,
    });
  }
  edits.sort((a, b) => b.start - a.start);
  let out = sourceText;
  for (const edit of edits) {
    out = out.slice(0, edit.start) + edit.replacement + out.slice(edit.end);
  }
  return out;
}

/** Resolve a relative specifier against the emitted tree: a sibling file
 *  (`spec.js`) or a directory barrel (`spec/index.js`). `tsc` emits a
 *  `.js` beside every `.d.ts`, so probing the `.js` covers both file
 *  kinds. Returns null when neither exists. */
function resolveAgainstTree(spec: string, fromDir: string): string | null {
  if (existsSync(join(fromDir, `${spec}.js`))) return `${spec}.js`;
  if (existsSync(join(fromDir, spec, "index.js"))) return `${spec}/index.js`;
  return null;
}

/** All `.js` and `.d.ts` files under `dir`, recursively. */
function emittedFiles(dir: string): string[] {
  const acc: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) acc.push(...emittedFiles(path));
    else if (entry.name.endsWith(".js") || entry.name.endsWith(".d.ts"))
      acc.push(path);
  }
  return acc;
}

export type RewriteReport = { fileCount: number; unresolved: string[] };

/** Rewrite every `.js`/`.d.ts` under `distDir` in place, resolving each
 *  relative specifier against its own directory. Returns the count of
 *  files processed and any relative specifiers that could not be resolved
 *  (each `<file>: <spec>`), so a caller can gate on an empty list. */
export function rewriteDistTree(distDir: string): RewriteReport {
  const unresolved: string[] = [];
  const files = emittedFiles(distDir);
  for (const file of files) {
    const fromDir = join(file, "..");
    const rewritten = rewriteSpecifiers(
      readFileSync(file, "utf8"),
      file,
      (spec) => {
        const resolved = resolveAgainstTree(spec, fromDir);
        if (resolved === null) unresolved.push(`${file}: ${spec}`);
        return resolved;
      },
    );
    writeFileSync(file, rewritten);
  }
  return { fileCount: files.length, unresolved };
}

if (import.meta.main) {
  const distDir = process.argv[2];
  if (distDir === undefined) {
    throw new Error("dist-rewrite: usage: dist-rewrite <dist-dir>");
  }
  if (!statSync(distDir).isDirectory()) {
    throw new Error(`dist-rewrite: not a directory: ${distDir}`);
  }
  const { fileCount, unresolved } = rewriteDistTree(distDir);
  if (unresolved.length > 0) {
    console.error(
      `dist-rewrite: ${unresolved.length} unresolved relative specifier(s)\n`,
    );
    for (const u of unresolved) console.error(`  - ${u}`);
    process.exit(1);
  }
  console.log(`dist-rewrite: ok (${fileCount} file(s) processed)`);
}
