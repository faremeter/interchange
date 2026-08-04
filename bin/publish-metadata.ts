#!/usr/bin/env bun
/* eslint-disable no-console */

// Publish-metadata guard for the workspace.
//
// Three fields must be present and correct on every package published to
// npm, or the tarball misbehaves:
//
//   - `files`: an allowlist, so only compiled output and legal text ship
//     — never source, tests, tsconfig, or `.tsbuildinfo`. It is
//     `["dist", "README.md", "LICENSE"]`; `LICENSE` is copied into each
//     package at publish time, and npm silently omits it when absent.
//   - `publishConfig.access: "public"`: scoped packages default to
//     restricted; without this an `@intx/*` publish is not installable by
//     outside consumers.
//   - `sideEffects`: each publishable package declares its own — `false`
//     so bundlers may tree-shake, or a list of the modules that install
//     something at import time so those survive tree-shaking. Whether a
//     module has an import-time side effect is a fact about that package's
//     own source, known only to its author, so the guard validates that
//     the declaration is present and well-formed rather than computing it
//     from a central list of package names. A package that forgets to
//     declare it fails the gate loudly instead of being silently forced to
//     `false` and tree-shaken away. Each declared glob must also name a
//     module that exists on disk or ships via `files`, so a typo or an
//     unshipped path is caught rather than shipped as a dead declaration.
//
// Those three are publish-tarball concerns, so they apply only to the
// non-private packages under `packages/` (the ones that ship). A fourth
// requirement has a different scope:
//
//   - `description`: a non-empty summary. npm shows it in search results
//     and on the package page, and it documents the package for anyone
//     reading the manifest. Every workspace member should carry one —
//     private members included — so this check enumerates all members the
//     root `workspaces` globs declare, not just the publishable packages.
//
// This module is both the one-time transform that sets the tarball fields
// and the check that keeps them, mirroring `exports-shape`. It runs in
// `make lint`, so a package added later without the fields fails the gate
// rather than shipping a broken or oversized tarball. `files` and
// `publishConfig` are mechanical — the transform computes and sets them —
// but `sideEffects` and `description` are author-owned: `--fix` seeds a
// missing `sideEffects` with the safe `false` default and never overwrites
// a real declaration, and it never authors a `description`. So `--fix`
// sets the mechanical fields, seeds a missing `sideEffects`, and then fails
// loudly on any member still carrying a malformed `sideEffects` or missing
// a `description` rather than reporting a success a later check-mode run
// would contradict.

import { join } from "node:path";
import { type } from "arktype";

import {
  readWorkspaceManifestPaths,
  readWorkspacePackages,
} from "./lib/packages";

const ACCESS = "public";

// A few packages read a package-root data directory at runtime, resolved
// via `import.meta.url` rather than through the module graph, so `dist`
// alone would drop it from the tarball and break the installed package.
const EXTRA_FILES: Record<string, string[]> = {
  "@intx/db": ["migrations"],
  "@intx/inference-discovery": ["media"],
};

/** The canonical `files` allowlist for a package by name: compiled output,
 *  any package-root runtime data it reads, then the readme and license. */
export function expectedFiles(name: string): string[] {
  return ["dist", ...(EXTRA_FILES[name] ?? []), "README.md", "LICENSE"];
}

// A well-formed `sideEffects` declaration: `false` (tree-shakeable), or a
// non-empty list of non-empty glob strings naming the modules that install
// something at import time. An empty array is rejected — "nothing has side
// effects" is spelled `false`, not `[]`.
const sideEffectsSchema = type("false | string[]").narrow((value, ctx) => {
  if (value === false) return true;
  if (value.length === 0)
    return ctx.mustBe("false or a non-empty array of glob strings");
  if (value.some((glob) => glob.trim().length === 0))
    return ctx.mustBe("false or an array of non-empty glob strings");
  return true;
});

const rawObjectSchema = type({ "[string]": "unknown" }).narrow((value, ctx) =>
  Array.isArray(value) ? ctx.mustBe("a non-array object") : true,
);

async function readRaw(path: string): Promise<Record<string, unknown>> {
  const raw = rawObjectSchema(await Bun.file(path).json());
  if (raw instanceof type.errors) {
    throw new Error(
      `publish-metadata: ${path} is not a well-formed manifest: ${raw.summary}`,
    );
  }
  return raw;
}

export type MetadataReport = { violations: string[]; packageCount: number };

function eq(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/** True when `glob` matches at least one real file under `dir`. */
function globMatches(dir: string, glob: string): boolean {
  return !new Bun.Glob(glob).scanSync(dir).next().done;
}

// Validate a package's declared `sideEffects` globs against what it ships.
// A glob is satisfied when it matches a file on disk or its root segment is
// a directory the package ships via `files`; the latter accepts a `./dist/*`
// entry even though `dist` is unbuilt at check time (this runs in `make
// lint`, before any dist emit). The array as a whole must also name at least
// one shipped module — a declaration of only source paths would be absent
// from the published tarball and silently tree-shaken — which `anyShipped`
// reports.
//
// Because `dist` is unbuilt here, this guard cannot confirm the emitted
// filenames themselves: a glob whose root ships (like `./dist/foo.js`)
// passes even if `foo.js` is never emitted. Confirming an emitted filename
// needs a built tree, which a lint-time guard does not have;
// `checkBuiltSideEffects` performs that confirmation against the tree
// `buildDist` leaves behind.
function resolveSideEffectGlobs(
  dir: string,
  files: unknown,
  globs: string[],
): { unshipped: string[]; anyShipped: boolean } {
  const shipped = new Set(
    Array.isArray(files)
      ? files.filter((f): f is string => typeof f === "string")
      : [],
  );
  const unshipped: string[] = [];
  let anyShipped = false;
  for (const glob of globs) {
    const matches = globMatches(dir, glob);
    const relative = glob.replace(/^\.\//, "");
    const slash = relative.indexOf("/");
    const rootSegment = slash === -1 ? relative : relative.slice(0, slash);
    const rootShipped = shipped.has(rootSegment);
    if (rootShipped) anyShipped = true;
    if (!matches && !rootShipped) unshipped.push(glob);
  }
  return { unshipped, anyShipped };
}

export async function checkWorkspaceMetadata(
  repoRoot: string,
): Promise<MetadataReport> {
  const violations: string[] = [];
  const list = readWorkspacePackages(repoRoot);
  for (const { name, dir, manifestPath } of list) {
    const raw = await readRaw(manifestPath);
    if (!eq(raw["files"], expectedFiles(name))) {
      violations.push(
        `${name}: "files" must be ${JSON.stringify(expectedFiles(name))}`,
      );
    }
    if (!eq(raw["publishConfig"], { access: ACCESS })) {
      violations.push(
        `${name}: "publishConfig" must be ${JSON.stringify({ access: ACCESS })}`,
      );
    }
    const sideEffects = raw["sideEffects"];
    if (sideEffects === undefined) {
      violations.push(
        `${name}: "sideEffects" must be declared (false or a non-empty array of glob strings)`,
      );
    } else {
      const validated = sideEffectsSchema(sideEffects);
      if (validated instanceof type.errors) {
        violations.push(
          `${name}: "sideEffects" must be false or a non-empty array of glob strings`,
        );
      } else if (validated !== false) {
        const { unshipped, anyShipped } = resolveSideEffectGlobs(
          dir,
          raw["files"],
          validated,
        );
        for (const glob of unshipped) {
          violations.push(
            `${name}: "sideEffects" entry ${glob} matches no file on disk and is not under a shipped path`,
          );
        }
        if (!anyShipped) {
          violations.push(
            `${name}: "sideEffects" declares only unshipped modules; at least one entry must be under a shipped path (e.g. ./dist/...)`,
          );
        }
      }
    }
  }
  return { violations, packageCount: list.length };
}

// Confirm each package's declared `sideEffects` globs against a tree where
// `dist` has been emitted (after `buildDist`). Unlike the lint-time check in
// `checkWorkspaceMetadata`, there is no `files`-coverage escape: with `dist`
// built, every non-`false` glob must match at least one real file, so a typo
// in an emitted path (`./dist/registr.js` for `./dist/register.js`) is caught
// rather than shipped as a dead declaration a consumer's bundler silently
// tree-shakes.
//
// Presence and well-formedness of `sideEffects` are `checkWorkspaceMetadata`'s
// constraint, enforced before any dist emit; this check owns only the emitted
// filename. A malformed or absent declaration reaching here means that gate
// did not run and pass first, so it throws rather than silently skipping a
// package it cannot check.
export async function checkBuiltSideEffects(
  repoRoot: string,
): Promise<MetadataReport> {
  const violations: string[] = [];
  const list = readWorkspacePackages(repoRoot);
  for (const { name, dir, manifestPath } of list) {
    const raw = await readRaw(manifestPath);
    const validated = sideEffectsSchema(raw["sideEffects"]);
    if (validated instanceof type.errors) {
      throw new Error(
        `publish-metadata: ${name} has an absent or malformed "sideEffects"; checkWorkspaceMetadata must run and pass before checkBuiltSideEffects`,
      );
    }
    if (validated === false) continue;
    for (const glob of validated) {
      if (!globMatches(dir, glob)) {
        violations.push(
          `${name}: "sideEffects" entry ${glob} matches no file in the built package directory`,
        );
      }
    }
  }
  return { violations, packageCount: list.length };
}

export type DescriptionReport = {
  violations: string[];
  manifestCount: number;
};

/** A `description` must be a present, non-whitespace string. */
function hasDescription(raw: Record<string, unknown>): boolean {
  const value = raw["description"];
  return typeof value === "string" && value.trim().length > 0;
}

/** Require a non-empty `description` on every workspace member the root
 *  `workspaces` globs declare — private members included. A malformed
 *  member manifest surfaces from `readRaw` rather than being skipped. */
export async function checkWorkspaceDescriptions(
  repoRoot: string,
): Promise<DescriptionReport> {
  const violations: string[] = [];
  const paths = readWorkspaceManifestPaths(repoRoot);
  for (const manifestPath of paths) {
    const raw = await readRaw(manifestPath);
    if (!hasDescription(raw)) {
      const name = typeof raw["name"] === "string" ? raw["name"] : manifestPath;
      violations.push(`${name}: "description" must be a non-empty string`);
    }
  }
  return { violations, manifestCount: paths.length };
}

/** Set the mechanical fields (`files`, `publishConfig`) on every non-private
 *  package and seed a missing `sideEffects` with the safe `false` default.
 *  An existing `sideEffects` declaration is never overwritten — a real glob
 *  list is authored knowledge, and a malformed value is left for the check
 *  to reject. Returns the packages changed. */
export async function fixWorkspaceMetadata(
  repoRoot: string,
): Promise<string[]> {
  const changed: string[] = [];
  for (const { name, manifestPath: path } of readWorkspacePackages(repoRoot)) {
    const raw = await readRaw(path);
    let mutated = false;
    if (!eq(raw["files"], expectedFiles(name))) {
      raw["files"] = expectedFiles(name);
      mutated = true;
    }
    if (!eq(raw["publishConfig"], { access: ACCESS })) {
      raw["publishConfig"] = { access: ACCESS };
      mutated = true;
    }
    if (raw["sideEffects"] === undefined) {
      raw["sideEffects"] = false;
      mutated = true;
    }
    if (mutated) {
      await Bun.write(path, JSON.stringify(raw, null, 2) + "\n");
      changed.push(name);
    }
  }
  return changed;
}

if (import.meta.main) {
  if (import.meta.dirname === undefined) {
    throw new Error(
      "publish-metadata: import.meta.dirname is undefined; cannot locate the repository root",
    );
  }
  const repoRoot = join(import.meta.dirname, "..");
  if (process.argv.includes("--fix")) {
    const changed = await fixWorkspaceMetadata(repoRoot);
    for (const name of changed) console.log(`  set metadata on ${name}`);
    console.log(`publish-metadata: updated ${changed.length} package(s)`);
    // `--fix` cannot author a `description` or repair a malformed
    // `sideEffects` — both are author-owned. Anything still wrong after the
    // fix is a real failure, so surface it and exit non-zero rather than
    // reporting a success that the check-mode run in `make lint` would
    // contradict.
    const metadata = await checkWorkspaceMetadata(repoRoot);
    const descriptions = await checkWorkspaceDescriptions(repoRoot);
    const violations = [...metadata.violations, ...descriptions.violations];
    if (violations.length > 0) {
      console.error(
        `\npublish-metadata: ${violations.length} violation(s) remain after --fix:\n`,
      );
      for (const v of violations) console.error(`  - ${v}`);
      process.exit(1);
    }
  } else {
    const metadata = await checkWorkspaceMetadata(repoRoot);
    const descriptions = await checkWorkspaceDescriptions(repoRoot);
    const violations = [...metadata.violations, ...descriptions.violations];
    if (violations.length > 0) {
      console.error(`publish-metadata: ${violations.length} violation(s)\n`);
      for (const v of violations) console.error(`  - ${v}`);
      console.error(
        `\nRun \`bun bin/publish-metadata.ts --fix\` to set the mechanical publish metadata; a "description" must be written by hand.`,
      );
      process.exit(1);
    }
    console.log(
      `publish-metadata: ok (${metadata.packageCount} non-private package(s), ${descriptions.manifestCount} manifest(s) with a description)`,
    );
  }
}
