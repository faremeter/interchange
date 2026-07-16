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
//   - `sideEffects`: `false` so bundlers may tree-shake, except where a
//     module installs something at import time. `@intx/log` does (its
//     entry points import the default console sink), so it names those
//     files instead.
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
// This module is both the one-time transform that sets the three tarball
// fields and the check that keeps them, mirroring `exports-shape`. It runs
// in `make lint`, so a package added later without the fields fails the
// gate rather than shipping a broken or oversized tarball. The transform
// never authors a `description` — wording is written by hand — so `--fix`
// sets the three mechanical fields and then fails loudly on any member
// still missing one rather than reporting a success a later check-mode run
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

// The only package with an import-time side effect: `src/index.ts` and
// `src/hono.ts` both `import "./default-sink"`, which installs the default
// console sink. Both source and emitted paths are named so the import
// survives tree-shaking whichever a downstream bundler resolves.
const LOG_SIDE_EFFECTS = [
  "./src/index.ts",
  "./src/hono.ts",
  "./src/default-sink.ts",
  "./dist/index.js",
  "./dist/hono.js",
  "./dist/default-sink.js",
];

/** The canonical `sideEffects` value for a package by name. */
export function expectedSideEffects(name: string): false | string[] {
  return name === "@intx/log" ? LOG_SIDE_EFFECTS : false;
}

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

export async function checkWorkspaceMetadata(
  repoRoot: string,
): Promise<MetadataReport> {
  const violations: string[] = [];
  const list = readWorkspacePackages(repoRoot);
  for (const { name, manifestPath } of list) {
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
    if (!eq(raw["sideEffects"], expectedSideEffects(name))) {
      violations.push(
        `${name}: "sideEffects" must be ${JSON.stringify(expectedSideEffects(name))}`,
      );
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

/** Set the three fields on every non-private package. Returns the packages
 *  changed. */
export async function fixWorkspaceMetadata(
  repoRoot: string,
): Promise<string[]> {
  const changed: string[] = [];
  for (const { name, manifestPath: path } of readWorkspacePackages(repoRoot)) {
    const raw = await readRaw(path);
    const sideEffects = expectedSideEffects(name);
    if (
      eq(raw["files"], expectedFiles(name)) &&
      eq(raw["publishConfig"], { access: ACCESS }) &&
      eq(raw["sideEffects"], sideEffects)
    ) {
      continue;
    }
    raw["files"] = expectedFiles(name);
    raw["sideEffects"] = sideEffects;
    raw["publishConfig"] = { access: ACCESS };
    await Bun.write(path, JSON.stringify(raw, null, 2) + "\n");
    changed.push(name);
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
    // `--fix` never authors a description. A member left without one is a
    // real failure, so surface it and exit non-zero rather than reporting a
    // success that the check-mode run in `make lint` would contradict.
    const { violations } = await checkWorkspaceDescriptions(repoRoot);
    if (violations.length > 0) {
      console.error(
        `\npublish-metadata: ${violations.length} member(s) still need a hand-written "description":\n`,
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
