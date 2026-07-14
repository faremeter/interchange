#!/usr/bin/env bun
/* eslint-disable no-console */

// Publish-metadata guard for the non-private workspace packages.
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
// This module is both the one-time transform that sets these fields and
// the check that keeps them, mirroring `exports-shape`. It runs in
// `make lint`, so a package added later without the fields fails the gate
// rather than shipping a broken or oversized tarball. Private packages
// are skipped.

import { join } from "node:path";
import { type } from "arktype";

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

const manifestSchema = type({
  name: "string",
  "private?": "boolean",
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

function nonPrivatePackageManifests(repoRoot: string): string[] {
  return [...new Bun.Glob("packages/*/package.json").scanSync(repoRoot)].sort();
}

/** Non-private packages, as `{ name, manifest-relative path }`. */
async function targets(
  repoRoot: string,
): Promise<{ name: string; rel: string }[]> {
  const out: { name: string; rel: string }[] = [];
  for (const rel of nonPrivatePackageManifests(repoRoot)) {
    const parsed = manifestSchema(await Bun.file(join(repoRoot, rel)).json());
    if (parsed instanceof type.errors) {
      throw new Error(
        `publish-metadata: ${rel} is not a well-formed manifest: ${parsed.summary}`,
      );
    }
    if (parsed.private === true) continue;
    out.push({ name: parsed.name, rel });
  }
  return out;
}

export type MetadataReport = { violations: string[]; packageCount: number };

function eq(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

export async function checkWorkspaceMetadata(
  repoRoot: string,
): Promise<MetadataReport> {
  const violations: string[] = [];
  const list = await targets(repoRoot);
  for (const { name, rel } of list) {
    const raw = await readRaw(join(repoRoot, rel));
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

/** Set the three fields on every non-private package. Returns the packages
 *  changed. */
export async function fixWorkspaceMetadata(
  repoRoot: string,
): Promise<string[]> {
  const changed: string[] = [];
  for (const { name, rel } of await targets(repoRoot)) {
    const path = join(repoRoot, rel);
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
  } else {
    const { violations, packageCount } = await checkWorkspaceMetadata(repoRoot);
    if (violations.length > 0) {
      console.error(`publish-metadata: ${violations.length} violation(s)\n`);
      for (const v of violations) console.error(`  - ${v}`);
      console.error(
        `\nRun \`bun bin/publish-metadata.ts --fix\` to set the publish metadata.`,
      );
      process.exit(1);
    }
    console.log(
      `publish-metadata: ok (${packageCount} non-private package(s))`,
    );
  }
}
