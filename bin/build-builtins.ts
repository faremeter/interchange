// Pack the workspace's built-in tool packages into npm-style tarballs
// under `dist/builtins/<basename>-<version>.tgz` (run via `make builtins`).
// The filename shape matches what the `package-registry` asset KindHandler
// accepts under `tarballs/`, so `bin/publish-tool-packages.ts` can upload
// the files without renaming. Deterministic packing (sorted entries, epoch
// mtimes, portable headers) keeps the SRI integrity reproducible across
// builds. Idempotent: re-running overwrites existing artifacts.

import { promises as fs } from "node:fs";
import path from "node:path";
import * as tar from "tar";
import ssri from "ssri";
import { type } from "arktype";

import { PackageJSON } from "@intx/types/package-json";

interface BuiltinSpec {
  /** npm package name as it appears in `package.json#name`. */
  name: string;
  /** Workspace-relative path to the package root. */
  packageDir: string;
}

// The built-in tool packages this binary ships with. Adding one means
// appending here, declaring `interchange.tools`, and pinning it on an
// agent definition.
const BUILTINS: BuiltinSpec[] = [
  { name: "@intx/tools-mail", packageDir: "packages/tools-mail" },
  { name: "@intx/tools-posix", packageDir: "packages/tools-posix" },
  { name: "@intx/tools-lsp", packageDir: "packages/tools-lsp" },
];

interface BuiltBuiltin {
  /** Package name as it appears in the manifest. */
  name: string;
  /** Pinned version. */
  version: string;
  /** SRI integrity of the produced tarball. */
  integrity: string;
  /** Path to the tarball, relative to the repo root. */
  tarballPath: string;
}

const REPO_ROOT = path.resolve(import.meta.dir, "..");
const OUT_DIR = path.join(REPO_ROOT, "dist", "builtins");

// npm package-name rules, mirrored from the REST boundary's
// ToolPackagePinName: lowercase, optional `@scope/` prefix, both
// halves non-empty and starting with a URL-safe character.
const NPM_PACKAGE_NAME_PATTERN =
  /^(?:@[a-z0-9][a-z0-9._-]*\/)?[a-z0-9][a-z0-9._-]*$/;

function assertValidPackageName(pkgName: string): void {
  if (!NPM_PACKAGE_NAME_PATTERN.test(pkgName)) {
    throw new Error(
      `${pkgName} is not a valid npm package name (expected lowercase, optional \`@scope/\` prefix, URL-safe characters)`,
    );
  }
}

/**
 * Filename-safe form of a package name. Scoped names flatten to
 * `@scope-tail` so same-tail scopes (`@scope-a/widget`,
 * `@scope-b/widget`) cannot overwrite each other in `dist/builtins/`.
 */
function tarballBaseName(pkgName: string): string {
  assertValidPackageName(pkgName);
  if (!pkgName.startsWith("@")) return pkgName;
  const slash = pkgName.indexOf("/");
  // `NPM_PACKAGE_NAME_PATTERN` guarantees the `/` and a non-empty tail;
  // reassert so the slice cannot silently produce an empty tail.
  if (slash === -1 || slash === pkgName.length - 1) {
    throw new Error(`scoped package name missing trailing segment: ${pkgName}`);
  }
  return `${pkgName.slice(0, slash)}-${pkgName.slice(slash + 1)}`;
}

async function readPackageJSON(packageDir: string): Promise<PackageJSON> {
  const absPkgDir = path.join(REPO_ROOT, packageDir);
  const raw = await fs.readFile(path.join(absPkgDir, "package.json"), "utf-8");
  const parsed: unknown = JSON.parse(raw);
  const validated = PackageJSON(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `${packageDir}/package.json did not match expected shape: ${validated.summary}`,
    );
  }
  return validated;
}

async function packBuiltin(
  spec: BuiltinSpec,
  pkg: PackageJSON,
): Promise<BuiltBuiltin> {
  const tarballName = `${tarballBaseName(spec.name)}-${pkg.version}.tgz`;
  const stagingDir = path.join(
    OUT_DIR,
    ".staging",
    `${spec.name.replace("/", "_")}-${pkg.version}`,
  );
  const packageStaging = path.join(stagingDir, "package");
  await fs.rm(stagingDir, { recursive: true, force: true });
  await fs.mkdir(packageStaging, { recursive: true });

  // Copy the source tree into staging, excluding node_modules, test
  // files, and tsbuildinfo cache to keep tarballs lean.
  const absPkgDir = path.join(REPO_ROOT, spec.packageDir);
  await copyPackageTree(absPkgDir, packageStaging);

  // Bundle the `interchange.tools` entry into a self-contained ESM
  // file so the tarball runs on bare Node: it inlines the workspace
  // deps (`@intx/*`, catalog-resolved `arktype`) and lets the packed
  // `package.json` drop the `dependencies` the npm resolver cannot
  // satisfy.
  const outRel = pkg.interchange?.tools;
  if (outRel === undefined) {
    throw new Error(
      `${spec.name} package.json has no interchange.tools field — it cannot be a built-in tool package`,
    );
  }
  const sourceRel = deriveSourceFromOutput(outRel);
  await bundleInterchangeEntry({
    absPkgDir,
    sourceRel,
    outRel,
    packageStaging,
  });

  // Point the packed `package.json` at the bundled entry and drop the
  // `workspace:` / `catalog:` dependency specs the closure resolver
  // cannot resolve (their targets are inlined into the bundle).
  const packedPkgJson: Record<string, unknown> = { ...pkg };
  // Override `tools` but preserve the rest of the interchange block
  // (notably `credentials`); rebuilding it with only `tools` would
  // drop static credential declarations.
  packedPkgJson.interchange = { ...pkg.interchange, tools: outRel };
  delete packedPkgJson.dependencies;
  delete packedPkgJson.devDependencies;
  delete packedPkgJson.optionalDependencies;
  // No built-in declares peerDependencies today, but a future
  // `workspace:`/`catalog:` peer dep would survive into the published
  // tarball and fail to resolve at sidecar apply time.
  delete packedPkgJson.peerDependencies;
  delete packedPkgJson.exports;
  await fs.writeFile(
    path.join(packageStaging, "package.json"),
    JSON.stringify(packedPkgJson, null, 2),
  );

  // Hand the sorted file list to `tar` so the tarball bytes are
  // deterministic across runs and machines.
  const tarballPath = path.join(OUT_DIR, tarballName);
  const tarEntries = await listFilesSorted(stagingDir, "package");
  // Normalize mode bits before packing: `tar.create` reads each
  // entry's mode from `fs.stat`, so differing umasks would otherwise
  // break byte-identical archives. 0o755 dirs / 0o644 files is the
  // canonical npm-pack shape.
  await normalizeStagingModes(stagingDir, tarEntries);
  // `mtime: new Date(0)` zeroes entry mtimes and `portable: true`
  // strips uid/gid/uname/gname, both for byte-identical artifacts.
  // `noDirRecurse: true` is required because `tarEntries` already
  // contains every directory and file; without it tar would re-pack
  // each directory's contents, emitting files two or three times.
  const createOpts: tar.TarOptionsWithAliasesAsyncFile = {
    cwd: stagingDir,
    gzip: true,
    file: tarballPath,
    portable: true,
    mtime: new Date(0),
    noDirRecurse: true,
  };
  await tar.create(createOpts, tarEntries);

  await fs.rm(stagingDir, { recursive: true, force: true });

  const bytes = await fs.readFile(tarballPath);
  const integrity = ssri.fromData(bytes, { algorithms: ["sha512"] }).toString();

  return {
    name: spec.name,
    version: pkg.version,
    integrity,
    tarballPath: path.relative(REPO_ROOT, tarballPath),
  };
}

// Set canonical mode bits (0o755 dirs, 0o644 files). Symlinks are
// skipped: POSIX `chmod` follows the link and would rewrite the
// target's mode.
async function normalizeStagingModes(
  cwd: string,
  entries: string[],
): Promise<void> {
  for (const rel of entries) {
    const abs = path.join(cwd, rel);
    const stat = await fs.lstat(abs);
    if (stat.isSymbolicLink()) continue;
    if (stat.isDirectory()) {
      await fs.chmod(abs, 0o755);
    } else if (stat.isFile()) {
      await fs.chmod(abs, 0o644);
    }
  }
}

// Recursively list `<cwd>/<root>`'s files and directories, relative
// to `cwd`, sorted lexically, so `tar.create` gets a deterministic
// entry order.
async function listFilesSorted(cwd: string, root: string): Promise<string[]> {
  const acc: string[] = [];
  async function walk(rel: string): Promise<void> {
    const abs = path.join(cwd, rel);
    const entries = await fs.readdir(abs, { withFileTypes: true });
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const entry of entries) {
      const childRel = path.join(rel, entry.name);
      acc.push(childRel);
      if (entry.isDirectory()) {
        await walk(childRel);
      }
    }
  }
  acc.push(root);
  await walk(root);
  return acc;
}

// Hand-curated denylist for the in-tree built-ins, NOT a substitute
// for `npm pack`'s `package.json#files` / `.npmignore` handling.
// DRIFT RISK: a future built-in that adds a new artifact dir
// (coverage/, .cache/, ...) needs a new entry here; the long-term
// fix is `npm pack`-style traversal.
async function copyPackageTree(src: string, dest: string): Promise<void> {
  const entries = await fs.readdir(src, { withFileTypes: true });
  for (const entry of entries) {
    if (
      entry.name === "node_modules" ||
      entry.name === "tsconfig.tsbuildinfo" ||
      entry.name === "dist" ||
      entry.name.endsWith(".test.ts")
    ) {
      continue;
    }
    const srcPath = path.join(src, entry.name);
    const destPath = path.join(dest, entry.name);
    if (entry.isDirectory()) {
      await fs.mkdir(destPath, { recursive: true });
      await copyPackageTree(srcPath, destPath);
    } else if (entry.isFile()) {
      await fs.copyFile(srcPath, destPath);
    }
  }
}

/**
 * Bundle `<absPkgDir>/<sourceRel>` and write the result at `outRel`
 * in the staging package. Workspace imports are inlined so the
 * output needs nothing from `node_modules/`; node builtins are left
 * external.
 */
async function bundleInterchangeEntry(args: {
  absPkgDir: string;
  sourceRel: string;
  outRel: string;
  packageStaging: string;
}): Promise<void> {
  const entryAbs = path.resolve(args.absPkgDir, args.sourceRel);
  // Mirror the sidecar loader's containment check: a `..`-bearing
  // `sourceRel` would otherwise let the bundler read outside the
  // package tree. Refuse to pack a bundle the loader would reject.
  const pkgDirAbs = path.resolve(args.absPkgDir);
  const pkgPrefix = pkgDirAbs.endsWith(path.sep)
    ? pkgDirAbs
    : pkgDirAbs + path.sep;
  if (entryAbs !== pkgDirAbs && !entryAbs.startsWith(pkgPrefix)) {
    throw new Error(
      `bundleInterchangeEntry: sourceRel ${JSON.stringify(args.sourceRel)} resolves to ${JSON.stringify(entryAbs)} which escapes the package directory ${JSON.stringify(pkgDirAbs)}`,
    );
  }
  await fs.access(entryAbs);
  const outAbs = path.resolve(args.packageStaging, args.outRel);
  await fs.mkdir(path.dirname(outAbs), { recursive: true });

  const result = await Bun.build({
    entrypoints: [entryAbs],
    outdir: path.dirname(outAbs),
    naming: path.basename(outAbs),
    target: "node",
    format: "esm",
    // Resolve `@intx/*` imports to TypeScript source via the
    // `intx-src` exports condition; no `dist` exists in-workspace.
    conditions: ["intx-src"],
    minify: false,
    sourcemap: "none",
  });
  if (!result.success) {
    const messages = result.logs
      .map((log) => (log instanceof Error ? log.message : String(log)))
      .join("\n");
    throw new Error(
      `Bun.build failed for ${args.sourceRel}:\n${messages || "(no diagnostics)"}`,
    );
  }
}

/**
 * Map `./dist/<name>.js` → `./src/<name>.ts`. The workspace stores
 * entries as `.ts` under `src/`; the packed tarball carries the
 * bundled `.js` under `dist/`, so the build must derive the source
 * path it feeds to the bundler.
 */
function deriveSourceFromOutput(outRel: string): string {
  const normalized = outRel.startsWith("./") ? outRel.slice(2) : outRel;
  if (!normalized.startsWith("dist/")) {
    throw new Error(
      `interchange.tools is "${outRel}"; expected a "./dist/<name>.js" path`,
    );
  }
  if (!normalized.endsWith(".js")) {
    throw new Error(
      `interchange.tools is "${outRel}"; expected a ".js" suffix on the packed output`,
    );
  }
  const stem = normalized.slice("dist/".length, -".js".length);
  return `./src/${stem}.ts`;
}

async function main(): Promise<void> {
  await fs.rm(OUT_DIR, { recursive: true, force: true });
  await fs.mkdir(OUT_DIR, { recursive: true });

  // Two BUILTINS entries producing the same tarball filename would
  // silently overwrite each other in OUT_DIR. Surface the collision
  // at build time. `tarballBaseName` already includes the scope, so
  // only true duplicates trip this.
  const seenFilenames = new Set<string>();
  for (const spec of BUILTINS) {
    const pkg = await readPackageJSON(spec.packageDir);
    const expected = `${tarballBaseName(spec.name)}-${pkg.version}.tgz`;
    if (seenFilenames.has(expected)) {
      throw new Error(
        `built-in tarball filename collision: ${expected} would be produced by more than one BUILTINS entry`,
      );
    }
    seenFilenames.add(expected);
    // The `interchange.tools` presence check lives in packBuiltin so
    // both call sites surface the same wording.
    const entry = await packBuiltin(spec, pkg);
    process.stdout.write(
      `  ${entry.name}@${entry.version} → ${entry.tarballPath} (${entry.integrity})\n`,
    );
  }
}

await main();
