// Pack the workspace's built-in tool packages into npm-style tarballs.
//
// The artifacts land under `dist/builtins/`:
//
//   dist/builtins/<basename>-<version>.tgz   # one per package
//
// The filename shape matches the `<basename>-<version>.tgz` pattern
// that the `package-registry` asset KindHandler accepts under
// `tarballs/`, so `bin/publish-tool-packages.ts` can upload the files
// directly without renaming.
//
// The packing logic is deterministic (sorted entries, epoch mtime,
// portable tar headers) so the SRI integrity is reproducible across
// build runs and machines. The asset-substrate publish path will
// consume these tarballs and their integrities; this script is the
// boundary that produces the bytes.
//
// Run via `make builtins`. The script is idempotent: re-running
// overwrites any existing artifacts.

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

// The set of built-in tool packages this binary ships with. Adding a
// new built-in means appending here, declaring `interchange.tools` in
// the package, and pinning the package on whichever agent definition
// wants it.
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

// npm's package-name rules, mirrored from the REST boundary's
// ToolPackagePinName: lowercase, optional `@scope/` prefix with both
// halves non-empty and starting with a URL-safe character. We reject
// malformed scoped names (`@foo/`, `@/bar`, `@scope`) here so the
// produced tarballs never carry a shape the resolver would reject.
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
 * Build the filename-safe form of a package name. Scoped names are
 * flattened with `@scope-tail` rather than stripped to the tail alone:
 * two scoped packages whose tails collide (`@scope-a/widget` and
 * `@scope-b/widget`) would otherwise overwrite each other in
 * `dist/builtins/`. The leading `@` and internal `-` both pass
 * `TARBALL_FILENAME_PATTERN`, so the resulting name remains a valid
 * tarball entry for the `package-registry` substrate.
 */
function tarballBaseName(pkgName: string): string {
  assertValidPackageName(pkgName);
  if (!pkgName.startsWith("@")) return pkgName;
  const slash = pkgName.indexOf("/");
  // The pattern above guarantees a `/` in any name starting with `@`,
  // and that both halves are non-empty; reassert the narrowing here so
  // the slice cannot silently produce an empty tail.
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

  // Copy the package's source tree into the staging area. We exclude
  // node_modules, test files, and tsbuildinfo cache to keep tarballs
  // lean and reproducible.
  const absPkgDir = path.join(REPO_ROOT, spec.packageDir);
  await copyPackageTree(absPkgDir, packageStaging);

  // Transpile the package's `interchange.tools` entry into a self-
  // contained JS bundle so the produced tarball runs on bare Node:
  // the entry's source form depends on Bun's runtime TypeScript
  // support and on workspace-relative imports (`@intx/agent`,
  // `@intx/types`, `arktype` via the bun catalog). Bundling produces
  // a single ESM file the loader can `import()` from any runtime, and
  // lets the packed `package.json` drop the workspace-style
  // `dependencies` the npm resolver cannot satisfy. The workspace
  // `interchange.tools` value is the *packed* output path so the
  // published tarball and the workspace file agree; the source file
  // the bundler reads is the same path under `src/` with `.ts` in
  // place of `.js`.
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

  // Rewrite the packed `package.json` to (a) point at the bundled
  // entry and (b) drop the `workspace:` / `catalog:` dependency specs
  // the closure resolver cannot resolve. Everything those specs
  // referred to has been inlined into the bundle, so the published
  // tarball needs no transitive closure of its own.
  const packedPkgJson: Record<string, unknown> = { ...pkg };
  packedPkgJson.interchange = { tools: outRel };
  delete packedPkgJson.dependencies;
  delete packedPkgJson.devDependencies;
  delete packedPkgJson.optionalDependencies;
  // No current built-in declares peerDependencies, but a future one
  // might — and a peer dep specified as `workspace:` or `catalog:`
  // would survive into the published tarball and fail to resolve at
  // sidecar apply time, exactly the failure mode the rest of these
  // deletions exist to prevent.
  delete packedPkgJson.peerDependencies;
  delete packedPkgJson.exports;
  await fs.writeFile(
    path.join(packageStaging, "package.json"),
    JSON.stringify(packedPkgJson, null, 2),
  );

  // Walk the staging tree, sort, and hand the file list to `tar` in a
  // stable order so the tarball bytes are deterministic across build
  // runs and machines. Combined with `mtime: epoch` (zero entry
  // mtimes) and `portable: true` (strip uid/gid/uname/gname), this
  // makes the SRI integrity reproducible — hub and sidecar built
  // from the same workspace yield bit-identical artifacts even when
  // built separately.
  const tarballPath = path.join(OUT_DIR, tarballName);
  const tarEntries = await listFilesSorted(stagingDir, "package");
  // Normalize the staged tree's mode bits before packing. `tar.create`
  // reads each entry's mode from `fs.stat`, so two builders with
  // different umasks would otherwise produce non-byte-identical
  // archives despite the rest of the determinism setup. 0o755 for
  // directories and 0o644 for regular files is the canonical npm-pack
  // shape.
  await normalizeStagingModes(stagingDir, tarEntries);
  // `mtime: new Date(0)` zeroes every entry's mtime so the artifact
  // is byte-identical across runs. `portable: true` strips uid/gid/
  // uname/gname for the same reason.
  //
  // `noDirRecurse: true` is required because `tarEntries` already
  // contains every directory AND every file under `<root>`. Without
  // it, tar would re-pack each directory's contents on top of the
  // explicit per-file entries, emitting the same file two or three
  // times in the same archive (once for itself, once per ancestor
  // directory entry).
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

// Set every entry in `entries` to canonical mode bits (0o755 for
// directories, 0o644 for regular files). Symlinks are left alone:
// POSIX `chmod` follows the link and would silently rewrite the
// target's mode, which is never what we want during a packing pass.
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

// Recursively list `<cwd>/<root>`'s files (and the directories along
// the way) as paths relative to `cwd`, sorted lexically. Used to hand
// `tar.create` a deterministic entry order rather than letting it
// walk in whatever order `fs.readdir` returns.
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

// The denylist below is hand-curated for the workspace's built-in
// packages, NOT a substitute for `npm pack`'s respect for
// `package.json#files` / `.npmignore`. External tool packages
// published to a registry should pack via the npm-team tooling
// directly; this script only services the in-tree built-ins whose
// shape we control.
//
// DRIFT RISK: when a future built-in introduces a new artifact dir
// (e.g. `coverage/`, `.cache/`, a generated stub directory), this
// list needs to grow to keep the packed tarball clean. The long-
// term answer is a migration to `npm pack`-style traversal that
// honors `package.json#files` and `.npmignore`; until that lands,
// every new artifact dir under a built-in needs an entry here.
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
 * Bundle `<absPkgDir>/<sourceRel>` and write the result under the
 * staging package at `outRel`. Bundling is configured so workspace
 * imports (`@intx/agent`, `@intx/types`, the catalog-resolved
 * `arktype`) are inlined; the produced JS file therefore needs
 * nothing from `node_modules/` at load time. Node-builtins
 * (`node:fs`, `node:path`, etc.) are left external because every
 * Node-compatible runtime ships them.
 */
async function bundleInterchangeEntry(args: {
  absPkgDir: string;
  sourceRel: string;
  outRel: string;
  packageStaging: string;
}): Promise<void> {
  const entryAbs = path.resolve(args.absPkgDir, args.sourceRel);
  // Build-time symmetry with the sidecar loader's containment check:
  // a `..`-bearing `sourceRel` slipped through some future refactor
  // would otherwise let the bundler read a file outside the package
  // tree. The published tarball is a redistribution boundary; refuse
  // to pack a bundle the loader would later reject for the same
  // reason.
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
 * tool-package entries as `.ts` under `src/`; the packed tarball
 * carries the bundled `.js` under `dist/`. Keeping `interchange.tools`
 * pointed at the packed path means the build script must derive the
 * source path it actually feeds to the bundler, which is what this
 * helper does.
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

  // Two BUILTINS entries that pack to the same tarball filename would
  // silently overwrite each other in OUT_DIR — the second pack would
  // win and the first would never reach the registry. Surface the
  // collision at build time rather than letting the operator chase a
  // missing tarball at publish time. `tarballBaseName` already includes
  // the scope, so a same-tail/different-scope pair is fine; only true
  // duplicates (`<basename>-<version>.tgz` already produced) trip this.
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
    // The interchange.tools presence check lives inside packBuiltin
    // so the failure surfaces with the same wording from both call
    // sites and there is one canonical place to update when the
    // contract changes.
    const entry = await packBuiltin(spec, pkg);
    process.stdout.write(
      `  ${entry.name}@${entry.version} → ${entry.tarballPath} (${entry.integrity})\n`,
    );
  }
}

await main();
