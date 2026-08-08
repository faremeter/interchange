// Closure materialization and per-instance store layout for the tool-package
// loader: fetching + SRI-verifying + extracting each platform-matching entry
// into the content-addressable cache, resolving dependency ranges, and
// hardlinking the closure into a per-instance store the loader then imports
// author code from. Extracted from `loader.ts` so this eval-free
// materialization concern is isolated from author-code loading. `loader.ts`
// re-exports `materializeClosure` and `storeEntryDir` for existing consumers.

import { promises as fs } from "node:fs";
import path from "node:path";
import semver from "semver";

import { getLogger } from "@intx/log";
import type { ToolPackageManifestEntry } from "@intx/types/tool-packages";

import { TarballIntegrityMismatchError } from "./cache";
import type {
  HostPlatform,
  MaterializeClosureArgs,
  MaterializeClosureResult,
} from "./loader";
import {
  ToolLoaderError,
  describeError,
  isEEXIST,
  isENOENT,
  platformListMatches,
} from "./loader-internal";

const logger = getLogger(["sidecar", "tool-packaging", "store-layout"]);

/**
 * Lay out a resolved manifest closure into the per-instance store
 * WITHOUT importing any author code. This is phases 1-2 of the loader:
 *
 *   1. Fetch + SRI-verify + extract each platform-matching entry into
 *      the content-addressable cache.
 *   2. Resolve the closure's dependency ranges by first arrival, then
 *      hardlink each entry into `<instanceScratchDir>/store/<name>/<version>/`
 *      and symlink each direct dep into that entry's `node_modules/` so
 *      Node's ancestor walk resolves bare-specifier imports.
 *
 * Returns the `storeDir` the closure was laid out under. The first real
 * `import()` of author code is NOT here — it belongs to `loadManifest`'s
 * phase-3 loop — so a caller (e.g. an install-time probe) can stage the
 * frozen closure into a package directory the loader consumes without
 * executing author code on its host.
 *
 * Only entries whose `os`/`cpu` match `host` are materialized; the rest
 * are skipped with a `platform.mismatch.skipped` debug log. Errors
 * surface as `ToolLoaderError` with a `category` matching the
 * corresponding `DeployApplyErrorCategory`.
 */
export async function materializeClosure(
  args: MaterializeClosureArgs,
): Promise<MaterializeClosureResult> {
  const filtered = args.manifest.entries.filter((entry) =>
    passesPlatformFilter(entry, args.host),
  );
  const storeDir = path.join(args.instanceScratchDir, "store");

  async function materialize(
    entry: ToolPackageManifestEntry,
  ): Promise<{ dir: string; release: () => void }> {
    // Resolve registry-sourced entries against the sidecar config
    // before doing any I/O. If the manifest references an unknown
    // registry name the apply fails loudly here, regardless of whether
    // the bytes are already cached, so the failure surfaces even on
    // cache hits that would otherwise hide the misconfiguration.
    if (entry.source.kind === "registry") {
      if (!args.registries.has(entry.source.registry)) {
        throw new ToolLoaderError({
          category: "registry.unknown",
          message: `manifest references registry "${entry.source.registry}" which is not in the sidecar config`,
          package: { name: entry.name, version: entry.version },
        });
      }
    } else if (entry.source.kind === "asset") {
      // Reject up front (parallel to the registry.unknown gate) so a
      // cache hit cannot hide a missing mount from the manifest fan-out.
      if (!args.assetMounts.has(entry.source.assetId)) {
        throw new ToolLoaderError({
          category: "asset.mount.missing",
          message: `manifest entry references assetId "${entry.source.assetId}" which is not in the deploy pack's asset-mounts map`,
          package: { name: entry.name, version: entry.version },
        });
      }
    }

    // Probe cache presence with `has` rather than `get`: the bytes are
    // only needed when they have to be fetched-then-stored, and
    // `extractTarball` below re-reads them from disk on the way to the
    // per-integrity unpack directory. `has` checks file existence
    // without reading or atime-touching the bytes, so a cache-hit
    // apply avoids the wasted read of a tarball that immediately gets
    // discarded.
    if (!(await args.cache.has(entry.integrity))) {
      const bytes = await args.fetchTarball(entry, {
        registries: args.registries,
        assetRoot: args.assetRoot,
        assetMounts: args.assetMounts,
      });
      try {
        await args.cache.put(entry.integrity, bytes);
      } catch (err) {
        if (err instanceof TarballIntegrityMismatchError) {
          throw new ToolLoaderError({
            category: "integrity.mismatch",
            message: `bytes for ${entry.name}@${entry.version} did not match pinned integrity`,
            package: { name: entry.name, version: entry.version },
          });
        }
        throw err;
      }
    }

    try {
      return await args.cache.extractTarball(entry.integrity);
    } catch (err) {
      // Eviction is reserved for the integrity-mismatch path: the bytes
      // on disk no longer match the pinned hash, so the entry is poison
      // and must be re-fetched. Other failures — tar parse errors, FS
      // transients (EIO, ENOSPC) — leave the cached bytes intact. The
      // cache's `evict` defers physical reclaim of the extraction tree
      // until every outstanding `release` from a concurrent
      // `extractTarball` has fired, so a parallel agent's in-flight
      // `hardlinkTree` walk against the same extraction will not
      // ENOENT mid-readdir.
      if (err instanceof TarballIntegrityMismatchError) {
        await args.cache.evict(entry.integrity);
      }
      throw new ToolLoaderError({
        category: "tarball.extract.failed",
        message: `tar extraction failed for ${entry.name}@${entry.version}: ${describeError(err)}`,
        package: { name: entry.name, version: entry.version },
      });
    }
  }

  // 1. Materialize every filtered entry into the cache and capture its
  //    extraction directory. This validates the manifest is
  //    registry-chain-consistent (each entry resolves end-to-end
  //    against its declared source) and primes the cache so the layout
  //    step can hardlink without re-fetching.
  //
  //    Each materialize() returns an `{ dir, release }` pair: the cache
  //    treats the returned `dir` as held until `release` is called, so a
  //    concurrent eviction of the same integrity defers its physical
  //    reclaim of the extraction tree until after the buildStoreLayout
  //    pass below has finished walking every dir to hardlink files out.
  //    Releases are aggregated and drained in a `finally` so an error
  //    mid-layout still hands the cache its references back. They are
  //    drained once the layout is built: the phase-3 import loop reads
  //    from the hardlinked store, not from these extraction trees, so
  //    the extraction handles are a phase-1-2 concern only.
  const extractionByEntry = new Map<string, string>();
  const entriesByNameVersion = new Map<string, ToolPackageManifestEntry>();
  const releases: (() => void)[] = [];
  try {
    for (const entry of filtered) {
      const handle = await materialize(entry);
      const key = `${entry.name}@${entry.version}`;
      extractionByEntry.set(key, handle.dir);
      entriesByNameVersion.set(key, entry);
      releases.push(handle.release);
    }

    // 2. Build the per-instance store layout. Each filtered entry gets a
    //    real directory at `<store>/<name>/<version>/` populated by
    //    hardlinks from its cache extraction; the direct-dependency walk
    //    then symlinks `node_modules/<dep>` into each layout dir so
    //    Node's standard ancestor walk resolves bare-specifier imports
    //    from inside the package's body against the closure's pinned
    //    versions.
    const rangeResolution = await resolveRangesByFirstArrival({
      topLevel: args.manifest.topLevel,
      filtered,
      extractionByEntry,
      entriesByNameVersion,
    });
    await buildStoreLayout({
      filtered,
      storeDir,
      extractionByEntry,
      rangeResolution,
    });
  } finally {
    for (const release of releases) {
      release();
    }
  }

  return { storeDir, entries: filtered };
}

/**
 * True when `entry` may run on `host`: its `os`/`cpu` platform lists (if
 * present) accept the host per npm's allow/block-list semantics. Pure —
 * the single source of truth for the host-match decision, shared by the
 * logging filter `passesPlatformFilter` and `loadManifest`'s phase-3
 * re-selection (which must not re-emit the mismatch debug logs
 * `materializeClosure` already wrote).
 */
function entryMatchesHost(
  entry: ToolPackageManifestEntry,
  host: HostPlatform,
): boolean {
  if (entry.os !== undefined && !platformListMatches(entry.os, host.os)) {
    return false;
  }
  if (entry.cpu !== undefined && !platformListMatches(entry.cpu, host.cpu)) {
    return false;
  }
  return true;
}

/**
 * `entryMatchesHost` with the `platform.mismatch.skipped` debug log a
 * rejected entry emits. `materializeClosure` filters with this so the
 * skip is diagnosable; the re-derived `platformListMatches` call on the
 * reject path only runs to pick the os-vs-cpu wording for the message.
 */
function passesPlatformFilter(
  entry: ToolPackageManifestEntry,
  host: HostPlatform,
): boolean {
  if (entryMatchesHost(entry, host)) return true;
  if (entry.os !== undefined && !platformListMatches(entry.os, host.os)) {
    logger.debug`platform.mismatch.skipped: ${entry.name}@${entry.version} requires os ${entry.os.join(",")} (host is ${host.os})`;
  } else if (
    entry.cpu !== undefined &&
    !platformListMatches(entry.cpu, host.cpu)
  ) {
    logger.debug`platform.mismatch.skipped: ${entry.name}@${entry.version} requires cpu ${entry.cpu.join(",")} (host is ${host.cpu})`;
  }
  return false;
}

export function storeEntryDir(
  storeDir: string,
  name: string,
  version: string,
): string {
  // `@scope/name` carries a slash that, taken naively, would push the
  // package's contents one directory deeper than `loadTopLevel`
  // expects. Mirror npm's on-disk shape: `node_modules/@scope/name/`,
  // so a scoped entry's dir is `<store>/@scope/name/<version>/`.
  return path.join(storeDir, name, version);
}

interface BuildStoreLayoutArgs {
  readonly filtered: readonly ToolPackageManifestEntry[];
  readonly storeDir: string;
  readonly extractionByEntry: ReadonlyMap<string, string>;
  readonly rangeResolution: RangeResolution;
}

/**
 * Build the per-instance `<store>/<name>/<version>/` tree for every
 * filtered manifest entry: hardlink each entry's source files in from
 * the cache extraction, then symlink each direct dep into the entry's
 * `node_modules/`. Hardlinks keep byte usage to one copy per integrity
 * per filesystem; symlinks at the `node_modules/` boundary let Node's
 * realpath-based resolver walk to the dep's own layout dir (with its
 * own `node_modules/`) so transitive resolution composes recursively.
 */
async function buildStoreLayout(args: BuildStoreLayoutArgs): Promise<void> {
  // First materialize every layout dir with its hardlinked contents.
  // node_modules symlinks come after, so a dep's layout dir is already
  // populated when its parent's symlink starts pointing at it.
  for (const entry of args.filtered) {
    const key = `${entry.name}@${entry.version}`;
    const extraction = args.extractionByEntry.get(key);
    if (extraction === undefined) {
      throw new Error(
        `internal: layout build for ${key} found no cache extraction`,
      );
    }
    const layoutDir = storeEntryDir(args.storeDir, entry.name, entry.version);
    await fs.mkdir(path.dirname(layoutDir), { recursive: true });
    await hardlinkTree(extraction, layoutDir);
  }

  for (const entry of args.filtered) {
    const key = `${entry.name}@${entry.version}`;
    const extraction = args.extractionByEntry.get(key);
    if (extraction === undefined) {
      throw new Error(
        `internal: layout link pass for ${key} found no cache extraction`,
      );
    }
    const layoutDir = storeEntryDir(args.storeDir, entry.name, entry.version);
    const deps = await readDirectDependencies(extraction, entry);

    if (deps.length === 0) continue;
    const modulesDir = path.join(layoutDir, "node_modules");
    await fs.mkdir(modulesDir, { recursive: true });

    for (const dep of deps) {
      const pickedVersion = args.rangeResolution.lookup(dep.name, dep.range);
      if (pickedVersion === null) {
        if (dep.optional) {
          logger.debug`optional.dropped.skipped: ${entry.name}@${entry.version} optional dep ${dep.name}@${dep.range} has no satisfying version in the closure (likely platform-filtered out)`;
          continue;
        }
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `${entry.name}@${entry.version} depends on ${dep.name}@${dep.range} but the manifest closure has no satisfying version; the resolver was expected to include it`,
          package: { name: entry.name, version: entry.version },
        });
      }
      const target = storeEntryDir(args.storeDir, dep.name, pickedVersion);
      const symlinkPath = path.join(modulesDir, dep.name);
      // Scoped deps live one directory deep under `node_modules/`;
      // ensure the scope dir exists before linking.
      await fs.mkdir(path.dirname(symlinkPath), { recursive: true });
      const relativeTarget = path.relative(path.dirname(symlinkPath), target);
      try {
        await fs.symlink(relativeTarget, symlinkPath, "dir");
      } catch (err) {
        if (!isEEXIST(err)) throw err;
        const existing = await fs.readlink(symlinkPath);
        if (existing !== relativeTarget) {
          // A symlink collision inside the loader's per-package
          // layout pass is a loader-layer invariant violation, not an
          // unknown error shape — route it through the same structured
          // envelope every other loader failure uses so atomic-apply
          // surfaces it as `package.entry.invalid` instead of falling
          // back to the unknown-shape catch-all (`factory.construct.
          // failed`).
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `symlink collision at ${symlinkPath}: existing target ${existing} differs from ${relativeTarget}`,
          });
        }
      }
    }
  }
}

interface ResolveRangesArgs {
  readonly topLevel: readonly {
    readonly name: string;
    readonly version: string;
  }[];
  readonly filtered: readonly ToolPackageManifestEntry[];
  readonly extractionByEntry: ReadonlyMap<string, string>;
  readonly entriesByNameVersion: ReadonlyMap<string, ToolPackageManifestEntry>;
}

interface RangeResolution {
  lookup(name: string, range: string): string | null;
}

/**
 * Walk the closure in BFS order from the top-level pins (in their
 * input order) and record, for each `(name, range)` first encountered,
 * the version chosen out of the closure. Subsequent edges with the
 * same `(name, range)` reuse the recorded pick instead of re-running
 * `semver.maxSatisfying` against the current closure shape.
 *
 * Mirrors the resolver's first-arrival-per-`(name, range)` semantics
 * on the loader side. Without this, two requirers with overlapping
 * ranges of the same dep could each pick a different version of that
 * dep — `maxSatisfying` is deterministic given its candidate set, but
 * the candidate set is the full closure for the name and a transitive
 * addition since the first arrival can shift the answer. Recording
 * the first arrival per range freezes the pick so every requirer in
 * the same equivalence class lands on the same version of the dep.
 *
 * Returns null for a `(name, range)` that has no satisfying entry in
 * the filtered closure; callers decide whether that is fatal (hard
 * dep) or skippable (optional dep).
 */
async function resolveRangesByFirstArrival(
  args: ResolveRangesArgs,
): Promise<RangeResolution> {
  const recorded = new Map<string, string | null>();
  const visited = new Set<string>();
  const filteredKeys = new Set(
    args.filtered.map((e) => `${e.name}@${e.version}`),
  );

  function rangeKey(name: string, range: string): string {
    return `${name}@${range}`;
  }

  function pickFromClosure(name: string, range: string): string | null {
    const candidates: string[] = [];
    for (const entry of args.entriesByNameVersion.values()) {
      if (entry.name !== name) continue;
      if (!filteredKeys.has(`${entry.name}@${entry.version}`)) continue;
      candidates.push(entry.version);
    }
    if (candidates.length === 0) return null;
    const valid = candidates.filter((v) => semver.valid(v) !== null);
    if (valid.length > 0) {
      const picked = semver.maxSatisfying(valid, range, {
        includePrerelease: true,
      });
      if (picked !== null) return picked;
    }
    // Literal-version fallback: when a transitive dep's range is
    // itself a concrete version string (e.g. `'1.0.0'` not
    // `'^1.0.0'`), `maxSatisfying` rejects on prerelease semantics but
    // the literal match is valid.
    if (candidates.includes(range)) return range;
    return null;
  }

  // BFS frontier carries the entry whose direct deps we are about to
  // fan out on next. Seed with the top-level pins in pin order, mapped
  // through the filtered closure so platform-filtered tops are skipped
  // (their deps would not have layout dirs to link into).
  const queue: ToolPackageManifestEntry[] = [];
  for (const pin of args.topLevel) {
    const key = `${pin.name}@${pin.version}`;
    const entry = args.entriesByNameVersion.get(key);
    if (entry === undefined) continue;
    if (!filteredKeys.has(key)) continue;
    if (visited.has(key)) continue;
    visited.add(key);
    queue.push(entry);
  }

  while (queue.length > 0) {
    const entry = queue.shift();
    if (entry === undefined) break;
    const extraction = args.extractionByEntry.get(
      `${entry.name}@${entry.version}`,
    );
    if (extraction === undefined) continue;
    const deps = await readDirectDependencies(extraction, entry);
    for (const dep of deps) {
      const key = rangeKey(dep.name, dep.range);
      // `recorded.get(key)` returning `null` is the "we picked this
      // range against the closure and got nothing" cached answer.
      // Caching the null is safe only because the closure is static
      // across this loader pass — `entriesByNameVersion` does not
      // grow underneath us. If a future change starts adding entries
      // mid-walk (e.g. lazy fetches during BFS), the cached null
      // would shadow the new candidates and produce a phantom miss;
      // the cache key would need to be invalidated alongside the
      // closure additions.
      let picked = recorded.get(key);
      if (picked === undefined) {
        picked = pickFromClosure(dep.name, dep.range);
        recorded.set(key, picked);
      }
      if (picked === null) continue;
      const depKey = `${dep.name}@${picked}`;
      if (visited.has(depKey)) continue;
      visited.add(depKey);
      const depEntry = args.entriesByNameVersion.get(depKey);
      if (depEntry === undefined) continue;
      queue.push(depEntry);
    }
  }

  return {
    lookup(name, range) {
      const key = rangeKey(name, range);
      if (recorded.has(key)) {
        const picked = recorded.get(key);
        return picked === undefined ? null : picked;
      }
      // The BFS only walks entries reachable from the top-level pins.
      // A dep declared by an entry the BFS did not reach (e.g. a
      // closure entry that no top-level chain ever required) is not
      // pre-recorded; fall through to a fresh pick from the closure
      // so the layout for such entries still resolves deterministically.
      const fallback = pickFromClosure(name, range);
      recorded.set(key, fallback);
      return fallback;
    },
  };
}

async function hardlinkTree(
  srcDir: string,
  destDir: string,
  extractionRoot: string = srcDir,
): Promise<void> {
  await fs.mkdir(destDir, { recursive: true });
  const entries = await fs.readdir(srcDir, { withFileTypes: true });
  for (const entry of entries) {
    const src = path.join(srcDir, entry.name);
    const dest = path.join(destDir, entry.name);
    if (entry.isDirectory()) {
      await hardlinkTree(src, dest, extractionRoot);
    } else if (entry.isFile()) {
      try {
        await fs.link(src, dest);
      } catch (err) {
        if (!isEEXIST(err)) throw err;
      }
    } else if (entry.isSymbolicLink()) {
      // Preserve symlinks from the tarball verbatim; npm packages
      // occasionally ship them and clobbering with a hardlink would
      // change the file's identity.
      //
      // ISOMORPHIC-LAYOUT ASSUMPTION: writing the source-side
      // relative target verbatim into the destination only works
      // because the source extraction tree and the per-instance
      // store tree mirror each other entry-for-entry — the symlink
      // copies into the same shape, so the relative target still
      // resolves to the same sibling in the destination. A future
      // change that flattens, reshapes, or partially copies the
      // extraction tree would invalidate every symlink it touched
      // and would need to rewrite the targets instead of preserving
      // them.
      //
      // Symlink targets originate from the tarball and cross the trust
      // boundary into the sidecar. Resolve each target against the
      // symlink's own directory and verify it lands inside the
      // extraction root; a target that escapes would let a malicious
      // tarball point at arbitrary sidecar-readable files via the
      // layout dir's `node_modules` walk.
      //
      // The `tar` package version we use rejects absolute symlink
      // targets during extraction, so by the time we observe a
      // symlink here it is necessarily relative.
      //
      // The immediate target of `src` may itself be a directory whose
      // own contents include another symlink. Resolving only the
      // first hop with `path.resolve(path.dirname(src), target)`
      // checks containment of the link's literal target — a chain
      // whose first hop lands inside the extraction root but whose
      // realpath ultimately escapes (target is a directory that
      // itself contains an escaping symlink) would slip past.
      // `fs.realpath` walks the full chain and returns the canonical
      // absolute path; verify containment against that.
      const target = await fs.readlink(src);
      // Compare against the realpath of the extraction root so a chain
      // whose canonical path lands under the same logical root, but
      // via a symlinked tmpdir prefix (notably macOS where `/tmp`
      // resolves to `/private/tmp`), is not incorrectly flagged as
      // an escape.
      let realExtractionRoot: string;
      try {
        realExtractionRoot = await fs.realpath(extractionRoot);
      } catch (err) {
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `tarball symlink ${src} → ${target}: extraction-root realpath failed: ${describeError(err)}`,
        });
      }
      // `path.resolve` produces the absolute path the symlink would
      // dereference to without following any links itself; realpath
      // walks the chain. A dangling symlink — one whose target chain
      // ENOENTs before the final inode — is harmless on disk (it
      // points at a name that does not exist), so the containment
      // check falls back to the literal resolved path in that case.
      // Any other realpath error is fatal; we cannot prove containment
      // and the package is rejected.
      //
      // The fallback anchors the literal resolution at `realpath(src
      // dirname)` rather than the as-declared `dirname(src)`. The
      // dirname already exists on disk (extraction wrote it); realpath
      // walks any symlinks in the prefix so the comparison against
      // `realExtractionRoot` is realpath-vs-realpath on both sides.
      // Without this, platforms whose extraction-root prefix contains
      // symlinks (notably macOS, where `/var/folders/...` resolves to
      // `/private/var/folders/...`) would reject a properly-contained
      // dangling link because the literal path keeps the as-declared
      // prefix while the extraction root has been realpath'd.
      let targetAbs: string;
      try {
        targetAbs = await fs.realpath(path.resolve(path.dirname(src), target));
      } catch (err) {
        if (!isENOENT(err)) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `tarball contains symlink ${src} → ${target} whose target could not be resolved: ${describeError(err)}`,
          });
        }
        let srcDirReal: string;
        try {
          srcDirReal = await fs.realpath(path.dirname(src));
        } catch (dirErr) {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `tarball symlink ${src} → ${target}: dirname realpath failed during dangling-link fallback: ${describeError(dirErr)}`,
          });
        }
        targetAbs = path.resolve(srcDirReal, target);
      }
      const realContainmentRoot = realExtractionRoot.endsWith(path.sep)
        ? realExtractionRoot
        : realExtractionRoot + path.sep;
      if (
        targetAbs !== realExtractionRoot &&
        !targetAbs.startsWith(realContainmentRoot)
      ) {
        throw new ToolLoaderError({
          category: "package.entry.invalid",
          message: `tarball contains symlink ${src} → ${target} that escapes the package extraction directory`,
        });
      }
      try {
        await fs.symlink(target, dest);
      } catch (err) {
        if (!isEEXIST(err)) throw err;
      }
    }
  }
}

interface DirectDep {
  readonly name: string;
  readonly range: string;
  readonly optional: boolean;
}

/**
 * Read the package.json at `extractionDir/package.json` and return the
 * union of `dependencies` and `optionalDependencies`. Each entry is
 * tagged with whether it came from the optional field so the layout
 * pass can decide whether a missing closure entry is fatal (hard dep)
 * or skippable (the resolver's platform filter excluded it from the
 * closure for this host).
 *
 * `dependencies` shadows `optionalDependencies` when the same name
 * appears in both — npm treats the dep as required in that case.
 */
async function readDirectDependencies(
  extractionDir: string,
  entry: { readonly name: string; readonly version: string },
): Promise<DirectDep[]> {
  const pkgJsonRaw = await fs.readFile(
    path.join(extractionDir, "package.json"),
    "utf8",
  );
  let pkg: unknown;
  try {
    pkg = JSON.parse(pkgJsonRaw);
  } catch (err) {
    throw new ToolLoaderError({
      category: "package.entry.invalid",
      message: `malformed package.json in ${entry.name}@${entry.version}: ${describeError(err)}`,
      package: { name: entry.name, version: entry.version },
    });
  }
  const byName = new Map<string, DirectDep>();
  if (pkg === null || typeof pkg !== "object") return [];
  const record: Record<string, unknown> = { ...pkg };
  // A non-string range value (number, null, nested object, array) is
  // a malformed package.json the npm CLI would also reject. Silently
  // dropping it would let the closure resolver later reject the apply
  // with a misleading `package.entry.invalid` for the wrong layer —
  // the malformation is here, not in the closure walk. Surface it as
  // `package.entry.invalid` directly so the operator-facing message
  // points at the bad package.
  //
  // Iteration order matters: write optionalDependencies FIRST, then
  // dependencies. The `dependencies` write overwrites the same key on
  // collision, which is the npm-shadowing rule documented above.
  // Reversing these two blocks would silently make the optional
  // declaration win and demote a hard dependency to optional.
  const optionalDeps = record["optionalDependencies"];
  if (optionalDeps !== undefined) {
    assertDepMapShape(optionalDeps, "optionalDependencies", entry);
    if (optionalDeps !== null && typeof optionalDeps === "object") {
      for (const [name, range] of Object.entries(optionalDeps)) {
        if (typeof range !== "string") {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `package.json field optionalDependencies["${name}"] in ${entry.name}@${entry.version} is ${typeof range}, expected a string range`,
            package: { name: entry.name, version: entry.version },
          });
        }
        byName.set(name, { name, range, optional: true });
      }
    }
  }
  const deps = record["dependencies"];
  if (deps !== undefined) {
    assertDepMapShape(deps, "dependencies", entry);
    if (deps !== null && typeof deps === "object") {
      for (const [name, range] of Object.entries(deps)) {
        if (typeof range !== "string") {
          throw new ToolLoaderError({
            category: "package.entry.invalid",
            message: `package.json field dependencies["${name}"] in ${entry.name}@${entry.version} is ${typeof range}, expected a string range`,
            package: { name: entry.name, version: entry.version },
          });
        }
        byName.set(name, { name, range, optional: false });
      }
    }
  }
  return Array.from(byName.values());
}

/**
 * Reject array-shaped `dependencies` / `optionalDependencies`. The
 * surrounding code narrows with `typeof X === "object"`, which is true
 * for arrays — and `Object.entries(["foo"])` produces `[["0", "foo"]]`,
 * feeding nonsense package names into the closure resolver. Failure
 * downstream is loud but the message points at the wrong layer. Reject
 * at the package-json read with a clear, structured failure instead.
 */
function assertDepMapShape(
  value: unknown,
  field: "dependencies" | "optionalDependencies",
  entry: { readonly name: string; readonly version: string },
): void {
  if (Array.isArray(value)) {
    throw new ToolLoaderError({
      category: "package.entry.invalid",
      message: `package.json#${field} for ${entry.name}@${entry.version} must be an object map of name→range, not an array`,
      package: { name: entry.name, version: entry.version },
    });
  }
}
