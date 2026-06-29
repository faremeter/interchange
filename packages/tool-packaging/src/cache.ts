// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// Content-addressable tarball cache, shared across agent instances on
// the sidecar.
//
// Tarballs are immutable bytes addressed by their SRI integrity. The
// cache lives at `rootDir/sha512/<2-char>/<rest>/tarball.tgz`. Get
// returns cached bytes or null; put verifies bytes against the
// integrity, writes them atomically, and evicts least-recently-used
// entries until the total cache size is under maxBytes. Evict removes
// one entry (used after extraction discovers a corrupted file on disk).
//
// `maxBytes` covers both the tarball bytes and the size of the
// extracted directory tree the loader hardlinks from. The extraction
// tree dominates disk usage in practice (tarballs are gzip-compressed;
// the unpacked tree is multiples larger), so the cap reflects the
// caller-visible cost of holding an entry.
//
// Integrity is verified on store and re-verified inside
// `extractTarball` before unpacking. `get` returns bytes without
// re-hashing — callers that route through `extractTarball` get the
// extra check for free; callers that consume the bytes directly are
// trusted to validate as appropriate.
//
// `extractTarball` is the second face of the same on-disk store: it
// unpacks the tarball into a sibling `extracted/` directory keyed by
// the same integrity, so the per-instance loader can symlink into a
// stable, deduplicated extraction without re-doing the tar work on
// every apply. The unpack is gated by a per-integrity tmp-and-rename
// dance with the same crash-safety properties as `put`.

import { promises as fs } from "node:fs";
import path from "node:path";
import ssri from "ssri";
import * as tar from "tar";

import { getLogger } from "@intx/log";
import { hexEncode } from "@intx/types";

const logger = getLogger(["sidecar", "tool-packaging", "cache"]);

// Defense-in-depth bound for dirSize recursion. Real npm extractions
// nest a handful of levels deep at most; symlink loops or pathological
// trees would otherwise spin until the process is killed.
const DIR_SIZE_MAX_DEPTH = 20;

// The on-disk layout shards by the first two characters of the
// integrity's base64 payload; entries with shorter payloads cannot
// produce a deterministic shard path. SRI integrities for sha512
// payloads are 88 characters base64, so this bound is purely
// defensive — but it keeps the shard layout's invariant explicit
// rather than implicit in the slice indices.
const MIN_INTEGRITY_PAYLOAD = 2;

export interface TarballCacheConfig {
  readonly rootDir: string;
  readonly maxBytes: number;
}

export interface TarballCache {
  get(integrity: string): Promise<Buffer | null>;
  /**
   * Presence probe. Returns true when an entry for `integrity` is
   * resident on disk. Cheaper than `get` for callers that only need
   * to decide whether a fetch is required — `has` checks file
   * existence without reading bytes or touching atime.
   */
  has(integrity: string): Promise<boolean>;
  put(integrity: string, bytes: Buffer): Promise<void>;
  /**
   * Mark the cache entry for `integrity` as poisoned and remove its
   * tarball bytes immediately so a subsequent `extractTarball` call
   * cannot reuse the on-disk extraction. The extraction directory's
   * physical reclaim is deferred until every in-flight reader released
   * by `extractTarball` has dropped its reference, so an evict that
   * races a concurrent `hardlinkTree` walk of the same extraction does
   * not pull the tree out from under the walk and surface as ENOENT.
   *
   * Until every reader releases, the on-disk extraction is left in
   * place but is no longer reachable via a fresh `extractTarball`
   * (because the tarball blob is gone). Callers that need the bytes
   * back must re-fetch and `put` them, which will trigger a fresh
   * extraction into a new directory once the deferred reclaim
   * completes.
   */
  evict(integrity: string): Promise<void>;
  /**
   * Unpack the cached tarball for `integrity` into a content-addressable
   * extraction directory and return its absolute path along with a
   * `release` callback the caller MUST invoke when it is done walking
   * the directory. The directory is reference-counted: a concurrent
   * `evict` for the same integrity defers the physical removal of the
   * extraction tree until every outstanding `release` has been called.
   *
   * The same path is returned on subsequent calls without re-extracting;
   * concurrent callers for the same integrity each get a path to a
   * fully-populated directory and each get their own `release` handle.
   *
   * Throws if `integrity` is not present in the cache; callers must
   * `put` (or otherwise materialize) the bytes first.
   */
  extractTarball(integrity: string): Promise<{
    readonly dir: string;
    readonly release: () => void;
  }>;
  /**
   * Walk the cache tree and remove any staged tmp paths left behind
   * by a `put` or `extractTarball` that crashed between staging and
   * the final rename. Callers should invoke this once at sidecar
   * boot, before any apply runs. Idempotent: a no-op if the tree
   * holds no orphans.
   *
   * Orphans take the shape `<entryDir>/tarball.tgz.tmp.<pid>.<rand>`
   * (from `put`) and `<entryDir>/extracted.tmp.<pid>.<rand>` (from
   * `extractTarball`). The single-process contract means a tmp path
   * surviving across boots cannot be in use by another process; it
   * is always safe to remove.
   */
  sweepOrphans(): Promise<void>;
  /** Test-only: total bytes currently stored. */
  size(): Promise<number>;
}

/**
 * Thrown by `put` when supplied bytes do not match the supplied
 * integrity. The cache never stores bytes that fail this check.
 */
export class TarballIntegrityMismatchError extends Error {
  readonly integrity: string;

  constructor(integrity: string) {
    super(`tarball bytes do not match integrity ${integrity}`);
    this.name = "TarballIntegrityMismatchError";
    this.integrity = integrity;
  }
}

/**
 * Construct a TarballCache rooted at `config.rootDir`.
 *
 * **Single-process contract.** Pointing two sidecar processes at the
 * same cache root is unsupported. The pid-prefixed staging path is
 * decorative for intra-process races — it keeps two concurrent puts
 * in the same process from clobbering each other's tmp files — but
 * cross-process races on the same integrity can still collide during
 * the final rename (one process moves the staged file into place, the
 * other's rename overwrites or fails depending on the platform's
 * rename-over-existing semantics). The atomicity guarantees in this
 * module assume a single owning process per `rootDir`.
 */
export function createTarballCache(config: TarballCacheConfig): TarballCache {
  if (config.maxBytes <= 0) {
    throw new Error("createTarballCache: maxBytes must be positive");
  }
  const rootDir = config.rootDir;

  // In-process refcount over (integrity, extraction directory) pairs.
  // `extractTarball` increments on success; the returned `release`
  // decrements. `evict` deletes the tarball blob immediately but
  // defers physical removal of the extraction tree until the count
  // reaches zero. This decouples mark-as-bad (atomic, prompt) from
  // physical reclaim (deferred, safe) so an integrity-mismatch evict
  // that races a concurrent `hardlinkTree` walk against the same
  // extraction does not pull the tree out from under the walk and
  // surface as ENOENT mid-readdir.
  //
  // The map keys on integrity, not extraction path, because both the
  // path and the integrity are 1:1 for a content-addressable cache.
  // Cross-process evicts are out of scope: the cache documents a
  // single-process contract, and this refcount only protects against
  // intra-process races between two agents on the same sidecar.
  const extractionRefcounts = new Map<string, number>();
  const pendingEvictions = new Set<string>();

  function acquireExtraction(integrity: string): void {
    const next = (extractionRefcounts.get(integrity) ?? 0) + 1;
    extractionRefcounts.set(integrity, next);
  }

  async function releaseExtraction(integrity: string): Promise<void> {
    const current = extractionRefcounts.get(integrity);
    if (current === undefined || current <= 0) {
      // The release is a caller-driven contract; an extra release
      // without a matching acquire is a programmer error. Log loudly
      // — the call site for this function is wrapped in `void` to
      // keep `release` synchronous for callers, so a throw here would
      // become an unhandled rejection on the microtask queue rather
      // than the immediate, observable failure the contract promises.
      // Logging at error level keeps the failure surfaced through the
      // operator's log pipeline without booby-trapping the process.
      logger.error`cache.release: extraction for ${integrity} released more times than acquired`;
      return;
    }
    if (current === 1) {
      extractionRefcounts.delete(integrity);
      if (pendingEvictions.has(integrity)) {
        pendingEvictions.delete(integrity);
        const entryDirPath = entryDir(integrity);
        try {
          await fs.rm(extractedDir(integrity), {
            recursive: true,
            force: true,
          });
          // The deferred-reclaim path is symmetric with the inline
          // sweep at `evictUntilUnderCap`: empty entry/shard/algorithm
          // parents must be swept too, otherwise every eviction that
          // raced an in-flight reader leaves an orphan empty directory
          // triple on disk that accumulates over the cache's lifetime.
          // `rmdirIfEmpty` is best-effort (ENOTEMPTY when siblings
          // remain) and silently no-ops if a different evict already
          // pruned the parent.
          await rmdirIfEmpty(entryDirPath);
          await rmdirIfEmpty(path.dirname(entryDirPath));
          await rmdirIfEmpty(path.dirname(path.dirname(entryDirPath)));
        } catch (err) {
          logger.warn`deferred eviction of ${extractedDir(integrity)} failed: ${err instanceof Error ? err.message : String(err)}`;
        }
      }
      return;
    }
    extractionRefcounts.set(integrity, current - 1);
  }

  function entryDir(integrity: string): string {
    const { algorithm, encoded } = parseIntegrity(integrity);
    if (encoded.length < MIN_INTEGRITY_PAYLOAD) {
      throw new Error(`integrity payload too short to shard: ${integrity}`);
    }
    return path.join(
      rootDir,
      algorithm,
      encoded.slice(0, MIN_INTEGRITY_PAYLOAD),
      encoded.slice(MIN_INTEGRITY_PAYLOAD),
    );
  }

  function entryPath(integrity: string): string {
    return path.join(entryDir(integrity), "tarball.tgz");
  }

  function extractedDir(integrity: string): string {
    return path.join(entryDir(integrity), "extracted");
  }

  /**
   * Advance the entry's atime so the cap-driven LRU sweep treats the
   * access as recent. Mirrors the explicit `utimes` in `cache.get`:
   * read-only mounts, `noatime`/`relatime` mounts, and FUSE
   * filesystems that refuse `utimes` must not fail an otherwise-
   * successful access. Log at debug and move on.
   */
  async function touchEntryAtime(integrity: string): Promise<void> {
    const file = entryPath(integrity);
    try {
      const now = new Date();
      const stat = await fs.stat(file);
      await fs.utimes(file, now, stat.mtime);
    } catch (err) {
      logger.debug`extractTarball atime update failed for ${file}; LRU ordering will be stale: ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  async function listEntries(): Promise<
    {
      integrity: string;
      entryDir: string;
      tarballPath: string;
      extractedPath: string;
      tarballSize: number;
      extractedSize: number;
      atimeMs: number;
    }[]
  > {
    const out: {
      integrity: string;
      entryDir: string;
      tarballPath: string;
      extractedPath: string;
      tarballSize: number;
      extractedSize: number;
      atimeMs: number;
    }[] = [];
    let rootExists = false;
    try {
      await fs.access(rootDir);
      rootExists = true;
    } catch {
      // rootDir does not exist yet; nothing to list.
    }
    if (!rootExists) return out;

    const algorithms = await fs.readdir(rootDir);
    for (const alg of algorithms) {
      const algDir = path.join(rootDir, alg);
      const shardEntries = await fs.readdir(algDir).catch(() => []);
      for (const shard of shardEntries) {
        const shardDir = path.join(algDir, shard);
        const leafEntries = await fs.readdir(shardDir).catch(() => []);
        for (const leaf of leafEntries) {
          const entryDir = path.join(shardDir, leaf);
          const file = path.join(entryDir, "tarball.tgz");
          try {
            const stat = await fs.stat(file);
            const extractedPath = path.join(entryDir, "extracted");
            // Contain dirSize failures here so a single corrupt
            // extraction tree (symlink-depth overflow, permission
            // refusal mid-walk, etc.) does not break cache accounting
            // for every subsequent put. The entry is still surfaced —
            // with `extractedSize: 0` — so the eviction sweep can
            // still reach it; the warning names the entry so the
            // operator can clear the broken tree by hand.
            let extractedSize = 0;
            try {
              extractedSize = await dirSize(extractedPath);
            } catch (err) {
              logger.warn`dirSize failed for ${extractedPath}; accounting that entry as 0 extracted bytes: ${err instanceof Error ? err.message : String(err)}`;
            }
            // Recover the integrity from the on-disk path. The layout
            // writes `<algorithm>/<sharded payload>` with `/` → `-`
            // substitution at write time (parseIntegrity sanitization);
            // reverse the substitution to land back on the SRI form
            // the caller passed. Standard base64 never produces `-`
            // organically, so reversing `-` → `/` is unambiguous.
            const integrity = `${alg}-${(shard + leaf).replace(/-/g, "/")}`;
            out.push({
              integrity,
              entryDir,
              tarballPath: file,
              extractedPath,
              tarballSize: stat.size,
              extractedSize,
              atimeMs: stat.atimeMs,
            });
          } catch {
            // File missing or unreadable; skip.
          }
        }
      }
    }
    return out;
  }

  /**
   * Sum the on-disk size of every regular file under `dir` recursively.
   * Returns 0 when `dir` does not exist. Hardlinks are counted once
   * per inode would be ideal, but `node:fs` does not expose inode-
   * dedup walking without a manual ino map; the loader hardlinks
   * extraction trees into per-instance store dirs, so the extraction
   * tree itself holds one link per file and `stat.size` per entry is
   * the right number to charge to this cache entry.
   *
   * ACCOUNTING vs. DISK USAGE: `maxBytes` bounds the sum reported by
   * this walker, not the actual disk consumption of the cache plus
   * its downstream hardlink consumers. The loader's per-instance
   * store dirs share inodes with `cache/extracted/`; evicting an
   * entry here drops the cache's reference but the underlying file
   * survives as long as any per-instance dir still points at it.
   * The cap is a steady-state ceiling on the cache tree's own
   * accounting, not a disk-usage limit. Concurrent vanishes during
   * the walk are silently dropped via the inner `lstat` try/catch
   * below; under the single-process contract this is rare, but the
   * returned `total` reports the cap-relevant sum within one sweep's
   * resolution rather than a strictly-consistent snapshot.
   *
   * Symlinks (both file- and directory-targeted) are NOT traversed:
   * `lstat` here returns the link itself rather than its target, and
   * `dirent.isSymbolicLink()` is the entry-walk equivalent. An npm
   * tarball that ships symlinks is preserved verbatim by the loader's
   * hardlink-tree pass; charging the link's own size (zero in our
   * accounting) avoids both symlink-loop divergence and double-counting
   * the target through whatever path also names it directly.
   *
   * Recursion is capped at `DIR_SIZE_MAX_DEPTH` as a defense-in-depth
   * against a pathological tarball whose real-directory nesting exceeds
   * what the cache layout (flat npm trees) ever expects.
   */
  async function dirSize(dir: string, depth = 0): Promise<number> {
    if (depth > DIR_SIZE_MAX_DEPTH) {
      throw new Error(
        `dirSize depth exceeded ${String(DIR_SIZE_MAX_DEPTH)} at ${dir}; likely a symlink loop in the cache extraction tree`,
      );
    }
    let total = 0;
    let entries: import("node:fs").Dirent[];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch (err) {
      if (isENOENT(err)) return 0;
      throw err;
    }
    for (const entry of entries) {
      const abs = path.join(dir, entry.name);
      if (entry.isSymbolicLink()) {
        continue;
      }
      if (entry.isDirectory()) {
        total += await dirSize(abs, depth + 1);
      } else if (entry.isFile()) {
        try {
          const stat = await fs.lstat(abs);
          total += stat.size;
        } catch {
          // Concurrent removal; ignore.
        }
      }
    }
    return total;
  }

  async function evictUntilUnderCap(justWritten?: string): Promise<void> {
    const entries = await listEntries();
    const total = entries.reduce(
      (sum, e) => sum + e.tarballSize + e.extractedSize,
      0,
    );
    if (total <= config.maxBytes) return;

    // The just-written entry is the caller's reason for sweeping; if
    // it is a single tarball larger than `maxBytes`, evicting it now
    // would force a refetch on the next apply and the new fetch would
    // be evicted again — perpetual churn. Treat it as ineligible for
    // this sweep so the immediate apply succeeds; subsequent puts can
    // evict it normally once it is no longer the LRU-newest.
    const justWrittenPath =
      justWritten !== undefined ? entryPath(justWritten) : undefined;
    const evictable = entries.filter(
      (e) => justWrittenPath === undefined || e.tarballPath !== justWrittenPath,
    );
    evictable.sort((a, b) => a.atimeMs - b.atimeMs);
    let remaining = total;
    for (const e of evictable) {
      if (remaining <= config.maxBytes) break;
      const reclaimable = e.tarballSize + e.extractedSize;
      try {
        // Drop the tarball blob immediately so a fresh `extractTarball`
        // call cannot reuse the on-disk extraction tree from this
        // entry. The extraction tree's physical reclaim is gated on
        // the in-flight refcount — concurrent readers from another
        // agent holding a `release` handle would otherwise see ENOENT
        // mid-readdir if we rm-ed it out from under them. Defer to the
        // last `release` to do the actual rm; if there are no readers
        // (the common case), reclaim is immediate.
        await fs.unlink(e.tarballPath);
        if ((extractionRefcounts.get(e.integrity) ?? 0) > 0) {
          pendingEvictions.add(e.integrity);
        } else {
          await fs.rm(e.extractedPath, { recursive: true, force: true });
          // Sweep the now-empty entry/shard/algorithm directories so
          // listEntries does not accumulate O(historical-evictions)
          // cost over the cache's lifetime. ENOTEMPTY means a sibling
          // entry still occupies the parent; that is the expected case
          // for any cache holding more than one entry per shard, so
          // swallow it silently and move on.
          await rmdirIfEmpty(e.entryDir);
          await rmdirIfEmpty(path.dirname(e.entryDir));
          await rmdirIfEmpty(path.dirname(path.dirname(e.entryDir)));
        }
        remaining -= reclaimable;
        logger.debug`evicted ${e.tarballPath} (tarball ${String(e.tarballSize)} + extracted ${String(e.extractedSize)} bytes); cache now ${String(remaining)} bytes`;
      } catch (err) {
        logger.warn`failed to evict ${e.tarballPath}: ${err instanceof Error ? err.message : String(err)}`;
      }
    }
    if (remaining > config.maxBytes) {
      // This warning may fire twice for the same integrity in one
      // loader pass — once after `put` writes the bytes and once after
      // `extractTarball` unpacks them, since both invoke the sweep
      // with the same `justWritten` integrity. Operator log
      // aggregation should dedup on `integrity` if the noise becomes
      // a problem at scale.
      logger.warn`cache exceeds maxBytes by ${String(remaining - config.maxBytes)} bytes after sweep; the just-written entry is exempt from its own sweep`;
    }
  }

  async function rmdirIfEmpty(dir: string): Promise<void> {
    try {
      await fs.rmdir(dir);
    } catch (err) {
      // ENOTEMPTY: a sibling entry still occupies the parent. ENOENT:
      // the directory was already removed (e.g. by a concurrent
      // evict). Both are expected; anything else propagates so a
      // misconfigured cache root surfaces instead of corrupting the
      // shard layout silently.
      if (isENOTEMPTY(err) || isENOENT(err)) return;
      throw err;
    }
  }

  return {
    async get(integrity) {
      const file = entryPath(integrity);
      let bytes: Buffer;
      try {
        bytes = await fs.readFile(file);
      } catch (err) {
        if (isENOENT(err)) return null;
        throw err;
      }
      // Touch atime so LRU ordering reflects this access. utimes
      // requires both atime and mtime; mtime is preserved. The atime
      // update is best-effort LRU bookkeeping: a read-only mount, a
      // noatime/relatime mount option, or a FUSE filesystem that
      // refuses utimes must not fail an otherwise-successful read.
      // Log at debug and return the bytes the caller already paid for.
      try {
        const now = new Date();
        const stat = await fs.stat(file);
        await fs.utimes(file, now, stat.mtime);
      } catch (err) {
        logger.debug`cache.get atime update failed for ${file}; LRU ordering will be stale: ${err instanceof Error ? err.message : String(err)}`;
      }
      return bytes;
    },

    async has(integrity) {
      const file = entryPath(integrity);
      try {
        await fs.access(file);
        return true;
      } catch (err) {
        if (isENOENT(err)) return false;
        throw err;
      }
    },

    async put(integrity, bytes) {
      const matched = ssri.checkData(bytes, integrity);
      if (matched === false) {
        throw new TarballIntegrityMismatchError(integrity);
      }

      const dir = entryDir(integrity);
      await fs.mkdir(dir, { recursive: true });
      const file = entryPath(integrity);
      // Add per-call randomness so two concurrent put()s for the same
      // integrity (e.g. two agents on the same sidecar racing into the
      // first apply) do not collide on the temp path.
      const tmp = `${file}.tmp.${String(process.pid)}.${hexEncode(crypto.getRandomValues(new Uint8Array(8)))}`;
      await fs.writeFile(tmp, bytes);
      // No fsync before rename: the cache is content-addressable and
      // rebuildable. A crash between write and rename leaves an orphaned
      // .tmp file that `sweepOrphans` clears on the next boot; a crash
      // after rename but before the data hits disk forces a re-fetch on
      // the next miss, validated by SRI. The persistence requirement
      // that earns an fsync is the apply pipeline's active-deploy-id,
      // not cache entries.
      await fs.rename(tmp, file);

      // No cap sweep here. `put` only stages the tarball bytes; the
      // entry's full on-disk footprint (tarball + extracted tree) is
      // not knowable until `extractTarball` lands. Sweeping now would
      // make the LRU decision against a half-sized entry — both
      // double-counting noise (the warn fires twice for one miss when
      // the sweep runs in both `put` and `extractTarball`) and a
      // semantically wrong choice (the entry will grow, possibly past
      // `maxBytes`, after the sweep already decided which neighbors
      // to evict). The sweep belongs in `extractTarball` once the
      // entry's bytes-on-disk are fully realized.
    },

    async evict(integrity) {
      const file = entryPath(integrity);
      const extracted = extractedDir(integrity);
      try {
        await fs.unlink(file);
        logger.debug`evicted cache entry for ${integrity}`;
      } catch (err) {
        if (!isENOENT(err)) throw err;
      }
      // The extraction is derived from the tarball bytes and is
      // useless once the tarball is gone. If a hardlinkTree walk is
      // in-flight for the same integrity, removing the tree now would
      // surface as ENOENT mid-walk; defer the physical reclaim until
      // every outstanding `release` from `extractTarball` has fired.
      // With no outstanding readers the reclaim runs inline.
      const inFlight = extractionRefcounts.get(integrity) ?? 0;
      if (inFlight > 0) {
        pendingEvictions.add(integrity);
        logger.debug`deferring extraction reclaim for ${integrity}: ${String(inFlight)} reader(s) in flight`;
        return;
      }
      await fs.rm(extracted, { recursive: true, force: true });
      // Symmetric with the inline cap-driven sweep: prune the now-empty
      // entry/shard/algorithm parents so `listEntries` does not
      // accumulate O(historical-evictions) cost. `rmdirIfEmpty`
      // tolerates ENOTEMPTY (siblings remain) silently.
      const entryDirPath = entryDir(integrity);
      await rmdirIfEmpty(entryDirPath);
      await rmdirIfEmpty(path.dirname(entryDirPath));
      await rmdirIfEmpty(path.dirname(path.dirname(entryDirPath)));
    },

    async extractTarball(integrity) {
      const finalDir = extractedDir(integrity);

      // Handle factory. The acquire MUST have already happened by the
      // time we call this — the cache-hit path acquires before the
      // stat (so a concurrent evict cannot race in between stat
      // resolution and acquire), the unpack path acquires after a
      // successful rename. Either way, `handOut` only constructs the
      // release-pair, it does not increment the refcount itself.
      const handOut = (): { dir: string; release: () => void } => {
        let released = false;
        return {
          dir: finalDir,
          release: () => {
            if (released) return;
            released = true;
            // Fire-and-forget: deferred reclaim runs asynchronously,
            // but release() returns synchronously so the loader's
            // walk-complete site does not need to await. Reclaim
            // failures are logged inside releaseExtraction.
            void releaseExtraction(integrity);
          },
        };
      };

      // Acquire BEFORE probing the on-disk extraction. A concurrent
      // `evict()` running between a stat-then-acquire would see
      // refcount 0, take the inline-reclaim path, and remove the tree
      // before the reader's acquire fires; the reader would then
      // receive a `dir` pointing at a path that has been (or is
      // being) unlinked. Acquiring first pins the refcount so any
      // concurrent evict routes through `pendingEvictions` instead,
      // and a stat miss (no extraction yet) releases the speculative
      // refcount before falling through to the unpack path.
      acquireExtraction(integrity);
      try {
        const stat = await fs.stat(finalDir);
        if (stat.isDirectory()) {
          // Touch the tarball's atime so the cap-driven LRU sweep
          // sees this access. The loader's hot path is `cache.has` +
          // `cache.extractTarball`; `cache.get` is the only other
          // entry point that calls `utimes`, and the loader does not
          // use it. Without this, hot integrities accessed only
          // through this path stay LRU-stale at their put time and
          // become preferential eviction targets on noatime/relatime
          // mounts. Best-effort, same as `get`.
          await touchEntryAtime(integrity);
          return handOut();
        }
      } catch (err) {
        if (!isENOENT(err)) {
          void releaseExtraction(integrity);
          throw err;
        }
      }
      // Stat missed; keep the speculative refcount HELD through the
      // entire unpack path so a concurrent evict cannot observe
      // refcount 0 between any await and reclaim either the tarball
      // bytes, the extraction tree, or the entry/shard/algorithm
      // parent directories (rmdirIfEmpty cascades up to those). With
      // the refcount pinned, evict routes through pendingEvictions
      // and the reader's unpack work observes a stable filesystem.
      try {
        // Whole tarball is read into memory for SRI verification and
        // tar extraction. The bytes only land in the cache after
        // passing the upload-time cap (`HUB_MAX_TARBALL_BYTES` on the
        // hub edge) for asset-sourced tarballs, or the fetch-time cap
        // (`maxRegistryTarballBytes` in the loader's HTTP fetcher)
        // for registry-sourced tarballs — both default to 10 MiB. At
        // those caps and the small concurrent-extraction count the
        // memory footprint is bounded; a streaming path (pipe
        // `fs.createReadStream` through `ssri.integrityStream` and
        // then into `tar.extract`) is the obvious optimization if
        // either cap grows materially.
        const bytes = await fs.readFile(entryPath(integrity)).catch((err) => {
          if (isENOENT(err)) {
            throw new Error(
              `extractTarball: tarball bytes for ${integrity} are not in the cache`,
            );
          }
          throw err;
        });

        // Re-verify the on-disk bytes against the integrity before
        // unpacking. `put` validates on store, but bitrot between
        // store and read can corrupt the payload in ways tar's
        // structural checks miss (a flipped bit inside a compressed
        // block can unpack to syntactically-valid but semantically-
        // wrong content that then surfaces as a far-removed dynamic-
        // import failure). Re-hashing here turns that failure into a
        // structured TarballIntegrityMismatchError the loader can
        // react to.
        const matched = ssri.checkData(bytes, integrity);
        if (matched === false) {
          throw new TarballIntegrityMismatchError(integrity);
        }

        // Stage the unpack into a per-call tmp directory and rename
        // it into place so a concurrent extractor either observes no
        // extraction (and stages its own) or a complete one. A crash
        // mid-unpack leaves an orphaned `.tmp.*` directory that
        // `sweepOrphans` clears on the next boot; a crash after the
        // rename leaves a valid extraction for the next caller.
        const stagingDir = `${finalDir}.tmp.${String(process.pid)}.${hexEncode(crypto.getRandomValues(new Uint8Array(8)))}`;
        await fs.mkdir(stagingDir, { recursive: true });
        try {
          await new Promise<void>((resolve, reject) => {
            const stream = tar.extract({ cwd: stagingDir, strip: 1 });
            stream.on("error", reject);
            // `close` is tar's documented post-flush event — it fires
            // after every entry has been written and the parser has
            // released its descriptors. `finish` fires earlier (when
            // the writable side closes) and can race the FS writes
            // the staged rename relies on.
            stream.on("close", resolve);
            stream.end(bytes);
          });
        } catch (err) {
          await fs.rm(stagingDir, { recursive: true, force: true });
          throw err;
        }

        try {
          await fs.rename(stagingDir, finalDir);
        } catch (err) {
          // POSIX rename returns ENOTEMPTY when the target is a
          // non-empty directory; EEXIST is a fallback for
          // filesystem-dependent cases (notably macOS HFS+ and some
          // FUSE mounts that surface EEXIST in lieu of ENOTEMPTY).
          // Either way means a concurrent extractor won the race: the
          // winner's directory is the canonical one, so sweep our
          // staging and return the existing path.
          await fs.rm(stagingDir, { recursive: true, force: true });
          if (!isEEXIST(err) && !isENOTEMPTY(err)) throw err;
        }

        // Extraction is a write that the eviction sweep needs to
        // charge against `maxBytes`. The integrity is treated as
        // just-written so the cap covers the entry's tarball +
        // extracted total while protecting this entry from its own
        // sweep.
        await evictUntilUnderCap(integrity);

        await touchEntryAtime(integrity);
        return handOut();
      } catch (err) {
        // Release the refcount on failure; the caller will not call
        // the handle's `release` because no handle was returned.
        void releaseExtraction(integrity);
        throw err;
      }
    },

    async sweepOrphans() {
      // The shard layout is `<rootDir>/<algorithm>/<2-char>/<rest>/`.
      // Orphan tmp paths live at the leaf entry directory level — both
      // `put` and `extractTarball` stage siblings of the canonical
      // `tarball.tgz` / `extracted` paths. Walk down to the entry-dir
      // depth and remove anything matching the `.tmp.<pid>.<rand>`
      // sibling pattern.
      let algorithms: string[];
      try {
        algorithms = await fs.readdir(rootDir);
      } catch (err) {
        if (isENOENT(err)) return;
        throw err;
      }
      for (const alg of algorithms) {
        const algDir = path.join(rootDir, alg);
        const shardEntries = await fs.readdir(algDir).catch(() => []);
        for (const shard of shardEntries) {
          const shardDir = path.join(algDir, shard);
          const leafEntries = await fs.readdir(shardDir).catch(() => []);
          for (const leaf of leafEntries) {
            const entryDir = path.join(shardDir, leaf);
            let children: string[];
            try {
              children = await fs.readdir(entryDir);
            } catch {
              continue;
            }
            for (const child of children) {
              if (!isOrphanTmpName(child)) continue;
              const abs = path.join(entryDir, child);
              try {
                await fs.rm(abs, { recursive: true, force: true });
                logger.debug`swept orphan cache tmp ${abs}`;
              } catch (err) {
                logger.warn`failed to sweep orphan cache tmp ${abs}: ${err instanceof Error ? err.message : String(err)}`;
              }
            }
          }
        }
      }
    },

    async size() {
      const entries = await listEntries();
      return entries.reduce(
        (sum, e) => sum + e.tarballSize + e.extractedSize,
        0,
      );
    },
  };
}

// Orphan tmp names take the form `<base>.tmp.<pid>.<hex>` where
// `<base>` is `tarball.tgz` (from `put`) or `extracted` (from
// `extractTarball`). Match the `.tmp.` infix on a known prefix so a
// future on-disk addition the cache layout does not accidentally fall
// under the sweep.
function isOrphanTmpName(name: string): boolean {
  return (
    name.startsWith("tarball.tgz.tmp.") || name.startsWith("extracted.tmp.")
  );
}

function isENOENT(err: unknown): boolean {
  return errCode(err) === "ENOENT";
}

function isEEXIST(err: unknown): boolean {
  return errCode(err) === "EEXIST";
}

function isENOTEMPTY(err: unknown): boolean {
  return errCode(err) === "ENOTEMPTY";
}

function errCode(err: unknown): string | null {
  if (err === null || typeof err !== "object") return null;
  if (!("code" in err)) return null;
  const c = (err as { code: unknown }).code;
  return typeof c === "string" ? c : null;
}

// Standard base64 alphabet (`A-Z`, `a-z`, `0-9`, `+`, `/`, `=`). The
// shard layout assumes this alphabet; see parseIntegrity below for
// why.
const STANDARD_BASE64_PATTERN = /^[A-Za-z0-9+/=]+$/;

function parseIntegrity(integrity: string): {
  algorithm: string;
  encoded: string;
} {
  // SRI form: "<algorithm>-<base64>[?<options>]" (per W3C SRI). We
  // accept the basic form; ssri.parse normalizes more elaborate input
  // but we want a stable on-disk layout independent of options.
  const dash = integrity.indexOf("-");
  if (dash === -1) {
    throw new Error(`integrity is not in SRI form: ${integrity}`);
  }
  const algorithm = integrity.slice(0, dash);
  const rest = integrity.slice(dash + 1);
  const queryAt = rest.indexOf("?");
  const encoded = queryAt === -1 ? rest : rest.slice(0, queryAt);
  // `/` is the only base64 alphabet character that would create
  // accidental nested directories on disk; escape it with `-`, which
  // is not in standard base64. `+` and `=` are filesystem-safe and
  // kept intact so the shard's first two characters carry their
  // original meaning and `entryPath` is injective on integrity input.
  //
  // Assumes standard base64 (`A-Z`, `a-z`, `0-9`, `+`, `/`, `=`) on
  // input. Base64url-encoded integrities (which use `-` and `_` in
  // place of `+` and `/`) would break injectivity because a literal
  // `-` already appears in the input. The npm registry uses standard
  // base64 for `dist.integrity`; if a registry ever serves
  // base64url, the cache layout has to change before this parser
  // will round-trip — fail loudly here rather than producing
  // colliding shard paths on disk.
  if (!STANDARD_BASE64_PATTERN.test(encoded)) {
    throw new Error(
      `integrity payload ${JSON.stringify(encoded)} is not standard base64; non-standard-base64 (e.g. base64url) integrities are not supported. If migrating to base64url, update the cache layout first.`,
    );
  }
  const sanitized = encoded.replace(/\//g, "-");
  return { algorithm, encoded: sanitized };
}
