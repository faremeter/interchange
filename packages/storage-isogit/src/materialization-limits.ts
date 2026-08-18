// Ceilings that bound the cost of materializing an UNTRUSTED git pack (a pushed
// asset) onto the host. The inline payload cap bounds COMPRESSED bytes; these
// bound what the pack expands to -- object count at index time and the file
// bytes / inode count a checkout writes to disk -- so a small pack cannot inflate
// to gigabytes or exhaust inodes on a shared host.
//
// The values are generous DoS ceilings, not tight quotas: a legitimate source
// workflow tree (source code) is a few megabytes and a few hundred files, well
// under these. They live here as one shared default so the probe, deploy, and
// skill/tarball callers cannot drift; a caller may pass its own limits when it
// owns a stricter policy.

export interface PackMaterializationLimits {
  /** Reject a pack whose header declares more than this many objects. */
  readonly maxPackObjects: number;
  /**
   * Reject a pack whose header declares a single object larger than this,
   * BEFORE it is inflated. Bounds the index-time memory a single highly
   * compressible blob (a "zip bomb") could allocate.
   */
  readonly maxObjectInflatedBytes: number;
  /** Reject a checkout once the cumulative bytes written exceed this. */
  readonly maxTreeBytes: number;
  /** Reject a checkout once the cumulative files + directories exceed this. */
  readonly maxTreeEntries: number;
}

export const DEFAULT_PACK_MATERIALIZATION_LIMITS: PackMaterializationLimits = {
  maxPackObjects: 500_000,
  maxObjectInflatedBytes: 64 * 1024 * 1024,
  maxTreeBytes: 256 * 1024 * 1024,
  maxTreeEntries: 100_000,
};
