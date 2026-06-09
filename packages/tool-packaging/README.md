# @intx/tool-packaging

Hub-side closure resolver and sidecar-side loader for the npm-distributed
tool-package format. The contract below describes what the layered
pipeline actually guarantees end-to-end; downstream code (the apply
protocol, the session service, the deploy-flow integration tests) leans
on it as written.

## Loader resolution contract

A tool package's `interchange.tools` entry, and the JS body it pulls in
transitively, may `import` (or `require`) any bare specifier the
manifest's resolved closure satisfies. The loader builds a per-instance
nested `node_modules/` layout under the scratch directory so each
package's direct dependencies are reachable through Node's standard
ancestor walk; the layout composes recursively, so a transitive's own
transitives are reachable from inside it.

Concretely the loader:

1. Walks every manifest entry and validates that each one is
   registry-chain-consistent: every entry the manifest says lives at a
   registry resolves end-to-end against that registry, and every entry
   that lives in an asset resolves against the asset's `assetMounts`
   map.
2. Materializes every entry into the content-addressable tarball
   cache: bytes pulled from the entry's source on a miss are verified
   through `cache.put`, and the bytes are then unpacked once via
   `cache.extractTarball`. A single sha512 produces a single
   extraction shared across instances.
3. Lays out each entry under `<scratch>/store/<name>/<version>/` by
   hardlinking the file tree from the cache extraction. Each layout
   directory then gets `node_modules/<dep>` symlinked into it pointing
   at the sibling `store/<dep>/<depVersion>/` chosen for that
   requirer. Hardlinks keep on-disk usage to one copy per integrity
   per filesystem; symlinks at the `node_modules/` boundary let
   Node's realpath-based resolver walk to the dep's own layout dir
   (which has its own `node_modules/`).
4. Dynamically imports each top-level entry's `interchange.tools`
   module and collects the `AnnotatedToolFactory` /
   `AnnotatedPluginFactory` values it exports.

Properties that fall out of this layout:

- **Closure satisfies itself.** A top-level package's body, plus every
  transitive it pulls in, resolves bare-specifier imports against the
  manifest closure with no help from the sidecar host's own
  `node_modules`.
- **No hoisting between top-levels.** Each top-level keeps its own
  nested view; two top-levels that disagree on a transitive's version
  get two distinct entries in the store, both materialized.
- **Version conflicts coexist.** Diamond dependencies share a single
  store entry (one symlink target per `(name, version)`); divergent
  versions live as separate store entries and Node's ancestor walk
  picks the right one for each requirer.
- **Runtime-agnostic tarballs.** The loader does no Bun-specific
  resolution. A tool package whose `interchange.tools` entry is a
  `.js` file with no workspace-only deps loads identically on Node
  and Bun. The workspace's built-in tool packages bundle their entries
  through `bin/build-builtins.ts` precisely so the produced tarballs
  satisfy this property.

## Optional-dependency semantics

The closure walker honors npm's "an optional subtree is best-effort"
contract: when an `optionalDependencies` edge fails to resolve — the
packument is absent, the range picks nothing, the registry rejects the
fetch — the whole subtree under that edge is dropped from the manifest
silently. A hard dep of an optional dep is part of that optional
subtree; a failure there drops the parent too, instead of leaving a
parent in the closure whose own hard requirement is missing. Failures
outside any optional subtree abort the walk and surface as a
`manifest.invalid` deploy-apply error.

## Asset modification race

The hub-side `AssetRegistrySource` reads tarball packuments from a
`package-registry` asset at resolution time. The sidecar's
`createPack` then resolves the asset's main ref and serves the tarball
bytes the resolver pinned. A concurrent PUT or DELETE against the
asset's `tarballs/` prefix in the window between the resolver's read
and the sidecar's fetch can cause the apply to fail with
`tarball.missing` or `integrity.mismatch`.

The substrate's `writeTreePreservingPrefix` primitive makes PUTs and
DELETEs against the asset atomic with respect to one another, but it
does not couple them to the sidecar's deploy-apply read. The
intentional behavior is: the apply fails loudly through the existing
`deploy.apply.error` frame channel. There is no silent corruption —
the integrity check on the sidecar side guarantees that mismatched
bytes are rejected — but the operator does see a failed apply if the
asset is mutated concurrently with a launch.

A follow-up that snapshots the asset's commit SHA at resolution time
and replays against it at apply time is in scope if this shows up in
operator practice; it is not implemented today.

## Author constraint: `import.meta.url` after the swap

Tool packages MUST NOT perform call-time dynamic imports relative to
`import.meta.url`. The loader imports each top-level package's
`interchange.tools` entry from a path under `<instanceDir>/pending/`
which the atomic-apply swap step renames to `<instanceDir>/active/`
once the load succeeds. Modules loaded that way therefore carry the
pending path on `import.meta.url`; an `await import(new URL(...,
import.meta.url))` evaluated after the swap completes will fail
because the original path is gone.

Top-level `import` declarations are unaffected — they resolve at
module-init time, before the swap fires — and bare-specifier dynamic
imports that flow through Node's `node_modules` resolver are
unaffected as well. The constraint is specific to URLs derived from
`import.meta.url` at call time.

A future loader change can lift this constraint by realpath-resolving
the entry path or by re-loading the entry from the post-swap
`active/` path; until then, package authors should keep dynamic
imports module-relative through bundlers or package exports.

## Follow-up reference

- **INTR-180**: drizzle mock replacement for tests that introspect
  the package's resolver outputs.
