# @intx/tool-packaging

Hub-side closure resolver and sidecar-side closure materialization for the npm-distributed
tool-package format. The contract below describes what the layered
pipeline actually guarantees end-to-end; downstream code (the session
service, the deploy-flow integration tests) leans
on it as written.

## Closure resolution contract

A closure package's body, and the JS it pulls in transitively, may
`import` (or `require`) any bare specifier the manifest's resolved
closure satisfies. Materialization builds a per-instance
nested `node_modules/` layout under the scratch directory so each
package's direct dependencies are reachable through Node's standard
ancestor walk; the layout composes recursively, so a transitive's own
transitives are reachable from inside it.

Concretely, materialization:

1. Walks every manifest entry and validates that each one is
   source-consistent: every entry the manifest says lives at a
   registry resolves end-to-end against that registry; an asset entry
   whose `package.format` is `"tarball"` resolves against the asset's
   `assetMounts` map; an asset entry whose `package.format` is
   `"source"` (a git subtree, used by source-format workflow
   definitions) resolves against the separate `gitDirs` map.
2. Materializes every entry into the content-addressable tarball
   cache: bytes pulled from the entry's source on a miss are verified
   through `cache.put`, and the bytes are then unpacked once via
   `cache.extractTarball`. A single sha512 produces a single
   extraction shared across instances. A `"source"` asset entry is the
   exception: it has no tarball and no SRI, so it bypasses the cache
   entirely — its subtree is checked out in place from the indexed
   `gitDir` (`materializeGitEntry`) and verified against the frozen git
   `treeOid` rather than an sha512.
3. Lays out each entry under `<scratch>/store/<name>/<version>/` by
   copying the file tree from the cache extraction — or, for a
   `"source"` entry, from its git checkout. Each layout directory then
   gets `node_modules/<dep>` symlinked into it pointing at the sibling
   `store/<dep>/<depVersion>/` chosen for that requirer. Symlinks at
   the `node_modules/` boundary let Node's realpath-based resolver walk
   to the dep's own layout dir (which has its own `node_modules/`).

Nothing in the closure is imported here; the workflow child evaluates it.

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
- **Runtime-agnostic tarballs.** The layout does no Bun-specific
  resolution. A package whose entry is a `.js` file with no
  workspace-only deps loads identically on Node
  and Bun.

## Optional-dependency semantics

The closure walker honors npm's "an optional subtree is best-effort"
contract: when an `optionalDependencies` edge fails to resolve — the
packument is absent, the range picks nothing, the registry rejects the
fetch — the whole subtree under that edge is dropped from the manifest
silently. A hard dep of an optional dep is part of that optional
subtree; a failure there drops the parent too, instead of leaving a
parent in the closure whose own hard requirement is missing. Failures
outside any optional subtree abort the walk and fail the deploy.

## Asset modification race

The Hub reads and packs a source asset at one commit, so a concurrent PUT or
DELETE against the asset's `tarballs/` prefix cannot change the bytes of a
deploy. The sidecar still verifies each tarball's SRI before it extracts it.

## Call-time imports and `import.meta.url`

Tool packages may resolve files relative to their own on-disk location
at run time — `import.meta.url`, `require.resolve()`, or an
`await import(new URL("./sibling.js", import.meta.url))` evaluated long
after the module first loaded. A closure's packages run from a path
under `<instanceDir>/packages/` that nothing moves, so a module's
`import.meta.url` stays valid for the life of the deployment.
See `ASSET-LAYOUT.md` for the on-disk layout and retention rules.

## Follow-up reference

- **INTR-180**: drizzle mock replacement for tests that introspect
  the package's resolver outputs.
