// @intx/tool-packaging — the single API boundary the rest of the tree
// uses to flow tool packages through the system.
//
// Owns every npm-team dependency (`npm-registry-fetch`,
// `npm-package-arg`, `npm-pick-manifest`, `semver`, `tar`, `ssri`). No
// other package in this monorepo imports those directly — consumers
// reach for `@intx/tool-packaging`'s clean surface instead. If the
// underlying npm tooling ever needs to be swapped or vendored, this
// package is the only boundary that has to change.
//
// Two surface areas:
//
//   - Hub-side (deploy assembly):
//       createClosureResolver({ source })
//         → resolveClosure(pins) → ToolPackageManifest
//
//   - Sidecar-side (deploy apply):
//       createTarballCache({ rootDir, maxBytes }) — content-addressable
//       store keyed by SRI integrity.
//       materializeClosure({ manifest, cache, fetchTarball, … }) — fetches,
//       extracts, and lays out a closure without importing any of it.
//       createTarballFetcher({ … }) — the production fetcher it takes.
//       parseToolRegistries(raw) and hostPlatform(os, cpu) are the
//       registry-map and host-platform boundary that apply consumes.
//       applyFrozenWorkflowClosure({ …, host }) — lays out a frozen
//       workflow closure for the workflow child to evaluate, importing
//       none of it.
//       createWorkflowClosureMaterializer({ …, host, materializeAssets })
//       — lays out a probe frame's frozen closure without importing
//       author code. Asset delivery is the caller-supplied callback.

export {
  type ClosureResolver,
  type ClosureResolverConfig,
  type MaterializedRef,
  type Packument,
  type PackumentFetcher,
  type PackumentVersion,
  type PeerDependencyViolation,
  type RegistryConfig,
  type RegistrySource,
  AssetRegistrySource,
  HttpRegistrySource,
  ManifestInvalidError,
  createClosureResolver,
  parsePin,
} from "./resolver";

export {
  type TarballCache,
  type TarballCacheConfig,
  TarballIntegrityMismatchError,
  createTarballCache,
} from "./cache";

export {
  type HostPlatform,
  type MaterializeClosureArgs,
  type MaterializeClosureResult,
  materializeClosure,
  storeEntryDir,
} from "./store-layout";

export {
  type TarballFetcher,
  type TarballFetcherConfig,
  createTarballFetcher,
} from "./tarball-fetcher";

export {
  type ClosureMaterializationErrorCategory,
  ClosureMaterializationError,
} from "./materialization-internal";

export {
  type ExtractPackageJSONOutcome,
  extractTarballPackageJSON,
} from "./package-json-extract";

export { hostPlatform, parseToolRegistries } from "./materialization-config";

export {
  type ApplyFrozenWorkflowClosureArgs,
  type AppliedWorkflowClosure,
  applyFrozenWorkflowClosure,
} from "./workflow-closure-apply";

export {
  type WorkflowClosureMaterializerConfig,
  createWorkflowClosureMaterializer,
} from "./workflow-closure-materialization";
