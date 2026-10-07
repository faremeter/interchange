// @intx/tool-packaging — the single API boundary the rest of the tree
// uses to flow tool packages through the system.
//
// Owns every npm-team dependency (`npm-registry-fetch`,
// `npm-package-arg`, `npm-pick-manifest`, `semver`, `tar`, `ssri`); no
// other package imports those directly. If the npm tooling is ever
// swapped or vendored, this package is the only boundary that changes.
//
// Surface areas:
//
//   - Hub-side (deploy assembly):
//       createClosureResolver({ registries, scopeRouting? })
//         → resolveClosure(pins) → ToolPackageManifest
//
//   - Sidecar-side (deploy apply):
//       createTarballCache / createToolLoader — content-addressable
//       store plus fetch/extract/import of each pinned package.
//       applyAtomic — per-deploy-id staging; maps every loader failure
//       category onto an `ApplyAtomicFailure`.
//       materializeToolPackages — validates a step manifest, applies
//       it, and persists the active deploy id.
//       applyFrozenWorkflowClosure / createWorkflowClosureMaterializer
//       — lay out frozen workflow closures and load definitions
//       through caller-supplied callbacks.

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
  type ScopeRoute,
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
  type LoadManifestArgs,
  type LoadedDirectorFactory,
  type LoadedToolFactory,
  type LoadedToolPackage,
  type LoaderConfig,
  type MaterializeClosureArgs,
  type MaterializeClosureResult,
  type TarballFetcher,
  type ToolLoader,
  ToolLoaderError,
  createToolLoader,
  materializeClosure,
  storeEntryDir,
} from "./loader";

export {
  type ApplyAtomicArgs,
  type ApplyAtomicFailure,
  type ApplyAtomicResult,
  type ApplyAtomicSuccess,
  applyAtomic,
} from "./atomic-apply";

export {
  type ExtractPackageJSONOutcome,
  extractTarballPackageJSON,
} from "./package-json-extract";

export { hostPlatform, parseToolRegistries } from "./materialization-config";

export {
  type StepToolFactory,
  materializeToolPackages,
} from "./tool-materialization";

export {
  type ApplyFrozenWorkflowClosureArgs,
  type AppliedWorkflowClosure,
  applyFrozenWorkflowClosure,
} from "./workflow-closure-apply";

export {
  type WorkflowClosureMaterializerConfig,
  createWorkflowClosureMaterializer,
} from "./workflow-closure-materialization";
