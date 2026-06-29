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
//       createClosureResolver({ registries, scopeRouting? })
//         → resolveClosure(pins) → ToolPackageManifest
//
//   - Sidecar-side (deploy apply):
//       createTarballCache({ rootDir, maxBytes }) — content-addressable
//       store keyed by SRI integrity.
//       createToolLoader({ cache, registries, host, … }) — fetches,
//       extracts, and dynamic-imports each pinned package.
//       applyAtomic({ manifest, loader, … }) — per-deploy-id apply
//       protocol that stages each deploy into its own never-renamed
//       directory and maps every loader failure category onto an
//       `ApplyAtomicFailure` the caller routes to the
//       `deploy.apply.error` frame channel.

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
  type TarballFetcher,
  type ToolLoader,
  ToolLoaderError,
  createToolLoader,
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
