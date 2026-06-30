// eslint-disable-next-line @typescript-eslint/triple-slash-reference -- npm-team packages ship no types; declarations.d.ts must be visible to downstream typecheckers that import from this package's source.
/// <reference path="./declarations.d.ts" />
// Hub-side npm closure resolver.
//
// Walks a list of agent-pinned tool packages, resolves each pin against
// a configured registry source (HTTP-backed `npm-registry-fetch` or an
// in-process `package-registry` asset), recurses on transitive
// `dependencies` and `optionalDependencies`, and produces a
// `ToolPackageManifest` carrying the full pinned closure with per-entry
// source tags and integrity strings.
//
// Per-package responsibilities:
//
//   - Spec parsing via npm-package-arg.
//   - Packument fetching via a `RegistrySource`:
//       - `HttpRegistrySource` wraps `npm-registry-fetch`.
//       - `AssetRegistrySource` opens tarballs out of a
//         `package-registry` asset via the asset service's in-process
//         read API.
//   - Version picking via npm-pick-manifest.
//   - Scope routing: `@scope/foo` requests can be routed to a specific
//     registry name from the scopeRouting table; everything else uses
//     the registry named by `defaultRegistry`.
//   - Per-entry source materialization: the walker asks the
//     `RegistrySource` for the entry's `source` and `tarballUrl`. HTTP
//     sources emit `kind: "registry"` plus the picked tarball URL;
//     asset sources emit `kind: "asset"` carrying the asset id and the
//     in-asset path.
//   - Peer-dependency validation: peerDependencies declared by any
//     entry must be satisfied by some other entry in the closure;
//     unsatisfied peers throw ManifestInvalidError before the closure
//     is returned. The walker captures peer-dep metadata during the
//     walk so validation does not require a second pass over the
//     network.

import npmPickManifest from "npm-pick-manifest";
import npmRegistryFetch from "npm-registry-fetch";
import npmPackageArg from "npm-package-arg";
import semver from "semver";
import ssri from "ssri";

import { getLogger } from "@intx/log";
import type { PackageJSON } from "@intx/types/package-json";

import { extractTarballPackageJSON } from "./package-json-extract";

const logger = getLogger(["hub", "tool-packaging", "resolver"]);

import {
  type ToolPackageManifest,
  type ToolPackageManifestEntry,
  type ToolPackagePin,
  type ToolPackageSource,
} from "@intx/types/tool-packages";

/**
 * One HTTP registry. `url` is the actual endpoint; `auth.token` and
 * `auth.basic` mirror the npm-registry-fetch options for bearer /
 * basic auth.
 *
 * The registry's identifier (the string `scopeRouting` entries and
 * manifest `registry` references point at) is the slot it occupies
 * in the registry map: in the hub-side resolver, the key of the
 * `ReadonlyMap<string, RegistrySource>` passed to
 * `createClosureResolver`; in the sidecar-side loader, the key of
 * the `ReadonlyMap<string, RegistryConfig>` on `LoaderConfig`.
 * Keeping the name on the map slot rather than on the value
 * eliminates the (until-now uninforced) "the map key must equal the
 * config's name" invariant.
 *
 * The default-registry decision is owned by `defaultRegistry` on
 * `ClosureResolverConfig`, not by a per-registry flag; only one
 * entry can be the default and centralizing the choice keeps that
 * invariant in the config object.
 */
export interface RegistryConfig {
  readonly url: string;
  readonly auth?: {
    readonly token?: string;
    readonly basic?: { readonly user: string; readonly pass: string };
  };
}

/** Route packages in a given scope (e.g. `@intx`) to a named registry. */
export interface ScopeRoute {
  readonly scope: string;
  readonly registry: string;
}

/**
 * One version manifest as it appears inside a packument. Fields are
 * non-readonly to match the npm-pick-manifest declaration; consumers
 * should treat values as immutable in practice.
 */
export interface PackumentVersion {
  name: string;
  version: string;
  dist: {
    tarball: string;
    integrity?: string;
  };
  dependencies?: Record<string, string>;
  optionalDependencies?: Record<string, string>;
  peerDependencies?: Record<string, string>;
  os?: string[];
  cpu?: string[];
}

/**
 * The packument document a registry serves for one package. Shape
 * matches what `npm-pick-manifest` accepts.
 */
export interface Packument {
  name: string;
  "dist-tags"?: Record<string, string>;
  versions: Record<string, PackumentVersion>;
}

/**
 * Test seam for HTTP packument fetches. The default `HttpRegistrySource`
 * wraps `npm-registry-fetch`; tests inject a function that returns a
 * static packument for a given `(name, registry)` pair.
 */
export type PackumentFetcher = (
  packageName: string,
  registry: RegistryConfig,
) => Promise<Packument>;

/**
 * The shape returned by `RegistrySource.materializeRefForEntry`. The
 * walker copies these fields onto the `ToolPackageManifestEntry` it
 * emits, alongside `name`, `version`, `integrity`, and the platform
 * filter metadata it derives from the picked packument version.
 */
export interface MaterializedRef {
  readonly source: ToolPackageSource;
  /** Set for HTTP-sourced entries so the sidecar can fetch without
   * re-resolving against the registry; asset-sourced entries omit it. */
  readonly tarballUrl?: string;
}

/**
 * A pluggable source of npm-style packuments and per-version source
 * tags. The walker treats every named registry as one of these,
 * regardless of whether bytes live behind an HTTP registry or inside a
 * `package-registry` asset.
 *
 * `name` is the identifier the source was registered under in the
 * resolver's registry map. Diagnostics that need to print a
 * registry's name read it from here; the impls are the source of
 * truth and the interface guarantees it is always available.
 */
export interface RegistrySource {
  readonly name: string;
  fetchPackument(name: string): Promise<Packument>;
  materializeRefForEntry(
    name: string,
    version: string,
    picked: PackumentVersion,
  ): MaterializedRef;
}

/**
 * Configuration for `createClosureResolver`.
 *
 * `registries` is keyed by the same name the agent's pin set
 * references via the `registry` field in `ToolPackagePin`-derived
 * entries (today indirectly via scope routing; built-ins reach this
 * map by the registry name the session service resolved them from).
 *
 * `defaultRegistry` names the entry in `registries` consulted for any
 * package whose scope does not match a `scopeRouting` entry. The map
 * must contain this key.
 */
export interface ClosureResolverConfig {
  readonly registries: ReadonlyMap<string, RegistrySource>;
  readonly defaultRegistry: string;
  readonly scopeRouting?: readonly ScopeRoute[];
}

export interface ClosureResolver {
  resolveClosure(pins: readonly ToolPackagePin[]): Promise<ToolPackageManifest>;
}

/**
 * One unsatisfied peer-dependency declaration discovered while
 * resolving a closure.
 */
export interface PeerDependencyViolation {
  readonly dependent: { readonly name: string; readonly version: string };
  readonly peer: { readonly name: string; readonly range: string };
  readonly satisfiedBy: { readonly version: string } | null;
}

/**
 * Thrown by `resolveClosure` when the resolved closure does not satisfy
 * one or more peer-dependency declarations, by the closure walker for
 * duplicate-name pins, and by the hub-side session-launch path when a
 * direct package-registry attachment overlaps a resolver-driven pin
 * for the same asset. The deploy-assembly path maps every shape to
 * the same `manifest.invalid` deploy-apply error category before the
 * deploy ships.
 *
 * The category is intentionally broad: peer-dep violations and
 * duplicate-name pins are structurally distinct defects but both
 * indicate a closure the operator cannot ship without changing the
 * pin set. Adding a per-shape category would expand the wire
 * taxonomy for no operator-facing gain; readers parsing
 * deploy-apply errors should look at the message for the structural
 * distinction.
 */
export class ManifestInvalidError extends Error {
  /**
   * Populated only when the constructor was invoked with the
   * structured peer-dependency form. String-constructed instances
   * (duplicate-name pins, direct-vs-resolver-asset conflicts) carry
   * an empty array — readers that need the structural distinction
   * should consult `message` rather than treating `violations.length
   * === 0` as a signal.
   */
  readonly violations: readonly PeerDependencyViolation[];

  constructor(
    violationsOrMessage: readonly PeerDependencyViolation[] | string,
  ) {
    if (typeof violationsOrMessage === "string") {
      super(violationsOrMessage);
      this.violations = [];
    } else {
      super(
        `manifest peer-dependency violations: ${violationsOrMessage
          .map(
            (v) =>
              `${v.dependent.name}@${v.dependent.version} requires ${v.peer.name}@${v.peer.range}`,
          )
          .join("; ")}`,
      );
      this.violations = violationsOrMessage;
    }
    this.name = "ManifestInvalidError";
  }
}

/**
 * HTTP-backed registry source. Wraps `npm-registry-fetch` for packument
 * lookups and emits registry-shaped manifest entries.
 */
export class HttpRegistrySource implements RegistrySource {
  readonly name: string;
  readonly #config: RegistryConfig;
  readonly #fetchPackument: PackumentFetcher;

  constructor(args: {
    /** Registry identifier. Becomes the map slot key in the
     * resolver's registries map. */
    readonly name: string;
    readonly config: RegistryConfig;
    /** Test seam: when omitted, the default `npm-registry-fetch`
     * wrapper is used. Production callers omit it. */
    readonly fetchPackument?: PackumentFetcher;
  }) {
    this.name = args.name;
    this.#config = args.config;
    this.#fetchPackument = args.fetchPackument ?? makeDefaultHttpFetcher();
  }

  async fetchPackument(name: string): Promise<Packument> {
    return this.#fetchPackument(name, this.#config);
  }

  materializeRefForEntry(
    _name: string,
    _version: string,
    picked: PackumentVersion,
  ): MaterializedRef {
    return {
      source: { kind: "registry" as const, registry: this.name },
      tarballUrl: picked.dist.tarball,
    };
  }
}

/**
 * Asset-backed registry source. Reads tarballs from a
 * `package-registry` asset via the asset service's in-process read
 * API, extracts each tarball's `package.json`, and synthesizes a
 * packument keyed by package name with one version entry per tarball.
 *
 * Caller passes bound `readBlob`/`listBlobs` methods so the resolver
 * does not import the asset service directly; the session service
 * adapts `AssetService.readAssetBlob` / `AssetService.listAssetBlobs`
 * to these signatures at construction.
 *
 * The packument is cached in-instance for the lifetime of the source,
 * so one resolution pass touches each tarball at most once even when
 * a name is asked for multiple times.
 */
export class AssetRegistrySource implements RegistrySource {
  readonly name: string;
  readonly #assetId: string;
  readonly #readBlob: (path: string) => Promise<Uint8Array>;
  readonly #listBlobs: (dir: string) => Promise<string[]>;
  // Packuments keyed by package name. Built on first access by
  // scanning every `tarballs/*.tgz` blob.
  #packumentsByName: Map<string, Packument> | null = null;
  // Asset-relative path keyed by `${name}@${version}` so
  // materializeRefForEntry can emit the entry's source tag without a
  // second scan of the asset tree.
  #pathByNameVersion = new Map<string, string>();

  constructor(args: {
    readonly name: string;
    readonly assetId: string;
    readonly readBlob: (path: string) => Promise<Uint8Array>;
    readonly listBlobs: (dir: string) => Promise<string[]>;
  }) {
    this.name = args.name;
    this.#assetId = args.assetId;
    this.#readBlob = args.readBlob;
    this.#listBlobs = args.listBlobs;
  }

  async fetchPackument(name: string): Promise<Packument> {
    if (this.#packumentsByName === null) {
      this.#packumentsByName = await this.#buildPackuments();
    }
    const p = this.#packumentsByName.get(name);
    if (p === undefined) {
      throw new Error(
        `asset registry "${this.name}" (asset ${this.#assetId}) has no tarball publishing package "${name}"`,
      );
    }
    return p;
  }

  /**
   * Resolve a `(name, version)` pair to its asset-relative tarball
   * path. Must be called after `fetchPackument` has populated the
   * internal index for that package — the walker calls them in that
   * order, so the ordering is implicit at the call site. Non-walker
   * callers must call `fetchPackument` first or accept the structured
   * error this method throws when the index lookup misses.
   */
  materializeRefForEntry(
    name: string,
    version: string,
    _picked: PackumentVersion,
  ): MaterializedRef {
    const key = `${name}@${version}`;
    const path = this.#pathByNameVersion.get(key);
    if (path === undefined) {
      // The index entry is missing for one of two reasons: either the
      // registry's `fetchPackument` was not called for `name` before
      // this materializer ran (caller-side ordering bug), or
      // `fetchPackument` ran but found no tarball publishing the
      // exact `name@version` pair (the resolver picked a version the
      // registry's listing did not advertise). Either is a precondition
      // violation; surface the situation rather than the inferred cause.
      throw new Error(
        `asset registry "${this.name}" (asset ${this.#assetId}) has no recorded path for ${key}; either fetchPackument was not called on this registry for "${name}" or the registry does not publish this version`,
      );
    }
    return {
      source: {
        kind: "asset" as const,
        assetId: this.#assetId,
        path,
      },
    };
  }

  async #buildPackuments(): Promise<Map<string, Packument>> {
    const blobs = await this.#listBlobs("tarballs");
    const byName = new Map<string, Packument>();
    for (const filename of blobs) {
      // Defensive: the package-registry kind handler enforces the
      // `tarballs/<filename>.tgz` shape on push, but a corrupt repo or
      // an out-of-band write could land non-tarball entries here. Skip
      // anything that does not look like a tarball rather than
      // accepting it as a synthetic packument entry.
      if (!filename.endsWith(".tgz")) continue;
      const repoPath = `tarballs/${filename}`;
      const bytes = await this.#readBlob(repoPath);
      const integrity = ssri
        .fromData(bytes, { algorithms: ["sha512"] })
        .toString();
      const extracted = await extractPackageJSON(this.name, filename, bytes);
      const validated = extracted.parsed;
      const packument = byName.get(validated.name);
      const versionEntry: PackumentVersion = {
        name: validated.name,
        version: validated.version,
        dist: { tarball: repoPath, integrity },
        ...readDependencyFields(extracted.raw),
      };
      if (packument === undefined) {
        byName.set(validated.name, {
          name: validated.name,
          versions: { [validated.version]: versionEntry },
        });
      } else {
        packument.versions[validated.version] = versionEntry;
      }
      this.#pathByNameVersion.set(
        `${validated.name}@${validated.version}`,
        repoPath,
      );
      logger.debug`asset registry "${this.name}" cataloged ${validated.name}@${validated.version} at ${repoPath}`;
    }
    return byName;
  }
}

/**
 * Extract the npm dependency-related fields from a raw package.json
 * object so the AssetRegistrySource can hand the walker a packument
 * that carries the transitive-dependency information. The shared
 * `PackageJSON` validator deliberately covers only the minimum set the
 * substrate enforces (name, version, `interchange.tools`); the
 * resolver-side packument needs more, but those fields are part of
 * npm's documented package.json schema rather than the substrate's
 * invariants, so they are validated here at the resolver boundary.
 */
function readDependencyFields(raw: unknown): Partial<PackumentVersion> {
  if (!isPlainObject(raw)) return {};
  const out: Partial<PackumentVersion> = {};
  const deps = raw["dependencies"];
  if (isStringRecord(deps)) out.dependencies = deps;
  const optionalDeps = raw["optionalDependencies"];
  if (isStringRecord(optionalDeps)) out.optionalDependencies = optionalDeps;
  const peerDeps = raw["peerDependencies"];
  if (isStringRecord(peerDeps)) out.peerDependencies = peerDeps;
  const osField = raw["os"];
  if (isStringArray(osField)) out.os = osField;
  const cpuField = raw["cpu"];
  if (isStringArray(cpuField)) out.cpu = cpuField;
  return out;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isStringRecord(value: unknown): value is Record<string, string> {
  // Reject arrays explicitly. `typeof [] === "object"` is true and
  // `Object.values(["foo"])` yields `["foo"]`, so an array of strings
  // would otherwise satisfy this predicate — and the resolver's
  // closure walk would then iterate `Object.entries(arr)` and treat
  // the numeric indices ("0", "1") as package names to fetch
  // packuments for. The sibling `isPlainObject` already rejects
  // arrays; mirror that here so the same guarantee holds at every
  // shape check.
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  for (const v of Object.values(value)) {
    if (typeof v !== "string") return false;
  }
  return true;
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}

/**
 * npm scope shape: `@` followed by a name character class that matches
 * what npm itself accepts in `validate-npm-package-name`. We validate
 * the routing entries here rather than during pin resolution because
 * a typo (`intx` for `@intx`) silently never matches and the package
 * falls through to the default registry — the exact failure mode the
 * scope-routing feature exists to prevent.
 */
const NPM_SCOPE_RE = /^@[a-z0-9][a-z0-9-_]*$/;

export function createClosureResolver(
  config: ClosureResolverConfig,
): ClosureResolver {
  if (config.registries.size === 0) {
    throw new Error("createClosureResolver: registries cannot be empty");
  }
  const maybeDefault = config.registries.get(config.defaultRegistry);
  if (maybeDefault === undefined) {
    throw new Error(
      `createClosureResolver: defaultRegistry "${config.defaultRegistry}" not present in registries map`,
    );
  }
  if (config.scopeRouting !== undefined) {
    for (const route of config.scopeRouting) {
      if (!NPM_SCOPE_RE.test(route.scope)) {
        throw new Error(
          `createClosureResolver: scopeRouting entry has invalid scope ${JSON.stringify(route.scope)}; scopes must match ${NPM_SCOPE_RE.toString()} (e.g. "@intx")`,
        );
      }
    }
  }
  const defaultSource: RegistrySource = maybeDefault;

  function resolveSource(packageName: string): RegistrySource {
    const scope = parseScope(packageName);
    if (scope !== null && config.scopeRouting !== undefined) {
      const route = config.scopeRouting.find((r) => r.scope === scope);
      if (route !== undefined) {
        const src = config.registries.get(route.registry);
        if (src === undefined) {
          throw new Error(
            `scopeRouting references unknown registry "${route.registry}" for scope "${scope}"`,
          );
        }
        return src;
      }
    }
    return defaultSource;
  }

  return {
    async resolveClosure(pins) {
      // Reject duplicate-name pins up front. The walker's
      // `topLevelResolved` map is keyed by name, so two pins with the
      // same name but different ranges would silently collapse to the
      // first arrival's resolved version — the second pin would never
      // be looked up at the topLevel emit site and the manifest would
      // record only one of the two as a top-level. The contract every
      // pin set must satisfy is one entry per name; surface a
      // violation at the resolver boundary rather than letting the
      // silent collapse happen downstream.
      const seenPinNames = new Set<string>();
      for (const pin of pins) {
        if (seenPinNames.has(pin.name)) {
          throw new ManifestInvalidError(
            `duplicate pin name ${pin.name}: each ToolPackagePin set must contain at most one entry per package name`,
          );
        }
        seenPinNames.add(pin.name);
      }
      const entries = new Map<string, ToolPackageManifestEntry>();
      const peerDeclarations: {
        dependent: { name: string; version: string };
        peer: { name: string; range: string };
      }[] = [];
      // Per-walk packument cache. The dedup that gates the queue is
      // keyed by `(name, range, subtreeId)` to keep subtree poisoning
      // contained — the same `(name, range)` seen first inside one
      // optional subtree must still be reprocessed when it later arrives
      // through an unrelated subtree, otherwise dropping the first
      // subtree would silently drop the dep for the second one too.
      // That correctness requirement would multiply packument requests
      // against the upstream registry; this cache keeps each
      // `(registry, name)` pair to one network round-trip per walk.
      const packumentCache = new Map<string, Promise<Packument>>();
      function fetchPackumentCached(
        source: RegistrySource,
        name: string,
      ): Promise<Packument> {
        // Length-prefix the registry name so two registries whose names
        // happen to overlap with the package-name separator (e.g. a
        // hypothetical registry literally named `foo::bar`) cannot
        // collide on the cache key with another registry. The prefix
        // makes the encoding injective on `(registryName, packageName)`
        // regardless of which characters appear in either string.
        const key = `${source.name.length.toString()}:${source.name}:${name}`;
        const existing = packumentCache.get(key);
        if (existing !== undefined) return existing;
        const promise = source.fetchPackument(name);
        packumentCache.set(key, promise);
        return promise;
      }
      // BFS so root-pin entries are visited before their transitive
      // descendants; output order is stable for tests.
      //
      // `subtreeId` identifies an optional subtree the entry belongs
      // to (null for the hard closure). Every `optionalDependencies`
      // edge opens a new subtree; its transitive descendants inherit
      // the same id so a failure anywhere inside drops the whole
      // subtree silently, matching npm's "the whole optional subtree
      // is best-effort" contract. Failures outside any subtree abort
      // the walk as before.
      const queue: {
        name: string;
        range: string;
        subtreeId: number | null;
      }[] = pins.map((p) => ({
        name: p.name,
        range: p.version,
        subtreeId: null,
      }));
      // Track `name@range` requests we have already issued so two
      // transitive paths to the same dependency at the same range do
      // not trigger duplicate packument fetches. Post-pick dedup
      // (`entries.has(key)` below) catches name@version duplicates
      // but only after the round-trip; this catches them before.
      const requested = new Set<string>();
      // Track the resolved (concrete) version each top-level pin
      // walked to, keyed by pin name. The sidecar's loader matches
      // top-level entries by `${name}@${concrete-version}` to decide
      // which packages contribute tool factories; emitting the pin's
      // raw range here would cause every range-form pin to silently
      // fail that lookup at apply time. Only the first arrival on
      // each pin name counts: a transitive dep that shares a
      // top-level name must not overwrite the top-level's resolved
      // version with its own.
      //
      // INVARIANT: the BFS queue is seeded from `pins` before any
      // transitive descendants are enqueued (see queue construction
      // above), so the first-arrival-wins record below is only safe
      // because BFS guarantees every top-level pin reaches its
      // `topLevelResolved.set` call before any transitive dep that
      // shares its name. Changing the walk order (e.g. switching to
      // DFS, or interleaving roots with transitives) would silently
      // invert this rule and let a transitive dep overwrite a
      // top-level resolution.
      const topLevelNames = new Set(pins.map((p) => p.name));
      const topLevelResolved = new Map<string, string>();

      // Optional-subtree bookkeeping. Subtree-tagged entries and peer
      // declarations stay in their per-subtree stash until the whole
      // subtree finishes successfully, at which point they merge into
      // the main `entries` / `peerDeclarations` collections. If any
      // resolution failure inside the subtree fires, the subtree is
      // marked poisoned, its stash is dropped, and subsequent queue
      // entries tagged with that subtree id are skipped silently.
      let nextSubtreeId = 0;
      const subtreeEntries = new Map<
        number,
        Map<string, ToolPackageManifestEntry>
      >();
      const subtreePeerDeclarations = new Map<
        number,
        {
          dependent: { name: string; version: string };
          peer: { name: string; range: string };
        }[]
      >();
      const subtreePending = new Map<number, number>();
      const poisonedSubtrees = new Set<number>();
      // Parent/child relationships between nested optional subtrees.
      // When an `optionalDependencies` edge fires inside another
      // optional subtree, the new subtree's id is registered as a
      // descendant of the enclosing one. Poisoning the outer subtree
      // cascades to every descendant so a stash that completed inside
      // a nested optional cannot leak entries into the top-level
      // closure once its enclosing best-effort context is dropped.
      const subtreeChildren = new Map<number, Set<number>>();

      function poisonSubtree(subtreeId: number, reason: string): void {
        if (poisonedSubtrees.has(subtreeId)) return;
        poisonedSubtrees.add(subtreeId);
        subtreeEntries.delete(subtreeId);
        subtreePeerDeclarations.delete(subtreeId);
        subtreePending.delete(subtreeId);
        logger.debug`optional subtree ${String(subtreeId)} dropped: ${reason}`;
        const children = subtreeChildren.get(subtreeId);
        if (children !== undefined) {
          subtreeChildren.delete(subtreeId);
          for (const childId of children) {
            poisonSubtree(
              childId,
              `parent subtree ${String(subtreeId)} poisoned`,
            );
          }
        }
      }

      function noteSubtreeArrival(subtreeId: number | null): void {
        if (subtreeId === null) return;
        subtreePending.set(subtreeId, (subtreePending.get(subtreeId) ?? 0) + 1);
      }

      function noteSubtreeCompletion(subtreeId: number | null): void {
        if (subtreeId === null) return;
        if (poisonedSubtrees.has(subtreeId)) return;
        const remaining = (subtreePending.get(subtreeId) ?? 0) - 1;
        if (remaining > 0) {
          subtreePending.set(subtreeId, remaining);
          return;
        }
        subtreePending.delete(subtreeId);
        const stash = subtreeEntries.get(subtreeId);
        if (stash !== undefined) {
          for (const [key, entry] of stash) {
            if (!entries.has(key)) entries.set(key, entry);
          }
          subtreeEntries.delete(subtreeId);
        }
        const peerStash = subtreePeerDeclarations.get(subtreeId);
        if (peerStash !== undefined) {
          for (const decl of peerStash) peerDeclarations.push(decl);
          subtreePeerDeclarations.delete(subtreeId);
        }
      }

      // The root pins all live in the hard closure; subtree-arrival
      // bookkeeping starts the moment an optional edge is followed
      // (see the dependency fan-out below).

      while (queue.length > 0) {
        const next = queue.shift();
        if (next === undefined) break;
        if (next.subtreeId !== null && poisonedSubtrees.has(next.subtreeId)) {
          noteSubtreeCompletion(next.subtreeId);
          continue;
        }
        // Dedup is keyed by `(name, range, subtreeId)` rather than
        // `(name, range)` alone: a request first seen inside an
        // optional subtree A and then re-seen inside an unrelated
        // subtree B must be reprocessed under B's id so dropping A
        // does not silently drop the dep from B's slice of the
        // closure. The hard closure (`subtreeId === null`) keys with
        // the literal "root" sentinel so its dedup behaviour is
        // unchanged from the pre-fix walker.
        const subtreeTag =
          next.subtreeId === null ? "root" : String(next.subtreeId);
        const requestKey = `${subtreeTag}::${next.name}@${next.range}`;
        if (requested.has(requestKey)) {
          noteSubtreeCompletion(next.subtreeId);
          continue;
        }
        requested.add(requestKey);

        const source = resolveSource(next.name);
        let packument: Packument;
        let picked: PackumentVersion;
        try {
          packument = await fetchPackumentCached(source, next.name);
          picked = npmPickManifest(packument, next.range);
        } catch (err) {
          if (next.subtreeId !== null) {
            // Tag the poisoning reason with whether the failure was
            // transport-shaped (connection refused, DNS, 5xx, etc.) or
            // structural (404, malformed packument, unsatisfiable
            // range). Operators reading the debug log need the
            // distinction to triage: transport failures point at the
            // registry or the network, structural failures point at
            // the pin set or the registry's published manifest.
            poisonSubtree(
              next.subtreeId,
              `${next.name}@${next.range} unresolvable (${classifyResolveError(err)}): ${err instanceof Error ? err.message : String(err)}`,
            );
            noteSubtreeCompletion(next.subtreeId);
            continue;
          }
          throw err;
        }

        if (topLevelNames.has(next.name) && !topLevelResolved.has(next.name)) {
          topLevelResolved.set(next.name, picked.version);
        }

        const key = `${picked.name}@${picked.version}`;
        const alreadyInHardClosure = entries.has(key);
        const alreadyInThisSubtree =
          next.subtreeId !== null &&
          (subtreeEntries.get(next.subtreeId)?.has(key) ?? false);
        if (alreadyInHardClosure || alreadyInThisSubtree) {
          noteSubtreeCompletion(next.subtreeId);
          continue;
        }

        if (picked.dist.integrity === undefined) {
          if (next.subtreeId !== null) {
            poisonSubtree(
              next.subtreeId,
              `${key} served with no dist.integrity`,
            );
            noteSubtreeCompletion(next.subtreeId);
            continue;
          }
          throw new Error(
            `registry "${source.name}" served ${key} with no dist.integrity`,
          );
        }

        // `materializeRefForEntry` throws when the registry's per-version
        // bookkeeping is inconsistent (e.g. an asset packument lists a
        // version the path index does not know about). Route the throw
        // through the same poison-vs-abort split that fetch and pick
        // failures use above, so a malformed asset-side packument
        // reached through an optional subtree contains the failure
        // rather than aborting the whole closure walk.
        let ref: MaterializedRef;
        try {
          ref = source.materializeRefForEntry(
            picked.name,
            picked.version,
            picked,
          );
        } catch (err) {
          if (next.subtreeId !== null) {
            poisonSubtree(
              next.subtreeId,
              `${key} materializeRefForEntry failed: ${err instanceof Error ? err.message : String(err)}`,
            );
            noteSubtreeCompletion(next.subtreeId);
            continue;
          }
          throw err;
        }
        const entry: ToolPackageManifestEntry = {
          name: picked.name,
          version: picked.version,
          integrity: picked.dist.integrity,
          source: ref.source,
          ...(ref.tarballUrl !== undefined
            ? { tarballUrl: ref.tarballUrl }
            : {}),
          ...(picked.os !== undefined ? { os: [...picked.os] } : {}),
          ...(picked.cpu !== undefined ? { cpu: [...picked.cpu] } : {}),
        };
        if (next.subtreeId === null) {
          entries.set(key, entry);
        } else {
          let stash = subtreeEntries.get(next.subtreeId);
          if (stash === undefined) {
            stash = new Map();
            subtreeEntries.set(next.subtreeId, stash);
          }
          stash.set(key, entry);
        }

        for (const [name, range] of Object.entries(picked.dependencies ?? {})) {
          // A hard dep of an optional entry inherits the same subtree:
          // dropping the parent silently on failure must not leave a
          // dangling requirement on its children, and a failure on
          // such a child must drop the whole subtree.
          const childId = next.subtreeId;
          noteSubtreeArrival(childId);
          queue.push({ name, range, subtreeId: childId });
        }
        for (const [name, range] of Object.entries(
          picked.optionalDependencies ?? {},
        )) {
          // Every optionalDependencies edge opens a new subtree, even
          // when the requirer already lives inside one — the inner
          // subtree's failure should be containable without poisoning
          // the outer. The reverse is not symmetric: poisoning the
          // outer must cascade into the inner so its merged entries do
          // not survive as orphans (see `subtreeChildren`).
          const childId = nextSubtreeId++;
          if (next.subtreeId !== null) {
            let kin = subtreeChildren.get(next.subtreeId);
            if (kin === undefined) {
              kin = new Set();
              subtreeChildren.set(next.subtreeId, kin);
            }
            kin.add(childId);
          }
          noteSubtreeArrival(childId);
          queue.push({ name, range, subtreeId: childId });
        }
        for (const [name, range] of Object.entries(
          picked.peerDependencies ?? {},
        )) {
          const decl = {
            dependent: { name: picked.name, version: picked.version },
            peer: { name, range },
          };
          if (next.subtreeId === null) {
            peerDeclarations.push(decl);
          } else {
            let stash = subtreePeerDeclarations.get(next.subtreeId);
            if (stash === undefined) {
              stash = [];
              subtreePeerDeclarations.set(next.subtreeId, stash);
            }
            stash.push(decl);
          }
        }
        noteSubtreeCompletion(next.subtreeId);
      }

      const violations = checkPeerDependencies(peerDeclarations, entries);
      if (violations.length > 0) {
        throw new ManifestInvalidError(violations);
      }

      return {
        schemaVersion: "1" as const,
        topLevel: pins.map((p) => {
          const resolved = topLevelResolved.get(p.name);
          if (resolved === undefined) {
            // Every pin enters the queue at the head of the BFS and
            // each queue entry either records a resolved version or
            // throws — reaching this point would indicate a defect
            // in the walker, not in the input.
            throw new Error(
              `resolver internal error: top-level pin ${p.name}@${p.version} did not resolve to a concrete version`,
            );
          }
          return { name: p.name, version: resolved };
        }),
        entries: Array.from(entries.values()),
      };
    },
  };
}

function checkPeerDependencies(
  declarations: readonly {
    dependent: { name: string; version: string };
    peer: { name: string; range: string };
  }[],
  entries: ReadonlyMap<string, ToolPackageManifestEntry>,
): PeerDependencyViolation[] {
  const versionsByName = new Map<string, string[]>();
  for (const e of entries.values()) {
    const list = versionsByName.get(e.name);
    if (list === undefined) versionsByName.set(e.name, [e.version]);
    else list.push(e.version);
  }

  const violations: PeerDependencyViolation[] = [];
  for (const d of declarations) {
    const candidates = versionsByName.get(d.peer.name) ?? [];
    const satisfying = candidates.filter((v) =>
      semver.satisfies(v, d.peer.range, { includePrerelease: true }),
    );
    if (satisfying.length === 0) {
      violations.push({
        dependent: d.dependent,
        peer: d.peer,
        satisfiedBy: null,
      });
    }
  }
  return violations;
}

function parseScope(packageName: string): string | null {
  if (!packageName.startsWith("@")) return null;
  const slash = packageName.indexOf("/");
  if (slash === -1) return null;
  // npm package names are case-insensitive at the registry layer but
  // npm's own validator forbids uppercase. Scope-routing lookups are
  // gated on NPM_SCOPE_RE which rejects uppercase, so an uppercase
  // scope in a package name would silently fail to match any routing
  // entry. Lowercase the lookup so a `@INTX/foo` import still routes
  // through a `@intx` routing entry rather than falling through to
  // the default registry.
  //
  // Pin-time validation rejects uppercase top-level names, but
  // transitive names sourced from registry packuments are not gated
  // against the same rule. The lowercasing here is the only defense
  // against a misconfigured registry returning mixed-case names that
  // would otherwise mis-route at fetch time.
  return packageName.slice(0, slash).toLowerCase();
}

/**
 * Categorize a closure-walk resolution failure as transport or
 * structural so the poison-reason log can route accordingly. Inputs
 * are the errors that bubble out of `npm-registry-fetch` (which
 * decorates HTTP errors with `statusCode` and DNS/connect errors
 * with `code`) and `npm-pick-manifest` (which throws for missing
 * versions / unsatisfiable ranges with no extra fields).
 *
 * Routing rules:
 *   - `code` in the ECONN/EAI/ENET family or `statusCode >= 500`
 *     → transport.
 *   - `statusCode === 404` or any non-2xx without the 5xx shape →
 *     structural (the registry answered; the answer was a
 *     missing-or-malformed packument).
 *   - Everything else (npm-pick-manifest's range-not-satisfiable,
 *     malformed-packument JSON parse failures) → structural.
 */
function classifyResolveError(err: unknown): "transport" | "structural" {
  if (err === null || typeof err !== "object") return "structural";
  const code = "code" in err ? (err as { code: unknown }).code : undefined;
  if (typeof code === "string") {
    if (
      code.startsWith("ECONN") ||
      code.startsWith("EAI") ||
      code.startsWith("ENET") ||
      code === "ETIMEDOUT" ||
      code === "EPIPE" ||
      code === "EHOSTUNREACH"
    ) {
      return "transport";
    }
  }
  const status =
    "statusCode" in err
      ? (err as { statusCode: unknown }).statusCode
      : undefined;
  if (typeof status === "number" && status >= 500) return "transport";
  return "structural";
}

function makeDefaultHttpFetcher(): PackumentFetcher {
  return async (packageName, registry) => {
    const fetchOpts: Record<string, unknown> = { registry: registry.url };
    if (registry.auth?.token !== undefined) {
      fetchOpts.token = registry.auth.token;
    }
    if (registry.auth?.basic !== undefined) {
      const { user, pass } = registry.auth.basic;
      // `npm-registry-fetch` builds the `Authorization: Basic` header by
      // base64-encoding `<username>:<password>` itself. Pre-encoding
      // `pass` would double-encode the password component (the registry
      // would see `base64(plaintext)` as the password, not `plaintext`).
      fetchOpts.forceAuth = { username: user, password: pass };
    }
    const url = `/${encodeNpmName(packageName)}`;
    return await npmRegistryFetch.json<Packument>(url, fetchOpts);
  };
}

function encodeNpmName(name: string): string {
  // Scoped names need the slash percent-encoded for the registry route.
  return name.startsWith("@") ? name.replace("/", "%2f") : name;
}

async function extractPackageJSON(
  registryName: string,
  filename: string,
  bytes: Uint8Array,
): Promise<{ parsed: PackageJSON; raw: unknown }> {
  const outcome = await extractTarballPackageJSON(bytes);
  if (outcome.kind === "missing-entry") {
    throw new ManifestInvalidError(
      `tarball ${filename} has no top-level package.json entry`,
    );
  }
  if (outcome.kind === "multiple-entries") {
    // The hub's package-registry kind handler rejects uploads with
    // multiple top-level package.json entries; a tarball reaching the
    // resolver with this shape implies a registry bypass or a
    // corrupted asset blob. Fail loudly rather than picking the first
    // entry — the sidecar's `tar.extract({ strip: 1 })` would load a
    // different descriptor than the resolver here.
    throw new ManifestInvalidError(
      `asset registry "${registryName}" tarball ${filename} contains multiple top-level package.json entries (${outcome.paths
        .map((p) => JSON.stringify(p))
        .join(", ")}); registry contents must hold exactly one`,
    );
  }
  if (outcome.kind === "parse-error") {
    throw new ManifestInvalidError(
      `tarball ${filename} failed to parse: ${outcome.message}`,
    );
  }
  if (outcome.kind === "json-error") {
    throw new ManifestInvalidError(
      `tarball ${filename} package.json is not valid JSON: ${outcome.message}`,
    );
  }
  if (outcome.kind === "shape-invalid") {
    throw new ManifestInvalidError(
      `asset registry "${registryName}" tarball ${filename} package.json failed validation: ${outcome.message}`,
    );
  }
  return { parsed: outcome.parsed, raw: outcome.raw };
}

/**
 * Parse and canonicalize a `name@range` spec into a pin. Throws on
 * unparseable specs or invalid version ranges. Callers that already
 * have a `ToolPackagePin` do not need this helper.
 *
 * A bare `*` is accepted because npm semantics treat it as the
 * any-version range; the resolver then picks whatever the registry
 * advertises as latest at deploy-assembly time. Operators who care
 * about reproducibility of tool-package closures should pin to a
 * concrete range (`^1.2.3`) — `*` lets the closure shift under the
 * agent without any change to the pin set.
 *
 * NOTE: `*` is special-cased in two places — here and inside the
 * `ToolPackagePinArray` narrow at `@intx/types/tool-packages`. The
 * sites live in separate packages by design (resolver vs. wire-type
 * validation) and cannot import each other; any new magic-range
 * additions need to be made at both call sites to keep the
 * REST-boundary validator and the resolver in agreement.
 */
export function parsePin(spec: string): ToolPackagePin {
  const parsed = npmPackageArg(spec);
  if (parsed.name === null) {
    throw new Error(`unparseable pin spec: ${spec}`);
  }
  // npm itself rejects uppercase in package names; packuments arrive
  // lowercased, so a mixed-case pin would self-resolve through
  // `topLevelResolved` and then silently fail the sidecar loader's
  // `${name}@${version}` lookup against the lowercase entry the
  // packument produced. Reject loudly at the boundary rather than
  // lowercasing the input — pin sets that mix cases are a bug in the
  // caller and should surface as such.
  if (parsed.name !== parsed.name.toLowerCase()) {
    throw new Error(
      `pin name must be lowercase (npm package-name rules): ${parsed.name}`,
    );
  }
  const range = parsed.fetchSpec;
  if (range === undefined || range === null) {
    throw new Error(`pin spec is missing a version range: ${spec}`);
  }
  if (range !== "*" && semver.validRange(range) === null) {
    throw new Error(`invalid version range for ${parsed.name}: ${range}`);
  }
  return { name: parsed.name, version: range };
}
