// KindHandler for the `package-registry` asset kind.
//
// A package-registry asset is a git repo whose top-level tree holds:
//
//   - `tarballs/<filename>.tgz` — one or more npm-style tarballs. Each
//     tarball is opened during validation; its inner `package.json`
//     must validate against `PackageJSON`. Anything else under
//     `tarballs/` is rejected.
//   - `package-registry.json` (optional) — a hub-authored index. The
//     substrate does not enforce its shape today; the resolver layer
//     consumes it when present.
//   - `.gitignore` — supplied by the asset routes' genesis init body.
//
// Any top-level entry outside this set fails the push.

import { getLogger } from "@intx/log";
import { extractTarballPackageJSON } from "@intx/tool-packaging";
import { hasCode } from "@intx/types";
import type { PackageJSON } from "@intx/types/package-json";

import {
  type AuthorizeFn,
  type KindHandler,
  type Principal,
  type ValidatePushResult,
} from "./repo-store";

const logger = getLogger(["hub-sessions", "package-registry-kind"]);

export const TARBALLS_PREFIX = "tarballs/";
export const REGISTRY_INDEX_PATH = "package-registry.json";

/**
 * Canonical asset name for the workspace's bundled package-registry —
 * the in-tree `@intx/tools-*` packages live in an asset of this name
 * under the workspace's root tenant. The hub's scope-routing config
 * maps `@intx` to this registry, the seed script ensures the asset
 * row exists, and the publish-tool-packages CLI defaults its target
 * registry to this name. The constant lives at one site so a rename
 * does not have to chase three independent string literals.
 *
 * Callers:
 *   - `bin/dev.ts` — orchestrator default for the registry the dev
 *     stack publishes the built-ins into.
 *   - `bin/seed.ts` — seeder that pins the workspace-builtins into
 *     the registry asset at boot.
 *   - `bin/publish-tool-packages.ts` — CLI default for the
 *     `--registry` flag.
 *
 * No test asserts that the seed's pinned built-ins actually land
 * under this exact name; a mismatch between the constant and a
 * caller would surface at apply time as a `tarball.missing`
 * structured failure, not as a build error.
 */
export const WORKSPACE_BUILTINS_REGISTRY = "workspace-builtins";

/**
 * Filename rule for tarballs in the repo tree. Filenames must end in
 * `.tgz`, start with an alphanumeric / underscore / scope-marker
 * character, and otherwise contain only filename-safe characters. The
 * leading-character constraint forbids hidden-style names like
 * `..tgz` or `.hidden.tgz` that the filesystem treats as dotfiles —
 * those would be invisible to a `readdir` shell expansion and split
 * the resolver's view of the registry tree from the operator's. The
 * same rule is enforced at the REST upload boundary so a push and a
 * PUT cannot produce diverging contents.
 */
export const TARBALL_FILENAME_PATTERN =
  /^[A-Za-z0-9_@+][A-Za-z0-9_.@+-]*\.tgz$/;

/**
 * Validates that `path` belongs to a tarball entry shape
 * `tarballs/<filename>.tgz`. Returns the bare filename when valid,
 * `null` otherwise.
 */
export function asTarballEntry(path: string): string | null {
  if (!path.startsWith(TARBALLS_PREFIX)) return null;
  const filename = path.slice(TARBALLS_PREFIX.length);
  if (filename.length === 0) return null;
  if (filename.includes("/")) return null;
  if (!TARBALL_FILENAME_PATTERN.test(filename)) return null;
  return filename;
}

type TarballValidationOutcome =
  | { ok: true; pkg: PackageJSON }
  | { ok: false; reason: string };

/**
 * Open an npm-style tarball, find the top-level `package.json` entry,
 * parse and validate it. Returns the typed descriptor on success; a
 * structured reason otherwise. The shape validation lives inside
 * `extractTarballPackageJSON`; this wrapper only translates the
 * outcome into the substrate's `ValidatePushResult`-shaped reason
 * strings.
 */
export async function validateTarballPackageJSON(
  filename: string,
  bytes: Uint8Array,
): Promise<TarballValidationOutcome> {
  const outcome = await extractTarballPackageJSON(bytes);
  if (outcome.kind === "missing-entry") {
    return {
      ok: false,
      reason: `tarball ${filename} has no top-level package.json entry`,
    };
  }
  if (outcome.kind === "multiple-entries") {
    // The hub validates the first top-level package.json the tar walk
    // emits, but the sidecar's `tar.extract({ strip: 1 })` overwrites
    // on every subsequent path with the same stripped name and ends up
    // loading the LAST entry. A tarball carrying more than one
    // `<seg>/package.json` therefore would have its hub-side validation
    // and sidecar-side runtime read different descriptors — exactly
    // the kind of TOCTOU gap a single signed integrity hash cannot
    // close. Reject the upload at the validation boundary.
    return {
      ok: false,
      reason: `tarball ${filename} contains multiple top-level package.json entries (${outcome.paths
        .map((p) => JSON.stringify(p))
        .join(
          ", ",
        )}); npm tarballs must hold exactly one top-level package directory`,
    };
  }
  if (outcome.kind === "parse-error") {
    return {
      ok: false,
      reason: `tarball ${filename} failed to parse: ${outcome.message}`,
    };
  }
  if (outcome.kind === "json-error") {
    return {
      ok: false,
      reason: `tarball ${filename} package.json is not valid JSON: ${outcome.message}`,
    };
  }
  if (outcome.kind === "shape-invalid") {
    return {
      ok: false,
      reason: `tarball ${filename} package.json failed validation: ${outcome.message}`,
    };
  }
  return { ok: true, pkg: outcome.parsed };
}

export const packageRegistryKindHandler: KindHandler = {
  kind: "package-registry",
  directoryPrefix: "assets/package-registry",
  async validatePush({
    repoId,
    ref,
    topLevelTreePaths,
    readBlob,
    listDir,
  }): Promise<ValidatePushResult> {
    // The substrate hands us every top-level tree entry — both
    // directories and files — from both the receivePack and writeTree
    // adapters. The allowed set is `tarballs`, `.gitignore`, and
    // `package-registry.json`.
    for (const entry of topLevelTreePaths) {
      if (
        entry === "tarballs" ||
        entry === ".gitignore" ||
        entry === REGISTRY_INDEX_PATH
      ) {
        continue;
      }
      return {
        ok: false,
        reason: `unexpected top-level entry ${JSON.stringify(entry)}; allowed: "tarballs", "${REGISTRY_INDEX_PATH}", ".gitignore"`,
      };
    }

    // Enumerate every entry under `tarballs/`, validate the filename
    // shape, then open each tarball and validate its package.json.
    let tarballChildren: string[];
    try {
      tarballChildren = await listDir("tarballs");
    } catch (err) {
      // Only "the tarballs subtree is absent" is a legitimate fall-
      // through case (genesis tree, or an upload that dropped the
      // prefix). Any other listDir failure — EACCES, EIO, transient
      // transport faults, malformed-tree errors — must surface as a
      // push rejection rather than be collapsed into "no tarballs to
      // validate," which would silently let a push through whose
      // tarballs subtree could not be enumerated.
      if (hasCode(err) && err.code === "NotFoundError") {
        tarballChildren = [];
      } else {
        return {
          ok: false,
          reason: `failed to list tarballs subtree: ${err instanceof Error ? err.message : String(err)}`,
        };
      }
    }

    // Two tarballs publishing the same `${name}@${version}` make the
    // resolver's AssetRegistrySource overwrite by fs.readdir order —
    // an undefined-behaviour outcome across filesystems. Reject the
    // collision at the substrate boundary so the registry asset's
    // closure is unambiguous regardless of how the loader walks it.
    const publishedNameVersions = new Set<string>();
    for (const child of tarballChildren) {
      const repoPath = `${TARBALLS_PREFIX}${child}`;
      const filename = asTarballEntry(repoPath);
      if (filename === null) {
        return {
          ok: false,
          reason: `tarball path ${JSON.stringify(repoPath)} must match tarballs/<filename>.tgz with filename-safe characters and a .tgz extension`,
        };
      }
      let bytes: Uint8Array;
      try {
        bytes = await readBlob(repoPath);
      } catch (cause) {
        return {
          ok: false,
          reason: `tarball ${repoPath} could not be read from the tree: ${
            cause instanceof Error ? cause.message : String(cause)
          }`,
        };
      }
      const outcome = await validateTarballPackageJSON(filename, bytes);
      if (!outcome.ok) {
        logger.debug`package-registry validatePush rejected ${repoId.kind}/${repoId.id} on ${ref}: ${outcome.reason}`;
        return { ok: false, reason: outcome.reason };
      }
      const key = `${outcome.pkg.name}@${outcome.pkg.version}`;
      if (publishedNameVersions.has(key)) {
        return {
          ok: false,
          reason: `package-registry contains multiple tarballs publishing ${key}; each name@version pair must appear in exactly one tarball`,
        };
      }
      publishedNameVersions.add(key);
    }

    return { ok: true };
  },
  onRefUpdated() {
    // No cached index today. The resolver reads the asset's contents
    // through `readAssetBlob`/`listAssetBlobs` at session time.
  },
};

/**
 * Authorize policy for package-registry repos. Sidecars never write
 * to package-registry repos (they consume contents through the
 * in-process read API, not the smart-HTTP layer); only the hub and
 * authenticated users with the right grant may push.
 */
export const packageRegistryAuthorize: AuthorizeFn = (
  principal: Principal,
  repoId,
  _ref,
  action,
) => {
  if (repoId.kind !== "package-registry") {
    return {
      allowed: false,
      reason: `package-registry authorize received non-package-registry repo ${repoId.kind}/${repoId.id}`,
    };
  }

  if (principal.kind === "hub") {
    return { allowed: true };
  }

  if (principal.kind === "sidecar") {
    return {
      allowed: false,
      reason: `sidecars do not access package-registry assets via the substrate; action=${action}`,
    };
  }

  // The smart-HTTP route layer treats package-registry repos as
  // user-write-denied by design: tarball writes are constrained to the
  // shape the kind handler validates (`tarballs/<filename>.tgz` plus a
  // hub-authored index), and the REST PUT/DELETE endpoints on the
  // asset routes (`PUT /api/tenants/:tid/assets/:assetId/tarballs/:filename`
  // and the matching DELETE) are the supported path for users who
  // need to publish a tarball. Smart-HTTP would let a user push
  // arbitrary tree shapes that the kind handler would then have to
  // reject after the fact; the REST surface validates ahead of write.
  return {
    allowed: false,
    reason: `principal kind ${principal.kind} cannot push to package-registry over smart-HTTP; use the REST tarball endpoints (PUT/DELETE /api/tenants/:tid/assets/:assetId/tarballs/:filename)`,
  };
};
