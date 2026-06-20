// In-process service for creating and attaching skill-asset repos.
//
// Three responsibilities are layered here, mirroring the substrate's
// own layering (DB row, repo bookkeeping, content validation):
//
//   createAsset    inserts the asset row and initializes an empty
//                  skill-kind repo via RepoStore.initRepo.
//   populateAsset  drives RepoStore.writeTree, which runs the kind
//                  handler's validatePush before advancing the ref.
//                  Content rejections surface as AssetValidationError.
//   attachAsset    inserts an agent_asset row, surfacing the
//                  (agentId, assetId) uniqueness violation (which
//                  prevents the same asset being attached to one
//                  agent twice) as AssetAttachError.
//
// The factory is closure-based to match createAgentRepoStore and
// createRepoStore. There is no class because there is no per-instance
// mutable state — every method is a pure function over the deps.

import fs from "node:fs";
import { asc, eq } from "drizzle-orm";
import git from "isomorphic-git";
import {
  type DB,
  pgErrorCode,
  PG_UNIQUE_VIOLATION,
  PG_FOREIGN_KEY_VIOLATION,
} from "@intx/db";
import {
  agentAsset as agentAssetTable,
  asset as assetTable,
} from "@intx/db/schema";
import { generateId } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import type { RepoKind } from "@intx/types/sidecar";

import type {
  InitRepoOpts,
  Principal,
  RepoStore,
  TreeContent,
} from "./repo-store";

const logger = getLogger(["hub-sessions", "asset-service"]);

export type Asset = {
  id: string;
  tenantId: string;
  kind: RepoKind;
  name: string;
  displayName: string | null;
  creatorPrincipalId: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type AccessMode = "read-only" | "read-write";

export type AgentAsset = {
  id: string;
  agentId: string;
  assetId: string;
  ref: string;
  accessMode: AccessMode;
  createdAt: Date;
};

export type AgentAssetWithAsset = AgentAsset & {
  asset: Pick<Asset, "id" | "tenantId" | "kind" | "name" | "displayName">;
};

export type CreateAssetParams = {
  tenantId: string;
  /** Accepted kinds: "skill", "package-registry", "workflow".
   * "agent-state" is rejected because those repos are managed by the
   * agent lifecycle, not the asset service. */
  kind: RepoKind;
  name: string;
  displayName?: string;
  creatorPrincipalId?: string;
  /** Forwarded verbatim to `repoStore.initRepo`. Lets the REST route
   * layer ship a per-asset `.gitignore` body (OS/editor cruft + build
   * artefacts + `keys/`) in the genesis tree without the service
   * encoding policy for any one consumer. When omitted, the substrate
   * default body applies. */
  initOpts?: InitRepoOpts;
};

export type PopulateAssetParams = {
  assetId: string;
  ref: string;
  tree: TreeContent;
  /** The principal authorized to write the kind. The substrate's
   * authorize gate uses this; the kind handler also relies on it
   * (e.g. skillAuthorize only permits `kind: "hub"` writes). */
  principal: Principal;
};

export type AttachAssetParams = {
  agentId: string;
  assetId: string;
  ref: string;
  accessMode?: AccessMode;
};

export interface AssetService {
  createAsset(params: CreateAssetParams): Promise<Asset>;
  populateAsset(params: PopulateAssetParams): Promise<{ commitSha: string }>;
  attachAsset(params: AttachAssetParams): Promise<AgentAsset>;
  listAgentAssets(agentId: string): Promise<AgentAssetWithAsset[]>;
  /**
   * In-process blob read. Resolves the asset's row, then reads the blob
   * at `path` from the commit pointed to by `ref` (defaults to
   * `refs/heads/main`). Throws `AssetServiceError("not_found", ...)`
   * when the asset, ref, or path do not exist.
   */
  readAssetBlob(params: ReadAssetBlobParams): Promise<Uint8Array>;
  /**
   * Enumerate the immediate child entry names at `dir` in the asset's
   * commit tree. `dir` is a repo-root-relative POSIX directory path
   * (no trailing slash, no leading slash); pass the empty string to
   * list the root. Throws `AssetServiceError("not_found", ...)` when
   * the asset or ref do not exist, or when `dir` is not a directory.
   */
  listAssetBlobs(params: ListAssetBlobsParams): Promise<string[]>;
}

export type ReadAssetBlobParams = {
  assetId: string;
  path: string;
  /** Defaults to `refs/heads/main`. */
  ref?: string;
};

export type ListAssetBlobsParams = {
  assetId: string;
  /** Empty string lists the tree root. */
  dir: string;
  /** Defaults to `refs/heads/main`. */
  ref?: string;
};

/**
 * Default ref the read API resolves against when callers do not
 * supply one. The smart-HTTP route and the REST tarball routes both
 * push to this ref so it carries the published-asset HEAD.
 */
export const DEFAULT_ASSET_REF = "refs/heads/main";

/** Discriminator for AssetServiceError variants. Lets callers branch
 * without instanceof gymnastics across the different error subclasses. */
export type AssetServiceErrorReason =
  | "unsupported_kind"
  | "duplicate_asset"
  | "duplicate_attachment"
  | "invalid_name"
  | "invalid_reference"
  | "name_reserved"
  | "not_found"
  | "path_violation";

// Asset names become the default workspace mountpath segment at
// session start (`skills/<asset.name>/`). The mountpath segment
// validator in applyAssetPack rejects anything outside a safe
// character set; validate at the createAsset boundary so a bad name
// fails at creation time rather than at materialization time. Names
// must be lowercase-kebab: lowercase letters, digits, hyphens, with
// no leading or trailing hyphen.
const ASSET_NAME_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

export class AssetServiceError extends Error {
  readonly reason: AssetServiceErrorReason;

  constructor(
    reason: AssetServiceErrorReason,
    message: string,
    cause?: unknown,
  ) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "AssetServiceError";
    this.reason = reason;
  }
}

function isAccessMode(value: string): value is AccessMode {
  return value === "read-only" || value === "read-write";
}

/**
 * Reject malformed `dir` arguments at the `listAssetBlobs` boundary
 * before any tree walk begins. The empty string is the documented
 * "list the root" form; anything else must be a relative path with
 * no leading slash, no trailing slash, no `..` segment, and no empty
 * segments (no `//`). This mirrors the same rules that
 * `validateClearPrefix` in the repo-store enforces on `clearPrefix`
 * arguments, surfaced here as a path-violation error so the caller
 * sees a structured rejection instead of a confusing "no directory
 * at /tarballs/" miss when an absolute or `..`-bearing input slips
 * past upstream validation.
 */
function assertWellFormedListDir(dir: string): void {
  if (dir === "") return;
  if (dir === "/" || dir.startsWith("/")) {
    throw new AssetServiceError(
      "path_violation",
      `listAssetBlobs: dir must be a relative path or "" for root; got ${JSON.stringify(dir)}`,
    );
  }
  if (dir.endsWith("/")) {
    throw new AssetServiceError(
      "path_violation",
      `listAssetBlobs: dir must not end with a trailing slash; got ${JSON.stringify(dir)}`,
    );
  }
  const segments = dir.split("/");
  for (const segment of segments) {
    if (segment === "") {
      throw new AssetServiceError(
        "path_violation",
        `listAssetBlobs: dir contains an empty segment ("//"): ${JSON.stringify(dir)}`,
      );
    }
    if (segment === "..") {
      throw new AssetServiceError(
        "path_violation",
        `listAssetBlobs: dir contains a ".." segment: ${JSON.stringify(dir)}`,
      );
    }
  }
}

function rowToAsset(row: typeof assetTable.$inferSelect): Asset {
  // The schema stores `kind` as plain text. RepoKind is an arktype
  // enum; narrow by exhaustive check so an out-of-band kind value
  // loudly fails rather than silently mistypes the returned shape.
  let narrowed: RepoKind;
  switch (row.kind) {
    case "agent-state":
      narrowed = "agent-state";
      break;
    case "skill":
      narrowed = "skill";
      break;
    case "package-registry":
      narrowed = "package-registry";
      break;
    case "workflow":
      narrowed = "workflow";
      break;
    default:
      throw new Error(
        `asset row ${row.id} has unknown kind ${JSON.stringify(row.kind)}`,
      );
  }
  return {
    id: row.id,
    tenantId: row.tenantId,
    kind: narrowed,
    name: row.name,
    displayName: row.displayName,
    creatorPrincipalId: row.creatorPrincipalId,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function rowToAgentAsset(row: typeof agentAssetTable.$inferSelect): AgentAsset {
  if (!isAccessMode(row.accessMode)) {
    throw new Error(
      `agent_asset row ${row.id} has unknown accessMode ${JSON.stringify(row.accessMode)}`,
    );
  }
  return {
    id: row.id,
    agentId: row.agentId,
    assetId: row.assetId,
    ref: row.ref,
    accessMode: row.accessMode,
    createdAt: row.createdAt,
  };
}

export function createAssetService(deps: {
  // only the drizzle handle is needed, not the connection pool — symmetric
  // with createHubSessionLookups and its sibling services.
  db: DB["db"];
  repoStore: RepoStore;
  /**
   * Names that the session service treats as configured HTTP registries
   * when assembling the per-launch package-registry map. A
   * `package-registry` asset whose name collides with one of these
   * shadows the corresponding HTTP registry at session-launch time
   * (asset wins on name collision). Creating such an asset is almost
   * always an operator footgun — silently rerouting the public npm
   * registry traffic to a tenant-owned asset — so reject the creation
   * up front rather than letting the misroute surface later. The host
   * threads in its `httpRegistries` keys; the asset service holds them
   * statically because the registry config is loaded at hub boot and
   * does not change at runtime.
   */
  reservedPackageRegistryNames?: ReadonlySet<string>;
}): AssetService {
  const { db, repoStore } = deps;
  const reservedPackageRegistryNames =
    deps.reservedPackageRegistryNames ?? new Set<string>();

  async function createAsset(params: CreateAssetParams): Promise<Asset> {
    if (
      params.kind !== "skill" &&
      params.kind !== "package-registry" &&
      params.kind !== "workflow"
    ) {
      throw new AssetServiceError(
        "unsupported_kind",
        `createAsset rejects kind ${JSON.stringify(params.kind)}: the asset service handles "skill", "package-registry", and "workflow" assets; other repo kinds are managed by their respective subsystems`,
      );
    }

    if (!ASSET_NAME_PATTERN.test(params.name)) {
      throw new AssetServiceError(
        "invalid_name",
        `createAsset rejects name ${JSON.stringify(
          params.name,
        )}: must be lowercase-kebab (letters, digits, hyphens; no leading or trailing hyphen)`,
      );
    }

    if (
      params.kind === "package-registry" &&
      reservedPackageRegistryNames.has(params.name)
    ) {
      // Session-launch builds the per-launch registry map by iterating
      // package-registry assets first and HTTP registries second, with
      // an asset-wins-on-collision rule. A `package-registry` asset
      // named after a configured HTTP registry would silently shadow
      // that registry for every session that resolves through this
      // tenant — almost certainly an operator misconfig, not an
      // intended override. Reject the creation so the operator sees
      // the collision at intent time instead of debugging an
      // unexpected reroute later.
      throw new AssetServiceError(
        "name_reserved",
        `createAsset rejects name ${JSON.stringify(
          params.name,
        )}: it collides with a configured HTTP registry of the same name and would silently shadow it at session launch`,
      );
    }

    const id = generateId("asset");
    const now = new Date();
    const insertRow = {
      id,
      tenantId: params.tenantId,
      kind: params.kind,
      name: params.name,
      displayName: params.displayName ?? null,
      creatorPrincipalId: params.creatorPrincipalId ?? null,
      createdAt: now,
      updatedAt: now,
    };

    // Init the repo before the row insert so a repo-init failure leaves
    // no orphan row in the database. initRepo is idempotent and the
    // generated id is locally unique, so a follow-up failure of the row
    // insert (duplicate, FK violation, etc.) leaves at worst an empty
    // unreferenced repo directory — harmless and reused on retry of a
    // logically identical asset. The asset-service db handle does not
    // expose transactions in the current narrowing, so this ordering is
    // the safest cross-cutting fix without widening the dep surface.
    //
    // Note: each failed insert with a fresh `id` does leave its own
    // orphan repo directory on disk. The directories carry no asset
    // row and no traffic, so they are inert; a periodic GC walker
    // that drops on-disk repos with no matching row is a follow-up.
    await repoStore.initRepo({ kind: params.kind, id }, params.initOpts);

    let inserted: typeof assetTable.$inferSelect;
    try {
      const rows = await db.insert(assetTable).values(insertRow).returning();
      const row = rows[0];
      if (row === undefined) {
        throw new Error("insert into asset returned no rows");
      }
      inserted = row;
    } catch (err) {
      if (pgErrorCode(err) === PG_UNIQUE_VIOLATION) {
        throw new AssetServiceError(
          "duplicate_asset",
          `asset (tenantId=${params.tenantId}, kind=${params.kind}, name=${params.name}) already exists`,
          err,
        );
      }
      throw err;
    }

    logger.debug`created asset ${id} (kind=${params.kind}, tenant=${params.tenantId}, name=${params.name})`;

    return rowToAsset(inserted);
  }

  async function populateAsset(
    params: PopulateAssetParams,
  ): Promise<{ commitSha: string }> {
    // The asset row carries `kind`. We must read it before writing so
    // the RepoId is shaped correctly; without it, callers could write
    // against the wrong kind handler.
    const row = await db.query.asset.findFirst({
      where: eq(assetTable.id, params.assetId),
    });
    if (row === undefined) {
      throw new AssetServiceError(
        "not_found",
        `populateAsset: asset ${params.assetId} not found`,
      );
    }
    const assetRow = rowToAsset(row);

    try {
      return await repoStore.writeTree(
        params.principal,
        { kind: assetRow.kind, id: assetRow.id },
        params.ref,
        params.tree,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.startsWith("path_violation:")) {
        throw new AssetServiceError("path_violation", msg, err);
      }
      throw err;
    }
  }

  async function attachAsset(params: AttachAssetParams): Promise<AgentAsset> {
    const id = generateId("agentAsset");
    const accessMode = params.accessMode ?? "read-only";
    const insertRow = {
      id,
      agentId: params.agentId,
      assetId: params.assetId,
      ref: params.ref,
      accessMode,
      createdAt: new Date(),
    };

    let inserted: typeof agentAssetTable.$inferSelect;
    try {
      const rows = await db
        .insert(agentAssetTable)
        .values(insertRow)
        .returning();
      const row = rows[0];
      if (row === undefined) {
        throw new Error("insert into agent_asset returned no rows");
      }
      inserted = row;
    } catch (err) {
      if (pgErrorCode(err) === PG_UNIQUE_VIOLATION) {
        throw new AssetServiceError(
          "duplicate_attachment",
          `agent_asset (agentId=${params.agentId}, assetId=${params.assetId}) already attached`,
          err,
        );
      }
      if (pgErrorCode(err) === PG_FOREIGN_KEY_VIOLATION) {
        throw new AssetServiceError(
          "invalid_reference",
          `agent_asset (agentId=${params.agentId}, assetId=${params.assetId}) references a missing agent or asset`,
          err,
        );
      }
      throw err;
    }

    return rowToAgentAsset(inserted);
  }

  async function listAgentAssets(
    agentId: string,
  ): Promise<AgentAssetWithAsset[]> {
    // Order by (createdAt, id) so the row sequence is stable across reads
    // — the available_skills stanza and pack fan-out both depend on a
    // deterministic order, and Postgres does not guarantee one without
    // an explicit orderBy.
    const rows = await db
      .select({
        agentAsset: agentAssetTable,
        asset: assetTable,
      })
      .from(agentAssetTable)
      .innerJoin(assetTable, eq(agentAssetTable.assetId, assetTable.id))
      .where(eq(agentAssetTable.agentId, agentId))
      .orderBy(asc(agentAssetTable.createdAt), asc(agentAssetTable.id));

    return rows.map((row) => {
      const aa = rowToAgentAsset(row.agentAsset);
      const a = rowToAsset(row.asset);
      return {
        ...aa,
        asset: {
          id: a.id,
          tenantId: a.tenantId,
          kind: a.kind,
          name: a.name,
          displayName: a.displayName,
        },
      };
    });
  }

  async function resolveAssetRowOrThrow(
    assetId: string,
    label: string,
  ): Promise<Asset> {
    const row = await db.query.asset.findFirst({
      where: eq(assetTable.id, assetId),
    });
    if (row === undefined) {
      throw new AssetServiceError(
        "not_found",
        `${label}: asset ${assetId} not found`,
      );
    }
    return rowToAsset(row);
  }

  async function resolveCommitTreeOid(
    asset: Asset,
    ref: string,
    label: string,
  ): Promise<{ dir: string; treeOid: string }> {
    const dir = repoStore.getRepoDir({ kind: asset.kind, id: asset.id });
    let commitSha: string;
    try {
      commitSha = await git.resolveRef({ fs, dir, ref });
    } catch (cause) {
      throw new AssetServiceError(
        "not_found",
        `${label}: asset ${asset.id} ref ${ref} not resolvable`,
        cause,
      );
    }
    const { commit } = await git.readCommit({ fs, dir, oid: commitSha });
    return { dir, treeOid: commit.tree };
  }

  async function readAssetBlob(
    params: ReadAssetBlobParams,
  ): Promise<Uint8Array> {
    const ref = params.ref ?? DEFAULT_ASSET_REF;
    const asset = await resolveAssetRowOrThrow(params.assetId, "readAssetBlob");
    const { dir, treeOid } = await resolveCommitTreeOid(
      asset,
      ref,
      "readAssetBlob",
    );
    try {
      const { blob } = await git.readBlob({
        fs,
        dir,
        oid: treeOid,
        filepath: params.path,
      });
      return blob;
    } catch (cause) {
      throw new AssetServiceError(
        "not_found",
        `readAssetBlob: asset ${params.assetId} has no blob at ${JSON.stringify(params.path)} on ref ${ref}`,
        cause,
      );
    }
  }

  async function listAssetBlobs(
    params: ListAssetBlobsParams,
  ): Promise<string[]> {
    assertWellFormedListDir(params.dir);
    const ref = params.ref ?? DEFAULT_ASSET_REF;
    const asset = await resolveAssetRowOrThrow(
      params.assetId,
      "listAssetBlobs",
    );
    const { dir, treeOid } = await resolveCommitTreeOid(
      asset,
      ref,
      "listAssetBlobs",
    );
    if (params.dir === "") {
      const { tree } = await git.readTree({ fs, dir, oid: treeOid });
      return tree.filter((e) => e.type === "blob").map((e) => e.path);
    }
    let currentOid = treeOid;
    for (const segment of params.dir.split("/")) {
      const { tree } = await git.readTree({ fs, dir, oid: currentOid });
      const entry = tree.find((e) => e.path === segment);
      if (entry === undefined || entry.type !== "tree") {
        throw new AssetServiceError(
          "not_found",
          `listAssetBlobs: asset ${params.assetId} has no directory at ${JSON.stringify(params.dir)} on ref ${ref}`,
        );
      }
      currentOid = entry.oid;
    }
    const { tree } = await git.readTree({ fs, dir, oid: currentOid });
    return tree.filter((e) => e.type === "blob").map((e) => e.path);
  }

  return {
    createAsset,
    populateAsset,
    attachAsset,
    listAgentAssets,
    readAssetBlob,
    listAssetBlobs,
  };
}
