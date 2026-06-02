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

import { asc, eq } from "drizzle-orm";
import { type DB } from "@intx/db";
import {
  agentAsset as agentAssetTable,
  asset as assetTable,
} from "@intx/db/schema";
import { generateId } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import { hasCode } from "@intx/types";
import type { RepoKind } from "@intx/types/sidecar";

import type {
  InitRepoOpts,
  Principal,
  RepoStore,
  TreeContent,
} from "./repo-store";

const logger = getLogger(["hub-sessions", "asset-service"]);

// Postgres SQLSTATE codes. drizzle / postgres-js surfaces the original
// error with `code` set; `hasCode` narrows safely.
const PG_UNIQUE_VIOLATION = "23505";
const PG_FOREIGN_KEY_VIOLATION = "23503";

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
  /** Only "skill" is supported. "agent-state" is rejected because those
   * repos are managed by the agent lifecycle, not the asset service. */
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
}

/** Discriminator for AssetServiceError variants. Lets callers branch
 * without instanceof gymnastics across the different error subclasses. */
export type AssetServiceErrorReason =
  | "unsupported_kind"
  | "duplicate_asset"
  | "duplicate_attachment"
  | "invalid_name"
  | "invalid_reference"
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

function rowToAsset(row: typeof assetTable.$inferSelect): Asset {
  // The schema stores `kind` as plain text. RepoKind is an arktype
  // enum of ("agent-state" | "skill"); narrow by exhaustive check so
  // an out-of-band kind value loudly fails rather than silently
  // mistypes the returned shape.
  let narrowed: RepoKind;
  switch (row.kind) {
    case "agent-state":
      narrowed = "agent-state";
      break;
    case "skill":
      narrowed = "skill";
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
}): AssetService {
  const { db, repoStore } = deps;

  async function createAsset(params: CreateAssetParams): Promise<Asset> {
    if (params.kind === "agent-state") {
      throw new AssetServiceError(
        "unsupported_kind",
        `createAsset rejects kind "agent-state": agent-state repos are managed by the agent lifecycle, not the asset service`,
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
      if (hasCode(err) && err.code === PG_UNIQUE_VIOLATION) {
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
      if (hasCode(err) && err.code === PG_UNIQUE_VIOLATION) {
        throw new AssetServiceError(
          "duplicate_attachment",
          `agent_asset (agentId=${params.agentId}, assetId=${params.assetId}) already attached`,
          err,
        );
      }
      if (hasCode(err) && err.code === PG_FOREIGN_KEY_VIOLATION) {
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

  return { createAsset, populateAsset, attachAsset, listAgentAssets };
}
