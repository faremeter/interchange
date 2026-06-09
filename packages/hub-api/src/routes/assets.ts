/**
 * Asset REST endpoint and smart-HTTP route group.
 *
 * Two distinct surfaces live in this file. The REST half — `POST /` —
 * is gated by the standard session + `requireGrant("asset:*", "create")`
 * pipeline and provisions the asset row plus the genesis-signed repo
 * via `assetService.createAsset`. The smart-HTTP half — every path
 * under `/:kind/:nameDotGit/...` — is gated by the bearer middleware
 * the app layer mounts ahead of it (`itx_pat_*` / `itx_svc_*` tokens)
 * and serves the four standard endpoints (`info/refs` for upload-pack
 * and receive-pack, then the two POST endpoints themselves).
 *
 * The smart-HTTP handler resolves URL `:kind/:name` to a concrete
 * `RepoId` by looking up the asset row `(tenantId, kind, name)`; on
 * miss the request is rejected with `404 not_found`. The handler
 * then resolves the authz verdict against `asset:<asset.id>` and the
 * grant verb derived from the `RepoAction`, and constructs the
 * `UserPrincipal` with the verdict pre-resolved so the substrate's
 * authorize gate only sanity-checks rather than re-querying.
 *
 * Bearer-claim `expiresAt` is a `Date` on the wire; the substrate's
 * `UserPrincipal.tokenClaims.expiresAt` is a `number`. The Date →
 * number conversion happens exactly once, at the route handler
 * boundary.
 */

import { and, eq } from "drizzle-orm";
import { Hono, type Context } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import { Buffer } from "node:buffer";
import ssri from "ssri";

import { authorize } from "@intx/authz";
import { asset as assetTable } from "@intx/db/schema";
import {
  listAssetsForTenant,
  resolveAssetById,
  type AssetWithOrigin,
  type DB,
} from "@intx/db";
import { repoActionToGrantVerb } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import {
  AssetServiceError,
  DEFAULT_ASSET_REF,
  TARBALLS_PREFIX,
  TARBALL_FILENAME_PATTERN,
  asTarballEntry,
  type AssetService,
  type Principal,
  type RefEntry,
  type RepoId,
  type RepoStore,
  type UserPrincipal,
} from "@intx/hub-sessions";
import type { RepoAction, RepoKind } from "@intx/types/sidecar";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import {
  AssetResponse,
  AssetWithOriginResponse,
  ErrorResponse,
} from "@intx/types";

import type { TenantEnv } from "../context";
import { ts } from "../format";
import type {
  GitTokenClaims,
  TenantGitTokenEnv,
} from "../middleware/git-token-auth";
import { idResource, type RequireGrant } from "../middleware/grant";
import {
  advertiseReceivePack,
  advertiseUploadPack,
  type RefSource,
} from "../git-http/advertise-refs";
import {
  handleUploadPack,
  type UploadPackRepoStore,
} from "../git-http/upload-pack";
import { handleReceivePack } from "../git-http/receive-pack";

const log = getLogger(["hub", "assets"]);

/**
 * Genesis `.gitignore` body shipped with every asset repo. Captures
 * the OS- and editor-cruft families that show up in skill-asset
 * workspaces in practice, plus the `keys/` directory the hub uses to
 * stage materialised credentials at session-start time. The list is
 * a deliberate literal here; new entries are policy decisions
 * reviewed at this file rather than fanned out through configuration.
 */
export const SANE_GITIGNORE = [
  ".DS_Store",
  "Thumbs.db",
  "desktop.ini",
  ".idea/",
  ".vscode/",
  "*.swp",
  "*.swo",
  "node_modules/",
  "dist/",
  "build/",
  "target/",
  "*.log",
  "keys/",
  "",
].join("\n");

// REST contract -----------------------------------------------------

const KIND_VALUES = ["agent-state", "skill", "package-registry"] as const;

const CreateAsset = type({
  kind: type.enumerated(...KIND_VALUES),
  name: "string",
  "displayName?": "string",
});

const AssetResponseSchema = type({
  id: "string",
  tenantId: "string",
  kind: type.enumerated(...KIND_VALUES),
  name: "string",
  displayName: "string | null",
  creatorPrincipalId: "string | null",
  createdAt: "string",
  updatedAt: "string",
});

// URL parsing for the smart-HTTP routes -----------------------------

// Asset names admitted by the service are lowercase-kebab. The
// smart-HTTP URL strips the `.git` suffix from the trailing path
// segment before the lookup; the kind segment is validated against
// the same enum the REST endpoint accepts.
const ASSET_NAME_URL_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;

function parseKind(raw: string): RepoKind | null {
  if (raw === "agent-state" || raw === "skill" || raw === "package-registry") {
    return raw;
  }
  return null;
}

function stripGitSuffix(raw: string): string | null {
  if (!raw.endsWith(".git")) return null;
  return raw.slice(0, -".git".length);
}

// Pre-resolved authz + UserPrincipal construction ------------------

type AssetLookup = {
  id: string;
  tenantId: string;
  kind: RepoKind;
  name: string;
};

function dateToNumber(d: Date): number {
  return d.getTime();
}

async function resolveAuthzVerdict(args: {
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  principalId: string;
  tenantId: string;
  assetId: string;
  action: RepoAction;
}): Promise<UserPrincipal["authz"]> {
  const resource = `asset:${args.assetId}`;
  const grantVerb = repoActionToGrantVerb(args.action);
  const verdict = await authorize(
    args.grantStore,
    args.principalId,
    args.tenantId,
    resource,
    grantVerb,
    args.conditionRegistry,
  );
  return {
    effect: verdict.effect === "allow" ? "allow" : "deny",
    resource,
    grantVerb,
  };
}

function buildUserPrincipal(args: {
  principalId: string;
  tenantId: string;
  authz: UserPrincipal["authz"];
  claims: GitTokenClaims;
}): UserPrincipal {
  return {
    kind: "user",
    principalId: args.principalId,
    tenantId: args.tenantId,
    authz: args.authz,
    tokenClaims: {
      refPattern: args.claims.refPattern,
      actions: args.claims.actions,
      expiresAt: dateToNumber(args.claims.expiresAt),
    },
  };
}

// Substrate adapters: bridge the substrate's RepoStore to the narrow
// per-handler contracts that advertise-refs and upload-pack expose.

function makeRefSource(
  repoStore: RepoStore,
  principal: UserPrincipal,
): RefSource {
  return {
    async listRefs(_p, repoId): Promise<RefEntry[]> {
      return repoStore.listRefs(principal, repoId);
    },
    async resolveHead(_p, repoId) {
      return repoStore.resolveHead(principal, repoId);
    },
  };
}

function makeUploadPackStore(
  repoStore: RepoStore,
  principal: UserPrincipal,
): UploadPackRepoStore {
  return {
    async listRefs(_p, repoId): Promise<RefEntry[]> {
      return repoStore.listRefs(principal, repoId);
    },
    async getRepoDir(_p, repoId): Promise<string> {
      return repoStore.getRepoDir(repoId);
    },
  };
}

// Routes ------------------------------------------------------------

export type CreateAssetRoutesDeps = {
  db: DB["db"];
  assetService: AssetService;
  repoStore: RepoStore;
  grantStore: GrantStore;
  conditionRegistry: ConditionRegistry;
  requireGrant: RequireGrant;
  /**
   * Maximum tarball payload accepted by the package-registry PUT
   * endpoint. Larger uploads are rejected with 413 before the request
   * body is buffered. The hub edge resolves this from the
   * `HUB_MAX_TARBALL_BYTES` env var (or its config default) so the
   * route handler receives a concrete cap.
   */
  maxTarballBytes: number;
};

function formatAsset(row: typeof assetTable.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    kind: row.kind,
    name: row.name,
    displayName: row.displayName,
    creatorPrincipalId: row.creatorPrincipalId,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

function formatAssetWithOrigin(
  row: typeof assetTable.$inferSelect,
  origin: AssetWithOrigin["origin"],
) {
  return {
    ...formatAsset(row),
    origin,
  };
}

/**
 * Drain `request.body` into a single Uint8Array, aborting as soon as
 * the accumulated byte count would exceed `maxBytes`. Returns `null`
 * when the body overruns the cap so the caller can emit 413 without
 * having to thread the cap into the catch path. Returns an empty
 * array when the body is absent.
 *
 * The pre-buffer Content-Length check upstream covers honest clients;
 * this guard catches the rest — clients that omit the header or lie
 * about it — by enforcing the limit as the bytes arrive.
 */
async function readBodyWithLimit(
  request: Request,
  maxBytes: number,
): Promise<Uint8Array | null> {
  const body = request.body;
  if (body === null) return new Uint8Array(0);
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value === undefined) continue;
      total += value.byteLength;
      if (total > maxBytes) {
        // `reader.cancel()` requests cancellation upstream, but whether
        // the runtime then drops the in-flight TCP frames or just
        // unsubscribes our reader is runtime-dependent (Bun, Node's
        // undici, and Cloudflare Workers each behave slightly
        // differently here). In the worst case the client may still
        // upload up to its own buffer of bytes after we return 413;
        // we treat that as acceptable because the server-side cost
        // is bounded by the runtime's per-request buffer, not by the
        // caller's malicious intent.
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

export function createAssetRoutes({
  db,
  assetService,
  repoStore,
  grantStore,
  conditionRegistry,
  requireGrant,
  maxTarballBytes,
}: CreateAssetRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

  app.get(
    "/",
    requireGrant("asset:*", "read"),
    describeRoute({
      tags: ["Assets"],
      summary: "List assets",
      description:
        "Lists assets for the tenant. With inherited=true (the default), assets defined on ancestor tenants are included; descendant tenants shadow ancestors when they declare the same (kind, name) pair. Each row carries an `origin` tag identifying the tenant that supplied it.",
      parameters: [
        {
          name: "kind",
          in: "query",
          schema: { type: "string" },
        },
        {
          name: "inherited",
          in: "query",
          schema: { type: "string", enum: ["true", "false"] },
        },
      ],
      responses: {
        200: {
          description: "List of assets",
          content: {
            "application/json": {
              schema: resolver(AssetWithOriginResponse.array()),
            },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const kindRaw = c.req.query("kind");
      const inheritedRaw = c.req.query("inherited");
      let inherited: boolean;
      if (inheritedRaw === undefined || inheritedRaw === "true") {
        inherited = true;
      } else if (inheritedRaw === "false") {
        inherited = false;
      } else {
        return c.json(
          {
            error: {
              code: "bad_request",
              message: `inherited must be "true" or "false", got ${JSON.stringify(inheritedRaw)}`,
            },
          },
          400,
        );
      }

      if (inherited) {
        const rows = await listAssetsForTenant(db, tenantCtx.id, kindRaw);
        return c.json(
          rows.map((row) => formatAssetWithOrigin(row, row.origin)),
        );
      }

      const conditions = [eq(assetTable.tenantId, tenantCtx.id)];
      if (kindRaw !== undefined) {
        conditions.push(eq(assetTable.kind, kindRaw));
      }
      const rows = await db.query.asset.findMany({
        where: and(...conditions),
      });
      return c.json(
        rows.map((row) =>
          formatAssetWithOrigin(row, {
            tenantId: row.tenantId,
            direct: true,
          }),
        ),
      );
    },
  );

  app.get(
    "/:assetId",
    requireGrant(idResource("asset", "assetId"), "read"),
    describeRoute({
      tags: ["Assets"],
      summary: "Get asset metadata",
      description:
        "Returns asset metadata. Resolves through the tenant hierarchy: assets declared on the tenant or any ancestor are visible. Sibling-tenant assets return 404 so callers cannot probe for cross-tenant existence.",
      responses: {
        200: {
          description: "Asset metadata",
          content: {
            "application/json": { schema: resolver(AssetResponse) },
          },
        },
        404: {
          description: "Asset not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const assetId = c.req.param("assetId");

      const row = await resolveAssetById(db, tenantCtx.id, assetId);
      if (row === null) {
        return c.json(
          { error: { code: "not_found", message: "Asset not found" } },
          404,
        );
      }

      return c.json(formatAsset(row));
    },
  );

  app.post(
    "/",
    requireGrant("asset:*", "create"),
    describeRoute({
      tags: ["Assets"],
      summary: "Create an asset",
      description:
        "Inserts an asset row and initializes the backing git repository with a hub-signed genesis commit and the asset-route .gitignore body.",
      responses: {
        201: {
          description: "Asset created",
          content: {
            "application/json": { schema: resolver(AssetResponseSchema) },
          },
        },
        400: {
          description: "Validation error",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        409: {
          description: "Asset already exists",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    validator("json", CreateAsset),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const principalCtx = c.get("principal");
      const body = c.req.valid("json");

      try {
        const asset = await assetService.createAsset({
          tenantId: tenantCtx.id,
          kind: body.kind,
          name: body.name,
          ...(body.displayName === undefined
            ? {}
            : { displayName: body.displayName }),
          creatorPrincipalId: principalCtx.id,
          initOpts: { gitignore: SANE_GITIGNORE },
        });
        log.info(
          "create succeeded {tenantId} kind={kind} name={name} id={id}",
          {
            tenantId: tenantCtx.id,
            kind: asset.kind,
            name: asset.name,
            id: asset.id,
          },
        );
        return c.json(formatAsset(asset), 201);
      } catch (err) {
        if (err instanceof AssetServiceError) {
          let status: 400 | 409;
          if (err.reason === "duplicate_asset") {
            status = 409;
          } else {
            status = 400;
          }
          log.info("create rejected {tenantId} code={code}", {
            tenantId: tenantCtx.id,
            code: err.reason,
          });
          return c.json(
            { error: { code: err.reason, message: err.message } },
            status,
          );
        }
        throw err;
      }
    },
  );

  // Internal sentinel thrown by the DELETE handler's merge callback so
  // the route layer can translate "filename absent at the locked-in
  // pre-image" into a 404 without re-checking outside the lock.
  class TarballNotFoundError extends Error {
    readonly filename: string;
    readonly assetId: string;
    constructor(filename: string, assetId: string) {
      super(`tarball ${filename} not found in asset ${assetId}`);
      this.name = "TarballNotFoundError";
      this.filename = filename;
      this.assetId = assetId;
    }
  }

  // ----- Tarball routes ---------------------------------------------
  //
  // The PUT/GET/DELETE handlers operate against package-registry
  // assets. They look the asset up through `resolveAssetById`, which
  // walks the tenant ancestor chain; a sibling-tenant asset id resolves
  // to null and surfaces as 404 so callers cannot probe across tenants.

  const TarballSummary = type({
    filename: "string",
    size: "number",
    integrity: "string",
  });

  const TarballListResponse = TarballSummary.array();

  const TarballPutResponse = type({
    commit: "string",
    integrity: "string",
  });

  const TarballDeleteResponse = type({
    commit: "string",
  });

  // The hub principal is what the package-registry kind authorize
  // grants writes to. Constructing it once keeps the call sites short.
  const hubPrincipal: Principal = { kind: "hub" };

  type ResolvedRegistry = {
    assetId: string;
    asset: { kind: RepoKind; id: string; name: string };
  };

  async function resolveRegistryAsset(
    tenantId: string,
    assetId: string,
  ): Promise<
    | { ok: true; resolved: ResolvedRegistry }
    | { ok: false; status: 404; code: string; message: string }
  > {
    const row = await resolveAssetById(db, tenantId, assetId);
    if (row === null) {
      return {
        ok: false,
        status: 404,
        code: "not_found",
        message: "Asset not found",
      };
    }
    if (row.kind !== "package-registry") {
      return {
        ok: false,
        status: 404,
        code: "not_found",
        message: `asset ${assetId} is not a package-registry`,
      };
    }
    return {
      ok: true,
      resolved: {
        assetId,
        asset: {
          kind: "package-registry",
          id: row.id,
          name: row.name,
        },
      },
    };
  }

  function validateTarballFilename(filename: string): string | null {
    if (filename.length === 0) return "filename must not be empty";
    if (!TARBALL_FILENAME_PATTERN.test(filename)) {
      return `filename must match ${TARBALL_FILENAME_PATTERN} and end in .tgz`;
    }
    if (asTarballEntry(`${TARBALLS_PREFIX}${filename}`) === null) {
      return "filename does not parse as a tarball entry";
    }
    return null;
  }

  app.put(
    "/:assetId/tarballs/:filename",
    requireGrant(idResource("asset", "assetId"), "write"),
    describeRoute({
      tags: ["Assets"],
      summary: "Upload a tarball into a package-registry asset",
      description:
        "Commits raw tarball bytes at tarballs/<filename> in the package-registry asset's git tree. Overwrites permitted. The kind handler validates the tarball's package.json before the commit is accepted.",
      responses: {
        200: {
          description: "Tarball stored",
          content: {
            "application/json": { schema: resolver(TarballPutResponse) },
          },
        },
        400: {
          description: "Invalid filename or rejected content",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Asset not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const assetId = c.req.param("assetId");
      const filename = c.req.param("filename");
      const filenameErr = validateTarballFilename(filename);
      if (filenameErr !== null) {
        return c.json(
          { error: { code: "bad_request", message: filenameErr } },
          400,
        );
      }

      const lookup = await resolveRegistryAsset(tenantCtx.id, assetId);
      if (!lookup.ok) {
        return c.json(
          { error: { code: lookup.code, message: lookup.message } },
          lookup.status,
        );
      }

      // Pre-buffer guard: reject obviously-oversize uploads on the
      // declared Content-Length before we read a single byte of body.
      // Clients that omit Content-Length still flow into the streaming
      // guard below, but the header check spares the server the
      // round-trip for the common case where the client knows its
      // payload size.
      const declaredLengthRaw = c.req.raw.headers.get("content-length");
      if (declaredLengthRaw !== null) {
        // RFC 9110 §8.6 defines Content-Length as `1*DIGIT`. `Number()`
        // accepts decimal, hex (`0x10`), and scientific notation
        // (`1e3`), which would let a malicious client smuggle a value
        // past the cap (`1e1` reads as 10 bytes but the literal header
        // text fails the digit check) or land an unrepresentably-large
        // value (`1e308`). Insist on the digit-only shape so the
        // header is exactly what RFC 9110 says it is.
        if (!/^\d+$/.test(declaredLengthRaw)) {
          return c.json(
            {
              error: {
                code: "bad_request",
                message: "Content-Length must be a non-negative integer",
              },
            },
            400,
          );
        }
        const declaredLength = Number(declaredLengthRaw);
        if (
          !Number.isFinite(declaredLength) ||
          declaredLength > maxTarballBytes
        ) {
          return c.json(
            {
              error: {
                code: "payload_too_large",
                message: `tarball exceeds maximum size of ${String(maxTarballBytes)} bytes`,
              },
            },
            413,
          );
        }
      }

      const bytes = await readBodyWithLimit(c.req.raw, maxTarballBytes);
      if (bytes === null) {
        return c.json(
          {
            error: {
              code: "payload_too_large",
              message: `tarball exceeds maximum size of ${String(maxTarballBytes)} bytes`,
            },
          },
          413,
        );
      }
      // Integrity is computed from the request bytes. The substrate
      // stores those bytes verbatim, so request-bytes-integrity ===
      // stored-blob-integrity by construction at this layer. Any
      // future substrate transformation (compression, re-encoding,
      // metadata stripping) must re-derive integrity from the
      // post-commit blob; otherwise the value returned here lies
      // about what the asset will serve back on GET.
      const integrity = ssri
        .fromData(Buffer.from(bytes), { algorithms: ["sha512"] })
        .toString();

      // Read-then-write happens inside the substrate's per-repo lock so
      // two concurrent PUTs against different filenames in the same
      // asset cannot both pre-image the prior tip and clobber each
      // other's just-staged entries.
      try {
        const { commitSha } = await repoStore.writeTreePreservingPrefix(
          hubPrincipal,
          { kind: "package-registry", id: lookup.resolved.asset.id },
          DEFAULT_ASSET_REF,
          {
            preservePrefix: TARBALLS_PREFIX,
            merge: async (existing) => {
              const files: Record<string, Uint8Array> = {};
              for (const [p, blob] of existing) {
                files[p] = blob;
              }
              files[`${TARBALLS_PREFIX}${filename}`] = bytes;
              return files;
            },
            message: `Upload ${filename}`,
          },
        );
        log.info(
          "tarball uploaded {tenantId} asset={assetId} file={filename}",
          {
            tenantId: tenantCtx.id,
            assetId,
            filename,
          },
        );
        return c.json({ commit: commitSha, integrity });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.startsWith("path_violation:")) {
          return c.json(
            { error: { code: "path_violation", message: msg } },
            400,
          );
        }
        throw err;
      }
    },
  );

  app.get(
    "/:assetId/tarballs",
    requireGrant(idResource("asset", "assetId"), "read"),
    describeRoute({
      tags: ["Assets"],
      summary: "List tarballs in a package-registry asset",
      description:
        "Returns the current set of tarballs under tarballs/ for the package-registry asset, with size and SRI integrity for each entry.",
      responses: {
        200: {
          description: "Tarball list",
          content: {
            "application/json": { schema: resolver(TarballListResponse) },
          },
        },
        404: {
          description: "Asset not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const assetId = c.req.param("assetId");
      const lookup = await resolveRegistryAsset(tenantCtx.id, assetId);
      if (!lookup.ok) {
        return c.json(
          { error: { code: lookup.code, message: lookup.message } },
          lookup.status,
        );
      }
      const names = await assetService
        .listAssetBlobs({
          assetId: lookup.resolved.asset.id,
          dir: "tarballs",
        })
        .catch((cause) => {
          if (
            cause instanceof AssetServiceError &&
            cause.reason === "not_found"
          ) {
            return [] as string[];
          }
          throw cause;
        });
      const out: { filename: string; size: number; integrity: string }[] = [];
      for (const name of names) {
        const blob = await assetService.readAssetBlob({
          assetId: lookup.resolved.asset.id,
          path: `${TARBALLS_PREFIX}${name}`,
        });
        const integrity = ssri
          .fromData(Buffer.from(blob), { algorithms: ["sha512"] })
          .toString();
        out.push({ filename: name, size: blob.byteLength, integrity });
      }
      return c.json(out);
    },
  );

  app.delete(
    "/:assetId/tarballs/:filename",
    requireGrant(idResource("asset", "assetId"), "write"),
    describeRoute({
      tags: ["Assets"],
      summary: "Delete a tarball from a package-registry asset",
      description:
        "Removes the named tarball from the package-registry asset and commits the resulting tree. Returns 404 if the asset or filename does not exist.",
      responses: {
        200: {
          description: "Tarball removed",
          content: {
            "application/json": { schema: resolver(TarballDeleteResponse) },
          },
        },
        400: {
          description: "Invalid filename",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
        404: {
          description: "Asset or filename not found",
          content: {
            "application/json": { schema: resolver(ErrorResponse) },
          },
        },
      },
    }),
    async (c) => {
      const tenantCtx = c.get("tenant");
      const assetId = c.req.param("assetId");
      const filename = c.req.param("filename");
      const filenameErr = validateTarballFilename(filename);
      if (filenameErr !== null) {
        return c.json(
          { error: { code: "bad_request", message: filenameErr } },
          400,
        );
      }

      const lookup = await resolveRegistryAsset(tenantCtx.id, assetId);
      if (!lookup.ok) {
        return c.json(
          { error: { code: lookup.code, message: lookup.message } },
          lookup.status,
        );
      }

      // Existence check + write-back happen inside the substrate's
      // per-repo lock via writeTreePreservingPrefix so two concurrent
      // DELETEs (or a concurrent PUT) cannot race the pre-image.
      // The merge callback throws TarballNotFoundError when the
      // target filename is absent at the locked-in pre-image; the
      // outer try translates it into a 404 response.
      try {
        const { commitSha } = await repoStore.writeTreePreservingPrefix(
          hubPrincipal,
          { kind: "package-registry", id: lookup.resolved.asset.id },
          DEFAULT_ASSET_REF,
          {
            preservePrefix: TARBALLS_PREFIX,
            merge: async (existing) => {
              const target = `${TARBALLS_PREFIX}${filename}`;
              if (!existing.has(target)) {
                throw new TarballNotFoundError(filename, assetId);
              }
              const files: Record<string, Uint8Array> = {};
              for (const [p, blob] of existing) {
                if (p === target) continue;
                files[p] = blob;
              }
              return files;
            },
            message: `Delete ${filename}`,
          },
        );
        log.info("tarball deleted {tenantId} asset={assetId} file={filename}", {
          tenantId: tenantCtx.id,
          assetId,
          filename,
        });
        return c.json({ commit: commitSha });
      } catch (err) {
        if (err instanceof TarballNotFoundError) {
          return c.json(
            {
              error: {
                code: "not_found",
                message: `tarball ${err.filename} not found in asset ${err.assetId}`,
              },
            },
            404,
          );
        }
        throw err;
      }
    },
  );

  // ----- Smart-HTTP route group -------------------------------------
  //
  // The bearer middleware is mounted by the app layer ahead of this
  // route group (so the `principal`, `tenant`, and `git-token-claims`
  // context variables are populated before any handler runs). The
  // handlers here resolve the asset row from `:kind/:nameDotGit`,
  // build the pre-resolved authz verdict, construct a UserPrincipal,
  // and dispatch to the wire handlers.

  async function resolveAssetFromUrl(
    c: { req: { param: (n: string) => string | undefined } },
    tenantId: string,
  ): Promise<
    | { ok: true; asset: AssetLookup; kind: RepoKind }
    | { ok: false; status: 400 | 404; code: string; message: string }
  > {
    const kindRaw = c.req.param("kind");
    const nameRaw = c.req.param("nameDotGit");
    if (kindRaw === undefined || nameRaw === undefined) {
      return {
        ok: false,
        status: 400,
        code: "bad_request",
        message: "Missing :kind or :name in URL",
      };
    }
    const kind = parseKind(kindRaw);
    if (kind === null) {
      return {
        ok: false,
        status: 404,
        code: "not_found",
        message: `unknown asset kind: ${kindRaw}`,
      };
    }
    const name = stripGitSuffix(nameRaw);
    if (name === null) {
      return {
        ok: false,
        status: 400,
        code: "bad_request",
        message: `URL :name must end in .git, got ${nameRaw}`,
      };
    }
    if (!ASSET_NAME_URL_PATTERN.test(name)) {
      return {
        ok: false,
        status: 400,
        code: "bad_request",
        message: `malformed asset name: ${name}`,
      };
    }
    // Smart-HTTP asset lookup is intentionally scoped to the requesting
    // tenant only — inherited assets are NOT visible here. The smart-HTTP
    // surface exposes the underlying git repo for direct clone/push, and
    // those repos live on exactly one tenant; an inherited asset's repo
    // lives on its owning ancestor and is reachable only via that
    // tenant's bearer token, not the descendant's. The REST tarball
    // routes use the tenancy walker (`resolveAssetById` and friends)
    // because they serve resolver-derived materializations that
    // descendants legitimately read from inherited rows. Do not widen
    // this query to the ancestor chain without rethinking how bearer
    // tokens scope to repo ownership.
    const row = await db.query.asset.findFirst({
      where: and(
        eq(assetTable.tenantId, tenantId),
        eq(assetTable.kind, kind),
        eq(assetTable.name, name),
      ),
    });
    if (row === undefined) {
      return {
        ok: false,
        status: 404,
        code: "not_found",
        message: `no asset ${kind}/${name}`,
      };
    }
    let narrowedKind: RepoKind;
    if (row.kind === "agent-state") narrowedKind = "agent-state";
    else if (row.kind === "skill") narrowedKind = "skill";
    else if (row.kind === "package-registry") narrowedKind = "package-registry";
    else {
      return {
        ok: false,
        status: 404,
        code: "not_found",
        message: `asset row ${row.id} carries unsupported kind ${row.kind}`,
      };
    }
    return {
      ok: true,
      asset: {
        id: row.id,
        tenantId: row.tenantId,
        kind: narrowedKind,
        name: row.name,
      },
      kind: narrowedKind,
    };
  }

  // Capture: tenant resolution is normally handled by the tenant
  // middleware (session-based), but bearer requests skip the user
  // session pipeline. The bearer middleware itself sets
  // `principal`/`tenant` on the context, so the handlers here read
  // straight from `c.get(...)` rather than re-querying the DB.

  type SmartHttpResolved = {
    principal: UserPrincipal;
    repoId: RepoId;
  };

  async function resolveSmartHttp(
    c: Context<TenantGitTokenEnv>,
    action: RepoAction,
  ): Promise<
    | { ok: true; resolved: SmartHttpResolved }
    | {
        ok: false;
        status: 400 | 403 | 404;
        code: string;
        message: string;
      }
  > {
    const tenantRow = c.get("tenant");
    const principalRow = c.get("principal");
    const claims: GitTokenClaims = c.get("git-token-claims");
    // The typed env makes this unreachable today, but if the route
    // module is ever mounted without the bearer middleware ahead of
    // it, surface a misconfiguration rather than a downstream
    // TypeError. A 401 would imply the client was unauthenticated;
    // a missing claims object means the server is misconfigured.
    if (claims === undefined) {
      throw new Error(
        "smart-HTTP route handler invoked without bearer middleware; check the mount order in app.ts",
      );
    }
    if (!claims.actions.includes(action)) {
      return {
        ok: false,
        status: 403,
        code: "forbidden",
        message: `token claims do not include action ${action}`,
      };
    }
    const tenantId = tenantRow.id;
    const resolvedAsset = await resolveAssetFromUrl(c, tenantId);
    if (!resolvedAsset.ok) return resolvedAsset;
    const authz = await resolveAuthzVerdict({
      grantStore,
      conditionRegistry,
      principalId: principalRow.id,
      tenantId,
      assetId: resolvedAsset.asset.id,
      action,
    });
    if (authz.effect !== "allow") {
      log.info("smart-HTTP authz denied {tenantId} asset={assetId}", {
        tenantId,
        assetId: resolvedAsset.asset.id,
      });
      return {
        ok: false,
        status: 403,
        code: "forbidden",
        message: "authz denied",
      };
    }
    const principal = buildUserPrincipal({
      principalId: principalRow.id,
      tenantId,
      authz,
      claims,
    });
    const repoId: RepoId = {
      kind: resolvedAsset.kind,
      id: resolvedAsset.asset.id,
    };
    return { ok: true, resolved: { principal, repoId } };
  }

  // The smart-HTTP sub-app is typed against `TenantGitTokenEnv` so the
  // bearer middleware's `git-token-claims` variable narrows naturally
  // at the handler site. The bearer middleware is mounted in `app.ts`
  // ahead of this route surface, so the variable is statically present.
  const smartHttp = new Hono<TenantGitTokenEnv>();

  smartHttp.get("/:kind/:nameDotGit/info/refs", async (c) => {
    const service = c.req.query("service");
    if (service !== "git-upload-pack" && service !== "git-receive-pack") {
      return c.json(
        {
          error: {
            code: "bad_request",
            message:
              "info/refs requires service=git-upload-pack or git-receive-pack",
          },
        },
        400,
      );
    }
    // info/refs maps to the `resolveRef` RepoAction for the bearer
    // claims gate; the substrate's createPack handler runs later and
    // its own gate covers the upload itself.
    const r = await resolveSmartHttp(c, "resolveRef");
    if (!r.ok) {
      return c.json({ error: { code: r.code, message: r.message } }, r.status);
    }
    const refSource = makeRefSource(repoStore, r.resolved.principal);
    const stream =
      service === "git-upload-pack"
        ? await advertiseUploadPack(
            refSource,
            r.resolved.principal,
            r.resolved.repoId,
          )
        : await advertiseReceivePack(
            refSource,
            r.resolved.principal,
            r.resolved.repoId,
          );
    return new Response(stream, {
      status: 200,
      headers: {
        "content-type": `application/x-${service}-advertisement`,
        "cache-control": "no-cache",
      },
    });
  });

  smartHttp.post("/:kind/:nameDotGit/git-upload-pack", async (c) => {
    const r = await resolveSmartHttp(c, "createPack");
    if (!r.ok) {
      return c.json({ error: { code: r.code, message: r.message } }, r.status);
    }
    return handleUploadPack(
      makeUploadPackStore(repoStore, r.resolved.principal),
      r.resolved.principal,
      r.resolved.repoId,
      c.req.raw,
    );
  });

  smartHttp.post("/:kind/:nameDotGit/git-receive-pack", async (c) => {
    const r = await resolveSmartHttp(c, "receivePack");
    if (!r.ok) {
      return c.json({ error: { code: r.code, message: r.message } }, r.status);
    }
    return handleReceivePack(
      repoStore,
      r.resolved.principal,
      r.resolved.repoId,
      c.req.raw,
    );
  });

  app.route("/", smartHttp);

  return app;
}

/**
 * Paths under the asset routes Hono app that the smart-HTTP wire
 * vocabulary touches. The hub-api app excludes these from the OpenAPI
 * document so the generated spec does not advertise binary wire
 * endpoints.
 */
export const ASSET_OPENAPI_EXCLUDE_GLOBS = [
  "/api/tenants/*/assets/*/*.git/info/refs",
  "/api/tenants/*/assets/*/*.git/git-upload-pack",
  "/api/tenants/*/assets/*/*.git/git-receive-pack",
] as const;
