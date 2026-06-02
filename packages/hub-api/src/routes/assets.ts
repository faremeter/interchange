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

import { authorize } from "@intx/authz";
import { asset as assetTable } from "@intx/db/schema";
import type { DB } from "@intx/db";
import { httpToRepoAction, repoActionToGrantVerb } from "@intx/hub-common";
import { getLogger } from "@intx/log";
import {
  AssetServiceError,
  type AssetService,
  type RefEntry,
  type RepoId,
  type RepoStore,
  type UserPrincipal,
} from "@intx/hub-sessions";
import type { RepoAction, RepoKind } from "@intx/types/sidecar";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";
import { ErrorResponse } from "@intx/types";

import type { TenantEnv } from "../context";
import { ts } from "../format";
import type { GitTokenClaims } from "../middleware/git-token-auth";
import type { RequireGrant } from "../middleware/grant";
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

const KIND_VALUES = ["agent-state", "skill"] as const;

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
  if (raw === "agent-state" || raw === "skill") return raw;
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
};

export function createAssetRoutes({
  db,
  assetService,
  repoStore,
  grantStore,
  conditionRegistry,
  requireGrant,
}: CreateAssetRoutesDeps): Hono<TenantEnv> {
  const app = new Hono<TenantEnv>();

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
        return c.json(
          {
            id: asset.id,
            tenantId: asset.tenantId,
            kind: asset.kind,
            name: asset.name,
            displayName: asset.displayName,
            creatorPrincipalId: asset.creatorPrincipalId,
            createdAt: ts(asset.createdAt),
            updatedAt: ts(asset.updatedAt),
          },
          201,
        );
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
    c: Context<TenantEnv>,
    action: RepoAction,
  ): Promise<
    | { ok: true; resolved: SmartHttpResolved }
    | {
        ok: false;
        status: 400 | 401 | 403 | 404;
        code: string;
        message: string;
      }
  > {
    const tenantRow = c.get("tenant");
    const principalRow = c.get("principal");
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the bearer middleware sets git-token-claims on the variable map; the typed env shape is not visible at this nested layer
    const claims = c.get("git-token-claims" as never) as
      | GitTokenClaims
      | undefined;
    if (claims === undefined) {
      return {
        ok: false,
        status: 401,
        code: "unauthorized",
        message: "bearer middleware did not populate git-token-claims",
      };
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

  app.get("/:kind/:nameDotGit/info/refs", async (c) => {
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

  app.post("/:kind/:nameDotGit/git-upload-pack", async (c) => {
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

  app.post("/:kind/:nameDotGit/git-receive-pack", async (c) => {
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

  // Suppress an unused-import lint on httpToRepoAction; the smart-HTTP
  // dispatch above is hand-rolled per-route (each handler hardcodes
  // its RepoAction). httpToRepoAction is still re-exported below for
  // downstream callers that prefer the table-driven shape.
  void httpToRepoAction;

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
