import type { Handler, MiddlewareHandler } from "hono";
import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import { honoLogger, type HonoContext } from "@intx/log/hono";
import { timeWindowEvaluator } from "@intx/authz";
import {
  type DB,
  type ApprovalStore,
  type SignalCorrelationStore,
  createGrantStore,
  createApprovalStore,
  createSignalCorrelationStore,
} from "@intx/db";
import type { ConditionRegistry, GrantStore } from "@intx/types/authz";

import type { AppEnv } from "./context";
import { createSessionMiddleware } from "./middleware/session";
import { createRequireGrant, type RequireGrant } from "./middleware/grant";
import { createResolveTenant, requireAuth } from "./middleware/tenant";
import type { GetSession } from "./session";
import type {
  AssetService,
  EventCollectorRegistry,
  RepoStore,
  SessionService,
  SidecarRouter,
} from "@intx/hub-sessions";

import { createMeRoutes } from "./routes/me";
import { createTenantRoutes } from "./routes/tenants";
import { createTenantFederationRoutes } from "./routes/tenant-federation";
import { createPrincipalRoutes, createInviteRoutes } from "./routes/principals";
import { createRoleRoutes, createRoleAssignRoutes } from "./routes/roles";
import { createGrantRoutes, createEvaluateRoutes } from "./routes/grants";
import { createAgentRoutes } from "./routes/agents";
import { createInstanceRoutes } from "./routes/instances";
import { createWorkflowRoutes } from "./routes/workflows";
import { createApprovalRoutes } from "./routes/approvals";
import { createWalletRoutes } from "./routes/wallets";
import { createProviderRoutes } from "./routes/providers";
import { createOAuthClientRoutes } from "./routes/oauth-clients";
import { createCredentialRoutes } from "./routes/credentials";
import { createOfferingRoutes } from "./routes/offerings";
import {
  createModelCatalogRoutes,
  createModelDiscoveryRoutes,
} from "./routes/models";
import { createModelProviderRoutes } from "./routes/model-providers";
import { createModelOfferingRoutes } from "./routes/model-offerings";
import { createObservabilityRoutes } from "./routes/observability";
import { createAgentDataRoutes } from "./routes/agent-data";
import {
  createMeGitTokenRoutes,
  createTenantGitTokenRoutes,
} from "./routes/git-tokens";
import {
  ASSET_OPENAPI_EXCLUDE_GLOBS,
  createAssetRoutes,
} from "./routes/assets";
import {
  AGENT_STATE_OPENAPI_EXCLUDE_GLOBS,
  createAgentStateDefinitionGitRoutes,
  createAgentStateInstanceGitRoutes,
  createAgentStateReceivePackDeny,
} from "./routes/agent-state-git";
import { createGitTokenAuth } from "./middleware/git-token-auth";

export type CreateHubContextMiddlewareDeps = {
  getSession: GetSession;
};

/**
 * Builds the per-request context middleware that resolves the
 * authenticated user and session from the incoming request and
 * exposes them via the Hono variable bag.
 */
export function createHubContextMiddleware({
  getSession,
}: CreateHubContextMiddlewareDeps): MiddlewareHandler<AppEnv> {
  return createSessionMiddleware(getSession);
}

export type MountHubRoutesDeps = {
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  sessionService: SessionService;
  eventCollectors: EventCollectorRegistry;
  grantStore?: GrantStore;
  conditionRegistry?: ConditionRegistry;
  approvalStore?: ApprovalStore;
  signalCorrelationStore?: SignalCorrelationStore;
  sidecarWsHandler?: Handler<AppEnv>;
  /**
   * The asset REST endpoint and smart-HTTP route group mount under
   * `/api/tenants/:tenantId/assets` when both are supplied. Tests
   * that have no reason to exercise the asset surface MUST pass
   * `null` for both to opt out explicitly; passing only one is a
   * wiring bug and throws at construction.
   */
  assetService: AssetService | null;
  repoStore: RepoStore | null;
  /**
   * Maximum tarball payload accepted by the package-registry PUT
   * endpoint. The hub edge owns the value; tests that exercise the
   * asset surface supply their own cap.
   */
  maxTarballBytes: number;
};

/**
 * Mounts every hub route group, middleware, and supporting endpoint
 * onto the provided Hono application. The caller is responsible for
 * having mounted the request logger and the context middleware first,
 * and for wiring their own auth handler at the path of their choice.
 *
 * `grantStore` and `conditionRegistry` default to the hub's standard
 * choices (a database-backed grant store and the time-window condition
 * evaluator) when not supplied.
 */
export function mountHubRoutes(
  app: Hono<AppEnv>,
  opts: MountHubRoutesDeps,
): void {
  const {
    db,
    sidecarRouter,
    sessionService,
    eventCollectors,
    sidecarWsHandler,
    assetService,
    repoStore,
    maxTarballBytes,
  } = opts;
  if ((assetService === null) !== (repoStore === null)) {
    throw new Error(
      "mountHubRoutes: assetService and repoStore must be provided together or both omitted",
    );
  }
  const grantStore = opts.grantStore ?? createGrantStore(db);
  const conditionRegistry: ConditionRegistry = opts.conditionRegistry ?? {
    time_window: timeWindowEvaluator,
  };
  const requireGrant: RequireGrant = createRequireGrant({
    grantStore,
    conditionRegistry,
  });
  const approvalStore = opts.approvalStore ?? createApprovalStore(db);
  const signalCorrelationStore =
    opts.signalCorrelationStore ?? createSignalCorrelationStore(db);
  const resolveTenant = createResolveTenant({ db });

  app.get("/status", (c) => c.json({ status: "ok" }));

  // User-scoped (cross-tenant) -- requires auth but not tenant membership
  app.use("/api/me/*", requireAuth);
  app.route("/api/me", createMeRoutes({ db }));

  // The git-tokens mint surface mounts under the same gate as the
  // smart-HTTP route groups: tokens are only useful when at least one
  // smart-HTTP route consumes them. Both deps null = no smart-HTTP
  // anywhere = no token-mint endpoints.
  if (repoStore !== null) {
    app.route("/api/me/git-tokens", createMeGitTokenRoutes({ db }));
  }

  // Smart-HTTP asset routes use bearer authentication instead of
  // session+tenant resolution. The bearer middleware mounts ahead of
  // resolveTenant so it populates `principal` + `tenant` first; the
  // tenant resolver short-circuits when both are already set, which
  // lets bearer-only requests bypass the session-required path.
  //
  // The gate is `repoStore !== null` rather than the two-dep check;
  // the XOR throw above already guarantees the deps move as a unit,
  // so checking either one is equivalent. Keeping a single shape
  // across every gate site makes the contract obvious to a reader.
  if (repoStore !== null) {
    // Constrain `:nameDotGit` to the `.git` suffix so the bearer
    // middleware does not capture the REST tarball routes that share
    // the `/api/tenants/:tenantId/assets/...` prefix.
    app.use(
      "/api/tenants/:tenantId/assets/:kind/:nameDotGit{[^/]+\\.git}/*",
      createGitTokenAuth({ db }),
    );
  }

  // Agent-state smart-HTTP read routes also use bearer auth. The
  // receive-pack denial middleware mounts BEFORE bearer auth so an
  // unauthenticated `git push -v` parses the pkt-line ERR record
  // rather than a generic 401. The bearer middleware then gates the
  // upload-pack half (advertise + POST) on a valid token.
  if (repoStore !== null) {
    app.use(
      "/api/tenants/:tenantId/agents/instances/:instanceId/state.git/*",
      createAgentStateReceivePackDeny(),
    );
    app.use(
      "/api/tenants/:tenantId/agents/definitions/:agentId/state.git/*",
      createAgentStateReceivePackDeny(),
    );
    app.use(
      "/api/tenants/:tenantId/agents/instances/:instanceId/state.git/*",
      createGitTokenAuth({ db }),
    );
    app.use(
      "/api/tenants/:tenantId/agents/definitions/:agentId/state.git/*",
      createGitTokenAuth({ db }),
    );
  }

  // Tenant-scoped middleware -- require auth + tenant membership for any
  // path under /api/tenants/:tenantId/*. Must be registered before routes
  // so Hono includes it in the middleware chain.
  app.use("/api/tenants/:tenantId/*", resolveTenant);

  // Global tenant routes (create needs auth, detail/update handle auth inline)
  app.route("/api/tenants", createTenantRoutes({ db }));

  // Tenant-scoped routes
  app.route(
    "/api/tenants/:tenantId/principals",
    createPrincipalRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/members/invite",
    createInviteRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/roles",
    createRoleRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/roles",
    createRoleAssignRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/grants",
    createGrantRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/evaluate",
    createEvaluateRoutes({ db, grantStore, conditionRegistry }),
  );
  app.route(
    "/api/tenants/:tenantId/agents/definitions",
    createAgentRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/agents/instances",
    createInstanceRoutes({
      db,
      sessionService,
      sidecarRouter,
      eventCollectors,
      grantStore,
      conditionRegistry,
      requireGrant,
    }),
  );

  // The workflow deploy + signal + listing surface needs the asset
  // service to hydrate a workflow definition from its workflow.json, and
  // the run-observe routes read the workflow-run repo through the repo
  // store. Gate on both being present; the XOR throw above keeps
  // assetService and repoStore moving as a unit, so this also narrows
  // both away from null for the route factory.
  if (assetService !== null && repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/workflows",
      createWorkflowRoutes({
        db,
        sessionService,
        sidecarRouter,
        assetService,
        repoStore,
        grantStore,
        requireGrant,
      }),
    );
  }

  app.route(
    "/api/tenants/:tenantId/approvals",
    createApprovalRoutes({
      db,
      sidecarRouter,
      grantStore,
      conditionRegistry,
      approvalStore,
      signalCorrelationStore,
    }),
  );
  app.route(
    "/api/tenants/:tenantId/wallets",
    createWalletRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/providers",
    createProviderRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/oauth-clients",
    createOAuthClientRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/credentials",
    createCredentialRoutes({ db, sidecarRouter, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/offerings",
    createOfferingRoutes({ db, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/catalog/models",
    createModelCatalogRoutes({ db, sidecarRouter, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/catalog/providers",
    createModelProviderRoutes({ db, sidecarRouter, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/catalog/offerings",
    createModelOfferingRoutes({ db, sidecarRouter, requireGrant }),
  );
  app.route(
    "/api/tenants/:tenantId/models",
    createModelDiscoveryRoutes({ db, requireGrant }),
  );
  if (repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/git-tokens",
      createTenantGitTokenRoutes({ db, requireGrant }),
    );
  }
  app.route("/api/tenants/:tenantId", createObservabilityRoutes());
  app.route(
    "/api/tenants/:tenantId/federation",
    createTenantFederationRoutes({ db }),
  );
  app.route("/api/tenants/:tenantId/agents/:agentId", createAgentDataRoutes());

  if (assetService !== null && repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/assets",
      createAssetRoutes({
        db,
        assetService,
        repoStore,
        grantStore,
        conditionRegistry,
        requireGrant,
        maxTarballBytes,
      }),
    );
  }

  if (repoStore !== null) {
    app.route(
      "/api/tenants/:tenantId/agents/instances",
      createAgentStateInstanceGitRoutes({
        db,
        repoStore,
        grantStore,
        conditionRegistry,
      }),
    );
    app.route(
      "/api/tenants/:tenantId/agents/definitions",
      createAgentStateDefinitionGitRoutes({
        db,
        repoStore,
        grantStore,
        conditionRegistry,
      }),
    );
  }

  if (sidecarWsHandler) {
    app.get("/api/sidecars/ws", sidecarWsHandler);
  }
}

export type CreateAppOpts = {
  getSession: GetSession;
  authHandler: Handler<AppEnv>;
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  sessionService: SessionService;
  eventCollectors: EventCollectorRegistry;
  grantStore?: GrantStore;
  approvalStore?: ApprovalStore;
  signalCorrelationStore?: SignalCorrelationStore;
  sidecarWsHandler?: Handler<AppEnv>;
  assetService: AssetService | null;
  repoStore: RepoStore | null;
  /**
   * Maximum tarball payload accepted by the package-registry PUT
   * endpoint. The hub edge resolves this from `HUB_MAX_TARBALL_BYTES`
   * (or its config default) and supplies a concrete value.
   */
  maxTarballBytes: number;
};

export function createApp({
  getSession,
  authHandler,
  db,
  sidecarRouter,
  sessionService,
  eventCollectors,
  grantStore,
  approvalStore,
  signalCorrelationStore,
  sidecarWsHandler,
  assetService,
  repoStore,
  maxTarballBytes,
}: CreateAppOpts) {
  const app = new Hono<AppEnv>();

  app.use(
    honoLogger({
      category: ["hub", "requests"],
      skip: (c: HonoContext) => c.req.path === "/status",
    }),
  );

  app.use(createHubContextMiddleware({ getSession }));

  app.all("/api/auth/*", authHandler);

  mountHubRoutes(app, {
    db,
    sidecarRouter,
    sessionService,
    eventCollectors,
    assetService,
    repoStore,
    maxTarballBytes,
    ...(grantStore ? { grantStore } : {}),
    ...(approvalStore ? { approvalStore } : {}),
    ...(signalCorrelationStore ? { signalCorrelationStore } : {}),
    ...(sidecarWsHandler ? { sidecarWsHandler } : {}),
  });

  app.get(
    "/openapi.json",
    openAPIRouteHandler(app, {
      documentation: {
        info: {
          title: "Interchange Hub",
          version: "0.0.0",
        },
      },
      exclude: [
        "/openapi.json",
        "/status",
        "/api/auth/**",
        ...ASSET_OPENAPI_EXCLUDE_GLOBS,
        ...AGENT_STATE_OPENAPI_EXCLUDE_GLOBS,
      ],
    }),
  );

  return app;
}

export type App = ReturnType<typeof createApp>;
