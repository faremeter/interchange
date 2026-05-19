import type { Handler, MiddlewareHandler } from "hono";
import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import { honoLogger, type HonoContext } from "@interchange/log/hono";
import { timeWindowEvaluator } from "@interchange/authz";
import { type DB, createGrantStore } from "@interchange/db";
import type { ConditionRegistry, GrantStore } from "@interchange/types/authz";

import type { AppEnv } from "./context";
import { createSessionMiddleware } from "./middleware/session";
import { createRequireGrant, type RequireGrant } from "./middleware/grant";
import { createResolveTenant, requireAuth } from "./middleware/tenant";
import type { GetSession } from "./session";
import type { SessionService } from "./session-service";
import type { SidecarRouter } from "./ws/sidecar-handler";
import type { EventCollectorRegistry } from "./event-collector-registry";

import { createMeRoutes } from "./routes/me";
import { createTenantRoutes } from "./routes/tenants";
import { createTenantFederationRoutes } from "./routes/tenant-federation";
import { createPrincipalRoutes, createInviteRoutes } from "./routes/principals";
import { createRoleRoutes, createRoleAssignRoutes } from "./routes/roles";
import { createGrantRoutes, createEvaluateRoutes } from "./routes/grants";
import { createAgentRoutes } from "./routes/agents";
import { createInstanceRoutes } from "./routes/instances";
import { createApprovalRoutes } from "./routes/approvals";
import { createWalletRoutes } from "./routes/wallets";
import { createProviderRoutes } from "./routes/providers";
import { createOAuthClientRoutes } from "./routes/oauth-clients";
import { createCredentialRoutes } from "./routes/credentials";
import { createOfferingRoutes, createModelRoutes } from "./routes/offerings";
import { createObservabilityRoutes } from "./routes/observability";
import { createAgentDataRoutes } from "./routes/agent-data";
import { createSidecarRoutes } from "./routes/sidecars";

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
  sidecarWsHandler?: Handler<AppEnv>;
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
  } = opts;
  const grantStore = opts.grantStore ?? createGrantStore(db);
  const conditionRegistry: ConditionRegistry = opts.conditionRegistry ?? {
    time_window: timeWindowEvaluator,
  };
  const requireGrant: RequireGrant = createRequireGrant({
    grantStore,
    conditionRegistry,
  });
  const resolveTenant = createResolveTenant({ db });

  app.get("/status", (c) => c.json({ status: "ok" }));

  // User-scoped (cross-tenant) -- requires auth but not tenant membership
  app.use("/api/me/*", requireAuth);
  app.route("/api/me", createMeRoutes({ db }));

  // Tenant-scoped middleware -- require auth + tenant membership for any
  // path under /api/tenants/:tenantId/*. Must be registered before routes
  // so Hono includes it in the middleware chain.
  app.use("/api/tenants/:tenantId/*", resolveTenant);

  // Global tenant routes (create needs auth, detail/update handle auth inline)
  app.route("/api/tenants", createTenantRoutes({ db }));
  app.route("/api/models", createModelRoutes());

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

  app.route("/api/tenants/:tenantId/approvals", createApprovalRoutes());
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
  app.route("/api/tenants/:tenantId", createObservabilityRoutes());
  app.route(
    "/api/tenants/:tenantId/federation",
    createTenantFederationRoutes({ db }),
  );
  app.route("/api/tenants/:tenantId/agents/:agentId", createAgentDataRoutes());

  app.route(
    "/api/sidecars",
    createSidecarRoutes(
      sidecarWsHandler ? { db, wsHandler: sidecarWsHandler } : { db },
    ),
  );
}

export type CreateAppOpts = {
  getSession: GetSession;
  authHandler: Handler<AppEnv>;
  db: DB["db"];
  sidecarRouter: SidecarRouter;
  sessionService: SessionService;
  eventCollectors: EventCollectorRegistry;
  grantStore?: GrantStore;
  sidecarWsHandler?: Handler<AppEnv>;
};

export function createApp({
  getSession,
  authHandler,
  db,
  sidecarRouter,
  sessionService,
  eventCollectors,
  grantStore,
  sidecarWsHandler,
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
    ...(grantStore ? { grantStore } : {}),
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
      exclude: ["/openapi.json", "/status", "/api/auth/**"],
    }),
  );

  return app;
}

export type App = ReturnType<typeof createApp>;
