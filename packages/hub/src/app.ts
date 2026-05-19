import type { Handler } from "hono";
import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import { honoLogger, type HonoContext } from "@interchange/log/hono";
import type { AppEnv } from "./context";
import { createSessionMiddleware } from "./middleware/session";
import { requireAuth, resolveTenant } from "./middleware/tenant";
import type { GetSession } from "./session";
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

import { type DB, createGrantStore } from "@interchange/db";
import type { ConditionRegistry, GrantStore } from "@interchange/types/authz";
import { timeWindowEvaluator } from "@interchange/authz";
import type { SessionService } from "./session-service";
import type { SidecarRouter } from "./ws/sidecar-handler";
import type { EventCollectorRegistry } from "./event-collector-registry";

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
  grantStore: externalGrantStore,
  sidecarWsHandler,
}: CreateAppOpts) {
  const app = new Hono<AppEnv>();
  const grantStore = externalGrantStore ?? createGrantStore(db);
  const conditionRegistry: ConditionRegistry = {
    time_window: timeWindowEvaluator,
  };

  app.use(
    honoLogger({
      category: ["hub", "requests"],
      skip: (c: HonoContext) => c.req.path === "/status",
    }),
  );

  app.use(async (c, next) => {
    c.set("db", db);
    c.set("grantStore", grantStore);
    c.set("conditionRegistry", conditionRegistry);
    c.set("sidecarRouter", sidecarRouter);
    c.set("sessionService", sessionService);
    c.set("eventCollectors", eventCollectors);
    await next();
  });

  app.use(createSessionMiddleware(getSession));

  app.all("/api/auth/*", authHandler);

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
  app.route("/api/tenants/:tenantId/principals", createPrincipalRoutes({ db }));
  app.route(
    "/api/tenants/:tenantId/members/invite",
    createInviteRoutes({ db }),
  );
  app.route("/api/tenants/:tenantId/roles", createRoleRoutes({ db }));
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/roles",
    createRoleAssignRoutes({ db }),
  );
  app.route("/api/tenants/:tenantId/grants", createGrantRoutes({ db }));
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/evaluate",
    createEvaluateRoutes({ db, grantStore, conditionRegistry }),
  );
  app.route(
    "/api/tenants/:tenantId/agents/definitions",
    createAgentRoutes({ db }),
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
    }),
  );

  app.route("/api/tenants/:tenantId/approvals", createApprovalRoutes());
  app.route("/api/tenants/:tenantId/wallets", createWalletRoutes({ db }));
  app.route("/api/tenants/:tenantId/providers", createProviderRoutes({ db }));
  app.route(
    "/api/tenants/:tenantId/oauth-clients",
    createOAuthClientRoutes({ db }),
  );
  app.route(
    "/api/tenants/:tenantId/credentials",
    createCredentialRoutes({ db, sidecarRouter }),
  );
  app.route("/api/tenants/:tenantId/offerings", createOfferingRoutes({ db }));
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
