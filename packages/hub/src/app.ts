import type { Handler } from "hono";
import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import { honoLogger, type HonoContext } from "@interchange/log/hono";
import type { AppEnv } from "./context";
import { createSessionMiddleware } from "./middleware/session";
import { requireAuth, resolveTenant } from "./middleware/tenant";
import type { GetSession } from "./session";
import { meRoutes } from "./routes/me";
import { tenantRoutes } from "./routes/tenants";
import { tenantFederationRoutes } from "./routes/tenant-federation";
import { principalRoutes, inviteRoutes } from "./routes/principals";
import { roleRoutes, roleAssignRoutes } from "./routes/roles";
import { grantRoutes, evaluateRoutes } from "./routes/grants";
import { agentRoutes } from "./routes/agents";
import { instanceRoutes } from "./routes/instances";

import { approvalRoutes } from "./routes/approvals";
import { walletRoutes } from "./routes/wallets";
import { providerRoutes } from "./routes/providers";
import { oauthClientRoutes } from "./routes/oauth-clients";
import { credentialRoutes } from "./routes/credentials";
import { offeringRoutes, modelRoutes } from "./routes/offerings";
import { observabilityRoutes } from "./routes/observability";
import { agentDataRoutes } from "./routes/agent-data";
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
  app.route("/api/me", meRoutes);

  // Tenant-scoped middleware -- require auth + tenant membership for any
  // path under /api/tenants/:tenantId/*. Must be registered before routes
  // so Hono includes it in the middleware chain.
  app.use("/api/tenants/:tenantId/*", resolveTenant);

  // Global tenant routes (create needs auth, detail/update handle auth inline)
  app.route("/api/tenants", tenantRoutes);
  app.route("/api/models", modelRoutes);

  // Tenant-scoped routes
  app.route("/api/tenants/:tenantId/principals", principalRoutes);
  app.route("/api/tenants/:tenantId/members/invite", inviteRoutes);
  app.route("/api/tenants/:tenantId/roles", roleRoutes);
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/roles",
    roleAssignRoutes,
  );
  app.route("/api/tenants/:tenantId/grants", grantRoutes);
  app.route(
    "/api/tenants/:tenantId/principals/:principalId/evaluate",
    evaluateRoutes,
  );
  app.route("/api/tenants/:tenantId/agents/definitions", agentRoutes);
  app.route("/api/tenants/:tenantId/agents/instances", instanceRoutes);

  app.route("/api/tenants/:tenantId/approvals", approvalRoutes);
  app.route("/api/tenants/:tenantId/wallets", walletRoutes);
  app.route("/api/tenants/:tenantId/providers", providerRoutes);
  app.route("/api/tenants/:tenantId/oauth-clients", oauthClientRoutes);
  app.route("/api/tenants/:tenantId/credentials", credentialRoutes);
  app.route("/api/tenants/:tenantId/offerings", offeringRoutes);
  app.route("/api/tenants/:tenantId", observabilityRoutes);
  app.route("/api/tenants/:tenantId/federation", tenantFederationRoutes);
  app.route("/api/tenants/:tenantId/agents/:agentId", agentDataRoutes);

  app.route("/api/sidecars", createSidecarRoutes(sidecarWsHandler));

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
