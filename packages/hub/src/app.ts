import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import { honoLogger, type HonoContext } from "@interchange/log/hono";
import type { Auth } from "./auth";
import type { AppEnv } from "./context";
import { requireAuth, resolveTenant } from "./middleware/tenant";
import { meRoutes } from "./routes/me";
import { tenantRoutes } from "./routes/tenants";
import { tenantFederationRoutes } from "./routes/tenant-federation";
import { principalRoutes, inviteRoutes } from "./routes/principals";
import { roleRoutes, roleAssignRoutes } from "./routes/roles";
import { grantRoutes, evaluateRoutes } from "./routes/grants";
import { agentRoutes } from "./routes/agents";
import { sessionRoutes } from "./routes/sessions";
import { approvalRoutes } from "./routes/approvals";
import { walletRoutes } from "./routes/wallets";
import { providerRoutes } from "./routes/providers";
import { oauthClientRoutes } from "./routes/oauth-clients";
import { credentialRoutes } from "./routes/credentials";
import { offeringRoutes, modelRoutes } from "./routes/offerings";
import { observabilityRoutes } from "./routes/observability";
import { agentDataRoutes } from "./routes/agent-data";
import { default as sidecarRoutes } from "./routes/sidecars";
import { createWsRoutes } from "./routes/ws";

import { type DB, createGrantStore } from "@interchange/db";
import type { ConditionRegistry } from "@interchange/types/authz";
import { timeWindowEvaluator } from "@interchange/authz";

export type CreateAppOpts = {
  auth: Auth;
  db: DB["db"];
};

export function createApp({ auth, db }: CreateAppOpts) {
  const app = new Hono<AppEnv>();
  const grantStore = createGrantStore(db);
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
    const result = await auth.api.getSession({
      headers: c.req.raw.headers,
    });
    c.set("user", result?.user ?? null);
    c.set("session", result?.session ?? null);
    await next();
  });

  app.all("/api/auth/*", (c) => {
    return auth.handler(c.req.raw);
  });

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
  app.route("/api/tenants/:tenantId/agents", agentRoutes);
  app.route("/api/tenants/:tenantId/sessions", sessionRoutes);
  app.route("/api/tenants/:tenantId/approvals", approvalRoutes);
  app.route("/api/tenants/:tenantId/wallets", walletRoutes);
  app.route("/api/tenants/:tenantId/providers", providerRoutes);
  app.route("/api/tenants/:tenantId/oauth-clients", oauthClientRoutes);
  app.route("/api/tenants/:tenantId/credentials", credentialRoutes);
  app.route("/api/tenants/:tenantId/offerings", offeringRoutes);
  app.route("/api/tenants/:tenantId", observabilityRoutes);
  app.route("/api/tenants/:tenantId/federation", tenantFederationRoutes);
  app.route("/api/tenants/:tenantId/agents/:agentId", agentDataRoutes);

  app.route("/api/sidecars", sidecarRoutes);

  const wsRoutes = createWsRoutes();
  app.get("/ws/agents/:agentId", wsRoutes["/ws/agents/:agentId"].GET);

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
