import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import type { Auth } from "./auth";
import type { AppEnv } from "./context";
import { meRoutes } from "./routes/me";
import { tenantRoutes } from "./routes/tenants";
import { principalRoutes, inviteRoutes } from "./routes/principals";
import { roleRoutes, roleAssignRoutes } from "./routes/roles";
import { grantRoutes, evaluateRoutes } from "./routes/grants";
import { agentRoutes } from "./routes/agents";
import { sessionRoutes } from "./routes/sessions";
import { approvalRoutes } from "./routes/approvals";
import { walletRoutes } from "./routes/wallets";
import { credentialRoutes } from "./routes/credentials";
import { capabilityRoutes, modelRoutes } from "./routes/capabilities";
import { observabilityRoutes } from "./routes/observability";
import { agentDataRoutes } from "./routes/agent-data";

export type CreateAppOpts = {
  auth: Auth;
};

export function createApp({ auth }: CreateAppOpts) {
  const app = new Hono<AppEnv>();

  app.use(async (c, next) => {
    const result = await auth.api.getSession({
      headers: c.req.raw.headers,
    });
    c.set("user", result?.user ?? null);
    c.set("session", result?.session ?? null);
    await next();
  });

  app.on(["POST", "GET"], "/api/auth/**", (c) => {
    return auth.handler(c.req.raw);
  });

  app.get("/status", (c) => c.json({ status: "ok" }));

  // User-scoped (cross-tenant)
  app.route("/api/me", meRoutes);

  // Global
  app.route("/api/tenants", tenantRoutes);
  app.route("/api/models", modelRoutes);

  // Tenant-scoped
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
  app.route("/api/tenants/:tenantId/credentials", credentialRoutes);
  app.route("/api/tenants/:tenantId/capabilities", capabilityRoutes);
  app.route("/api/tenants/:tenantId", observabilityRoutes);
  app.route("/api/tenants/:tenantId/agents/:agentId", agentDataRoutes);

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
