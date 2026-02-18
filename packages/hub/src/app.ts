import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import type { Auth } from "./auth";
import type { AppEnv } from "./context";

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
