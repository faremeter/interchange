import { Hono } from "hono";
import { openAPIRouteHandler } from "hono-openapi";

import type { AppEnv } from "./context";

export function createApp() {
  const app = new Hono<AppEnv>();

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
      exclude: ["/openapi.json", "/status"],
    }),
  );

  return app;
}

export type App = ReturnType<typeof createApp>;
