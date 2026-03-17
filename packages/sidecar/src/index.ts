import { Hono } from "hono";
import { Sidecar, type SidecarConfig } from "./types";
import { OpenCodeManager } from "./opencode";
import { createAgentRoutes } from "./routes/agent";
import { createToolRoutes } from "./routes/tools";
import { createCredentialRoutes } from "./routes/credentials";

export async function createApp(config: SidecarConfig) {
  const sidecar = new Sidecar(config);
  const opencode = new OpenCodeManager(sidecar);

  const app = new Hono();

  app.get("/health", (c) => {
    return c.json({
      healthy: true,
      sidecarId: config.sidecarId,
      opencodeRunning: opencode.isRunning(),
    });
  });

  const agentRoutes = createAgentRoutes(opencode);
  app.post("/agents", agentRoutes["/agents"].POST);
  app.get("/agents/:id", agentRoutes["/agents/:id"].GET);
  app.delete("/agents/:id", agentRoutes["/agents/:id"].DELETE);
  app.post("/agents/:id/message", agentRoutes["/agents/:id/message"].POST);
  app.get("/agents/:id/events", agentRoutes["/agents/:id/events"].GET);

  const toolRoutes = createToolRoutes();
  app.post("/tools/:toolId/invoke", toolRoutes["/tools/:toolId/invoke"].POST);

  const credentialRoutes = createCredentialRoutes(sidecar);
  app.put(
    "/credentials/:agentId",
    credentialRoutes["/credentials/:agentId"].PUT,
  );
  app.get(
    "/credentials/:agentId",
    credentialRoutes["/credentials/:agentId"].GET,
  );
  app.post(
    "/credentials/:agentId/refresh",
    credentialRoutes["/credentials/:agentId/refresh"].POST,
  );

  await opencode.start();

  return {
    app,
    sidecar,
    opencode,
  };
}

export type App = Awaited<ReturnType<typeof createApp>>;
