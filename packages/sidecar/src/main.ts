import { createApp } from "./index";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar"]);

async function main() {
  const sidecarPort = parseInt(process.env.SIDECAR_PORT || "4097");
  const opencodePort = parseInt(process.env.OPENCODE_PORT || "4096");
  const hubUrl = process.env.HUB_URL || "http://localhost:3000";
  const opencodePassword = process.env.OPENCODE_SERVER_PASSWORD;

  const config: Parameters<typeof createApp>[0] = {
    port: sidecarPort,
    sidecarId: crypto.randomUUID(),
    hubUrl,
    opencodePort,
    ...(opencodePassword ? { opencodePassword } : {}),
  };

  logger.info("Starting sidecar", {
    port: config.port,
    sidecarId: config.sidecarId,
    hubUrl: config.hubUrl,
    opencodePort: config.opencodePort,
  });

  const { app } = await createApp(config);

  logger.info(`Sidecar listening on port ${config.port}`);

  return app;
}

const app = await main();

export default {
  port: parseInt(process.env.SIDECAR_PORT || "4097"),
  fetch: app.fetch,
  // Disable idle timeout so long-lived SSE connections are not killed.
  idleTimeout: 0,
};
