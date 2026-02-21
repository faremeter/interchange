import type { Context } from "hono";
import { Sidecar } from "../types";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "routes", "credentials"]);

export function createCredentialRoutes(sidecar: Sidecar) {
  return {
    "/credentials/:agentId": {
      PUT: async (c: Context) => {
        const _agentId = c.req.param("agentId");
        const body = await c.req.json<{
          credentials?: {
            id: string;
            type: string;
            data: Record<string, string>;
          }[];
        }>();
        const { credentials } = body;

        logger.info(`Storing credentials for agent ${_agentId}`);
        sidecar.storeCredentials(_agentId, credentials || []);

        return c.json({ success: true });
      },
      GET: async (c: Context) => {
        const agentId = c.req.param("agentId");
        const credentials = sidecar.getCredentials(agentId);
        return c.json({ credentials });
      },
    },
    "/credentials/:agentId/refresh": {
      POST: async (_c: Context) => {
        return _c.json({
          error: "Refresh not yet implemented - would request from Hub",
        });
      },
    },
  };
}
