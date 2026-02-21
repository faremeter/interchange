import type { Context } from "hono";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "routes", "tools"]);

export function createToolRoutes() {
  return {
    "/tools/:toolId/invoke": {
      POST: async (c: Context) => {
        const toolId = c.req.param("toolId");
        const body = await c.req.json<{
          agentId: string;
          params: Record<string, unknown>;
        }>();
        const { params: _params } = body;

        logger.info(`Tool invocation: ${toolId}`);

        return c.json({
          toolId,
          result:
            "Tool invocation not yet implemented - credentials would be added here",
        });
      },
    },
  };
}
