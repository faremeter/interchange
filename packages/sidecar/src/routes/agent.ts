import type { Context } from "hono";
import { OpenCodeManager } from "../opencode";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "routes", "agent"]);

export function createAgentRoutes(opencode: OpenCodeManager) {
  return {
    "/agents": {
      POST: async (c: Context) => {
        const body = await c.req.json<{
          agentId: string;
          systemPrompt?: string;
          skills?: string[];
        }>();
        const { agentId, systemPrompt, skills } = body;

        logger.info(`Creating agent session for ${agentId}`);

        const result = await opencode.createSession(
          systemPrompt || "You are a helpful assistant.",
          skills || [],
        );

        if (!result) {
          return c.json({ error: "Failed to create session" }, 500);
        }

        return c.json({
          id: result.id,
          agentId,
          status: "running",
          initialResponse: result.initialResponse,
        });
      },
    },
    "/agents/:id": {
      GET: async (c: Context) => {
        const id = c.req.param("id");
        return c.json({ id, status: "running" });
      },
      DELETE: async (c: Context) => {
        const id = c.req.param("id");
        await opencode.deleteSession(id);
        return c.json({ success: true });
      },
    },
    "/agents/:id/message": {
      POST: async (c: Context) => {
        const id = c.req.param("id");
        const body = await c.req.json<{ text: string }>();
        const { text } = body;

        const result = await opencode.sendMessage(id, text);

        if (!result) {
          return c.json({ error: "Failed to send message" }, 500);
        }

        return c.json({ text: result.text });
      },
    },
  };
}
