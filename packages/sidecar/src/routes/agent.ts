import type { Context } from "hono";
import { OpenCodeManager } from "../opencode";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "routes", "agent"]);

function sseEvent(type: string, data: Record<string, unknown>): string {
  return `data: ${JSON.stringify({ type, data })}\n\n`;
}

function translateEvent(raw: string): string | null {
  let event: { type: string; properties: Record<string, unknown> };
  try {
    event = JSON.parse(raw) as typeof event;
  } catch {
    return null;
  }

  const { type, properties } = event;

  if (type === "message.part.delta") {
    const p = properties as {
      field?: string;
      delta?: string;
      partID?: string;
      sessionID?: string;
      messageID?: string;
    };
    if (p.field !== "text") return null;
    return sseEvent("inference.token", {
      delta: p.delta ?? "",
      partId: p.partID,
      sessionId: p.sessionID,
      messageId: p.messageID,
    });
  }

  if (type === "message.part.updated") {
    const part = (
      properties as {
        part?: {
          type?: string;
          id?: string;
          sessionID?: string;
          messageID?: string;
          text?: string;
        };
      }
    ).part;
    if (!part || part.type !== "text") return null;
    // Only emit part_start on first creation (text is empty string)
    if (part.text !== "") return null;
    return sseEvent("inference.part_start", {
      partId: part.id,
      sessionId: part.sessionID,
      messageId: part.messageID,
    });
  }

  if (type === "message.updated") {
    const info = (
      properties as {
        info?: { id?: string; sessionID?: string; finish?: string };
      }
    ).info;
    if (!info || info.finish !== "stop") return null;
    return sseEvent("inference.done", {
      messageId: info.id,
      sessionId: info.sessionID,
    });
  }

  if (type === "session.idle") {
    const p = properties as { sessionID?: string };
    return sseEvent("session.idle", { sessionId: p.sessionID });
  }

  return null;
}

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

        return c.json({ queued: true });
      },
    },
    "/agents/:id/events": {
      GET: (c: Context) => {
        const id = c.req.param("id");
        const encoder = new TextEncoder();
        let cleanup: (() => void) | null = null;

        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            cleanup = opencode.subscribe(id, (raw) => {
              const line = translateEvent(raw);
              if (!line) return;
              try {
                controller.enqueue(encoder.encode(line));
              } catch {
                cleanup?.();
                cleanup = null;
              }
            });
          },
          cancel() {
            cleanup?.();
            cleanup = null;
          },
        });

        return new Response(stream, {
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
          },
        });
      },
    },
  };
}
