import type { Context } from "hono";
import { OpenCodeManager } from "../opencode";
import { getLogger } from "@interchange/log";

const logger = getLogger(["sidecar", "routes", "agent"]);

function sseEvent(type: string, data: Record<string, unknown>): string {
  return `data: ${JSON.stringify({ type, data })}\n\n`;
}

function translateEvent(
  raw: string,
  doneSent?: Set<string>,
  partTypes?: Map<string, string>,
): string | null {
  let event: { type: string; properties: Record<string, unknown> };
  try {
    event = JSON.parse(raw) as typeof event;
  } catch {
    return null;
  }

  const { type, properties } = event;

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
    if (!part || !part.id) return null;
    // Record the part type so delta events can be filtered by part type.
    if (partTypes && part.type) {
      partTypes.set(part.id, part.type);
    }
    if (part.type !== "text") return null;
    // Only emit part_start on first creation (text is empty string)
    if (part.text !== "") return null;
    return sseEvent("inference.part_start", {
      partId: part.id,
      sessionId: part.sessionID,
      messageId: part.messageID,
    });
  }

  if (type === "message.part.delta") {
    const p = properties as {
      field?: string;
      delta?: string;
      partID?: string;
      sessionID?: string;
      messageID?: string;
    };
    if (p.field !== "text") return null;
    // Skip deltas for non-text parts (e.g. reasoning). We know the part type
    // because message.part.updated always precedes message.part.delta.
    if (partTypes && p.partID && partTypes.get(p.partID) !== "text")
      return null;
    return sseEvent("inference.token", {
      delta: p.delta ?? "",
      partId: p.partID,
      sessionId: p.sessionID,
      messageId: p.messageID,
    });
  }

  if (type === "message.updated") {
    const info = (
      properties as {
        info?: { id?: string; sessionID?: string; finish?: string };
      }
    ).info;
    if (!info || info.finish !== "stop") return null;
    // Deduplicate: OpenCode emits message.updated(finish=stop) more than once.
    if (doneSent && info.id) {
      if (doneSent.has(info.id)) return null;
      doneSent.add(info.id);
    }
    return sseEvent("inference.done", {
      messageId: info.id,
      sessionId: info.sessionID,
    });
  }

  if (type === "session.idle") {
    const p = properties as { sessionID?: string };
    return sseEvent("session.idle", { sessionId: p.sessionID });
  }

  if (type === "history.message") {
    const p = properties as {
      role?: string;
      text?: string;
      sessionID?: string;
    };
    if (!p.role || !p.text) return null;
    return sseEvent("history.message", {
      role: p.role,
      text: p.text,
      sessionId: p.sessionID,
    });
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
        // Track message IDs for which inference.done has been emitted.
        // OpenCode sends message.updated(finish=stop) more than once per
        // message; deduplicate so the client sees exactly one inference.done.
        const doneSent = new Set<string>();
        // Track part types (text vs reasoning etc) so delta events from
        // non-text parts (e.g. reasoning) are not forwarded to the client.
        const partTypes = new Map<string, string>();

        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            cleanup = opencode.subscribe(id, (raw) => {
              const line = translateEvent(raw, doneSent, partTypes);
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
    "/agents/:id/stream": {
      GET: async (c: Context) => {
        const id = c.req.param("id");
        const message = c.req.query("message");

        if (!message) {
          return c.json({ error: "message query parameter required" }, 400);
        }

        // Track if we've received any messages
        let gotMessage = false;

        const stream = new ReadableStream({
          start(controller) {
            const encoder = new TextEncoder();

            const sendEvent = (event: string, data: unknown) => {
              // Check if controller is still valid
              try {
                const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
                controller.enqueue(encoder.encode(msg));
              } catch {
                // Controller closed, ignore
              }
            };

            sendEvent("start", {});

            // Try streaming, but fallback after timeout
            const timeout = setTimeout(() => {
              if (!gotMessage) {
                // No LLM or timeout - send fallback response
                sendEvent("message", {
                  type: "message",
                  text: `I received your message: "${message}". Note: No LLM is currently configured for this agent.`,
                });
                sendEvent("done", {});
                try {
                  controller.close();
                } catch {
                  // Already closed
                }
              }
            }, 5000);

            opencode
              .sendMessageStream(id, message, (event, data) => {
                if (event === "message" || event === "content") {
                  gotMessage = true;
                  clearTimeout(timeout);
                }
                sendEvent(event, data);
              })
              .catch((error) => {
                logger.error(`Stream error: ${error}`);
                clearTimeout(timeout);
                if (!gotMessage) {
                  sendEvent("message", {
                    type: "message",
                    text: `Error: ${error}`,
                  });
                }
                sendEvent("done", {});
                try {
                  controller.close();
                } catch {
                  // Already closed
                }
              });
          },
        });

        return new Response(stream, {
          headers: {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            Connection: "keep-alive",
          },
        });
      },
    },
  };
}
