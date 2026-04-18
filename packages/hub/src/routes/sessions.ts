import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";

import { agent, provider } from "@interchange/db/schema";
import { resolveCredentialRequirement } from "@interchange/db";
import {
  CreateSession,
  SessionResponse,
  SessionStatus,
  SendMessage,
  MessageResponse,
  ErrorResponse,
} from "@interchange/types";
import type { ProviderConfig } from "@interchange/types/runtime";

import type { TenantEnv } from "../context";
import { requireGrant } from "../middleware/grant";
import { generateId } from "../ids";

const app = new Hono<TenantEnv>();

app.post(
  "/",
  requireGrant("session:*", "create"),
  describeRoute({
    tags: ["Sessions"],
    summary: "Create a session",
    description:
      "Creates a new session with the specified agent. Returns a session ID and session token (JWT for WebSocket authentication). Invoker grants become grants with source 'invoker' scoped to the session lifetime.",
    responses: {
      201: {
        description: "Session created",
        content: {
          "application/json": { schema: resolver(SessionResponse) },
        },
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      409: {
        description: "Agent not launchable",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", CreateSession),
  async (c) => {
    const tenant = c.get("tenant");
    const principal = c.get("principal");
    const db = c.get("db");
    const sidecarRouter = c.get("sidecarRouter");
    const body = c.req.valid("json" as never) as {
      agentId: string;
      invokerCapabilities?: {
        resource: string;
        action: string;
        conditions?: Record<string, unknown> | null;
      }[];
    };

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, body.agentId), eq(agent.tenantId, tenant.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    if (row.status === "running") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: "Agent already has an active session",
          },
        },
        409,
      );
    }

    if (!row.systemPrompt) {
      return c.json(
        {
          error: {
            code: "not_launchable",
            message:
              "Agent cannot be launched without a system prompt configured",
          },
        },
        409,
      );
    }

    const agentAddress = `${row.id}@${tenant.domain}`;

    const credentialRequirements = (row.credentialRequirements ?? []) as {
      providerName: string;
      scopes?: string[];
      source: "tenant" | "creator" | "invoker";
      name?: string;
    }[];

    const providers: ProviderConfig[] = [];
    for (const req of credentialRequirements) {
      let resolved;
      try {
        resolved = await resolveCredentialRequirement(
          db,
          tenant.id,
          req,
          row.principalId,
          principal.id,
        );
      } catch (err) {
        return c.json(
          {
            error: {
              code: "credential_error",
              message:
                err instanceof Error
                  ? err.message
                  : "Credential resolution failed",
            },
          },
          409,
        );
      }

      if (!resolved) {
        return c.json(
          {
            error: {
              code: "credential_missing",
              message: `No credential found for provider "${req.providerName}" (source: ${req.source})`,
            },
          },
          409,
        );
      }

      const providerRow = await db.query.provider.findFirst({
        where: eq(provider.id, resolved.providerId),
      });

      if (!providerRow) {
        return c.json(
          {
            error: {
              code: "provider_missing",
              message: `Provider not found for credential "${resolved.id}"`,
            },
          },
          409,
        );
      }

      const metadata = (providerRow.metadata ?? {}) as {
        baseURL?: string;
      };

      if (!metadata.baseURL) {
        return c.json(
          {
            error: {
              code: "provider_misconfigured",
              message: `Provider "${providerRow.name}" has no baseURL configured`,
            },
          },
          409,
        );
      }

      providers.push({
        provider: providerRow.plugin,
        baseURL: metadata.baseURL,
        apiKey: resolved.secret,
      });
    }

    const modelConfig = (row.modelConfig ?? {}) as {
      defaultModel?: string;
    };

    if (!modelConfig.defaultModel) {
      return c.json(
        {
          error: {
            code: "not_launchable",
            message: "Agent has no default model configured",
          },
        },
        409,
      );
    }

    const sessionId = generateId("session");
    const now = new Date().toISOString();

    try {
      await sidecarRouter.sendSessionCreate(agentAddress, {
        sessionId,
        agentId: row.id,
        tenantId: tenant.id,
        agentAddress,
        systemPrompt: row.systemPrompt,
        tools: [],
        toolPolicy: [],
        providers,
        defaultModel: modelConfig.defaultModel,
      });
    } catch (err) {
      return c.json(
        {
          error: {
            code: "sidecar_unavailable",
            message:
              err instanceof Error
                ? err.message
                : "Failed to dispatch session to sidecar",
          },
        },
        502,
      );
    }

    await db
      .update(agent)
      .set({ status: "running", updatedAt: new Date() })
      .where(and(eq(agent.id, row.id), eq(agent.status, "deployed")));

    return c.json(
      {
        id: sessionId,
        tenantId: tenant.id,
        agentId: row.id,
        principalId: principal.id,
        status: "idle" as const,
        createdAt: now,
        updatedAt: now,
      },
      201,
    );
  },
);

app.get(
  "/",
  describeRoute({
    tags: ["Sessions"],
    summary: "List sessions in the tenant",
    description:
      "Lists the caller's sessions within this tenant. Filterable by agentId.",
    parameters: [{ name: "agentId", in: "query", schema: { type: "string" } }],
    responses: {
      200: {
        description: "List of sessions",
        content: {
          "application/json": {
            schema: resolver(SessionResponse.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:sessionId",
  describeRoute({
    tags: ["Sessions"],
    summary: "Get session metadata",
    description:
      "Returns session status, agent, creation time, and last activity.",
    responses: {
      200: {
        description: "Session details",
        content: {
          "application/json": { schema: resolver(SessionResponse) },
        },
      },
      404: {
        description: "Session not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.delete(
  "/:sessionId",
  describeRoute({
    tags: ["Sessions"],
    summary: "End a session",
    description:
      "Ends the session and expires all invoker grants associated with it.",
    responses: {
      204: {
        description: "Session ended",
      },
      404: {
        description: "Session not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.post(
  "/:sessionId/messages",
  describeRoute({
    tags: ["Sessions"],
    summary: "Send a message to the agent",
    description:
      "Persists the user message and returns it. The agent's response streams over the session channel (WebSocket or SSE), not in this HTTP response.",
    responses: {
      201: {
        description: "Message sent",
        content: {
          "application/json": { schema: resolver(MessageResponse) },
        },
      },
      400: {
        description: "Validation error",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", SendMessage),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:sessionId/messages",
  describeRoute({
    tags: ["Sessions"],
    summary: "List messages in a session",
    description:
      "Returns messages with all parts (text, reasoning, tool calls, etc.). Cursor-paginated.",
    responses: {
      200: {
        description: "List of messages",
        content: {
          "application/json": {
            schema: resolver(MessageResponse.array()),
          },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:sessionId/messages/:messageId",
  describeRoute({
    tags: ["Sessions"],
    summary: "Get a single message",
    description: "Returns a message with all its parts.",
    responses: {
      200: {
        description: "Message details",
        content: {
          "application/json": { schema: resolver(MessageResponse) },
        },
      },
      404: {
        description: "Message not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.post(
  "/:sessionId/abort",
  describeRoute({
    tags: ["Sessions"],
    summary: "Abort current operation",
    description: "Aborts the agent's current inference or tool execution.",
    responses: {
      204: {
        description: "Abort signal sent",
      },
      404: {
        description: "Session not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:sessionId/status",
  describeRoute({
    tags: ["Sessions"],
    summary: "Get session status",
    description:
      "Returns the current session status: idle, busy, retry (with attempt info), or waiting_approval.",
    responses: {
      200: {
        description: "Session status",
        content: {
          "application/json": { schema: resolver(SessionStatus) },
        },
      },
      404: {
        description: "Session not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

app.get(
  "/:sessionId/events",
  describeRoute({
    tags: ["Sessions"],
    summary: "SSE event stream (fallback)",
    description:
      "Server-Sent Events stream for clients that cannot use WebSocket. Same event types as the WebSocket session channel. Server-to-client only; use POST .../messages for client-to-server.",
    responses: {
      200: {
        description: "SSE event stream",
        content: {
          "text/event-stream": {},
        },
      },
    },
  }),
  (c) =>
    c.json(
      { error: { code: "not_implemented", message: "Not implemented" } },
      501,
    ),
);

export { app as sessionRoutes };
