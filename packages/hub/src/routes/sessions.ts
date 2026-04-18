import { eq, and, desc } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import { streamSSE } from "hono/streaming";
import { agent, agentSession, provider } from "@interchange/db/schema";
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
import { requireGrant, idResource } from "../middleware/grant";
import { generateId } from "../ids";

const CredentialRequirement = type({
  providerName: "string",
  "scopes?": "string[]",
  source: "'tenant' | 'creator' | 'invoker'",
  "name?": "string",
});

const CredentialRequirements = CredentialRequirement.array();

const ProviderMetadata = type({
  baseURL: "string",
});

const ModelConfig = type({
  defaultModel: "string",
});

const AbortBody = type({
  "reason?":
    "'user_disconnect' | 'wallet_exhaustion' | 'admin_kill' | 'session_timeout' | 'credential_revocation'",
});

type SessionRow = typeof agentSession.$inferSelect;

// The DB tracks lifecycle state (active/ending/ended) while the API
// exposes runtime state (idle/busy/retry/waiting_approval) plus the
// terminal lifecycle states. For active sessions, real-time status
// comes from GET /:sessionId/status; the list/get endpoints return
// "idle" as a baseline.
function formatSession(row: SessionRow) {
  const status = row.status === "active" ? ("idle" as const) : row.status;
  return {
    id: row.id,
    tenantId: row.tenantId,
    agentId: row.agentId,
    principalId: row.principalId,
    status,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
    lastActivityAt: null,
  };
}

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
    const body = c.req.valid("json");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, body.agentId), eq(agent.tenantId, tenant.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    if (row.status !== "deployed") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: `Agent is not in a launchable state (status: ${row.status})`,
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

    const parsedRequirements = CredentialRequirements(
      row.credentialRequirements ?? [],
    );
    if (parsedRequirements instanceof type.errors) {
      return c.json(
        {
          error: {
            code: "not_launchable",
            message: `Invalid credential requirements: ${parsedRequirements.summary}`,
          },
        },
        409,
      );
    }
    const credentialRequirements = parsedRequirements;

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

      const metadata = ProviderMetadata(providerRow.metadata ?? {});
      if (metadata instanceof type.errors) {
        return c.json(
          {
            error: {
              code: "provider_misconfigured",
              message: `Provider "${providerRow.name}" metadata is invalid: ${metadata.summary}`,
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

    const modelConfig = ModelConfig(row.modelConfig ?? {});
    if (modelConfig instanceof type.errors) {
      return c.json(
        {
          error: {
            code: "not_launchable",
            message: `Agent model configuration is invalid: ${modelConfig.summary}`,
          },
        },
        409,
      );
    }

    const sessionId = generateId("session");
    const now = new Date();

    await db.insert(agentSession).values({
      id: sessionId,
      tenantId: tenant.id,
      agentId: row.id,
      principalId: principal.id,
      status: "active",
      createdAt: now,
      updatedAt: now,
    });

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
      await db
        .update(agentSession)
        .set({ status: "ended", endedAt: new Date(), updatedAt: new Date() })
        .where(eq(agentSession.id, sessionId));

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
      .set({
        status: "running",
        sessionId,
        updatedAt: new Date(),
      })
      .where(and(eq(agent.id, row.id), eq(agent.status, "deployed")));

    return c.json(
      {
        id: sessionId,
        tenantId: tenant.id,
        agentId: row.id,
        principalId: principal.id,
        status: "idle" as const,
        createdAt: now.toISOString(),
        updatedAt: now.toISOString(),
        lastActivityAt: null,
      },
      201,
    );
  },
);

app.get(
  "/",
  requireGrant("session:*", "read"),
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
  async (c) => {
    const tenant = c.get("tenant");
    const principal = c.get("principal");
    const db = c.get("db");
    const agentId = c.req.query("agentId");

    const conditions = [
      eq(agentSession.tenantId, tenant.id),
      eq(agentSession.principalId, principal.id),
    ];
    if (agentId !== undefined) {
      conditions.push(eq(agentSession.agentId, agentId));
    }

    const rows = await db.query.agentSession.findMany({
      where: and(...conditions),
      orderBy: [desc(agentSession.createdAt)],
    });

    return c.json(rows.map(formatSession), 200);
  },
);

app.get(
  "/:sessionId",
  requireGrant(idResource("session", "sessionId"), "read"),
  describeRoute({
    tags: ["Sessions"],
    summary: "Get session metadata",
    description: "Returns session status, agent, and creation time.",
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
  async (c) => {
    const tenant = c.get("tenant");
    const db = c.get("db");
    const sessionId = c.req.param("sessionId");

    const row = await db.query.agentSession.findFirst({
      where: and(
        eq(agentSession.id, sessionId),
        eq(agentSession.tenantId, tenant.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Session not found" } },
        404,
      );
    }

    return c.json(formatSession(row), 200);
  },
);

app.delete(
  "/:sessionId",
  requireGrant(idResource("session", "sessionId"), "manage"),
  describeRoute({
    tags: ["Sessions"],
    summary: "End a session",
    description: "Ends the session and tears down the sidecar process.",
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
      409: {
        description: "Session already ended or ending",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      502: {
        description: "Sidecar unavailable",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenant = c.get("tenant");
    const db = c.get("db");
    const sidecarRouter = c.get("sidecarRouter");
    const sessionId = c.req.param("sessionId");

    const sessionRow = await db.query.agentSession.findFirst({
      where: and(
        eq(agentSession.id, sessionId),
        eq(agentSession.tenantId, tenant.id),
      ),
    });

    if (!sessionRow) {
      return c.json(
        { error: { code: "not_found", message: "Session not found" } },
        404,
      );
    }

    if (sessionRow.status !== "active") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: `Session is already ${sessionRow.status}`,
          },
        },
        409,
      );
    }

    const agentRow = await db.query.agent.findFirst({
      where: and(
        eq(agent.id, sessionRow.agentId),
        eq(agent.tenantId, tenant.id),
      ),
    });

    if (!agentRow) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    const agentAddress = `${agentRow.id}@${tenant.domain}`;

    await db
      .update(agentSession)
      .set({ status: "ending", updatedAt: new Date() })
      .where(eq(agentSession.id, sessionId));

    try {
      await sidecarRouter.sendSessionDestroy(agentAddress);
    } catch (err) {
      await db
        .update(agentSession)
        .set({ status: "active", updatedAt: new Date() })
        .where(eq(agentSession.id, sessionId));

      return c.json(
        {
          error: {
            code: "sidecar_unavailable",
            message:
              err instanceof Error
                ? err.message
                : "Failed to reach sidecar for session teardown",
          },
        },
        502,
      );
    }

    await db
      .update(agent)
      .set({ status: "deployed", sessionId: null, updatedAt: new Date() })
      .where(and(eq(agent.id, agentRow.id), eq(agent.status, "running")));

    await db
      .update(agentSession)
      .set({ status: "ended", endedAt: new Date(), updatedAt: new Date() })
      .where(eq(agentSession.id, sessionId));

    return c.body(null, 204);
  },
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
  requireGrant(idResource("session", "sessionId"), "manage"),
  describeRoute({
    tags: ["Sessions"],
    summary: "Abort current operation",
    description: "Aborts the agent's current inference or tool execution.",
    responses: {
      204: {
        description: "Abort signal sent",
      },
      404: {
        description: "Session or agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      409: {
        description: "Session not active",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      502: {
        description: "Sidecar unavailable",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", AbortBody),
  async (c) => {
    const tenant = c.get("tenant");
    const db = c.get("db");
    const sidecarRouter = c.get("sidecarRouter");
    const sessionId = c.req.param("sessionId");
    const body = c.req.valid("json");

    const sessionRow = await db.query.agentSession.findFirst({
      where: and(
        eq(agentSession.id, sessionId),
        eq(agentSession.tenantId, tenant.id),
      ),
    });

    if (!sessionRow) {
      return c.json(
        { error: { code: "not_found", message: "Session not found" } },
        404,
      );
    }

    if (sessionRow.status !== "active") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: `Session is already ${sessionRow.status}`,
          },
        },
        409,
      );
    }

    const agentRow = await db.query.agent.findFirst({
      where: and(
        eq(agent.id, sessionRow.agentId),
        eq(agent.tenantId, tenant.id),
      ),
    });

    if (!agentRow) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    const agentAddress = `${agentRow.id}@${tenant.domain}`;

    try {
      await sidecarRouter.sendSessionAbort(
        agentAddress,
        body.reason ?? "user_disconnect",
      );
    } catch (err) {
      return c.json(
        {
          error: {
            code: "sidecar_unavailable",
            message:
              err instanceof Error
                ? err.message
                : "Failed to reach sidecar for abort",
          },
        },
        502,
      );
    }

    return c.body(null, 204);
  },
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
  requireGrant(idResource("session", "sessionId"), "read"),
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
      404: {
        description: "Session not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      410: {
        description: "Session ended",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenant = c.get("tenant");
    const db = c.get("db");
    const sidecarRouter = c.get("sidecarRouter");
    const sessionId = c.req.param("sessionId");

    const sessionRow = await db.query.agentSession.findFirst({
      where: and(
        eq(agentSession.id, sessionId),
        eq(agentSession.tenantId, tenant.id),
      ),
    });

    if (!sessionRow) {
      return c.json(
        { error: { code: "not_found", message: "Session not found" } },
        404,
      );
    }

    if (sessionRow.status === "ended") {
      return c.json(
        { error: { code: "gone", message: "Session has ended" } },
        410,
      );
    }

    return streamSSE(c, async (stream) => {
      const noop = () => undefined;

      const unsubscribe = sidecarRouter.subscribeSession(sessionId, (event) => {
        stream
          .writeSSE({
            event: "agent.event",
            data: JSON.stringify(event),
          })
          .catch(noop);
      });

      const keepalive = setInterval(() => {
        stream.write(": keepalive\n\n").catch(noop);
      }, 30_000);

      stream.onAbort(() => {
        clearInterval(keepalive);
        unsubscribe();
      });

      // Keep the stream open until the client disconnects.
      await new Promise<void>(noop);
    });
  },
);

export { app as sessionRoutes };
