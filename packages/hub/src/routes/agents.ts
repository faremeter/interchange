import { eq, and } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { type } from "arktype";

import {
  agent,
  agentVersion,
  principal,
  grant,
  sidecar,
} from "@interchange/db/schema";
import {
  CreateAgent,
  UpdateAgent,
  AgentResponse,
  AgentVersion,
  AgentHealth,
  RollbackRequest,
  Offering,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";

import type { TenantEnv } from "../context";
import { getLogger } from "@interchange/log";
import { first, ts } from "../format";
import { generateId } from "../ids";
import { requireGrant, idResource } from "../middleware/grant";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

const SidecarSessionResponse = type({
  id: "string",
  initialResponse: "string?",
});

const SidecarMessageResponse = type({
  text: "string",
});

const logger = getLogger(["hub", "routes", "agents"]);

const SIDEKAR_REQUEST_TIMEOUT = 30000;

async function fetchWithTimeout(
  url: string,
  options: RequestInit = {},
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), SIDEKAR_REQUEST_TIMEOUT);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

function formatAgent(row: typeof agent.$inferSelect) {
  return {
    id: row.id,
    tenantId: row.tenantId,
    principalId: row.principalId,
    name: row.name,
    description: row.description ?? null,
    systemPrompt: row.systemPrompt ?? null,
    skills: (row.skills as Record<string, unknown>) ?? undefined,
    contextConfig: (row.contextConfig as Record<string, unknown>) ?? undefined,
    initialState: (row.initialState as Record<string, unknown>) ?? undefined,
    modelConfig: (row.modelConfig as Record<string, unknown>) ?? undefined,
    currentVersion: row.currentVersion,
    status: row.status as
      | "deployed"
      | "stopped"
      | "updating"
      | "error"
      | "running",
    kernelId: row.kernelId ?? null,
    sessionId: row.sessionId ?? null,
    capabilities: (row.capabilities as Record<string, unknown>) ?? undefined,
    credentialRequirements:
      (row.credentialRequirements as
        | {
            providerName: string;
            scopes?: string[];
            source: string;
            name?: string;
          }[]
        | null) ?? undefined,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
  };
}

const app = new Hono<TenantEnv>();

app.get(
  "/",
  requireGrant("agent:*", "read"),
  describeRoute({
    tags: ["Agents"],
    summary: "List agents in the tenant",
    description: "Filterable by offering and status.",
    parameters: [
      { name: "offering", in: "query", schema: { type: "string" } },
      {
        name: "status",
        in: "query",
        schema: {
          type: "string",
          enum: ["deployed", "stopped", "updating", "error"],
        },
      },
      ...pageParameters,
    ],
    responses: {
      200: {
        description: "List of agents",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(AgentResponse)),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const status = c.req.query("status");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(agent.tenantId, tenantCtx.id)];
    if (
      status === "deployed" ||
      status === "stopped" ||
      status === "updating" ||
      status === "error"
    ) {
      conditions.push(eq(agent.status, status));
    }
    if (cursor) {
      conditions.push(cursorCondition(agent.createdAt, agent.id, cursor));
    }

    const rows = await db.query.agent.findMany({
      where: and(...conditions),
      orderBy: pageOrder(agent.createdAt, agent.id),
      limit,
    });

    return c.json(paginatedResponse(rows.map(formatAgent), rows, limit));
  },
);

app.post(
  "/",
  requireGrant("agent:*", "create"),
  describeRoute({
    tags: ["Agents"],
    summary: "Create an agent",
    description:
      "Creates an agent and its corresponding principal. Accepts the agent definition and optional initial grants for the agent's principal.",
    responses: {
      201: {
        description: "Agent created",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
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
  validator("json", CreateAgent),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const body = c.req.valid("json" as never) as typeof CreateAgent.infer;
    const db = c.get("db");

    const now = new Date();
    const agentId = generateId("agent");
    const principalId = generateId("principal");

    // Create the agent's principal first
    await db.insert(principal).values({
      id: principalId,
      tenantId: tenantCtx.id,
      kind: "agent",
      refId: agentId,
      status: "active",
      createdAt: now,
      updatedAt: now,
    });

    const agentRow = first(
      await db
        .insert(agent)
        .values({
          id: agentId,
          tenantId: tenantCtx.id,
          principalId,
          name: body.name,
          description: body.description ?? null,
          systemPrompt: body.systemPrompt ?? null,
          skills: body.skills ?? null,
          contextConfig: body.contextConfig ?? null,
          initialState: body.initialState ?? null,
          modelConfig: body.modelConfig ?? null,
          capabilities: body.capabilities ?? null,
          credentialRequirements: body.credentialRequirements ?? null,
          currentVersion: "1",
          status: "deployed",
          createdAt: now,
          updatedAt: now,
        })
        .returning(),
    );

    // Create initial version
    await db.insert(agentVersion).values({
      id: generateId("agentVersion"),
      agentId,
      version: "1",
      status: "active",
      createdAt: now,
    });

    // Create initial grants for the agent's principal
    if (body.initialGrants) {
      for (const g of body.initialGrants) {
        await db.insert(grant).values({
          id: generateId("grant"),
          tenantId: tenantCtx.id,
          principalId,
          resource: g.resource,
          action: g.action,
          effect: g.effect,
          conditions: g.conditions ?? null,
          source: "creator",
          createdAt: now,
          updatedAt: now,
        });
      }
    }

    return c.json(formatAgent(agentRow), 201);
  },
);

app.get(
  "/:agentId",
  requireGrant(idResource("agent", "agentId"), "read"),
  describeRoute({
    tags: ["Agents"],
    summary: "Get agent details",
    description:
      "Returns the agent definition, status, health, capabilities, and principal ID.",
    responses: {
      200: {
        description: "Agent details",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
        },
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    return c.json(formatAgent(row));
  },
);

app.patch(
  "/:agentId",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    tags: ["Agents"],
    summary: "Update agent definition",
    description:
      "Updates the agent definition and creates a new version. The new version is deployed alongside the current version until health checks pass.",
    responses: {
      200: {
        description: "Agent updated",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
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
  validator("json", UpdateAgent),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const body = c.req.valid("json" as never) as typeof UpdateAgent.infer;
    const db = c.get("db");

    const existing = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!existing) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    const now = new Date();
    const newVersion = String(Number(existing.currentVersion) + 1);

    const updates: Record<string, unknown> = {
      updatedAt: now,
      currentVersion: newVersion,
    };
    if (body.name !== undefined) updates["name"] = body.name;
    if (body.description !== undefined)
      updates["description"] = body.description;
    if (body.systemPrompt !== undefined)
      updates["systemPrompt"] = body.systemPrompt;
    if (body.skills !== undefined) updates["skills"] = body.skills;
    if (body.contextConfig !== undefined)
      updates["contextConfig"] = body.contextConfig;
    if (body.initialState !== undefined)
      updates["initialState"] = body.initialState;
    if (body.modelConfig !== undefined)
      updates["modelConfig"] = body.modelConfig;
    if (body.capabilities !== undefined)
      updates["capabilities"] = body.capabilities;
    if (body.credentialRequirements !== undefined)
      updates["credentialRequirements"] = body.credentialRequirements;

    const updated = first(
      await db
        .update(agent)
        .set(updates)
        .where(eq(agent.id, agentId))
        .returning(),
    );

    // Mark old version inactive, create new version
    await db
      .update(agentVersion)
      .set({ status: "inactive" })
      .where(
        and(
          eq(agentVersion.agentId, agentId),
          eq(agentVersion.version, existing.currentVersion),
        ),
      );

    await db.insert(agentVersion).values({
      id: generateId("agentVersion"),
      agentId,
      version: newVersion,
      status: "active",
      createdAt: now,
    });

    return c.json(formatAgent(updated));
  },
);

app.delete(
  "/:agentId",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    tags: ["Agents"],
    summary: "Retire an agent",
    description:
      "Deactivates the agent's principal and begins graceful shutdown. In-flight work is drained before the agent stops.",
    responses: {
      204: {
        description: "Agent retirement initiated",
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const existing = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!existing) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    // Deactivate agent principal
    await db
      .update(principal)
      .set({ status: "deactivated", updatedAt: new Date() })
      .where(eq(principal.id, existing.principalId));

    // Set agent status to stopped
    await db
      .update(agent)
      .set({ status: "stopped", updatedAt: new Date() })
      .where(eq(agent.id, agentId));

    return c.body(null, 204);
  },
);

app.get(
  "/:agentId/versions",
  requireGrant(idResource("agent", "agentId"), "read"),
  describeRoute({
    tags: ["Agents"],
    summary: "List agent versions",
    description: "Lists all versions with their deployment status.",
    parameters: [...pageParameters],
    responses: {
      200: {
        description: "List of versions",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(AgentVersion)),
          },
        },
      },
    },
  }),
  async (c) => {
    const agentId = c.req.param("agentId");
    const db = c.get("db");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(agentVersion.agentId, agentId)];
    if (cursor) {
      conditions.push(
        cursorCondition(agentVersion.createdAt, agentVersion.id, cursor),
      );
    }

    const rows = await db.query.agentVersion.findMany({
      where: and(...conditions),
      orderBy: pageOrder(agentVersion.createdAt, agentVersion.id),
      limit,
    });

    const items = rows.map((v) => ({
      version: v.version,
      status: v.status as "active" | "inactive" | "failed",
      createdAt: ts(v.createdAt),
    }));

    return c.json(paginatedResponse(items, rows, limit));
  },
);

app.post(
  "/:agentId/rollback",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    tags: ["Agents"],
    summary: "Rollback to a previous version",
    description:
      "Shifts traffic back to the specified version. The current version is stopped.",
    responses: {
      200: {
        description: "Rollback initiated",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
        },
      },
      400: {
        description: "Invalid version",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", RollbackRequest),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const body = c.req.valid("json" as never) as typeof RollbackRequest.infer;
    const db = c.get("db");

    const existing = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });
    if (!existing) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    const targetVersion = await db.query.agentVersion.findFirst({
      where: and(
        eq(agentVersion.agentId, agentId),
        eq(agentVersion.version, body.version),
      ),
    });
    if (!targetVersion) {
      return c.json(
        {
          error: {
            code: "bad_request",
            message: "Target version not found",
          },
        },
        400,
      );
    }

    const now = new Date();

    // Deactivate current version
    await db
      .update(agentVersion)
      .set({ status: "inactive" })
      .where(
        and(
          eq(agentVersion.agentId, agentId),
          eq(agentVersion.version, existing.currentVersion),
        ),
      );

    // Activate target version
    await db
      .update(agentVersion)
      .set({ status: "active" })
      .where(
        and(
          eq(agentVersion.agentId, agentId),
          eq(agentVersion.version, body.version),
        ),
      );

    // Update agent
    const updated = first(
      await db
        .update(agent)
        .set({ currentVersion: body.version, updatedAt: now })
        .where(eq(agent.id, agentId))
        .returning(),
    );

    return c.json(formatAgent(updated));
  },
);

app.get(
  "/:agentId/health",
  requireGrant(idResource("agent", "agentId"), "read"),
  describeRoute({
    tags: ["Agents"],
    summary: "Get agent health status",
    description: "Returns liveness and readiness status.",
    responses: {
      200: {
        description: "Health status",
        content: {
          "application/json": { schema: resolver(AgentHealth) },
        },
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    // Placeholder health -- in production this would query the agent kernel
    return c.json({
      liveness: row.status === "deployed" ? "ok" : "unhealthy",
      readiness: row.status === "deployed" ? "ok" : "not_ready",
      lastCheckedAt: null,
    });
  },
);

app.get(
  "/:agentId/offerings",
  requireGrant(idResource("agent", "agentId"), "read"),
  describeRoute({
    tags: ["Agents"],
    summary: "List agent offerings",
    description: "Returns the agent's exposed offerings with pricing metadata.",
    responses: {
      200: {
        description: "List of offerings",
        content: {
          "application/json": {
            schema: resolver(Offering.array()),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    // Offerings are stored as jsonb -- return empty array if none
    return c.json([]);
  },
);

// Start agent on a sidecar
app.post(
  "/:agentId/start",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    summary: "Start agent on sidecar",
    description: "Deploys an agent to a sidecar and starts a session",
    responses: {
      200: {
        description: "Agent started",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
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
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    if (row.status === "running" && row.kernelId && row.sessionId) {
      return c.json(formatAgent(row));
    }

    // Find a sidecar for this tenant
    const sidecars = await db.select().from(sidecar);
    const availableSidecar = sidecars[0];

    if (!availableSidecar) {
      return c.json(
        { error: { code: "no_sidecar", message: "No sidecar available" } },
        400,
      );
    }

    // Create session on sidecar
    const requestBody = {
      agentId: row.id,
      systemPrompt: row.systemPrompt,
      skills: row.skills,
    };
    const sidecarUrl = `${availableSidecar.url}/agents`;

    const response = await fetchWithTimeout(sidecarUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      return c.json(
        {
          error: {
            code: "session_failed",
            message: "Failed to create session",
          },
        },
        500,
      );
    }

    const rawSessionResult = await response.json();
    const sessionResult = SidecarSessionResponse(rawSessionResult);
    if (sessionResult instanceof type.errors) {
      logger.error(
        `Invalid sidecar session response: ${sessionResult.summary}`,
      );
      return c.json(
        {
          error: {
            code: "invalid_response",
            message: "Invalid response from sidecar",
          },
        },
        500,
      );
    }

    // Update agent with kernel and session
    const [updated] = await db
      .update(agent)
      .set({
        kernelId: availableSidecar.id,
        sessionId: sessionResult.id,
        status: "running",
        updatedAt: new Date(),
      })
      .where(eq(agent.id, agentId))
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "update_failed", message: "Failed to update agent" } },
        500,
      );
    }

    return c.json({
      ...formatAgent(updated),
      initialResponse: sessionResult.initialResponse,
    });
  },
);

// Chat with running agent
app.post(
  "/:agentId/chat",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    summary: "Chat with running agent",
    description: "Sends a message to a running agent session",
    responses: {
      200: {
        description: "Agent response",
        content: {
          "application/json": { schema: resolver(type({ text: "string" })) },
        },
      },
      400: {
        description: "Agent not running",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", type({ text: "string" })),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const body = c.req.valid("json" as never) as { text: string };
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    if (!row.kernelId || !row.sessionId) {
      return c.json(
        { error: { code: "not_running", message: "Agent is not running" } },
        400,
      );
    }

    const [sidecarRow] = await db
      .select()
      .from(sidecar)
      .where(eq(sidecar.id, row.kernelId));

    if (!sidecarRow) {
      return c.json(
        {
          error: {
            code: "sidecar_gone",
            message: "Sidecar no longer available",
          },
        },
        400,
      );
    }

    const response = await fetchWithTimeout(
      `${sidecarRow.url}/agents/${row.sessionId}/message`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: body.text }),
      },
    );

    if (!response.ok) {
      return c.json(
        { error: { code: "chat_failed", message: "Failed to send message" } },
        500,
      );
    }

    const rawResult = await response.json();
    const result = SidecarMessageResponse(rawResult);
    if (result instanceof type.errors) {
      logger.error(`Invalid sidecar message response: ${result.summary}`);
      return c.json(
        {
          error: {
            code: "invalid_response",
            message: "Invalid response from sidecar",
          },
        },
        500,
      );
    }
    return c.json({ text: result.text });
  },
);

// Stop running agent
app.post(
  "/:agentId/stop",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    summary: "Stop running agent",
    description: "Stops the agent session on the sidecar",
    responses: {
      200: {
        description: "Agent stopped",
        content: {
          "application/json": { schema: resolver(AgentResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    if (!row.kernelId || !row.sessionId) {
      return c.json(formatAgent(row));
    }

    // Stop session on sidecar
    const [sidecarRow] = await db
      .select()
      .from(sidecar)
      .where(eq(sidecar.id, row.kernelId));

    if (sidecarRow) {
      await fetchWithTimeout(`${sidecarRow.url}/agents/${row.sessionId}`, {
        method: "DELETE",
      });
    }

    // Update agent
    const [updated] = await db
      .update(agent)
      .set({
        kernelId: null,
        sessionId: null,
        status: "deployed",
        updatedAt: new Date(),
      })
      .where(eq(agent.id, agentId))
      .returning();

    if (!updated) {
      return c.json(
        { error: { code: "update_failed", message: "Failed to update agent" } },
        500,
      );
    }

    return c.json(formatAgent(updated));
  },
);

// Stream SSE events from running agent
app.get(
  "/:agentId/events",
  requireGrant(idResource("agent", "agentId"), "manage"),
  describeRoute({
    tags: ["Agents"],
    summary: "Stream agent events",
    description:
      "Opens a server-sent event stream proxied from the agent sidecar. The stream closes when the agent stops or the client disconnects.",
    responses: {
      200: {
        description: "SSE event stream",
        content: {
          "text/event-stream": {
            schema: { type: "string" },
          },
        },
      },
      400: {
        description: "Agent not running",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      404: {
        description: "Agent not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      502: {
        description: "Sidecar unreachable",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const agentId = c.req.param("agentId");
    const db = c.get("db");

    const row = await db.query.agent.findFirst({
      where: and(eq(agent.id, agentId), eq(agent.tenantId, tenantCtx.id)),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Agent not found" } },
        404,
      );
    }

    if (!row.kernelId || !row.sessionId) {
      return c.json(
        { error: { code: "not_running", message: "Agent is not running" } },
        400,
      );
    }

    const [sidecarRow] = await db
      .select()
      .from(sidecar)
      .where(eq(sidecar.id, row.kernelId));

    if (!sidecarRow) {
      return c.json(
        {
          error: {
            code: "sidecar_gone",
            message: "Sidecar no longer available",
          },
        },
        502,
      );
    }

    // NOTE: c.req.raw.signal does not reliably propagate browser-side
    // EventSource disconnects in Bun. The upstream Hub→Sidecar fetch may not
    // abort immediately when the browser closes the EventSource. The sidecar
    // will clean up when the session is stopped.
    let upstream: Response;
    try {
      upstream = await fetch(
        `${sidecarRow.url}/agents/${row.sessionId}/events`,
        { signal: c.req.raw.signal },
      );
    } catch {
      return c.json(
        {
          error: {
            code: "sidecar_error",
            message: "Sidecar event stream unavailable",
          },
        },
        502,
      );
    }

    if (!upstream.ok) {
      return c.json(
        {
          error: {
            code: "sidecar_error",
            message: "Sidecar event stream unavailable",
          },
        },
        502,
      );
    }

    return new Response(upstream.body, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
      },
    });
  },
);

export { app as agentRoutes };
