import { eq, and, inArray, asc, gt, or } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { streamSSE } from "hono/streaming";
import { type } from "arktype";

import {
  agent,
  agentInstance,
  agentSession,
  messagePart,
  provider,
  sessionMessage,
} from "@interchange/db/schema";
import { resolveCredentialRequirement } from "@interchange/db";
import { evaluateGrants } from "@interchange/authz";

import { generateKeyPair, createNodeCrypto } from "@interchange/crypto-node";
import {
  CreateAgentInstance,
  AgentInstanceResponse,
  SendMessage,
  MessageResponse,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";
import type {
  CryptoProvider,
  ProviderConfig,
} from "@interchange/types/runtime";
import { SessionLaunchError } from "../session-service";

import type { TenantEnv } from "../context";
import { requireGrant, idResource } from "../middleware/grant";
import { generateId } from "../ids";
import { first, ts } from "../format";
import {
  parsePageParams,
  cursorCondition,
  pageOrder,
  paginatedResponse,
  pageParameters,
} from "../pagination";

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

function formatInstance(row: typeof agentInstance.$inferSelect) {
  return {
    id: row.id,
    agentId: row.agentId,
    tenantId: row.tenantId,
    address: row.address,
    status: row.status,
    publicKey: row.publicKey ?? null,
    kernelId: row.kernelId ?? null,
    sidecarId: row.sidecarId ?? null,
    createdAt: ts(row.createdAt),
    updatedAt: ts(row.updatedAt),
    endedAt: row.endedAt ? ts(row.endedAt) : null,
  };
}

const app = new Hono<TenantEnv>();

app.post(
  "/",
  requireGrant("instance:*", "create"),
  describeRoute({
    tags: ["Instances"],
    summary: "Deploy an agent instance",
    description:
      "Creates a new running instance of the specified agent definition. Resolves credentials, provisions the agent on a sidecar, and starts the session.",
    responses: {
      201: {
        description: "Instance deployed",
        content: {
          "application/json": { schema: resolver(AgentInstanceResponse) },
        },
      },
      404: {
        description: "Agent definition not found",
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
      502: {
        description: "Sidecar unavailable",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  validator("json", CreateAgentInstance),
  async (c) => {
    const tenant = c.get("tenant");
    const principal = c.get("principal");
    const db = c.get("db");
    const sessionService = c.get("sessionService");
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

    const grantStore = c.get("grantStore");
    const agentGrants = await grantStore.collectGrants(
      row.principalId,
      tenant.id,
    );
    const nonInvokerGrants = agentGrants.filter((g) => g.source !== "invoker");
    const invokerRequirements = agentGrants.filter(
      (g) => g.source === "invoker",
    );

    if (invokerRequirements.length > 0) {
      // Collect the invoking user's grants once (not per-requirement).
      const invokerGrants = await grantStore.collectGrants(
        principal.id,
        tenant.id,
      );
      // Only system/role/creator grants can be delegated. Invoker-sourced
      // grants cannot be transitively re-delegated — this prevents
      // privilege escalation through delegation chains.
      const delegatable = invokerGrants.filter((g) => g.source !== "invoker");

      for (const req of invokerRequirements) {
        const result = await evaluateGrants(
          delegatable,
          req.resource,
          req.action,
        );
        if (result.effect !== "allow") {
          return c.json(
            {
              error: {
                code: "insufficient_grants",
                message: `Invoker lacks authority for ${req.resource}/${req.action}`,
              },
            },
            403,
          );
        }
      }
    }

    // Invoker-sourced grants are ephemeral — they expire when the instance
    // is torn down via agent.undeploy. This relies on the single-instance
    // invariant (at most one running instance per agent) enforced below.
    // TODO: record invokerPrincipalId for audit trail (INTR-21 follow-up)
    const grants = [...nonInvokerGrants, ...invokerRequirements];

    const sessionId = generateId("session");
    const instanceId = generateId("instance");
    const now = new Date();

    const existing = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.address, agentAddress),
        inArray(agentInstance.status, ["running", "updating"]),
      ),
    });
    if (existing) {
      return c.json(
        {
          error: {
            code: "conflict",
            message: `Agent already has an active instance (${existing.id})`,
          },
        },
        409,
      );
    }

    // Create a transitional agentSession row to satisfy the FK on
    // agentInstance.sessionId and the message tables. This coupling
    // is removed when sessions are fully retired.
    await db.insert(agentSession).values({
      id: sessionId,
      tenantId: tenant.id,
      agentId: row.id,
      principalId: principal.id,
      status: "active",
      createdAt: now,
      updatedAt: now,
    });

    await db
      .delete(agentInstance)
      .where(
        and(
          eq(agentInstance.address, agentAddress),
          inArray(agentInstance.status, ["stopped", "error", "deployed"]),
        ),
      );

    await db.insert(agentInstance).values({
      id: instanceId,
      agentId: row.id,
      tenantId: tenant.id,
      principalId: row.principalId,
      address: agentAddress,
      sessionId,
      status: "deployed",
      createdAt: now,
      updatedAt: now,
    });

    const eventCollectors = c.get("eventCollectors");
    eventCollectors.create(agentAddress, tenant.id, sessionId, instanceId);

    const skills = (row.skills ?? []) as {
      name: string;
      definition: Record<string, unknown>;
    }[];

    try {
      await sessionService.launchSession({
        agentAddress,
        agentId: row.id,
        config: {
          sessionId,
          agentId: row.id,
          tenantId: tenant.id,
          principalId: row.principalId,
          agentAddress,
          systemPrompt: row.systemPrompt,
          tools: [],
          grants,
          providers,
          defaultModel: modelConfig.defaultModel,
        },
        deployContent: {
          systemPrompt: row.systemPrompt,
          skills,
        },
      });
    } catch (err) {
      eventCollectors.abandon(agentAddress);

      const failedAt = new Date();

      await db
        .update(agentSession)
        .set({ status: "ended", endedAt: failedAt, updatedAt: failedAt })
        .where(eq(agentSession.id, sessionId));

      const leaked = err instanceof SessionLaunchError && err.leakedAgent;

      if (leaked) {
        await db
          .update(agentInstance)
          .set({ status: "error", updatedAt: failedAt })
          .where(eq(agentInstance.id, instanceId));
      } else {
        await db.delete(agentInstance).where(eq(agentInstance.id, instanceId));
      }

      return c.json(
        {
          error: {
            code: "sidecar_unavailable",
            message:
              err instanceof Error
                ? err.message
                : "Failed to dispatch agent to sidecar",
          },
        },
        502,
      );
    }

    const launchedAt = new Date();

    const launched = first(
      await db
        .update(agentInstance)
        .set({ status: "running", updatedAt: launchedAt })
        .where(eq(agentInstance.id, instanceId))
        .returning(),
    );

    return c.json(formatInstance(launched), 201);
  },
);

app.get(
  "/",
  requireGrant("instance:*", "read"),
  describeRoute({
    tags: ["Instances"],
    summary: "List agent instances",
    description:
      "Lists agent instances in the tenant. Filterable by agentId and status.",
    parameters: [
      { name: "agentId", in: "query", schema: { type: "string" } },
      {
        name: "status",
        in: "query",
        schema: {
          type: "string",
          enum: ["deployed", "running", "updating", "error", "stopped"],
        },
      },
      ...pageParameters,
    ],
    responses: {
      200: {
        description: "List of instances",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(AgentInstanceResponse)),
          },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const agentId = c.req.query("agentId");
    const status = c.req.query("status");
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

    const conditions = [eq(agentInstance.tenantId, tenantCtx.id)];
    if (agentId !== undefined) {
      conditions.push(eq(agentInstance.agentId, agentId));
    }
    if (
      status === "deployed" ||
      status === "running" ||
      status === "updating" ||
      status === "error" ||
      status === "stopped"
    ) {
      conditions.push(eq(agentInstance.status, status));
    }
    if (cursor) {
      conditions.push(
        cursorCondition(agentInstance.createdAt, agentInstance.id, cursor),
      );
    }

    const rows = await db.query.agentInstance.findMany({
      where: and(...conditions),
      orderBy: pageOrder(agentInstance.createdAt, agentInstance.id),
      limit,
    });

    return c.json(
      paginatedResponse(
        rows.map((r) => formatInstance(r)),
        rows,
        limit,
      ),
    );
  },
);

app.get(
  "/:instanceId",
  requireGrant(idResource("instance", "instanceId"), "read"),
  describeRoute({
    tags: ["Instances"],
    summary: "Get instance detail",
    description:
      "Returns instance runtime state including status, public key, and sidecar assignment.",
    responses: {
      200: {
        description: "Instance detail",
        content: {
          "application/json": { schema: resolver(AgentInstanceResponse) },
        },
      },
      404: {
        description: "Instance not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const eventCollectors = c.get("eventCollectors");
    const instanceId = c.req.param("instanceId");

    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, instanceId),
        eq(agentInstance.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    const result = formatInstance(row) as Record<string, unknown>;

    // Enrich with runtime status from the event collector if available.
    const runtimeStatus = eventCollectors.getStatus(row.address);
    if (runtimeStatus !== undefined) {
      result["runtimeStatus"] = runtimeStatus.status;
    }

    return c.json(result);
  },
);

app.delete(
  "/:instanceId",
  requireGrant(idResource("instance", "instanceId"), "manage"),
  describeRoute({
    tags: ["Instances"],
    summary: "Stop an instance",
    description:
      "Stops the running instance and undeploys the agent from the sidecar.",
    responses: {
      204: {
        description: "Instance stopped",
      },
      404: {
        description: "Instance not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      409: {
        description: "Instance already stopped",
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
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const sessionService = c.get("sessionService");
    const sidecarRouter = c.get("sidecarRouter");
    const instanceId = c.req.param("instanceId");

    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, instanceId),
        eq(agentInstance.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    if (row.status === "stopped") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: "Instance is already stopped",
          },
        },
        409,
      );
    }

    const eventCollectors = c.get("eventCollectors");

    try {
      await sessionService.endSession(row.address, "instance_stopped");
    } catch (err) {
      return c.json(
        {
          error: {
            code: "sidecar_unavailable",
            message:
              err instanceof Error
                ? err.message
                : "Failed to reach sidecar for instance teardown",
          },
        },
        502,
      );
    }

    const endedAt = new Date();

    await db
      .update(agentInstance)
      .set({
        status: "stopped",
        sessionId: null,
        updatedAt: endedAt,
        endedAt,
      })
      .where(eq(agentInstance.id, instanceId));

    // End associated session rows.
    if (row.sessionId) {
      await db
        .update(agentSession)
        .set({ status: "ended", endedAt, updatedAt: endedAt })
        .where(eq(agentSession.id, row.sessionId));
    }

    eventCollectors.abandon(row.address);
    instanceKeyCache.delete(instanceId);

    sidecarRouter.dispatchAgentEvent(row.address, {
      type: "session.ended",
    });

    return c.body(null, 204);
  },
);

app.get(
  "/:instanceId/events",
  requireGrant(idResource("instance", "instanceId"), "read"),
  describeRoute({
    tags: ["Instances"],
    summary: "SSE event stream",
    description:
      "Server-Sent Events stream for agent events. Use POST .../messages for client-to-server messaging.",
    responses: {
      200: {
        description: "SSE event stream",
        content: {
          "text/event-stream": {},
        },
      },
      404: {
        description: "Instance not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      410: {
        description: "Instance stopped",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const sidecarRouter = c.get("sidecarRouter");
    const instanceId = c.req.param("instanceId");

    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, instanceId),
        eq(agentInstance.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    if (row.status === "stopped") {
      return c.json(
        { error: { code: "gone", message: "Instance has stopped" } },
        410,
      );
    }

    return streamSSE(c, async (stream) => {
      const noop = () => undefined;

      const unsubscribe = sidecarRouter.subscribeAgent(row.address, (event) => {
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

app.post(
  "/:instanceId/abort",
  requireGrant(idResource("instance", "instanceId"), "manage"),
  describeRoute({
    tags: ["Instances"],
    summary: "Abort current operation",
    description: "Aborts the agent's current inference or tool execution.",
    responses: {
      204: {
        description: "Abort signal sent",
      },
      404: {
        description: "Instance not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      409: {
        description: "Instance not running",
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
    const tenantCtx = c.get("tenant");
    const db = c.get("db");
    const sidecarRouter = c.get("sidecarRouter");
    const instanceId = c.req.param("instanceId");
    const body = c.req.valid("json");

    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, instanceId),
        eq(agentInstance.tenantId, tenantCtx.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    if (row.status !== "running") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: `Instance is not running (status: ${row.status})`,
          },
        },
        409,
      );
    }

    try {
      await sidecarRouter.sendSessionAbort(
        row.address,
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

// Crypto providers for signing outbound messages, keyed by instance ID.
// Evicted when an instance is stopped.
const instanceKeyCache = new Map<string, Promise<CryptoProvider>>();

function getInstanceCryptoProvider(
  instanceId: string,
): Promise<CryptoProvider> {
  let pending = instanceKeyCache.get(instanceId);
  if (pending !== undefined) return pending;
  pending = generateKeyPair().then((kp) => createNodeCrypto(kp));
  instanceKeyCache.set(instanceId, pending);
  return pending;
}

app.post(
  "/:instanceId/messages",
  requireGrant(idResource("instance", "instanceId"), "write"),
  describeRoute({
    tags: ["Instances"],
    summary: "Send a message to the agent",
    description:
      "Persists the user message and dispatches it to the running agent. The agent's response streams over the instance SSE channel.",
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
      404: {
        description: "Instance not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      409: {
        description: "Instance not running",
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
  validator("json", SendMessage),
  async (c) => {
    const tenant = c.get("tenant");
    const db = c.get("db");
    const sessionService = c.get("sessionService");
    const principal = c.get("principal");
    const instanceId = c.req.param("instanceId");
    const body = c.req.valid("json");

    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, instanceId),
        eq(agentInstance.tenantId, tenant.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    if (row.status !== "running") {
      return c.json(
        {
          error: {
            code: "conflict",
            message: `Instance is not running (status: ${row.status})`,
          },
        },
        409,
      );
    }

    if (!row.sessionId) {
      return c.json(
        {
          error: {
            code: "conflict",
            message: "Instance has no active session",
          },
        },
        409,
      );
    }

    const messageId = generateId("message");
    const partId = generateId("messagePart");
    const now = new Date();

    const user = c.get("user");
    const fromAddr = `${principal.refId}@${tenant.domain}`;
    const from = user?.name ? `"${user.name}" <${fromAddr}>` : fromAddr;
    const mimeMessageId = `<${messageId}@${tenant.domain}>`;

    await db.insert(sessionMessage).values({
      id: messageId,
      sessionId: row.sessionId,
      instanceId,
      tenantId: tenant.id,
      role: "user",
      from,
      status: "pending",
      createdAt: now,
    });

    await db.insert(messagePart).values({
      id: partId,
      messageId,
      sessionId: row.sessionId,
      type: "text",
      content: body.content,
      ordinal: 0,
    });

    // Fetch recent delivered user messages for the MIME References chain.
    // RFC 2822 does not require all prior message IDs; the most recent
    // are sufficient for threading.
    const priorMessages = await db
      .select({ id: sessionMessage.id })
      .from(sessionMessage)
      .where(
        and(
          eq(sessionMessage.instanceId, instanceId),
          eq(sessionMessage.role, "user"),
          eq(sessionMessage.status, "delivered"),
        ),
      )
      .orderBy(asc(sessionMessage.createdAt), asc(sessionMessage.id))
      .limit(100);

    const priorIds = priorMessages.map((m) => `<${m.id}@${tenant.domain}>`);
    const lastId = priorIds[priorIds.length - 1];

    const cryptoProvider = await getInstanceCryptoProvider(instanceId);

    try {
      await sessionService.sendUserMessage({
        agentAddress: row.address,
        from,
        messageId: mimeMessageId,
        date: now,
        content: body.content,
        ...(lastId !== undefined ? { inReplyTo: lastId } : {}),
        ...(priorIds.length > 0 ? { references: priorIds } : {}),
        sessionId: row.sessionId,
        tenantId: tenant.id,
        cryptoProvider,
      });

      await db
        .update(sessionMessage)
        .set({ status: "delivered" })
        .where(eq(sessionMessage.id, messageId));
    } catch (err) {
      await db
        .update(sessionMessage)
        .set({ status: "failed" })
        .where(eq(sessionMessage.id, messageId));

      return c.json(
        {
          error: {
            code: "sidecar_unavailable",
            message:
              err instanceof Error
                ? err.message
                : "Failed to deliver message to sidecar",
          },
        },
        502,
      );
    }

    return c.json(
      {
        id: messageId,
        sessionId: row.sessionId,
        role: "user" as const,
        status: "delivered" as const,
        createdAt: now.toISOString(),
        from,
        parts: [
          {
            id: partId,
            type: "text" as const,
            content: body.content,
          },
        ],
      },
      201,
    );
  },
);

app.get(
  "/:instanceId/messages",
  requireGrant(idResource("instance", "instanceId"), "read"),
  describeRoute({
    tags: ["Instances"],
    summary: "List messages for an instance",
    description:
      "Returns messages with all parts in chronological order. Cursor-paginated using ?cursor=<messageId>&limit=<n>.",
    parameters: [
      { name: "cursor", in: "query", schema: { type: "string" } },
      { name: "limit", in: "query", schema: { type: "integer" } },
    ],
    responses: {
      200: {
        description: "List of messages",
        content: {
          "application/json": {
            schema: resolver(MessageResponse.array()),
          },
        },
      },
      404: {
        description: "Instance not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const tenant = c.get("tenant");
    const db = c.get("db");
    const instanceId = c.req.param("instanceId");
    const cursor = c.req.query("cursor");
    const rawLimit = Number(c.req.query("limit") ?? 50);
    const limit =
      Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 100) : 50;

    const row = await db.query.agentInstance.findFirst({
      where: and(
        eq(agentInstance.id, instanceId),
        eq(agentInstance.tenantId, tenant.id),
      ),
    });

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    let cursorFilter;
    if (cursor !== undefined) {
      const cursorRow = await db.query.sessionMessage.findFirst({
        where: and(
          eq(sessionMessage.id, cursor),
          eq(sessionMessage.instanceId, instanceId),
        ),
        columns: { createdAt: true },
      });
      if (cursorRow) {
        cursorFilter = or(
          gt(sessionMessage.createdAt, cursorRow.createdAt),
          and(
            eq(sessionMessage.createdAt, cursorRow.createdAt),
            gt(sessionMessage.id, cursor),
          ),
        );
      }
    }

    const messages = await db
      .select()
      .from(sessionMessage)
      .where(and(eq(sessionMessage.instanceId, instanceId), cursorFilter))
      .orderBy(asc(sessionMessage.createdAt), asc(sessionMessage.id))
      .limit(limit);

    const messageIds = messages.map((m) => m.id);

    const parts =
      messageIds.length > 0
        ? await db
            .select()
            .from(messagePart)
            .where(inArray(messagePart.messageId, messageIds))
            .orderBy(asc(messagePart.ordinal))
        : [];

    const partsByMessage = new Map<string, typeof parts>();
    for (const part of parts) {
      let list = partsByMessage.get(part.messageId);
      if (list === undefined) {
        list = [];
        partsByMessage.set(part.messageId, list);
      }
      list.push(part);
    }

    return c.json(
      messages.map((m) => ({
        id: m.id,
        sessionId: m.sessionId,
        role: m.role,
        status: m.status,
        createdAt: m.createdAt.toISOString(),
        from: m.from,
        parts: (partsByMessage.get(m.id) ?? []).map((p) => ({
          id: p.id,
          type: p.type,
          content: p.content,
          metadata: p.metadata,
        })),
      })),
    );
  },
);

export { app as instanceRoutes };
