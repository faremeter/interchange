import { eq, and, inArray, asc } from "drizzle-orm";
import { Hono } from "hono";
import { describeRoute, resolver, validator } from "hono-openapi";
import { streamSSE } from "hono/streaming";
import { type } from "arktype";

import {
  agent,
  agentInstance,
  agentRole,
  agentSession,
  grant as grantTable,
  inferenceTurn,
  principal as principalTable,
  principalRole,
  provider,
  sessionMail,
  turnPart,
} from "@interchange/db/schema";
import { resolveCredentialRequirement } from "@interchange/db";
import { evaluateGrants, authorize } from "@interchange/authz";
import { parseMailToEmail, extractPartByPath } from "@interchange/mime";

import { generateKeyPair, createNodeCrypto } from "@interchange/crypto-node";
import {
  CreateAgentInstance,
  AgentInstanceResponse,
  GrantRequirement,
  SendMessage,
  MailResponse,
  InferenceTurnResponse,
  ErrorResponse,
  paginatedSchema,
} from "@interchange/types";
import type { GrantEffect, GrantOrigin } from "@interchange/types";
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
const GrantRequirements = GrantRequirement.array();

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

function formatInstance(
  row: typeof agentInstance.$inferSelect,
  agentName: string,
) {
  return {
    id: row.id,
    agentId: row.agentId,
    agentName,
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

    const instanceId = generateId("instance");
    const agentAddress = `${instanceId}@${tenant.domain}`;

    // --- Credential resolution ---

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

    const creatorPrincipalId = row.creatorPrincipalId;
    if (!creatorPrincipalId) {
      return c.json(
        {
          error: {
            code: "not_launchable",
            message:
              "Definition has no creator principal for credential resolution",
          },
        },
        409,
      );
    }

    const providers: ProviderConfig[] = [];
    for (const req of parsedRequirements) {
      let resolved;
      try {
        resolved = await resolveCredentialRequirement(
          db,
          tenant.id,
          req,
          creatorPrincipalId,
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

    // --- Model config ---

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

    // --- Grant requirement resolution (creator/invoker delegation) ---

    const grantStore = c.get("grantStore");
    const instancePrincipalId = generateId("principal");

    // Collect invoker's grants once — used for both creator and invoker resolution.
    const invokerGrants = await grantStore.collectGrants(
      principal.id,
      tenant.id,
    );
    // Only system/role/creator grants can be delegated. Invoker-sourced
    // grants cannot be transitively re-delegated.
    const delegatableInvokerGrants = invokerGrants.filter(
      (g) => g.origin !== "invoker",
    );

    // Accumulate grant rows in memory; write to DB only after all
    // requirements resolve. This avoids orphaned rows on partial failure.
    const grantRows: {
      id: string;
      tenantId: string;
      principalId: string;
      resource: string;
      action: string;
      effect: GrantEffect;
      conditions: Record<string, unknown> | null;
      origin: GrantOrigin;
      expiresAt: Date | null;
      createdAt: Date;
      updatedAt: Date;
    }[] = [];

    const now = new Date();
    const INVOKER_GRANT_TTL_MS = 24 * 60 * 60 * 1000;
    const invokerExpiresAt = new Date(now.getTime() + INVOKER_GRANT_TTL_MS);

    const parsedGrantReqs = GrantRequirements(row.grantRequirements ?? []);
    if (parsedGrantReqs instanceof type.errors) {
      return c.json(
        {
          error: {
            code: "not_launchable",
            message: `Invalid grant requirements: ${parsedGrantReqs.summary}`,
          },
        },
        409,
      );
    }

    // Collect creator's grants once for all creator-sourced requirements.
    const hasCreatorReqs = parsedGrantReqs.some((r) => r.source === "creator");
    const creatorGrants = hasCreatorReqs
      ? await grantStore.collectGrants(creatorPrincipalId, tenant.id)
      : [];

    for (const req of parsedGrantReqs) {
      const effect = req.effect ?? "allow";

      if (req.source === "creator") {
        const result = await evaluateGrants(
          creatorGrants,
          req.resource,
          req.action,
        );
        if (result.effect !== "allow") {
          return c.json(
            {
              error: {
                code: "insufficient_grants",
                message: `Creator lacks authority to delegate ${req.resource}/${req.action}`,
              },
            },
            403,
          );
        }
        grantRows.push({
          id: generateId("grant"),
          tenantId: tenant.id,
          principalId: instancePrincipalId,
          resource: req.resource,
          action: req.action,
          effect,
          conditions: req.conditions ?? null,
          origin: "creator",
          expiresAt: null,
          createdAt: now,
          updatedAt: now,
        });
      } else if (req.source === "invoker") {
        const result = await evaluateGrants(
          delegatableInvokerGrants,
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
        grantRows.push({
          id: generateId("grant"),
          tenantId: tenant.id,
          principalId: instancePrincipalId,
          resource: req.resource,
          action: req.action,
          effect,
          conditions: req.conditions ?? null,
          origin: "invoker",
          expiresAt: invokerExpiresAt,
          createdAt: now,
          updatedAt: now,
        });
      } else {
        return c.json(
          {
            error: {
              code: "not_launchable",
              message: `Unknown grant requirement source: ${req.source}`,
            },
          },
          409,
        );
      }
    }

    // Process ad-hoc invoker grants from the launch request.
    if (body.invokerGrants) {
      for (const ig of body.invokerGrants) {
        const effect = ig.effect ?? "allow";
        const result = await evaluateGrants(
          delegatableInvokerGrants,
          ig.resource,
          ig.action,
        );
        if (result.effect !== "allow") {
          return c.json(
            {
              error: {
                code: "insufficient_grants",
                message: `Invoker lacks authority for ${ig.resource}/${ig.action}`,
              },
            },
            403,
          );
        }
        grantRows.push({
          id: generateId("grant"),
          tenantId: tenant.id,
          principalId: instancePrincipalId,
          resource: ig.resource,
          action: ig.action,
          effect,
          conditions: ig.conditions ?? null,
          origin: "invoker",
          expiresAt: invokerExpiresAt,
          createdAt: now,
          updatedAt: now,
        });
      }
    }

    // --- Resolve agent role assignments for the instance principal ---

    const agentRoleRows = await db.query.agentRole.findMany({
      where: eq(agentRole.agentId, row.id),
    });
    const agentRoleIds = agentRoleRows.map((a) => a.roleId);
    const agentRoleAssignments =
      agentRoleIds.length > 0
        ? (
            await db.query.role.findMany({
              where: (r, { inArray, and: a }) =>
                a(inArray(r.id, agentRoleIds), eq(r.tenantId, tenant.id)),
              columns: { id: true },
            })
          ).map((r) => ({ roleId: r.id }))
        : [];

    // --- Write all DB rows in a transaction ---

    const sessionId = generateId("session");

    await db.transaction(async (tx) => {
      // Create per-instance principal
      await tx.insert(principalTable).values({
        id: instancePrincipalId,
        tenantId: tenant.id,
        kind: "agent",
        refId: instanceId,
        status: "active",
        createdAt: now,
        updatedAt: now,
      });

      // Assign the agent definition's roles to the instance principal so
      // that grants flow through the existing RBAC path (collectGrants).
      for (const { roleId } of agentRoleAssignments) {
        await tx.insert(principalRole).values({
          principalId: instancePrincipalId,
          roleId,
          createdAt: now,
        });
      }

      // Materialize grants on the instance principal
      for (const g of grantRows) {
        await tx.insert(grantTable).values(g);
      }

      // Transitional agentSession row (FK requirement)
      await tx.insert(agentSession).values({
        id: sessionId,
        tenantId: tenant.id,
        agentId: row.id,
        principalId: principal.id,
        status: "active",
        createdAt: now,
        updatedAt: now,
      });

      // Create instance row
      await tx.insert(agentInstance).values({
        id: instanceId,
        agentId: row.id,
        tenantId: tenant.id,
        principalId: instancePrincipalId,
        address: agentAddress,
        sessionId,
        status: "deployed",
        createdAt: now,
        updatedAt: now,
      });
    });

    // Collect the materialized grants for the deploy frame
    const grants = await grantStore.collectGrants(
      instancePrincipalId,
      tenant.id,
    );

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
          principalId: instancePrincipalId,
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

      // Deactivate the instance principal created during this launch
      await db
        .update(principalTable)
        .set({ status: "deactivated", updatedAt: failedAt })
        .where(eq(principalTable.id, instancePrincipalId));

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

    return c.json(formatInstance(launched, row.name), 201);
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

    const rows = await db
      .select({
        instance: agentInstance,
        agentName: agent.name,
      })
      .from(agentInstance)
      .innerJoin(agent, eq(agentInstance.agentId, agent.id))
      .where(and(...conditions))
      .orderBy(...pageOrder(agentInstance.createdAt, agentInstance.id))
      .limit(limit);

    return c.json(
      paginatedResponse(
        rows.map((r) => formatInstance(r.instance, r.agentName)),
        rows.map((r) => r.instance),
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

    const [row] = await db
      .select({
        instance: agentInstance,
        agentName: agent.name,
      })
      .from(agentInstance)
      .innerJoin(agent, eq(agentInstance.agentId, agent.id))
      .where(
        and(
          eq(agentInstance.id, instanceId),
          eq(agentInstance.tenantId, tenantCtx.id),
        ),
      )
      .limit(1);

    if (!row) {
      return c.json(
        { error: { code: "not_found", message: "Instance not found" } },
        404,
      );
    }

    const result = formatInstance(row.instance, row.agentName) as Record<
      string,
      unknown
    >;

    // Enrich with runtime status from the event collector if available.
    const runtimeStatus = eventCollectors.getStatus(row.instance.address);
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

    // Deactivate the per-instance principal. The refId guard ensures we
    // only deactivate the principal created for this specific instance.
    await db
      .update(principalTable)
      .set({ status: "deactivated", updatedAt: endedAt })
      .where(
        and(
          eq(principalTable.id, row.principalId),
          eq(principalTable.refId, instanceId),
        ),
      );

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
  "/:instanceId/mail",
  requireGrant(idResource("instance", "instanceId"), "write"),
  describeRoute({
    tags: ["Instances"],
    summary: "Send mail to the agent",
    description:
      "Persists the user message as a mail record and dispatches it to the running agent. Returns JMAP Email-shaped response.",
    responses: {
      201: {
        description: "Mail sent",
        content: {
          "application/json": { schema: resolver(MailResponse) },
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
    const sidecarRouter = c.get("sidecarRouter");
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

    const mailId = generateId("sessionMail");
    const now = new Date();

    const user = c.get("user");
    const fromAddr = `${principal.refId}@${tenant.domain}`;
    const from = user?.name ? `"${user.name}" <${fromAddr}>` : fromAddr;
    const mimeMessageId = `<${mailId}@${tenant.domain}>`;

    // Fetch recent delivered inbound mail for the MIME References chain.
    const priorMail = await db
      .select({ id: sessionMail.id })
      .from(sessionMail)
      .where(
        and(
          eq(sessionMail.instanceId, instanceId),
          eq(sessionMail.direction, "inbound"),
          eq(sessionMail.status, "delivered"),
        ),
      )
      .orderBy(asc(sessionMail.createdAt), asc(sessionMail.id))
      .limit(100);

    const priorIds = priorMail.map((m) => `<${m.id}@${tenant.domain}>`);
    const lastId = priorIds[priorIds.length - 1];

    const cryptoProvider = await getInstanceCryptoProvider(instanceId);

    let rawMIME: Uint8Array;
    try {
      rawMIME = await sessionService.sendUserMessage({
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
    } catch (err) {
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

    const mailCreatedAt = new Date();

    await db.insert(sessionMail).values({
      id: mailId,
      sessionId: row.sessionId,
      instanceId,
      tenantId: tenant.id,
      direction: "inbound",
      status: "delivered",
      raw: rawMIME,
      createdAt: mailCreatedAt,
    });

    const parsed = parseMailToEmail(rawMIME, mailId);
    sidecarRouter.dispatchAgentEvent(row.address, {
      type: "mail.delivered",
      data: { ...parsed, receivedAt: mailCreatedAt.toISOString() },
    });

    return c.json(
      {
        id: mailId,
        sessionId: row.sessionId,
        instanceId,
        direction: "inbound" as const,
        status: "delivered" as const,
        receivedAt: mailCreatedAt.toISOString(),
        ...parsed,
      },
      201,
    );
  },
);

app.get(
  "/:instanceId/mail",
  requireGrant(idResource("instance", "instanceId"), "read"),
  describeRoute({
    tags: ["Instances"],
    summary: "List mail for an instance",
    description:
      "Returns parsed JMAP Email objects in reverse chronological order. Cursor-paginated.",
    parameters: [...pageParameters],
    responses: {
      200: {
        description: "List of mail",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(MailResponse)),
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
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

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

    const conditions = [eq(sessionMail.instanceId, instanceId)];
    if (cursor) {
      conditions.push(
        cursorCondition(sessionMail.createdAt, sessionMail.id, cursor),
      );
    }

    const rows = await db
      .select()
      .from(sessionMail)
      .where(and(...conditions))
      .orderBy(...pageOrder(sessionMail.createdAt, sessionMail.id))
      .limit(limit);

    const items = rows.map((m) => {
      const parsed = parseMailToEmail(m.raw, m.id);
      return {
        id: m.id,
        sessionId: m.sessionId,
        instanceId: m.instanceId ?? null,
        direction: m.direction,
        status: m.status,
        receivedAt: m.createdAt.toISOString(),
        ...parsed,
      };
    });

    return c.json(paginatedResponse(items, rows, limit));
  },
);

app.get(
  "/:instanceId/turns",
  requireGrant(idResource("instance", "instanceId"), "read"),
  describeRoute({
    tags: ["Instances"],
    summary: "List inference turns for an instance",
    description:
      "Returns inference turns with their parts in reverse chronological order. Cursor-paginated.",
    parameters: [...pageParameters],
    responses: {
      200: {
        description: "List of inference turns",
        content: {
          "application/json": {
            schema: resolver(paginatedSchema(InferenceTurnResponse)),
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
    const { limit, cursor } = parsePageParams({
      cursor: c.req.query("cursor"),
      limit: c.req.query("limit"),
    });

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

    const conditions = [eq(inferenceTurn.instanceId, instanceId)];
    if (cursor) {
      conditions.push(
        cursorCondition(inferenceTurn.startedAt, inferenceTurn.id, cursor),
      );
    }

    const turns = await db
      .select()
      .from(inferenceTurn)
      .where(and(...conditions))
      .orderBy(...pageOrder(inferenceTurn.startedAt, inferenceTurn.id))
      .limit(limit);

    const turnIds = turns.map((t) => t.id);

    const parts =
      turnIds.length > 0
        ? await db
            .select()
            .from(turnPart)
            .where(inArray(turnPart.turnId, turnIds))
            .orderBy(asc(turnPart.ordinal))
        : [];

    const partsByTurn = new Map<string, typeof parts>();
    for (const part of parts) {
      let list = partsByTurn.get(part.turnId);
      if (list === undefined) {
        list = [];
        partsByTurn.set(part.turnId, list);
      }
      list.push(part);
    }

    const items = turns.map((t) => ({
      id: t.id,
      sessionId: t.sessionId,
      instanceId: t.instanceId,
      model: t.model,
      status: t.status,
      startedAt: t.startedAt.toISOString(),
      endedAt: t.endedAt ? t.endedAt.toISOString() : null,
      parts: (partsByTurn.get(t.id) ?? []).map((p) => ({
        id: p.id,
        type: p.type,
        content: p.content ?? null,
        metadata: p.metadata ?? null,
        ordinal: p.ordinal,
      })),
    }));

    return c.json(
      paginatedResponse(
        items,
        turns.map((t) => ({ createdAt: t.startedAt, id: t.id })),
        limit,
      ),
    );
  },
);

app.get(
  "/blobs/:blobId",
  describeRoute({
    tags: ["Instances"],
    summary: "Fetch a blob by ID",
    description:
      "Returns raw bytes for a MIME part. Blob IDs are issued by the mail parsing layer.",
    responses: {
      200: {
        description: "Blob bytes",
        content: { "application/octet-stream": {} },
      },
      400: {
        description: "Invalid blob ID",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      403: {
        description: "Forbidden",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
      404: {
        description: "Blob not found",
        content: {
          "application/json": { schema: resolver(ErrorResponse) },
        },
      },
    },
  }),
  async (c) => {
    const blobId = c.req.param("blobId");

    // Blob IDs have the format: blob_<mailId>_<partPath>
    // where partPath is an IMAP-style section specifier (digits and dots only).
    // mailId may itself contain underscores, so we match the suffix.
    const blobMatch = /^blob_(.+?)_(\d[\d.]*)$/.exec(blobId);
    if (!blobMatch) {
      return c.json(
        { error: { code: "bad_request", message: "Invalid blob ID format" } },
        400,
      );
    }

    const mailId = blobMatch[1];
    const partPath = blobMatch[2];

    if (!mailId || !partPath) {
      return c.json(
        { error: { code: "bad_request", message: "Invalid blob ID format" } },
        400,
      );
    }

    const tenant = c.get("tenant");
    const db = c.get("db");

    const mailRow = await db.query.sessionMail.findFirst({
      where: and(
        eq(sessionMail.id, mailId),
        eq(sessionMail.tenantId, tenant.id),
      ),
    });

    if (!mailRow) {
      return c.json(
        { error: { code: "not_found", message: "Blob not found" } },
        404,
      );
    }

    const resolvedInstanceId = mailRow.instanceId;
    if (!resolvedInstanceId) {
      return c.json(
        { error: { code: "not_found", message: "Blob not found" } },
        404,
      );
    }

    const principal = c.get("principal");
    const grantStore = c.get("grantStore");
    const conditionRegistry = c.get("conditionRegistry");

    const authResult = await authorize(
      grantStore,
      principal.id,
      tenant.id,
      `instance:${resolvedInstanceId}`,
      "read",
      conditionRegistry,
    );

    if (authResult.effect !== "allow") {
      return c.json(
        {
          error: {
            code: "forbidden",
            message: "You do not have permission to perform this action",
          },
        },
        403,
      );
    }

    let partBytes: Uint8Array;
    try {
      partBytes = extractPartByPath(mailRow.raw, partPath);
    } catch {
      return c.json(
        { error: { code: "not_found", message: "Blob not found" } },
        404,
      );
    }

    return c.body(
      partBytes.buffer.slice(
        partBytes.byteOffset,
        partBytes.byteOffset + partBytes.byteLength,
      ) as ArrayBuffer,
      200,
      {
        "Content-Type": "application/octet-stream",
      },
    );
  },
);

export { app as instanceRoutes };
