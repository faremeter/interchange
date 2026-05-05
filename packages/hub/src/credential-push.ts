// Shared logic for resolving and pushing provider credentials to sidecars.
//
// Used by the credentials PATCH route (tenant-wide push after secret rotation)
// and by the reconnect handler (per-instance refresh after sidecar reconnect).

import { eq, and } from "drizzle-orm";
import { type } from "arktype";
import { getLogger } from "@interchange/log";
import {
  agent,
  agentInstance,
  agentSession,
  provider,
} from "@interchange/db/schema";
import { resolveCredentialRequirement } from "@interchange/db";
import type { ProviderConfig } from "@interchange/types/runtime";
import type { DB } from "@interchange/db";

import type { SidecarRouter } from "./ws/sidecar-handler";

const log = getLogger(["hub", "credentials"]);

const CredentialRequirement = type({
  providerName: "string",
  "scopes?": "string[]",
  source: "'tenant' | 'creator' | 'invoker'",
  "name?": "string",
});
const CredentialRequirements = CredentialRequirement.array();

const ProviderMetadata = type({ baseURL: "string" });

/**
 * Resolve the full ProviderConfig[] for a single running instance by
 * re-resolving each credential requirement from the agent definition.
 *
 * Returns an empty array if no requirements are defined or none could
 * be resolved.
 */
export async function resolveInstanceProviders(
  db: DB["db"],
  tenantId: string,
  instance: { agentId: string; sessionId: string | null },
): Promise<ProviderConfig[]> {
  const agentRow = await db.query.agent.findFirst({
    where: eq(agent.id, instance.agentId),
  });
  if (!agentRow) return [];

  const requirements = CredentialRequirements(
    agentRow.credentialRequirements ?? [],
  );
  if (requirements instanceof type.errors) {
    log.warn`Invalid credential requirements for agent ${agentRow.id}: ${requirements.summary}`;
    return [];
  }

  let invokerPrincipalId: string | null = null;
  if (instance.sessionId) {
    const session = await db.query.agentSession.findFirst({
      where: eq(agentSession.id, instance.sessionId),
    });
    if (session) {
      invokerPrincipalId = session.principalId;
    }
  }

  const providers: ProviderConfig[] = [];
  for (const req of requirements) {
    if (req.source === "creator" && !agentRow.creatorPrincipalId) {
      continue;
    }
    if (req.source === "invoker" && !invokerPrincipalId) {
      continue;
    }

    let resolved;
    try {
      resolved = await resolveCredentialRequirement(
        db,
        tenantId,
        req,
        agentRow.creatorPrincipalId ?? "",
        invokerPrincipalId,
      );
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      log.warn`Failed to resolve credential for provider ${req.providerName}: ${msg}`;
      continue;
    }
    if (!resolved) continue;

    const providerRow = await db.query.provider.findFirst({
      where: eq(provider.id, resolved.providerId),
    });
    if (!providerRow) continue;

    const metadata = ProviderMetadata(providerRow.metadata ?? {});
    if (metadata instanceof type.errors) {
      log.warn`Invalid provider metadata for provider ${providerRow.id}: ${metadata.summary}`;
      continue;
    }

    providers.push({
      provider: providerRow.plugin,
      baseURL: metadata.baseURL,
      apiKey: resolved.secret,
    });
  }

  return providers;
}

/**
 * After a credential secret is rotated, find all running instances in the
 * tenant that may use credentials from the affected provider, re-resolve
 * their full providers array, and push updates to sidecars.
 *
 * Errors are logged per-instance but do not propagate.
 */
export async function pushProviderUpdates(
  db: DB["db"],
  sidecarRouter: SidecarRouter,
  tenantId: string,
): Promise<void> {
  const instances = await db.query.agentInstance.findMany({
    where: and(
      eq(agentInstance.tenantId, tenantId),
      eq(agentInstance.status, "running"),
    ),
  });

  if (instances.length === 0) return;

  const results = await Promise.allSettled(
    instances.map(async (instance) => {
      const providers = await resolveInstanceProviders(db, tenantId, instance);
      if (providers.length === 0) return;
      await sidecarRouter.sendProvidersUpdate(instance.address, providers);
    }),
  );

  for (const result of results) {
    if (result.status === "rejected") {
      log.warn`Failed to push provider update: ${String(result.reason)}`;
    }
  }
}
