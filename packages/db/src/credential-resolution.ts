import { eq, and, isNull } from "drizzle-orm";
import { type } from "arktype";

import { getLogger } from "@intx/log";
import { CredentialRequirement as CredentialRequirementType } from "@intx/types";
import type { ProviderConfig } from "@intx/types/runtime";

import type { DB } from "./client";
import { agent } from "./schema/agents";
import { agentSession } from "./schema/sessions";
import { credential } from "./schema/credentials";
import { oauthClient } from "./schema/oauth-clients";
import { provider } from "./schema/providers";
import { getAncestorChain } from "./tenant-hierarchy";

const log = getLogger(["db", "credentials"]);

const CredentialRequirements = CredentialRequirementType.array();

export const ProviderMetadata = type({ baseURL: "string" });

/**
 * Resolves a provider by name, walking up the tenant hierarchy.
 * Returns the first match (child shadows parent).
 */
export async function resolveProviderByName(
  db: DB["db"],
  tenantId: string,
  name: string,
) {
  const chain = await getAncestorChain(db, tenantId);

  for (const tid of chain) {
    const row = await db.query.provider.findFirst({
      where: and(eq(provider.tenantId, tid), eq(provider.name, name)),
    });
    if (row) return row;
  }

  return null;
}

/**
 * Resolves an OAuth client for a provider, walking up the tenant hierarchy.
 * Returns the first match (child shadows parent).
 */
export async function resolveOAuthClient(
  db: DB["db"],
  tenantId: string,
  providerId: string,
) {
  const chain = await getAncestorChain(db, tenantId);

  for (const tid of chain) {
    const row = await db.query.oauthClient.findFirst({
      where: and(
        eq(oauthClient.tenantId, tid),
        eq(oauthClient.providerId, providerId),
      ),
    });
    if (row) return row;
  }

  return null;
}

/**
 * Resolves a credential by name, walking up the tenant hierarchy.
 * Returns the first match (child shadows parent).
 */
export async function resolveCredentialByName(
  db: DB["db"],
  tenantId: string,
  name: string,
) {
  const chain = await getAncestorChain(db, tenantId);

  for (const tid of chain) {
    const row = await db.query.credential.findFirst({
      where: and(eq(credential.tenantId, tid), eq(credential.name, name)),
    });
    if (row) return row;
  }

  return null;
}

/**
 * Resolves a credential by ID, validating that it belongs to the
 * given tenant or one of its ancestors.
 */
export async function resolveCredentialById(
  db: DB["db"],
  tenantId: string,
  credentialId: string,
) {
  const row = await db.query.credential.findFirst({
    where: eq(credential.id, credentialId),
  });

  if (!row) return null;

  const chain = await getAncestorChain(db, tenantId);
  if (!chain.includes(row.tenantId)) return null;

  return row;
}

type CredentialRequirement = {
  providerName: string;
  scopes?: string[];
  source: "tenant" | "creator" | "invoker";
  name?: string;
};

/**
 * Resolves a credential matching an agent definition requirement.
 * Used at agent launch time by the control plane.
 */
export async function resolveCredentialRequirement(
  db: DB["db"],
  tenantId: string,
  requirement: CredentialRequirement,
  creatorPrincipalId: string | null,
  invokerPrincipalId: string | null,
) {
  const resolvedProvider = await resolveProviderByName(
    db,
    tenantId,
    requirement.providerName,
  );
  if (!resolvedProvider) return null;

  const chain = await getAncestorChain(db, tenantId);

  const principalFilter =
    requirement.source === "tenant"
      ? null
      : requirement.source === "creator"
        ? creatorPrincipalId
        : invokerPrincipalId;

  for (const tid of chain) {
    const conditions = [
      eq(credential.tenantId, tid),
      eq(credential.providerId, resolvedProvider.id),
      eq(credential.status, "active"),
    ];

    if (principalFilter === null) {
      conditions.push(isNull(credential.principalId));
    } else if (principalFilter) {
      conditions.push(eq(credential.principalId, principalFilter));
    }

    if (requirement.name) {
      conditions.push(eq(credential.name, requirement.name));
    }

    const rows = await db.query.credential.findMany({
      where: and(...conditions),
    });

    const matching = rows.filter((row) => {
      if (!requirement.scopes || requirement.scopes.length === 0) return true;
      const rowScopes = row.scopes ?? [];
      return requirement.scopes.every((s) => rowScopes.includes(s));
    });

    const [sole] = matching;
    if (matching.length === 1 && sole) return sole;
    if (matching.length > 1) {
      throw new Error(
        `Ambiguous credential match: ${matching.length} credentials match ` +
          `provider=${requirement.providerName} source=${requirement.source} ` +
          `in tenant ${tid}. Specify a name to disambiguate.`,
      );
    }
  }

  return null;
}

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
        agentRow.creatorPrincipalId,
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
