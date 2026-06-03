import { eq, and, isNull } from "drizzle-orm";
import { type } from "arktype";

import { getLogger } from "@intx/log";
import { CredentialRequirement as CredentialRequirementType } from "@intx/types";
import type { InferenceSource } from "@intx/types/runtime";

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

const AgentModelConfig = type({ defaultModel: "string" });

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
 * Outcome of resolving one credential requirement to an
 * `InferenceSource`. Callers map `failed` variants to their own
 * error-handling convention (the API route surfaces 409s; the
 * background credential pushers log and continue).
 */
export type CredentialOutcome =
  | { ok: true; source: InferenceSource }
  | {
      ok: false;
      reason: "credential_error";
      requirement: CredentialRequirement;
      message: string;
    }
  | {
      ok: false;
      reason: "credential_missing";
      requirement: CredentialRequirement;
    }
  | {
      ok: false;
      reason: "skipped";
      requirement: CredentialRequirement;
    }
  | {
      ok: false;
      reason: "provider_missing";
      credentialId: string;
    }
  | {
      ok: false;
      reason: "provider_misconfigured";
      providerName: string;
      summary: string;
    };

/**
 * Resolve one credential requirement to an `InferenceSource`, stamping
 * `defaultModel` as the model identity. Skips silently (`reason:
 * "skipped"`) when the requirement targets a principal that does not
 * exist (creator without a creator id, invoker without a session
 * principal); all other failure modes are surfaced with structured
 * reasons.
 */
export async function resolveOneCredential(
  db: DB["db"],
  tenantId: string,
  req: CredentialRequirement,
  creatorPrincipalId: string | null,
  invokerPrincipalId: string | null,
  defaultModel: string,
): Promise<CredentialOutcome> {
  // Reject requirements whose targeted principal does not exist. Without
  // these guards a null creator or invoker would fall through to
  // `resolveCredentialRequirement`, where the principal filter becomes
  // `isNull(credential.principalId)` — the tenant-credential lookup — and
  // a tenant credential would be returned as if it satisfied the
  // creator- or invoker-source requirement.
  if (req.source === "creator" && !creatorPrincipalId) {
    return { ok: false, reason: "skipped", requirement: req };
  }
  if (req.source === "invoker" && !invokerPrincipalId) {
    return { ok: false, reason: "skipped", requirement: req };
  }

  let resolved;
  try {
    resolved = await resolveCredentialRequirement(
      db,
      tenantId,
      req,
      creatorPrincipalId,
      invokerPrincipalId,
    );
  } catch (err: unknown) {
    return {
      ok: false,
      reason: "credential_error",
      requirement: req,
      message: err instanceof Error ? err.message : String(err),
    };
  }
  if (!resolved) {
    return { ok: false, reason: "credential_missing", requirement: req };
  }

  const providerRow = await db.query.provider.findFirst({
    where: eq(provider.id, resolved.providerId),
  });
  if (!providerRow) {
    return { ok: false, reason: "provider_missing", credentialId: resolved.id };
  }

  const metadata = ProviderMetadata(providerRow.metadata ?? {});
  if (metadata instanceof type.errors) {
    return {
      ok: false,
      reason: "provider_misconfigured",
      providerName: providerRow.name,
      summary: metadata.summary,
    };
  }

  return {
    ok: true,
    source: {
      id: `${providerRow.plugin}:${defaultModel}`,
      provider: providerRow.plugin,
      baseURL: metadata.baseURL,
      apiKey: resolved.secret,
      model: defaultModel,
    },
  };
}

/**
 * Resolve the full `InferenceSource[]` for a single running instance by
 * re-resolving each credential requirement from the agent definition.
 *
 * Failure modes (invalid agent definition, malformed `modelConfig`, any
 * per-credential resolution failure) collapse to an empty array with
 * structured `db.credentials` log lines. Callers that need to surface
 * per-credential outcomes to operators should call `resolveOneCredential`
 * directly — see `packages/hub-api/src/routes/instances.ts` for the
 * 409-mapping pattern.
 */
export async function resolveInstanceSources(
  db: DB["db"],
  tenantId: string,
  instance: { agentId: string; sessionId: string | null },
): Promise<InferenceSource[]> {
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

  const modelConfig = AgentModelConfig(agentRow.modelConfig ?? {});
  if (modelConfig instanceof type.errors) {
    log.warn`Invalid modelConfig for agent ${agentRow.id}: ${modelConfig.summary}`;
    return [];
  }
  const defaultModel = modelConfig.defaultModel;

  let invokerPrincipalId: string | null = null;
  if (instance.sessionId) {
    const session = await db.query.agentSession.findFirst({
      where: eq(agentSession.id, instance.sessionId),
    });
    if (session) {
      invokerPrincipalId = session.principalId;
    }
  }

  const sources: InferenceSource[] = [];
  for (const req of requirements) {
    const outcome = await resolveOneCredential(
      db,
      tenantId,
      req,
      agentRow.creatorPrincipalId,
      invokerPrincipalId,
      defaultModel,
    );
    if (outcome.ok) {
      sources.push(outcome.source);
      continue;
    }
    switch (outcome.reason) {
      case "skipped":
        break;
      case "credential_error":
        log.warn`Failed to resolve credential for provider ${outcome.requirement.providerName}: ${outcome.message}`;
        break;
      case "credential_missing":
        break;
      case "provider_missing":
        break;
      case "provider_misconfigured":
        log.warn`Invalid provider metadata for provider ${outcome.providerName}: ${outcome.summary}`;
        break;
    }
  }

  return sources;
}
