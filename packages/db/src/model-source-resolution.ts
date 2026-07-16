import { and, eq } from "drizzle-orm";

import { evaluateGrants } from "@intx/authz";
import {
  InvokerModelPreferences,
  ModelRequirements,
  type ModelRequirement,
  type ProviderPreference,
} from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import type { InferenceSource } from "@intx/types/runtime";

import {
  listVisibleOfferings,
  type ResolvedOffering,
} from "./catalog-resolution";
import type { DB } from "./client";
import { resolveCredentialById } from "./credential-resolution";
import { createGrantStore } from "./grant-store";
import { agent } from "./schema/agents";

/**
 * Why a single offering could not be turned into a launchable source.
 * `wallet_backed` providers are storable in the catalog but not launchable
 * in this credential-backed-only release.
 */
export type SourceSkip =
  | { reason: "wallet_backed"; provider: string }
  | { reason: "credential_unresolved"; provider: string }
  | { reason: "credential_unauthorized"; provider: string }
  | { reason: "provider_misconfigured"; provider: string };

/**
 * Outcome of resolving an agent's model requirements to an ordered set of
 * inference sources. The order is the routing order: the head is the
 * default, the tail is the failover chain.
 *
 * `model_unavailable.skips` enumerates the offerings that were eligible for
 * the model but could not produce a launchable source (wallet-backed, or an
 * unresolvable credential). It is empty when no offering was eligible at all
 * — the model is absent from the tenant catalog, or the capability and
 * preference filters excluded every offering — a case the launch path can
 * distinguish from a populated `skips` when explaining the failure.
 */
export type CatalogSourceResolution =
  | { ok: true; sources: InferenceSource[] }
  | { ok: false; reason: "no_requirements" }
  | {
      ok: false;
      reason: "model_unavailable";
      model: string;
      skips: SourceSkip[];
    };

function byPriority(a: ResolvedOffering, b: ResolvedOffering): number {
  if (a.offering.priority !== b.offering.priority) {
    return a.offering.priority - b.offering.priority;
  }
  // Deterministic tiebreak so the head/defaultSource never flaps across
  // resolutions. Equal-priority load balancing is future work.
  return a.offering.id < b.offering.id ? -1 : 1;
}

/**
 * Applies a provider preference over an already-priority-sorted candidate
 * list. `pin` restricts to the named providers (ordered by the preference);
 * `prefer` fronts the named providers in preference order and keeps the rest
 * as fallback. With no preference the priority order stands.
 */
function applyPreference(
  candidates: ResolvedOffering[],
  preference: ProviderPreference | undefined,
): ResolvedOffering[] {
  if (preference === undefined) return candidates;

  const rank = new Map(preference.order.map((name, i) => [name, i]));

  // Partition by whether the preference names the provider, capturing the
  // rank as we go. `ranked` only ever holds named providers, so the sort
  // reads a concrete rank and never needs a fallback for a missing one.
  const ranked: { offering: ResolvedOffering; rank: number }[] = [];
  const rest: ResolvedOffering[] = [];
  for (const offering of candidates) {
    const position = rank.get(offering.provider.name);
    if (position === undefined) {
      rest.push(offering);
    } else {
      ranked.push({ offering, rank: position });
    }
  }
  ranked.sort((a, b) => a.rank - b.rank);
  const named = ranked.map((entry) => entry.offering);

  // `pin` drops every provider the preference did not name; `prefer` keeps
  // the rest as fallback after the named providers.
  return preference.mode === "pin" ? named : [...named, ...rest];
}

async function buildSource(
  db: DB["db"],
  tenantId: string,
  resolved: ResolvedOffering,
  creatorGrants: GrantRule[],
): Promise<
  { ok: true; source: InferenceSource } | { ok: false; skip: SourceSkip }
> {
  const { provider, model, offering } = resolved;

  if (provider.credentialId === null) {
    // No credential reference. A wallet-backed provider is a valid catalog
    // row but not launchable in this credential-backed-only release;
    // anything else is a misconfigured row.
    if (provider.walletId !== null) {
      return {
        ok: false,
        skip: { reason: "wallet_backed", provider: provider.name },
      };
    }
    return {
      ok: false,
      skip: { reason: "provider_misconfigured", provider: provider.name },
    };
  }

  // Resolve the secret through the tenant-scoped credential resolver so a
  // provider row referencing a credential outside the tenant's ancestor
  // chain cannot leak that secret (the chain is the authority).
  const credential = await resolveCredentialById(
    db,
    tenantId,
    provider.credentialId,
  );
  if (credential === null) {
    return {
      ok: false,
      skip: { reason: "credential_unresolved", provider: provider.name },
    };
  }

  // The tenant chain proves the credential is reachable; it does not prove the
  // agent's creator is authorized to spend it. Gate the secret on the creator
  // holding a `credential:{id}` / `use` grant. Fail closed: anything other than
  // an `allow` effect (including `ask`, `deny`, and no matching grant at all)
  // withholds the secret so it never enters a launchable source.
  const authorization = await evaluateGrants(
    creatorGrants,
    `credential:${credential.id}`,
    "use",
  );
  if (authorization.effect !== "allow") {
    return {
      ok: false,
      skip: { reason: "credential_unauthorized", provider: provider.name },
    };
  }

  return {
    ok: true,
    source: {
      id: offering.id,
      provider: provider.plugin,
      baseURL: provider.baseURL,
      apiKey: credential.secret,
      model: model.canonicalName,
      capabilities: offering.capabilities,
    },
  };
}

/**
 * Resolves an agent's model requirements against the tenant catalog into an
 * ordered `InferenceSource[]` for the harness.
 *
 * For each requirement, the tenant-visible offerings for the named model are
 * filtered by the required capabilities, ordered by catalog priority, then
 * reordered by the creator preference and finally the invoker preference
 * (invoker preferences key on the canonical model name). Each surviving
 * offering is resolved to a credential-backed source; offerings that cannot
 * produce one are skipped. A required model that yields no source makes the
 * agent unlaunchable.
 *
 * `creatorGrants` are the agent creator's collected grants. A credential-backed
 * source is only emitted when the creator holds `credential:{id}` / `use` for
 * the referenced credential; otherwise the offering is skipped
 * (`credential_unauthorized`) and its secret is withheld.
 */
export async function resolveModelSources(
  db: DB["db"],
  tenantId: string,
  requirements: ModelRequirement[],
  creatorGrants: GrantRule[],
  opts?: { invokerPreferences?: Record<string, ProviderPreference> },
): Promise<CatalogSourceResolution> {
  if (requirements.length === 0) {
    return { ok: false, reason: "no_requirements" };
  }

  const visible = await listVisibleOfferings(db, tenantId);
  const sources: InferenceSource[] = [];

  for (const requirement of requirements) {
    let candidates = visible
      .filter((o) => o.model.canonicalName === requirement.model)
      .sort(byPriority);

    if (requirement.capabilities && requirement.capabilities.length > 0) {
      const required = requirement.capabilities;
      candidates = candidates.filter((o) =>
        required.every((c) => o.offering.capabilities.includes(c)),
      );
    }

    candidates = applyPreference(candidates, requirement.providers);
    candidates = applyPreference(
      candidates,
      opts?.invokerPreferences?.[requirement.model],
    );

    const skips: SourceSkip[] = [];
    const modelSources: InferenceSource[] = [];
    for (const candidate of candidates) {
      const built = await buildSource(db, tenantId, candidate, creatorGrants);
      if (built.ok) {
        modelSources.push(built.source);
      } else {
        skips.push(built.skip);
      }
    }

    if (modelSources.length === 0) {
      return {
        ok: false,
        reason: "model_unavailable",
        model: requirement.model,
        skips,
      };
    }
    sources.push(...modelSources);
  }

  return { ok: true, sources };
}

/**
 * Resolves the ordered sources for a running instance from persisted state:
 * the agent definition's model requirements and the invoker's launch-time
 * preferences stored on the instance row. Launch, credential-rotation push,
 * and sidecar reconnect all resolve through this, so a running instance's
 * source list is a pure function of persisted state — re-resolution
 * reproduces the launch ordering, including the invoker's reorder/restrict.
 */
export async function resolveInstanceModelSources(
  db: DB["db"],
  tenantId: string,
  instance: { agentId: string; modelPreferences: unknown },
): Promise<CatalogSourceResolution> {
  // Scope the agent lookup to the resolving tenant, matching the launch
  // route. A cross-tenant agentId resolves to nothing rather than
  // contributing another tenant's model names or preferences.
  const agentRow = await db.query.agent.findFirst({
    where: and(eq(agent.id, instance.agentId), eq(agent.tenantId, tenantId)),
  });
  if (agentRow === undefined) {
    return { ok: false, reason: "no_requirements" };
  }

  const requirements =
    agentRow.modelRequirements !== null
      ? ModelRequirements.assert(agentRow.modelRequirements)
      : [];

  const preferences =
    instance.modelPreferences !== null
      ? InvokerModelPreferences.assert(instance.modelPreferences)
      : [];
  const invokerPreferences: Record<string, ProviderPreference> = {};
  for (const preference of preferences) {
    invokerPreferences[preference.model] = preference.providers;
  }

  // The authorizing party for a credential-backed source is the agent's
  // creator, recorded on the definition. Re-resolution (rotation, reconnect)
  // must re-check the creator's `credential:{id}` / `use` grant, so collect
  // the creator's grants here rather than threading them through the push
  // callers.
  const creatorGrants = await createGrantStore(db).collectGrants(
    agentRow.creatorPrincipalId,
    tenantId,
  );

  return resolveModelSources(db, tenantId, requirements, creatorGrants, {
    invokerPreferences,
  });
}
