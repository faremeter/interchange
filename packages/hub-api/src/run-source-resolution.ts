// Shared inference-source resolution for run creation.
//
// A run's inference sources come from its definition resolved against the
// tenant catalog -- never from the request body. This is the one place that
// turns a definition's model-requirements manifest into the ordered,
// credential-bearing source chain a run launches against, so the launch route
// and (later) the unified multi-step path cannot drift.

import { resolveModelSources, type DB } from "@intx/db";
import type { GrantStore } from "@intx/types/authz";
import type { ModelRequirement, ProviderPreference } from "@intx/types";
import type { InferenceSource } from "@intx/types/runtime";

export type DefinitionSourceResolution =
  | { ok: true; sources: InferenceSource[]; defaultSource: string }
  | { ok: false; message: string };

/**
 * Resolve a definition's ordered inference-source chain against the tenant
 * catalog. The chain head is the active source; the tail is the failover chain
 * (the run pins the whole chain). Requirements come from the definition's
 * `modelRequirements` manifest when set, else are derived from the single
 * step's declared model (`fallbackModel`) for a definition that carries no
 * manifest. A source carries a credential secret only where the definition's
 * creator holds `credential:{id}` / `use`, fail-closed. Collection reaches up
 * the tenant ancestor chain so an inherited credential's use-grant -- stamped
 * with the credential's own tenant -- is not withheld by single-tenant
 * collection. On failure the message is caller-facing (a 409 not_launchable).
 */
export async function resolveDefinitionSources(args: {
  db: DB["db"];
  grantStore: GrantStore;
  tenantId: string;
  creatorPrincipalId: string;
  modelRequirements: ModelRequirement[] | null;
  fallbackModel: string | null;
  invokerPreferences: Record<string, ProviderPreference>;
}): Promise<DefinitionSourceResolution> {
  const requirements: ModelRequirement[] =
    args.modelRequirements !== null
      ? args.modelRequirements
      : args.fallbackModel !== null
        ? [{ model: args.fallbackModel }]
        : [];

  const creatorGrants = await args.grantStore.collectGrantsInChain(
    args.creatorPrincipalId,
    args.tenantId,
  );

  const resolution = await resolveModelSources(
    args.db,
    args.tenantId,
    requirements,
    creatorGrants,
    { invokerPreferences: args.invokerPreferences },
  );
  if (!resolution.ok) {
    const message =
      resolution.reason === "no_requirements"
        ? "This definition declares no model requirements; cannot resolve any inference sources"
        : `No launchable inference source for model "${resolution.model}"` +
          (resolution.skips.length > 0
            ? ` (${resolution.skips
                .map((skip) => `${skip.provider}: ${skip.reason}`)
                .join(", ")})`
            : "");
    return { ok: false, message };
  }

  const [headSource] = resolution.sources;
  if (headSource === undefined) {
    return {
      ok: false,
      message: "Inference source resolution produced no sources",
    };
  }
  return {
    ok: true,
    sources: resolution.sources,
    defaultSource: headSource.id,
  };
}
