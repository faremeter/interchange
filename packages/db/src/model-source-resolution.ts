import { and, eq } from "drizzle-orm";

import {
  InvokerModelPreferences,
  ModelRequirements,
  credentialAad,
  type CredentialCipher,
  type ModelRequirement,
  type ProviderPreference,
} from "@intx/types";
import type { InferenceSource } from "@intx/types/runtime";
import type { CredentialMaterialEntry } from "@intx/types/sidecar";

import {
  listVisibleOfferings,
  type ResolvedOffering,
} from "./catalog-resolution";
import type { DB } from "./client";
import { resolveCredentialById } from "./credential-resolution";
import { parseModelOfferingRow } from "./parse-row";
import { workflowDefinition } from "./schema/workflow-definitions";

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
 * inference sources. The order is the routing order: the head is the default,
 * the tail is the failover chain.
 *
 * `model_unavailable.skips` enumerates the offerings that were eligible for the
 * model but could not produce a launchable source. It is empty when no offering
 * was eligible at all -- the model is absent from the tenant catalog, or the
 * capability and preference filters excluded every offering -- a case the launch
 * path can distinguish from a populated `skips` when explaining the failure.
 */
export type CatalogSourceResolution =
  | {
      ok: true;
      sources: InferenceSource[];
      // The credential material backing the resolved sources, deduped by
      // `credentialId`. The secret rides here so the caller can seal it into
      // the run's unified credential-material cell.
      materials: CredentialMaterialEntry[];
    }
  | { ok: false; reason: "no_requirements" }
  | {
      ok: false;
      reason: "model_unavailable";
      model: string;
      skips: SourceSkip[];
    };

export type OfferingSourceResolution =
  | {
      ok: true;
      sources: InferenceSource[];
    }
  | {
      ok: false;
      reason: "offering_unavailable";
      offeringId: string;
      skip?: SourceSkip;
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
  credentialCipher: CredentialCipher,
): Promise<
  | { ok: true; source: InferenceSource; material: CredentialMaterialEntry }
  | { ok: false; skip: SourceSkip }
> {
  const { provider, model, offering } = resolved;

  if (provider.credentialId === null) {
    // No credential reference. A wallet-backed provider is a valid catalog row
    // but not launchable in this credential-backed-only release; anything else
    // is a misconfigured row.
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
  // provider row referencing a credential outside the tenant's ancestor chain
  // cannot leak that secret (the chain is the authority).
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

  // Authority to use a credential through a catalog provider is ownership
  // within the tenant hierarchy, the same rule the tool-binding path resolves.
  // `resolveCredentialById` above already proved the credential is reachable in
  // this tenant's ancestor chain, so a tenant-owned credential (`principalId IS
  // NULL`) is authorized by ownership alone. A principal-owned credential is not
  // usable through a shared catalog provider; fail closed.
  if (credential.principalId !== null) {
    return {
      ok: false,
      skip: { reason: "credential_unauthorized", provider: provider.name },
    };
  }

  // Validate the row once at this DB-to-runtime boundary: this narrows the
  // jsonb `quirks` from `unknown` to a `Record | null` and checks
  // `capabilities` against the curated enum. `quirks` is spread in only when
  // non-null so a source with no accommodations omits the key entirely.
  // Decrypt the stored secret at the one point it is used. Strict: a
  // non-ciphertext value throws rather than delivering a bad key -- fail
  // closed. The secret rides the credential-material cell (keyed by
  // `credentialId`), NOT the source config.
  const secret = await credentialCipher.decrypt(
    credential.secret,
    credentialAad(credential.id, "secret"),
  );
  const parsed = parseModelOfferingRow(offering);
  return {
    ok: true,
    source: {
      id: offering.id,
      provider: provider.plugin,
      baseURL: provider.baseURL,
      credentialId: provider.credentialId,
      model: model.canonicalName,
      capabilities: parsed.capabilities,
      ...(parsed.quirks !== null ? { quirks: parsed.quirks } : {}),
    },
    material: {
      credentialId: provider.credentialId,
      providerKey: provider.plugin,
      origin: provider.baseURL,
      secret,
    },
  };
}

/**
 * Rebuild an exact, ordered source chain from durable catalog offering ids.
 * The recovery counterpart to launch-time source selection: it keeps secrets
 * out of persisted launch specs and rechecks tenant visibility and credential
 * ownership on every launch.
 */
export async function resolveSourcesByOfferingIds(
  db: DB["db"],
  tenantId: string,
  offeringIds: readonly string[],
  credentialCipher: CredentialCipher,
): Promise<OfferingSourceResolution> {
  const visible = await listVisibleOfferings(db, tenantId);
  const byId = new Map(visible.map((entry) => [entry.offering.id, entry]));
  const sources: InferenceSource[] = [];
  for (const offeringId of offeringIds) {
    const offering = byId.get(offeringId);
    if (offering === undefined) {
      return { ok: false, reason: "offering_unavailable", offeringId };
    }
    const built = await buildSource(db, tenantId, offering, credentialCipher);
    if (!built.ok) {
      return {
        ok: false,
        reason: "offering_unavailable",
        offeringId,
        skip: built.skip,
      };
    }
    sources.push(built.source);
  }
  return { ok: true, sources };
}

/**
 * Resolve an agent's model requirements against the tenant catalog into an
 * ordered `InferenceSource[]` for the harness.
 *
 * For each requirement, the tenant-visible offerings for the named model are
 * filtered by the required capabilities, ordered by catalog priority, then
 * reordered by the creator preference and finally the invoker preference.
 * Each surviving offering is resolved to a credential-backed source; offerings
 * that cannot produce one are skipped. A required model that yields no source
 * makes the agent unlaunchable.
 *
 * A credential-backed source is emitted only when the launching tenant owns
 * the referenced credential within its hierarchy (ownership is the authority);
 * otherwise the offering is skipped (`credential_unauthorized`).
 */
export async function resolveModelSources(
  db: DB["db"],
  tenantId: string,
  requirements: ModelRequirement[],
  credentialCipher: CredentialCipher, // decrypts each secret before the projection drops it
  opts?: {
    invokerPreferences?: Record<string, ProviderPreference>;
  },
): Promise<CatalogSourceResolution> {
  if (requirements.length === 0) {
    return { ok: false, reason: "no_requirements" };
  }

  const visible = await listVisibleOfferings(db, tenantId);
  const sources: InferenceSource[] = [];
  // Dedupe material by credentialId across every requirement's chain.
  const materials = new Map<string, CredentialMaterialEntry>();

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
    const modelMaterials: CredentialMaterialEntry[] = [];
    for (const candidate of candidates) {
      const built = await buildSource(
        db,
        tenantId,
        candidate,
        credentialCipher,
      );
      if (built.ok) {
        modelSources.push(built.source);
        modelMaterials.push(built.material);
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
    for (const material of modelMaterials) {
      if (!materials.has(material.credentialId)) {
        materials.set(material.credentialId, material);
      }
    }
  }

  return { ok: true, sources, materials: [...materials.values()] };
}

/**
 * Resolve an agent's model requirements to the credential-free
 * `{ provider, model }` inference preferences a folded workflow definition
 * carries on its step agent. Reuses `resolveModelSources` and projects its
 * ordered sources to their identity, dropping the credentials: a definition's
 * inference preference is hash-only, and the credential-bearing sources are
 * supplied per deploy on the workflow path.
 *
 * A requirement set that resolves to no source is a hard failure: a folded
 * agent whose model is unresolvable is undeployable, so this raises rather
 * than synthesizing an empty preference list. It also raises when the resolved
 * sources are not injective on `(provider, model)` -- the projection would
 * otherwise freeze an ambiguous preference list whose lost `offering.id`
 * distinctions cannot be recovered after the agent's columns are dropped.
 */
export async function resolveInferencePreferences(
  db: DB["db"],
  tenantId: string,
  requirements: ModelRequirement[],
  credentialCipher: CredentialCipher, // decrypts each secret before the projection drops it
): Promise<{ provider: string; model: string }[]> {
  const resolution = await resolveModelSources(
    db,
    tenantId,
    requirements,
    credentialCipher,
  );
  if (!resolution.ok) {
    throw new Error(
      `cannot resolve inference preferences for the folded definition: ${resolution.reason}`,
    );
  }

  // The projection to `{ provider (plugin), model }` drops the `offering.id`
  // that keys each resolved source. Two distinct offerings can share a provider
  // plugin and canonical model, so the projection is only well-defined when the
  // resolved sources are injective on `(provider, model)`. If they collapse, the
  // folded definition's inference preferences cannot distinguish which offering
  // each entry meant, and once the agent's columns are dropped that distinction
  // is unrecoverable. Refuse to synthesize an ambiguous preference list.
  const preferences = resolution.sources.map((source) => ({
    provider: source.provider,
    model: source.model,
  }));
  const seen = new Set<string>();
  for (const preference of preferences) {
    // Join on a NUL: it cannot appear in a provider plugin or canonical model
    // name, so the composite key is collision-free.
    const key = `${preference.provider}\u0000${preference.model}`;
    if (seen.has(key)) {
      throw new Error(
        `cannot resolve inference preferences for the folded definition: ` +
          `the model resolution is not injective on (provider, model) -- ` +
          `two offerings collapse to (${preference.provider}, ${preference.model}); ` +
          `a folded agent requires a distinguishable inference preference per source`,
      );
    }
    seen.add(key);
  }
  return preferences;
}

/**
 * Resolve the ordered sources for a running instance from persisted state: the
 * definition's model requirements and the invoker's launch-time preferences
 * stored on the instance row. The credential-rotation and catalog-edit source
 * push resolves through this, so a running instance's source list is a pure
 * function of persisted state.
 */
export async function resolveInstanceModelSources(
  db: DB["db"],
  tenantId: string,
  instance: { definitionId: string; modelPreferences: unknown },
  credentialCipher: CredentialCipher,
): Promise<CatalogSourceResolution> {
  // Resolve from the run's own definition by primary key, scoped to the
  // resolving tenant: a rotation or catalog edit reproduces the launch's model
  // resolution rather than drifting, and a definition in another tenant
  // resolves to nothing.
  const definitionRow = await db.query.workflowDefinition.findFirst({
    where: and(
      eq(workflowDefinition.id, instance.definitionId),
      eq(workflowDefinition.tenantId, tenantId),
    ),
  });
  // A credential-backed source is authorized by tenant ownership of the
  // credential (walked in `buildSource`), not by the definition's creator, so a
  // definition with no creator can still launch a tenant-owned inference
  // source. Only a missing definition (or one in another tenant) fails closed.
  if (definitionRow === undefined) {
    return { ok: false, reason: "no_requirements" };
  }

  const requirements =
    definitionRow.modelRequirements !== null
      ? ModelRequirements.assert(definitionRow.modelRequirements)
      : [];

  const preferences =
    instance.modelPreferences !== null
      ? InvokerModelPreferences.assert(instance.modelPreferences)
      : [];
  const invokerPreferences: Record<string, ProviderPreference> = {};
  for (const preference of preferences) {
    invokerPreferences[preference.model] = preference.providers;
  }

  return resolveModelSources(db, tenantId, requirements, credentialCipher, {
    invokerPreferences,
  });
}
