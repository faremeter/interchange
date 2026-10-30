import {
  credentialAad,
  type CredentialCipher,
  type ModelRequirement,
  type ProviderPreference,
} from "@intx/types";
import type { InferenceSource } from "@intx/types/runtime";
import type { CredentialMaterialEntry } from "@intx/types/credential-delivery";

import {
  listVisibleOfferings,
  type ResolvedOffering,
} from "./catalog-resolution";
import type { DB } from "./client";
import {
  resolveDeliverableCredential,
  type DeliverableCredentialResolution,
} from "./credential-resolution";
import { parseModelOfferingRow } from "./parse-row";

/**
 * Why a single offering could not be turned into a launchable source.
 * `wallet_backed` providers are storable in the catalog but not launchable
 * in this credential-backed-only release.
 */
export type SourceSkip =
  | { reason: "wallet_backed"; provider: string }
  | { reason: "credential_unresolved"; provider: string }
  | { reason: "credential_unauthorized"; provider: string }
  | { reason: "credential_inactive"; provider: string }
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
  | {
      ok: true;
      sources: InferenceSource[];
      // The credential material backing the resolved sources, deduped by
      // `credentialId`. A source references its credential by id only; the
      // secret rides here so the caller can seal it into the run's unified
      // credential-material cell.
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

const CREDENTIAL_SKIP_REASONS = {
  unresolved: "credential_unresolved",
  unauthorized: "credential_unauthorized",
  inactive: "credential_inactive",
} as const satisfies Record<
  Exclude<DeliverableCredentialResolution, { ok: true }>["reason"],
  SourceSkip["reason"]
>;

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

  // Resolve the secret through the delivery resolver so a provider row
  // referencing a credential outside the tenant's ancestor chain, a
  // principal-owned credential, or a credential that is not active cannot leak
  // its secret. Each refusal is a distinct skip so the operator sees why.
  const resolution = await resolveDeliverableCredential(
    db,
    tenantId,
    provider.credentialId,
  );
  if (!resolution.ok) {
    return {
      ok: false,
      skip: {
        reason: CREDENTIAL_SKIP_REASONS[resolution.reason],
        provider: provider.name,
      },
    };
  }
  const credential = resolution.credential;

  // Validate the row once at this DB-to-runtime boundary. This narrows the
  // jsonb `quirks` from `unknown` to a `Record | null` and checks
  // `capabilities` against the curated enum. `quirks` is spread in only when
  // non-null so a source with no accommodations omits the key entirely
  // (InferenceSource.quirks is present-or-absent, never null). The capability
  // filter in resolveModelSources still reads the raw row's `capabilities`
  // for its `.includes` check; that read-only comparison cannot be corrupted
  // into a wrong routing decision, so it is left as-is.
  // Decrypt the stored secret at the one point it is used. Strict: a
  // non-ciphertext value (a row not yet re-keyed, or a write that failed to
  // encrypt) throws rather than delivering a bad key -- fail closed. The secret
  // rides the credential-material cell (keyed by `credentialId`), NOT the source
  // config -- the source only references the credential by id, so no secret is
  // inline in the pinned/persisted source.
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
      secretVersion: credential.secretVersion,
    },
  };
}

/**
 * Rebuild an exact, ordered source chain from durable catalog offering ids.
 * This is the recovery counterpart to launch-time source selection: it keeps
 * secrets out of persisted launch specs and rechecks tenant visibility and
 * credential ownership on every launch.
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
 * A credential-backed source is emitted only when the launching tenant owns
 * the referenced credential within its hierarchy (ownership is the authority)
 * and the credential is active; otherwise the offering is skipped
 * (`credential_unresolved`, `credential_unauthorized` or `credential_inactive`)
 * and its secret is withheld.
 */
export async function resolveModelSources(
  db: DB["db"],
  tenantId: string,
  requirements: ModelRequirement[],
  // Decrypts each resolved credential secret at its point of use in
  // `buildSource`. Required: the edge (the launch route / rotation push /
  // allocation service) owns the cipher and always supplies a real one; the
  // noop fallback for a keyless composition is resolved once at that edge
  // (`resolveCredentialCipher` in the hub app), never defaulted here.
  credentialCipher: CredentialCipher,
  opts?: {
    invokerPreferences?: Record<string, ProviderPreference>;
  },
): Promise<CatalogSourceResolution> {
  if (requirements.length === 0) {
    return { ok: false, reason: "no_requirements" };
  }

  const visible = await listVisibleOfferings(db, tenantId);
  const sources: InferenceSource[] = [];
  // Dedupe material by credentialId across every requirement's chain: one
  // credential backing several offerings is delivered once.
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
 * carries on its step agent. Reuses `resolveModelSources` -- the same catalog
 * + creator-grant resolution the instance launch path runs -- and projects its
 * ordered sources to their `{ provider (plugin), model }` identity, dropping
 * the credentials: a definition's inference preference is hash-only, and the
 * credential-bearing sources are supplied per deploy on the workflow path.
 *
 * A requirement set that resolves to no source is a hard failure. A folded
 * agent whose model is unresolvable is undeployable, so this raises rather
 * than synthesizing an empty preference list that would silently strip the
 * agent's inference. It also raises when the resolved sources are not
 * injective on `(provider, model)` -- the projection would otherwise freeze
 * an ambiguous preference list whose lost `offering.id` distinctions cannot be
 * recovered after the agent's columns are dropped.
 */
export async function resolveInferencePreferences(
  db: DB["db"],
  tenantId: string,
  requirements: ModelRequirement[],
  // Required even though the result discards the credential: this reuses the
  // full `resolveModelSources` path, which decrypts each secret at its point of
  // use in `buildSource` before the `{provider, model}` projection drops it. So
  // the caller must still supply the edge's real cipher; a noop here would throw
  // the moment a tenant's secret is stored encrypted.
  credentialCipher: CredentialCipher,
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
  // that keys each resolved source. Two distinct offerings can share a
  // provider plugin and canonical model -- the catalog permits two
  // `model_provider` rows on the same plugin (uniqueness is on the provider
  // name), each offering the same model -- so the projection is only
  // well-defined when the resolved sources are injective on `(provider,
  // model)`. If they collapse, the folded definition's inference preferences
  // cannot distinguish which offering each entry meant, and once the agent's
  // columns are dropped that distinction is unrecoverable. Refuse to
  // synthesize an ambiguous preference list: fail loud so the collapse
  // surfaces in the fold/materialize manifest rather than freezing silently.
  const preferences = resolution.sources.map((source) => ({
    provider: source.provider,
    model: source.model,
  }));
  const seen = new Set<string>();
  for (const preference of preferences) {
    // Join on a NUL: it cannot appear in a provider plugin or canonical model
    // name, so the composite key is collision-free. Written as an explicit
    // unicode escape rather than an invisible raw control byte in the source.
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
