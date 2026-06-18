import { eq } from "drizzle-orm";

import type { DB } from "./client";
import { model, modelOffering, modelProvider } from "./schema/catalog";
import { getAncestorChain } from "./tenant-hierarchy";

export type ModelRow = typeof model.$inferSelect;
export type ModelProviderRow = typeof modelProvider.$inferSelect;
export type ModelOfferingRow = typeof modelOffering.$inferSelect;

/**
 * Where a resolved catalog entry came from. `direct` is true when the
 * row lives on the input tenant itself, false when it was inherited
 * from an ancestor.
 */
export type Origin = { tenantId: string; direct: boolean };

export type VisibleModel = { row: ModelRow; origin: Origin };
export type VisibleProvider = { row: ModelProviderRow; origin: Origin };

/**
 * A catalog offering resolved in a tenant's context. The `offering`
 * supplies priority, capabilities, and deployment tags; `model` and
 * `provider` are the entries *visible to the resolving tenant* for the
 * offering's `(canonicalName, providerName)` identity — so a child that
 * shadows a provider sees inherited offerings routed through its own
 * provider configuration, not the ancestor's.
 */
export type ResolvedOffering = {
  offering: ModelOfferingRow;
  model: ModelRow;
  provider: ModelProviderRow;
  origin: Origin;
};

/**
 * Lists the models visible to a tenant, walking the ancestor chain
 * leaf-to-root. The first row to claim a `canonicalName` wins (a
 * descendant shadows an ancestor). A winning row marked `disabled`
 * suppresses the name for the tenant and its descendants, so it is
 * omitted from the result.
 *
 * Scoping derives entirely from the ancestor chain; the denormalized
 * `tenantId` on referencing rows is never the scoping authority.
 */
export async function listVisibleModels(
  db: DB["db"],
  tenantId: string,
): Promise<VisibleModel[]> {
  const chain = await getAncestorChain(db, tenantId);
  const byKey = new Map<string, VisibleModel>();

  for (const tid of chain) {
    const rows = await db.query.model.findMany({
      where: eq(model.tenantId, tid),
    });
    for (const row of rows) {
      if (byKey.has(row.canonicalName)) continue;
      byKey.set(row.canonicalName, {
        row,
        origin: { tenantId: tid, direct: tid === tenantId },
      });
    }
  }

  return Array.from(byKey.values()).filter((entry) => !entry.row.disabled);
}

/**
 * Lists the model providers visible to a tenant. Same leaf-to-root
 * shadowing and disable-suppression as {@link listVisibleModels},
 * keyed on the provider `name`.
 */
export async function listVisibleProviders(
  db: DB["db"],
  tenantId: string,
): Promise<VisibleProvider[]> {
  const chain = await getAncestorChain(db, tenantId);
  const byKey = new Map<string, VisibleProvider>();

  for (const tid of chain) {
    const rows = await db.query.modelProvider.findMany({
      where: eq(modelProvider.tenantId, tid),
    });
    for (const row of rows) {
      if (byKey.has(row.name)) continue;
      byKey.set(row.name, {
        row,
        origin: { tenantId: tid, direct: tid === tenantId },
      });
    }
  }

  return Array.from(byKey.values()).filter((entry) => !entry.row.disabled);
}

// A NUL separator cannot collide: it never appears in a Postgres `text`
// value, so distinct (canonicalName, providerName) pairs never map to the
// same composite key even when a name contains spaces or punctuation.
const KEY_SEPARATOR = "\u0000";

function offeringKey(canonicalName: string, providerName: string): string {
  return `${canonicalName}${KEY_SEPARATOR}${providerName}`;
}

/**
 * Lists the offerings visible to a tenant.
 *
 * An offering's cross-tenant identity is `(model canonicalName, provider
 * name)`, not its row ids — a child that shadows a model or provider must
 * shadow the inherited offerings for that pairing. So each offering's
 * `modelId`/`providerId` are dereferenced to their canonical identity
 * (across every tenant in the chain, since the referents may be inherited)
 * and the chain is walked leaf-to-root with that identity as the key:
 * first wins, `disabled` suppresses.
 *
 * Visibility then cascades from the model/provider passes: an offering
 * survives only if both its model `canonicalName` and provider `name` are
 * themselves visible. Disabling a provider therefore removes every
 * inherited offering against it, even offerings the disabling tenant never
 * touched. The cascade is applied after the model and provider passes
 * complete, never interleaved.
 *
 * The returned `model`/`provider` are the entries *visible to the
 * resolving tenant* (resolved by canonicalName/name), so shadowed
 * configuration applies to inherited offerings.
 */
export async function listVisibleOfferings(
  db: DB["db"],
  tenantId: string,
): Promise<ResolvedOffering[]> {
  const chain = await getAncestorChain(db, tenantId);

  const visibleModels = new Map(
    (await listVisibleModels(db, tenantId)).map((m) => [
      m.row.canonicalName,
      m,
    ]),
  );
  const visibleProviders = new Map(
    (await listVisibleProviders(db, tenantId)).map((p) => [p.row.name, p]),
  );

  // Dereference offering referents to their canonical identity. The
  // referents may live on any tenant in the chain (an offering can be
  // inherited), so the id maps span every tenant, including rows that are
  // themselves shadowed or disabled — the maps answer "what does this id
  // name", and the cascade below decides visibility.
  const modelNameById = new Map<string, string>();
  const providerNameById = new Map<string, string>();
  for (const tid of chain) {
    for (const row of await db.query.model.findMany({
      where: eq(model.tenantId, tid),
    })) {
      modelNameById.set(row.id, row.canonicalName);
    }
    for (const row of await db.query.modelProvider.findMany({
      where: eq(modelProvider.tenantId, tid),
    })) {
      providerNameById.set(row.id, row.name);
    }
  }

  const byKey = new Map<string, { row: ModelOfferingRow; origin: Origin }>();
  for (const tid of chain) {
    const rows = await db.query.modelOffering.findMany({
      where: eq(modelOffering.tenantId, tid),
    });
    for (const row of rows) {
      const canonicalName = modelNameById.get(row.modelId);
      const providerName = providerNameById.get(row.providerId);
      if (canonicalName === undefined || providerName === undefined) continue;
      const key = offeringKey(canonicalName, providerName);
      if (byKey.has(key)) continue;
      byKey.set(key, {
        row,
        origin: { tenantId: tid, direct: tid === tenantId },
      });
    }
  }

  const resolved: ResolvedOffering[] = [];
  for (const { row, origin } of byKey.values()) {
    if (row.disabled) continue;
    const canonicalName = modelNameById.get(row.modelId);
    const providerName = providerNameById.get(row.providerId);
    if (canonicalName === undefined || providerName === undefined) continue;
    const visibleModel = visibleModels.get(canonicalName);
    const visibleProvider = visibleProviders.get(providerName);
    // Cascade: the offering survives only if its model and provider are
    // both visible (a disabled model or provider removes its offerings).
    if (visibleModel === undefined || visibleProvider === undefined) continue;
    resolved.push({
      offering: row,
      model: visibleModel.row,
      provider: visibleProvider.row,
      origin,
    });
  }

  return resolved;
}
