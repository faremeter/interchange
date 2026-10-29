import { eq, and, inArray, isNull } from "drizzle-orm";

import { credentialBindingConsumer } from "@intx/authz";
import { credentialAad } from "@intx/types";
import type { CredentialBinding, CredentialCipher } from "@intx/types";
import type {
  CredentialBindingDescriptor,
  CredentialDelivery,
  CredentialMaterialEntry,
} from "@intx/types/credential-delivery";

import type { DB } from "./client";
import { credential } from "./schema/credentials";
import { oauthClient } from "./schema/oauth-clients";
import { provider } from "./schema/providers";
import { getAncestorChain } from "./tenant-hierarchy";

/**
 * Thrown by `resolveCredentialRequirement` when more than one credential
 * matches a requirement and no name disambiguates them. A distinct type so
 * callers can catch *this* condition (a launch-blocking configuration error)
 * without also swallowing the DB reads the resolver performs first -- an
 * infrastructure failure must surface, not be mislabeled as ambiguity.
 */
export class AmbiguousCredentialError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AmbiguousCredentialError";
  }
}

/**
 * Thrown when a referenced credential is not a tenant-owned credential the given
 * tenant can use -- it does not exist, is not reachable in the tenant's ancestor
 * chain, or is principal-owned. A distinct type so a caller can map a client's
 * bad credential reference to a 4xx (a launch-blocking configuration error),
 * separate from the infrastructure faults the resolvers otherwise surface.
 */
export class CredentialUnauthorizedError extends Error {
  readonly credentialId: string;
  readonly tenantId: string;
  constructor(credentialId: string, tenantId: string) {
    super(
      `credential ${credentialId} is not a tenant-owned credential usable by tenant ${tenantId}`,
    );
    this.name = "CredentialUnauthorizedError";
    this.credentialId = credentialId;
    this.tenantId = tenantId;
  }
}

type InactiveCredentialStatus = Exclude<
  (typeof credential.$inferSelect)["status"],
  "active"
>;

/**
 * Thrown when a referenced credential is a tenant-owned credential the given
 * tenant can use, but its status is not `active`, so its secret must not be
 * delivered. Distinct from `CredentialUnauthorizedError` so the operator sees a
 * lifecycle problem (a revoked, expired or errored key), not a missing or
 * foreign one.
 */
export class CredentialInactiveError extends Error {
  readonly credentialId: string;
  readonly status: InactiveCredentialStatus;
  constructor(credentialId: string, status: InactiveCredentialStatus) {
    super(`credential ${credentialId} has status ${status}, not active`);
    this.name = "CredentialInactiveError";
    this.credentialId = credentialId;
    this.status = status;
  }
}

/**
 * Resolve a credential USABLE by the launching tenant purely through ownership:
 * it exists, is reachable in the tenant's ancestor chain, and is tenant-owned
 * (`principalId IS NULL`). Returns the row -- the proof of authority -- or null.
 *
 * This is the one "usable by ownership" resolver: a tenant may use what it (or
 * an ancestor) owns, and descendants inherit; a principal-owned credential is
 * never usable this way, its delegation flowing from its owner instead. It
 * wraps `resolveCredentialById` (the ancestor-chain check) with the tenant-owned
 * gate, so callers do not re-implement the predicate. It does not consult the
 * credential's status: a caller that only checks a reference (a catalog provider
 * pointing at a credential) may name a credential that is not active yet. The
 * rails that deliver a secret go through `resolveDeliverableCredential`, which
 * adds the lifecycle rule.
 */
export async function resolveTenantOwnedCredentialById(
  db: DB["db"],
  tenantId: string,
  credentialId: string,
) {
  const row = await resolveCredentialById(db, tenantId, credentialId);
  return row !== null && row.principalId === null ? row : null;
}

export type DeliverableCredentialResolution =
  | { ok: true; credential: typeof credential.$inferSelect }
  | { ok: false; reason: "unresolved" | "unauthorized" }
  | { ok: false; reason: "inactive"; status: InactiveCredentialStatus };

/**
 * Resolve a credential whose secret may be DELIVERED to a run as inference
 * material. Authority is ownership within the tenant hierarchy, the same rule
 * the credential-binding path resolves (`source: "tenant"`): the credential must
 * exist and be reachable in the tenant's ancestor chain (`unresolved`
 * otherwise), and be tenant-owned (`unauthorized` for a principal-owned one,
 * which is not usable through a shared catalog provider). No personal grant and
 * no role grant is consulted. Delivery also requires `status = "active"`, the
 * binding path's lifecycle rule, so a revoked, expired or errored credential is
 * never delivered (`inactive`).
 *
 * Every delivery rail calls this live -- launch, deploy, and each rotation or
 * catalog-edit re-push -- so a catalog edit that adds an eligible credential is
 * picked up without re-materializing any grant, and a re-push after a revoke
 * cannot deliver the revoked secret again.
 */
export async function resolveDeliverableCredential(
  db: DB["db"],
  tenantId: string,
  credentialId: string,
): Promise<DeliverableCredentialResolution> {
  const row = await resolveCredentialById(db, tenantId, credentialId);
  if (row === null) return { ok: false, reason: "unresolved" };
  if (row.principalId !== null) return { ok: false, reason: "unauthorized" };
  if (row.status !== "active") {
    return { ok: false, reason: "inactive", status: row.status };
  }
  return { ok: true, credential: row };
}

/**
 * Resolve a set of inference-source credentialIds into the credential material
 * delivered on a run's unified credential-material cell. An inference source
 * references its credential by id only; for each DISTINCT id this resolves the
 * secret under the SAME authority `buildSource` uses
 * (`resolveDeliverableCredential`) -- the credential must exist, be reachable in
 * the tenant's ancestor chain, be tenant-owned (`principalId IS NULL`), and be
 * active -- then decrypts it at the single point of use. `providerKey`/`origin`
 * come from the credential's own provider row (`provider.plugin` /
 * `provider.apiBaseUrl`), so the material describes the credential itself,
 * independent of any caller-supplied source fields.
 *
 * Fails CLOSED, by throwing, on the first credentialId that is unresolved, not
 * tenant-owned, not active (`CredentialInactiveError`), references a missing
 * provider, or whose provider has no API origin to pin -- a secret is never
 * dropped nor delivered without an origin. The single resolver for every
 * inference source credentialId -> material, shared by the deploy composition
 * (top-level + inline body sources) and any other caller that must materialize
 * inference credentials for the cell.
 */
export async function resolveInferenceMaterials(
  db: DB["db"],
  tenantId: string,
  credentialIds: Iterable<string>,
  credentialCipher: CredentialCipher,
): Promise<CredentialMaterialEntry[]> {
  const materials = new Map<string, CredentialMaterialEntry>();
  for (const credentialId of credentialIds) {
    if (materials.has(credentialId)) continue;
    const resolution = await resolveDeliverableCredential(
      db,
      tenantId,
      credentialId,
    );
    if (!resolution.ok) {
      if (resolution.reason === "inactive") {
        throw new CredentialInactiveError(credentialId, resolution.status);
      }
      throw new CredentialUnauthorizedError(credentialId, tenantId);
    }
    const row = resolution.credential;
    const providerRow = await db.query.provider.findFirst({
      where: eq(provider.id, row.providerId),
    });
    if (providerRow === undefined) {
      throw new Error(
        `credential ${credentialId} references provider ${row.providerId}, which does not exist`,
      );
    }
    if (providerRow.apiBaseUrl === null || providerRow.apiBaseUrl === "") {
      throw new Error(
        `provider ${providerRow.name} backing credential ${credentialId} has no API base URL; cannot pin an origin for its material`,
      );
    }
    materials.set(credentialId, {
      credentialId: row.id,
      providerKey: providerRow.plugin,
      origin: providerRow.apiBaseUrl,
      secret: await credentialCipher.decrypt(
        row.secret,
        credentialAad(row.id, "secret"),
      ),
      secretVersion: row.secretVersion,
    });
  }
  return [...materials.values()];
}

// A delivered credential stays LIVE until its row is gone or deliberately
// revoked; otherwise its material must leave the sidecar. The one lifecycle
// rule for eviction, shared by the reconnect re-resolve and the post-send
// re-check.
function isLiveCredential<
  T extends { status: (typeof credential.$inferSelect)["status"] },
>(row: T | undefined): row is T {
  return row !== undefined && row.status !== "revoked";
}

/**
 * Return the ids among `credentialIds` whose credential is removed: deleted or
 * `revoked`. A caller that already delivered these ids sends a revoke for the
 * result. It reads only id and status, so it never decrypts and never fails on
 * a misconfigured provider -- a removal must not be blocked by an unrelated
 * fault in a credential that is still alive.
 */
export async function findRemovedCredentialIds(
  db: DB["db"],
  credentialIds: Iterable<string>,
): Promise<string[]> {
  const ids = [...new Set(credentialIds)];
  if (ids.length === 0) return [];
  const rows = await db
    .select({ id: credential.id, status: credential.status })
    .from(credential)
    .where(inArray(credential.id, ids));
  const byId = new Map(rows.map((row) => [row.id, row]));
  return ids.filter((id) => !isLiveCredential(byId.get(id)));
}

/**
 * Return the ids among `delivered` whose live credential holds a higher
 * `secretVersion` than the one delivered: rotated since the delivery read it.
 * A delivery with no version counts as version 0. Like
 * `findRemovedCredentialIds`, it reads no secret and never decrypts.
 */
export async function findRotatedCredentialIds(
  db: DB["db"],
  delivered: readonly Pick<
    CredentialMaterialEntry,
    "credentialId" | "secretVersion"
  >[],
): Promise<string[]> {
  if (delivered.length === 0) return [];
  const rows = await db
    .select({
      id: credential.id,
      status: credential.status,
      secretVersion: credential.secretVersion,
    })
    .from(credential)
    .where(
      inArray(
        credential.id,
        delivered.map((material) => material.credentialId),
      ),
    );
  const byId = new Map(rows.map((row) => [row.id, row]));
  const rotated = new Set<string>();
  for (const material of delivered) {
    const row = byId.get(material.credentialId);
    if (
      isLiveCredential(row) &&
      row.secretVersion > (material.secretVersion ?? 0)
    ) {
      rotated.add(material.credentialId);
    }
  }
  return [...rotated];
}

/**
 * Re-resolve the CURRENT material for a set of already-authorized credentialIds,
 * for the reconnect resync. Unlike `resolveInferenceMaterials`, a credential
 * that no longer exists or is `revoked` is DROPPED (omitted) rather than
 * throwing: the resync reflects credential lifecycle, so a dead id is simply
 * absent from the reconciled delivery and the child evicts it.
 *
 * A credential that IS alive but whose material cannot be resolved (its provider
 * vanished or has no API base URL) is NOT a lifecycle removal, so it throws --
 * the caller aborts the whole reconcile rather than delivering a partial set
 * paired with a spurious revoke. A rotated secret is picked up because the row's
 * current secret is decrypted here.
 *
 * Ids are looked up by primary key (globally unique) and come from the
 * deployment's own persisted delivery, so no re-authorization is performed --
 * this reflects lifecycle only.
 */
export async function reresolveCurrentMaterials(
  db: DB["db"],
  credentialIds: Iterable<string>,
  credentialCipher: CredentialCipher,
): Promise<CredentialMaterialEntry[]> {
  const materials = new Map<string, CredentialMaterialEntry>();
  for (const credentialId of credentialIds) {
    if (materials.has(credentialId)) continue;
    const row = await db.query.credential.findFirst({
      where: eq(credential.id, credentialId),
    });
    if (!isLiveCredential(row)) continue;
    const providerRow = await db.query.provider.findFirst({
      where: eq(provider.id, row.providerId),
    });
    if (providerRow === undefined) {
      throw new Error(
        `credential ${credentialId} references provider ${row.providerId}, which does not exist`,
      );
    }
    if (providerRow.apiBaseUrl === null || providerRow.apiBaseUrl === "") {
      throw new Error(
        `provider ${providerRow.name} backing credential ${credentialId} has no API base URL; cannot pin an origin for its material`,
      );
    }
    materials.set(credentialId, {
      credentialId: row.id,
      providerKey: providerRow.plugin,
      origin: providerRow.apiBaseUrl,
      secret: await credentialCipher.decrypt(
        row.secret,
        credentialAad(row.id, "secret"),
      ),
      secretVersion: row.secretVersion,
    });
  }
  return [...materials.values()];
}

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
 * Used at agent launch time by the control plane to satisfy a definition's
 * credential bindings (inference sources resolve through the catalog, not this
 * path).
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
    // Return the resolved provider alongside the credential. The provider was
    // already fetched above to constrain the credential query, so surfacing it
    // spares the caller a second lookup for the provider facts (plugin, base
    // URL) a resolved credential is always paired with.
    if (matching.length === 1 && sole) {
      return { credential: sole, provider: resolvedProvider };
    }
    if (matching.length > 1) {
      throw new AmbiguousCredentialError(
        `Ambiguous credential match: ${matching.length} credentials match ` +
          `provider=${requirement.providerName} source=${requirement.source} ` +
          `in tenant ${tid}. Specify a name to disambiguate.`,
      );
    }
  }

  return null;
}

/**
 * A launch-blocking CONFIGURATION failure for one binding: a binding no
 * credential resolves, a provider with no API origin (can't pin an http
 * handle), or an ambiguous match. Distinct from an infrastructure failure (a DB
 * read fault), which `buildCredentialDelivery` THROWS rather than returning --
 * so a caller never mistakes a transient fault for revocation.
 */
export type CredentialDeliveryFailure = {
  code: "unresolved" | "no_origin" | "ambiguous";
  binding: { provider: string; consumer: string; handle: string };
  message: string;
};

/**
 * Outcome of `buildCredentialDelivery`. `ok: true` carries the material +
 * descriptors delivered to its consumers; `delivery` is `undefined` when there
 * are no bindings. `ok: false` carries the first launch-blocking configuration
 * failure for the caller to map to a fail-closed response.
 */
export type BuildCredentialDeliveryResult =
  | {
      ok: true;
      delivery: CredentialDelivery | undefined;
    }
  | { ok: false; reason: CredentialDeliveryFailure };

/**
 * Resolve a definition's credential bindings into the material + per-handle
 * descriptors delivered to its consumers. Each credential's secret is decrypted
 * once, keyed by credentialId (a credential backing several handles is
 * decrypted once).
 *
 * This path delivers credential material only; it does not mint, stamp, or
 * carry any grant. Credential-use authorization is enforced by a separate grant
 * layer.
 *
 * The workflow-deploy path (`deployCodeSourcedWorkflow`) is the only caller
 * today, using `delivery`. Returning a discriminated result rather than an HTTP
 * response keeps it safe to call off the request path: a configuration failure
 * is `ok: false`, and a transient DB read fault THROWS so the caller surfaces it
 * rather than silently dropping a still-valid credential.
 */
export async function buildCredentialDelivery(args: {
  db: DB["db"];
  tenantId: string;
  bindings: readonly CredentialBinding[];
  creatorPrincipalId: string | null;
  invokerPrincipalId: string | null;
  credentialCipher: CredentialCipher;
}): Promise<BuildCredentialDeliveryResult> {
  const materials = new Map<string, CredentialMaterialEntry>();
  const descriptors: CredentialBindingDescriptor[] = [];

  for (const binding of args.bindings) {
    const consumer = credentialBindingConsumer(binding);
    const context = {
      provider: binding.provider,
      consumer,
      handle: binding.handle,
    };

    let resolved: Awaited<ReturnType<typeof resolveCredentialRequirement>>;
    try {
      resolved = await resolveCredentialRequirement(
        args.db,
        args.tenantId,
        {
          providerName: binding.provider,
          source: binding.locator,
          ...(binding.name !== undefined ? { name: binding.name } : {}),
        },
        args.creatorPrincipalId,
        args.invokerPrincipalId,
      );
    } catch (e) {
      // AmbiguousCredentialError is a launch-blocking config failure; any other
      // throw is a DB read fault the resolver performs first -- surface it, do
      // not mislabel it as revocation.
      if (!(e instanceof AmbiguousCredentialError)) throw e;
      return {
        ok: false,
        reason: {
          code: "ambiguous",
          binding: context,
          message: `Ambiguous credential for the binding on provider ${binding.provider} (consumer ${consumer}, handle ${binding.handle}): ${e.message}`,
        },
      };
    }

    if (resolved === null) {
      return {
        ok: false,
        reason: {
          code: "unresolved",
          binding: context,
          message: `No credential resolves the binding for provider ${binding.provider} (consumer ${consumer}, handle ${binding.handle})`,
        },
      };
    }

    // The credential is delivered as an origin-pinned http handle, so its
    // provider must declare an API origin. A provider without one (OAuth-login
    // only) cannot back a credential binding; fail closed rather than deliver
    // an un-pinnable secret.
    const providerOrigin = resolved.provider.apiBaseUrl;
    if (providerOrigin === null || providerOrigin === "") {
      return {
        ok: false,
        reason: {
          code: "no_origin",
          binding: context,
          message: `Provider ${binding.provider} has no API base URL; cannot deliver an origin-pinned credential (consumer ${consumer}, handle ${binding.handle})`,
        },
      };
    }

    const credentialId = resolved.credential.id;
    // A binding resolves only a tenant-owned credential (the `tenant` locator
    // filters `principalId IS NULL` in resolveCredentialRequirement), so its use
    // is authorized by ownership -- already proven by this walk-up resolution,
    // not re-checked here. This path delivers credential material only; it does
    // not mint or carry any grant. Credential-use authorization is enforced by a
    // separate grant layer.
    descriptors.push({
      handle: binding.handle,
      credentialId,
      consumer,
    });
    if (!materials.has(credentialId)) {
      // Decrypt at the single point of use. A decrypt failure propagates and
      // fails the caller closed; there is no placeholder secret.
      materials.set(credentialId, {
        credentialId,
        providerKey: resolved.provider.plugin,
        origin: providerOrigin,
        secret: await args.credentialCipher.decrypt(
          resolved.credential.secret,
          credentialAad(credentialId, "secret"),
        ),
        secretVersion: resolved.credential.secretVersion,
      });
    }
  }

  return {
    ok: true,
    delivery:
      descriptors.length > 0
        ? { bindings: descriptors, materials: [...materials.values()] }
        : undefined,
  };
}
