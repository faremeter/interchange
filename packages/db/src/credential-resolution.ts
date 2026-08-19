import { eq, and, isNull } from "drizzle-orm";

import { toolConsumer } from "@intx/authz";
import { credentialAad } from "@intx/types";
import type { CredentialBinding, CredentialCipher } from "@intx/types";
import type {
  CredentialBindingDescriptor,
  CredentialDelivery,
  CredentialMaterialEntry,
} from "@intx/types/sidecar";

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
 * Resolve a credential USABLE by the launching tenant purely through ownership:
 * it exists, is reachable in the tenant's ancestor chain, and is tenant-owned
 * (`principalId IS NULL`). Returns the row -- the proof of authority -- or null.
 *
 * This is the one "usable by ownership" resolver: a tenant may use what it (or
 * an ancestor) owns, and descendants inherit; a principal-owned credential is
 * never usable this way, its delegation flowing from its owner instead. It
 * wraps `resolveCredentialById` (the ancestor-chain check) with the tenant-owned
 * gate, so callers do not re-implement the predicate. `buildSource` expands it
 * inline only to distinguish an unresolved reference from a principal-owned one.
 */
export async function resolveTenantOwnedCredentialById(
  db: DB["db"],
  tenantId: string,
  credentialId: string,
) {
  const row = await resolveCredentialById(db, tenantId, credentialId);
  return row !== null && row.principalId === null ? row : null;
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
 * tool and integration credentials (inference sources resolve through the
 * catalog, not this path).
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
  binding: { provider: string; package: string; handle: string };
  message: string;
};

/**
 * Outcome of `buildCredentialDelivery`. `ok: true` carries the material +
 * descriptors delivered to the tools; `delivery` is `undefined` when there are
 * no bindings. `ok: false` carries the first launch-blocking configuration
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
 * descriptors delivered to its tools. Each credential's secret is decrypted
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
    const context = {
      provider: binding.provider,
      package: binding.package,
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
          message: `Ambiguous credential for the binding on provider ${binding.provider} (package ${binding.package}, handle ${binding.handle}): ${e.message}`,
        },
      };
    }

    if (resolved === null) {
      return {
        ok: false,
        reason: {
          code: "unresolved",
          binding: context,
          message: `No credential resolves the binding for provider ${binding.provider} (package ${binding.package}, handle ${binding.handle})`,
        },
      };
    }

    // The credential is delivered as an origin-pinned http handle, so its
    // provider must declare an API origin. A provider without one (OAuth-login
    // only) cannot back a tool credential; fail closed rather than deliver an
    // un-pinnable secret.
    const providerOrigin = resolved.provider.apiBaseUrl;
    if (providerOrigin === null || providerOrigin === "") {
      return {
        ok: false,
        reason: {
          code: "no_origin",
          binding: context,
          message: `Provider ${binding.provider} has no API base URL; cannot deliver an origin-pinned credential (package ${binding.package}, handle ${binding.handle})`,
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
      consumer: toolConsumer(binding.package),
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
