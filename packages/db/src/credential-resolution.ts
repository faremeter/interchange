import { eq, and, isNull } from "drizzle-orm";

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
 * gate, so callers do not re-implement the predicate.
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
