// Fixture seeding helpers for the real-database resolution tests. Rows go
// through the real drizzle client, so the schema's NOT NULL / FK / unique /
// CHECK constraints are honored. Helpers fill columns the resolvers never
// read with deterministic values, so a test only specifies the fields its
// assertions depend on.

import type { DB } from "@intx/db";
import { createPrincipalKeyStore } from "@intx/db";
import type { WorkflowRunCredentialRefs } from "@intx/db/schema";
import { TenantSlug, type GrantWalkSnapshot } from "@intx/types";
import { createNoopCredentialCipher } from "@intx/crypto";
import {
  asset,
  credential,
  grant,
  model,
  modelOffering,
  modelProvider,
  oauthClient,
  principal,
  provider,
  tenant,
  wallet,
  workflowDefinition,
  workflowDefinitionVersion,
  workflowRun,
} from "@intx/db/schema";

type Db = DB["db"];

export type SeedTenant = {
  id: string;
  parentId?: string | null;
};

/**
 * Derive a tenant slug from a tenant id: every `_` becomes `-` (a tenant
 * id's `tnt_` prefix is not a legal DNS label character), then the result
 * is checked against `TenantSlug`, the grammar the create route admits, so
 * it cannot drift from the `tenant_slug_dns_label_check` constraint.
 * Refusing here names the seed call that supplied the id, instead of a
 * constraint violation on a column the failing test never reads. A slug
 * collision (`tnt_a_b` vs `tnt_a-b`) stays unchecked: `slug` is UNIQUE, so
 * it already surfaces as a unique violation.
 */
export function tenantSlugFromId(id: string): string {
  const slug = id.replaceAll("_", "-");
  try {
    return TenantSlug.assert(slug);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `tenantSlugFromId: id ${JSON.stringify(id)} derives the slug ` +
        `${JSON.stringify(slug)}, which is not a legal DNS label: ${reason}`,
      { cause },
    );
  }
}

/**
 * Insert a set of tenants honoring the self-referential `parent_id` FK:
 * a row is inserted only once its parent exists, so callers can pass a
 * tree in any order. `slug` derives from the id and `domain` from the
 * lowercased slug, the same chain the tenant create route builds.
 */
export async function seedTenants(
  db: Db,
  tenants: SeedTenant[],
): Promise<void> {
  let remaining = [...tenants];
  const inserted = new Set<string>();
  while (remaining.length > 0) {
    const ready = remaining.filter(
      (t) =>
        t.parentId === undefined ||
        t.parentId === null ||
        inserted.has(t.parentId),
    );
    if (ready.length === 0) {
      throw new Error(
        `seedTenants: unresolvable parent references among ${remaining
          .map((t) => t.id)
          .join(", ")}`,
      );
    }
    for (const t of ready) {
      const slug = tenantSlugFromId(t.id);
      await db.insert(tenant).values({
        id: t.id,
        name: t.id,
        slug,
        domain: `${slug.toLowerCase()}.example.test`,
        parentId: t.parentId ?? null,
      });
      inserted.add(t.id);
    }
    remaining = remaining.filter((t) => !inserted.has(t.id));
  }
}

export type SeedAsset = {
  id: string;
  tenantId: string;
  kind: string;
  name: string;
  displayName?: string | null;
  creatorPrincipalId?: string | null;
};

export async function seedAsset(db: Db, a: SeedAsset): Promise<void> {
  await db.insert(asset).values({
    id: a.id,
    tenantId: a.tenantId,
    kind: a.kind,
    name: a.name,
    displayName: a.displayName ?? null,
    creatorPrincipalId: a.creatorPrincipalId ?? null,
  });
}

export type SeedPrincipal = {
  id: string;
  tenantId: string;
  kind?: "user" | "agent" | "workflow";
  refId?: string;
  status?: "active" | "suspended" | "invited" | "deactivated";
};

export async function seedPrincipal(db: Db, p: SeedPrincipal): Promise<void> {
  await db.insert(principal).values({
    id: p.id,
    tenantId: p.tenantId,
    kind: p.kind ?? "user",
    refId: p.refId ?? p.id,
    status: p.status ?? "active",
  });
}

/**
 * Mint the active `principal_key` a real principal carries in production.
 * `seedPrincipal` bypasses the create-principal key mint, so a fixture
 * whose principal must sign (e.g. the caller of a hub-originated mail
 * trigger) mints the key here, sealed with the noop cipher the app falls
 * back to when no `PRINCIPAL_KEY_ENCRYPTION_KEY` is configured. Returns the
 * hex public key for signature verification.
 */
export async function seedPrincipalKey(
  db: Db,
  principalId: string,
): Promise<string> {
  const keyStore = createPrincipalKeyStore({
    db,
    cipher: createNoopCredentialCipher(),
  });
  return keyStore.generate(principalId);
}

export type SeedProvider = {
  id: string;
  tenantId: string;
  name: string;
  plugin?: string;
  apiBaseUrl?: string;
};

export async function seedProvider(db: Db, p: SeedProvider): Promise<void> {
  await db.insert(provider).values({
    id: p.id,
    tenantId: p.tenantId,
    name: p.name,
    plugin: p.plugin ?? "test-plugin",
    apiBaseUrl: p.apiBaseUrl ?? null,
  });
}

export type SeedOAuthClient = {
  id: string;
  tenantId: string;
  providerId: string;
  name?: string;
  clientId?: string;
  clientSecret?: string;
};

export async function seedOAuthClient(
  db: Db,
  c: SeedOAuthClient,
): Promise<void> {
  await db.insert(oauthClient).values({
    id: c.id,
    tenantId: c.tenantId,
    providerId: c.providerId,
    name: c.name ?? c.id,
    clientId: c.clientId ?? `${c.id}-client`,
    clientSecret: c.clientSecret ?? `${c.id}-secret`,
  });
}

export type SeedCredential = {
  id: string;
  tenantId: string;
  providerId: string;
  name: string;
  type?: "api_key" | "oauth_token" | "certificate" | "other";
  secret?: string;
  refreshSecret?: string | null;
  status?: "active" | "expired" | "revoked" | "error";
  principalId?: string | null;
  scopes?: string[] | null;
  oauthClientId?: string | null;
};

export async function seedCredential(db: Db, c: SeedCredential): Promise<void> {
  await db.insert(credential).values({
    id: c.id,
    tenantId: c.tenantId,
    providerId: c.providerId,
    name: c.name,
    type: c.type ?? "api_key",
    secret: c.secret ?? `${c.id}-secret`,
    refreshSecret: c.refreshSecret ?? null,
    status: c.status ?? "active",
    principalId: c.principalId ?? null,
    scopes: c.scopes ?? null,
    oauthClientId: c.oauthClientId ?? null,
  });
}

export type SeedWallet = {
  id: string;
  tenantId: string;
  name?: string;
  backendType?: "crypto" | "fiat" | "credits";
  currency?: string;
};

export async function seedWallet(db: Db, w: SeedWallet): Promise<void> {
  await db.insert(wallet).values({
    id: w.id,
    tenantId: w.tenantId,
    name: w.name ?? w.id,
    backendType: w.backendType ?? "credits",
    currency: w.currency ?? "USD",
  });
}

export type SeedModel = {
  id: string;
  tenantId: string;
  canonicalName: string;
  displayName?: string | null;
  description?: string | null;
  disabled?: boolean;
};

export async function seedModel(db: Db, m: SeedModel): Promise<void> {
  await db.insert(model).values({
    id: m.id,
    tenantId: m.tenantId,
    canonicalName: m.canonicalName,
    displayName: m.displayName ?? null,
    description: m.description ?? null,
    disabled: m.disabled ?? false,
  });
}

export type SeedModelProvider = {
  id: string;
  tenantId: string;
  name: string;
  plugin?: "anthropic" | "openai" | "openai-compatible" | "google-genai";
  baseURL?: string;
  // The schema's XOR check requires exactly one of these; callers supply one.
  credentialId?: string | null;
  walletId?: string | null;
  disabled?: boolean;
};

export async function seedModelProvider(
  db: Db,
  p: SeedModelProvider,
): Promise<void> {
  await db.insert(modelProvider).values({
    id: p.id,
    tenantId: p.tenantId,
    name: p.name,
    plugin: p.plugin ?? "anthropic",
    baseURL: p.baseURL ?? "https://api.anthropic.com",
    credentialId: p.credentialId ?? null,
    walletId: p.walletId ?? null,
    disabled: p.disabled ?? false,
  });
}

export type SeedModelOffering = {
  id: string;
  tenantId: string;
  modelId: string;
  providerId: string;
  priority?: number;
  capabilities?: string[];
  deploymentTags?: string[];
  quirks?: Record<string, unknown>;
  disabled?: boolean;
};

export async function seedModelOffering(
  db: Db,
  o: SeedModelOffering,
): Promise<void> {
  await db.insert(modelOffering).values({
    id: o.id,
    tenantId: o.tenantId,
    modelId: o.modelId,
    providerId: o.providerId,
    priority: o.priority ?? 0,
    capabilities: o.capabilities ?? [],
    deploymentTags: o.deploymentTags ?? [],
    quirks: o.quirks ?? null,
    disabled: o.disabled ?? false,
  });
}

export type SeedWorkflowRun = {
  id: string;
  tenantId: string;
  anchorRunId?: string | null;
  definitionId?: string;
  principalId?: string | null;
  address?: string | null;
  publicKey?: string | null;
  status?: "deployed" | "running" | "completed" | "failed" | "cancelled";
  credentialRefs?: WorkflowRunCredentialRefs | null;
  createdAt?: Date;
  endedAt?: Date | null;
};

// workflow_run.definition_id is NOT NULL; anchor a run that does not care
// which definition it uses on a per-tenant throwaway created once.
async function ensureSeedDefinition(db: Db, tenantId: string): Promise<string> {
  const id = `wfd_seed_${tenantId}`;
  await db
    .insert(workflowDefinition)
    .values({ id, tenantId, name: `seed-def-${tenantId}` })
    .onConflictDoNothing({ target: workflowDefinition.id });
  return id;
}

export async function seedWorkflowRun(
  db: Db,
  r: SeedWorkflowRun,
): Promise<void> {
  const definitionId =
    r.definitionId ?? (await ensureSeedDefinition(db, r.tenantId));
  await db.insert(workflowRun).values({
    id: r.id,
    tenantId: r.tenantId,
    anchorRunId: r.anchorRunId ?? null,
    definitionId,
    principalId: r.principalId ?? null,
    address: r.address ?? null,
    publicKey: r.publicKey ?? null,
    status: r.status ?? "running",
    ...(r.credentialRefs !== undefined
      ? { credentialRefs: r.credentialRefs }
      : {}),
    ...(r.createdAt !== undefined ? { createdAt: r.createdAt } : {}),
    ...(r.endedAt !== undefined ? { endedAt: r.endedAt } : {}),
  });
}

export type SeedWorkflowDefinitionVersion = {
  definitionId: string;
  id?: string;
  version?: string;
  status?: "active" | "inactive" | "failed";
  approvedWireHash?: string | null;
  grantSnapshot: GrantWalkSnapshot | null;
};

// Freeze a grant-walk snapshot onto a definition's version row the way a
// deploy-time approval does; both trigger paths read the run's grants from
// this row, keyed by (definitionId, version "1").
export async function seedWorkflowDefinitionVersion(
  db: Db,
  v: SeedWorkflowDefinitionVersion,
): Promise<void> {
  await db.insert(workflowDefinitionVersion).values({
    id: v.id ?? `wdv_${v.definitionId}`,
    definitionId: v.definitionId,
    version: v.version ?? "1",
    status: v.status ?? "active",
    approvedWireHash: v.approvedWireHash ?? "a".repeat(64),
    grantSnapshot: v.grantSnapshot,
  });
}

export type SeedGrant = {
  id: string;
  tenantId: string;
  resource: string;
  action: string;
  principalId?: string | null;
  roleId?: string | null;
  effect?: "allow" | "deny" | "ask";
  origin?: "system" | "role" | "creator" | "invoker";
  conditions?: Record<string, unknown> | null;
  expiresAt?: Date | null;
};

export async function seedGrant(db: Db, g: SeedGrant): Promise<void> {
  await db.insert(grant).values({
    id: g.id,
    tenantId: g.tenantId,
    resource: g.resource,
    action: g.action,
    principalId: g.principalId ?? null,
    roleId: g.roleId ?? null,
    effect: g.effect ?? "allow",
    origin: g.origin ?? "creator",
    conditions: g.conditions ?? null,
    expiresAt: g.expiresAt ?? null,
  });
}
