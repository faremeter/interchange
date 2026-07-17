// Fixture seeding helpers for the real-database resolution tests.
//
// These insert rows through the real drizzle client, so they honor the
// schema's NOT NULL / FK / unique / CHECK constraints — the things the
// old query-introspection mocks silently bypassed. Helpers fill
// required columns the resolvers never read with deterministic values
// derived from the row id, so a test only specifies the fields its
// assertions actually depend on.

import type { DB } from "@intx/db";
import {
  agent,
  agentInstance,
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
  workflowDeployment,
} from "@intx/db/schema";

type Db = DB["db"];

export type SeedTenant = {
  id: string;
  parentId?: string | null;
};

/**
 * Insert a set of tenants honoring the immediate self-referential
 * `parent_id` FK: a row is inserted only once its parent already
 * exists, so callers can pass a tree in any order. `slug` and `domain`
 * are derived from the id to satisfy their NOT NULL + UNIQUE
 * constraints.
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
      await db.insert(tenant).values({
        id: t.id,
        name: t.id,
        slug: t.id,
        domain: `${t.id}.example.test`,
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
};

export async function seedAsset(db: Db, a: SeedAsset): Promise<void> {
  await db.insert(asset).values({
    id: a.id,
    tenantId: a.tenantId,
    kind: a.kind,
    name: a.name,
    displayName: a.displayName ?? null,
    creatorPrincipalId: null,
  });
}

export type SeedWorkflowDeployment = {
  id: string;
  tenantId: string;
  definitionAssetId: string;
  address?: string;
  publicKey?: string | null;
  status?: "deployed" | "error";
};

export async function seedWorkflowDeployment(
  db: Db,
  d: SeedWorkflowDeployment,
): Promise<void> {
  await db.insert(workflowDeployment).values({
    id: d.id,
    tenantId: d.tenantId,
    definitionAssetId: d.definitionAssetId,
    address: d.address ?? `ins_${d.id}@example.test`,
    publicKey: d.publicKey ?? null,
    status: d.status ?? "deployed",
  });
}

export type SeedPrincipal = {
  id: string;
  tenantId: string;
  kind?: "user" | "agent";
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

export type SeedProvider = {
  id: string;
  tenantId: string;
  name: string;
  plugin?: string;
};

export async function seedProvider(db: Db, p: SeedProvider): Promise<void> {
  await db.insert(provider).values({
    id: p.id,
    tenantId: p.tenantId,
    name: p.name,
    plugin: p.plugin ?? "test-plugin",
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
    disabled: o.disabled ?? false,
  });
}

export type SeedAgent = {
  id: string;
  tenantId: string;
  creatorPrincipalId: string;
  name?: string;
  modelRequirements?: unknown;
};

export async function seedAgent(db: Db, a: SeedAgent): Promise<void> {
  await db.insert(agent).values({
    id: a.id,
    tenantId: a.tenantId,
    creatorPrincipalId: a.creatorPrincipalId,
    name: a.name ?? a.id,
    modelRequirements: a.modelRequirements ?? null,
  });
}

export type SeedAgentInstance = {
  id: string;
  tenantId: string;
  agentId: string;
  principalId: string;
  address?: string;
};

export async function seedAgentInstance(
  db: Db,
  i: SeedAgentInstance,
): Promise<void> {
  await db.insert(agentInstance).values({
    id: i.id,
    tenantId: i.tenantId,
    agentId: i.agentId,
    principalId: i.principalId,
    address: i.address ?? `${i.id}.agent.test`,
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
