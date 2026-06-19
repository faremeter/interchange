import type { QueryClient } from "@tanstack/react-query";
import { queryOptions } from "@tanstack/react-query";

import { api } from "@/lib/api";

type TenantResponse = {
  id: string;
  name: string;
  slug: string;
  domain: string;
  parentId: string | null;
  config?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
};

type PrincipalResponse = {
  id: string;
  tenantId: string;
  kind: "user" | "agent";
  refId: string;
  displayName: string;
  email?: string;
  status: "active" | "suspended" | "invited" | "deactivated";
  roles: { id: string; name: string }[];
  createdAt: string;
  updatedAt: string;
};

type RoleResponse = {
  id: string;
  tenantId: string;
  name: string;
  description: string | null;
  isSystem: boolean;
  createdAt: string;
  updatedAt: string;
};

type CredentialRequirement = {
  providerName: string;
  scopes?: string[];
  source: "tenant" | "creator" | "invoker";
  name?: string;
};

type GrantRequirement = {
  resource: string;
  action: string;
  effect?: "allow" | "deny" | "ask";
  source: "tenant" | "creator" | "invoker";
  conditions?: Record<string, unknown> | null;
};

export type AgentResponse = {
  id: string;
  tenantId: string;
  creatorPrincipalId: string;
  name: string;
  description: string | null;
  systemPrompt: string | null;
  status: "deployed" | "stopped";
  currentVersion: string;
  capabilities: Record<string, unknown> | null;
  credentialRequirements?: CredentialRequirement[];
  grantRequirements?: GrantRequirement[];
  roles?: { id: string; name: string }[];
  createdAt: string;
  updatedAt: string;
};

export type AgentInstanceResponse = {
  id: string;
  agentId: string;
  agentName: string;
  tenantId: string;
  address: string;
  status: "deployed" | "running" | "updating" | "error" | "stopped";
  publicKey: string | null;
  kernelId: string | null;
  sidecarId: string | null;
  runtimeStatus?: "idle" | "busy" | "waiting_approval";
  createdAt: string;
  updatedAt: string;
  endedAt: string | null;
};

type GrantResponse = {
  id: string;
  tenantId: string;
  roleId: string | null;
  roleName: string | null;
  principalId: string | null;
  principalName: string | null;
  resource: string;
  action: string;
  effect: "allow" | "deny" | "ask";
  origin: "system" | "role" | "creator" | "invoker";
  createdAt: string;
  updatedAt: string;
};

type CredentialResponse = {
  id: string;
  tenantId: string;
  name: string;
  type: "api_key" | "oauth_token" | "certificate" | "other";
  description: string | null;
  metadata: Record<string, unknown> | null;
  createdAt: string;
  updatedAt: string;
};

type WalletResponse = {
  id: string;
  tenantId: string;
  name: string;
  backendType: "crypto" | "fiat" | "credits";
  currency: string;
  balance: string;
  config?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
};

type OfferingResponse = {
  id: string;
  agentId: string;
  agentName: string;
  tenantId: string;
  name: string;
  description: string | null;
  pricing?: {
    base?: { amount: string; currency: string };
    methods?: string[];
    negotiable?: boolean;
    bounds?: { min?: string; max?: string };
  };
  schema: Record<string, unknown> | null;
};

type ProviderResponse = {
  id: string;
  tenantId: string;
  name: string;
  plugin: string;
  scopes: string[] | null;
  createdAt: string;
  updatedAt: string;
};

export function tenantProvidersQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "providers"],
    queryFn: async () => {
      const res = await api<{ data: ProviderResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/providers?inherited=true`,
      );
      return res.data;
    },
  });
}

export function tenantDetailQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId],
    queryFn: () => api<TenantResponse>("GET", `/api/tenants/${tenantId}`),
  });
}

export function tenantPrincipalsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "principals"],
    queryFn: async () => {
      const res = await api<{ data: PrincipalResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/principals`,
      );
      return res.data;
    },
  });
}

export function tenantRolesQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "roles"],
    queryFn: async () => {
      const res = await api<{ data: RoleResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/roles`,
      );
      return res.data;
    },
  });
}

export function tenantAgentsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "agents"],
    queryFn: async () => {
      const res = await api<{ data: AgentResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/agents/definitions`,
      );
      return res.data;
    },
  });
}

export function agentDetailQuery(tenantId: string, agentId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "agents", agentId],
    queryFn: () =>
      api<AgentResponse>(
        "GET",
        `/api/tenants/${tenantId}/agents/definitions/${agentId}`,
      ),
  });
}

export function tenantInstancesQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "instances", { status: "running" }],
    queryFn: async () => {
      const res = await api<{ data: AgentInstanceResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/agents/instances?status=running`,
      );
      return res.data;
    },
  });
}

export function agentAllInstancesQuery(tenantId: string, agentId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "instances", { agentId }],
    queryFn: async () => {
      const res = await api<{ data: AgentInstanceResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/agents/instances?agentId=${agentId}`,
      );
      return res.data;
    },
  });
}

export function instanceDetailQuery(tenantId: string, instanceId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "instances", instanceId],
    queryFn: () =>
      api<AgentInstanceResponse>(
        "GET",
        `/api/tenants/${tenantId}/agents/instances/${instanceId}`,
      ),
    refetchInterval: 3000,
  });
}

export function offeringDetailQuery(tenantId: string, offeringId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "offerings", offeringId],
    queryFn: () =>
      api<OfferingResponse>(
        "GET",
        `/api/tenants/${tenantId}/offerings/${offeringId}`,
      ),
  });
}

export function roleDetailQuery(tenantId: string, roleId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "roles", roleId],
    queryFn: () =>
      api<RoleResponse>("GET", `/api/tenants/${tenantId}/roles/${roleId}`),
  });
}

export function principalDetailQuery(tenantId: string, principalId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "principals", principalId],
    queryFn: () =>
      api<PrincipalResponse>(
        "GET",
        `/api/tenants/${tenantId}/principals/${principalId}`,
      ),
  });
}

export function grantDetailQuery(tenantId: string, grantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "grants", grantId],
    queryFn: () =>
      api<GrantResponse>("GET", `/api/tenants/${tenantId}/grants/${grantId}`),
  });
}

export function credentialDetailQuery(tenantId: string, credentialId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "credentials", credentialId],
    queryFn: () =>
      api<CredentialResponse>(
        "GET",
        `/api/tenants/${tenantId}/credentials/${credentialId}`,
      ),
  });
}

export function walletDetailQuery(tenantId: string, walletId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "wallets", walletId],
    queryFn: () =>
      api<WalletResponse>(
        "GET",
        `/api/tenants/${tenantId}/wallets/${walletId}`,
      ),
  });
}

export function tenantGrantsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "grants"],
    queryFn: async () => {
      const res = await api<{ data: GrantResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/grants`,
      );
      return res.data;
    },
  });
}

export function principalGrantsQuery(tenantId: string, principalId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "grants", { principalId }],
    queryFn: async () => {
      const res = await api<{ data: GrantResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/grants?principalId=${principalId}`,
      );
      return res.data;
    },
  });
}

export function tenantCredentialsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "credentials"],
    queryFn: async () => {
      const res = await api<{ data: CredentialResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/credentials`,
      );
      return res.data;
    },
  });
}

export function tenantWalletsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "wallets"],
    queryFn: async () => {
      const res = await api<{ data: WalletResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/wallets`,
      );
      return res.data;
    },
  });
}

export function tenantOfferingsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "offerings"],
    queryFn: async () => {
      const res = await api<{ data: OfferingResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/offerings`,
      );
      return res.data;
    },
  });
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

function invalidate(qc: QueryClient, tenantId: string, segment: string) {
  return qc.invalidateQueries({ queryKey: ["tenants", tenantId, segment] });
}

// Tenant

export function updateTenantMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: { name?: string; config?: Record<string, unknown> }) =>
      api<TenantResponse>("PATCH", `/api/tenants/${tenantId}`, body),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tenants", tenantId] });
      qc.invalidateQueries({ queryKey: ["me", "principals"] });
    },
  };
}

// Roles

export function createRoleMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: { name: string; description?: string }) =>
      api<RoleResponse>("POST", `/api/tenants/${tenantId}/roles`, body),
    onSuccess: () => invalidate(qc, tenantId, "roles"),
  };
}

export function updateRoleMutation(
  tenantId: string,
  roleId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: { name?: string; description?: string }) =>
      api<RoleResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/roles/${roleId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "roles"),
  };
}

export function deleteRoleMutation(
  tenantId: string,
  roleId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>("DELETE", `/api/tenants/${tenantId}/roles/${roleId}`),
    onSuccess: () => invalidate(qc, tenantId, "roles"),
  };
}

// Role assignment

export function assignRoleMutation(
  tenantId: string,
  principalId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (roleId: string) =>
      api<undefined>(
        "POST",
        `/api/tenants/${tenantId}/principals/${principalId}/roles/${roleId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "principals"),
  };
}

export function removeRoleMutation(
  tenantId: string,
  principalId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (roleId: string) =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/principals/${principalId}/roles/${roleId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "principals"),
  };
}

// Principals

export function inviteMemberMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: { email: string; roleId?: string }) =>
      api<PrincipalResponse>(
        "POST",
        `/api/tenants/${tenantId}/members/invite`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "principals"),
  };
}

export function updatePrincipalMutation(
  tenantId: string,
  principalId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: { status: "active" | "suspended" | "deactivated" }) =>
      api<PrincipalResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/principals/${principalId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "principals"),
  };
}

export function deletePrincipalMutation(
  tenantId: string,
  principalId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/principals/${principalId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "principals"),
  };
}

// Agents

type CreateAgentBody = {
  name: string;
  description?: string;
  systemPrompt?: string;
};

type UpdateAgentBody = {
  name?: string;
  description?: string;
  systemPrompt?: string;
  credentialRequirements?: CredentialRequirement[];
  grantRequirements?: GrantRequirement[];
  roleIds?: string[];
};

export function createAgentMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateAgentBody) =>
      api<AgentResponse>(
        "POST",
        `/api/tenants/${tenantId}/agents/definitions`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "agents"),
  };
}

export function updateAgentMutation(
  tenantId: string,
  agentId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateAgentBody) =>
      api<AgentResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/agents/definitions/${agentId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "agents"),
  };
}

export function deleteAgentMutation(
  tenantId: string,
  agentId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/agents/definitions/${agentId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "agents"),
  };
}

export function deployInstanceMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: { agentId: string }) =>
      api<AgentInstanceResponse>(
        "POST",
        `/api/tenants/${tenantId}/agents/instances`,
        body,
      ),
    onSuccess: () => {
      invalidate(qc, tenantId, "instances");
      invalidate(qc, tenantId, "agents");
    },
  };
}

export function stopInstanceMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (instanceId: string) =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/agents/instances/${instanceId}`,
      ),
    onSuccess: () => {
      invalidate(qc, tenantId, "instances");
      invalidate(qc, tenantId, "agents");
    },
  };
}

// Grants

type CreateGrantBody = {
  resource: string;
  action: string;
  effect: "allow" | "deny" | "ask";
  origin: "system" | "role" | "creator" | "invoker";
  roleId?: string | null;
  principalId?: string | null;
  conditions?: Record<string, unknown> | null;
  expiresAt?: string | null;
};

type UpdateGrantBody = {
  effect?: "allow" | "deny" | "ask";
  conditions?: Record<string, unknown> | null;
  expiresAt?: string | null;
};

export function createGrantMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateGrantBody) =>
      api<GrantResponse>("POST", `/api/tenants/${tenantId}/grants`, body),
    onSuccess: () => invalidate(qc, tenantId, "grants"),
  };
}

export function updateGrantMutation(
  tenantId: string,
  grantId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateGrantBody) =>
      api<GrantResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/grants/${grantId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "grants"),
  };
}

export function deleteGrantMutation(
  tenantId: string,
  grantId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>("DELETE", `/api/tenants/${tenantId}/grants/${grantId}`),
    onSuccess: () => invalidate(qc, tenantId, "grants"),
  };
}

// Credentials

export type CreateCredentialBody = {
  providerId: string;
  name: string;
  type: "api_key" | "oauth_token" | "certificate" | "other";
  secret: string;
  description?: string;
  metadata?: Record<string, unknown>;
};

type UpdateCredentialBody = {
  name?: string;
  description?: string;
  secret?: string;
  metadata?: Record<string, unknown>;
};

export function createCredentialMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateCredentialBody) =>
      api<CredentialResponse>(
        "POST",
        `/api/tenants/${tenantId}/credentials`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "credentials"),
  };
}

export function updateCredentialMutation(
  tenantId: string,
  credentialId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateCredentialBody) =>
      api<CredentialResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/credentials/${credentialId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "credentials"),
  };
}

export function deleteCredentialMutation(
  tenantId: string,
  credentialId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/credentials/${credentialId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "credentials"),
  };
}

// Wallets

type CreateWalletBody = {
  name: string;
  backendType: "crypto" | "fiat" | "credits";
  currency: string;
  config?: Record<string, unknown>;
};

type UpdateWalletBody = {
  name?: string;
  config?: Record<string, unknown>;
};

export function createWalletMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateWalletBody) =>
      api<WalletResponse>("POST", `/api/tenants/${tenantId}/wallets`, body),
    onSuccess: () => invalidate(qc, tenantId, "wallets"),
  };
}

export function updateWalletMutation(
  tenantId: string,
  walletId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateWalletBody) =>
      api<WalletResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/wallets/${walletId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "wallets"),
  };
}

export function deleteWalletMutation(
  tenantId: string,
  walletId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>("DELETE", `/api/tenants/${tenantId}/wallets/${walletId}`),
    onSuccess: () => invalidate(qc, tenantId, "wallets"),
  };
}

// Offerings

type CreateOfferingBody = {
  agentId: string;
  name: string;
  description?: string;
  pricing?: {
    base?: { amount: string; currency: string };
    methods?: string[];
    negotiable?: boolean;
    bounds?: { min?: string; max?: string };
  };
  schema?: Record<string, unknown>;
};

type UpdateOfferingBody = {
  name?: string;
  description?: string;
  pricing?: CreateOfferingBody["pricing"];
  schema?: Record<string, unknown>;
};

export function createOfferingMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateOfferingBody) =>
      api<OfferingResponse>("POST", `/api/tenants/${tenantId}/offerings`, body),
    onSuccess: () => invalidate(qc, tenantId, "offerings"),
  };
}

export function updateOfferingMutation(
  tenantId: string,
  offeringId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateOfferingBody) =>
      api<OfferingResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/offerings/${offeringId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "offerings"),
  };
}

export function deleteOfferingMutation(
  tenantId: string,
  offeringId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/offerings/${offeringId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "offerings"),
  };
}

// ---------------------------------------------------------------------------
// Model catalog
// ---------------------------------------------------------------------------

export type ModelProviderPluginValue =
  | "anthropic"
  | "openai"
  | "openai-compatible"
  | "google-genai";

export type CatalogModelResponse = {
  id: string;
  tenantId: string;
  canonicalName: string;
  displayName: string | null;
  description: string | null;
  disabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export type CatalogModelProviderResponse = {
  id: string;
  tenantId: string;
  name: string;
  plugin: ModelProviderPluginValue;
  baseURL: string;
  credentialId: string | null;
  walletId: string | null;
  disabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export type CatalogModelOfferingResponse = {
  id: string;
  tenantId: string;
  modelId: string;
  providerId: string;
  priority: number;
  deploymentTags: string[];
  capabilities: string[];
  disabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export type CatalogPricingRowResponse = {
  id: string;
  tenantId: string;
  offeringId: string;
  currency: string;
  inputTokenPrice: string | null;
  outputTokenPrice: string | null;
  cacheReadTokenPrice: string | null;
  cacheWriteTokenPrice: string | null;
  thinkingTokenPrice: string | null;
  perRequestFee: string | null;
  perImageFee: string | null;
  perAudioFee: string | null;
  effectiveFrom: string;
  createdAt: string;
};

export function tenantCatalogModelsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "catalog-models"],
    queryFn: async () => {
      const res = await api<{ data: CatalogModelResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/catalog/models`,
      );
      return res.data;
    },
  });
}

export function catalogModelDetailQuery(tenantId: string, modelId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "catalog-models", modelId],
    queryFn: () =>
      api<CatalogModelResponse>(
        "GET",
        `/api/tenants/${tenantId}/catalog/models/${modelId}`,
      ),
  });
}

export function tenantModelProvidersQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "model-providers"],
    queryFn: async () => {
      const res = await api<{ data: CatalogModelProviderResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/catalog/providers`,
      );
      return res.data;
    },
  });
}

export function modelProviderDetailQuery(tenantId: string, providerId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "model-providers", providerId],
    queryFn: () =>
      api<CatalogModelProviderResponse>(
        "GET",
        `/api/tenants/${tenantId}/catalog/providers/${providerId}`,
      ),
  });
}

export function tenantModelOfferingsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "model-offerings"],
    queryFn: async () => {
      const res = await api<{ data: CatalogModelOfferingResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/catalog/offerings`,
      );
      return res.data;
    },
  });
}

export function modelOfferingDetailQuery(tenantId: string, offeringId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "model-offerings", offeringId],
    queryFn: () =>
      api<CatalogModelOfferingResponse>(
        "GET",
        `/api/tenants/${tenantId}/catalog/offerings/${offeringId}`,
      ),
  });
}

export function offeringPricingQuery(tenantId: string, offeringId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "model-offerings", offeringId, "pricing"],
    queryFn: () =>
      api<CatalogPricingRowResponse[]>(
        "GET",
        `/api/tenants/${tenantId}/catalog/offerings/${offeringId}/pricing`,
      ),
  });
}

export type CreateModelBody = {
  canonicalName: string;
  displayName?: string | null;
  description?: string | null;
};

export type UpdateModelBody = {
  displayName?: string | null;
  description?: string | null;
  disabled?: boolean;
};

export function createCatalogModelMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateModelBody) =>
      api<CatalogModelResponse>(
        "POST",
        `/api/tenants/${tenantId}/catalog/models`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "catalog-models"),
  };
}

export function updateCatalogModelMutation(
  tenantId: string,
  modelId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateModelBody) =>
      api<CatalogModelResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/catalog/models/${modelId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "catalog-models"),
  };
}

export function deleteCatalogModelMutation(
  tenantId: string,
  modelId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/catalog/models/${modelId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "catalog-models"),
  };
}

export type CreateModelProviderBody = {
  name: string;
  plugin: ModelProviderPluginValue;
  baseURL: string;
  credentialId?: string | null;
  walletId?: string | null;
};

export type UpdateModelProviderBody = {
  name?: string;
  baseURL?: string;
  disabled?: boolean;
};

export function createModelProviderMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateModelProviderBody) =>
      api<CatalogModelProviderResponse>(
        "POST",
        `/api/tenants/${tenantId}/catalog/providers`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "model-providers"),
  };
}

export function updateModelProviderMutation(
  tenantId: string,
  providerId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateModelProviderBody) =>
      api<CatalogModelProviderResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/catalog/providers/${providerId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "model-providers"),
  };
}

export function deleteModelProviderMutation(
  tenantId: string,
  providerId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/catalog/providers/${providerId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "model-providers"),
  };
}

export type CreateModelOfferingBody = {
  modelId: string;
  providerId: string;
  priority?: number;
  deploymentTags?: string[];
  capabilities?: string[];
};

export type UpdateModelOfferingBody = {
  priority?: number;
  deploymentTags?: string[];
  capabilities?: string[];
  disabled?: boolean;
};

export function createModelOfferingMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateModelOfferingBody) =>
      api<CatalogModelOfferingResponse>(
        "POST",
        `/api/tenants/${tenantId}/catalog/offerings`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "model-offerings"),
  };
}

export function updateModelOfferingMutation(
  tenantId: string,
  offeringId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateModelOfferingBody) =>
      api<CatalogModelOfferingResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/catalog/offerings/${offeringId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "model-offerings"),
  };
}

export function deleteModelOfferingMutation(
  tenantId: string,
  offeringId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/catalog/offerings/${offeringId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "model-offerings"),
  };
}

export type CreatePricingRowBody = {
  currency: string;
  effectiveFrom?: string;
  inputTokenPrice?: string | null;
  outputTokenPrice?: string | null;
  cacheReadTokenPrice?: string | null;
  cacheWriteTokenPrice?: string | null;
  thinkingTokenPrice?: string | null;
  perRequestFee?: string | null;
  perImageFee?: string | null;
  perAudioFee?: string | null;
};

export function createPricingRowMutation(
  tenantId: string,
  offeringId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: CreatePricingRowBody) =>
      api<CatalogPricingRowResponse>(
        "POST",
        `/api/tenants/${tenantId}/catalog/offerings/${offeringId}/pricing`,
        body,
      ),
    onSuccess: () =>
      qc.invalidateQueries({
        queryKey: [
          "tenants",
          tenantId,
          "model-offerings",
          offeringId,
          "pricing",
        ],
      }),
  };
}
