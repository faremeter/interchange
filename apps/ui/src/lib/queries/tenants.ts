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

type AgentResponse = {
  id: string;
  tenantId: string;
  principalId: string;
  name: string;
  description: string | null;
  status: "deployed" | "stopped" | "updating" | "error";
  currentVersion: string;
  createdAt: string;
  updatedAt: string;
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
  source: "system" | "role" | "creator" | "invoker";
  createdAt: string;
  updatedAt: string;
};

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
        `/api/tenants/${tenantId}/agents`,
      );
      return res.data;
    },
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

type CapabilityResponse = {
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

export function tenantCapabilitiesQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "capabilities"],
    queryFn: async () => {
      const res = await api<{ data: CapabilityResponse[] }>(
        "GET",
        `/api/tenants/${tenantId}/capabilities`,
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

// Principals

export function inviteMemberMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: { email: string; roleId?: string }) =>
      api<PrincipalResponse>(
        "POST",
        `/api/tenants/${tenantId}/invitations`,
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
};

export function createAgentMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateAgentBody) =>
      api<AgentResponse>("POST", `/api/tenants/${tenantId}/agents`, body),
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
        `/api/tenants/${tenantId}/agents/${agentId}`,
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
      api<undefined>("DELETE", `/api/tenants/${tenantId}/agents/${agentId}`),
    onSuccess: () => invalidate(qc, tenantId, "agents"),
  };
}

// Grants

type CreateGrantBody = {
  resource: string;
  action: string;
  effect: "allow" | "deny" | "ask";
  source: "system" | "role" | "creator" | "invoker";
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

type CreateCredentialBody = {
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

// Capabilities

type CreateCapabilityBody = {
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

type UpdateCapabilityBody = {
  name?: string;
  description?: string;
  pricing?: CreateCapabilityBody["pricing"];
  schema?: Record<string, unknown>;
};

export function createCapabilityMutation(tenantId: string, qc: QueryClient) {
  return {
    mutationFn: (body: CreateCapabilityBody) =>
      api<CapabilityResponse>(
        "POST",
        `/api/tenants/${tenantId}/capabilities`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "capabilities"),
  };
}

export function updateCapabilityMutation(
  tenantId: string,
  capabilityId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: (body: UpdateCapabilityBody) =>
      api<CapabilityResponse>(
        "PATCH",
        `/api/tenants/${tenantId}/capabilities/${capabilityId}`,
        body,
      ),
    onSuccess: () => invalidate(qc, tenantId, "capabilities"),
  };
}

export function deleteCapabilityMutation(
  tenantId: string,
  capabilityId: string,
  qc: QueryClient,
) {
  return {
    mutationFn: () =>
      api<undefined>(
        "DELETE",
        `/api/tenants/${tenantId}/capabilities/${capabilityId}`,
      ),
    onSuccess: () => invalidate(qc, tenantId, "capabilities"),
  };
}
