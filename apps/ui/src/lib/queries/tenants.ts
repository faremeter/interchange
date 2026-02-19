import { queryOptions } from "@tanstack/react-query";

import { api } from "../api";

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
    queryFn: () =>
      api<PrincipalResponse[]>("GET", `/api/tenants/${tenantId}/principals`),
  });
}

export function tenantRolesQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "roles"],
    queryFn: () => api<RoleResponse[]>("GET", `/api/tenants/${tenantId}/roles`),
  });
}

export function tenantAgentsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "agents"],
    queryFn: () =>
      api<AgentResponse[]>("GET", `/api/tenants/${tenantId}/agents`),
  });
}

export function tenantGrantsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "grants"],
    queryFn: () =>
      api<GrantResponse[]>("GET", `/api/tenants/${tenantId}/grants`),
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
    queryFn: () =>
      api<CredentialResponse[]>("GET", `/api/tenants/${tenantId}/credentials`),
  });
}

export function tenantWalletsQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "wallets"],
    queryFn: () =>
      api<WalletResponse[]>("GET", `/api/tenants/${tenantId}/wallets`),
  });
}

export function tenantCapabilitiesQuery(tenantId: string) {
  return queryOptions({
    queryKey: ["tenants", tenantId, "capabilities"],
    queryFn: () =>
      api<CapabilityResponse[]>("GET", `/api/tenants/${tenantId}/capabilities`),
  });
}
