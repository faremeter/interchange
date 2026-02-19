import { queryOptions } from "@tanstack/react-query";

import { api } from "@/lib/api";

type UserProfile = {
  id: string;
  name: string;
  email: string;
  emailVerified: boolean;
  image: string | null;
  createdAt: string;
  updatedAt: string;
};

type PrincipalSummary = {
  principalId: string;
  tenantId: string;
  tenantName: string;
  tenantSlug: string;
  kind: "user" | "agent";
  status: "active" | "suspended" | "invited" | "deactivated";
  roles: { id: string; name: string }[];
};

type AgentSummary = {
  id: string;
  tenantId: string;
  tenantName: string;
  name: string;
  description: string | null;
  status: "deployed" | "stopped" | "updating" | "error";
};

export const meProfileQuery = queryOptions({
  queryKey: ["me", "profile"],
  queryFn: () => api<UserProfile>("GET", "/api/me"),
});

export const mePrincipalsQuery = queryOptions({
  queryKey: ["me", "principals"],
  queryFn: async () => {
    const res = await api<{ data: PrincipalSummary[] }>(
      "GET",
      "/api/me/principals",
    );
    return res.data;
  },
});

export const meAgentsQuery = queryOptions({
  queryKey: ["me", "agents"],
  queryFn: async () => {
    const res = await api<{ data: AgentSummary[] }>("GET", "/api/me/agents");
    return res.data;
  },
});
