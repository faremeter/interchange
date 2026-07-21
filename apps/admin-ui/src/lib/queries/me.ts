import { queryOptions } from "@tanstack/react-query";

import { api } from "@/lib/api";
import { infiniteListQuery } from "@/lib/queries/pagination";

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
  kind: "user" | "agent" | "workflow";
  status: "active" | "suspended" | "invited" | "deactivated";
  roles: { id: string; name: string }[];
};

export const meProfileQuery = queryOptions({
  queryKey: ["me", "profile"],
  queryFn: () => api<UserProfile>("GET", "/api/me"),
});

export const mePrincipalsInfiniteQuery = infiniteListQuery<PrincipalSummary>(
  ["me", "principals"],
  "/api/me/principals",
);

type InstanceSummary = {
  id: string;
  tenantId: string;
  tenantName: string;
  agentId: string;
  agentName: string;
  address: string;
  status: "deployed" | "running" | "updating" | "error" | "stopped";
  createdAt: string;
};

export const meInstancesInfiniteQuery = infiniteListQuery<InstanceSummary>(
  ["me", "instances"],
  "/api/me/instances",
);
