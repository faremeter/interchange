import { type } from "arktype";

import { SidecarCapabilityPolicy } from "./sidecar-capabilities";

export const TenantConfig = type({
  "sidecarPlacement?": SidecarCapabilityPolicy,
  "[string]": "unknown",
});
export type TenantConfig = typeof TenantConfig.infer;

export const CreateTenant = type({
  name: "string",
  slug: "string",
  "parentId?": "string | null",
});

export const UpdateTenant = type({
  "name?": "string",
  "config?": TenantConfig,
});

export const TenantResponse = type({
  id: "string",
  name: "string",
  slug: "string",
  domain: "string",
  "parentId?": "string | null",
  "config?": TenantConfig,
  createdAt: "string",
  updatedAt: "string",
});

export const FederationTrust = type({
  tenantId: "string",
  tenantName: "string",
  tenantDomain: "string",
  direction: "'inbound' | 'outbound' | 'bilateral'",
  createdAt: "string",
});

export const CreateFederationTrust = type({
  targetTenantId: "string",
  direction: "'inbound' | 'outbound' | 'bilateral'",
});
