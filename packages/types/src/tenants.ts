import { type } from "arktype";

import { SidecarCapabilityPolicy } from "./sidecar-capabilities";
import { WorkflowLifecyclePolicy } from "./workflow-lifecycle";

export const TenantConfig = type({
  "lifecycle?": WorkflowLifecyclePolicy,
  "sidecarPlacement?": SidecarCapabilityPolicy,
  "[string]": "unknown",
});
export type TenantConfig = typeof TenantConfig.infer;

/** Top-level `config` keys to change; `null` removes a key. */
export const TenantConfigPatch = type({
  "lifecycle?": WorkflowLifecyclePolicy.or("null"),
  "sidecarPlacement?": SidecarCapabilityPolicy.or("null"),
  "[string]": "unknown",
});
export type TenantConfigPatch = typeof TenantConfigPatch.infer;

export const CreateTenant = type({
  name: "string",
  slug: "string",
  "parentId?": "string | null",
});

export const UpdateTenant = type({
  "name?": "string",
  "config?": TenantConfigPatch,
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
