import { type } from "arktype";

import { SidecarCapabilityPolicy } from "./sidecar-capabilities";

export const TenantConfig = type({
  "sidecarPlacement?": SidecarCapabilityPolicy,
  "[string]": "unknown",
});
export type TenantConfig = typeof TenantConfig.infer;

/**
 * A tenant slug, constrained to a single DNS label: the create route derives
 * `tenant.domain` from the lowercased slug, and that domain is both the sender
 * stamp on outbound mail and the recipient run address. The grammar is RFC 1035
 * section 2.3.1 as relaxed by RFC 1123 section 2.1.
 *
 * https://www.rfc-editor.org/rfc/rfc1035#section-2.3.1
 * https://www.rfc-editor.org/rfc/rfc1123#section-2.1
 */
export const TenantSlug = type(
  /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/,
).and("string<=63");
export type TenantSlug = typeof TenantSlug.infer;

export const CreateTenant = type({
  name: "string",
  slug: TenantSlug,
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
