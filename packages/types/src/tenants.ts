import { type } from "arktype";

export const CreateTenant = type({
  name: "string",
  slug: "string",
  "parentId?": "string | null",
});

export const UpdateTenant = type({
  "name?": "string",
  "config?": "Record<string, unknown>",
});

export const TenantResponse = type({
  id: "string",
  name: "string",
  slug: "string",
  domain: "string",
  "parentId?": "string | null",
  "config?": "Record<string, unknown>",
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
