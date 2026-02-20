import { type } from "arktype";

export const credentialTypes = [
  "api_key",
  "oauth_token",
  "certificate",
  "other",
] as const;
export type CredentialType = (typeof credentialTypes)[number];

export const credentialStatuses = [
  "active",
  "expired",
  "revoked",
  "error",
] as const;
export type CredentialStatus = (typeof credentialStatuses)[number];

const CredType = type.enumerated(...credentialTypes);
const CredStatus = type.enumerated(...credentialStatuses);

export const CreateCredential = type({
  providerId: "string",
  name: "string",
  type: CredType,
  "principalId?": "string",
  "oauthClientId?": "string",
  "description?": "string",
  secret: "string",
  "refreshSecret?": "string",
  "scopes?": "string[]",
  "expiresAt?": "string",
  "metadata?": "Record<string, unknown>",
});

export const UpdateCredential = type({
  "name?": "string",
  "description?": "string",
  "secret?": "string",
  "refreshSecret?": "string | null",
  "scopes?": "string[] | null",
  "expiresAt?": "string | null",
  "status?": CredStatus,
  "metadata?": "Record<string, unknown>",
});

export const CredentialResponse = type({
  id: "string",
  tenantId: "string",
  providerId: "string",
  "principalId?": "string | null",
  "oauthClientId?": "string | null",
  name: "string",
  type: CredType,
  "description?": "string | null",
  "scopes?": "string[] | null",
  "expiresAt?": "string | null",
  status: CredStatus,
  "metadata?": "Record<string, unknown> | null",
  createdAt: "string",
  updatedAt: "string",
});
