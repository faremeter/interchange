import { type } from "arktype";

export const CreateCredential = type({
  providerId: "string",
  name: "string",
  type: "'api_key' | 'oauth_token' | 'certificate' | 'other'",
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
  "status?": "'active' | 'expired' | 'revoked' | 'error'",
  "metadata?": "Record<string, unknown>",
});

export const CredentialResponse = type({
  id: "string",
  tenantId: "string",
  providerId: "string",
  "principalId?": "string | null",
  "oauthClientId?": "string | null",
  name: "string",
  type: "'api_key' | 'oauth_token' | 'certificate' | 'other'",
  "description?": "string | null",
  "scopes?": "string[] | null",
  "expiresAt?": "string | null",
  status: "'active' | 'expired' | 'revoked' | 'error'",
  "metadata?": "Record<string, unknown> | null",
  createdAt: "string",
  updatedAt: "string",
});
