import { type } from "arktype";

export const CreateCredential = type({
  name: "string",
  type: "'api_key' | 'oauth_token' | 'certificate' | 'other'",
  "description?": "string",
  secret: "string",
  "metadata?": "Record<string, unknown>",
});

export const UpdateCredential = type({
  "name?": "string",
  "description?": "string",
  "secret?": "string",
  "metadata?": "Record<string, unknown>",
});

export const CredentialResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  type: "'api_key' | 'oauth_token' | 'certificate' | 'other'",
  "description?": "string | null",
  "metadata?": "Record<string, unknown> | null",
  createdAt: "string",
  updatedAt: "string",
});
