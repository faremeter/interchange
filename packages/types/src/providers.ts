import { type } from "arktype";

export const CreateProvider = type({
  name: "string",
  plugin: "string",
  "authorizationUrl?": "string",
  "tokenUrl?": "string",
  "userInfoUrl?": "string",
  "scopes?": "string[]",
  "metadata?": "Record<string, unknown>",
});

export const UpdateProvider = type({
  "name?": "string",
  "plugin?": "string",
  "authorizationUrl?": "string | null",
  "tokenUrl?": "string | null",
  "userInfoUrl?": "string | null",
  "scopes?": "string[] | null",
  "metadata?": "Record<string, unknown> | null",
});

export const ProviderResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  plugin: "string",
  "authorizationUrl?": "string | null",
  "tokenUrl?": "string | null",
  "userInfoUrl?": "string | null",
  "scopes?": "string[] | null",
  "metadata?": "Record<string, unknown> | null",
  createdAt: "string",
  updatedAt: "string",
});
