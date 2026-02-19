import { type } from "arktype";

export const CreateOAuthClient = type({
  providerId: "string",
  name: "string",
  clientId: "string",
  clientSecret: "string",
  "redirectUris?": "string[]",
  "defaultScopes?": "string[]",
  "metadata?": "Record<string, unknown>",
});

export const UpdateOAuthClient = type({
  "name?": "string",
  "clientId?": "string",
  "clientSecret?": "string",
  "redirectUris?": "string[] | null",
  "defaultScopes?": "string[] | null",
  "metadata?": "Record<string, unknown> | null",
});

export const OAuthClientResponse = type({
  id: "string",
  tenantId: "string",
  providerId: "string",
  name: "string",
  "redirectUris?": "string[] | null",
  "defaultScopes?": "string[] | null",
  "metadata?": "Record<string, unknown> | null",
  createdAt: "string",
  updatedAt: "string",
});
