import { type } from "arktype";

const redirectUrisDescription =
  "Allowed OAuth redirect URIs for this client. The authorization callback must match one of these.";

const defaultScopesDescription =
  "Scopes requested by default when initiating an authorization flow with this client.";

const oauthClientMetadataDescription =
  "Free-form client-specific configuration not covered by the typed fields. Not interpreted by the hub.";

export const CreateOAuthClient = type({
  providerId: "string",
  name: "string",
  clientId: "string",
  clientSecret: "string",
  "redirectUris?": type("string[]").describe(redirectUrisDescription),
  "defaultScopes?": type("string[]").describe(defaultScopesDescription),
  "metadata?": type("Record<string, unknown>").describe(
    oauthClientMetadataDescription,
  ),
});

export const UpdateOAuthClient = type({
  "name?": "string",
  "clientId?": "string",
  "clientSecret?": "string",
  "redirectUris?": type("string[] | null").describe(redirectUrisDescription),
  "defaultScopes?": type("string[] | null").describe(defaultScopesDescription),
  "metadata?": type("Record<string, unknown> | null").describe(
    oauthClientMetadataDescription,
  ),
});

export const OAuthClientResponse = type({
  id: "string",
  tenantId: "string",
  providerId: "string",
  name: "string",
  "redirectUris?": type("string[] | null").describe(redirectUrisDescription),
  "defaultScopes?": type("string[] | null").describe(defaultScopesDescription),
  "metadata?": type("Record<string, unknown> | null").describe(
    oauthClientMetadataDescription,
  ),
  createdAt: "string",
  updatedAt: "string",
});
