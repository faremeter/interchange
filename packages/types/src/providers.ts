import { type } from "arktype";

const pluginDescription =
  "Identifier of the integration this provider drives (for example the inference backend). Used to dispatch to the matching plugin and as the prefix when forming fully-qualified model ids (`plugin:model`).";

const providerScopesDescription =
  "OAuth scopes associated with this provider integration.";

const providerMetadataDescription =
  "Free-form provider-specific configuration not covered by the typed fields. Not interpreted by the hub.";

export const CreateProvider = type({
  name: "string",
  plugin: type("string").describe(pluginDescription),
  "authorizationUrl?": "string",
  "tokenUrl?": "string",
  "userInfoUrl?": "string",
  "scopes?": type("string[]").describe(providerScopesDescription),
  "metadata?": type("Record<string, unknown>").describe(
    providerMetadataDescription,
  ),
});

export const UpdateProvider = type({
  "name?": "string",
  "plugin?": type("string").describe(pluginDescription),
  "authorizationUrl?": "string | null",
  "tokenUrl?": "string | null",
  "userInfoUrl?": "string | null",
  "scopes?": type("string[] | null").describe(providerScopesDescription),
  "metadata?": type("Record<string, unknown> | null").describe(
    providerMetadataDescription,
  ),
});

export const ProviderResponse = type({
  id: "string",
  tenantId: "string",
  name: "string",
  plugin: type("string").describe(pluginDescription),
  "authorizationUrl?": "string | null",
  "tokenUrl?": "string | null",
  "userInfoUrl?": "string | null",
  "scopes?": type("string[] | null").describe(providerScopesDescription),
  "metadata?": type("Record<string, unknown> | null").describe(
    providerMetadataDescription,
  ),
  createdAt: "string",
  updatedAt: "string",
});
