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

const credentialTypeDescription =
  "Kind of secret material this credential holds: `api_key`, `oauth_token`, `certificate`, or `other`. Determines how `secret` (and `refreshSecret` for OAuth) is interpreted when the credential is used.";

const credentialStatusDescription =
  "Usability state of the credential: `active` (usable), `expired` (past its `expiresAt`), `revoked` (deliberately invalidated), or `error` (last use failed, e.g. rejected by the provider).";

const credentialScopesDescription =
  "Permissions granted to this credential by the provider (for example OAuth scopes). Informational on the credential record; the provider is the authority on what the secret can actually do.";

const credentialMetadataDescription =
  "Free-form provider- or integration-specific data attached to the credential. Not interpreted by the hub.";

export const CreateCredential = type({
  providerId: "string",
  name: "string",
  type: CredType.describe(credentialTypeDescription),
  "principalId?": "string",
  "oauthClientId?": "string",
  "description?": "string",
  secret: "string",
  "refreshSecret?": "string",
  "scopes?": type("string[]").describe(credentialScopesDescription),
  "expiresAt?": "string",
  "metadata?": type("Record<string, unknown>").describe(
    credentialMetadataDescription,
  ),
});

export const UpdateCredential = type({
  "name?": "string",
  "description?": "string",
  "secret?": "string",
  "refreshSecret?": "string | null",
  "scopes?": type("string[] | null").describe(credentialScopesDescription),
  "expiresAt?": "string | null",
  "status?": CredStatus.describe(credentialStatusDescription),
  "metadata?": type("Record<string, unknown>").describe(
    credentialMetadataDescription,
  ),
});

export const CredentialResponse = type({
  id: "string",
  tenantId: "string",
  providerId: "string",
  "principalId?": "string | null",
  "oauthClientId?": "string | null",
  name: "string",
  type: CredType.describe(credentialTypeDescription),
  "description?": "string | null",
  "scopes?": type("string[] | null").describe(credentialScopesDescription),
  "expiresAt?": "string | null",
  status: CredStatus.describe(credentialStatusDescription),
  "metadata?": type("Record<string, unknown> | null").describe(
    credentialMetadataDescription,
  ),
  createdAt: "string",
  updatedAt: "string",
});
