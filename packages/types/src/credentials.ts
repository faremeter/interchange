import { type } from "arktype";

import { ToolCredentialHandle } from "./package-json";

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

export const credentialRequirementSources = [
  "tenant",
  "creator",
  "invoker",
] as const;
export type CredentialRequirementSource =
  (typeof credentialRequirementSources)[number];

const CredType = type.enumerated(...credentialTypes);
const CredStatus = type.enumerated(...credentialStatuses);
const CredentialSourceType = type.enumerated(...credentialRequirementSources);

// A credential binding on a workflow definition maps one consumer's declared
// credential handle to a concrete credential resolved fresh at launch. The
// consumer is a tool package, keyed `(package, handle)`, or an action handler,
// keyed `(action, handle)`; a binding names exactly one of the two. `locator`
// is which credential namespace the name is resolved in; today only `tenant`
// exists (a tenant-owned credential, authorized by ownership). A second locator
// that resolves a principal-owned credential -- and the delegation authority
// axis it would need -- is future work, added with the code that consumes it.
export const credentialBindingLocators = ["tenant"] as const;
export type CredentialBindingLocator =
  (typeof credentialBindingLocators)[number];

const BindingLocator = type.enumerated(...credentialBindingLocators);

const credentialBindingFields = {
  handle: ToolCredentialHandle.describe(
    "The credential handle the consumer declared; unique within its consumer.",
  ),
  provider: type("string").describe(
    "The provider the bound credential resolves against.",
  ),
  "name?": type("string").describe(
    "Optional credential name, a tiebreaker when several credentials match the provider and locator.",
  ),
  locator: BindingLocator.describe(
    "Which credential namespace the binding resolves the credential in. `tenant` resolves a tenant-owned credential by provider/name through the tenant walk-up; its use is authorized by tenant ownership.",
  ),
} as const;

// Each branch forbids the other branch's key, so an object that names both
// consumers fails validation instead of matching the first branch with the
// extra key ignored. The forbidden key is typed `never`, which reports only
// "never" for such an object. `undefined` reports a clearer message, but it
// makes `toJsonSchema()` throw, and the API docs generator converts every
// exported type.
export const CredentialBinding = type({
  package: type("string").describe(
    "The tool package the declared handle belongs to: the tool factory's bundle id.",
  ),
  "action?": "never",
  ...credentialBindingFields,
}).or({
  action: type("string > 0").describe(
    "The action handler the declared handle belongs to: the handler's export name in the workflow's `interchange.actions` module.",
  ),
  "package?": "never",
  ...credentialBindingFields,
});
export type CredentialBinding = typeof CredentialBinding.infer;

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

export const CredentialRequirement = type({
  providerName: "string",
  "scopes?": "string[]",
  source: CredentialSourceType.describe(
    "Whose credential satisfies this requirement at launch: `tenant` (a credential owned by the tenant), `creator` (the definition author's), or `invoker` (whoever launched the workflow run).",
  ),
  "name?": "string",
});
