import { type } from "arktype";

/**
 * The decrypted credential material and per-handle binding descriptors
 * delivered to a running agent so its consumers -- tools and action handlers --
 * can use provider-backed credentials. Secrets are decrypted hub-side and ride
 * this payload on the live channel ONLY -- the deploy frame at launch, a
 * `credentials.update` frame on rotation, a run's `run.grants` barrier (its
 * hub tokens), and the child's in-memory cell. They
 * are NEVER written to disk (they do not ride the git-committed grants file)
 * and NEVER copied into any snapshot, event, or state -- redaction is by
 * construction, mirroring how an `InferenceSource`'s `apiKey` stays off every
 * egress type.
 *
 * `materials` is keyed by `credentialId` (a credential can back several handles,
 * so its secret is stored once); `bindings` maps each declared handle to the
 * credential that backs it and the consumer identity allowed to use it.
 */
export const CredentialMaterialEntry = type({
  credentialId: "string",
  providerKey: "string",
  origin: "string",
  secret: "string",
});
export type CredentialMaterialEntry = typeof CredentialMaterialEntry.infer;

export const CredentialBindingDescriptor = type({
  handle: "string",
  credentialId: "string",
  consumer: "string",
});
export type CredentialBindingDescriptor =
  typeof CredentialBindingDescriptor.infer;

export const CredentialDelivery = type({
  bindings: CredentialBindingDescriptor.array(),
  materials: CredentialMaterialEntry.array(),
});
export type CredentialDelivery = typeof CredentialDelivery.infer;

/**
 * The reserved credential handle a tool or an action handler declares to call
 * the hub REST API as its run principal. Deploy binds it without an authored
 * `credentialBindings` entry, and an authored binding may not name it.
 */
export const HUB_CREDENTIAL_HANDLE = "interchange.hub";

/**
 * The delivery `credentialId` of a consumer's hub credential, and the id its
 * `credential:{id}` / `use` grant names. It is fixed per consumer, so the grant
 * can be staged before the run's token exists; a deployment has one run
 * principal, so one id per consumer cannot carry another run's token.
 */
export function hubCredentialId(consumer: string): string {
  return `hub:${consumer}`;
}
