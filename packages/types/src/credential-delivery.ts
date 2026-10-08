import { type } from "arktype";

/**
 * The decrypted credential material and per-handle binding descriptors
 * delivered to a running agent so its tools can use provider-backed
 * credentials. Secrets are decrypted hub-side and ride this payload on the
 * live channel ONLY -- the deploy frame at launch, a `credentials.update`
 * frame on rotation, and the child's in-memory cell. They are NEVER written to
 * disk (they do not ride the git-committed grants file) and NEVER copied into
 * any snapshot, event, or state -- redaction is by construction, mirroring how
 * an `InferenceSource`'s `apiKey` stays off every egress type.
 *
 * `materials` is keyed by `credentialId` (a credential can back several handles,
 * so its secret is stored once); `bindings` maps each declared tool handle to
 * the credential that backs it and the consumer identity allowed to use it.
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
