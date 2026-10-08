// How the signer behind a signature is identified. The only signer today is a
// principal whose Ed25519 private key the hub custodies (`local-principal`).

import { type } from "arktype";

/**
 * A signer whose private key the hub custodies on a principal's behalf.
 * `publicKey` is the RESOLVED, hex-encoded Ed25519 key read from the hub's
 * own principal-key store -- the trusted key for `principalId`. It MUST NOT
 * be populated from untrusted input (e.g. a key claimed on an inbound
 * message): a verifier resolves the key from the store by `principalId` and
 * checks the signature against that, never against a key from the wire.
 */
export const LocalPrincipalSigner = type({
  kind: "'local-principal'",
  principalId: "string",
  publicKey: "string",
});
export type LocalPrincipalSigner = typeof LocalPrincipalSigner.infer;

/**
 * Discriminated union over how a signature's signer is identified, keyed on
 * `kind`. Widen it here with `.or()` and every by-value consumer that
 * switches on `kind` gains a compile error for an unhandled variant.
 */
export const SignerIdentity = LocalPrincipalSigner;
export type SignerIdentity = typeof SignerIdentity.infer;
