// How the signer behind a signature is identified.
//
// The only signer today is a principal whose Ed25519 private key the hub
// custodies (`local-principal`). The union is keyed on `kind` so a future
// signer flavour (say a hub-held key, or an externally-held key) is added with
// `.or()` and every by-value consumer that switches on `kind` gains a compile
// error for the unhandled variant.

import { type } from "arktype";

/**
 * A signer whose private key the hub custodies on a principal's behalf.
 *
 * `publicKey` is the RESOLVED, hex-encoded Ed25519 public key read from the
 * hub's own principal-key store -- it is the trusted key for `principalId`. It
 * MUST NOT be populated from untrusted input (e.g. a public key claimed on an
 * inbound message): a verifier resolves the key from the store by `principalId`
 * and checks the signature against that, never against a key from the wire.
 */
export const LocalPrincipalSigner = type({
  kind: "'local-principal'",
  principalId: "string",
  publicKey: "string",
});
export type LocalPrincipalSigner = typeof LocalPrincipalSigner.infer;

/**
 * Discriminated union over how a signature's signer is identified, keyed on
 * `kind`. Only the hub-custodied `local-principal` signer exists today; widen
 * it here with `.or()` and every by-value consumer follows.
 */
export const SignerIdentity = LocalPrincipalSigner;
export type SignerIdentity = typeof SignerIdentity.infer;
