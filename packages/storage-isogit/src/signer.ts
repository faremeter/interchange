/**
 * A function that signs a git commit payload and returns the armored
 * signature string for embedding in the gpgsig header.
 *
 * Callers bind this to their signing implementation (e.g. createSSHSignature
 * with an Ed25519 key pair). The store does not own key material.
 */
export type CommitSigner = (payload: string) => Promise<string>;
