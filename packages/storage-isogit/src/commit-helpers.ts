import type { CommitSigner } from "./signer";

export type SigningArgs = {
  onSign?: (args: { payload: string }) => Promise<{ signature: string }>;
  signingKey?: string;
};

export function buildSigningArgs(
  signer: CommitSigner | undefined,
): SigningArgs {
  if (signer === undefined) return {};
  return {
    signingKey: "sshsig",
    onSign: async ({ payload }) => ({ signature: await signer(payload) }),
  };
}
