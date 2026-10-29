import { sql, type SQL } from "drizzle-orm";

import { credentialAad, type CredentialCipher } from "@intx/types";

import { credential } from "./schema/credentials";

/**
 * The column values that replace a credential's secret, for the `set` of the
 * UPDATE that writes it. Every write of a new secret goes through here, so the
 * secret and its `secretVersion` change together. The version increments in
 * the UPDATE itself, under the row lock, so two concurrent writes still get
 * distinct, ordered versions.
 */
export async function credentialSecretColumns(
  credentialCipher: CredentialCipher,
  credentialId: string,
  secret: string,
): Promise<{ secret: string; secretVersion: SQL }> {
  return {
    secret: await credentialCipher.encrypt(
      secret,
      credentialAad(credentialId, "secret"),
    ),
    secretVersion: sql`${credential.secretVersion} + 1`,
  };
}
