import { createEnvKeyCredentialCipher } from "@intx/crypto";
import type { CredentialCipher } from "@intx/types";

// Deterministic CredentialCipher for tests: a fixed key so a test that
// seeds an encrypted secret and the app under test that decrypts it
// agree. The same fill(7) key is inlined in the few package-internal
// tests that cannot import this harness.
const TEST_CREDENTIAL_KEY = new Uint8Array(32).fill(7);

export function createTestCredentialCipher(): CredentialCipher {
  return createEnvKeyCredentialCipher(TEST_CREDENTIAL_KEY);
}
