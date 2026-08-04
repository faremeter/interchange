import { createEnvKeyCredentialCipher } from "@intx/crypto";
import type { CredentialCipher } from "@intx/types";

// A deterministic CredentialCipher for tests. The key is fixed so that a test
// which seeds an encrypted secret and the app-under-test that decrypts it agree
// on the key. Production supplies a real key via CREDENTIAL_ENCRYPTION_KEY. The
// same fill(7) key is inlined in the few package-internal tests that cannot
// import this harness (they depend on @intx/crypto directly).
const TEST_CREDENTIAL_KEY = new Uint8Array(32).fill(7);

export function createTestCredentialCipher(): CredentialCipher {
  return createEnvKeyCredentialCipher(TEST_CREDENTIAL_KEY);
}
