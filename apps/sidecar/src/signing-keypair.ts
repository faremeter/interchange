import fs from "node:fs/promises";
import path from "node:path";
import { derivePublicKeyBytes, generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";

const PRIVATE_KEY_FILENAME = "ed25519.private";
const PUBLIC_KEY_FILENAME = "ed25519.public";

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

async function exists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

/**
 * Load the sidecar's persisted Ed25519 signing keypair, minting a fresh
 * one on first boot.
 *
 * The 32-byte seed in `ed25519.private` is the sole source of truth for
 * the sidecar's identity; the public key is always derived from it. The
 * `ed25519.public` file is an identity anchor, not a cache: on every load
 * it is cross-checked against the seed-derived public key. A mismatch
 * means either the seed or the public file was corrupted or swapped (a
 * bad backup restore, disk bitrot, an operator fat-finger). Rather than
 * trusting the file and advertising a public key the sidecar cannot sign
 * with -- which would make every signature fail verification at the hub,
 * far from the root cause -- we fail loudly here at boot. We cannot tell
 * which of the two files rotted, so we halt and let an operator decide
 * (restore the seed, or remove the directory to mint a fresh identity).
 */
export async function loadOrMintSidecarKeypair(
  signingDir: string,
): Promise<KeyPair> {
  const privateKeyPath = path.join(signingDir, PRIVATE_KEY_FILENAME);
  const publicKeyPath = path.join(signingDir, PUBLIC_KEY_FILENAME);

  const [havePriv, havePub] = await Promise.all([
    exists(privateKeyPath),
    exists(publicKeyPath),
  ]);
  if (havePriv !== havePub) {
    throw new Error(
      `sidecar signing keypair under ${signingDir} is partial: privateKey=${String(havePriv)} publicKey=${String(havePub)}; remove the directory to reset`,
    );
  }
  if (havePriv && havePub) {
    const [priv, pub] = await Promise.all([
      fs.readFile(privateKeyPath),
      fs.readFile(publicKeyPath),
    ]);
    const seed = new Uint8Array(priv);
    const storedPublicKey = new Uint8Array(pub);
    let derivedPublicKey: Uint8Array;
    try {
      derivedPublicKey = await derivePublicKeyBytes(seed);
    } catch (cause) {
      throw new Error(
        `sidecar signing seed at ${privateKeyPath} is not a valid Ed25519 seed; remove ${signingDir} to reset`,
        { cause },
      );
    }
    if (!bytesEqual(derivedPublicKey, storedPublicKey)) {
      throw new Error(
        `sidecar signing public key at ${publicKeyPath} does not match the key derived from the seed at ${privateKeyPath}; the sidecar would advertise a public key it cannot sign with, so every signature would fail verification at the hub; restore the matching seed, or remove ${signingDir} to mint a fresh identity`,
      );
    }
    return { privateKey: seed, publicKey: derivedPublicKey };
  }

  const keyPair = await generateKeyPair();
  await fs.mkdir(signingDir, { recursive: true });
  await Promise.all([
    fs.writeFile(privateKeyPath, keyPair.privateKey, { mode: 0o600 }),
    fs.writeFile(publicKeyPath, keyPair.publicKey),
  ]);
  return keyPair;
}
