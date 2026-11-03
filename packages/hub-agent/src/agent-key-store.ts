// Per-agent Ed25519 key custody.
//
// Persists key pairs as raw 32-byte binary files on disk and produces
// them on demand. Keypair generation is host-supplied so the package does
// not pin a particular crypto backend.

import fsp from "node:fs/promises";
import { hasCode } from "@intx/types/has-code";
import type { KeyPair } from "@intx/types/runtime";

import { keysDir, privateKeyPath, publicKeyPath } from "./agent-paths";

export type AgentKeyStoreDeps = {
  dataDir: string;
  generateKeyPair: () => Promise<KeyPair>;
};

export type AgentKeyStore = {
  /**
   * Load the existing keypair for an agent, or mint and persist a new
   * one. The `isNew` flag is true when the keypair was just generated.
   */
  loadOrGenerateKey(
    address: string,
  ): Promise<{ keyPair: KeyPair; isNew: boolean }>;
};

export function createAgentKeyStore(deps: AgentKeyStoreDeps): AgentKeyStore {
  const { dataDir, generateKeyPair } = deps;

  async function loadKeyFromDisk(address: string): Promise<KeyPair | null> {
    const privPath = privateKeyPath(dataDir, address);
    const pubPath = publicKeyPath(dataDir, address);

    const [privExists, pubExists] = await Promise.all([
      fileExists(privPath),
      fileExists(pubPath),
    ]);

    if (privExists !== pubExists) {
      const missing = privExists ? "public" : "private";
      throw new Error(
        `Corrupt key pair for "${address}": ${missing} key file is missing`,
      );
    }

    if (!privExists) return null;

    const [privateKey, publicKey] = await Promise.all([
      fsp.readFile(privPath),
      fsp.readFile(pubPath),
    ]);
    const keyPair: KeyPair = {
      privateKey: new Uint8Array(privateKey),
      publicKey: new Uint8Array(publicKey),
    };
    return keyPair;
  }

  async function loadOrGenerateKey(
    address: string,
  ): Promise<{ keyPair: KeyPair; isNew: boolean }> {
    const fromDisk = await loadKeyFromDisk(address);
    if (fromDisk !== null) return { keyPair: fromDisk, isNew: false };

    const keyPair = await generateKeyPair();
    await fsp.mkdir(keysDir(dataDir, address), { recursive: true });
    await Promise.all([
      fsp.writeFile(privateKeyPath(dataDir, address), keyPair.privateKey, {
        mode: 0o600,
      }),
      fsp.writeFile(publicKeyPath(dataDir, address), keyPair.publicKey),
    ]);
    return { keyPair, isNew: true };
  }

  return {
    loadOrGenerateKey,
  };
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fsp.access(filePath);
    return true;
  } catch (err: unknown) {
    if (hasCode(err) && err.code === "ENOENT") return false;
    // Any other failure mode (EACCES, EBUSY, EIO, …) must surface so a
    // restart does not silently mint a fresh key over an existing one
    // when the existence check is denied or transiently failing.
    throw err;
  }
}
