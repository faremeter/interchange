// Per-agent Ed25519 key custody.
//
// Persists key pairs as raw 32-byte binary files on disk and produces
// them on demand. The cryptographic primitives (keypair generation,
// deploy-commit verification) are host-supplied so the package does
// not pin a particular crypto backend.
//
// In addition to the on-disk persistence, the store keeps an in-memory
// cache of the keypair and the paired hub public key for every agent
// that has been loaded or recorded during the process lifetime. The
// cache backs the per-frame crypto operations the wire layer needs:
// verifyDeployCommit for incoming deploy packs.

import fsp from "node:fs/promises";
import { hasCode, hexDecode } from "@intx/types";
import type { KeyPair } from "@intx/types/runtime";

import { keysDir, privateKeyPath, publicKeyPath } from "./agent-paths";

export type AgentKeyStoreDeps = {
  dataDir: string;
  generateKeyPair: () => Promise<KeyPair>;
  /**
   * Verify an SSH signature block against the supplied public key.
   * Used by verifyDeployCommit.
   */
  verifySSHSig: (
    payload: string,
    signature: string,
    publicKey: Uint8Array,
  ) => Promise<boolean>;
};

export type AgentKeyStore = {
  /**
   * Load the existing keypair for an agent, or mint and persist a new
   * one. The keypair is also cached in memory. The `isNew` flag is true
   * when the keypair was just generated.
   */
  loadOrGenerateKey(
    address: string,
  ): Promise<{ keyPair: KeyPair; isNew: boolean }>;
  /**
   * Record the hub public key the agent has been paired with. Cached in
   * memory only; the deploy path re-records it on every deploy, so it
   * does not need to survive a restart.
   */
  recordHubKey(address: string, hexHubPublicKey: string): void;
  /**
   * Verify an SSH signature against the cached hub public key for the
   * given address. Throws when no hub key is cached — a deploy pack
   * cannot be verified without one.
   */
  verifyDeployCommit(
    address: string,
    payload: string,
    signature: string,
  ): Promise<boolean>;
  /**
   * Drop the in-memory caches for an agent. Called on undeploy.
   */
  forgetAgent(address: string): void;
};

export function createAgentKeyStore(deps: AgentKeyStoreDeps): AgentKeyStore {
  const { dataDir, generateKeyPair, verifySSHSig } = deps;

  const agentKeys = new Map<string, KeyPair>();
  const hubKeys = new Map<string, Uint8Array>();

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
    agentKeys.set(address, keyPair);
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
    agentKeys.set(address, keyPair);
    return { keyPair, isNew: true };
  }

  function recordHubKey(address: string, hexHubPublicKey: string): void {
    hubKeys.set(address, hexDecode(hexHubPublicKey));
  }

  async function verifyDeployCommit(
    address: string,
    payload: string,
    signature: string,
  ): Promise<boolean> {
    const hubKey = hubKeys.get(address);
    if (hubKey === undefined) {
      throw new Error(
        `signature_invalid: no hub public key recorded for "${address}"`,
      );
    }
    return verifySSHSig(payload, signature, hubKey);
  }

  function forgetAgent(address: string): void {
    agentKeys.delete(address);
    hubKeys.delete(address);
  }

  return {
    loadOrGenerateKey,
    recordHubKey,
    verifyDeployCommit,
    forgetAgent,
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
