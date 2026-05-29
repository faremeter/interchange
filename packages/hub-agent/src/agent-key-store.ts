// Per-agent Ed25519 key custody.
//
// Persists key pairs as raw 32-byte binary files on disk and produces
// them on demand. The cryptographic primitives (keypair generation,
// challenge signing, deploy-commit verification) are host-supplied so
// the package does not pin a particular crypto backend.
//
// In addition to the on-disk persistence, the store keeps an in-memory
// cache of the keypair and the paired hub public key for every agent
// that has been loaded or recorded during the process lifetime. The
// cache backs the per-frame crypto operations the wire layer needs:
// signChallenge for challenge response frames and verifyDeployCommit
// for incoming deploy packs.

import fsp from "node:fs/promises";
import { getLogger } from "@intx/log";
import { hasCode, hexDecode } from "@intx/types";
import type { KeyPair } from "@intx/types/runtime";

import { keysDir, privateKeyPath, publicKeyPath } from "./agent-paths";

const logger = getLogger(["interchange", "hub-agent", "key-store"]);

export type AgentKeyEntry = {
  address: string;
  keyPair: KeyPair;
};

export type AgentKeyStoreDeps = {
  dataDir: string;
  generateKeyPair: () => Promise<KeyPair>;
  /**
   * Sign `payload` with the supplied raw Ed25519 private key. Returns
   * the raw 64-byte detached signature. Used by signChallenge.
   */
  signEd25519: (privateKey: Uint8Array, payload: Uint8Array) => Uint8Array;
  /**
   * Verify an SSH signature block against the supplied public key.
   * Used by verifyDeployCommit.
   */
  verifySSHSig: (
    payload: string,
    signature: string,
    publicKey: Uint8Array,
  ) => boolean;
};

export type AgentKeyStore = {
  /**
   * Load the existing keypair for an agent, or mint and persist a new
   * one. The keypair is also cached in memory so subsequent
   * signChallenge calls do not touch disk. The `isNew` flag is true
   * when the keypair was just generated.
   */
  loadOrGenerateKey(
    address: string,
  ): Promise<{ keyPair: KeyPair; isNew: boolean }>;
  /**
   * Read every persisted keypair in the data directory and warm the
   * in-memory cache with each one. Used by the sidecar's restore path
   * to recover agents across restarts. Directories with a partial
   * keypair (one of the two files present) are skipped with a warning;
   * the operator can re-pair or remove them manually.
   */
  scanKeys(): Promise<AgentKeyEntry[]>;
  /**
   * Sign the challenge payload with the agent's cached private key.
   * Returns null when no key is cached for the address — the caller
   * (HubLink) treats that as "skip this challenge."
   */
  signChallenge(address: string, payload: Uint8Array): Uint8Array | null;
  /**
   * Record the hub public key the agent has been paired with. Cached
   * in memory only; on-disk persistence of the pairing record lives in
   * AgentRepoStore.persistPairing.
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
  ): boolean;
  /**
   * Drop the in-memory caches for an agent. Called on undeploy and on
   * challenge.failed.
   */
  forgetAgent(address: string): void;
};

export function createAgentKeyStore(deps: AgentKeyStoreDeps): AgentKeyStore {
  const { dataDir, generateKeyPair, signEd25519, verifySSHSig } = deps;

  const agentKeys = new Map<string, KeyPair>();
  const hubKeys = new Map<string, Uint8Array>();

  async function loadOrGenerateKey(
    address: string,
  ): Promise<{ keyPair: KeyPair; isNew: boolean }> {
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

    if (privExists) {
      const [privateKey, publicKey] = await Promise.all([
        fsp.readFile(privPath),
        fsp.readFile(pubPath),
      ]);
      const keyPair: KeyPair = {
        privateKey: new Uint8Array(privateKey),
        publicKey: new Uint8Array(publicKey),
      };
      agentKeys.set(address, keyPair);
      return { keyPair, isNew: false };
    }

    const keyPair = await generateKeyPair();
    await fsp.mkdir(keysDir(dataDir, address), { recursive: true });
    await Promise.all([
      fsp.writeFile(privPath, keyPair.privateKey, { mode: 0o600 }),
      fsp.writeFile(pubPath, keyPair.publicKey),
    ]);
    agentKeys.set(address, keyPair);
    return { keyPair, isNew: true };
  }

  async function scanKeys(): Promise<AgentKeyEntry[]> {
    let entries;
    try {
      entries = await fsp.readdir(dataDir, { withFileTypes: true });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "ENOENT") return [];
      throw err;
    }

    const results: AgentKeyEntry[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const dir = `${dataDir}/${entry.name}`;
      const privPath = `${dir}/keys/id_ed25519`;
      const pubPath = `${dir}/keys/id_ed25519.pub`;
      const [privOk, pubOk] = await Promise.all([
        fileExists(privPath),
        fileExists(pubPath),
      ]);
      if (!privOk && !pubOk) continue;
      if (!privOk || !pubOk) {
        logger.warn`Skipping ${entry.name}: incomplete key pair (private=${String(privOk)}, public=${String(pubOk)})`;
        continue;
      }
      const metaPath = `${dir}/agent.json`;
      const metaRaw = await readOptional(metaPath);
      if (metaRaw === null) {
        // A key pair without a metadata file means the agent was
        // half-provisioned and crashed. The restore composer logs
        // and skips it; the operator can pair the key with a new
        // config or remove the directory.
        continue;
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(metaRaw);
      } catch {
        continue;
      }
      if (typeof parsed !== "object" || parsed === null) continue;
      if (!("address" in parsed)) continue;
      const address = parsed.address;
      if (typeof address !== "string") continue;
      const [privateKey, publicKey] = await Promise.all([
        fsp.readFile(privPath),
        fsp.readFile(pubPath),
      ]);
      const keyPair: KeyPair = {
        privateKey: new Uint8Array(privateKey),
        publicKey: new Uint8Array(publicKey),
      };
      agentKeys.set(address, keyPair);
      results.push({ address, keyPair });
    }
    return results;
  }

  function signChallenge(
    address: string,
    payload: Uint8Array,
  ): Uint8Array | null {
    const keyPair = agentKeys.get(address);
    if (keyPair === undefined) return null;
    return signEd25519(keyPair.privateKey, payload);
  }

  function recordHubKey(address: string, hexHubPublicKey: string): void {
    hubKeys.set(address, hexDecode(hexHubPublicKey));
  }

  function verifyDeployCommit(
    address: string,
    payload: string,
    signature: string,
  ): boolean {
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
    scanKeys,
    signChallenge,
    recordHubKey,
    verifyDeployCommit,
    forgetAgent,
  };
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fsp.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function readOptional(filePath: string): Promise<string | null> {
  try {
    return await fsp.readFile(filePath, "utf-8");
  } catch (err: unknown) {
    if (hasCode(err) && err.code === "ENOENT") return null;
    throw err;
  }
}
