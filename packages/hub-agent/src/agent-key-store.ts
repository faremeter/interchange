// Per-agent Ed25519 key custody.
//
// Persists key pairs as raw 32-byte binary files on disk and produces
// them on demand. The cryptographic primitives (keypair generation,
// challenge signing, deploy-commit verification) are host-supplied so
// the package does not pin a particular crypto backend. Today only
// generation is injected; signing and verification join this seam in
// the HarnessBuilder/AgentCrypto work later in the extraction series.

import fsp from "node:fs/promises";
import { getLogger } from "@intx/log";
import { hasCode } from "@intx/types";
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
};

export type AgentKeyStore = {
  /**
   * Load the existing keypair for an agent, or mint and persist a new
   * one. The `isNew` flag is true when the keypair was just generated.
   */
  loadOrGenerateKey(
    address: string,
  ): Promise<{ keyPair: KeyPair; isNew: boolean }>;
  /**
   * Read every persisted keypair in the data directory. Used by the
   * sidecar's restore path to recover agents across restarts. Directories
   * with a partial keypair (one of the two files present) are skipped
   * with a warning; the operator can re-pair or remove them manually.
   */
  scanKeys(): Promise<AgentKeyEntry[]>;
};

export function createAgentKeyStore(deps: AgentKeyStoreDeps): AgentKeyStore {
  const { dataDir, generateKeyPair } = deps;

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
      return {
        keyPair: {
          privateKey: new Uint8Array(privateKey),
          publicKey: new Uint8Array(publicKey),
        },
        isNew: false,
      };
    }

    const keyPair = await generateKeyPair();
    await fsp.mkdir(keysDir(dataDir, address), { recursive: true });
    await Promise.all([
      fsp.writeFile(privPath, keyPair.privateKey, { mode: 0o600 }),
      fsp.writeFile(pubPath, keyPair.publicKey),
    ]);
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
      results.push({
        address,
        keyPair: {
          privateKey: new Uint8Array(privateKey),
          publicKey: new Uint8Array(publicKey),
        },
      });
    }
    return results;
  }

  return { loadOrGenerateKey, scanKeys };
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
