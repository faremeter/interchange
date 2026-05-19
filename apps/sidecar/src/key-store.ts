// Per-agent Ed25519 key pair persistence.
//
// Each agent's key pair is stored as raw 32-byte binary files alongside
// its isogit repository. An agent.json metadata file stores the original
// agent address since sanitizeAddress is lossy and the directory name
// cannot be reliably reversed.

import fs from "node:fs/promises";
import path from "node:path";
import { type } from "arktype";
import { getLogger } from "@intx/log";
import { generateKeyPair } from "@intx/crypto-node";
import { type KeyPair, HarnessConfig } from "@intx/types/runtime";
import { sanitizeAddress } from "./session-manager";
import { hasCode } from "./errors";

const logger = getLogger(["interchange", "sidecar", "keystore"]);

const AgentMeta = type({
  version: "1",
  address: "string",
  config: HarnessConfig,
  "hubPublicKey?": "string",
});
type AgentMeta = typeof AgentMeta.infer;

export type AgentKeyEntry = {
  address: string;
  keyPair: KeyPair;
  config: HarnessConfig;
  hubPublicKey?: string;
};

/**
 * Load an existing key pair for an agent, or generate and persist a new one.
 *
 * Keys are stored as raw 32-byte binary files at:
 *   <dataDir>/<sanitized-address>/keys/id_ed25519      (private key)
 *   <dataDir>/<sanitized-address>/keys/id_ed25519.pub   (public key)
 *   <dataDir>/<sanitized-address>/agent.json            (original address)
 */
export async function loadOrGenerateKeyPair(
  dataDir: string,
  agentAddress: string,
): Promise<{ keyPair: KeyPair; isNew: boolean }> {
  const agentDir = path.join(dataDir, sanitizeAddress(agentAddress));
  const keysDir = path.join(agentDir, "keys");
  const privPath = path.join(keysDir, "id_ed25519");
  const pubPath = path.join(keysDir, "id_ed25519.pub");
  const metaPath = path.join(agentDir, "agent.json");

  const privExists = await fileExists(privPath);
  const pubExists = await fileExists(pubPath);

  if (privExists !== pubExists) {
    const missing = privExists ? "public" : "private";
    throw new Error(
      `Corrupt key pair for "${agentAddress}": ${missing} key file is missing`,
    );
  }

  if (privExists) {
    const [privateKey, publicKey] = await Promise.all([
      fs.readFile(privPath),
      fs.readFile(pubPath),
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

  await fs.mkdir(keysDir, { recursive: true });
  await Promise.all([
    fs.writeFile(privPath, keyPair.privateKey, { mode: 0o600 }),
    fs.writeFile(pubPath, keyPair.publicKey),
    fs.writeFile(
      metaPath,
      JSON.stringify({ version: 1, address: agentAddress }),
    ),
  ]);

  return { keyPair, isNew: true };
}

/**
 * Persist the agent's harness config to its agent.json metadata file.
 * Called after a session is successfully created so the sidecar can
 * restore the session on restart.
 *
 * Reads the existing file first to preserve fields not explicitly
 * provided (e.g. hubPublicKey written by a prior call).
 */
export async function persistAgentConfig(
  dataDir: string,
  agentAddress: string,
  config: HarnessConfig,
  hubPublicKey?: string,
): Promise<void> {
  const agentDir = path.join(dataDir, sanitizeAddress(agentAddress));
  const metaPath = path.join(agentDir, "agent.json");

  let existingHubPublicKey: string | undefined;
  try {
    const raw = await fs.readFile(metaPath, "utf-8");
    const parsed = AgentMeta(JSON.parse(raw));
    if (!(parsed instanceof type.errors)) {
      existingHubPublicKey = parsed.hubPublicKey;
    }
  } catch (err: unknown) {
    if (hasCode(err) && err.code === "ENOENT") {
      // File doesn't exist yet — start fresh.
    } else {
      throw err;
    }
  }

  const meta: AgentMeta = { version: 1, address: agentAddress, config };
  if (hubPublicKey !== undefined) {
    meta.hubPublicKey = hubPublicKey;
  } else if (existingHubPublicKey !== undefined) {
    meta.hubPublicKey = existingHubPublicKey;
  }
  await fs.writeFile(metaPath, JSON.stringify(meta));
}

/**
 * Remove the persisted config from agent.json, leaving only the address.
 * Called when a session is destroyed so the agent is not restored on restart.
 */
export async function clearAgentConfig(
  dataDir: string,
  agentAddress: string,
): Promise<void> {
  const agentDir = path.join(dataDir, sanitizeAddress(agentAddress));
  const metaPath = path.join(agentDir, "agent.json");
  await fs.writeFile(
    metaPath,
    JSON.stringify({ version: 1, address: agentAddress }),
  );
}

/**
 * Scan the data directory for agent repositories that have key pairs
 * and persisted configs. Returns the address, key pair, and config for
 * each restorable agent.
 *
 * Agents with key pairs but no persisted config are skipped with a
 * warning — they exist on disk but cannot be restored without a
 * re-deploy from the hub.
 */
export async function scanExistingAgents(
  dataDir: string,
): Promise<AgentKeyEntry[]> {
  const dirExists = await fileExists(dataDir);
  if (!dirExists) return [];

  const entries = await fs.readdir(dataDir, { withFileTypes: true });
  const results: AgentKeyEntry[] = [];

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;

    const agentDir = path.join(dataDir, entry.name);
    const privPath = path.join(agentDir, "keys", "id_ed25519");
    const pubPath = path.join(agentDir, "keys", "id_ed25519.pub");
    const metaPath = path.join(agentDir, "agent.json");

    const [privOk, pubOk, metaOk] = await Promise.all([
      fileExists(privPath),
      fileExists(pubPath),
      fileExists(metaPath),
    ]);

    if (!privOk || !pubOk) {
      if (privOk || pubOk || metaOk) {
        logger.warn`Skipping ${entry.name}: incomplete key pair (private=${String(privOk)}, public=${String(pubOk)})`;
      }
      continue;
    }

    if (!metaOk) {
      logger.warn`Skipping ${entry.name}: missing agent.json`;
      continue;
    }

    const [privateKey, publicKey, metaRaw] = await Promise.all([
      fs.readFile(privPath),
      fs.readFile(pubPath),
      fs.readFile(metaPath, "utf-8"),
    ]);

    const parsed: unknown = JSON.parse(metaRaw);
    const meta = AgentMeta(parsed);
    if (meta instanceof type.errors) {
      logger.warn`Skipping ${entry.name}: invalid agent.json: ${meta.summary}`;
      continue;
    }

    const agentEntry: AgentKeyEntry = {
      address: meta.address,
      config: meta.config,
      keyPair: {
        privateKey: new Uint8Array(privateKey),
        publicKey: new Uint8Array(publicKey),
      },
    };
    if (meta.hubPublicKey !== undefined) {
      agentEntry.hubPublicKey = meta.hubPublicKey;
    }
    results.push(agentEntry);
  }

  return results;
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}
