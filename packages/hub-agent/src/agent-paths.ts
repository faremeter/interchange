// Per-agent on-disk layout helpers.
//
// The sanitization scheme is an internal implementation detail: it gives
// each agent address a stable filesystem-safe directory name, but the
// mapping is lossy and the directory name cannot be reversed. Callers
// that need to find an agent's directory must go through this module
// (or through AgentRepoStore.getAgentDir) rather than computing the
// path independently.

import path from "node:path";

// The per-agent key-file layout the address-keyed helpers below build on.
const KEYS_DIR_NAME = "keys";
const PRIVATE_KEY_FILE = "id_ed25519";
const PUBLIC_KEY_FILE = "id_ed25519.pub";

export function sanitizeAddress(address: string): string {
  return address.replace(/@/g, "_at_").replace(/[^a-zA-Z0-9_-]/g, "_");
}

export function agentDir(dataDir: string, address: string): string {
  return path.join(dataDir, sanitizeAddress(address));
}

export function keysDir(dataDir: string, address: string): string {
  return path.join(agentDir(dataDir, address), KEYS_DIR_NAME);
}

export function privateKeyPath(dataDir: string, address: string): string {
  return path.join(keysDir(dataDir, address), PRIVATE_KEY_FILE);
}

export function publicKeyPath(dataDir: string, address: string): string {
  return path.join(keysDir(dataDir, address), PUBLIC_KEY_FILE);
}
