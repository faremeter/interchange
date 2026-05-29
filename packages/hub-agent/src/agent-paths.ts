// Per-agent on-disk layout helpers.
//
// The sanitization scheme is an internal implementation detail: it gives
// each agent address a stable filesystem-safe directory name, but the
// mapping is lossy and the directory name cannot be reversed. Callers
// that need to find an agent's directory must go through this module
// (or through AgentRepoStore.getAgentDir) rather than computing the
// path independently.

import path from "node:path";

// The single source of truth for the per-agent layout filenames. The
// scan paths in agent-repo-store and agent-key-store cannot use the
// address-keyed helpers below because they walk the data directory
// before any agent.json has been parsed — those callers join these
// constants against the on-disk directory name directly.
export const KEYS_DIR_NAME = "keys";
export const PRIVATE_KEY_FILE = "id_ed25519";
export const PUBLIC_KEY_FILE = "id_ed25519.pub";
export const META_FILE = "agent.json";

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

export function metaPath(dataDir: string, address: string): string {
  return path.join(agentDir(dataDir, address), META_FILE);
}
