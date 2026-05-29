// Per-agent on-disk layout helpers.
//
// The sanitization scheme is an internal implementation detail: it gives
// each agent address a stable filesystem-safe directory name, but the
// mapping is lossy and the directory name cannot be reversed. Callers
// that need to find an agent's directory must go through this module
// (or through AgentRepoStore.getAgentDir) rather than computing the
// path independently.

import path from "node:path";

export function sanitizeAddress(address: string): string {
  return address.replace(/@/g, "_at_").replace(/[^a-zA-Z0-9_-]/g, "_");
}

export function agentDir(dataDir: string, address: string): string {
  return path.join(dataDir, sanitizeAddress(address));
}

export function keysDir(dataDir: string, address: string): string {
  return path.join(agentDir(dataDir, address), "keys");
}

export function privateKeyPath(dataDir: string, address: string): string {
  return path.join(keysDir(dataDir, address), "id_ed25519");
}

export function publicKeyPath(dataDir: string, address: string): string {
  return path.join(keysDir(dataDir, address), "id_ed25519.pub");
}

export function metaPath(dataDir: string, address: string): string {
  return path.join(agentDir(dataDir, address), "agent.json");
}
