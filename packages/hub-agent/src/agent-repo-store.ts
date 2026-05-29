// Per-agent on-disk repository layout.
//
// Owns the isogit repo wrapper, the deploy-pack apply / state-pack
// produce flow, and the persistence of the per-agent `agent.json`
// metadata blob. Key custody lives in AgentKeyStore alongside this
// store; both share the directory-layout helpers in agent-paths.

import fs from "node:fs";
import fsp from "node:fs/promises";
import git from "isomorphic-git";
import { type } from "arktype";
import { getLogger } from "@intx/log";
import { hasCode } from "@intx/types";
import { HarnessConfig } from "@intx/types/runtime";
import {
  initAgentRepo,
  applyPack,
  createDeployPack,
  currentBranch,
  type CommitVerifier,
} from "@intx/storage-isogit";

import { agentDir, metaPath } from "./agent-paths";

const logger = getLogger(["interchange", "hub-agent", "repo-store"]);

const AgentMeta = type({
  version: "1",
  address: "string",
  config: HarnessConfig,
  "hubPublicKey?": "string",
});
type AgentMeta = typeof AgentMeta.infer;

/**
 * A per-agent metadata record as it appears on disk. `hubPublicKey`
 * — the hub identity this agent has been paired with — is set once
 * by `persistPairing` and is otherwise preserved across `persistConfig`
 * updates.
 */
export type AgentConfigEntry = {
  address: string;
  config: HarnessConfig;
  hubPublicKey?: string;
};

export type ApplyDeployPackArgs = {
  address: string;
  pack: Uint8Array;
  ref: string;
  commitSha: string;
  transferId: string;
  verifyCommit?: CommitVerifier;
};

export type AgentRepoStore = {
  /**
   * Resolve the on-disk directory for an agent. Exposed for integration
   * tests that need to assert against disk state without depending on
   * the (intentionally opaque) directory naming scheme.
   */
  getAgentDir(address: string): string;
  initRepo(address: string): Promise<void>;
  applyDeployPack(args: ApplyDeployPackArgs): Promise<void>;
  createStatePack(
    address: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
  getDeployRef(address: string): Promise<string | null>;
  remove(address: string): Promise<void>;
  /**
   * Write the agent's HarnessConfig to disk. Preserves the existing
   * `hubPublicKey` field if one was previously recorded by
   * `persistPairing`. To update the pairing key, call `persistPairing`
   * separately.
   */
  persistConfig(address: string, config: HarnessConfig): Promise<void>;
  /**
   * Record the hub public key this agent has been paired with. Set-once
   * in normal operation; rewriting an existing value is permitted but
   * indicates the agent has been re-paired with a different hub.
   */
  persistPairing(address: string, hubPublicKey: string): Promise<void>;
  /**
   * Scan the data directory for agent.json files and return the
   * persisted config + pairing key for each. Files that fail to
   * parse are skipped with a warning.
   */
  scanConfigs(): Promise<AgentConfigEntry[]>;
};

export function createAgentRepoStore(config: {
  dataDir: string;
}): AgentRepoStore {
  const { dataDir } = config;

  function getAgentDir(address: string): string {
    return agentDir(dataDir, address);
  }

  async function initRepo(address: string): Promise<void> {
    await initAgentRepo(getAgentDir(address));
  }

  async function applyDeployPackImpl(args: ApplyDeployPackArgs): Promise<void> {
    const { address, pack, ref, commitSha, transferId, verifyCommit } = args;
    await applyPack(
      getAgentDir(address),
      pack,
      ref,
      commitSha,
      transferId,
      verifyCommit,
    );
    logger.info`Applied deploy pack for ${address} at ${commitSha.slice(0, 8)}`;
  }

  async function createStatePack(
    address: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    const dir = getAgentDir(address);
    const branch = await currentBranch(dir);
    const ref = `refs/heads/${branch}`;
    const { pack, commitSha } = await createDeployPack(dir, ref);
    return { pack, commitSha, ref };
  }

  async function getDeployRef(address: string): Promise<string | null> {
    const dir = getAgentDir(address);
    try {
      return await git.resolveRef({ fs, dir, ref: "refs/heads/deploy" });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return null;
      }
      throw err;
    }
  }

  async function remove(address: string): Promise<void> {
    await fsp.rm(getAgentDir(address), { recursive: true });
    logger.info`Deleted agent directory for ${address}`;
  }

  async function readMeta(address: string): Promise<AgentMeta | null> {
    try {
      const raw = await fsp.readFile(metaPath(dataDir, address), "utf-8");
      const parsed: unknown = JSON.parse(raw);
      const validated = AgentMeta(parsed);
      if (validated instanceof type.errors) {
        logger.warn`agent.json invalid for ${address}: ${validated.summary}`;
        return null;
      }
      return validated;
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "ENOENT") {
        return null;
      }
      throw err;
    }
  }

  async function writeMeta(meta: AgentMeta): Promise<void> {
    await fsp.writeFile(metaPath(dataDir, meta.address), JSON.stringify(meta));
  }

  async function persistConfig(
    address: string,
    config: HarnessConfig,
  ): Promise<void> {
    const existing = await readMeta(address);
    const meta: AgentMeta = { version: 1, address, config };
    if (existing?.hubPublicKey !== undefined) {
      meta.hubPublicKey = existing.hubPublicKey;
    }
    await writeMeta(meta);
  }

  async function persistPairing(
    address: string,
    hubPublicKey: string,
  ): Promise<void> {
    const existing = await readMeta(address);
    if (existing === null) {
      throw new Error(
        `Cannot persist hub pairing for "${address}": no existing agent.json`,
      );
    }
    await writeMeta({ ...existing, hubPublicKey });
  }

  async function scanConfigs(): Promise<AgentConfigEntry[]> {
    let entries: fs.Dirent[];
    try {
      entries = await fsp.readdir(dataDir, { withFileTypes: true });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "ENOENT") return [];
      throw err;
    }

    const results: AgentConfigEntry[] = [];
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      // Read agent.json directly by directory name — we cannot reverse
      // sanitizeAddress, so the address comes from the file's `address`
      // field, not the directory name.
      const metaFile = `${dataDir}/${entry.name}/agent.json`;
      let raw: string;
      try {
        raw = await fsp.readFile(metaFile, "utf-8");
      } catch (err: unknown) {
        if (hasCode(err) && err.code === "ENOENT") continue;
        throw err;
      }
      const parsed: unknown = JSON.parse(raw);
      const validated = AgentMeta(parsed);
      if (validated instanceof type.errors) {
        logger.warn`Skipping ${entry.name}: invalid agent.json: ${validated.summary}`;
        continue;
      }
      const result: AgentConfigEntry = {
        address: validated.address,
        config: validated.config,
      };
      if (validated.hubPublicKey !== undefined) {
        result.hubPublicKey = validated.hubPublicKey;
      }
      results.push(result);
    }
    return results;
  }

  return {
    getAgentDir,
    initRepo,
    applyDeployPack: applyDeployPackImpl,
    createStatePack,
    getDeployRef,
    remove,
    persistConfig,
    persistPairing,
    scanConfigs,
  };
}
