// Agent manager: creates and manages harness instances per agent.
//
// Each agent gets its own harness backed by a scoped view of the shared
// InMemoryTransport. The manager handles agent lifecycle (deploy, undeploy,
// abort) in response to control frames from the hub.

import path from "node:path";
import { getLogger } from "@interchange/log";
import { evaluateGrants } from "@interchange/authz";
import {
  createHarness,
  readDeployTree,
  type Harness,
} from "@interchange/harness";
import { hasProvider } from "@interchange/inference";
import { createNodeCrypto } from "@interchange/crypto-node";
import {
  createIsogitStore,
  applyPack,
  createDeployPack,
  currentBranch,
} from "@interchange/storage-isogit";
import type { InMemoryTransport } from "@interchange/message-memory";
import type { GrantRule } from "@interchange/types/authz";
import type {
  InboundMessage,
  InferenceEvent,
  KeyPair,
  HarnessConfig as AgentConfig,
} from "@interchange/types/runtime";

import {
  loadOrGenerateKeyPair,
  hexEncode,
  persistAgentConfig,
  clearAgentConfig,
  scanExistingAgents,
  type AgentKeyEntry,
} from "./key-store";

const logger = getLogger(["interchange", "sidecar", "agents"]);

export type AgentSession = {
  harness: Harness;
  agentAddress: string;
  agentId: string;
  grants: { current: GrantRule[] };
  config: AgentConfig;
};

export type SessionEventSink = (
  agentAddress: string,
  sessionId: string,
  event: InferenceEvent,
) => void;

export type SessionManagerConfig = {
  transport: InMemoryTransport;
  dataDir: string;
  onEvent: SessionEventSink;
};

// Sanitize an agent address into a safe directory name.
// Replaces `@` with `_at_` and any character outside [a-zA-Z0-9_-] with `_`.
export function sanitizeAddress(address: string): string {
  return address.replace(/@/g, "_at_").replace(/[^a-zA-Z0-9_-]/g, "_");
}

export type CreateSessionResult = {
  sessionId: string;
  publicKey: string;
  keyPair: KeyPair;
};

export type RestoreResult = {
  restored: AgentKeyEntry[];
  failed: string[];
};

export type SessionManager = {
  createSession(config: AgentConfig): Promise<CreateSessionResult>;
  destroySession(agentAddress: string): void;
  abortSession(agentAddress: string, reason: string): void;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  updateGrants(agentAddress: string, grants: GrantRule[]): Promise<void>;
  hasSession(agentAddress: string): boolean;
  getAddresses(): string[];
  restoreSessions(): Promise<RestoreResult>;
  applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
  ): Promise<void>;
  createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { transport, dataDir, onEvent } = config;
  const sessions = new Map<string, AgentSession>();
  const pending = new Set<string>();

  async function createSession(
    agentConfig: AgentConfig,
  ): Promise<CreateSessionResult> {
    const { agentAddress } = agentConfig;

    if (sessions.has(agentAddress) || pending.has(agentAddress)) {
      throw new Error(`Session already exists for agent "${agentAddress}"`);
    }

    pending.add(agentAddress);

    try {
      // Generate or load the agent's per-agent key pair.
      const { keyPair, isNew } = await loadOrGenerateKeyPair(
        dataDir,
        agentAddress,
      );
      const crypto = createNodeCrypto(keyPair);

      if (isNew) {
        logger.info`Generated new key pair for ${agentAddress}`;
      }

      await persistAgentConfig(dataDir, agentAddress, agentConfig);

      // Register the agent on the local transport so it can send/receive mail.
      transport.registerAgent(agentAddress, crypto);
      const agentTransport = transport.getTransportForAgent(agentAddress);

      const provider = agentConfig.providers.find((p) =>
        hasProvider(p.provider),
      );
      if (provider === undefined) {
        throw new Error(
          `No inference provider configured for agent "${agentAddress}"`,
        );
      }

      const sessionId = agentConfig.sessionId;

      const storeDir = path.join(dataDir, sanitizeAddress(agentAddress));
      const storage = await createIsogitStore(storeDir);

      // Read tool definitions and prompt from the deploy tree if a deploy
      // pack has previously been applied. When no deploy tree exists (first
      // deploy before any pack arrives), falls back to HarnessConfig values.
      // A subsequent deploy pack takes effect on the next session restart.
      const deployTree = await readDeployTree(storeDir);
      const systemPrompt = deployTree.systemPrompt ?? agentConfig.systemPrompt;

      const { principalId, tenantId } = agentConfig;
      const grantsRef = { current: agentConfig.grants };
      const authorize = async (resource: string, action: string) =>
        evaluateGrants(grantsRef.current, resource, action, {
          principalId,
          tenantId,
        });

      const harness = createHarness({
        address: agentAddress,
        systemPrompt,
        provider: {
          ...provider,
          model: agentConfig.defaultModel,
        },
        transport: agentTransport,
        crypto,
        storage,
        authorize,
        auditStore: storage,
        deployTools: deployTree.tools,
        tools: {
          async run(call) {
            return {
              callId: call.id,
              content: `Tool "${call.name}" is defined in the deploy pack but handler execution is not yet supported`,
              isError: true,
            };
          },
        },
        onEvent(event: InferenceEvent) {
          onEvent(agentAddress, sessionId, event);
        },
      });

      sessions.set(agentAddress, {
        harness,
        agentAddress,
        agentId: agentConfig.agentId,
        grants: grantsRef,
        config: agentConfig,
      });

      harness.start();

      const publicKey = hexEncode(keyPair.publicKey);
      logger.info`Deployed agent ${agentAddress} (session ${sessionId})`;
      return { sessionId, publicKey, keyPair };
    } finally {
      pending.delete(agentAddress);
    }
  }

  function destroySession(agentAddress: string): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    sessions.delete(agentAddress);
    transport.unregisterAgent(agentAddress);
    clearAgentConfig(dataDir, agentAddress).catch((err) => {
      logger.warn`Failed to clear persisted config for ${agentAddress}: ${String(err)}`;
    });
    logger.info`Undeployed agent ${agentAddress}`;
  }

  function abortSession(agentAddress: string, reason: string): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    sessions.delete(agentAddress);
    transport.unregisterAgent(agentAddress);
    clearAgentConfig(dataDir, agentAddress).catch((err) => {
      logger.warn`Failed to clear persisted config for ${agentAddress}: ${String(err)}`;
    });
    logger.info`Aborted agent ${agentAddress}: ${reason}`;
  }

  function deliverMessage(agentAddress: string, message: InboundMessage): void {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.deliver(message);
  }

  function hasSession(agentAddress: string): boolean {
    return sessions.has(agentAddress);
  }

  function getAddresses(): string[] {
    return [...sessions.keys()];
  }

  async function restoreSessions(): Promise<RestoreResult> {
    const existing = await scanExistingAgents(dataDir);
    const restored: AgentKeyEntry[] = [];
    const failed: string[] = [];

    for (const entry of existing) {
      if (sessions.has(entry.address)) {
        restored.push(entry);
        continue;
      }
      try {
        await createSession(entry.config);
        restored.push(entry);
        logger.info`Restored session for ${entry.address}`;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error`Failed to restore session for ${entry.address}: ${msg}`;
        failed.push(entry.address);
      }
    }

    return { restored, failed };
  }

  async function updateGrants(
    agentAddress: string,
    grants: GrantRule[],
  ): Promise<void> {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.grants.current = grants;
    session.config = { ...session.config, grants };
    await persistAgentConfig(dataDir, agentAddress, session.config);
    logger.info`Updated grants for ${agentAddress} (${String(grants.length)} rules)`;
  }

  async function applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
  ): Promise<void> {
    const dir = path.join(dataDir, sanitizeAddress(agentAddress));
    await applyPack(dir, pack, ref, commitSha, transferId);
    logger.info`Applied deploy pack for ${agentAddress} at ${commitSha.slice(0, 8)}`;
  }

  async function createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }

    const dir = path.join(dataDir, sanitizeAddress(agentAddress));
    const branch = await currentBranch(dir);
    const ref = `refs/heads/${branch}`;
    const { pack, commitSha } = await createDeployPack(dir, ref);
    return { pack, commitSha, ref };
  }

  return {
    createSession,
    destroySession,
    abortSession,
    deliverMessage,
    updateGrants,
    hasSession,
    getAddresses,
    restoreSessions,
    applyDeployPack,
    createStatePack,
  };
}
