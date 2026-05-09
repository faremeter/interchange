// Agent manager: creates and manages harness instances per agent.
//
// Each agent gets its own harness backed by a scoped view of the shared
// InMemoryTransport. The manager handles agent lifecycle (deploy, undeploy,
// abort) in response to control frames from the hub.
//
// Deploy is a two-phase operation: provisionAgent sets up disk state (keys,
// repo, config) without starting inference, and startSession reads the
// deploy tree and launches the harness. This separation lets the hub push
// a deploy pack between the two phases so tools are available at session
// start.

import fs from "node:fs";
import path from "node:path";
import git from "isomorphic-git";
import { getLogger } from "@interchange/log";
import { evaluateGrants } from "@interchange/authz";
import {
  createHarness,
  readDeployTree,
  type Harness,
  type DeployToolInfo,
} from "@interchange/harness";
import { createPosixTools } from "@interchange/tools-posix";
import { hasProvider } from "@interchange/inference";
import { createNodeCrypto, createSshSignature } from "@interchange/crypto-node";
import {
  createIsogitStore,
  createMailAuditStore,
  initAgentRepo,
  applyPack,
  createDeployPack,
  currentBranch,
  type CommitVerifier,
  type MailAuditStore,
} from "@interchange/storage-isogit";
import type { InMemoryTransport } from "@interchange/message-memory";
import type { GrantRule } from "@interchange/types/authz";
import type {
  InboundMessage,
  InferenceEvent,
  KeyPair,
  ProviderConfig,
  ToolRunner,
  HarnessConfig as AgentConfig,
} from "@interchange/types/runtime";

import {
  loadOrGenerateKeyPair,
  hexEncode,
  persistAgentConfig,
  scanExistingAgents,
  type AgentKeyEntry,
} from "./key-store";

const logger = getLogger(["interchange", "sidecar", "agents"]);

function hasCode(err: unknown): err is { code: string } {
  return (
    typeof err === "object" &&
    err !== null &&
    "code" in err &&
    typeof (err as { code: unknown }).code === "string"
  );
}

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

export type ProvisionResult = {
  publicKey: string;
  keyPair: KeyPair;
};

export type RestoreResult = {
  restored: AgentKeyEntry[];
  failed: string[];
};

export type SessionManager = {
  provisionAgent(config: AgentConfig): Promise<ProvisionResult>;
  startSession(agentAddress: string): Promise<void>;
  destroySession(agentAddress: string): Promise<void>;
  abortSession(agentAddress: string, reason: string): Promise<void>;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  updateGrants(agentAddress: string, grants: GrantRule[]): Promise<void>;
  updateProviders(
    agentAddress: string,
    providers: ProviderConfig[],
  ): Promise<void>;
  hasSession(agentAddress: string): boolean;
  isProvisioned(agentAddress: string): boolean;
  getAddresses(): string[];
  restoreSessions(): Promise<RestoreResult>;
  applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
    verifyCommit?: CommitVerifier,
  ): Promise<void>;
  createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }>;
  deleteAgentDir(agentAddress: string): Promise<void>;
  getDeployRef(agentAddress: string): Promise<string | null>;
  persistHubPublicKey(
    agentAddress: string,
    hubPublicKey: string,
  ): Promise<void>;
  commitInboundMail(
    agentAddress: string,
    rawMessage: Uint8Array,
  ): Promise<void>;
  getSessionId(agentAddress: string): string | undefined;
};

// Agents that have been provisioned but not yet started. Holds the config
// needed to create the harness once startSession is called.
type ProvisionedAgent = {
  config: AgentConfig;
  keyPair: KeyPair;
};

export function buildToolDispatch(deployTools: DeployToolInfo[]): ToolRunner {
  const posixTools = createPosixTools();
  const posixNames = new Set(posixTools.definitions.map((d) => d.name));
  const handlerIndex = new Set(
    deployTools.filter((t) => t.hasHandler).map((t) => t.definition.name),
  );
  const deployNames = new Set(deployTools.map((t) => t.definition.name));

  return {
    async run(call, signal) {
      if (handlerIndex.has(call.name)) {
        return {
          callId: call.id,
          content: `Tool "${call.name}" has a handler.ts but custom handler execution is not yet implemented`,
          isError: true,
        };
      }

      if (posixNames.has(call.name)) {
        return posixTools.run(call, signal);
      }

      if (deployNames.has(call.name)) {
        return {
          callId: call.id,
          content: `Tool "${call.name}" is declared in the deploy tree but has no handler and does not match a built-in tool`,
          isError: true,
        };
      }

      return {
        callId: call.id,
        content: `Unknown tool: "${call.name}"`,
        isError: true,
      };
    },
  };
}

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { transport, dataDir, onEvent } = config;
  const sessions = new Map<string, AgentSession>();
  const provisioned = new Map<string, ProvisionedAgent>();
  const pending = new Set<string>();
  const mailStores = new Map<string, MailAuditStore>();

  // Per-agent promise chain to serialize mail commits and avoid concurrent
  // git operations on the same repository.
  const mailCommitQueues = new Map<string, Promise<void>>();

  function enqueueMailCommit(
    agentAddress: string,
    fn: () => Promise<void>,
  ): void {
    const prev = mailCommitQueues.get(agentAddress) ?? Promise.resolve();
    const next = prev
      .catch(() => undefined)
      .then(fn)
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error`Mail audit commit failed for ${agentAddress}: ${msg}`;
      });
    mailCommitQueues.set(agentAddress, next);
  }

  async function drainMailQueue(agentAddress: string): Promise<void> {
    const pending = mailCommitQueues.get(agentAddress);
    mailCommitQueues.delete(agentAddress);
    if (pending !== undefined) await pending;
  }

  transport.addMessageSentHandler(
    async ({ senderAddress, rawMessage, messageId }) => {
      const store = mailStores.get(senderAddress);
      if (store === undefined) {
        logger.warn`No mail store for sender ${senderAddress}, skipping outbound audit for ${messageId}`;
        return;
      }
      enqueueMailCommit(senderAddress, async () => {
        const result = await store.commitMail(rawMessage, "out", {
          ignoreDuplicate: true,
        });
        if (result !== null) {
          logger.info`Committed outbound mail ${messageId} for ${senderAddress}`;
        }
      });
    },
  );

  async function provisionAgent(
    agentConfig: AgentConfig,
  ): Promise<ProvisionResult> {
    const { agentAddress } = agentConfig;

    if (
      sessions.has(agentAddress) ||
      provisioned.has(agentAddress) ||
      pending.has(agentAddress)
    ) {
      throw new Error(`Agent already exists for address "${agentAddress}"`);
    }

    pending.add(agentAddress);

    try {
      const { keyPair, isNew } = await loadOrGenerateKeyPair(
        dataDir,
        agentAddress,
      );

      if (isNew) {
        logger.info`Generated new key pair for ${agentAddress}`;
      }

      const storeDir = path.join(dataDir, sanitizeAddress(agentAddress));
      await initAgentRepo(storeDir);
      await persistAgentConfig(dataDir, agentAddress, agentConfig);

      provisioned.set(agentAddress, { config: agentConfig, keyPair });

      const publicKey = hexEncode(keyPair.publicKey);
      logger.info`Provisioned agent ${agentAddress}`;
      return { publicKey, keyPair };
    } finally {
      pending.delete(agentAddress);
    }
  }

  async function startSession(agentAddress: string): Promise<void> {
    const entry = provisioned.get(agentAddress);
    if (entry === undefined) {
      throw new Error(`No provisioned agent for address "${agentAddress}"`);
    }
    if (sessions.has(agentAddress)) {
      throw new Error(`Session already running for agent "${agentAddress}"`);
    }

    const { config: agentConfig, keyPair } = entry;

    const provider = agentConfig.providers.find((p) => hasProvider(p.provider));
    if (provider === undefined) {
      throw new Error(
        `No inference provider configured for agent "${agentAddress}"`,
      );
    }

    provisioned.delete(agentAddress);

    try {
      const crypto = createNodeCrypto(keyPair);

      transport.registerAgent(agentAddress, crypto);
      const agentTransport = transport.getTransportForAgent(agentAddress);

      const sessionId = agentConfig.sessionId;

      const storeDir = path.join(dataDir, sanitizeAddress(agentAddress));
      const signer = async (payload: string) =>
        createSshSignature(payload, keyPair.privateKey, keyPair.publicKey);
      const storage = await createIsogitStore(storeDir, signer);
      const mailStore = await createMailAuditStore(storeDir, signer);
      mailStores.set(agentAddress, mailStore);

      const deployTree = await readDeployTree(storeDir);
      const systemPrompt = deployTree.systemPrompt ?? agentConfig.systemPrompt;

      const { principalId, tenantId } = agentConfig;
      const grantsRef = { current: agentConfig.grants };
      const authorize = async (resource: string, action: string) =>
        evaluateGrants(grantsRef.current, resource, action, {
          principalId,
          tenantId,
        });

      const deployToolDefs = deployTree.tools.map((t) => t.definition);
      const toolDispatch = buildToolDispatch(deployTree.tools);

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
        deployTools: deployToolDefs,
        tools: toolDispatch,
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
      logger.info`Started session for ${agentAddress} (session ${sessionId})`;
    } catch (err) {
      sessions.delete(agentAddress);
      mailStores.delete(agentAddress);
      mailCommitQueues.delete(agentAddress);
      try {
        transport.unregisterAgent(agentAddress);
      } catch (cleanupErr) {
        // Best-effort cleanup; don't mask the original error.
        logger.error`Failed to unregister transport for ${agentAddress}: ${String(cleanupErr)}`;
      }
      provisioned.set(agentAddress, entry);
      throw err;
    }
  }

  async function destroySession(agentAddress: string): Promise<void> {
    if (provisioned.has(agentAddress)) {
      provisioned.delete(agentAddress);
      logger.info`Removed provisioned agent ${agentAddress}`;
      return;
    }
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    await drainMailQueue(agentAddress);
    sessions.delete(agentAddress);
    mailStores.delete(agentAddress);
    transport.unregisterAgent(agentAddress);
    logger.info`Stopped session for ${agentAddress}`;
  }

  async function abortSession(
    agentAddress: string,
    reason: string,
  ): Promise<void> {
    if (provisioned.has(agentAddress)) {
      provisioned.delete(agentAddress);
      logger.info`Aborted provisioned agent ${agentAddress}: ${reason}`;
      return;
    }
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    session.harness.stop();
    await drainMailQueue(agentAddress);
    sessions.delete(agentAddress);
    mailStores.delete(agentAddress);
    transport.unregisterAgent(agentAddress);
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

  function isProvisioned(agentAddress: string): boolean {
    return provisioned.has(agentAddress);
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
        await provisionAgent(entry.config);
        await startSession(entry.address);
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

  async function updateProviders(
    agentAddress: string,
    providers: ProviderConfig[],
  ): Promise<void> {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    const provider = providers.find((p) => hasProvider(p.provider));
    if (provider === undefined) {
      throw new Error(
        `No supported provider in update for agent "${agentAddress}"`,
      );
    }
    session.harness.setProviderConfig({
      ...provider,
      model: session.config.defaultModel,
    });
    session.config = { ...session.config, providers };
    await persistAgentConfig(dataDir, agentAddress, session.config);
    logger.info`Updated providers for ${agentAddress}`;
  }

  async function applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
    verifyCommit?: CommitVerifier,
  ): Promise<void> {
    const dir = path.join(dataDir, sanitizeAddress(agentAddress));
    await applyPack(dir, pack, ref, commitSha, transferId, verifyCommit);
    logger.info`Applied deploy pack for ${agentAddress} at ${commitSha.slice(0, 8)}`;
  }

  async function createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    const dir = path.join(dataDir, sanitizeAddress(agentAddress));
    const branch = await currentBranch(dir);
    const ref = `refs/heads/${branch}`;
    const { pack, commitSha } = await createDeployPack(dir, ref);
    return { pack, commitSha, ref };
  }

  async function deleteAgentDir(agentAddress: string): Promise<void> {
    const agentDir = path.join(dataDir, sanitizeAddress(agentAddress));
    await fs.promises.rm(agentDir, { recursive: true });
    logger.info`Deleted agent directory for ${agentAddress}`;
  }

  async function persistHubPublicKey(
    agentAddress: string,
    hubPublicKey: string,
  ): Promise<void> {
    const session = sessions.get(agentAddress);
    const prov = provisioned.get(agentAddress);
    const config = session?.config ?? prov?.config;
    if (config === undefined) {
      throw new Error(
        `Cannot persist hub key: no config for "${agentAddress}"`,
      );
    }
    await persistAgentConfig(dataDir, agentAddress, config, hubPublicKey);
  }

  async function commitInboundMail(
    agentAddress: string,
    rawMessage: Uint8Array,
  ): Promise<void> {
    const store = mailStores.get(agentAddress);
    if (store === undefined) {
      logger.warn`No mail store for ${agentAddress}, skipping inbound audit`;
      return;
    }
    enqueueMailCommit(agentAddress, async () => {
      const result = await store.commitMail(rawMessage, "in", {
        ignoreDuplicate: true,
      });
      if (result !== null) {
        logger.info`Committed inbound mail ${result.messageId} for ${agentAddress}`;
      }
    });
  }

  function getSessionId(agentAddress: string): string | undefined {
    return sessions.get(agentAddress)?.config.sessionId;
  }

  async function getDeployRef(agentAddress: string): Promise<string | null> {
    const dir = path.join(dataDir, sanitizeAddress(agentAddress));
    try {
      return await git.resolveRef({ fs, dir, ref: "refs/heads/deploy" });
    } catch (err: unknown) {
      if (hasCode(err) && err.code === "NotFoundError") {
        return null;
      }
      throw err;
    }
  }

  return {
    provisionAgent,
    startSession,
    destroySession,
    abortSession,
    deliverMessage,
    updateGrants,
    updateProviders,
    hasSession,
    isProvisioned,
    getAddresses,
    restoreSessions,
    applyDeployPack,
    createStatePack,
    deleteAgentDir,
    getDeployRef,
    persistHubPublicKey,
    commitInboundMail,
    getSessionId,
  };
}
