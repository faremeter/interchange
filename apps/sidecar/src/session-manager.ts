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
import { getLogger } from "@intx/log";
import { evaluateGrants } from "@intx/authz";
import { createHarness, readDeployTree, type Harness } from "@intx/harness";
import { createPosixTools } from "@intx/tools-posix";
import { createLSPPlugin } from "@intx/tools-lsp";
import { hasProvider } from "@intx/inference";
import { createNodeCrypto, createSshSignature } from "@intx/crypto-node";
import {
  createIsogitStore,
  createMailAuditStore,
  type CommitVerifier,
  type MailAuditStore,
} from "@intx/storage-isogit";
import type { InMemoryTransport } from "@intx/mail-memory";
import type { GrantRule } from "@intx/types/authz";
import type {
  ConnectorThreadState,
  InboundMessage,
  InferenceEvent,
  InferenceSource,
  KeyPair,
  HarnessConfig as AgentConfig,
} from "@intx/types/runtime";
import { createBlobReader } from "@intx/types/runtime";

import { hexEncode } from "@intx/types";
import type { AgentKeyStore, AgentRepoStore } from "@intx/hub-agent";

const logger = getLogger(["interchange", "sidecar", "agents"]);

export type AgentSession = {
  harness: Harness;
  agentAddress: string;
  agentId: string;
  grants: { current: GrantRule[] };
  config: AgentConfig;
  disposers: (() => Promise<void>)[];
};

export type SessionEventSink = (
  agentAddress: string,
  sessionId: string,
  event: InferenceEvent,
) => void;

export type ConnectorStateSink = (
  agentAddress: string,
  state: ConnectorThreadState | null,
) => void;

export type SessionManagerConfig = {
  transport: InMemoryTransport;
  repoStore: AgentRepoStore;
  keyStore: AgentKeyStore;
  onEvent: SessionEventSink;
  onConnectorStateChanged: ConnectorStateSink;
};

export type ProvisionResult = {
  publicKey: string;
  keyPair: KeyPair;
};

export type RestoredAgent = {
  address: string;
  keyPair: KeyPair;
  hubPublicKey?: string;
};

export type RestoreResult = {
  restored: RestoredAgent[];
  failed: string[];
};

export type SessionManager = {
  provisionAgent(config: AgentConfig): Promise<ProvisionResult>;
  startSession(agentAddress: string): Promise<void>;
  destroySession(agentAddress: string): Promise<void>;
  abortSession(agentAddress: string, reason: string): Promise<void>;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  updateGrants(agentAddress: string, grants: GrantRule[]): Promise<void>;
  updateSources(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
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

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const { transport, repoStore, keyStore, onEvent, onConnectorStateChanged } =
    config;
  const sessions = new Map<string, AgentSession>();
  const provisioned = new Map<string, ProvisionedAgent>();
  const pending = new Set<string>();
  const mailStores = new Map<string, MailAuditStore>();

  // Checkpoint hash captured from the most recent connector.reply event for
  // each agent. Consumed (deleted) by the MessageSentHandler so that only
  // the connector reply mail commit receives the linkage — subsequent
  // tool-initiated sends do not carry a stale hash.
  //
  // Ordering guarantee: the harness calls onEvent() synchronously inside
  // handleEvent(), which fires before the detached transport.send() resolves.
  // Because executeSend() always awaits at least once (signature generation)
  // before calling MessageSentHandler, the map write in onEvent completes
  // before the handler reads it.
  const lastCheckpointHashes = new Map<string, string>();

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
      const checkpointHash = lastCheckpointHashes.get(senderAddress);
      lastCheckpointHashes.delete(senderAddress);
      enqueueMailCommit(senderAddress, async () => {
        const result = await store.commitMail(rawMessage, "out", {
          ignoreDuplicate: true,
          ...(checkpointHash !== undefined ? { checkpointHash } : {}),
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
      const { keyPair, isNew } = await keyStore.loadOrGenerateKey(agentAddress);

      if (isNew) {
        logger.info`Generated new key pair for ${agentAddress}`;
      }

      await repoStore.initRepo(agentAddress);
      await repoStore.persistConfig(agentAddress, agentConfig);

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

    const source = agentConfig.sources.find(
      (s) => s.id === agentConfig.defaultSource,
    );
    if (source === undefined) {
      throw new Error(
        `No source matches defaultSource "${agentConfig.defaultSource}" for agent "${agentAddress}"`,
      );
    }
    if (!hasProvider(source.provider)) {
      throw new Error(
        `Source provider "${source.provider}" is not registered for agent "${agentAddress}"`,
      );
    }

    provisioned.delete(agentAddress);

    try {
      const crypto = createNodeCrypto(keyPair);

      transport.register(agentAddress, crypto);
      const agentTransport = transport.getTransportFor(agentAddress);

      const sessionId = agentConfig.sessionId;

      const storeDir = repoStore.getAgentDir(agentAddress);
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

      const workDir = path.join(storeDir, "workspace");
      await fs.promises.mkdir(workDir, { recursive: true });

      const blobReader = createBlobReader(storage);
      const posixTools = createPosixTools({
        cwd: workDir,
        plugins: [createLSPPlugin({ cwd: workDir })],
        blobReader,
      });

      const harness = createHarness({
        address: agentAddress,
        systemPrompt,
        source,
        transport: agentTransport,
        crypto,
        storage,
        authorize,
        auditStore: storage,
        deployTools: deployTree.tools,
        tools: posixTools,
        onEvent(event: InferenceEvent) {
          if (
            event.type === "connector.reply" &&
            event.data.checkpointHash !== undefined
          ) {
            lastCheckpointHashes.set(agentAddress, event.data.checkpointHash);
          }
          onEvent(agentAddress, sessionId, event);
        },
        onConnectorStateChanged(state) {
          onConnectorStateChanged(agentAddress, state);
        },
      });

      sessions.set(agentAddress, {
        harness,
        agentAddress,
        agentId: agentConfig.agentId,
        grants: grantsRef,
        config: agentConfig,
        disposers: [() => posixTools.dispose()],
      });

      harness.start();
      logger.info`Started session for ${agentAddress} (session ${sessionId})`;
    } catch (err) {
      sessions.delete(agentAddress);
      mailStores.delete(agentAddress);
      mailCommitQueues.delete(agentAddress);
      try {
        transport.unregister(agentAddress);
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
    await runDisposers(session, agentAddress);
    await drainMailQueue(agentAddress);
    sessions.delete(agentAddress);
    mailStores.delete(agentAddress);
    transport.unregister(agentAddress);
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
    await runDisposers(session, agentAddress);
    await drainMailQueue(agentAddress);
    sessions.delete(agentAddress);
    mailStores.delete(agentAddress);
    transport.unregister(agentAddress);
    logger.info`Aborted agent ${agentAddress}: ${reason}`;
  }

  async function runDisposers(
    session: AgentSession,
    agentAddress: string,
  ): Promise<void> {
    for (const disposer of session.disposers) {
      await disposer().catch((err: unknown) => {
        logger.error`Disposer failed for ${agentAddress}: ${String(err)}`;
      });
    }
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
    // Restore policy: an agent is restorable only when the on-disk
    // state from both stores is consistent. We scan keys and configs
    // independently and join on address.
    //
    //   - config + key both present  → restorable
    //   - key without config         → log warn, skip (operator can
    //                                   re-pair or remove the dir)
    //   - config without key         → log error, skip (the agent
    //                                   identity is unrecoverable)
    //
    // Mismatches today surface only via the logger. A structured event
    // surface is planned alongside the orchestrator extraction.
    const [keysByAddress, configEntries] = await Promise.all([
      keyStore.scanKeys().then((k) => new Map(k.map((e) => [e.address, e]))),
      repoStore.scanConfigs(),
    ]);

    const restored: RestoredAgent[] = [];
    const failed: string[] = [];
    const matched = new Set<string>();

    for (const entry of configEntries) {
      const keyEntry = keysByAddress.get(entry.address);
      if (keyEntry === undefined) {
        logger.error`Cannot restore "${entry.address}": agent.json exists but key pair is missing`;
        failed.push(entry.address);
        continue;
      }
      matched.add(entry.address);

      const agent: RestoredAgent = {
        address: entry.address,
        keyPair: keyEntry.keyPair,
      };
      if (entry.hubPublicKey !== undefined) {
        agent.hubPublicKey = entry.hubPublicKey;
      }

      if (sessions.has(entry.address)) {
        restored.push(agent);
        continue;
      }
      try {
        await provisionAgent(entry.config);
        if (entry.hubPublicKey !== undefined) {
          await repoStore.persistPairing(entry.address, entry.hubPublicKey);
        }
        await startSession(entry.address);
        restored.push(agent);
        logger.info`Restored session for ${entry.address}`;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        failed.push(entry.address);
        logger.error`Failed to restore session for ${entry.address}: ${msg}`;
      }
    }

    for (const [address] of keysByAddress) {
      if (matched.has(address)) continue;
      logger.warn`Skipping "${address}": key pair on disk but no agent.json to restore from`;
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
    await repoStore.persistConfig(agentAddress, session.config);
    logger.info`Updated grants for ${agentAddress} (${String(grants.length)} rules)`;
  }

  async function updateSources(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void> {
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(`No session exists for agent "${agentAddress}"`);
    }
    const source = sources.find((s) => s.id === defaultSource);
    if (source === undefined) {
      throw new Error(
        `No source matches defaultSource "${defaultSource}" in update for agent "${agentAddress}"`,
      );
    }
    if (!hasProvider(source.provider)) {
      throw new Error(
        `Source provider "${source.provider}" is not registered for agent "${agentAddress}"`,
      );
    }
    session.harness.setSource(source);
    session.config = { ...session.config, sources, defaultSource };
    await repoStore.persistConfig(agentAddress, session.config);
    logger.info`Updated sources for ${agentAddress}`;
  }

  async function applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
    verifyCommit?: CommitVerifier,
  ): Promise<void> {
    const args =
      verifyCommit !== undefined
        ? {
            address: agentAddress,
            pack,
            ref,
            commitSha,
            transferId,
            verifyCommit,
          }
        : { address: agentAddress, pack, ref, commitSha, transferId };
    await repoStore.applyDeployPack(args);
  }

  async function createStatePack(
    agentAddress: string,
  ): Promise<{ pack: Uint8Array; commitSha: string; ref: string }> {
    return repoStore.createStatePack(agentAddress);
  }

  async function deleteAgentDir(agentAddress: string): Promise<void> {
    await repoStore.remove(agentAddress);
  }

  async function persistHubPublicKey(
    agentAddress: string,
    hubPublicKey: string,
  ): Promise<void> {
    await repoStore.persistPairing(agentAddress, hubPublicKey);
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
    return repoStore.getDeployRef(agentAddress);
  }

  return {
    provisionAgent,
    startSession,
    destroySession,
    abortSession,
    deliverMessage,
    updateGrants,
    updateSources,
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
