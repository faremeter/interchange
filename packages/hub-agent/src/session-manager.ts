// Per-agent harness lifecycle.
//
// SessionManager owns the cross-agent state (provisioned/sessions maps,
// the per-agent mail-commit queue, the last-checkpoint-hash buffer used
// to thread connector reply hashes into outbound mail commits) and the
// global transport handlers (addMessageSentHandler). Per-agent harness
// construction lives behind the HarnessBuilder seam, supplied by the
// host. The package itself depends only on the lifecycle infrastructure
// (transport interface, mail-audit store type, crypto provider type)
// and the stores from this same package — it does not pin the concrete
// tool, storage, authz, or inference packages the harness is wired up
// against. The host owns those.
//
// Construction split between this module and the builder:
//   - SessionManager handles: provisioned/sessions bookkeeping,
//     transport.register/unregister/getTransportFor, AgentCrypto
//     instantiation, the mail-commit queue, the addMessageSentHandler
//     subscription, the rollback path on builder failure.
//   - HarnessBuilder handles: storage + mailStore construction (from
//     the per-agent signer), authz wiring (grantsRef + authorize
//     closure), tool composition, harness construction.
// The boundary keeps the cross-agent state owned by SessionManager and
// the per-agent construction owned by the host. Pushing transport
// registration into the builder would break the invariant; pushing
// authz out of the builder would re-couple the package to authz.

import path from "node:path";

import { getLogger } from "@intx/log";
import { hexEncode } from "@intx/types";
import type { HubTransport } from "@intx/mail-memory";
import type { GrantRule } from "@intx/types/authz";
import type { DeployApplyErrorFrame } from "@intx/types/sidecar";
import type {
  ConnectorThreadState,
  CryptoProvider,
  HarnessConfig as AgentConfig,
  InboundMessage,
  InferenceEvent,
  InferenceSource,
  KeyPair,
} from "@intx/types/runtime";
import type { Harness } from "@intx/harness";

import type { AgentKeyEntry, AgentKeyStore } from "./agent-key-store";
import type { AgentRepoStore } from "./agent-repo-store";
import type { HarnessBuilder, HarnessBundle } from "./harness-builder";
import { applyAssetPack as applyAssetPackFn } from "./apply-asset-pack";

const logger = getLogger(["interchange", "hub-agent", "session"]);

/**
 * Public session record. The grants ref and disposers live inside the
 * HarnessBundle the builder produced — they are not part of this type.
 */
export type AgentSession = {
  agentAddress: string;
  agentId: string;
  config: AgentConfig;
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

export type DeployApplyErrorSink = (
  agentAddress: string,
  payload: Omit<DeployApplyErrorFrame, "type" | "agentAddress">,
) => void;

export type SessionManagerConfig = {
  transport: HubTransport;
  repoStore: AgentRepoStore;
  keyStore: AgentKeyStore;
  buildHarness: HarnessBuilder;
  /**
   * Per-agent crypto factory. Receives the agent's raw key pair and
   * returns a CryptoProvider bound to it. Keeps the package free of
   * `@intx/crypto-node`.
   */
  createAgentCrypto: (keyPair: KeyPair) => CryptoProvider;
  onEvent: SessionEventSink;
  onConnectorStateChanged: ConnectorStateSink;
  /**
   * Optional: emit a deploy-apply error frame to the hub when the
   * harness builder rejects an apply attempt. Wired by the sidecar
   * app to hub-link's frame channel. Hosts without tool-package
   * distribution can omit this.
   */
  onDeployApplyError?: DeployApplyErrorSink;
  /**
   * Maximum number of agents whose sessions `restoreSessions` restores
   * concurrently. Restore work is per-agent isolated (per-agent git dir,
   * per-address repo lock), so agents restore in parallel up to this
   * bound and a single slow or wedged agent no longer gates the rest.
   * Defaults to `DEFAULT_RESTORE_CONCURRENCY`.
   */
  restoreConcurrency?: number;
};

/**
 * Default `restoreConcurrency`: how many agents `restoreSessions`
 * restores in parallel when the caller does not override it.
 */
const DEFAULT_RESTORE_CONCURRENCY = 4;

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

export type AgentEventListener = (event: InferenceEvent) => void;

export type SessionManager = {
  provisionAgent(config: AgentConfig): Promise<ProvisionResult>;
  startSession(agentAddress: string): Promise<void>;
  destroySession(agentAddress: string): Promise<void>;
  abortSession(agentAddress: string, reason: string): Promise<void>;
  deliverMessage(agentAddress: string, message: InboundMessage): void;
  updateGrants(agentAddress: string, grants: GrantRule[]): Promise<void>;
  /**
   * Subscribe to InferenceEvents scoped to a specific agent address.
   * Returns a disposer that removes the listener. Listeners fire
   * synchronously before the global `onEvent` sink during dispatch.
   * Production wires this against the workflow-host supervisor's
   * trivial-launch path so the per-message reactor brackets the
   * trivial workflow's run-event chain through `recordRunEvent`.
   */
  onAgentEvent(agentAddress: string, listener: AgentEventListener): () => void;
  updateSources(
    agentAddress: string,
    sources: InferenceSource[],
    defaultSource: string,
  ): Promise<void>;
  hasSession(agentAddress: string): boolean;
  isProvisioned(agentAddress: string): boolean;
  getAddresses(): string[];
  restoreSessions(): Promise<RestoreResult>;
  /**
   * Apply a deploy pack to the agent's repo. Thin wrapper around
   * AgentRepoStore for callers that already have a SessionManager handle.
   */
  applyDeployPack(
    agentAddress: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
    transferId: string,
    verifyCommit?: (payload: string, signature: string) => boolean,
  ): Promise<void>;
  /**
   * Materialize an asset pack at `<workspaceRoot>/<mountPath>/` for the
   * agent. The workspace root is per-agent; this is distinct from the
   * agent's deploy git tree. Asset packs are unsigned in v1 — no
   * `verifyCommit` parameter.
   */
  applyAssetPack(
    agentAddress: string,
    mountPath: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
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

type ProvisionedAgent = {
  config: AgentConfig;
  keyPair: KeyPair;
};

type LiveSession = AgentSession & {
  harness: Harness;
  bundle: HarnessBundle;
};

export function createSessionManager(
  config: SessionManagerConfig,
): SessionManager {
  const {
    transport,
    repoStore,
    keyStore,
    buildHarness,
    createAgentCrypto,
    onEvent,
    onConnectorStateChanged,
    onDeployApplyError,
    restoreConcurrency = DEFAULT_RESTORE_CONCURRENCY,
  } = config;
  if (!Number.isInteger(restoreConcurrency) || restoreConcurrency < 1) {
    throw new Error(
      `createSessionManager: restoreConcurrency must be a positive integer; got ${String(restoreConcurrency)}`,
    );
  }

  const sessions = new Map<string, LiveSession>();
  const provisioned = new Map<string, ProvisionedAgent>();
  const pending = new Set<string>();

  // Per-agent InferenceEvent fan-out. Subscribers register against a
  // specific agentAddress; the dispatch site looks the listener set up
  // by the event's owning address (the closure captured in startSession)
  // and fires every listener synchronously before the global `onEvent`
  // sink runs. Disposers remove a single listener and prune the set on
  // emptiness so addresses without subscribers cost a single map miss.
  const agentEventListeners = new Map<string, Set<AgentEventListener>>();

  function onAgentEvent(
    agentAddress: string,
    listener: AgentEventListener,
  ): () => void {
    let set = agentEventListeners.get(agentAddress);
    if (set === undefined) {
      set = new Set();
      agentEventListeners.set(agentAddress, set);
    }
    set.add(listener);
    return () => {
      const current = agentEventListeners.get(agentAddress);
      if (current === undefined) return;
      current.delete(listener);
      if (current.size === 0) agentEventListeners.delete(agentAddress);
    };
  }

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
    const inflight = mailCommitQueues.get(agentAddress);
    mailCommitQueues.delete(agentAddress);
    if (inflight !== undefined) await inflight;
  }

  transport.addMessageSentHandler(
    async ({ senderAddress, rawMessage, messageId }) => {
      const session = sessions.get(senderAddress);
      if (session === undefined) {
        // Outbound mail for an address with no active session is a
        // protocol violation — the transport should not have accepted
        // the send. Surfacing this loudly catches the contract break
        // before it propagates as silently-dropped audit records.
        throw new Error(
          `No active session for sender "${senderAddress}" — cannot audit outbound mail ${messageId}`,
        );
      }
      const mailStore = session.bundle.mailStore;
      const checkpointHash = lastCheckpointHashes.get(senderAddress);
      lastCheckpointHashes.delete(senderAddress);
      enqueueMailCommit(senderAddress, async () => {
        const result = await mailStore.commitMail(rawMessage, "out", {
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
    buildHarness.canBuildSource(source);

    provisioned.delete(agentAddress);

    try {
      const crypto = createAgentCrypto(keyPair);
      transport.register(agentAddress, crypto);
      const agentTransport = transport.getTransportFor(agentAddress);

      const sessionId = agentConfig.sessionId;
      const storeDir = repoStore.getAgentDir(agentAddress);

      const bundle = await buildHarness.build({
        agentAddress,
        agentConfig,
        sources: agentConfig.sources,
        defaultSource: agentConfig.defaultSource,
        storeDir,
        agentTransport,
        crypto,
        onEvent(event: InferenceEvent) {
          if (
            event.type === "connector.reply" &&
            event.data.checkpointHash !== undefined
          ) {
            lastCheckpointHashes.set(agentAddress, event.data.checkpointHash);
          }
          // Per-agent listeners fire before the global sink so an
          // in-process consumer (the workflow-host trivial-launch
          // subscriber) sees events at the same instant the hub
          // forwarder does. Exceptions from a listener must not
          // suppress the global forwarder; collect and rethrow only
          // after `onEvent` has run.
          let firstError: unknown;
          const set = agentEventListeners.get(agentAddress);
          if (set !== undefined) {
            for (const listener of set) {
              try {
                listener(event);
              } catch (err: unknown) {
                if (firstError === undefined) firstError = err;
                else
                  logger.error`agent-event listener for ${agentAddress} threw: ${String(err)}`;
              }
            }
          }
          onEvent(agentAddress, sessionId, event);
          if (firstError !== undefined) throw firstError;
        },
        onConnectorStateChanged(state) {
          onConnectorStateChanged(agentAddress, state);
        },
        ...(onDeployApplyError !== undefined
          ? {
              emitDeployApplyError: (payload) => {
                onDeployApplyError(agentAddress, payload);
              },
            }
          : {}),
      });

      sessions.set(agentAddress, {
        agentAddress,
        agentId: agentConfig.agentId,
        config: agentConfig,
        harness: bundle.harness,
        bundle,
      });

      // The composition-layer harness is started by `createHarness` --
      // by the time the builder returns the bundle, the agent's
      // reactor is already running. No separate start() step.
      logger.info`Started session for ${agentAddress} (session ${sessionId})`;
    } catch (err) {
      sessions.delete(agentAddress);
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

  async function runDisposers(
    session: LiveSession,
    agentAddress: string,
  ): Promise<Error[]> {
    // Run every disposer even if some fail; an exception from one must
    // not leak the others. Errors are collected and returned so the
    // caller (destroy / abort) can decide whether to surface a
    // partial-teardown condition; today the caller logs the summary
    // and continues, since session teardown is also invoked from
    // recovery paths where a thrown aggregate would obscure the
    // original failure.
    const errors: Error[] = [];
    for (const disposer of session.bundle.disposers) {
      try {
        await disposer();
      } catch (err: unknown) {
        const e = err instanceof Error ? err : new Error(String(err));
        logger.error`Disposer failed for ${agentAddress}: ${e.message}`;
        errors.push(e);
      }
    }
    return errors;
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
    await session.harness.close();
    const disposerErrors = await runDisposers(session, agentAddress);
    await drainMailQueue(agentAddress);
    sessions.delete(agentAddress);
    transport.unregister(agentAddress);
    if (disposerErrors.length > 0) {
      logger.warn`Stopped session for ${agentAddress} with ${String(disposerErrors.length)} disposer failure(s)`;
    } else {
      logger.info`Stopped session for ${agentAddress}`;
    }
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
    await session.harness.close();
    const disposerErrors = await runDisposers(session, agentAddress);
    await drainMailQueue(agentAddress);
    sessions.delete(agentAddress);
    transport.unregister(agentAddress);
    if (disposerErrors.length > 0) {
      logger.warn`Aborted agent ${agentAddress} (${reason}) with ${String(disposerErrors.length)} disposer failure(s)`;
    } else {
      logger.info`Aborted agent ${agentAddress}: ${reason}`;
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
    // state from both stores is consistent. Scan keys and configs
    // independently, then join on address. A config without a key is
    // surfaced as a hard failure — the agent's identity cannot be
    // recovered without the key pair on disk.
    //
    // Key-without-config does not appear here because AgentKeyStore's
    // scanKeys already requires a parseable agent.json before
    // surfacing the directory, so the orphan-key case is filtered at
    // the store boundary and never reaches this join.
    const [keysByAddress, configEntries] = await Promise.all([
      keyStore.scanKeys().then((k) => new Map(k.map((e) => [e.address, e]))),
      repoStore.scanConfigs(),
    ]);

    // Restore agents with bounded parallelism. Each agent's provision +
    // startSession is per-agent isolated (per-agent git dir, per-address
    // repo lock), so running several at once cannot corrupt shared
    // state, and one slow or wedged agent no longer gates the rest.
    // Outcomes are written into per-index slots so the returned order is
    // independent of completion order.
    type Outcome =
      | { kind: "restored"; agent: RestoredAgent }
      | { kind: "failed"; address: string };
    const outcomes = new Array<Outcome | undefined>(configEntries.length);

    async function restoreOne(index: number): Promise<void> {
      const entry = configEntries[index];
      if (entry === undefined) return;
      const keyEntry: AgentKeyEntry | undefined = keysByAddress.get(
        entry.address,
      );
      if (keyEntry === undefined) {
        logger.error`Cannot restore "${entry.address}": agent.json exists but key pair is missing`;
        outcomes[index] = { kind: "failed", address: entry.address };
        return;
      }

      const agent: RestoredAgent = {
        address: entry.address,
        keyPair: keyEntry.keyPair,
      };
      if (entry.hubPublicKey !== undefined) {
        agent.hubPublicKey = entry.hubPublicKey;
      }

      if (sessions.has(entry.address)) {
        outcomes[index] = { kind: "restored", agent };
        return;
      }
      try {
        await provisionAgent(entry.config);
        if (entry.hubPublicKey !== undefined) {
          await repoStore.persistPairing(entry.address, entry.hubPublicKey);
        }
        await startSession(entry.address);
        outcomes[index] = { kind: "restored", agent };
        logger.info`Restored session for ${entry.address}`;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        outcomes[index] = { kind: "failed", address: entry.address };
        logger.error`Failed to restore session for ${entry.address}: ${msg}`;
      }
    }

    // Worker pool: each worker pulls the next index until the list
    // drains. Reading and advancing `cursor` is synchronous (no await
    // between), so each index is handed to exactly one worker.
    let cursor = 0;
    async function worker(): Promise<void> {
      for (;;) {
        const index = cursor;
        cursor += 1;
        if (index >= configEntries.length) return;
        await restoreOne(index);
      }
    }
    const workerCount = Math.min(restoreConcurrency, configEntries.length);
    await Promise.all(Array.from({ length: workerCount }, () => worker()));

    const restored: RestoredAgent[] = [];
    const failed: string[] = [];
    for (const outcome of outcomes) {
      if (outcome === undefined) continue;
      if (outcome.kind === "restored") restored.push(outcome.agent);
      else failed.push(outcome.address);
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
    session.bundle.updateGrants(grants);
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
    buildHarness.canBuildSource(source);
    session.harness.setSources(sources, defaultSource);
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
    verifyCommit?: (payload: string, signature: string) => boolean,
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

  async function applyAssetPack(
    agentAddress: string,
    mountPath: string,
    pack: Uint8Array,
    ref: string,
    commitSha: string,
  ): Promise<void> {
    const workspaceRoot = path.join(
      repoStore.getAgentDir(agentAddress),
      "workspace",
    );
    await applyAssetPackFn({
      workspaceRoot,
      mountPath,
      pack,
      ref,
      commitSha,
    });
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
    const session = sessions.get(agentAddress);
    if (session === undefined) {
      throw new Error(
        `No active session for "${agentAddress}" — cannot audit inbound mail`,
      );
    }
    const mailStore = session.bundle.mailStore;
    enqueueMailCommit(agentAddress, async () => {
      const result = await mailStore.commitMail(rawMessage, "in", {
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
    onAgentEvent,
    updateSources,
    hasSession,
    isProvisioned,
    getAddresses,
    restoreSessions,
    applyDeployPack,
    applyAssetPack,
    createStatePack,
    deleteAgentDir,
    getDeployRef,
    persistHubPublicKey,
    commitInboundMail,
    getSessionId,
  };
}
