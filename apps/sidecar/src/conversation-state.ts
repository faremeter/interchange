// Durable conversation state for the warm single-step agent (design §3c).
//
// A long-lived single-step agent holds its multi-turn conversation in
// the reactor's in-memory turn buffer, backed by a per-step isogit
// `ContextStore`. That store is rooted per run/attempt, so it is lost
// the moment the warm agent's child is killed and respawned: the
// rebuilt agent loads a fresh, empty per-run store and the conversation
// continuity is gone.
//
// This module makes the warm agent's conversation DURABLE in the
// workflow-run substrate (the single-writer proxy `RepoStore`, written
// through the supervisor). The conversation snapshot -- the reactor's
// `turns` plus the harness's connector/metadata state -- is committed
// under the workflow-run repo at a stable per-agent path
// (`agent-state/<agentKey>/conversation.json`), sibling to the per-run
// event log under `runs/<runId>/...` and NOT confused with it. On a new
// run (and after a child respawn, once the warm agent is rebuilt
// lazily) the snapshot is restored from the substrate into the agent's
// local store BEFORE the agent's reactor loads, so multi-turn
// continuity holds across runs and across respawn.
//
// Persistence sink (the riskiest part, design §6). The connector
// router's `snapshot()` / `restore()` surface and the harness's
// `createWrappedStorageOverrides` are reused, but the persistence sink
// is repointed from the agent's local isogit store to the workflow-run
// substrate. The substrate write is the proxy
// `writeTreePreservingPrefix` scoped to the `agent-state/<agentKey>/`
// prefix; because the supervisor is the single writer and serializes
// every write to the workflow-run ref under a per-repo lock, and the
// conversation prefix is disjoint from the run-event prefix
// (`runs/<runId>/events/`), the conversation write never races nor
// clobbers the run-event log -- both pass through the same single
// writer, and the preserve-prefix merge leaves the run subtree
// byte-for-byte intact.
//
// Commit granularity (greybeard's pick, design §3c open question). The
// design calls for connector-state-change-driven commits via the
// router's `onStateChanged` hook. In the unified-host warm-agent path
// the agent receives synthesized step inputs and never drives the
// connector router (the supervisor owns mail), so `onStateChanged`
// stays dormant and never fires. The commit therefore falls back to the
// run boundary (per message) -- strictly coarser than per-change and
// far coarser than the reactor's per-cycle local commits, so the
// single-writer workflow-run ref sees one conversation write per
// inbound message at most. The `onStateChanged` wiring is retained so a
// future path that does drive the router gets change-driven commits for
// free.
//
// Defensive: a restore failure surfaces (a lost conversation on respawn
// is a correctness failure, not a silently-fresh start). A mirror
// failure surfaces so a dropped durability write is visible rather than
// leaving the next respawn to read a stale snapshot.

import fs from "node:fs";
import path from "node:path";

import { type } from "arktype";

import { getLogger } from "@intx/log";
import { createConnectorRouter } from "@intx/harness";
import { createIsogitStore } from "@intx/storage-isogit";
import type { Principal, RepoId, RepoStore } from "@intx/hub-sessions";
import { WORKFLOW_RUN_AGENT_STATE_PREFIX } from "@intx/hub-sessions";
import {
  ConnectorThreadState,
  TokenUsage,
  type AuditStore,
  type ContextStore,
  type ConversationTurn,
  type PendingOperation,
} from "@intx/types/runtime";

const logger = getLogger(["sidecar", "workflow-child", "conversation-state"]);

const CONVERSATION_FILE = "conversation.json";

/**
 * On-disk shape of a per-agent conversation snapshot committed to the
 * workflow-run substrate. Carries everything a rebuilt agent's reactor
 * needs to resume continuity: the durable turn history plus the
 * non-turn reactor metadata (pending async operations, cumulative token
 * usage) and the harness connector-thread state.
 *
 * Validated on read because it crosses back into the program from the
 * substrate working tree -- a corrupt or partially-written snapshot
 * must surface at the boundary, never be half-applied into the agent.
 */
const ConversationSnapshot = type({
  turns: "unknown[]",
  pendingOperations: "unknown[]",
  tokenUsage: TokenUsage,
  connectorState: ConnectorThreadState.or("null"),
});

/**
 * Loaded conversation snapshot the restore path applies into the warm
 * agent's local store before its reactor loads.
 */
interface LoadedSnapshot {
  turns: ConversationTurn[];
  pendingOperations: PendingOperation[];
  tokenUsage: TokenUsage;
  connectorState: ConnectorThreadState | null;
}

export interface DurableConversationStoreOpts {
  /**
   * Local per-agent isogit store root. Stable across runs (NOT keyed by
   * runId) so a warm agent's reactor loads the same on-disk store on
   * every message; the substrate is the cross-respawn durable mirror of
   * this store's conversation content.
   */
  localStoreDir: string;
  /** Commit signer for the local isogit store. */
  signer: (payload: string) => Promise<string>;
  /** Proxy workflow-run substrate (single-writer via the supervisor). */
  substrate: RepoStore;
  /** Workflow-run repo identity for the deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref the conversation snapshot is committed to. */
  workflowRunRef: string;
  /** Principal the substrate write is authored under. */
  principal: Principal;
  /**
   * Stable per-agent key the snapshot is filed under
   * (`agent-state/<agentKey>/`). The warm single-step agent's stepId is
   * the natural key: it is stable across that agent's whole lifetime and
   * disjoint from any runId.
   */
  agentKey: string;
}

/**
 * A `ContextStore` for the warm agent whose conversation content is
 * durably mirrored to the workflow-run substrate. The reactor sees a
 * normal `ContextStore` (its per-cycle commits land in the fast local
 * isogit store); `restoreFromSubstrate` and `mirrorToSubstrate` move
 * the conversation snapshot between the local store and the durable
 * substrate path.
 */
export interface DurableConversationStore {
  /**
   * The store the warm agent's env binds as `storage` and `audit`. It is
   * both `ContextStore` (conversation + connector state) and
   * `AuditStore` (tool-authorization records), matching the per-run
   * isogit store the non-warm path uses.
   */
  readonly storage: ContextStore & AuditStore;
  /**
   * Pull the prior conversation snapshot from the substrate into the
   * local store so the agent's reactor `load()` sees it. Called before
   * the warm agent is built (lazy first build and respawn rebuild).
   * Returns `true` when a snapshot was found and applied, `false` when
   * none exists yet (the genuine first-ever run). A read that finds a
   * snapshot but cannot parse it throws -- a corrupt snapshot is a
   * correctness failure that must not silently start the agent fresh.
   */
  restoreFromSubstrate(): Promise<boolean>;
  /**
   * Commit the local store's current conversation snapshot to the
   * substrate. Called at the run boundary (after the agent's send
   * settles). A write failure surfaces.
   */
  mirrorToSubstrate(): Promise<void>;
}

export async function createDurableConversationStore(
  opts: DurableConversationStoreOpts,
): Promise<DurableConversationStore> {
  await fs.promises.mkdir(opts.localStoreDir, { recursive: true });
  const baseStorage = await createIsogitStore(opts.localStoreDir, opts.signer);

  // Reuse the connector router + the harness storage-override seam. The
  // router's `onStateChanged` is the change-driven commit hook the
  // design names; in the synthesized-step-input warm path it stays
  // dormant (the supervisor owns mail, so the agent never routes a
  // connector message), so the run-boundary mirror is the operative
  // commit trigger. The wiring is retained verbatim so a future path
  // that drives the router gets change-driven mirrors with no further
  // work.
  const connectorRouter = createConnectorRouter({
    onStateChanged: () => {
      void mirrorToSubstrate().catch((cause) => {
        logger.error`connector-state-change conversation mirror failed for ${opts.agentKey}: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    },
  });

  const agentStatePrefix = `${WORKFLOW_RUN_AGENT_STATE_PREFIX}/${encodeURIComponent(opts.agentKey)}/`;
  const conversationPath = `${agentStatePrefix}${CONVERSATION_FILE}`;

  function substrateConversationFsPath(): string {
    const repoDir = opts.substrate.getRepoDir(opts.workflowRunRepoId);
    return path.join(
      repoDir,
      WORKFLOW_RUN_AGENT_STATE_PREFIX,
      encodeURIComponent(opts.agentKey),
      CONVERSATION_FILE,
    );
  }

  async function readSubstrateSnapshot(): Promise<LoadedSnapshot | null> {
    let raw: string;
    try {
      raw = await fs.promises.readFile(substrateConversationFsPath(), "utf8");
    } catch (cause) {
      if (isErrnoNotFound(cause)) return null;
      throw cause;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch (cause) {
      throw new Error(
        `sidecar conversation-state: substrate snapshot for ${opts.agentKey} is not valid JSON; refusing to start the warm agent fresh on a corrupt snapshot`,
        { cause },
      );
    }
    const validated = ConversationSnapshot(parsed);
    if (validated instanceof type.errors) {
      throw new Error(
        `sidecar conversation-state: substrate snapshot for ${opts.agentKey} failed validation: ${validated.summary}; refusing to start the warm agent fresh on a corrupt snapshot`,
      );
    }
    // The arktype validator confirms the structural shape; the turn and
    // pending-operation arrays carry the reactor's own vocabulary, which
    // the reactor re-narrows on load. Surface them as the loaded shape.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- ConversationSnapshot enforces the structural shape; turn/operation element narrows live in the reactor on load
    return validated as unknown as LoadedSnapshot;
  }

  async function restoreFromSubstrate(): Promise<boolean> {
    const snapshot = await readSubstrateSnapshot();
    if (snapshot === null) return false;
    // Write the snapshot's turns + metadata into the local store's
    // working tree and commit, so the agent's reactor `load()` reads the
    // restored conversation. `setConnectorState` buffers the connector
    // state for the metadata write; `restore()` mirrors it into the
    // router so a future change-driven mirror carries the right base.
    await baseStorage.writeTurns(snapshot.turns);
    baseStorage.setConnectorState(snapshot.connectorState);
    connectorRouter.restore(snapshot.connectorState);
    await baseStorage.writeMetadata({
      pendingOperations: snapshot.pendingOperations,
      tokenUsage: snapshot.tokenUsage,
    });
    await baseStorage.commit({
      message: `restore conversation for ${opts.agentKey} from substrate`,
    });
    return true;
  }

  async function mirrorToSubstrate(): Promise<void> {
    const loaded = await baseStorage.load();
    const snapshot = {
      turns: loaded.turns,
      pendingOperations: loaded.pendingOperations,
      tokenUsage: loaded.tokenUsage,
      connectorState: loaded.connectorState,
    };
    const serialized = JSON.stringify(snapshot);
    await opts.substrate.writeTreePreservingPrefix(
      opts.principal,
      opts.workflowRunRepoId,
      opts.workflowRunRef,
      {
        preservePrefix: agentStatePrefix,
        merge: async () => ({ [conversationPath]: serialized }),
        message: `mirror conversation for ${opts.agentKey} to substrate`,
      },
    );
  }

  return {
    storage: baseStorage,
    restoreFromSubstrate,
    mirrorToSubstrate,
  };
}

export interface DurableConversationRegistryOpts {
  /** Sidecar data dir; per-agent local stores root under it. */
  dataDir: string;
  /** Workflow-run repo identity for the deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref. */
  workflowRunRef: string;
  /** Proxy workflow-run substrate (single-writer via the supervisor). */
  substrate: RepoStore;
  /** Principal the substrate write is authored under. */
  principal: Principal;
  /** Commit signer for the per-agent local isogit stores. */
  signer: (payload: string) => Promise<string>;
}

/**
 * Per-agent durable-conversation store registry (design §3c). One store
 * per warm agent key, built lazily and reused across runs in the same
 * child. The first `acquire` for a key builds the store and restores its
 * prior conversation snapshot from the substrate -- the path that runs
 * on the lazy first build AND on the respawn rebuild, so the warm agent
 * resumes its conversation across child respawn. The registry is empty
 * after a respawn (it lives in the child's address space); the substrate
 * is the durable mirror that survives.
 */
export interface DurableConversationRegistry {
  acquire(key: string): Promise<DurableConversationStore>;
  get(key: string): DurableConversationStore;
}

export function createDurableConversationRegistry(
  opts: DurableConversationRegistryOpts,
): DurableConversationRegistry {
  const stores = new Map<string, DurableConversationStore>();
  // De-dup concurrent first-acquires for the same key so two in-flight
  // step invocations for one warm agent never build two stores (which
  // would double-restore and split the durable mirror).
  const building = new Map<string, Promise<DurableConversationStore>>();

  function localStoreDir(key: string): string {
    return path.join(
      opts.dataDir,
      "agent-conversation-state",
      opts.workflowRunRepoId.id,
      encodeURIComponent(key),
    );
  }

  async function acquire(key: string): Promise<DurableConversationStore> {
    const existing = stores.get(key);
    if (existing !== undefined) return existing;
    const inFlight = building.get(key);
    if (inFlight !== undefined) return inFlight;
    const promise = (async () => {
      const store = await createDurableConversationStore({
        localStoreDir: localStoreDir(key),
        signer: opts.signer,
        substrate: opts.substrate,
        workflowRunRepoId: opts.workflowRunRepoId,
        workflowRunRef: opts.workflowRunRef,
        principal: opts.principal,
        agentKey: key,
      });
      // Restore the prior conversation BEFORE the store is observable
      // (and before the warm agent's reactor `load()` reads it). On a
      // genuine first-ever run this is a no-op (no snapshot yet); on a
      // respawn rebuild it pulls the pre-respawn conversation back from
      // the substrate. A restore failure surfaces -- a lost conversation
      // on respawn is a correctness failure, not a silently-fresh start.
      await store.restoreFromSubstrate();
      stores.set(key, store);
      building.delete(key);
      return store;
    })().catch((cause) => {
      building.delete(key);
      throw cause;
    });
    building.set(key, promise);
    return promise;
  }

  function get(key: string): DurableConversationStore {
    const store = stores.get(key);
    if (store === undefined) {
      throw new Error(
        `sidecar conversation-state: no durable conversation store for ${JSON.stringify(key)}; the run-boundary mirror ran before the warm agent's env was built`,
      );
    }
    return store;
  }

  return { acquire, get };
}

function isErrnoNotFound(cause: unknown): boolean {
  if (cause === null || typeof cause !== "object") return false;
  const code = (cause as { code?: unknown }).code;
  return code === "ENOENT";
}
