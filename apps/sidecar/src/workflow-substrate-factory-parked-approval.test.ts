// `loadParkedApproval` durable reads: the sidecar binding recovers a parked
// correlation's approval snapshot for the child's re-registration enumeration.
//
// Two layouts, verified against the production read helpers:
//   - COLD (multi-step): the snapshot lives in the step's committed state in
//     the workflow-run substrate, stamped with the attempt that parked; the
//     read reconstructs it and ignores another attempt's state. A step that
//     parked before cold step state was committed is read from its local
//     per-attempt store instead.
//   - WARM (single-step): the snapshot lives in the durable conversation
//     store, mirrored to the step's state directory in the workflow-run
//     substrate (or, for a deployment that predates it, the legacy
//     `agent-state/<stepId>/`). The read reconstructs it from the substrate
//     WITHOUT going through the live registry -- proving a respawned child
//     (whose live store is unbuilt) still recovers the snapshot.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type {
  ApprovalSnapshot,
  ConversationTurn,
  PendingOperation,
} from "@intx/types/runtime";
import { generateKeyPair } from "@intx/crypto";
import {
  createRepoStore,
  serializeStepStateWalEntry,
  stepStateWalEntryPath,
  workflowRunKindHandler,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
  type RepoId,
  type RepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import { createIsogitStore } from "@intx/storage-isogit/node";

import {
  persistColdRecoveredPark,
  readColdParkedApprovalSnapshot,
  readColdParkedPendingOperations,
  readWarmParkedApprovalSnapshot,
  readWarmParkedPendingOperations,
  stepStorageRoot,
  toParkedApprovalOps,
} from "./workflow-substrate-factory";
import {
  createDurableConversationStore,
  readStepState,
} from "./conversation-state";

const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: "parked-approval",
};
const WORKFLOW_RUN_REF = "refs/heads/main";
const RUN_ID = "run_parked";
const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: WORKFLOW_RUN_REPO_ID.id,
};
const COMMITTED_READS = {
  workflowRunRef: WORKFLOW_RUN_REF,
  principal: PRINCIPAL,
};
const EMPTY_USAGE = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  thinking: 0,
};

const snapshot: ApprovalSnapshot = {
  name: "charge_card",
  description: "Charge the customer's card",
  inputSchema: { type: "object" },
  arguments: { amount: 100 },
};

function pendingApproval(
  correlationId: string,
  approvalSnapshot?: ApprovalSnapshot,
): PendingOperation {
  return {
    correlationId,
    kind: "approval",
    registeredAt: 0,
    gateId: `gate-${correlationId}`,
    ...(approvalSnapshot !== undefined ? { approvalSnapshot } : {}),
  };
}

async function makeTempDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "parked-approval-"));
}

/**
 * A workflow-run substrate with the production kind handler validating every
 * write, rooted beside the host data dir `dataDir` so a step's local stores
 * never land in the repo's working tree.
 */
async function createSubstrate(dataDir: string): Promise<RepoStore> {
  const substrate = createRepoStore({
    dataDir: path.join(dataDir, "substrate"),
    signingKey: await generateKeyPair(),
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  await substrate.writeTree(
    { kind: "hub" },
    WORKFLOW_RUN_REPO_ID,
    WORKFLOW_RUN_REF,
    { files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" }, message: "genesis" },
  );
  return substrate;
}

const testSigner = (payload: string): Promise<string> =>
  Promise.resolve(`sig:${payload.length}`);

/**
 * Park attempt `attempt` of step "s" in `runId` the way the cold path does:
 * its durable store commits the pending operations to the step's state.
 */
async function parkColdStep(
  substrate: RepoStore,
  runId: string,
  attempt: number,
  pendingOperations: PendingOperation[],
): Promise<void> {
  const store = await createDurableConversationStore({
    localStoreDir: await makeTempDir(),
    signer: testSigner,
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    runId,
    stepId: "s",
    lifetime: { kind: "attempt", attempt },
  });
  await store.restoreFromSubstrate();
  await store.storage.writeMetadata({
    pendingOperations,
    tokenUsage: EMPTY_USAGE,
  });
  await store.mirrorToSubstrate();
}

describe("readColdParkedApprovalSnapshot", () => {
  async function coordinate(correlationId: string) {
    const dataDir = await makeTempDir();
    return {
      dataDir,
      substrate: await createSubstrate(dataDir),
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: "run-1",
      stepId: "s",
      attempt: 1,
      correlationId,
    };
  }

  test("reconstructs the snapshot from the step's committed state", async () => {
    const at = await coordinate("corr-1");
    await parkColdStep(at.substrate, "run-1", 1, [
      pendingApproval("corr-1", snapshot),
    ]);

    expect(await readColdParkedApprovalSnapshot(at)).toEqual(snapshot);
  });

  test("returns undefined when the step has no committed state", async () => {
    const at = await coordinate("corr-x");

    expect(await readColdParkedApprovalSnapshot(at)).toBeUndefined();
  });

  test("returns undefined for a correlation with no matching pending op", async () => {
    const at = await coordinate("corr-other");
    await parkColdStep(at.substrate, "run-1", 1, [
      pendingApproval("corr-a", snapshot),
    ]);

    expect(await readColdParkedApprovalSnapshot(at)).toBeUndefined();
  });

  test("returns undefined for a matching op that carries no snapshot", async () => {
    const at = await coordinate("corr-1");
    await parkColdStep(at.substrate, "run-1", 1, [pendingApproval("corr-1")]);

    expect(await readColdParkedApprovalSnapshot(at)).toBeUndefined();
  });
});

describe("readWarmParkedApprovalSnapshot", () => {
  test("reconstructs the snapshot from the durable substrate mirror", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const store = await createDurableConversationStore({
      localStoreDir: await makeTempDir(),
      signer: testSigner,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: "s",
      lifetime: { kind: "deployment" },
    });
    // Suspend-time state: the parked approval's pending op, then mirrored to
    // the substrate at the run boundary. No live registry is involved on the
    // read side, mirroring a respawned child whose warm store is unbuilt.
    await store.storage.writeMetadata({
      pendingOperations: [pendingApproval("corr-1", snapshot)],
      tokenUsage: EMPTY_USAGE,
    });
    await store.mirrorToSubstrate();

    const got = await readWarmParkedApprovalSnapshot({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: RUN_ID,
      stepId: "s",
      correlationId: "corr-1",
    });
    expect(got).toEqual(snapshot);
  });

  test("returns undefined when no durable state exists for the agent", async () => {
    const substrate = await createSubstrate(await makeTempDir());

    const got = await readWarmParkedApprovalSnapshot({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: RUN_ID,
      stepId: "never-ran",
      correlationId: "corr-x",
    });
    expect(got).toBeUndefined();
  });

  test("returns undefined for a matching op that carries no snapshot", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const store = await createDurableConversationStore({
      localStoreDir: await makeTempDir(),
      signer: testSigner,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: "s",
      lifetime: { kind: "deployment" },
    });
    await store.storage.writeMetadata({
      pendingOperations: [pendingApproval("corr-1")],
      tokenUsage: EMPTY_USAGE,
    });
    await store.mirrorToSubstrate();

    const got = await readWarmParkedApprovalSnapshot({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: RUN_ID,
      stepId: "s",
      correlationId: "corr-1",
    });
    expect(got).toBeUndefined();
  });
});

// The pending-operations readers back the resume classifier's `readParkedApprovalOps`
// binding (the crash-mid-park recovery hook), where the snapshot readers back the
// re-registration enumeration. Both project the same durable store; these pin the
// enumeration variant returns every parked op, not just one matched by correlationId.
describe("readColdParkedPendingOperations", () => {
  async function coordinate(runId: string, attempt: number) {
    const dataDir = await makeTempDir();
    return {
      dataDir,
      substrate: await createSubstrate(dataDir),
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId,
      stepId: "s",
      attempt,
    };
  }

  /**
   * Park a step the way a sidecar that predates committed cold step state
   * did: the pending operations live only in its local per-attempt store.
   */
  async function parkLocally(
    at: Awaited<ReturnType<typeof coordinate>>,
    pendingOperations: PendingOperation[],
  ): Promise<void> {
    const store = await createIsogitStore(stepStorageRoot(at), testSigner);
    await store.writeMetadata({ pendingOperations, tokenUsage: EMPTY_USAGE });
  }

  test("returns every pending operation from the step's committed state", async () => {
    const at = await coordinate("run-1", 1);
    await parkColdStep(at.substrate, "run-1", 1, [
      pendingApproval("corr-1", snapshot),
    ]);

    expect(await readColdParkedPendingOperations(at)).toEqual([
      pendingApproval("corr-1", snapshot),
    ]);
  });

  test("ignores another attempt's committed state", async () => {
    const at = await coordinate("run-1", 2);
    await parkColdStep(at.substrate, "run-1", 1, [
      pendingApproval("corr-1", snapshot),
    ]);

    expect(await readColdParkedPendingOperations(at)).toEqual([]);
  });

  test("reads a park that predates committed step state from the local store", async () => {
    const at = await coordinate("run-1", 1);
    await parkLocally(at, [pendingApproval("corr-1", snapshot)]);

    expect(await readColdParkedPendingOperations(at)).toEqual([
      pendingApproval("corr-1", snapshot),
    ]);
  });

  function turn(text: string): ConversationTurn {
    return { role: "user", content: [{ type: "text", text }], timestamp: 0 };
  }

  /**
   * One cold invocation over a local store (the attempt's own unless
   * `localStoreDir` names another host's): restore, record the turn's
   * conversation and pending operations locally, and mirror them to the
   * substrate unless the invocation crashed first.
   */
  async function runColdTurn(
    at: Awaited<ReturnType<typeof coordinate>>,
    turns: ConversationTurn[],
    pendingOperations: PendingOperation[],
    outcome: "mirrored" | "crashed",
    localStoreDir = stepStorageRoot(at),
  ): Promise<void> {
    const store = await createDurableConversationStore({
      localStoreDir,
      signer: testSigner,
      substrate: at.substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: at.runId,
      stepId: at.stepId,
      lifetime: { kind: "attempt", attempt: at.attempt },
    });
    await store.restoreFromSubstrate();
    await store.storage.writeTurns(turns);
    await store.storage.writeMetadata({
      pendingOperations,
      tokenUsage: EMPTY_USAGE,
    });
    await store.storage.commit({ message: "turn" });
    if (outcome === "mirrored") await store.mirrorToSubstrate();
  }

  test("reads a park the local store holds ahead of the committed state", async () => {
    const at = await coordinate("run-1", 1);
    await runColdTurn(
      at,
      [turn("ask")],
      [pendingApproval("corr-1", snapshot)],
      "mirrored",
    );
    // The approved turn parks on a second gate and crashes before its mirror.
    await runColdTurn(
      at,
      [turn("ask"), turn("approved")],
      [pendingApproval("corr-2", snapshot)],
      "crashed",
    );

    expect(await readColdParkedPendingOperations(at)).toEqual([
      pendingApproval("corr-2", snapshot),
    ]);
  });

  test("reads a retried attempt's park from its local store over an earlier attempt's committed state", async () => {
    const at = await coordinate("run-1", 2);
    await parkColdStep(at.substrate, "run-1", 1, [
      pendingApproval("corr-1", snapshot),
    ]);
    await runColdTurn(
      at,
      [turn("retry")],
      [pendingApproval("corr-2", snapshot)],
      "crashed",
    );

    expect(await readColdParkedPendingOperations(at)).toEqual([
      pendingApproval("corr-2", snapshot),
    ]);
  });

  test("reads the committed state over a local store that no longer extends it", async () => {
    const at = await coordinate("run-1", 1);
    // The attempt ran on and committed from another host, leaving this host's
    // local store behind.
    await runColdTurn(
      at,
      [turn("elsewhere")],
      [pendingApproval("corr-1", snapshot)],
      "mirrored",
      await makeTempDir(),
    );
    const stale = await createIsogitStore(stepStorageRoot(at), testSigner);
    await stale.writeTurns([turn("left behind")]);
    await stale.writeMetadata({
      pendingOperations: [pendingApproval("stale", snapshot)],
      tokenUsage: EMPTY_USAGE,
    });

    expect(await readColdParkedPendingOperations(at)).toEqual([
      pendingApproval("corr-1", snapshot),
    ]);
  });

  test("returns an empty list without creating a repo when neither holds state", async () => {
    const at = await coordinate("missing", 1);

    expect(await readColdParkedPendingOperations(at)).toEqual([]);
    await expect(fs.stat(stepStorageRoot(at))).rejects.toThrow();
  });

  describe("persistColdRecoveredPark", () => {
    /** Count the substrate writes `persistColdRecoveredPark` makes. */
    function countWrites(substrate: RepoStore): {
      substrate: RepoStore;
      writes: () => number;
    } {
      let writes = 0;
      return {
        substrate: new Proxy(substrate, {
          get(target, prop, receiver) {
            if (prop === "writeTreePreservingPrefix") {
              return (
                ...args: Parameters<RepoStore["writeTreePreservingPrefix"]>
              ) => {
                writes += 1;
                return target.writeTreePreservingPrefix(...args);
              };
            }
            return Reflect.get(target, prop, receiver);
          },
        }),
        writes: () => writes,
      };
    }

    function persist(
      at: Awaited<ReturnType<typeof coordinate>>,
      correlationId: string,
      substrate: RepoStore = at.substrate,
    ): Promise<void> {
      return persistColdRecoveredPark({
        ...at,
        substrate,
        signer: testSigner,
        correlationId,
      });
    }

    /** The same attempt read by a replacement host with an empty data dir. */
    async function onReplacementHost(
      at: Awaited<ReturnType<typeof coordinate>>,
    ) {
      return { ...at, dataDir: await makeTempDir() };
    }

    test("commits a park only the local store holds, so a replacement host finds it", async () => {
      const at = await coordinate("run-1", 1);
      await runColdTurn(
        at,
        [turn("ask")],
        [pendingApproval("corr-1", snapshot)],
        "mirrored",
      );
      await runColdTurn(
        at,
        [turn("ask"), turn("approved")],
        [pendingApproval("corr-2", snapshot)],
        "crashed",
      );
      const hostB = await onReplacementHost(at);
      expect(
        await readColdParkedApprovalSnapshot({
          ...hostB,
          correlationId: "corr-2",
        }),
      ).toBeUndefined();

      await persist(at, "corr-2");

      expect(
        await readColdParkedApprovalSnapshot({
          ...hostB,
          correlationId: "corr-2",
        }),
      ).toEqual(snapshot);
      const committed = await readStepState(at);
      expect(committed?.turns).toEqual([turn("ask"), turn("approved")]);
      expect(committed?.attempt).toBe(1);
    });

    test("writes nothing when the committed state already holds the park", async () => {
      const at = await coordinate("run-1", 1);
      await runColdTurn(
        at,
        [turn("ask")],
        [pendingApproval("corr-1", snapshot)],
        "mirrored",
      );
      const counted = countWrites(at.substrate);

      await persist(at, "corr-1", counted.substrate);
      await persist(await onReplacementHost(at), "corr-1", counted.substrate);

      expect(counted.writes()).toBe(0);
    });

    test("replaces an earlier attempt's committed state with the retried attempt's park", async () => {
      const at = await coordinate("run-1", 2);
      await parkColdStep(at.substrate, "run-1", 1, [
        pendingApproval("corr-1", snapshot),
      ]);
      await runColdTurn(
        at,
        [turn("retry")],
        [pendingApproval("corr-2", snapshot)],
        "crashed",
      );

      await persist(at, "corr-2");

      const committed = await readStepState(at);
      expect(committed?.attempt).toBe(2);
      expect(committed?.turns).toEqual([turn("retry")]);
      expect(committed?.pendingOperations).toEqual([
        pendingApproval("corr-2", snapshot),
      ]);
    });

    test("fails without creating a store when no copy holds the park", async () => {
      const at = await coordinate("run-1", 1);
      const counted = countWrites(at.substrate);

      await expect(persist(at, "corr-1", counted.substrate)).rejects.toThrow(
        /neither the committed step state nor a current local store/,
      );
      expect(counted.writes()).toBe(0);
      await expect(fs.stat(stepStorageRoot(at))).rejects.toThrow();
    });

    test("fails without touching a local store that no longer extends the committed state", async () => {
      const at = await coordinate("run-1", 1);
      await runColdTurn(
        at,
        [turn("elsewhere")],
        [pendingApproval("corr-1", snapshot)],
        "mirrored",
        await makeTempDir(),
      );
      const stale = await createIsogitStore(stepStorageRoot(at), testSigner);
      await stale.writeTurns([turn("left behind")]);
      await stale.writeMetadata({
        pendingOperations: [pendingApproval("stale", snapshot)],
        tokenUsage: EMPTY_USAGE,
      });
      const counted = countWrites(at.substrate);

      await expect(persist(at, "stale", counted.substrate)).rejects.toThrow(
        /neither the committed step state nor a current local store/,
      );
      expect(counted.writes()).toBe(0);
      const local = await (await createIsogitStore(stepStorageRoot(at))).load();
      expect(local.turns).toEqual([turn("left behind")]);
    });
  });
});

describe("readWarmParkedPendingOperations", () => {
  test("reconstructs the pending operations from the durable substrate mirror", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const store = await createDurableConversationStore({
      localStoreDir: await makeTempDir(),
      signer: testSigner,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: "s",
      lifetime: { kind: "deployment" },
    });
    await store.storage.writeMetadata({
      pendingOperations: [pendingApproval("corr-1", snapshot)],
      tokenUsage: EMPTY_USAGE,
    });
    await store.mirrorToSubstrate();

    const got = await readWarmParkedPendingOperations({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: RUN_ID,
      stepId: "s",
    });
    expect(got).toEqual([pendingApproval("corr-1", snapshot)]);
  });

  test("reads a legacy agent-state copy the warm restore has not moved yet", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const entryPath = stepStateWalEntryPath(
      `${WORKFLOW_RUN_AGENT_STATE_PREFIX}/s/`,
      0,
    );
    await substrate.writeTree(
      { kind: "hub" },
      WORKFLOW_RUN_REPO_ID,
      WORKFLOW_RUN_REF,
      {
        files: {
          [entryPath]: serializeStepStateWalEntry(0, [], {
            pendingOperations: [pendingApproval("corr-1", snapshot)],
            tokenUsage: EMPTY_USAGE,
            connectorState: null,
          }),
        },
        message: "legacy conversation",
      },
    );

    const got = await readWarmParkedPendingOperations({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: RUN_ID,
      stepId: "s",
    });
    expect(got).toEqual([pendingApproval("corr-1", snapshot)]);
  });

  test("returns an empty list when no durable state exists for the agent", async () => {
    const substrate = await createSubstrate(await makeTempDir());

    const got = await readWarmParkedPendingOperations({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      runId: RUN_ID,
      stepId: "never-ran",
    });
    expect(got).toEqual([]);
  });
});

describe("toParkedApprovalOps", () => {
  test("projects approval ops to correlationId plus the epoch-ms deadline", async () => {
    const withDeadline: PendingOperation = {
      correlationId: "corr-timeout",
      kind: "approval",
      registeredAt: 0,
      gateId: "gate-corr-timeout",
      timeoutAt: 1_700_000_000_000,
    };

    const got = toParkedApprovalOps([
      pendingApproval("corr-1", snapshot),
      withDeadline,
    ]);

    expect(got).toEqual([
      { correlationId: "corr-1" },
      { correlationId: "corr-timeout", timeoutAtMs: 1_700_000_000_000 },
    ]);
  });

  test("omits timeoutAtMs for an indefinite-hold park", async () => {
    const got = toParkedApprovalOps([pendingApproval("corr-1")]);
    expect(got).toEqual([{ correlationId: "corr-1" }]);
    const first = got[0];
    if (first === undefined) throw new Error("unreachable");
    expect("timeoutAtMs" in first).toBe(false);
  });
});
