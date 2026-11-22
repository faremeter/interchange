// `loadParkedApproval` durable reads: the sidecar binding recovers a parked
// correlation's approval snapshot for the child's re-registration enumeration.
//
// The snapshot lives in the step's committed state in the workflow-run
// substrate, under the top-level run at the key of the step's place, stamped
// with the invocation that parked; the read reconstructs it and ignores
// another invocation's state. A read for a park's gate also takes state with
// no stamp parked on that gate. A step that parked before cold step state was
// committed is read from its local per-attempt store instead.

import { afterAll, describe, expect, spyOn, test } from "bun:test";
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
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
  type RepoId,
  type RepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import { createDefaultDirectorRegistry, defineAgent } from "@intx/agent";
import { createIsogitStore } from "@intx/storage-isogit/node";
import { signalName } from "@intx/types/signals";
import { stepPlaceKey, type StepPlace } from "@intx/types/step-place";
import {
  childWorkflow,
  createInMemoryBlobSubstrate,
  createInMemoryRepoStore,
  createInMemoryScheduler,
  createInMemorySignalChannel,
  createNoopDrainController,
  createSuspendableChildHandle,
  defineWorkflow,
  loop,
  loopBodyRunId,
  rewriteInlineChildWorkflowBodies,
  runtimeRun,
  sectionBodyRunId,
  step,
  type RepoStore as RuntimeRepoStore,
  type StepInvoker,
  type WorkflowDefinition,
  type WorkflowEvent,
  type WorkflowRuntimeEnv,
} from "@intx/workflow";
import { commitBuffered, dropChain } from "@intx/workflow/runtime";
import { waitForEvent } from "@intx/workflow/testing";
import {
  createDurableConversationStore,
  readStepState,
} from "@intx/workflow-host";
import { commitUnstampedStepState } from "@intx/workflow-host/testing";

import { createWorkflowRunRepoStore } from "../adapters/repo-store";
import type { ControlChannelSender } from "../ipc/control-sender";
import { createParkReleases } from "./park-releases";
import { collectParkedApprovalCorrelations } from "./parked-correlations";
import {
  createDeploymentHostScheduler,
  createParkedApprovalHooks,
  persistColdRecoveredPark,
  readColdParkedApprovalSnapshot,
  readColdParkedPendingOperations,
  stepStorageRoot,
  toParkedApprovalOps,
} from "./substrate-factory";

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
    turnId: `turn-${correlationId}`,
    gated: true,
    ...(approvalSnapshot !== undefined ? { approvalSnapshot } : {}),
  };
}

const tempDirs: string[] = [];

afterAll(async () => {
  for (const dir of tempDirs.splice(0)) {
    await fs.rm(dir, { recursive: true, force: true });
  }
});

async function makeTempDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "parked-approval-"));
  tempDirs.push(dir);
  return dir;
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
 * Park attempt `attempt` of step "s" in `runId`, at `stepPath`, the way the
 * cold path does: its durable store commits the pending operations to the
 * state of the step's place under the top-level run.
 */
async function parkColdStep(
  substrate: RepoStore,
  runId: string,
  attempt: number,
  pendingOperations: PendingOperation[],
  stepPath: readonly string[] = ["s"],
): Promise<void> {
  const store = await createDurableConversationStore({
    localStoreDir: await makeTempDir(),
    signer: testSigner,
    substrate,
    workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    runId: RUN_ID,
    stepId: stepPlaceKey({ stepPath }),
    lifetime: {
      kind: "invocation",
      invocation: { runId, stepId: "s", attempt },
    },
    mailboxAddress: "agent@example.com",
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
      topLevelRunId: RUN_ID,
      runId: "run-1",
      stepId: "s",
      attempt: 1,
      place: { stepPath: ["s"] },
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

  describe("over a gate park with no stamp", () => {
    // Commit a park on gate `corr-1` with no stamp, as an earlier version
    // wrote it, from a local store the reads never see.
    async function parkWithNoStamp(substrate: RepoStore): Promise<void> {
      await commitUnstampedStepState({
        substrate,
        workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
        workflowRunRef: WORKFLOW_RUN_REF,
        principal: PRINCIPAL,
        runId: RUN_ID,
        stepId: "s",
        turns: [],
        metadata: {
          pendingOperations: [
            {
              ...pendingApproval("corr-1", snapshot),
              suspendedCall: {
                id: "call-1",
                name: "charge_card",
                arguments: {},
              },
            },
          ],
          tokenUsage: EMPTY_USAGE,
          connectorState: null,
        },
      });
    }

    test("a read for the park's gate takes the park on a host with no local store", async () => {
      const at = await coordinate("corr-1");
      await parkWithNoStamp(at.substrate);
      expect(
        (await readStepState({ ...at, runId: RUN_ID }))?.invocation,
      ).toBeUndefined();

      expect(await readColdParkedApprovalSnapshot(at)).toEqual(snapshot);
    });

    test("a read for another gate does not take the park", async () => {
      const at = await coordinate("corr-2");
      await parkWithNoStamp(at.substrate);

      // The snapshot read filters by gate, so the pending operations show
      // whether the read took the park.
      expect(await readColdParkedPendingOperations(at)).toEqual([]);
      expect(await readColdParkedApprovalSnapshot(at)).toBeUndefined();
    });

    test("a re-registration on a replacement host collects the park", async () => {
      const at = await coordinate("corr-1");
      await parkWithNoStamp(at.substrate);
      const runtimeRepoStore = createInMemoryRepoStore();
      const log: WorkflowEvent[] = [
        {
          kind: "RunStarted",
          seq: 1,
          at: new Date(0).toISOString(),
          runId: RUN_ID,
          definitionHash: "x",
          trigger: { type: "manual", payload: undefined },
        },
        {
          kind: "StepStarted",
          seq: 2,
          at: new Date(0).toISOString(),
          stepId: "s",
          attempt: 1,
          input: { ref: "inline:null" },
        },
        {
          kind: "SignalAwaited",
          seq: 3,
          at: new Date(0).toISOString(),
          stepId: "s",
          signalName: signalName("corr-1"),
          parkKind: "approval",
        },
      ];
      for (const event of log) await runtimeRepoStore.append(RUN_ID, event);
      const hooks = createParkedApprovalHooks({
        dataDir: at.dataDir,
        substrate: at.substrate,
        workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
        ...COMMITTED_READS,
        signer: testSigner,
        mailboxAddress: "agent@example.com",
        topLevelRunId: RUN_ID,
      });

      expect(
        await collectParkedApprovalCorrelations({
          substrate: at.substrate,
          repoId: WORKFLOW_RUN_REPO_ID,
          runtimeRepoStore,
          loadParkedApproval: hooks.loadParkedApproval,
          readParkedApprovalOps: hooks.readParkedApprovalOps,
        }),
      ).toEqual([
        {
          runId: RUN_ID,
          correlationId: "corr-1",
          parkKind: "approval",
          snapshot,
        },
      ]);
    });

    // Crash recovery reads a step's parks without a correlation id, so it
    // cannot tell whose park state with no stamp holds.
    test("a read with no correlation id does not take the park", async () => {
      const { correlationId: _, ...at } = await coordinate("corr-1");
      await parkWithNoStamp(at.substrate);

      expect(await readColdParkedPendingOperations(at)).toEqual([]);
    });
  });
});

// The pending-operations readers back the resume classifier's `readParkedApprovalOps`
// binding (the crash-mid-park recovery hook), where the snapshot readers back the
// re-registration enumeration. Both project the same durable store; these pin the
// enumeration variant returns every parked op, not just one matched by correlationId.
describe("readColdParkedPendingOperations", () => {
  async function coordinate(
    runId: string,
    attempt: number,
    place: StepPlace = { stepPath: ["s"] },
  ) {
    const dataDir = await makeTempDir();
    return {
      dataDir,
      substrate: await createSubstrate(dataDir),
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      topLevelRunId: RUN_ID,
      runId,
      stepId: "s",
      attempt,
      place,
    };
  }

  /** The committed state of the key of `at`'s place, whatever its stamp. */
  function readCommitted(at: Awaited<ReturnType<typeof coordinate>>) {
    return readStepState({
      ...at,
      runId: at.topLevelRunId,
      stepId: stepPlaceKey(at.place),
    });
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

  test("reads a body step's park from the key of its place", async () => {
    const stepPath = ["section", "s"];
    const at = await coordinate("run-body-1", 1, { stepPath });
    await parkColdStep(
      at.substrate,
      "run-body-1",
      1,
      [pendingApproval("corr-1", snapshot)],
      stepPath,
    );

    expect(await readColdParkedPendingOperations(at)).toEqual([
      pendingApproval("corr-1", snapshot),
    ]);
    expect(
      await readColdParkedPendingOperations(
        await coordinate("run-body-1", 1, { stepPath: ["other", "s"] }),
      ),
    ).toEqual([]);
  });

  test("ignores a park another invocation of the same place committed", async () => {
    const stepPath = ["section", "s"];
    const at = await coordinate("run-body-2", 1, { stepPath });
    await parkColdStep(
      at.substrate,
      "run-body-1",
      1,
      [pendingApproval("corr-1", snapshot)],
      stepPath,
    );

    expect(await readColdParkedPendingOperations(at)).toEqual([]);
    expect(
      await readColdParkedApprovalSnapshot({ ...at, correlationId: "corr-1" }),
    ).toBeUndefined();
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
      runId: at.topLevelRunId,
      stepId: stepPlaceKey(at.place),
      lifetime: {
        kind: "invocation",
        invocation: { runId: at.runId, stepId: at.stepId, attempt: at.attempt },
      },
      mailboxAddress: "agent@example.com",
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

  test("reads the committed state over a restore lost between its two writes", async () => {
    const at = await coordinate("run-1", 1);
    // The attempt parked on another host; this host replaces it.
    await runColdTurn(
      at,
      [turn("ask")],
      [pendingApproval("corr-1", snapshot)],
      "mirrored",
      await makeTempDir(),
    );
    const store = await createDurableConversationStore({
      localStoreDir: stepStorageRoot(at),
      signer: testSigner,
      substrate: at.substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: at.topLevelRunId,
      stepId: stepPlaceKey(at.place),
      lifetime: {
        kind: "invocation",
        invocation: { runId: at.runId, stepId: at.stepId, attempt: at.attempt },
      },
      mailboxAddress: "agent@example.com",
    });
    // The host is lost at the restore's second write, whichever file it is.
    const lost = new Error("host lost");
    let writes = 0;
    const local = store.storage;
    const writeTurns = local.writeTurns.bind(local);
    const writeMetadata = local.writeMetadata.bind(local);
    const turnsSpy = spyOn(local, "writeTurns").mockImplementation(
      (...args) => {
        writes += 1;
        return writes === 2 ? Promise.reject(lost) : writeTurns(...args);
      },
    );
    const metadataSpy = spyOn(local, "writeMetadata").mockImplementation(
      (...args) => {
        writes += 1;
        return writes === 2 ? Promise.reject(lost) : writeMetadata(...args);
      },
    );
    try {
      await expect(store.restoreFromSubstrate()).rejects.toBe(lost);
    } finally {
      turnsSpy.mockRestore();
      metadataSpy.mockRestore();
    }

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
        mailboxAddress: "agent@example.com",
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
      const committed = await readCommitted(at);
      expect(committed?.turns).toEqual([turn("ask"), turn("approved")]);
      expect(committed?.invocation).toEqual({
        runId: "run-1",
        stepId: "s",
        attempt: 1,
      });
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

      const committed = await readCommitted(at);
      expect(committed?.invocation?.attempt).toBe(2);
      expect(committed?.turns).toEqual([turn("retry")]);
      expect(committed?.pendingOperations).toEqual([
        pendingApproval("corr-2", snapshot),
      ]);
    });

    test("commits another invocation's park recorded locally over a rolled-back park", async () => {
      const stepPath = ["section", "s"];
      const first = await coordinate("run-body-1", 1, { stepPath });
      const gate = (correlationId: string): PendingOperation => ({
        ...pendingApproval(correlationId, snapshot),
        suspendedCall: {
          id: `call-${correlationId}`,
          name: "t",
          arguments: {},
        },
      });
      await runColdTurn(
        first,
        [turn("first event")],
        [gate("corr-1")],
        "mirrored",
      );
      // The next event's body continues the conversation rolled back to the
      // start of the first body's parked turn, parks, and crashes before its
      // mirror.
      const second = {
        ...first,
        runId: "run-body-2",
      };
      await runColdTurn(
        second,
        [turn("second event")],
        [gate("corr-2")],
        "crashed",
      );

      expect(await readColdParkedPendingOperations(second)).toEqual([
        gate("corr-2"),
      ]);
      await persist(second, "corr-2");

      const committed = await readCommitted(second);
      expect(committed?.turns).toEqual([turn("second event")]);
      expect(committed?.pendingOperations).toEqual([gate("corr-2")]);
      expect(committed?.parkedTurnStart).toBe(0);
      expect(committed?.invocation?.runId).toBe("run-body-2");
      expect(
        await readColdParkedApprovalSnapshot({
          ...(await onReplacementHost(second)),
          correlationId: "corr-2",
        }),
      ).toEqual(snapshot);
    });

    test("commits a body step's park under the key of its place", async () => {
      const stepPath = ["section", "s"];
      const at = await coordinate("run-body-1", 1, { stepPath });
      await runColdTurn(
        at,
        [turn("ask")],
        [pendingApproval("corr-1", snapshot)],
        "crashed",
      );

      await persist(at, "corr-1");

      const committed = await readCommitted(at);
      expect(committed?.pendingOperations).toEqual([
        pendingApproval("corr-1", snapshot),
      ]);
      expect(committed?.invocation).toEqual({
        runId: "run-body-1",
        stepId: "s",
        attempt: 1,
      });
      expect(
        await readStepState({ ...at, runId: "run-body-1", stepId: "s" }),
      ).toBeNull();
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

describe("createParkedApprovalHooks", () => {
  const agent = defineAgent({
    id: "a",
    systemPrompt: "s",
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "anthropic", model: "m" }] },
  });
  const agentBody = (id: string): WorkflowDefinition =>
    defineWorkflow({
      id,
      trigger: { type: "manual" },
      steps: { s: step({ agent }) },
    });

  // A deployment whose hooks read the parks of its runs.
  async function deployment(): Promise<{
    substrate: RepoStore;
    hooks: ReturnType<typeof createParkedApprovalHooks>;
  }> {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    const hooks = createParkedApprovalHooks({
      dataDir,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      signer: testSigner,
      mailboxAddress: "agent@example.com",
      topLevelRunId: RUN_ID,
    });
    return { substrate, hooks };
  }

  test.each([
    ["the top-level run", RUN_ID, ["s"]],
    ["a childWorkflow child", "run-child", ["child", "s"]],
    [
      "an onTrigger body",
      sectionBodyRunId(RUN_ID, "section", 0),
      ["section", "s"],
    ],
    [
      "a loop iteration inside a child",
      loopBodyRunId("run-child", "loop", 0),
      ["child", "loop", "s"],
    ],
  ])(
    "%s reads its parks from its cold step state",
    async (_name, runId, stepPath) => {
      const { substrate, hooks } = await deployment();
      await parkColdStep(
        substrate,
        runId,
        1,
        [pendingApproval("corr-cold", snapshot)],
        stepPath,
      );
      const at = { runId, stepId: "s", attempt: 1, place: { stepPath } };

      expect(
        await hooks.loadParkedApproval({ ...at, correlationId: "corr-cold" }),
      ).toEqual(snapshot);
      expect(await hooks.readParkedApprovalOps(at)).toEqual([
        { correlationId: "corr-cold" },
      ]);
    },
  );

  test("a body does not adopt the park an earlier body of its place committed", async () => {
    const { substrate, hooks } = await deployment();
    const stepPath = ["section", "s"];
    await parkColdStep(
      substrate,
      sectionBodyRunId(RUN_ID, "section", 0),
      1,
      [
        {
          ...pendingApproval("corr-stale", snapshot),
          suspendedCall: { id: "call-stale", name: "t", arguments: {} },
        },
      ],
      stepPath,
    );
    const at = {
      runId: sectionBodyRunId(RUN_ID, "section", 1),
      stepId: "s",
      attempt: 1,
      place: { stepPath },
    };

    expect(await hooks.readParkedApprovalOps(at)).toEqual([]);
    expect(
      await hooks.loadParkedApproval({ ...at, correlationId: "corr-stale" }),
    ).toBeUndefined();
    await expect(
      hooks.persistRecoveredPark({ ...at, correlationId: "corr-stale" }),
    ).rejects.toThrow(
      "neither the committed step state nor a current local store holds its pending operation",
    );
  });

  // A loop must name another step as its onExhausted, so a workflow's one
  // step cannot be a loop.
  test.each(["loop", "done"])(
    "a deployment's one step cannot be a loop, whose onExhausted names %s",
    (onExhausted) => {
      expect(() =>
        defineWorkflow({
          id: "looped",
          trigger: { type: "manual" },
          steps: {
            loop: loop({
              body: agentBody("loop-body"),
              while: "cont",
              carry: "next",
              input: { literal: 0 },
              maxIterations: 3,
              onExhausted,
            }),
          },
        }),
      ).toThrow("onExhausted");
    },
  );

  // The container plans its relays from the body's parks, read through its
  // own env, while the body recovers its park through the body's.
  test("a resume relays an approval its spawned body stored but never logged", async () => {
    const dataDir = await makeTempDir();
    const substrate = await createSubstrate(dataDir);
    const childRunId = "run-body";
    const outer = defineWorkflow({
      id: "outer",
      trigger: { type: "manual" },
      steps: { spawn: childWorkflow({ definition: agentBody("body") }) },
    });
    const { workflow: rewritten, bodies } =
      rewriteInlineChildWorkflowBodies(outer);
    const body = bodies[0]?.definition;
    if (body === undefined) throw new Error("the inline body was not lifted");
    const hooks = createParkedApprovalHooks({
      dataDir,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      ...COMMITTED_READS,
      signer: testSigner,
      mailboxAddress: "agent@example.com",
      topLevelRunId: RUN_ID,
    });
    const invokeStep: StepInvoker = async (req) =>
      req.resume !== undefined
        ? { output: null }
        : {
            suspend: {
              correlationId: "corr-1",
              kind: "approval",
              approvalSnapshot: snapshot,
            },
          };
    const envFor = (
      repoStore: RuntimeRepoStore,
      run: WorkflowDefinition,
      extra: Partial<WorkflowRuntimeEnv>,
    ): WorkflowRuntimeEnv => {
      const clock = (): Date => new Date();
      let ids = 0;
      return {
        repoStore,
        scheduler: createInMemoryScheduler({ repoStore, clock }),
        signalChannel: createInMemorySignalChannel(),
        blobs: createInMemoryBlobSubstrate(),
        directors: createDefaultDirectorRegistry(),
        authorize: async () => ({
          effect: "allow",
          matchingGrants: [],
          resolvedBy: null,
        }),
        invokeStep,
        spawnChild: () => Promise.reject(new Error("the body spawns nothing")),
        clock,
        newId: (prefix) =>
          prefix === "run" ? childRunId : `${prefix}-${String((ids += 1))}`,
        drain: createNoopDrainController(run),
        hasUpstreamSignalResolver: true,
        ...extra,
      };
    };
    const outerEnv = (
      repoStore: RuntimeRepoStore,
      parkHooks: Partial<typeof hooks>,
      onPark: () => void,
    ): WorkflowRuntimeEnv =>
      envFor(repoStore, rewritten, {
        ...parkHooks,
        onPark,
        spawnChild: async (spawn) =>
          createSuspendableChildHandle(envFor(repoStore, body, parkHooks), {
            definition: body,
            childRunId: spawn.childRunId,
            input: spawn.input,
            depth: spawn.depth,
            maxChildSpawnDepth: spawn.maxChildSpawnDepth,
            stepPathPrefix: spawn.stepPathPrefix,
            signal: spawn.signal,
            ...(spawn.resumeFromEvents !== undefined
              ? { resumeFromEvents: spawn.resumeFromEvents }
              : {}),
            surfaceInputPark: true,
          }),
      });

    // Run to the relayed park for the logs, then drop both from the body's
    // park on: the body stored its approval but crashed before logging it.
    const firstRepo = createInMemoryRepoStore();
    const relayedFirst = Promise.withResolvers<undefined>();
    const stop = new AbortController();
    const first = runtimeRun(
      rewritten,
      outerEnv(firstRepo, {}, () => relayedFirst.resolve(undefined)),
      { runId: RUN_ID, localAbort: stop.signal },
    );
    await relayedFirst.promise;
    const beforePark = async (runId: string): Promise<WorkflowEvent[]> => {
      const log = [...(await firstRepo.read(runId))];
      const parked = log.findIndex((event) => event.kind === "SignalAwaited");
      expect(parked).toBeGreaterThan(0);
      return log.slice(0, parked);
    };
    const outerLog = await beforePark(RUN_ID);
    const bodyLog = await beforePark(childRunId);
    stop.abort();
    await first.complete;
    await parkColdStep(
      substrate,
      childRunId,
      1,
      [pendingApproval("corr-1", snapshot)],
      ["spawn", "s"],
    );

    const repo = createInMemoryRepoStore();
    await repo.appendBatch(childRunId, bodyLog);
    const relayed = Promise.withResolvers<undefined>();
    const resumed = runtimeRun(
      rewritten,
      outerEnv(repo, hooks, () => relayed.resolve(undefined)),
      { runId: RUN_ID, resumeFromEvents: outerLog },
    );
    await relayed.promise;
    await resumed.signal(
      signalName("corr-1"),
      { outcome: "approved" },
      "approval-decision",
    );
    expect((await resumed.complete).terminalStatus).toBe("completed");
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
      turnId: "turn-corr-timeout",
      gated: true,
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

describe("createDeploymentHostScheduler", () => {
  test("a retire the scheduler flushes sends park.release", async () => {
    const substrate = await createSubstrate(await makeTempDir());
    const runId = "run_scheduler_flush";
    const at = "2026-01-01T00:00:00.000Z";
    const history = createWorkflowRunRepoStore({
      substrate,
      repoId: WORKFLOW_RUN_REPO_ID,
      principal: PRINCIPAL,
      ref: WORKFLOW_RUN_REF,
    });
    await history.appendBatch(runId, [
      {
        kind: "RunStarted",
        seq: 1,
        at,
        runId,
        definitionHash: "definition",
        trigger: { type: "manual", payload: null },
      },
      {
        kind: "StepStarted",
        seq: 2,
        at,
        stepId: "ask",
        attempt: 1,
        input: { ref: "inline:null" },
      },
      {
        kind: "SignalAwaited",
        seq: 3,
        at,
        stepId: "ask",
        signalName: signalName("corr-1"),
        parkKind: "approval",
      },
      // Already due, so the scheduler fires it as it starts.
      { kind: "TimerSet", seq: 4, at, timerId: "t-due", fireAt: at },
    ]);
    // The run buffers a retire that nothing has flushed when the scheduler
    // commits the timer.
    await commitBuffered({ repoStore: history }, runId, {
      kind: "SignalAwaitAbandoned",
      seq: 0,
      at,
      stepId: "ask",
      signalName: signalName("corr-1"),
    });
    const releases: unknown[] = [];
    const sender: ControlChannelSender = {
      seq: 0,
      send: (payload) => {
        if (payload.type === "park.release") releases.push(payload.data);
        return Promise.resolve();
      },
    };
    const scheduler = createDeploymentHostScheduler({
      substrate,
      principal: PRINCIPAL,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      parkReleases: createParkReleases(sender),
    });
    try {
      await scheduler.start();
      await waitForEvent(
        history,
        runId,
        (event) => event.kind === "TimerFired",
      );

      expect(releases).toEqual([{ runId, correlationId: "corr-1" }]);
      expect((await history.read(runId)).map((event) => event.kind)).toEqual([
        "RunStarted",
        "StepStarted",
        "SignalAwaited",
        "TimerSet",
        "SignalAwaitAbandoned",
        "TimerFired",
      ]);
    } finally {
      await scheduler.stop();
      dropChain(runId);
    }
  });
});
