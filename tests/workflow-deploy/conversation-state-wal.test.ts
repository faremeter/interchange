// Unit tests for the warm-agent durable conversation store's two-tier
// WAL + checkpoint layout (Phase D1). These cover the three properties the
// D1 plan calls out:
//
//   (a) bounded WAL: writing N > K turns keeps the live WAL <= K entries
//       between checkpoints, and a checkpoint folds at the K boundary;
//   (b) exact restore: reconstruct the EXACT turn list + metadata after a
//       mix of checkpoint + WAL-tail (N = K + a few);
//   (c) anti-regression for the O(N^2) bug: each per-turn WAL append
//       payload is ONE turn's delta, never the whole conversation -- the
//       property whose absence produced the measured ~60 ms/msg growth.
//
// The tests drive a REAL `createRepoStore` workflow-run substrate and a
// REAL isogit local store (the production path), so the bucket/checkpoint
// commits, the preserve-prefix merges, and the working-tree reconstruction
// are all exercised end to end -- not mocked.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { type } from "arktype";

import { generateKeyPair } from "@intx/crypto";
import { createSSHSignature } from "@intx/crypto";
import type {
  KeyPair,
  ConnectorThreadState,
  ConversationTurn,
  PendingOperation,
  TokenUsage,
} from "@intx/types/runtime";
import { waitUntil } from "@intx/types/testing";
import type {
  RepoId,
  RepoStore,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  createRepoStore,
  workflowRunKindHandler,
  buildStepStateCheckpoint,
  serializeStepStateWalEntry,
  stepStateWalEntryPath,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
  workflowRunStepStatePrefix,
} from "@intx/hub-sessions";
import { userTurn } from "@intx/inference-testing";
import {
  createDurableConversationStore,
  readDurableConversation,
  reconstructDurableConversation,
  type DurableConversationLifetime,
  type DurableConversationStore,
} from "@intx/sidecar-app/src/conversation-state";

const WORKFLOW_RUN_REF = "refs/heads/main";
const RUN_ID = "run_durability";
const STEP_ID = "step-1";
// Must mirror the production constant in conversation-state.ts. Asserted
// indirectly by the bounded-WAL test below: a drift here would surface as
// a checkpoint that folds at the wrong boundary.
const CHECKPOINT_INTERVAL = 64;

const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  anchorRunId: "durability-unit",
};

function tokenUsage(input: number): TokenUsage {
  return { input, output: 0, cacheRead: 0, cacheWrite: 0, thinking: 0 };
}

const CheckpointMetaShape = type({
  checkpointSeq: "number",
  turnCount: "number",
});

// Mirror of the on-disk per-boundary WAL entry shape (boundary seq + the
// boundary's new-turn delta array + metadata). Validating at the read
// boundary keeps the test honest about the layout without an unchecked `as`.
const WalEntryShape = type({
  seq: "number",
  turns: "unknown[]",
});

/** Read and validate the checkpoint pointer's seq/count fields. */
function readCheckpointMeta(stateDir: string): {
  checkpointSeq: number;
  turnCount: number;
} {
  const raw: unknown = JSON.parse(
    fs.readFileSync(path.join(stateDir, "checkpoint.meta.json"), "utf8"),
  );
  const meta = CheckpointMetaShape(raw);
  if (meta instanceof type.errors) {
    throw new Error(`checkpoint.meta.json failed validation: ${meta.summary}`);
  }
  return meta;
}

interface Harness {
  baseDir: string;
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  signer: (payload: string) => Promise<string>;
  stateDir: string;
}

async function makeHarness(): Promise<Harness> {
  const baseDir = await fs.promises.mkdtemp(
    path.join(os.tmpdir(), "conversation-state-unit-"),
  );
  const signingKey: KeyPair = await generateKeyPair();
  const workflowRunRepoId: RepoId = {
    kind: "workflow-run",
    id: "durability-unit",
  };
  const substrate = createRepoStore({
    dataDir: baseDir,
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  await substrate.writeTree(
    { kind: "hub" },
    workflowRunRepoId,
    WORKFLOW_RUN_REF,
    {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    },
  );
  const signer = (payload: string): Promise<string> =>
    Promise.resolve(
      createSSHSignature(payload, signingKey.privateKey, signingKey.publicKey),
    );
  const stateDir = path.join(
    substrate.getRepoDir(workflowRunRepoId),
    workflowRunStepStatePrefix(RUN_ID, STEP_ID),
  );
  return { baseDir, substrate, workflowRunRepoId, signer, stateDir };
}

async function makeStore(
  h: Harness,
  localDir: string,
  lifetime: DurableConversationLifetime = { kind: "deployment" },
): Promise<DurableConversationStore> {
  return createDurableConversationStore({
    localStoreDir: localDir,
    signer: h.signer,
    substrate: h.substrate,
    workflowRunRepoId: h.workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    runId: RUN_ID,
    stepId: STEP_ID,
    lifetime,
  });
}

/**
 * Append turns to the store's local isogit store exactly as the warm
 * agent's reactor would (writeTurns + writeMetadata + commit), then run the
 * synchronous run-boundary mirror. Returns the full turn list so callers
 * can assert exact reconstruction.
 */
async function pushAndMirror(
  store: DurableConversationStore,
  priorTurns: ConversationTurn[],
  newTurns: ConversationTurn[],
  meta: { tokenUsageInput: number },
): Promise<ConversationTurn[]> {
  const all = [...priorTurns, ...newTurns];
  await store.storage.writeTurns(all);
  await store.storage.writeMetadata({
    pendingOperations: [],
    tokenUsage: tokenUsage(meta.tokenUsageInput),
  });
  await store.storage.commit({ message: "turn" });
  await store.mirrorToSubstrate();
  return all;
}

function walBucketDir(stateDir: string, bucket: number): string {
  return path.join(stateDir, "wal", String(bucket));
}

/** Count every WAL entry blob across every bucket. */
function countWalEntries(stateDir: string): number {
  const walRoot = path.join(stateDir, "wal");
  if (!fs.existsSync(walRoot)) return 0;
  let count = 0;
  for (const bucket of fs.readdirSync(walRoot)) {
    count += fs
      .readdirSync(path.join(walRoot, bucket))
      .filter((f) => f.endsWith(".json")).length;
  }
  return count;
}

function readWalEntry(stateDir: string, seq: number): unknown {
  const bucket = Math.floor(seq / 128);
  const raw = fs.readFileSync(
    path.join(walBucketDir(stateDir, bucket), `${String(seq)}.json`),
    "utf8",
  );
  return JSON.parse(raw);
}

describe("durable conversation store WAL + checkpoint (Phase D1)", () => {
  let h: Harness;
  let localDir: string;

  beforeEach(async () => {
    h = await makeHarness();
    localDir = path.join(h.baseDir, "local");
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  test("a per-boundary WAL append payload is only the new-turn delta, not the whole conversation", async () => {
    // This is THE anti-regression test for the O(N^2) bug: the old
    // whole-blob mirror re-serialized every prior turn on every message.
    // After D1, each per-boundary WAL entry carries only that boundary's
    // new turns (here exactly one) -- so its size is independent of how many
    // turns precede it.
    const store = await makeStore(h, localDir);

    const built: ConversationTurn[] = [];
    for (let i = 0; i < 8; i += 1) {
      const turn = userTurn(`m${String(i)}`);
      built.push(turn);
      await store.storage.writeTurns([...built]);
      await store.storage.writeMetadata({
        pendingOperations: [],
        tokenUsage: tokenUsage(i),
      });
      await store.storage.commit({ message: "turn" });
      await store.mirrorToSubstrate();

      // The WAL entry just written for boundary i carries exactly that
      // boundary's one new turn and not any prior turn.
      const entry = readWalEntry(h.stateDir, i);
      const validated = WalEntryShape(entry);
      if (validated instanceof type.errors) {
        throw new Error(
          `WAL entry ${String(i)} failed validation: ${validated.summary}`,
        );
      }
      expect(validated.seq).toBe(i);
      // The entry's turns array is the DELTA (one new turn) -- never the
      // full conversation. A regression to the O(N) whole-blob payload would
      // make this array length grow with i.
      expect(validated.turns.length).toBe(1);
      expect(validated.turns[0]).toEqual(turn);
    }

    // The serialized size of the latest entry is within a small constant of
    // the first entry's size: it does not grow with the turn count.
    const first = fs.statSync(
      path.join(walBucketDir(h.stateDir, 0), "0.json"),
    ).size;
    const last = fs.statSync(
      path.join(walBucketDir(h.stateDir, 0), "7.json"),
    ).size;
    expect(Math.abs(last - first)).toBeLessThan(64);
  });

  test("a turnless boundary still durably persists advanced metadata", async () => {
    // The regression the per-turn keying caused: a mirror boundary with NO
    // new turns dropped its metadata, so a respawn restored STALE metadata.
    // onRunBoundary -> mirrorToSubstrate runs in step-invoker's finally even
    // when the send throws, so a turnless-but-metadata-mutating boundary is
    // reachable. Each boundary must commit its metadata UNCONDITIONALLY.
    const store = await makeStore(h, localDir);

    // Boundary 1: one turn, baseline metadata (tokenUsage.input=10).
    const turns = [userTurn("only")];
    await store.storage.writeTurns(turns);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(10),
    });
    await store.storage.commit({ message: "turn" });
    await store.mirrorToSubstrate();

    // Boundary 2: NO new turns, but metadata advances (tokenUsage.input
    // 10->99, pendingOperations 0->1). The turn list is unchanged.
    const pendingOp: PendingOperation = {
      correlationId: "corr-1",
      kind: "approval",
      registeredAt: 0,
      gateId: "gate-1",
    };
    await store.storage.writeTurns(turns);
    await store.storage.writeMetadata({
      pendingOperations: [pendingOp],
      tokenUsage: tokenUsage(99),
    });
    await store.storage.commit({ message: "metadata-only" });
    await store.mirrorToSubstrate();

    // Two WAL entries exist: boundary 0 (one turn) and boundary 1 (zero
    // turns, advanced metadata).
    expect(countWalEntries(h.stateDir)).toBe(2);
    const turnlessEntry = WalEntryShape(readWalEntry(h.stateDir, 1));
    if (turnlessEntry instanceof type.errors) {
      throw new Error(`turnless entry invalid: ${turnlessEntry.summary}`);
    }
    expect(turnlessEntry.turns.length).toBe(0);

    // A fresh store (a respawn) reconstructs the LATEST metadata, not the
    // stale boundary-1 values. This is the assertion that would have caught
    // the dropped-metadata defect.
    const reconstructed = await reconstructDurableConversation(
      h.stateDir,
      STEP_ID,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual(turns);
    expect(reconstructed.tokenUsage).toEqual(tokenUsage(99));
    expect(reconstructed.pendingOperations).toEqual([pendingOp]);
  });

  test("writing N > K turns keeps the live WAL <= K and folds a checkpoint at K", async () => {
    const store = await makeStore(h, localDir);

    const n = CHECKPOINT_INTERVAL + 5;
    const built: ConversationTurn[] = [];
    for (let i = 0; i < n; i += 1) {
      built.push(userTurn(`t${String(i)}`));
      await store.storage.writeTurns([...built]);
      await store.storage.writeMetadata({
        pendingOperations: [],
        tokenUsage: tokenUsage(i),
      });
      await store.storage.commit({ message: "turn" });
      await store.mirrorToSubstrate();

      const totalCommitted = i + 1;
      const liveWal = countWalEntries(h.stateDir);
      // The live WAL never exceeds K: it grows turn by turn until it hits
      // K, then compaction folds it to empty.
      expect(liveWal).toBeLessThanOrEqual(CHECKPOINT_INTERVAL);

      const checkpointMetaPath = path.join(h.stateDir, "checkpoint.meta.json");
      if (totalCommitted < CHECKPOINT_INTERVAL) {
        // No checkpoint has folded yet; everything is in the WAL.
        expect(fs.existsSync(checkpointMetaPath)).toBe(false);
        expect(liveWal).toBe(totalCommitted);
      } else if (totalCommitted === CHECKPOINT_INTERVAL) {
        // The K-th turn triggers compaction: the WAL truncates to empty and
        // the checkpoint folds exactly K turns.
        expect(liveWal).toBe(0);
        const meta = readCheckpointMeta(h.stateDir);
        expect(meta.checkpointSeq).toBe(CHECKPOINT_INTERVAL);
        expect(meta.turnCount).toBe(CHECKPOINT_INTERVAL);
      } else {
        // After the fold, the WAL holds only the post-checkpoint tail.
        expect(liveWal).toBe(totalCommitted - CHECKPOINT_INTERVAL);
      }
    }
  });

  describe("a failing compaction", () => {
    function storeFailingCheckpoints(
      failures: number,
    ): Promise<DurableConversationStore> {
      let remaining = failures;
      const writeTreePreservingPrefix: RepoStore["writeTreePreservingPrefix"] =
        (principal, repoId, ref, args) => {
          if (!args.preservePrefix.includes("/wal/") && remaining > 0) {
            remaining -= 1;
            return Promise.reject(new Error("checkpoint write refused"));
          }
          return h.substrate.writeTreePreservingPrefix(
            principal,
            repoId,
            ref,
            args,
          );
        };
      const substrate = new Proxy(h.substrate, {
        get(target, prop, receiver): unknown {
          if (prop === "writeTreePreservingPrefix") {
            return writeTreePreservingPrefix;
          }
          return Reflect.get(target, prop, receiver);
        },
      });
      return createDurableConversationStore({
        localStoreDir: localDir,
        signer: h.signer,
        substrate,
        workflowRunRepoId: h.workflowRunRepoId,
        workflowRunRef: WORKFLOW_RUN_REF,
        principal: PRINCIPAL,
        runId: RUN_ID,
        stepId: STEP_ID,
        lifetime: { kind: "deployment" },
      });
    }

    test("keeps the boundary and compacts at the next one", async () => {
      const store = await storeFailingCheckpoints(1);
      let turns: ConversationTurn[] = [];
      for (let i = 0; i < CHECKPOINT_INTERVAL; i += 1) {
        turns = await pushAndMirror(store, turns, [userTurn(`t${String(i)}`)], {
          tokenUsageInput: i,
        });
      }
      expect(countWalEntries(h.stateDir)).toBe(CHECKPOINT_INTERVAL);
      expect(fs.existsSync(path.join(h.stateDir, "checkpoint.meta.json"))).toBe(
        false,
      );

      turns = await pushAndMirror(store, turns, [userTurn("next")], {
        tokenUsageInput: CHECKPOINT_INTERVAL,
      });
      expect(countWalEntries(h.stateDir)).toBe(0);
      expect(readCheckpointMeta(h.stateDir)).toMatchObject({
        checkpointSeq: CHECKPOINT_INTERVAL + 1,
        turnCount: CHECKPOINT_INTERVAL + 1,
      });
      expect(
        (await reconstructDurableConversation(h.stateDir, STEP_ID))?.turns,
      ).toEqual(turns);
    });

    test("fails the mirror once the WAL reaches twice the interval, keeping every turn", async () => {
      const store = await storeFailingCheckpoints(Number.POSITIVE_INFINITY);
      const limit = 2 * CHECKPOINT_INTERVAL;
      let turns: ConversationTurn[] = [];
      for (let i = 0; i < limit - 1; i += 1) {
        turns = await pushAndMirror(store, turns, [userTurn(`t${String(i)}`)], {
          tokenUsageInput: i,
        });
      }

      turns = [...turns, userTurn("last")];
      await store.storage.writeTurns(turns);
      await store.storage.commit({ message: "turn" });
      await expect(store.mirrorToSubstrate()).rejects.toThrow(
        `failed with ${String(limit)} WAL entries uncompacted`,
      );
      expect(countWalEntries(h.stateDir)).toBe(limit);
      expect(
        (await reconstructDurableConversation(h.stateDir, STEP_ID))?.turns,
      ).toEqual(turns);
    });
  });

  test("restore reconstructs the EXACT turn list and metadata across checkpoint + WAL tail", async () => {
    const store = await makeStore(h, localDir);

    const n = CHECKPOINT_INTERVAL + 7;
    const built: ConversationTurn[] = [];
    for (let i = 0; i < n; i += 1) {
      // Distinct content + model per turn so an off-by-one or reordering in
      // the fold/replay would change the reconstructed list. Assistant
      // turns carry a `model`; user turns omit the optional field entirely
      // (exactOptionalPropertyTypes forbids an explicit `undefined`).
      const turn: ConversationTurn =
        i % 2 === 0
          ? {
              role: "user",
              content: [{ type: "text", text: `turn-${String(i)}` }],
              timestamp: i,
            }
          : {
              role: "assistant",
              content: [{ type: "text", text: `turn-${String(i)}` }],
              model: "stub-model",
              timestamp: i,
            };
      built.push(turn);
      await store.storage.writeTurns([...built]);
      await store.storage.writeMetadata({
        pendingOperations: [],
        tokenUsage: tokenUsage(i),
      });
      await store.storage.commit({ message: "turn" });
      await store.mirrorToSubstrate();
    }

    // Sanity: the conversation now spans a folded checkpoint plus a WAL
    // tail, the exact mix the restore must stitch.
    expect(fs.existsSync(path.join(h.stateDir, "checkpoint.json"))).toBe(true);
    expect(countWalEntries(h.stateDir)).toBe(n - CHECKPOINT_INTERVAL);

    // Reconstruct via the production read path and assert byte-equivalent
    // turns + the latest metadata.
    const reconstructed = await reconstructDurableConversation(
      h.stateDir,
      STEP_ID,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual(built);
    expect(reconstructed.tokenUsage).toEqual(tokenUsage(n - 1));
    expect(reconstructed.connectorState).toBeNull();
    expect(reconstructed.pendingOperations).toEqual([]);

    // A fresh store (modelling a respawn with an empty local FS) restores
    // the same conversation into its previously-empty local store -- the
    // cross-respawn continuity guarantee, now through checkpoint + WAL.
    const freshLocalDir = path.join(h.baseDir, "respawn-local");
    const fresh = await makeStore(h, freshLocalDir);
    const found = await fresh.restoreFromSubstrate();
    expect(found).toBe(true);
    const loaded = await fresh.storage.load();
    expect(loaded.turns).toEqual(built);
    expect(loaded.tokenUsage).toEqual(tokenUsage(n - 1));
  });

  test("a restore with no prior durable state returns false (genuine first run)", async () => {
    const store = await makeStore(h, localDir);
    expect(await store.restoreFromSubstrate()).toBe(false);
    const loaded = await store.storage.load();
    expect(loaded.turns).toEqual([]);
  });

  test("a corrupt WAL entry throws on reconstruction (no silent fresh start)", async () => {
    const store = await makeStore(h, localDir);
    await pushAndMirror(store, [], [userTurn("only")], { tokenUsageInput: 1 });

    // Corrupt the single WAL blob in place.
    fs.writeFileSync(
      path.join(walBucketDir(h.stateDir, 0), "0.json"),
      "{ not json",
    );
    await expect(
      reconstructDurableConversation(h.stateDir, STEP_ID),
    ).rejects.toThrow(/not valid JSON/);
  });

  test("a seq gap in the WAL throws on reconstruction (a lost turn must surface)", async () => {
    const store = await makeStore(h, localDir);
    const built: ConversationTurn[] = [];
    for (let i = 0; i < 3; i += 1) {
      built.push(userTurn(`g${String(i)}`));
      await store.storage.writeTurns([...built]);
      await store.storage.writeMetadata({
        pendingOperations: [],
        tokenUsage: tokenUsage(i),
      });
      await store.storage.commit({ message: "turn" });
      await store.mirrorToSubstrate();
    }
    // Remove the middle WAL entry to simulate a lost append.
    fs.rmSync(path.join(walBucketDir(h.stateDir, 0), "1.json"));
    await expect(
      reconstructDurableConversation(h.stateDir, STEP_ID),
    ).rejects.toThrow(/seq gap/);
  });

  test("a turn appended during the WAL write is not skipped by the next mirror", async () => {
    // Regression guard: mirrorToSubstrate slices its new-turn delta from the
    // reactor's live array BEFORE the appendWalEntry await, then advances the
    // mirrored turn count AFTER it. peekTurns returns that array by reference,
    // so a turn the reactor appends DURING the await must be counted as the
    // count actually persisted -- not the post-await live length -- or the
    // next mirror slices past it and drops it from the WAL permanently.
    const liveTurns: ConversationTurn[] = [userTurn("a")];
    let injected = false;

    // Wrap the substrate so the first WAL append (boundary 0) appends a turn
    // to the reactor's live array mid-write, reproducing the concurrent
    // append in the between-slice-and-count window.
    const writeTreePreservingPrefix: RepoStore["writeTreePreservingPrefix"] = (
      principal,
      repoId,
      ref,
      args,
    ) => {
      if (!injected && args.preservePrefix.includes("/wal/")) {
        injected = true;
        liveTurns.push(userTurn("b"));
      }
      return h.substrate.writeTreePreservingPrefix(
        principal,
        repoId,
        ref,
        args,
      );
    };
    const wrappedSubstrate = new Proxy(h.substrate, {
      get(target, prop, receiver): unknown {
        if (prop === "writeTreePreservingPrefix") {
          return writeTreePreservingPrefix;
        }
        return Reflect.get(target, prop, receiver);
      },
    });

    const store = await createDurableConversationStore({
      localStoreDir: localDir,
      signer: h.signer,
      substrate: wrappedSubstrate,
      workflowRunRepoId: h.workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: STEP_ID,
      lifetime: { kind: "deployment" },
    });

    // Boundary 0: the local store holds [a]; during its WAL append the
    // wrapper appends b to the reactor's live array.
    await store.storage.writeTurns(liveTurns);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(0),
    });
    await store.storage.commit({ message: "turn-a" });
    await store.mirrorToSubstrate();
    expect(injected).toBe(true);

    // Boundary 1: the reactor has since persisted [a, b] locally. The mirror
    // must pick up b. The pre-fix code counted b as already mirrored at
    // boundary 0 (reading the live array length after the await) and sliced
    // past it here, so boundary 1's WAL entry was an empty delta and b was
    // lost from the durable log.
    await store.storage.writeTurns(liveTurns);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(1),
    });
    await store.storage.commit({ message: "turn-b" });
    await store.mirrorToSubstrate();

    const entry = WalEntryShape(readWalEntry(h.stateDir, 1));
    if (entry instanceof type.errors) {
      throw new Error(`WAL entry 1 failed validation: ${entry.summary}`);
    }
    expect(entry.turns.length).toBe(1);

    // Reconstruction yields both turns; under the bug it would yield only [a].
    const reconstructed = await reconstructDurableConversation(
      h.stateDir,
      STEP_ID,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual([userTurn("a"), userTurn("b")]);
  });

  test("overlapping mirror runs serialize onto distinct boundaries and lose no turns", async () => {
    // The dormant race this guards against: the awaited onRunBoundary mirror
    // and the fire-and-forget onStateChanged mirror share the mirrored-count
    // state with no serialization. Two overlapping runs both read the same
    // boundary seq AND the same mirroredTurnCount, so they collide on one
    // boundary and double-advance the turn count -- and a following mirror
    // then slices past a genuinely new turn and drops it from the log.
    //
    // The barrier makes the interleave deterministic: the first WAL append is
    // held until a would-be-concurrent second mirror has had time to reach
    // its own append. Without serialization the second append runs on the
    // stale counts; with it the second mirror does not start until the first
    // advances them.
    let releaseFirstAppend!: () => void;
    const firstAppendHeld = new Promise<void>((resolve) => {
      releaseFirstAppend = resolve;
    });
    let heldOnce = false;
    const writeTreePreservingPrefix: RepoStore["writeTreePreservingPrefix"] =
      async (principal, repoId, ref, args) => {
        if (!heldOnce && args.preservePrefix.includes("/wal/")) {
          heldOnce = true;
          await firstAppendHeld;
        }
        return h.substrate.writeTreePreservingPrefix(
          principal,
          repoId,
          ref,
          args,
        );
      };
    const wrappedSubstrate = new Proxy(h.substrate, {
      get(target, prop, receiver): unknown {
        if (prop === "writeTreePreservingPrefix") {
          return writeTreePreservingPrefix;
        }
        return Reflect.get(target, prop, receiver);
      },
    });

    const store = await createDurableConversationStore({
      localStoreDir: localDir,
      signer: h.signer,
      substrate: wrappedSubstrate,
      workflowRunRepoId: h.workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: STEP_ID,
      lifetime: { kind: "deployment" },
    });

    // The local store holds [a, b]; fire two overlapping mirrors of it.
    const ab = [userTurn("a"), userTurn("b")];
    await store.storage.writeTurns(ab);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(0),
    });
    await store.storage.commit({ message: "ab" });

    const both = Promise.all([
      store.mirrorToSubstrate(),
      store.mirrorToSubstrate(),
    ]);
    // Let a would-be-concurrent second mirror reach its append before the
    // first is released. Under serialization the second has not started, so
    // only the first append is held here.
    //
    // The duration is load-bearing and must stay a sleep: it IS the interleave
    // window this test measures. Under the correct implementation nothing
    // happens during it, so there is no state to wait for -- a predicate would
    // hold immediately and the second append would land after the release,
    // where it can no longer collide.
    await new Promise((resolve) => setTimeout(resolve, 40));
    releaseFirstAppend();
    await both;

    // A following mirror carries a genuinely new turn. If the overlapping
    // pair over-counted the mirrored turn count, this mirror slices past `c`
    // and drops it from the durable log.
    const abc = [...ab, userTurn("c")];
    await store.storage.writeTurns(abc);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(1),
    });
    await store.storage.commit({ message: "abc" });
    await store.mirrorToSubstrate();

    const reconstructed = await reconstructDurableConversation(
      h.stateDir,
      STEP_ID,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual(abc);
  });

  test("a mirror re-entered by restore's connector-state change cannot clobber a concurrent boundary", async () => {
    // Restore-reentrancy guard. restoreFromSubstrate calls
    // connectorRouter.restore(), which fires onStateChanged synchronously when
    // the restored connector state differs from the router's initial null --
    // and onStateChanged enqueues a (turnless) mirror. That reentrant mirror
    // shares the mirrored-count state with restore and with the next real
    // mirror. Unless restore runs on the same serialization tail (and
    // establishes the counts before the connector restore), the reentrant
    // append and a following real mirror can land on the SAME boundary seq,
    // and the turnless one can overwrite the real turn.
    //
    // Seed two durable boundaries, the second carrying a non-null connector
    // state -- exactly the future condition (a non-null connector state) under
    // which this otherwise-dormant path activates.
    const seed = await makeStore(h, localDir);
    await seed.storage.writeTurns([userTurn("a")]);
    await seed.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(0),
    });
    await seed.storage.commit({ message: "a" });
    await seed.mirrorToSubstrate();

    const connectorState: ConnectorThreadState = {
      threadRoot: "<root@example.com>",
      lastMessageId: "<last@example.com>",
      replyTo: "user@example.com",
      cc: [],
    };
    seed.storage.setConnectorState(connectorState);
    await seed.storage.writeTurns([userTurn("a"), userTurn("b")]);
    await seed.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(1),
    });
    await seed.storage.commit({ message: "b" });
    await seed.mirrorToSubstrate();

    // A fresh store restores those two boundaries; a barrier holds the first
    // WAL append (the reentrant turnless mirror) until a real `c` mirror is
    // enqueued behind it. Every boundary seq the substrate writes is recorded
    // (parsed from the append's commit message) so a same-seq clobber fails
    // loud even if a future change masks the turn-loss symptom.
    const seqsWritten: number[] = [];
    // Substrate writes that have RETURNED, as opposed to the entries recorded
    // in `seqsWritten` on the way in. The reentrant mirror is fire-and-forget,
    // so this is how the assertions below know its append landed.
    let writesCompleted = 0;
    let releaseFirstAppend!: () => void;
    const firstAppendHeld = new Promise<void>((resolve) => {
      releaseFirstAppend = resolve;
    });
    let heldOnce = false;
    const writeTreePreservingPrefix: RepoStore["writeTreePreservingPrefix"] =
      async (principal, repoId, ref, args) => {
        const matched = /WAL boundary (\d+)/.exec(args.message);
        if (matched !== null) seqsWritten.push(Number(matched[1]));
        if (!heldOnce && args.preservePrefix.includes("/wal/")) {
          heldOnce = true;
          await firstAppendHeld;
        }
        const result = await h.substrate.writeTreePreservingPrefix(
          principal,
          repoId,
          ref,
          args,
        );
        writesCompleted += 1;
        return result;
      };
    const wrappedSubstrate = new Proxy(h.substrate, {
      get(target, prop, receiver): unknown {
        if (prop === "writeTreePreservingPrefix") {
          return writeTreePreservingPrefix;
        }
        return Reflect.get(target, prop, receiver);
      },
    });

    const freshLocalDir = path.join(h.baseDir, "reentry-local");
    const store = await createDurableConversationStore({
      localStoreDir: freshLocalDir,
      signer: h.signer,
      substrate: wrappedSubstrate,
      workflowRunRepoId: h.workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: STEP_ID,
      lifetime: { kind: "deployment" },
    });

    const restored = await store.restoreFromSubstrate();
    expect(restored).toBe(true);
    // Wait until the reentrant mirror's append is parked, so it is
    // unambiguously the first (held) WAL append.
    await waitUntil(() => heldOnce);
    expect(heldOnce).toBe(true);

    // A real mirror carrying a genuinely new turn `c`, enqueued behind the
    // parked reentrant mirror.
    await store.storage.writeTurns([
      userTurn("a"),
      userTurn("b"),
      userTurn("c"),
    ]);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(2),
    });
    await store.storage.commit({ message: "c" });
    const cMirror = store.mirrorToSubstrate();

    // Let a would-be-concurrent `c` mirror reach and pass its own append
    // before the reentrant one is released.
    //
    // The duration is load-bearing and must stay a sleep: it IS the interleave
    // window this test measures. Under the correct implementation the `c`
    // mirror is queued behind the parked reentrant one and does nothing during
    // it, so there is no state to wait for.
    await new Promise((resolve) => setTimeout(resolve, 40));
    releaseFirstAppend();
    await cMirror;
    // The reentrant mirror is fire-and-forget, so await its append landing on
    // the substrate rather than a delay. Exactly two writes run through the
    // wrapper: `runMirror` appends one WAL entry per boundary unconditionally
    // (turnless boundaries included), and the live WAL never reaches
    // CHECKPOINT_INTERVAL here, so neither mirror adds a compaction write.
    // `heldOnce` proves the first entered and `await cMirror` proves the
    // second completed, so the count reaches two once the released reentrant
    // write returns.
    await waitUntil(() => writesCompleted >= 2);

    const reconstructed = await reconstructDurableConversation(
      h.stateDir,
      STEP_ID,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual([
      userTurn("a"),
      userTurn("b"),
      userTurn("c"),
    ]);
    // No two WAL appends targeted the same boundary seq.
    expect(new Set(seqsWritten).size).toBe(seqsWritten.length);
  });
});

describe("durable conversation store attempt-scoped local store", () => {
  let h: Harness;
  let localDir: string;
  const attemptOne: DurableConversationLifetime = {
    kind: "attempt",
    attempt: 1,
  };

  beforeEach(async () => {
    h = await makeHarness();
    localDir = path.join(h.baseDir, "attempt-1");
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  test("an invocation after a crash that beat the mirror resumes from the local store", async () => {
    const first = await makeStore(h, localDir, attemptOne);
    await first.restoreFromSubstrate();
    const committedTurns = await pushAndMirror(first, [], [userTurn("first")], {
      tokenUsageInput: 1,
    });

    // The next invocation's reactor commits its turn locally, then the host
    // dies before the settle mirror.
    const crashed = await makeStore(h, localDir, attemptOne);
    await crashed.restoreFromSubstrate();
    const localTurns = [...committedTurns, userTurn("second")];
    await crashed.storage.writeTurns(localTurns);
    await crashed.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(2),
    });
    await crashed.storage.commit({ message: "turn" });

    const resumed = await makeStore(h, localDir, attemptOne);
    expect(await resumed.restoreFromSubstrate()).toBe(true);
    const loaded = await resumed.storage.load();
    expect(loaded.turns).toEqual(localTurns);
    expect(loaded.tokenUsage).toEqual(tokenUsage(2));

    await pushAndMirror(resumed, loaded.turns, [userTurn("third")], {
      tokenUsageInput: 3,
    });
    const committed = await reconstructDurableConversation(h.stateDir, STEP_ID);
    expect(committed?.turns).toEqual([...localTurns, userTurn("third")]);
    expect(committed?.attempt).toBe(1);
  });

  test("a local store that no longer extends the committed turns is replaced by them", async () => {
    const elsewhere = await makeStore(
      h,
      path.join(h.baseDir, "other-host"),
      attemptOne,
    );
    await elsewhere.restoreFromSubstrate();
    const committedTurns = await pushAndMirror(
      elsewhere,
      [],
      [userTurn("committed")],
      { tokenUsageInput: 1 },
    );
    const leftBehind = await makeStore(h, localDir, attemptOne);
    await leftBehind.storage.writeTurns([userTurn("left behind")]);
    await leftBehind.storage.commit({ message: "turn" });

    const resumed = await makeStore(h, localDir, attemptOne);
    await resumed.restoreFromSubstrate();
    expect((await resumed.storage.load()).turns).toEqual(committedTurns);
  });
});

describe("durable conversation store legacy agent-state move", () => {
  let h: Harness;
  let localDir: string;
  const legacyPrefix = `${WORKFLOW_RUN_AGENT_STATE_PREFIX}/${STEP_ID}/`;

  beforeEach(async () => {
    h = await makeHarness();
    localDir = path.join(h.baseDir, "local");
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  /**
   * Commit a conversation where a deployment that predates the step-state
   * layout kept it: a checkpoint plus a WAL tail under `agent-state/<stepId>/`.
   */
  async function writeLegacyConversation(files: Record<string, string>) {
    await h.substrate.writeTreePreservingPrefix(
      PRINCIPAL,
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: legacyPrefix,
        merge: async () => files,
        message: "legacy conversation",
      },
    );
  }

  const legacyMetadata = {
    pendingOperations: [],
    tokenUsage: tokenUsage(7),
    connectorState: null,
  };

  function legacyFiles(): Record<string, string> {
    return {
      ...buildStepStateCheckpoint(legacyPrefix, 2, {
        turns: [userTurn("a"), userTurn("b")],
        ...legacyMetadata,
      }),
      [stepStateWalEntryPath(legacyPrefix, 2)]: serializeStepStateWalEntry(
        2,
        [userTurn("c")],
        legacyMetadata,
      ),
    };
  }

  test("a restore moves the legacy conversation into the state directory and keeps appending there", async () => {
    await writeLegacyConversation(legacyFiles());
    const legacyDir = path.join(
      h.substrate.getRepoDir(h.workflowRunRepoId),
      legacyPrefix,
    );

    const store = await makeStore(h, localDir);
    expect(await store.restoreFromSubstrate()).toBe(true);
    const restored = await store.storage.load();
    expect(restored.turns).toEqual([
      userTurn("a"),
      userTurn("b"),
      userTurn("c"),
    ]);
    expect(restored.tokenUsage).toEqual(tokenUsage(7));
    expect(fs.existsSync(legacyDir)).toBe(false);
    expect(readCheckpointMeta(h.stateDir)).toMatchObject({
      checkpointSeq: 3,
      turnCount: 3,
    });

    await pushAndMirror(store, restored.turns, [userTurn("d")], {
      tokenUsageInput: 8,
    });
    const reconstructed = await reconstructDurableConversation(
      h.stateDir,
      STEP_ID,
    );
    expect(reconstructed?.turns).toEqual([
      userTurn("a"),
      userTurn("b"),
      userTurn("c"),
      userTurn("d"),
    ]);
    expect(reconstructed?.boundaryCount).toBe(4);
  });

  test("a restore after an interrupted move keeps the state directory and drops the legacy copy", async () => {
    const mover = await makeStore(h, path.join(h.baseDir, "mover"));
    await pushAndMirror(mover, [], [userTurn("moved")], { tokenUsageInput: 1 });
    await writeLegacyConversation(legacyFiles());

    const store = await makeStore(h, localDir);
    expect(await store.restoreFromSubstrate()).toBe(true);
    expect((await store.storage.load()).turns).toEqual([userTurn("moved")]);
    expect(
      fs.existsSync(
        path.join(h.substrate.getRepoDir(h.workflowRunRepoId), legacyPrefix),
      ),
    ).toBe(false);
  });

  test("a read before the move finds the legacy conversation", async () => {
    await writeLegacyConversation(legacyFiles());
    const read = await readDurableConversation({
      substrate: h.substrate,
      workflowRunRepoId: h.workflowRunRepoId,
      runId: RUN_ID,
      stepId: STEP_ID,
    });
    expect(read?.turns).toEqual([userTurn("a"), userTurn("b"), userTurn("c")]);
  });
});
