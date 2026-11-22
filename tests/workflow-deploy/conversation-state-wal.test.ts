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
// commits, the preserve-prefix merges, and the committed-tree reconstruction
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
  ConversationTurn,
  PendingOperation,
  TokenUsage,
} from "@intx/types/runtime";
import type {
  RepoId,
  RepoStore,
  WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  createRepoStore,
  serializeStepStateWalEntry,
  stepStateWalEntryPath,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
  workflowRunStepSeedPath,
  workflowRunStepStatePrefix,
} from "@intx/hub-sessions";
import { userTurn } from "@intx/inference-testing";
import { createInboundMessage } from "@intx/mime";
import {
  createDurableConversationStore,
  readStepState,
  type DurableConversationLifetime,
  type DurableConversationStore,
} from "@intx/workflow-host";

const WORKFLOW_RUN_REF = "refs/heads/main";
const RUN_ID = "run_durability";
const STEP_ID = "step-1";
const STATE_PREFIX = workflowRunStepStatePrefix(RUN_ID, STEP_ID);
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
  signingKey: KeyPair;
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
    STATE_PREFIX,
  );
  return {
    baseDir,
    signingKey,
    substrate,
    workflowRunRepoId,
    signer,
    stateDir,
  };
}

/** The step's committed state, read the way a restore reads it. */
function readCommitted(h: Harness) {
  return readStepState({
    substrate: h.substrate,
    workflowRunRepoId: h.workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    runId: RUN_ID,
    stepId: STEP_ID,
  });
}

/**
 * A second substrate over the harness repo that skips push validation, to
 * commit damage the validated substrate refuses, as a corrupted object store
 * would present it.
 */
function unvalidatedSubstrate(h: Harness): RepoStore {
  return createRepoStore({
    dataDir: h.baseDir,
    signingKey: h.signingKey,
    handlers: {
      "workflow-run": {
        ...workflowRunKindHandler,
        validatePush: async () => ({ ok: true }),
      },
    },
    authorize: () => ({ allowed: true }),
  });
}

/** The lifetime of attempt `attempt` of step `STEP_ID` in run `runId`. */
function invocation(
  attempt: number,
  runId = RUN_ID,
): DurableConversationLifetime {
  return {
    kind: "invocation",
    invocation: { runId, stepId: STEP_ID, attempt },
  };
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
    mailboxAddress: "agent@example.com",
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
    // The run-boundary mirrorToSubstrate runs after the send settles even
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
      turnId: "turn-1",
      gated: true,
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
    const reconstructed = await readCommitted(h);
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual(turns);
    expect(reconstructed.tokenUsage).toEqual(tokenUsage(99));
    expect(reconstructed.pendingOperations).toEqual([pendingOp]);
  });

  test("a mirror after a failed local checkpoint commits nothing until a later checkpoint completes", async () => {
    const store = await makeStore(h, localDir);
    const first = await pushAndMirror(store, [], [userTurn("one")], {
      tokenUsageInput: 1,
    });
    const pendingOp: PendingOperation = {
      correlationId: "corr-1",
      kind: "approval",
      registeredAt: 0,
      gateId: "gate-1",
      turnId: "turn-1",
      gated: true,
    };
    const metadata = {
      pendingOperations: [pendingOp],
      tokenUsage: tokenUsage(2),
    };
    const second = [...first, userTurn("two")];

    // A directory where the manifest goes fails its write, so the checkpoint
    // stops before the metadata that carries the pending approval, as the
    // reactor's does.
    const manifestPath = path.join(localDir, "manifest.jsonl");
    await store.storage.writeTurns(second);
    await fs.promises.mkdir(manifestPath);
    await expect(store.storage.writeManifest([])).rejects.toThrow();
    await fs.promises.rmdir(manifestPath);

    await expect(store.mirrorToSubstrate()).rejects.toThrow(
      /failed its last checkpoint/,
    );
    const unchanged = await readCommitted(h);
    expect(unchanged?.turns).toEqual(first);
    expect(unchanged?.pendingOperations).toEqual([]);

    // A connector-state save commits the stale metadata again but does not
    // complete the failed checkpoint.
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(1),
    });
    await store.storage.commit({ message: "connector state" });
    await expect(store.mirrorToSubstrate()).rejects.toThrow(
      /failed its last checkpoint/,
    );

    await store.storage.writeTurns(second);
    await store.storage.writeManifest([]);
    await store.storage.writeMetadata(metadata);
    await store.storage.commit({ message: "turn" });
    await store.mirrorToSubstrate();
    const mirrored = await readCommitted(h);
    expect(mirrored?.turns).toEqual(second);
    expect(mirrored?.pendingOperations).toEqual([pendingOp]);
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
        mailboxAddress: "agent@example.com",
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
      expect((await readCommitted(h))?.turns).toEqual(turns);
    });

    test("fails the turn whose fold fails once the WAL holds twice the interval, appending nothing", async () => {
      const store = await storeFailingCheckpoints(Number.POSITIVE_INFINITY);
      const limit = 2 * CHECKPOINT_INTERVAL;
      let turns: ConversationTurn[] = [];
      for (let i = 0; i < limit; i += 1) {
        turns = await pushAndMirror(store, turns, [userTurn(`t${String(i)}`)], {
          tokenUsageInput: i,
        });
      }
      expect(countWalEntries(h.stateDir)).toBe(limit);

      await store.storage.writeTurns([...turns, userTurn("last")]);
      await store.storage.commit({ message: "turn" });
      await expect(store.mirrorToSubstrate()).rejects.toThrow(
        `failed with ${String(limit)} WAL entries uncompacted`,
      );
      expect(countWalEntries(h.stateDir)).toBe(limit);
      expect((await readCommitted(h))?.turns).toEqual(turns);
    });

    test("commits the next turn as a fold once the WAL holds twice the interval", async () => {
      const limit = 2 * CHECKPOINT_INTERVAL;
      // Every compaction from the interval up to the cap fails.
      const store = await storeFailingCheckpoints(
        limit - CHECKPOINT_INTERVAL + 1,
      );
      let turns: ConversationTurn[] = [];
      for (let i = 0; i < limit; i += 1) {
        turns = await pushAndMirror(store, turns, [userTurn(`t${String(i)}`)], {
          tokenUsageInput: i,
        });
      }
      expect(countWalEntries(h.stateDir)).toBe(limit);

      turns = await pushAndMirror(store, turns, [userTurn("last")], {
        tokenUsageInput: limit,
      });
      expect(countWalEntries(h.stateDir)).toBe(0);
      expect(readCheckpointMeta(h.stateDir)).toMatchObject({
        checkpointSeq: limit + 1,
        turnCount: limit + 1,
      });
      expect((await readCommitted(h))?.turns).toEqual(turns);
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
    const reconstructed = await readCommitted(h);
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

  test("a restore with neither committed state nor a seed drops a conversation only the local store holds", async () => {
    // The agent's first turn reached its local store, and the host died
    // before the run-boundary mirror committed it.
    const crashed = await makeStore(h, localDir);
    await crashed.storage.writeTurns([userTurn("never committed")]);
    await crashed.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(5),
    });
    await crashed.storage.commit({ message: "turn" });

    const respawned = await makeStore(h, localDir);
    expect(await respawned.restoreFromSubstrate()).toBe(false);
    const loaded = await respawned.storage.load();
    expect(loaded.turns).toEqual([]);
    expect(loaded.tokenUsage).toEqual(tokenUsage(0));

    await pushAndMirror(respawned, [], [userTurn("first committed")], {
      tokenUsageInput: 1,
    });
    expect((await readCommitted(h))?.turns).toEqual([
      userTurn("first committed"),
    ]);
  });

  test("a corrupt WAL entry throws on reconstruction (no silent fresh start)", async () => {
    const store = await makeStore(h, localDir);
    await pushAndMirror(store, [], [userTurn("only")], { tokenUsageInput: 1 });

    await unvalidatedSubstrate(h).writeTree(
      PRINCIPAL,
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: { [stepStateWalEntryPath(STATE_PREFIX, 0)]: "{ not json" },
        message: "corrupt WAL boundary 0",
      },
    );
    await expect(readCommitted(h)).rejects.toThrow(/not valid JSON/);
    await expect(
      makeStore(h, path.join(h.baseDir, "respawned")).then((respawned) =>
        respawned.restoreFromSubstrate(),
      ),
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
    // Drop the middle WAL entry from the committed tree to simulate a lost
    // append.
    const lost = stepStateWalEntryPath(STATE_PREFIX, 1);
    await unvalidatedSubstrate(h).writeTreePreservingPrefix(
      PRINCIPAL,
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: lost.slice(0, lost.lastIndexOf("/") + 1),
        merge: async (existing) =>
          Object.fromEntries(
            [...existing].filter(([blobPath]) => blobPath !== lost),
          ),
        message: "lose WAL boundary 1",
      },
    );
    await expect(readCommitted(h)).rejects.toThrow(/seq gap/);
  });

  test("a restore reads the committed state, not a working tree a crashed write left behind", async () => {
    const store = await makeStore(h, localDir);
    const first = await pushAndMirror(store, [], [userTurn("one")], {
      tokenUsageInput: 1,
    });
    const committed = await pushAndMirror(store, first, [userTurn("two")], {
      tokenUsageInput: 2,
    });

    // A write removes its prefix from the working tree and rewrites it before
    // it commits: a crash in between leaves the bucket missing entries, and a
    // commit that fails leaves an entry no commit holds.
    const bucket = walBucketDir(h.stateDir, 0);
    fs.rmSync(bucket, { recursive: true });
    fs.mkdirSync(bucket);
    fs.writeFileSync(
      path.join(bucket, "2.json"),
      serializeStepStateWalEntry(2, [userTurn("never committed")], {
        pendingOperations: [],
        tokenUsage: tokenUsage(9),
        connectorState: null,
      }),
    );

    const respawned = await makeStore(h, path.join(h.baseDir, "respawned"));
    expect(await respawned.restoreFromSubstrate()).toBe(true);
    const loaded = await respawned.storage.load();
    expect(loaded.turns).toEqual(committed);
    expect(loaded.tokenUsage).toEqual(tokenUsage(2));

    await pushAndMirror(respawned, committed, [userTurn("three")], {
      tokenUsageInput: 3,
    });
    const after = await readCommitted(h);
    expect(after?.turns).toEqual([...committed, userTurn("three")]);
    expect(after?.boundaryCount).toBe(3);
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
      mailboxAddress: "agent@example.com",
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
    const reconstructed = await readCommitted(h);
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual([userTurn("a"), userTurn("b")]);
  });

  test("overlapping mirror runs serialize onto distinct boundaries and lose no turns", async () => {
    // The race this guards against: two mirrors of one store share the
    // mirrored-count state with no serialization. Two overlapping runs both
    // read the same boundary seq AND the same mirroredTurnCount, so they
    // collide on one boundary and double-advance the turn count -- and a
    // following mirror then slices past a genuinely new turn and drops it
    // from the log.
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
      mailboxAddress: "agent@example.com",
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

    const reconstructed = await readCommitted(h);
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual(abc);
  });
});

describe("durable conversation store attempt-scoped local store", () => {
  let h: Harness;
  let localDir: string;
  const attemptOne = invocation(1);

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
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([...localTurns, userTurn("third")]);
    expect(committed?.invocation?.attempt).toBe(1);
  });

  test("a restore whose read of the state ref fails throws instead of starting over from the local store", async () => {
    const first = await makeStore(h, localDir, attemptOne);
    await first.restoreFromSubstrate();
    const once = await pushAndMirror(first, [], [userTurn("one")], {
      tokenUsageInput: 1,
    });
    const committedTurns = await pushAndMirror(first, once, [userTurn("two")], {
      tokenUsageInput: 2,
    });

    // An emptied ref file reads as a missing ref to isomorphic-git. Taken as
    // "no state", the local store would count as ahead and the next commit
    // would rewrite WAL seq 0 over the committed history.
    const refFile = path.join(
      h.substrate.getRepoDir(h.workflowRunRepoId),
      ".git",
      ...WORKFLOW_RUN_REF.split("/"),
    );
    const sha = await fs.promises.readFile(refFile, "utf8");
    await fs.promises.writeFile(refFile, "");
    const blind = await makeStore(h, localDir, attemptOne);
    await expect(blind.restoreFromSubstrate()).rejects.toThrow(
      /exists but could not be read/,
    );

    await fs.promises.writeFile(refFile, sha);
    expect((await readCommitted(h))?.turns).toEqual(committedTurns);
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

  test("a seeded attempt whose first turn crashed before its mirror resumes from its local store, not the seed", async () => {
    const seedTurns = [userTurn("imported question")];
    await h.substrate.writeTree(
      { kind: "hub" },
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: {
          [workflowRunStepSeedPath(RUN_ID, STEP_ID)]: JSON.stringify({
            version: 1,
            turns: seedTurns,
            tokenUsage: tokenUsage(11),
            connectorState: null,
          }),
        },
        message: "write the step's seed",
      },
    );
    const parked: PendingOperation = {
      correlationId: "corr-seeded",
      kind: "approval",
      registeredAt: 0,
      gateId: "gate-corr-seeded",
      turnId: "turn-corr-seeded",
      gated: true,
    };
    const crashed = await makeStore(h, localDir, attemptOne);
    await crashed.restoreFromSubstrate();
    const localTurns = [...seedTurns, userTurn("asked")];
    await crashed.storage.writeTurns(localTurns);
    await crashed.storage.writeMetadata({
      pendingOperations: [parked],
      tokenUsage: tokenUsage(12),
    });
    await crashed.storage.commit({ message: "suspend" });
    expect(await readCommitted(h)).toBeNull();

    const resumed = await makeStore(h, localDir, attemptOne);
    await resumed.restoreFromSubstrate();
    const loaded = await resumed.storage.load();
    expect(loaded.turns).toEqual(localTurns);
    expect(loaded.pendingOperations).toEqual([parked]);
  });

  test("a seeded attempt resumed from its local store folds its first mirror into a checkpoint", async () => {
    const seedTurns = [userTurn("imported question")];
    await h.substrate.writeTree(
      { kind: "hub" },
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: {
          [workflowRunStepSeedPath(RUN_ID, STEP_ID)]: JSON.stringify({
            version: 1,
            turns: seedTurns,
            tokenUsage: tokenUsage(11),
            connectorState: null,
          }),
        },
        message: "write the step's seed",
      },
    );
    const crashed = await makeStore(h, localDir, attemptOne);
    await crashed.restoreFromSubstrate();
    const localTurns = [...seedTurns, userTurn("asked")];
    await crashed.storage.writeTurns(localTurns);
    await crashed.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(12),
    });
    await crashed.storage.commit({ message: "turn" });
    expect(await readCommitted(h)).toBeNull();

    const resumed = await makeStore(h, localDir, attemptOne);
    await resumed.restoreFromSubstrate();
    const all = await pushAndMirror(resumed, localTurns, [userTurn("next")], {
      tokenUsageInput: 13,
    });

    expect((await readCommitted(h))?.turns).toEqual(all);
    // The local copy carries the seed's turns, so they land in a checkpoint,
    // not in a WAL entry that every later append to its bucket would carry
    // along.
    expect(readCheckpointMeta(h.stateDir).checkpointSeq).toBe(1);
    expect(
      fs.existsSync(path.join(walBucketDir(h.stateDir, 0), "0.json")),
    ).toBe(false);
  });
});

describe("durable conversation store with a seed", () => {
  let h: Harness;
  const seed = {
    version: 1 as const,
    turns: [
      userTurn("imported question"),
      {
        role: "assistant" as const,
        content: [{ type: "text" as const, text: "imported answer" }],
        model: "stub-model",
        timestamp: 1,
      },
    ],
    tokenUsage: tokenUsage(11),
    connectorState: null,
  };
  const seedPath = workflowRunStepSeedPath(RUN_ID, STEP_ID);

  beforeEach(async () => {
    h = await makeHarness();
    await h.substrate.writeTree(
      { kind: "hub" },
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: { [seedPath]: JSON.stringify(seed) },
        message: "write the step's seed",
      },
    );
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  test("a step with no state of its own starts from its seed and commits it with its first turn", async () => {
    const store = await makeStore(h, path.join(h.baseDir, "local"));
    expect(await store.restoreFromSubstrate()).toBe(true);
    const loaded = await store.storage.load();
    expect(loaded.turns).toEqual(seed.turns);
    expect(loaded.tokenUsage).toEqual(seed.tokenUsage);
    expect(loaded.connectorState).toEqual(seed.connectorState);
    expect(loaded.pendingOperations).toEqual([]);

    await pushAndMirror(store, loaded.turns, [userTurn("next")], {
      tokenUsageInput: 12,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([...seed.turns, userTurn("next")]);
    expect(committed?.connectorState).toEqual(seed.connectorState);

    // The seeded turns land in a checkpoint with the first turn, not in a WAL
    // entry that every later append to its bucket would carry along.
    expect(readCheckpointMeta(h.stateDir)).toMatchObject({
      checkpointSeq: 1,
      turnCount: seed.turns.length + 1,
    });
    expect(
      fs.existsSync(path.join(walBucketDir(h.stateDir, 0), "0.json")),
    ).toBe(false);
  });

  test("a seeded first fold that fails is retried by the next mirror", async () => {
    let refused = false;
    const substrate = new Proxy(h.substrate, {
      get(target, prop, receiver): unknown {
        if (prop === "writeTreePreservingPrefix") {
          const write: RepoStore["writeTreePreservingPrefix"] = (
            principal,
            repoId,
            ref,
            args,
          ) => {
            if (!refused && !args.preservePrefix.includes("/wal/")) {
              refused = true;
              return Promise.reject(new Error("fold refused"));
            }
            return target.writeTreePreservingPrefix(
              principal,
              repoId,
              ref,
              args,
            );
          };
          return write;
        }
        return Reflect.get(target, prop, receiver);
      },
    });
    const store = await createDurableConversationStore({
      localStoreDir: path.join(h.baseDir, "local"),
      signer: h.signer,
      substrate,
      workflowRunRepoId: h.workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: STEP_ID,
      lifetime: invocation(1),
      mailboxAddress: "agent@example.com",
    });
    await store.restoreFromSubstrate();

    const one = [...seed.turns, userTurn("one")];
    await store.storage.writeTurns(one);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(12),
    });
    await store.storage.commit({ message: "turn" });
    await expect(store.mirrorToSubstrate()).rejects.toThrow(/fold refused/);
    expect(await readCommitted(h)).toBeNull();

    const two = await pushAndMirror(store, one, [userTurn("two")], {
      tokenUsageInput: 13,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual(two);
    expect(committed?.invocation?.attempt).toBe(1);
    // The retry folds again, so the seeded turns still land in a checkpoint
    // rather than in one WAL entry.
    expect(readCheckpointMeta(h.stateDir)).toMatchObject({
      checkpointSeq: 1,
      turnCount: two.length,
    });
    expect(
      fs.existsSync(path.join(walBucketDir(h.stateDir, 0), "0.json")),
    ).toBe(false);
  });

  test("a step's own state wins over its seed", async () => {
    const first = await makeStore(h, path.join(h.baseDir, "first"));
    await first.restoreFromSubstrate();
    await pushAndMirror(first, seed.turns, [userTurn("own turn")], {
      tokenUsageInput: 12,
    });

    const respawned = await makeStore(h, path.join(h.baseDir, "respawned"));
    expect(await respawned.restoreFromSubstrate()).toBe(true);
    const loaded = await respawned.storage.load();
    expect(loaded.turns).toEqual([...seed.turns, userTurn("own turn")]);
    expect(loaded.tokenUsage).toEqual(tokenUsage(12));
  });

  test("a retried attempt continues the earlier attempt's committed turns, not the seed", async () => {
    const attemptOne = await makeStore(
      h,
      path.join(h.baseDir, "attempt-1"),
      invocation(1),
    );
    await attemptOne.restoreFromSubstrate();
    await pushAndMirror(attemptOne, seed.turns, [userTurn("attempt one")], {
      tokenUsageInput: 12,
    });

    const attemptTwo = await makeStore(
      h,
      path.join(h.baseDir, "attempt-2"),
      invocation(2),
    );
    expect(await attemptTwo.restoreFromSubstrate()).toBe(true);
    const loaded = await attemptTwo.storage.load();
    expect(loaded.turns).toEqual([...seed.turns, userTurn("attempt one")]);

    await pushAndMirror(attemptTwo, loaded.turns, [userTurn("attempt two")], {
      tokenUsageInput: 13,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([
      ...seed.turns,
      userTurn("attempt one"),
      userTurn("attempt two"),
    ]);
    expect(committed?.invocation?.attempt).toBe(2);
  });

  test("another run's invocation of the same key continues the committed turns, not the seed", async () => {
    const first = await makeStore(
      h,
      path.join(h.baseDir, "first-run"),
      invocation(1, "run-body-1"),
    );
    await first.restoreFromSubstrate();
    await pushAndMirror(first, seed.turns, [userTurn("first run")], {
      tokenUsageInput: 12,
    });

    const second = await makeStore(
      h,
      path.join(h.baseDir, "second-run"),
      invocation(1, "run-body-2"),
    );
    expect(await second.restoreFromSubstrate()).toBe(true);
    const loaded = await second.storage.load();
    expect(loaded.turns).toEqual([...seed.turns, userTurn("first run")]);

    await pushAndMirror(second, loaded.turns, [userTurn("second run")], {
      tokenUsageInput: 13,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([
      ...seed.turns,
      userTurn("first run"),
      userTurn("second run"),
    ]);
    // A plain continue appends to the committed history instead of folding.
    expect(readCheckpointMeta(h.stateDir).checkpointSeq).toBe(1);
    expect(
      fs.existsSync(path.join(walBucketDir(h.stateDir, 0), "1.json")),
    ).toBe(true);
    expect(committed?.invocation).toEqual({
      runId: "run-body-2",
      stepId: STEP_ID,
      attempt: 1,
    });
  });

  test("a restore reads the committed seed, not the working tree's copy", async () => {
    await fs.promises.writeFile(
      path.join(h.substrate.getRepoDir(h.workflowRunRepoId), seedPath),
      "not json",
    );
    const store = await makeStore(h, path.join(h.baseDir, "local"));
    expect(await store.restoreFromSubstrate()).toBe(true);
    expect((await store.storage.load()).turns).toEqual(seed.turns);
  });

  test("a damaged seed fails the restore instead of starting the step without it", async () => {
    await unvalidatedSubstrate(h).writeTree(
      { kind: "hub" },
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      { files: { [seedPath]: "not json" }, message: "damage the seed" },
    );
    const store = await makeStore(h, path.join(h.baseDir, "local"));
    await expect(store.restoreFromSubstrate()).rejects.toThrow(
      /is not valid JSON/,
    );
  });
});

describe("durable conversation store across invocations of one key", () => {
  let h: Harness;

  beforeEach(async () => {
    h = await makeHarness();
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  /** A pending approval gate, which carries the call the approval re-runs. */
  function gate(correlationId: string): PendingOperation {
    return {
      correlationId,
      kind: "approval",
      registeredAt: 0,
      gateId: `gate-${correlationId}`,
      turnId: `turn-${correlationId}`,
      gated: true,
      suspendedCall: {
        id: `call-${correlationId}`,
        name: "charge",
        arguments: {},
      },
    };
  }

  function toolCall(correlationId: string): ConversationTurn {
    return {
      role: "assistant",
      content: [
        {
          type: "tool_call",
          id: `call-${correlationId}`,
          name: "charge",
          arguments: {},
        },
      ],
      model: "stub-model",
      timestamp: 0,
    };
  }

  function toolResult(correlationId: string): ConversationTurn {
    return {
      role: "user",
      content: [
        {
          type: "tool_result",
          callId: `call-${correlationId}`,
          content: [{ type: "text", text: "charged" }],
        },
      ],
      timestamp: 0,
    };
  }

  function mail(messageId: string) {
    return createInboundMessage({
      from: "alice@example.com",
      to: "agent@example.com",
      content: "please charge the card",
      messageId,
      interchangeType: "conversation.message",
    });
  }

  /**
   * One turn of an invocation the way the step invoker drives it: restore,
   * begin the turn, seed its mail, record its turns and pending operations
   * locally, park on `parkOn`, and mirror. A resume passes the gate it
   * resumes and no mail.
   */
  async function runTurn(
    lifetime: DurableConversationLifetime,
    turn: {
      localDir: string;
      resume?: string;
      mail?: string;
      turns: (prior: ConversationTurn[]) => ConversationTurn[];
      pendingOperations: PendingOperation[];
      parkOn?: string;
      outcome?: "mirrored" | "crashed";
    },
  ): Promise<DurableConversationStore> {
    const store = await makeStore(h, turn.localDir, lifetime);
    await store.restoreFromSubstrate();
    await store.beginTurn(
      turn.resume !== undefined ? { resume: turn.resume } : {},
    );
    if (turn.mail !== undefined) await store.seedInbound(mail(turn.mail));
    const prior = (await store.storage.load()).turns;
    await store.storage.writeTurns(turn.turns(prior));
    await store.storage.writeMetadata({
      pendingOperations: turn.pendingOperations,
      tokenUsage: tokenUsage(prior.length + 1),
    });
    await store.storage.commit({ message: "turn" });
    if (turn.parkOn !== undefined) await store.parkTurn(turn.parkOn);
    if (turn.outcome !== "crashed") await store.mirrorToSubstrate();
    return store;
  }

  function local(name: string): string {
    return path.join(h.baseDir, name);
  }

  test("a park records where its turn started, and a resume that parks again keeps it", async () => {
    const own = invocation(1, "run-body-1");
    await runTurn(own, {
      localDir: local("a"),
      turns: () => [userTurn("earlier")],
      pendingOperations: [],
    });
    await runTurn(own, {
      localDir: local("a"),
      mail: "<m1@example.com>",
      turns: (prior) => [...prior, userTurn("charge it"), toolCall("c1")],
      pendingOperations: [gate("c1")],
      parkOn: "c1",
    });
    expect((await readCommitted(h))?.parkedTurnStart).toBe(1);

    // The resume answers the first gate and parks on a second one.
    await runTurn(own, {
      localDir: local("a"),
      resume: "c1",
      turns: (prior) => [...prior, toolResult("c1"), toolCall("c2")],
      pendingOperations: [gate("c2")],
      parkOn: "c2",
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toHaveLength(5);
    expect(committed?.parkedTurnStart).toBe(1);

    // A turn that settles without a gate records none.
    await runTurn(own, {
      localDir: local("a"),
      resume: "c2",
      turns: (prior) => [...prior, toolResult("c2")],
      pendingOperations: [],
    });
    expect((await readCommitted(h))?.parkedTurnStart).toBeUndefined();
  });

  test("another invocation rolls back a turn left parked to the start of that turn", async () => {
    const first = invocation(1, "run-body-1");
    await runTurn(first, {
      localDir: local("first"),
      turns: () => [userTurn("earlier")],
      pendingOperations: [],
    });
    await runTurn(first, {
      localDir: local("first"),
      mail: "<m1@example.com>",
      turns: (prior) => [...prior, userTurn("charge it"), toolCall("c1")],
      pendingOperations: [gate("c1")],
      parkOn: "c1",
    });
    await runTurn(first, {
      localDir: local("first"),
      resume: "c1",
      turns: (prior) => [...prior, toolResult("c1"), toolCall("c2")],
      pendingOperations: [gate("c2")],
      parkOn: "c2",
    });
    const parked = await readCommitted(h);
    if (parked === null) throw new Error("expected committed step state");
    expect(Object.keys(parked.parkedReplyTargets)).toEqual(["c2"]);
    expect(parked.connectorState).not.toBeNull();

    const next = await makeStore(h, local("next"), invocation(1, "run-body-2"));
    expect(await next.restoreFromSubstrate()).toBe(true);
    const loaded = await next.storage.load();
    expect(loaded.turns).toEqual([userTurn("earlier")]);
    expect(loaded.pendingOperations).toEqual([]);
    expect(loaded.connectorState).toEqual(parked.connectorState);
    expect(loaded.tokenUsage).toEqual(parked.tokenUsage);

    await pushAndMirror(next, loaded.turns, [userTurn("next event")], {
      tokenUsageInput: 9,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([
      userTurn("earlier"),
      userTurn("next event"),
    ]);
    expect(committed?.pendingOperations).toEqual([]);
    expect(committed?.parkedReplyTargets).toEqual({});
    expect(committed?.parkedTurnStart).toBeUndefined();
    expect(committed?.invocation?.runId).toBe("run-body-2");
    // The rollback is not a prefix of the committed history, so it folds.
    expect(countWalEntries(h.stateDir)).toBe(0);
  });

  test("an invocation keeps its own park, and resumes it", async () => {
    const own = invocation(1, "run-body-1");
    await runTurn(own, {
      localDir: local("a"),
      turns: () => [userTurn("charge it"), toolCall("c1")],
      pendingOperations: [gate("c1")],
    });

    const replacement = await makeStore(h, local("b"), own);
    await replacement.restoreFromSubstrate();
    const loaded = await replacement.storage.load();
    expect(loaded.turns).toEqual([userTurn("charge it"), toolCall("c1")]);
    expect(loaded.pendingOperations).toEqual([gate("c1")]);
  });

  test("a park recorded locally over a rolled-back state survives a crash before its mirror", async () => {
    await runTurn(invocation(1, "run-body-1"), {
      localDir: local("first"),
      turns: () => [userTurn("earlier"), userTurn("first"), toolCall("c1")],
      pendingOperations: [gate("c1")],
    });
    // The earlier turn was committed before the park in the same checkpoint,
    // so the park records the start of a turn that began with nothing.
    expect((await readCommitted(h))?.parkedTurnStart).toBe(0);

    const second = invocation(1, "run-body-2");
    await runTurn(second, {
      localDir: local("second"),
      turns: (prior) => [...prior, userTurn("second"), toolCall("c2")],
      pendingOperations: [gate("c2")],
      outcome: "crashed",
    });

    const resumed = await makeStore(h, local("second"), second);
    await resumed.restoreFromSubstrate();
    const loaded = await resumed.storage.load();
    expect(loaded.turns).toEqual([userTurn("second"), toolCall("c2")]);
    expect(loaded.pendingOperations).toEqual([gate("c2")]);
    await resumed.mirrorToSubstrate();
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([userTurn("second"), toolCall("c2")]);
    expect(committed?.pendingOperations).toEqual([gate("c2")]);
    expect(committed?.parkedTurnStart).toBe(0);
  });

  test("a park's turn start survives the first mirror's fold over a seed", async () => {
    const seedTurns = [userTurn("imported question"), userTurn("imported")];
    await h.substrate.writeTree(
      { kind: "hub" },
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: {
          [workflowRunStepSeedPath(RUN_ID, STEP_ID)]: JSON.stringify({
            version: 1,
            turns: seedTurns,
            tokenUsage: tokenUsage(1),
            connectorState: null,
          }),
        },
        message: "write the step's seed",
      },
    );
    await runTurn(invocation(1, "run-body-1"), {
      localDir: local("first"),
      turns: (prior) => [...prior, userTurn("charge it"), toolCall("c1")],
      pendingOperations: [gate("c1")],
    });
    expect(readCheckpointMeta(h.stateDir).checkpointSeq).toBe(1);
    expect((await readCommitted(h))?.parkedTurnStart).toBe(2);

    const next = await makeStore(h, local("next"), invocation(1, "run-body-2"));
    await next.restoreFromSubstrate();
    expect((await next.storage.load()).turns).toEqual(seedTurns);
  });

  test("a park's turn start survives compaction at the checkpoint interval", async () => {
    const own = invocation(1, "run-body-1");
    const store = await makeStore(h, local("a"), own);
    await store.restoreFromSubstrate();
    let turns: ConversationTurn[] = [];
    for (let i = 0; i < CHECKPOINT_INTERVAL - 1; i++) {
      await store.beginTurn({});
      turns = await pushAndMirror(store, turns, [userTurn(`t${String(i)}`)], {
        tokenUsageInput: i,
      });
    }
    await store.beginTurn({});
    turns = [...turns, userTurn("charge it"), toolCall("c1")];
    await store.storage.writeTurns(turns);
    await store.storage.writeMetadata({
      pendingOperations: [gate("c1")],
      tokenUsage: tokenUsage(CHECKPOINT_INTERVAL),
    });
    await store.storage.commit({ message: "park" });
    await store.mirrorToSubstrate();

    // The park's boundary reached the interval, so the WAL folded.
    expect(countWalEntries(h.stateDir)).toBe(0);
    const raw: unknown = JSON.parse(
      fs.readFileSync(path.join(h.stateDir, "checkpoint.meta.json"), "utf8"),
    );
    expect(raw).toMatchObject({ parkedTurnStart: CHECKPOINT_INTERVAL - 1 });

    const next = await makeStore(h, local("next"), invocation(1, "run-body-2"));
    await next.restoreFromSubstrate();
    expect((await next.storage.load()).turns).toEqual(
      turns.slice(0, CHECKPOINT_INTERVAL - 1),
    );
  });

  test("a restore refuses a parked turn start past the committed turns", async () => {
    await h.substrate.writeTreePreservingPrefix(
      PRINCIPAL,
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: STATE_PREFIX,
        merge: async () => ({
          [stepStateWalEntryPath(STATE_PREFIX, 0)]: serializeStepStateWalEntry(
            0,
            [userTurn("only")],
            {
              pendingOperations: [gate("c1")],
              tokenUsage: tokenUsage(1),
              connectorState: null,
              invocation: { runId: "run-body-1", stepId: STEP_ID, attempt: 1 },
              parkedTurnStart: 2,
            },
          ),
        }),
        message: "corrupt park start",
      },
    );

    for (const runId of ["run-body-1", "run-body-2"]) {
      const store = await makeStore(h, local(runId), invocation(1, runId));
      await expect(store.restoreFromSubstrate()).rejects.toThrow(
        /parked turn starting at turn 2, past its 1 turns/,
      );
    }
  });

  test("the deployment lifetime never records a turn start nor rolls back", async () => {
    const warm = await makeStore(h, local("warm"));
    await warm.restoreFromSubstrate();
    await warm.beginTurn({});
    await warm.storage.writeTurns([userTurn("charge it"), toolCall("c1")]);
    await warm.storage.writeMetadata({
      pendingOperations: [gate("c1")],
      tokenUsage: tokenUsage(1),
    });
    await warm.storage.commit({ message: "park" });
    await warm.mirrorToSubstrate();

    const committed = await readCommitted(h);
    expect(committed?.parkedTurnStart).toBeUndefined();
    expect(committed?.invocation).toBeUndefined();

    const respawned = await makeStore(h, local("respawned"));
    await respawned.restoreFromSubstrate();
    const loaded = await respawned.storage.load();
    expect(loaded.turns).toEqual([userTurn("charge it"), toolCall("c1")]);
    expect(loaded.pendingOperations).toEqual([gate("c1")]);
  });
});

describe("durable conversation store over a gate park with no recorded turn start", () => {
  let h: Harness;
  const dangling: ConversationTurn = {
    role: "assistant",
    content: [
      { type: "tool_call", id: "call-c1", name: "charge", arguments: {} },
    ],
    model: "stub-model",
    timestamp: 0,
  };

  beforeEach(async () => {
    h = await makeHarness();
    // A park on a gate the way a store wrote it before `parkedTurnStart`
    // existed: the pending gate and its tool call, and no turn start.
    await h.substrate.writeTreePreservingPrefix(
      PRINCIPAL,
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        preservePrefix: STATE_PREFIX,
        merge: async () => ({
          [stepStateWalEntryPath(STATE_PREFIX, 0)]: serializeStepStateWalEntry(
            0,
            [userTurn("charge it"), dangling],
            {
              pendingOperations: [
                {
                  correlationId: "c1",
                  kind: "approval",
                  registeredAt: 0,
                  gateId: "gate-c1",
                  suspendedCall: {
                    id: "call-c1",
                    name: "charge",
                    arguments: {},
                  },
                },
              ],
              tokenUsage: tokenUsage(1),
              connectorState: null,
              invocation: { runId: "run-body-1", stepId: STEP_ID, attempt: 1 },
            },
          ),
        }),
        message: "park a gate with no recorded turn start",
      },
    );
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  test("another invocation starts over with no conversation and folds its first mirror", async () => {
    const next = await makeStore(
      h,
      path.join(h.baseDir, "next"),
      invocation(1, "run-body-2"),
    );
    expect(await next.restoreFromSubstrate()).toBe(false);
    const loaded = await next.storage.load();
    expect(loaded.turns).toEqual([]);
    expect(loaded.pendingOperations).toEqual([]);

    await pushAndMirror(next, loaded.turns, [userTurn("next event")], {
      tokenUsageInput: 9,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([userTurn("next event")]);
    expect(committed?.pendingOperations).toEqual([]);
    expect(committed?.invocation?.runId).toBe("run-body-2");
    expect(countWalEntries(h.stateDir)).toBe(0);
    expect(readCheckpointMeta(h.stateDir)).toMatchObject({
      checkpointSeq: 2,
      turnCount: 1,
    });
  });

  test("another invocation starts over from the seed when the step has one", async () => {
    const seed = {
      version: 1 as const,
      turns: [userTurn("imported question")],
      tokenUsage: tokenUsage(11),
      connectorState: null,
    };
    await h.substrate.writeTree(
      { kind: "hub" },
      h.workflowRunRepoId,
      WORKFLOW_RUN_REF,
      {
        files: {
          [workflowRunStepSeedPath(RUN_ID, STEP_ID)]: JSON.stringify(seed),
        },
        message: "write the step's seed",
      },
    );
    const next = await makeStore(
      h,
      path.join(h.baseDir, "next"),
      invocation(1, "run-body-2"),
    );
    expect(await next.restoreFromSubstrate()).toBe(true);
    const loaded = await next.storage.load();
    expect(loaded.turns).toEqual(seed.turns);
    expect(loaded.pendingOperations).toEqual([]);

    await pushAndMirror(next, loaded.turns, [userTurn("next event")], {
      tokenUsageInput: 12,
    });
    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual([...seed.turns, userTurn("next event")]);
    expect(countWalEntries(h.stateDir)).toBe(0);
  });

  test("the invocation that parked continues its own park as it is", async () => {
    const own = await makeStore(
      h,
      path.join(h.baseDir, "own"),
      invocation(1, "run-body-1"),
    );
    expect(await own.restoreFromSubstrate()).toBe(true);
    const loaded = await own.storage.load();
    expect(loaded.turns).toEqual([userTurn("charge it"), dangling]);
    expect(loaded.pendingOperations.map((op) => op.correlationId)).toEqual([
      "c1",
    ]);
  });

  test("a resume of the park that parks again records no turn start, so another invocation starts over", async () => {
    const own = await makeStore(
      h,
      path.join(h.baseDir, "own"),
      invocation(1, "run-body-1"),
    );
    await own.restoreFromSubstrate();
    await own.beginTurn({ resume: "c1" });
    const prior = (await own.storage.load()).turns;
    const result: ConversationTurn = {
      role: "user",
      content: [
        {
          type: "tool_result",
          callId: "call-c1",
          content: [{ type: "text", text: "ok" }],
        },
      ],
      timestamp: 0,
    };
    const second: ConversationTurn = {
      role: "assistant",
      content: [
        { type: "tool_call", id: "call-c2", name: "charge", arguments: {} },
      ],
      model: "stub-model",
      timestamp: 0,
    };
    await own.storage.writeTurns([...prior, result, second]);
    await own.storage.writeMetadata({
      pendingOperations: [
        {
          correlationId: "c2",
          kind: "approval",
          registeredAt: 0,
          gateId: "gate-c2",
          suspendedCall: { id: "call-c2", name: "charge", arguments: {} },
        },
      ],
      tokenUsage: tokenUsage(2),
    });
    await own.storage.commit({ message: "resume parks again" });
    await own.mirrorToSubstrate();
    expect((await readCommitted(h))?.parkedTurnStart).toBeUndefined();

    const next = await makeStore(
      h,
      path.join(h.baseDir, "next"),
      invocation(1, "run-body-2"),
    );
    expect(await next.restoreFromSubstrate()).toBe(false);
    expect((await next.storage.load()).turns).toEqual([]);
  });
});

// State an earlier version wrote carries no stamp: a single-step agent's
// state, and a multi-step step's state that carries only a bare `attempt`.
for (const [label, legacyStamp] of [
  ["no stamp", {}],
  ["a bare attempt", { attempt: 1 }],
] as const) {
  describe(`durable conversation store over a gate park with ${label}`, () => {
    let h: Harness;
    const dangling: ConversationTurn = {
      role: "assistant",
      content: [
        { type: "tool_call", id: "call-c1", name: "charge", arguments: {} },
      ],
      model: "stub-model",
      timestamp: 0,
    };

    beforeEach(async () => {
      h = await makeHarness();
      await h.substrate.writeTreePreservingPrefix(
        PRINCIPAL,
        h.workflowRunRepoId,
        WORKFLOW_RUN_REF,
        {
          preservePrefix: STATE_PREFIX,
          merge: async () => ({
            [stepStateWalEntryPath(STATE_PREFIX, 0)]: JSON.stringify({
              seq: 0,
              turns: [userTurn("charge it"), dangling],
              metadata: {
                pendingOperations: [
                  {
                    correlationId: "c1",
                    kind: "approval",
                    registeredAt: 0,
                    gateId: "gate-c1",
                    suspendedCall: {
                      id: "call-c1",
                      name: "charge",
                      arguments: {},
                    },
                  },
                ],
                tokenUsage: tokenUsage(1),
                connectorState: null,
                ...legacyStamp,
              },
            }),
          }),
          message: `park a gate with ${label}`,
        },
      );
    });

    afterEach(async () => {
      await fs.promises.rm(h.baseDir, { recursive: true, force: true });
    });

    function resumeOf(correlationId: string): DurableConversationLifetime {
      return {
        kind: "invocation",
        invocation: { runId: RUN_ID, stepId: STEP_ID, attempt: 1 },
        resumes: correlationId,
      };
    }

    test("an approval resume of the gate continues the park and commits the turn stamped", async () => {
      const own = await makeStore(
        h,
        path.join(h.baseDir, "own"),
        resumeOf("c1"),
      );
      expect(await own.restoreFromSubstrate()).toBe(true);
      const loaded = await own.storage.load();
      expect(loaded.turns).toEqual([userTurn("charge it"), dangling]);
      expect(
        loaded.pendingOperations.some(
          (op) => op.correlationId === "c1" && op.suspendedCall !== undefined,
        ),
      ).toBe(true);

      await own.beginTurn({ resume: "c1" });
      const result: ConversationTurn = {
        role: "user",
        content: [
          {
            type: "tool_result",
            callId: "call-c1",
            content: [{ type: "text", text: "ok" }],
          },
        ],
        timestamp: 0,
      };
      const done: ConversationTurn = {
        role: "assistant",
        content: [{ type: "text", text: "charged" }],
        model: "stub-model",
        timestamp: 0,
      };
      await pushAndMirror(own, loaded.turns, [result, done], {
        tokenUsageInput: 2,
      });

      const committed = await readCommitted(h);
      expect(committed?.turns).toEqual([
        userTurn("charge it"),
        dangling,
        result,
        done,
      ]);
      expect(committed?.pendingOperations).toEqual([]);
      expect(committed?.invocation).toEqual({
        runId: RUN_ID,
        stepId: STEP_ID,
        attempt: 1,
      });
      // The resume continues the committed history, so its mirror appends.
      expect(countWalEntries(h.stateDir)).toBe(2);
    });

    test("an invocation that resumes another gate starts over", async () => {
      const other = await makeStore(
        h,
        path.join(h.baseDir, "other"),
        resumeOf("c2"),
      );
      expect(await other.restoreFromSubstrate()).toBe(false);
      const loaded = await other.storage.load();
      expect(loaded.turns).toEqual([]);
      expect(loaded.pendingOperations).toEqual([]);
    });

    test("an invocation that resumes no gate starts over", async () => {
      const next = await makeStore(
        h,
        path.join(h.baseDir, "next"),
        invocation(1),
      );
      expect(await next.restoreFromSubstrate()).toBe(false);
      expect((await next.storage.load()).turns).toEqual([]);
    });
  });
}

describe("an invocation's reply by mail", () => {
  let h: Harness;
  // Set to make the local store's commits fail, as a full or broken local
  // disk would. The substrate signs with its own key, so it still commits.
  let localCommitsFail: boolean;
  const SENT = "<sent-1@agent.example.com>";

  beforeEach(async () => {
    h = await makeHarness();
    localCommitsFail = false;
  });

  afterEach(async () => {
    await fs.promises.rm(h.baseDir, { recursive: true, force: true });
  });

  function storeFor(
    name: string,
    lifetime: DurableConversationLifetime,
  ): Promise<DurableConversationStore> {
    return createDurableConversationStore({
      localStoreDir: path.join(h.baseDir, name),
      signer: async (payload) => {
        if (localCommitsFail) throw new Error("local disk is full");
        return h.signer(payload);
      },
      substrate: h.substrate,
      workflowRunRepoId: h.workflowRunRepoId,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      runId: RUN_ID,
      stepId: STEP_ID,
      lifetime,
      mailboxAddress: "agent@example.com",
    });
  }

  const reply: ConversationTurn = {
    role: "assistant",
    content: [{ type: "text", text: "charged" }],
    model: "stub-model",
    timestamp: 0,
  };

  /**
   * Run a mail turn up to its sent reply: seed the mail, commit the turn
   * locally the way the reactor does, and compose the reply.
   */
  async function replyTurn(store: DurableConversationStore): Promise<{
    turns: ConversationTurn[];
    references: readonly string[];
  }> {
    await store.restoreFromSubstrate();
    await store.beginTurn({});
    await store.seedInbound(
      createInboundMessage({
        from: "alice@example.com",
        to: "agent@example.com",
        content: "please charge the card",
        messageId: "<a@example.com>",
        interchangeType: "conversation.message",
      }),
    );
    const turns = [userTurn("please charge the card"), reply];
    await store.storage.writeTurns(turns);
    await store.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(1),
    });
    await store.storage.commit({ message: "reply" });
    const parts = store.composeReply();
    if (parts === null) throw new Error("expected a reply address");
    return { turns, references: parts.references ?? [] };
  }

  test("a reply whose local thread record fails to save commits its turn and the thread advance", async () => {
    const store = await storeFor("own", invocation(1, "run-body-1"));
    const { turns, references } = await replyTurn(store);

    localCommitsFail = true;
    await store.onReplySent(
      { messageId: SENT, status: "delivered" },
      references,
    );
    await store.mirrorToSubstrate();
    localCommitsFail = false;

    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual(turns);
    expect(committed?.connectorState?.lastMessageId).toBe(SENT);

    const next = await storeFor("next", invocation(1, "run-body-2"));
    expect(await next.restoreFromSubstrate()).toBe(true);
    const loaded = await next.storage.load();
    expect(loaded.turns).toEqual(turns);
    expect(loaded.connectorState?.lastMessageId).toBe(SENT);
  });

  test("a same-host restore of the invocation after its local thread record failed commits the thread from before the reply", async () => {
    const store = await storeFor("own", invocation(1, "run-body-1"));
    const { turns, references } = await replyTurn(store);
    await store.onReplySent(
      { messageId: SENT, status: "delivered" },
      references,
    );
    // The local metadata write fails on the file system, so the local store
    // keeps the thread from before the reply.
    fs.chmodSync(path.join(h.baseDir, "own"), 0o555);
    try {
      await store.mirrorToSubstrate();
    } finally {
      fs.chmodSync(path.join(h.baseDir, "own"), 0o755);
    }
    expect((await readCommitted(h))?.connectorState?.lastMessageId).toBe(SENT);

    // An input resume of the same invocation on the same host keeps the
    // local store, and its next turn commits the older thread.
    const again = await storeFor("own", invocation(1, "run-body-1"));
    await again.restoreFromSubstrate();
    expect((await again.storage.load()).connectorState?.lastMessageId).toBe(
      "<a@example.com>",
    );
    await again.beginTurn({});
    await again.storage.writeTurns([...turns, userTurn("next input")]);
    await again.storage.writeMetadata({
      pendingOperations: [],
      tokenUsage: tokenUsage(2),
    });
    await again.storage.commit({ message: "next" });
    await again.mirrorToSubstrate();
    expect((await readCommitted(h))?.connectorState?.lastMessageId).toBe(
      "<a@example.com>",
    );
  });

  test("a sent reply's thread advance reaches the local store once its turn commits", async () => {
    const store = await storeFor("own", invocation(1, "run-body-1"));
    const { references } = await replyTurn(store);

    await store.onReplySent(
      { messageId: SENT, status: "delivered" },
      references,
    );
    expect((await store.storage.load()).connectorState?.lastMessageId).toBe(
      "<a@example.com>",
    );
    await store.mirrorToSubstrate();

    expect((await store.storage.load()).connectorState?.lastMessageId).toBe(
      SENT,
    );
  });

  test("a sent reply the thread refuses commits the turn without the thread advance", async () => {
    const store = await storeFor("own", invocation(1, "run-body-1"));
    const { turns, references } = await replyTurn(store);

    // A turn that answers no mail has no thread to advance.
    await store.beginTurn({});
    await expect(
      store.onReplySent({ messageId: SENT, status: "delivered" }, references),
    ).rejects.toThrow();
    await store.mirrorToSubstrate();

    const committed = await readCommitted(h);
    expect(committed?.turns).toEqual(turns);
    expect(committed?.connectorState?.lastMessageId).toBe("<a@example.com>");
  });

  test("a failed reactor checkpoint still stops the mirror of a turn whose reply was sent", async () => {
    const store = await storeFor("own", invocation(1, "run-body-1"));
    const { references } = await replyTurn(store);
    // The reactor's next local commit fails, so the local files can lag the
    // agent's state.
    localCommitsFail = true;
    await store.storage.writeTurns([userTurn("more")]);
    await expect(store.storage.commit({ message: "turn" })).rejects.toThrow(
      /local disk is full/,
    );
    localCommitsFail = false;

    await store.onReplySent(
      { messageId: SENT, status: "delivered" },
      references,
    );
    await expect(store.mirrorToSubstrate()).rejects.toThrow(
      /failed its last checkpoint/,
    );
    expect(await readCommitted(h)).toBeNull();
  });
});
