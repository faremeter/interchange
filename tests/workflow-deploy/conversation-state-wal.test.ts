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
  workflowRunKindHandler,
  WORKFLOW_RUN_AGENT_STATE_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";

import {
  createDurableConversationStore,
  reconstructDurableConversation,
  type DurableConversationStore,
} from "@intx/sidecar-app/src/conversation-state";

const WORKFLOW_RUN_REF = "refs/heads/main";
const AGENT_KEY = "step-1";
// Must mirror the production constant in conversation-state.ts. Asserted
// indirectly by the bounded-WAL test below: a drift here would surface as
// a checkpoint that folds at the wrong boundary.
const CHECKPOINT_INTERVAL = 64;

const PRINCIPAL: WorkflowRunWorkflowProcessPrincipal = {
  kind: "workflow-process",
  deploymentId: "durability-unit",
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
function readCheckpointMeta(agentStateDir: string): {
  checkpointSeq: number;
  turnCount: number;
} {
  const raw: unknown = JSON.parse(
    fs.readFileSync(path.join(agentStateDir, "checkpoint.meta.json"), "utf8"),
  );
  const meta = CheckpointMetaShape(raw);
  if (meta instanceof type.errors) {
    throw new Error(`checkpoint.meta.json failed validation: ${meta.summary}`);
  }
  return meta;
}

function userTurn(text: string): ConversationTurn {
  return { role: "user", content: [{ type: "text", text }], timestamp: 0 };
}

interface Harness {
  baseDir: string;
  substrate: RepoStore;
  workflowRunRepoId: RepoId;
  signer: (payload: string) => Promise<string>;
  agentStateDir: string;
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
  const agentStateDir = path.join(
    substrate.getRepoDir(workflowRunRepoId),
    WORKFLOW_RUN_AGENT_STATE_PREFIX,
    encodeURIComponent(AGENT_KEY),
  );
  return { baseDir, substrate, workflowRunRepoId, signer, agentStateDir };
}

async function makeStore(
  h: Harness,
  localDir: string,
): Promise<DurableConversationStore> {
  return createDurableConversationStore({
    localStoreDir: localDir,
    signer: h.signer,
    substrate: h.substrate,
    workflowRunRepoId: h.workflowRunRepoId,
    workflowRunRef: WORKFLOW_RUN_REF,
    principal: PRINCIPAL,
    agentKey: AGENT_KEY,
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

function walBucketDir(agentStateDir: string, bucket: number): string {
  return path.join(agentStateDir, "wal", String(bucket));
}

/** Count every WAL entry blob across every bucket. */
function countWalEntries(agentStateDir: string): number {
  const walRoot = path.join(agentStateDir, "wal");
  if (!fs.existsSync(walRoot)) return 0;
  let count = 0;
  for (const bucket of fs.readdirSync(walRoot)) {
    count += fs
      .readdirSync(path.join(walRoot, bucket))
      .filter((f) => f.endsWith(".json")).length;
  }
  return count;
}

function readWalEntry(agentStateDir: string, seq: number): unknown {
  const bucket = Math.floor(seq / 128);
  const raw = fs.readFileSync(
    path.join(walBucketDir(agentStateDir, bucket), `${String(seq)}.json`),
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
      const entry = readWalEntry(h.agentStateDir, i);
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
      path.join(walBucketDir(h.agentStateDir, 0), "0.json"),
    ).size;
    const last = fs.statSync(
      path.join(walBucketDir(h.agentStateDir, 0), "7.json"),
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
    expect(countWalEntries(h.agentStateDir)).toBe(2);
    const turnlessEntry = WalEntryShape(readWalEntry(h.agentStateDir, 1));
    if (turnlessEntry instanceof type.errors) {
      throw new Error(`turnless entry invalid: ${turnlessEntry.summary}`);
    }
    expect(turnlessEntry.turns.length).toBe(0);

    // A fresh store (a respawn) reconstructs the LATEST metadata, not the
    // stale boundary-1 values. This is the assertion that would have caught
    // the dropped-metadata defect.
    const reconstructed = await reconstructDurableConversation(
      h.agentStateDir,
      AGENT_KEY,
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
      const liveWal = countWalEntries(h.agentStateDir);
      // The live WAL never exceeds K: it grows turn by turn until it hits
      // K, then compaction folds it to empty.
      expect(liveWal).toBeLessThanOrEqual(CHECKPOINT_INTERVAL);

      const checkpointMetaPath = path.join(
        h.agentStateDir,
        "checkpoint.meta.json",
      );
      if (totalCommitted < CHECKPOINT_INTERVAL) {
        // No checkpoint has folded yet; everything is in the WAL.
        expect(fs.existsSync(checkpointMetaPath)).toBe(false);
        expect(liveWal).toBe(totalCommitted);
      } else if (totalCommitted === CHECKPOINT_INTERVAL) {
        // The K-th turn triggers compaction: the WAL truncates to empty and
        // the checkpoint folds exactly K turns.
        expect(liveWal).toBe(0);
        const meta = readCheckpointMeta(h.agentStateDir);
        expect(meta.checkpointSeq).toBe(CHECKPOINT_INTERVAL);
        expect(meta.turnCount).toBe(CHECKPOINT_INTERVAL);
      } else {
        // After the fold, the WAL holds only the post-checkpoint tail.
        expect(liveWal).toBe(totalCommitted - CHECKPOINT_INTERVAL);
      }
    }
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
    expect(fs.existsSync(path.join(h.agentStateDir, "checkpoint.json"))).toBe(
      true,
    );
    expect(countWalEntries(h.agentStateDir)).toBe(n - CHECKPOINT_INTERVAL);

    // Reconstruct via the production read path and assert byte-equivalent
    // turns + the latest metadata.
    const reconstructed = await reconstructDurableConversation(
      h.agentStateDir,
      AGENT_KEY,
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
      path.join(walBucketDir(h.agentStateDir, 0), "0.json"),
      "{ not json",
    );
    await expect(
      reconstructDurableConversation(h.agentStateDir, AGENT_KEY),
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
    fs.rmSync(path.join(walBucketDir(h.agentStateDir, 0), "1.json"));
    await expect(
      reconstructDurableConversation(h.agentStateDir, AGENT_KEY),
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
      agentKey: AGENT_KEY,
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

    const entry = WalEntryShape(readWalEntry(h.agentStateDir, 1));
    if (entry instanceof type.errors) {
      throw new Error(`WAL entry 1 failed validation: ${entry.summary}`);
    }
    expect(entry.turns.length).toBe(1);

    // Reconstruction yields both turns; under the bug it would yield only [a].
    const reconstructed = await reconstructDurableConversation(
      h.agentStateDir,
      AGENT_KEY,
    );
    if (reconstructed === null) throw new Error("expected a reconstruction");
    expect(reconstructed.turns).toEqual([userTurn("a"), userTurn("b")]);
  });
});
