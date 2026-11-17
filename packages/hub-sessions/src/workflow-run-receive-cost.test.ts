// The cost of one workflow-run pack receive as its commit count grows.
// The receive validates every commit against its parent, and each commit
// carries the whole history before it. These tests pin that the reads
// and parses of one receive grow linearly with its commit count, and
// that one write reads each object it needs once.

import { describe, test, expect, afterAll, beforeAll, spyOn } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import git from "isomorphic-git";
import { generateKeyPair } from "@intx/crypto";
import type { KeyPair } from "@intx/types/runtime";
import {
  workflowRunKindHandler,
  WORKFLOW_RUN_ADDRESSES_PREFIX,
  WORKFLOW_RUN_GITIGNORE_PATH,
  WORKFLOW_RUN_INBOX_DIR,
  WORKFLOW_RUN_MAILBOX_INBOX_DIR,
  WORKFLOW_RUN_MAILBOX_INDEX_FILE,
  WORKFLOW_RUN_MAILBOX_PREFIX,
  WORKFLOW_RUN_PROCESSING_DIR,
  WORKFLOW_RUN_RUNS_PREFIX,
  workflowRunStepStatePrefix,
} from "./workflow-run-kind";
import {
  serializeStepStateWalEntry,
  stepStateWalEntryPath,
} from "./step-state";
import { MAILBOX_INDEX_VERSION } from "./mailbox-index";
import { createRepoStore } from "./repo-store";
import type { Principal, RepoId } from "./repo-store";
import {
  createPackFixture,
  type PackFixture,
} from "./workflow-run-kind.test-helpers";

const REF = "refs/heads/events";
const HUB: Principal = { kind: "hub" };

// The size of a long sidecar history, and half of it. Each linear bound
// holds at both sizes, so a cost that grows faster than the commit count
// fails at the larger one.
const LONG = 1400;
const SHORT = LONG / 2;

// Each test receives one or two packs of up to LONG commits.
const RECEIVE_TIMEOUT_MS = 300_000;

const tempDirs: string[] = [];
let signingKey: KeyPair;

beforeAll(async () => {
  signingKey = await generateKeyPair();
});

afterAll(async () => {
  for (const d of tempDirs.splice(0)) {
    await fs.promises.rm(d, { recursive: true, force: true }).catch(() => {
      /* best effort */
    });
  }
});

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

function eventBody(seq: number, type: string): string {
  return JSON.stringify({ seq, type });
}

// One run that gains one event per commit, so commit k carries k events.
async function longRunFixture(commits: number): Promise<PackFixture> {
  const fixture = await createPackFixture(
    await makeTempDir("wfr-cost-long-src-"),
    REF,
  );
  const events = `${WORKFLOW_RUN_RUNS_PREFIX}/run-1/events`;
  await fixture.commit({
    puts: {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${events}/0.json`]: eventBody(0, "RunStarted"),
    },
  });
  for (let seq = 1; seq < commits; seq++) {
    await fixture.commit({
      puts: {
        [`${events}/${String(seq)}.json`]: eventBody(seq, "StepCompleted"),
      },
    });
  }
  return fixture;
}

const ADDRESS = "agent@example.com";
const ADDRESS_DIR = `${WORKFLOW_RUN_ADDRESSES_PREFIX}/${encodeURIComponent(ADDRESS)}`;
const MAILBOX_INBOX = `${WORKFLOW_RUN_MAILBOX_PREFIX}/${WORKFLOW_RUN_MAILBOX_INBOX_DIR}`;
const RUNS = 40;

// Commits spread over RUNS runs, with a claim-check inbox and processing
// set and a mailbox that both grow with the commit count. The
// claim-check and mailbox checks are not scoped to the changed runs, so
// each commit checks every entry of both.
async function manyRunsFixture(commits: number): Promise<PackFixture> {
  const fixture = await createPackFixture(
    await makeTempDir("wfr-cost-many-src-"),
    REF,
  );
  await fixture.commit({ puts: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" } });
  const nextSeq = new Map<string, number>();
  const inbox: { filename: string; body: string }[] = [];
  let uid = 0;
  for (let i = 1; i < commits; i++) {
    const runId = `run-${String(i % RUNS)}`;
    const seq = nextSeq.get(runId) ?? 0;
    nextSeq.set(runId, seq + 1);
    const puts: Record<string, string> = {
      [`${WORKFLOW_RUN_RUNS_PREFIX}/${runId}/events/${String(seq)}.json`]:
        eventBody(seq, seq === 0 ? "RunStarted" : "StepCompleted"),
    };
    const deletes: string[] = [];
    if (i % 4 === 0) {
      const messageId = `m${String(i)}`;
      const filename = `${String(i)}-${messageId}.json`;
      const body = JSON.stringify({
        messageId,
        receivedAt: i,
        address: ADDRESS,
      });
      inbox.push({ filename, body });
      puts[`${ADDRESS_DIR}/${WORKFLOW_RUN_INBOX_DIR}/${filename}`] = body;
    }
    if (i % 8 === 0) {
      // Move the oldest inbox entry to processing under its filename.
      const moved = inbox.shift();
      if (moved === undefined) throw new Error("unreachable");
      deletes.push(
        `${ADDRESS_DIR}/${WORKFLOW_RUN_INBOX_DIR}/${moved.filename}`,
      );
      puts[`${ADDRESS_DIR}/${WORKFLOW_RUN_PROCESSING_DIR}/${moved.filename}`] =
        moved.body;
    }
    if (i % 3 === 0) {
      uid += 1;
      puts[`${MAILBOX_INBOX}/${String(uid)}.eml`] =
        `Subject: message ${String(uid)}\r\n\r\nbody\r\n`;
      puts[`${MAILBOX_INBOX}/${WORKFLOW_RUN_MAILBOX_INDEX_FILE}`] =
        JSON.stringify({
          version: MAILBOX_INDEX_VERSION,
          uidValidity: 1,
          uidNext: uid + 1,
          highestModSeq: 0,
          messages: [],
          expunged: [],
        });
    }
    await fixture.commit({ puts, deletes });
  }
  return fixture;
}

async function freshStore(): Promise<{
  store: ReturnType<typeof createRepoStore>;
  repoId: RepoId;
}> {
  const store = createRepoStore({
    dataDir: await makeTempDir("wfr-cost-tgt-"),
    signingKey,
    handlers: { "workflow-run": workflowRunKindHandler },
    authorize: () => ({ allowed: true }),
  });
  const repoId: RepoId = { kind: "workflow-run", id: "cost" };
  await store.initRepo(repoId);
  return { store, repoId };
}

type ReceiveCost = {
  readCommit: number;
  readTree: number;
  // Every event and queue-entry parse reads its blob, so blob reads
  // bound the parses.
  readBlob: number;
};

// Receive the first `count` commits of `fixture` into a fresh store and
// count the git object reads the receive makes.
async function receiveCost(
  fixture: PackFixture,
  count: number,
): Promise<ReceiveCost> {
  const { store, repoId } = await freshStore();
  const pack = await fixture.pack(count);
  const tip = fixture.commits[count - 1];
  if (tip === undefined) throw new Error(`fixture has no commit ${count}`);
  const readCommit = spyOn(git, "readCommit");
  const readTree = spyOn(git, "readTree");
  const readBlob = spyOn(git, "readBlob");
  try {
    await store.receivePack(HUB, repoId, REF, pack, tip, null);
    expect(await store.resolveRef(HUB, repoId, REF)).toBe(tip);
    return {
      readCommit: readCommit.mock.calls.length,
      readTree: readTree.mock.calls.length,
      readBlob: readBlob.mock.calls.length,
    };
  } finally {
    readCommit.mockRestore();
    readTree.mockRestore();
    readBlob.mockRestore();
  }
}

describe("workflow-run pack receive cost", () => {
  test(
    "a receive that appends to one run reads and parses linearly in its commits",
    async () => {
      const fixture = await longRunFixture(LONG);
      for (const n of [SHORT, LONG]) {
        const cost = await receiveCost(fixture, n);
        // Per commit: two commit reads (the walk that finds the new
        // commits, and validation); the four trees on the new event's
        // path, which are new in each commit; and the new event's blob.
        // An event that an earlier commit carried is proven unchanged by
        // its blob OID and is parsed once per receive, not once per
        // commit.
        expect(cost.readCommit).toBeLessThanOrEqual(2 * n + 10);
        expect(cost.readTree).toBeLessThanOrEqual(4 * n + 10);
        expect(cost.readBlob).toBeLessThanOrEqual(n + 10);
      }
    },
    RECEIVE_TIMEOUT_MS,
  );

  test(
    "a receive over many runs, a claim-check queue and a mailbox reads and parses linearly in its commits",
    async () => {
      const fixture = await manyRunsFixture(LONG);
      for (const n of [SHORT, LONG]) {
        const cost = await receiveCost(fixture, n);
        // Per commit: two commit reads; the new trees on each changed
        // path, about 3.6 on average for this mix; and one blob for
        // each new event, inbox entry, processing entry and mailbox
        // index. A retained queue entry is parsed once per receive, and
        // a retained message is proven unchanged by its blob OID. The
        // check that skips a claim-check or mailbox subtree the parent
        // holds unchanged reads the parent's trees on that path, about
        // 1.7 more per commit here. The receive already parses each
        // retained entry once, so here the skip saves no blob reads;
        // the gain is on a single write, which would parse them all.
        expect(cost.readCommit).toBeLessThanOrEqual(2 * n + 10);
        expect(cost.readTree).toBeLessThanOrEqual(6 * n + 20);
        expect(cost.readBlob).toBeLessThanOrEqual(2 * n + 10);
      }
    },
    RECEIVE_TIMEOUT_MS,
  );
});

describe("workflow-run write cost", () => {
  const events = `${WORKFLOW_RUN_RUNS_PREFIX}/run-1/events`;

  async function seededStore() {
    const { store, repoId } = await freshStore();
    const seed: Record<string, string> = {
      [WORKFLOW_RUN_GITIGNORE_PATH]: "",
      [`${MAILBOX_INBOX}/${WORKFLOW_RUN_MAILBOX_INDEX_FILE}`]: JSON.stringify({
        version: MAILBOX_INDEX_VERSION,
        uidValidity: 1,
        uidNext: 4,
        highestModSeq: 0,
        messages: [],
        expunged: [],
      }),
    };
    for (let seq = 0; seq < 10; seq++) {
      seed[`${events}/${String(seq)}.json`] = eventBody(
        seq,
        seq === 0 ? "RunStarted" : "StepCompleted",
      );
    }
    for (let uid = 1; uid <= 3; uid++) {
      seed[`${MAILBOX_INBOX}/${String(uid)}.eml`] =
        `Subject: message ${String(uid)}\r\n\r\nbody\r\n`;
    }
    seed[`${ADDRESS_DIR}/${WORKFLOW_RUN_INBOX_DIR}/1-m1.json`] = JSON.stringify(
      { messageId: "m1", receivedAt: 1, address: ADDRESS },
    );
    seed[stepStateWalEntryPath(workflowRunStepStatePrefix("run-1", "s"), 0)] =
      serializeStepStateWalEntry(0, [], {
        pendingOperations: [],
        tokenUsage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          thinking: 0,
        },
        connectorState: null,
      });
    await store.writeTree(HUB, repoId, REF, {
      files: seed,
      message: "seed",
    });
    return { store, repoId };
  }

  async function expectEachObjectReadOnce(
    write: () => Promise<unknown>,
  ): Promise<void> {
    const readCommit = spyOn(git, "readCommit");
    const readTree = spyOn(git, "readTree");
    try {
      await write();
      const repeated = (calls: { oid: string }[][]): string[] => {
        const seen = new Set<string>();
        const again: string[] = [];
        for (const [args] of calls) {
          if (args === undefined) continue;
          if (seen.has(args.oid)) again.push(args.oid);
          seen.add(args.oid);
        }
        return again;
      };
      expect(readTree.mock.calls.length).toBeGreaterThan(0);
      expect(repeated(readCommit.mock.calls)).toEqual([]);
      expect(repeated(readTree.mock.calls)).toEqual([]);
    } finally {
      readCommit.mockRestore();
      readTree.mockRestore();
    }
  }

  test("a delta append reads each commit and tree at most once", async () => {
    const { store, repoId } = await seededStore();
    await expectEachObjectReadOnce(() =>
      store.writeTreeDelta(HUB, repoId, REF, {
        computeDelta: async () => ({
          puts: { [`${events}/10.json`]: eventBody(10, "StepCompleted") },
          deletes: [],
        }),
        changedPathPrefixes: new Set([`${WORKFLOW_RUN_RUNS_PREFIX}/run-1/`]),
        message: "append",
      }),
    );
  });

  test("a tree write reads each commit and tree at most once", async () => {
    const { store, repoId } = await seededStore();
    await expectEachObjectReadOnce(() =>
      store.writeTree(HUB, repoId, REF, {
        files: { [`${events}/10.json`]: eventBody(10, "StepCompleted") },
        message: "append",
      }),
    );
  });

  test("a prefix-preserving write reads each commit and tree at most once", async () => {
    const { store, repoId } = await seededStore();
    await expectEachObjectReadOnce(() =>
      store.writeTreePreservingPrefix(HUB, repoId, REF, {
        preservePrefix: `${events}/`,
        merge: async (existing) => ({
          ...Object.fromEntries(existing),
          [`${events}/10.json`]: eventBody(10, "StepCompleted"),
        }),
        message: "append",
      }),
    );
  });

  test("an append's reads do not grow with the claim-check queue or the mailbox", async () => {
    async function appendReads(queued: number): Promise<number> {
      const { store, repoId } = await freshStore();
      const seed: Record<string, string> = {
        [WORKFLOW_RUN_GITIGNORE_PATH]: "",
        [`${events}/0.json`]: eventBody(0, "RunStarted"),
        [`${MAILBOX_INBOX}/${WORKFLOW_RUN_MAILBOX_INDEX_FILE}`]: JSON.stringify(
          {
            version: MAILBOX_INDEX_VERSION,
            uidValidity: 1,
            uidNext: queued + 1,
            highestModSeq: 0,
            messages: [],
            expunged: [],
          },
        ),
      };
      for (let i = 1; i <= queued; i++) {
        seed[`${MAILBOX_INBOX}/${String(i)}.eml`] =
          `Subject: message ${String(i)}\r\n\r\nbody\r\n`;
        seed[
          `${ADDRESS_DIR}/${WORKFLOW_RUN_INBOX_DIR}/${String(i)}-m${String(i)}.json`
        ] = JSON.stringify({
          messageId: `m${String(i)}`,
          receivedAt: i,
          address: ADDRESS,
        });
      }
      await store.writeTree(HUB, repoId, REF, { files: seed, message: "seed" });

      const readTree = spyOn(git, "readTree");
      const readBlob = spyOn(git, "readBlob");
      try {
        await store.writeTreeDelta(HUB, repoId, REF, {
          computeDelta: async () => ({
            puts: { [`${events}/1.json`]: eventBody(1, "StepCompleted") },
            deletes: [],
          }),
          changedPathPrefixes: new Set([`${WORKFLOW_RUN_RUNS_PREFIX}/run-1/`]),
          message: "append",
        });
        return readTree.mock.calls.length + readBlob.mock.calls.length;
      } finally {
        readTree.mockRestore();
        readBlob.mockRestore();
      }
    }
    expect(await appendReads(100)).toBe(await appendReads(5));
  });
});
