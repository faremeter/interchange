// The cost of one workflow-run pack receive as its commit count grows.
// The receive validates every commit against its parent, and each commit
// carries the whole history before it. These tests pin that the reads
// and parses of one receive grow linearly with its commit count.

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
} from "./workflow-run-kind";
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
        mailAuditRef: { store: "audit", path: `mail/${messageId}` },
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
        JSON.stringify({ uidValidity: 1, uidNext: uid + 1 });
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
        // path, about four on average for this mix; and one blob for
        // each new event, inbox entry and processing entry. A retained
        // queue entry is parsed once per receive, and a retained message
        // is proven unchanged by its blob OID.
        expect(cost.readCommit).toBeLessThanOrEqual(2 * n + 10);
        expect(cost.readTree).toBeLessThanOrEqual(4 * n + 20);
        expect(cost.readBlob).toBeLessThanOrEqual(2 * n + 10);
      }
    },
    RECEIVE_TIMEOUT_MS,
  );
});
