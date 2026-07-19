// `loadParkedApproval` durable reads: the sidecar binding recovers a parked
// correlation's approval snapshot for the child's re-registration enumeration.
//
// Two layouts, verified against the production read helpers:
//   - COLD (multi-step): the snapshot lives in the per-attempt isogit step
//     store on disk; the read loads it, and returns undefined (without
//     manufacturing a repo) when the store dir is absent.
//   - WARM (single-step): the snapshot lives in the durable conversation
//     store, mirrored to the workflow-run substrate under `agent-state/<stepId>/`.
//     The read reconstructs it from the substrate WITHOUT going through the live
//     registry -- proving a respawned child (whose live store is unbuilt) still
//     recovers the snapshot.

import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { ApprovalSnapshot, PendingOperation } from "@intx/types/runtime";
import type {
  Principal,
  RepoId,
  RepoStore,
} from "@intx/hub-sessions/substrate";
import { createIsogitStore } from "@intx/storage-isogit";

import {
  readColdParkedApprovalSnapshot,
  readWarmParkedApprovalSnapshot,
  stepStorageRoot,
} from "./workflow-substrate-factory";
import { createDurableConversationStore } from "./conversation-state";

const WORKFLOW_RUN_REPO_ID: RepoId = {
  kind: "workflow-run",
  id: "parked-approval",
};
const WORKFLOW_RUN_REF = "refs/heads/main";
const PRINCIPAL: Principal = { kind: "workflow-process" };
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

const testSigner = (payload: string): Promise<string> =>
  Promise.resolve(`sig:${payload.length}`);

describe("readColdParkedApprovalSnapshot", () => {
  test("loads the snapshot from a parked step's on-disk store", async () => {
    const dataDir = await makeTempDir();
    const coordinate = {
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      runId: "run-1",
      stepId: "s",
      attempt: 1,
    };
    const store = await createIsogitStore(
      stepStorageRoot(coordinate),
      testSigner,
    );
    await store.writeMetadata({
      pendingOperations: [pendingApproval("corr-1", snapshot)],
      tokenUsage: EMPTY_USAGE,
    });

    const got = await readColdParkedApprovalSnapshot({
      ...coordinate,
      correlationId: "corr-1",
    });
    expect(got).toEqual(snapshot);
  });

  test("returns undefined without creating a repo when the store is absent", async () => {
    const dataDir = await makeTempDir();
    const coordinate = {
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      runId: "missing",
      stepId: "s",
      attempt: 1,
    };

    const got = await readColdParkedApprovalSnapshot({
      ...coordinate,
      correlationId: "corr-x",
    });
    expect(got).toBeUndefined();
    // The read is a read: a missing store is never manufactured on disk.
    await expect(fs.stat(stepStorageRoot(coordinate))).rejects.toThrow();
  });

  test("returns undefined for a correlation with no matching pending op", async () => {
    const dataDir = await makeTempDir();
    const coordinate = {
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      runId: "run-2",
      stepId: "s",
      attempt: 1,
    };
    const store = await createIsogitStore(
      stepStorageRoot(coordinate),
      testSigner,
    );
    await store.writeMetadata({
      pendingOperations: [pendingApproval("corr-a", snapshot)],
      tokenUsage: EMPTY_USAGE,
    });

    const got = await readColdParkedApprovalSnapshot({
      ...coordinate,
      correlationId: "corr-other",
    });
    expect(got).toBeUndefined();
  });

  test("returns undefined for a matching op that carries no snapshot", async () => {
    const dataDir = await makeTempDir();
    const coordinate = {
      dataDir,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      runId: "run-3",
      stepId: "s",
      attempt: 1,
    };
    const store = await createIsogitStore(
      stepStorageRoot(coordinate),
      testSigner,
    );
    await store.writeMetadata({
      pendingOperations: [pendingApproval("corr-1")],
      tokenUsage: EMPTY_USAGE,
    });

    const got = await readColdParkedApprovalSnapshot({
      ...coordinate,
      correlationId: "corr-1",
    });
    expect(got).toBeUndefined();
  });
});

/**
 * Read every file under `<repoDir>/<prefix>` into a path->bytes map, keyed by
 * the repo-relative path, so a `writeTreePreservingPrefix` merge callback sees
 * the prior subtree the way the real substrate presents it.
 */
async function readPrefixEntries(
  repoDir: string,
  prefix: string,
): Promise<Map<string, Uint8Array>> {
  const entries = new Map<string, Uint8Array>();
  const prefixDir = path.join(repoDir, prefix);
  let names: string[];
  try {
    names = await fs.readdir(prefixDir, { recursive: true });
  } catch {
    return entries;
  }
  for (const name of names) {
    const full = path.join(prefixDir, name);
    if (!(await fs.stat(full)).isFile()) continue;
    entries.set(`${prefix}${name}`, await fs.readFile(full));
  }
  return entries;
}

/**
 * A substrate stub that persists `writeTreePreservingPrefix` to disk under
 * `getRepoDir`, so a durable-conversation mirror round-trips through the real
 * checkpoint/WAL layout the read reconstructs from. Any other method surfaces
 * as a precise failure.
 */
function createStubSubstrate(baseDir: string): RepoStore {
  const repoDirFor = (repoId: RepoId): string =>
    path.join(baseDir, repoId.kind, repoId.id);
  const stub: Partial<RepoStore> = {
    getRepoDir: repoDirFor,
    async writeTreePreservingPrefix(_principal, repoId, _ref, args) {
      const repoDir = repoDirFor(repoId);
      const existing = await readPrefixEntries(repoDir, args.preservePrefix);
      const merged = await args.merge(existing);
      await fs.rm(path.join(repoDir, args.preservePrefix), {
        recursive: true,
        force: true,
      });
      for (const [relPath, content] of Object.entries(merged)) {
        const full = path.join(repoDir, relPath);
        await fs.mkdir(path.dirname(full), { recursive: true });
        await fs.writeFile(full, content);
      }
      return { commitSha: "deadbeefcafef00d", newlyTerminalRuns: [] };
    },
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- test stub; missing methods surface as a precise failure via the proxy
  return new Proxy(stub as RepoStore, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (value !== undefined) return value;
      return () => {
        throw new Error(
          `stub substrate: ${String(prop)} not implemented for this test`,
        );
      };
    },
  });
}

describe("readWarmParkedApprovalSnapshot", () => {
  test("reconstructs the snapshot from the durable substrate mirror", async () => {
    const substrate = createStubSubstrate(await makeTempDir());
    const store = await createDurableConversationStore({
      localStoreDir: await makeTempDir(),
      signer: testSigner,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      agentKey: "s",
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
      stepId: "s",
      correlationId: "corr-1",
    });
    expect(got).toEqual(snapshot);
  });

  test("returns undefined when no durable state exists for the agent", async () => {
    const substrate = createStubSubstrate(await makeTempDir());

    const got = await readWarmParkedApprovalSnapshot({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      stepId: "never-ran",
      correlationId: "corr-x",
    });
    expect(got).toBeUndefined();
  });

  test("returns undefined for a matching op that carries no snapshot", async () => {
    const substrate = createStubSubstrate(await makeTempDir());
    const store = await createDurableConversationStore({
      localStoreDir: await makeTempDir(),
      signer: testSigner,
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      workflowRunRef: WORKFLOW_RUN_REF,
      principal: PRINCIPAL,
      agentKey: "s",
    });
    await store.storage.writeMetadata({
      pendingOperations: [pendingApproval("corr-1")],
      tokenUsage: EMPTY_USAGE,
    });
    await store.mirrorToSubstrate();

    const got = await readWarmParkedApprovalSnapshot({
      substrate,
      workflowRunRepoId: WORKFLOW_RUN_REPO_ID,
      stepId: "s",
      correlationId: "corr-1",
    });
    expect(got).toBeUndefined();
  });
});
