import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import { createAgentRepoStore } from "./agent-repo";
import { enqueueInbox, WORKFLOW_RUN_GITIGNORE_PATH } from "./workflow-run-kind";
import type { Principal, RepoId } from "./repo-store";
import type { SidecarAllocationRouter } from "./ws/sidecar-handler";
import { restoreWorkflowRunToAllocation } from "./workflow-run-restore";

test("sends the Hub's main and events refs sequentially to the exact allocation", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "hub-wfr-restore-"));
  try {
    const signingKey = await generateKeyPair();
    const agentRepoStore = createAgentRepoStore({
      dataDir: root,
      signingKey,
    });
    const agentAddress = "run_hub_restore@workflow.test";
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(agentAddress),
    };
    const principal: Principal = { kind: "hub" };
    await agentRepoStore.repoStore.writeTree(
      principal,
      repoId,
      "refs/heads/main",
      {
        files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
        message: "Initialize workflow run",
      },
    );
    await enqueueInbox(agentRepoStore.repoStore, principal, repoId, {
      address: agentAddress,
      messageId: "message-1",
      receivedAt: 1,
      mailAuditRef: { store: "mail", path: "message-1" },
    });

    const sends: {
      target: { allocationId: string; generation: number };
      agentAddress: string;
      ref: string;
      commitSha: string;
      pack: Uint8Array;
    }[] = [];
    // This focused test exercises the one allocation-router method the
    // restore helper consumes; the full router contract is tested at its
    // websocket boundary in sidecar-handler.test.ts.
    const allocationRouter: Pick<
      SidecarAllocationRouter,
      "sendWorkflowRunPackToAllocation"
    > = {
      async sendWorkflowRunPackToAllocation(
        target,
        destination,
        pack,
        ref,
        commitSha,
      ) {
        sends.push({
          target,
          agentAddress: destination,
          ref,
          commitSha,
          pack,
        });
      },
    };
    const allocationTarget = { allocationId: "alloc-restore", generation: 4 };

    await restoreWorkflowRunToAllocation({
      agentRepoStore,
      allocationRouter,
      allocationTarget,
      agentAddress,
    });

    expect(sends.map((send) => send.ref)).toEqual([
      "refs/heads/main",
      "refs/heads/events",
    ]);
    for (const send of sends) {
      expect(send.target).toEqual(allocationTarget);
      expect(send.agentAddress).toBe(agentAddress);
      expect(send.pack.byteLength).toBeGreaterThan(0);
      expect(send.commitSha).toHaveLength(40);
    }
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("does not send anything for a prepared deployment with no prior run refs", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "hub-wfr-empty-"));
  try {
    const agentRepoStore = createAgentRepoStore({
      dataDir: root,
      signingKey: await generateKeyPair(),
    });
    let sends = 0;
    const allocationRouter: Pick<
      SidecarAllocationRouter,
      "sendWorkflowRunPackToAllocation"
    > = {
      async sendWorkflowRunPackToAllocation() {
        sends += 1;
      },
    };

    await restoreWorkflowRunToAllocation({
      agentRepoStore,
      allocationRouter,
      allocationTarget: { allocationId: "alloc-new", generation: 1 },
      agentAddress: "run_new@workflow.test",
    });

    expect(sends).toBe(0);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
