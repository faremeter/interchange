import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import {
  createAgentRepoStore,
  enqueueInbox,
  WORKFLOW_RUN_GITIGNORE_PATH,
  type Principal,
  type RepoId,
  type WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import { createWorkflowRunPackClient } from "./workflow-run-pack-client";
import { createWorkflowRunPackRestorer } from "./workflow-run-pack-restore";

test("restored refs survive replacement and the next sidecar commit fast-forwards the Hub", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "wfr-restore-"));
  try {
    const sourceKey = await generateKeyPair();
    const targetKey = await generateKeyPair();
    const source = createAgentRepoStore({
      dataDir: path.join(root, "hub"),
      signingKey: sourceKey,
    });
    const target = createAgentRepoStore({
      dataDir: path.join(root, "replacement"),
      signingKey: targetKey,
    });
    const agentAddress = "ins_dep_restore@workflow.test";
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(agentAddress),
    };
    const hubPrincipal: Principal = { kind: "hub" };
    const supervisorPrincipal: WorkflowRunSupervisorPrincipal = {
      kind: "supervisor",
      anchorRunId: repoId.id,
    };

    await source.repoStore.writeTree(hubPrincipal, repoId, "refs/heads/main", {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "Initialize workflow run",
    });
    await source.repoStore.writeTree(hubPrincipal, repoId, "refs/heads/main", {
      files: {
        "runs/run-before-replacement/grants.json": JSON.stringify({
          grants: [],
        }),
      },
      message: "Persist run grants",
    });
    await enqueueInbox(source.repoStore, hubPrincipal, repoId, {
      address: agentAddress,
      messageId: "message-before-replacement",
      receivedAt: 1,
      mailAuditRef: { store: "mail", path: "before" },
    });

    const pushed: string[] = [];
    const packClient = createWorkflowRunPackClient({
      substrate: target.repoStore,
      hubLink: {
        async pushWorkflowRunPack(pack) {
          pushed.push(pack.commitSha);
          const expectedOldSha = await source.repoStore.resolveRef(
            hubPrincipal,
            pack.repoId,
            pack.ref,
          );
          await source.repoStore.receivePack(
            hubPrincipal,
            pack.repoId,
            pack.ref,
            pack.pack,
            pack.commitSha,
            expectedOldSha,
          );
        },
      },
    });
    const restore = createWorkflowRunPackRestorer({
      substrate: target.repoStore,
      markRestored: packClient.markRestored,
    });

    for (const ref of ["refs/heads/main", "refs/heads/events"] as const) {
      const pack = await source.repoStore.createPack(hubPrincipal, repoId, ref);
      await restore({ agentAddress, repoId, ...pack });
      expect(await target.repoStore.resolveRef(hubPrincipal, repoId, ref)).toBe(
        pack.commitSha,
      );
    }
    const restoredInbox = path.join(
      target.repoStore.getRepoDir(repoId),
      "addresses",
      encodeURIComponent(agentAddress),
      "inbox",
      "1-message-before-replacement.json",
    );
    expect(JSON.parse(await fs.readFile(restoredInbox, "utf8"))).toMatchObject({
      messageId: "message-before-replacement",
      address: agentAddress,
    });
    expect(
      JSON.parse(
        await fs.readFile(
          path.join(
            target.repoStore.getRepoDir(repoId),
            "runs",
            "run-before-replacement",
            "grants.json",
          ),
          "utf8",
        ),
      ),
    ).toEqual({ grants: [] });

    // A reconnect at the restored tip is a no-op, not an empty pack that the
    // Hub would reject because it contains no declared tip object.
    await packClient.push({
      agentAddress,
      repoId,
      ref: "refs/heads/events",
    });
    expect(pushed).toEqual([]);

    await enqueueInbox(target.repoStore, supervisorPrincipal, repoId, {
      address: agentAddress,
      messageId: "message-after-replacement",
      receivedAt: 2,
      mailAuditRef: { store: "mail", path: "after" },
    });
    await packClient.push({
      agentAddress,
      repoId,
      ref: "refs/heads/events",
    });

    expect(pushed).toHaveLength(1);
    expect(
      await source.repoStore.resolveRef(
        hubPrincipal,
        repoId,
        "refs/heads/events",
      ),
    ).toBe(
      await target.repoStore.resolveRef(
        hubPrincipal,
        repoId,
        "refs/heads/events",
      ),
    );
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
