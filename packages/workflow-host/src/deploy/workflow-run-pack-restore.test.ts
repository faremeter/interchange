import { expect, test } from "bun:test";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { WORKFLOW_RUN_GITIGNORE_PATH } from "@intx/hub-sessions";
import {
  createAgentRepoStore,
  enqueueInbox,
  type Principal,
  type RepoId,
  type WorkflowRunSupervisorPrincipal,
} from "@intx/hub-sessions/substrate";

import { createWorkflowRunPackClient } from "./workflow-run-pack-client";
import { createWorkflowRunPackRestorer } from "./workflow-run-pack-restore";

function deriveWorkflowRunRepoId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
}

test("a first deploy keeps its seeded refs and the next sidecar commit fast-forwards the Hub", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "wfr-restore-"));
  try {
    const sourceKey = await generateKeyPair();
    const targetKey = await generateKeyPair();
    const source = createAgentRepoStore({
      dataDir: path.join(root, "hub"),
      signingKey: sourceKey,
    });
    const target = createAgentRepoStore({
      dataDir: path.join(root, "sidecar"),
      signingKey: targetKey,
    });
    const agentAddress = "run_restore@workflow.test";
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
        "runs/run-seeded/grants.json": JSON.stringify({
          grants: [],
        }),
      },
      message: "Persist run grants",
    });
    await enqueueInbox(source.repoStore, hubPrincipal, repoId, {
      address: agentAddress,
      messageId: "message-seeded",
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
      deriveWorkflowRunRepoId,
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
      "1-message-seeded.json",
    );
    expect(JSON.parse(await fs.readFile(restoredInbox, "utf8"))).toMatchObject({
      messageId: "message-seeded",
      address: agentAddress,
    });
    expect(
      JSON.parse(
        await fs.readFile(
          path.join(
            target.repoStore.getRepoDir(repoId),
            "runs",
            "run-seeded",
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
      messageId: "message-after-deploy",
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

async function seededDeployment(root: string) {
  const hub = createAgentRepoStore({
    dataDir: path.join(root, "hub"),
    signingKey: await generateKeyPair(),
  });
  const sidecar = createAgentRepoStore({
    dataDir: path.join(root, "sidecar"),
    signingKey: await generateKeyPair(),
  });
  const agentAddress = "run_restore@workflow.test";
  const repoId: RepoId = {
    kind: "workflow-run",
    id: deriveWorkflowRunRepoId(agentAddress),
  };
  const hubPrincipal: Principal = { kind: "hub" };
  await hub.repoStore.writeTree(hubPrincipal, repoId, "refs/heads/main", {
    files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
    message: "Initialize workflow run",
  });
  const pack = await hub.repoStore.createPack(
    hubPrincipal,
    repoId,
    "refs/heads/main",
  );
  const restored: string[] = [];
  const restore = createWorkflowRunPackRestorer({
    deriveWorkflowRunRepoId,
    substrate: sidecar.repoStore,
    markRestored: (_repoId, _ref, commitSha) => restored.push(commitSha),
  });
  const sidecarMain = () =>
    sidecar.repoStore.resolveRef(hubPrincipal, repoId, "refs/heads/main");
  return {
    sidecar,
    agentAddress,
    repoId,
    pack,
    restore,
    restored,
    sidecarMain,
  };
}

test("a retried first deploy takes the same seed again", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "wfr-restore-"));
  try {
    const { agentAddress, repoId, pack, restore, restored, sidecarMain } =
      await seededDeployment(root);

    await restore({ agentAddress, repoId, ...pack });
    await restore({ agentAddress, repoId, ...pack });

    expect(restored).toEqual([pack.commitSha, pack.commitSha]);
    expect(await sidecarMain()).toBe(pack.commitSha);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("a retried first deploy takes the seed over the genesis an earlier attempt left", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "wfr-restore-"));
  try {
    const {
      sidecar,
      agentAddress,
      repoId,
      pack,
      restore,
      restored,
      sidecarMain,
    } = await seededDeployment(root);
    // An attempt that failed after creating the repository left only the
    // store's genesis on main.
    await sidecar.repoStore.initRepo(repoId);
    const genesis = await sidecarMain();
    expect(genesis).not.toBeNull();
    expect(genesis).not.toBe(pack.commitSha);

    await restore({ agentAddress, repoId, ...pack });

    expect(restored).toEqual([pack.commitSha]);
    expect(await sidecarMain()).toBe(pack.commitSha);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});

test("a seed never moves a branch the sidecar already has", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "wfr-restore-"));
  try {
    const {
      sidecar,
      agentAddress,
      repoId,
      pack,
      restore,
      restored,
      sidecarMain,
    } = await seededDeployment(root);
    await sidecar.repoStore.writeTree(
      { kind: "hub" },
      repoId,
      "refs/heads/main",
      {
        files: { "runs/left-behind/grants.json": JSON.stringify({}) },
        message: "History of an earlier copy",
      },
    );
    const leftBehind = await sidecarMain();

    await expect(restore({ agentAddress, repoId, ...pack })).rejects.toThrow(
      "workflow_run_restore_conflict",
    );

    expect(await sidecarMain()).toBe(leftBehind);
    expect(restored).toEqual([]);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
