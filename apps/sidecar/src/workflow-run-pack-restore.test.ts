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

import { coldStepStorageRoot } from "./step-storage-root";
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
      dataDir: path.join(root, "replacement"),
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

test("replacing the host's history discards the multi-step step stores built on it", async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "wfr-restore-stores-"));
  const exists = (target: string) =>
    fs.stat(target).then(
      () => true,
      () => false,
    );
  try {
    const source = createAgentRepoStore({
      dataDir: path.join(root, "hub"),
      signingKey: await generateKeyPair(),
    });
    const hostDataDir = path.join(root, "host");
    const host = createAgentRepoStore({
      dataDir: hostDataDir,
      signingKey: await generateKeyPair(),
    });
    const agentAddress = "run_restore_stores@workflow.test";
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(agentAddress),
    };
    const hubPrincipal: Principal = { kind: "hub" };
    const ref = "refs/heads/main";
    await source.repoStore.writeTree(hubPrincipal, repoId, ref, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "Initialize workflow run",
    });
    const restore = createWorkflowRunPackRestorer({
      substrate: host.repoStore,
      dataDir: hostDataDir,
      markRestored: () => undefined,
    });
    const hubTip = await source.repoStore.createPack(hubPrincipal, repoId, ref);
    await restore({ agentAddress, repoId, ...hubTip });

    const attemptStore = path.join(
      coldStepStorageRoot({ dataDir: hostDataDir, workflowRunRepoId: repoId }),
      "run-1",
      "steps",
      "s",
      "attempt-1",
    );
    const warmWorkspace = path.join(
      hostDataDir,
      "workflow-step-state",
      repoId.id,
      "warm",
      "s",
      "workspace",
    );
    await fs.mkdir(attemptStore, { recursive: true });
    await fs.writeFile(path.join(attemptStore, "turns.jsonl"), "");
    await fs.mkdir(warmWorkspace, { recursive: true });

    // Replaying the tip the host already holds replaces nothing.
    await restore({ agentAddress, repoId, ...hubTip });
    expect(await exists(attemptStore)).toBe(true);

    // The claim-check ref holds no step state, so replacing only its history
    // keeps the stores built on the unchanged state ref.
    const eventsRef = "refs/heads/events";
    await enqueueInbox(source.repoStore, hubPrincipal, repoId, {
      address: agentAddress,
      messageId: "message-on-hub",
      receivedAt: 1,
      mailAuditRef: { store: "mail", path: "hub" },
    });
    const hubEventsTip = await source.repoStore.createPack(
      hubPrincipal,
      repoId,
      eventsRef,
    );
    await restore({ agentAddress, repoId, ...hubEventsTip });
    const supervisorPrincipal: WorkflowRunSupervisorPrincipal = {
      kind: "supervisor",
      anchorRunId: repoId.id,
    };
    await enqueueInbox(host.repoStore, supervisorPrincipal, repoId, {
      address: agentAddress,
      messageId: "message-the-hub-never-received",
      receivedAt: 2,
      mailAuditRef: { store: "mail", path: "host" },
    });
    await restore({ agentAddress, repoId, ...hubEventsTip });
    expect(
      await host.repoStore.resolveRef(hubPrincipal, repoId, eventsRef),
    ).toBe(hubEventsTip.commitSha);
    expect(await exists(attemptStore)).toBe(true);

    // The host commits past what the Hub received, and the Hub then replays
    // its own tip over it.
    await host.repoStore.writeTree(hubPrincipal, repoId, ref, {
      files: { "runs/run-1/grants.json": JSON.stringify({ grants: [] }) },
      message: "Persist run grants",
    });
    await restore({ agentAddress, repoId, ...hubTip });

    expect(await host.repoStore.resolveRef(hubPrincipal, repoId, ref)).toBe(
      hubTip.commitSha,
    );
    expect(await exists(attemptStore)).toBe(false);
    expect(await exists(warmWorkspace)).toBe(true);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
