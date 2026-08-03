import { promises as fs } from "node:fs";
import path from "node:path";

import {
  WORKFLOW_RUN_RESTORE_REFS,
  type RepoId,
  type RepoStore,
} from "@intx/hub-sessions";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

const RESTORABLE_REFS: readonly string[] = WORKFLOW_RUN_RESTORE_REFS;

export type WorkflowRunPackRestoreArgs = {
  agentAddress: string;
  repoId: RepoId;
  pack: Uint8Array;
  ref: string;
  commitSha: string;
};

export type WorkflowRunPackRestorer = (
  args: WorkflowRunPackRestoreArgs,
) => Promise<void>;

type MaterializedFile = {
  oid: string;
  read(): Promise<Uint8Array>;
};

async function materializeRestoredRefs(
  substrate: RepoStore,
  repoId: RepoId,
): Promise<void> {
  const hubPrincipal = { kind: "hub" } as const;
  const files = new Map<string, MaterializedFile>();
  const directories = new Set<string>();

  for (const ref of WORKFLOW_RUN_RESTORE_REFS) {
    const reads = await substrate.openCommittedReads(hubPrincipal, repoId, ref);
    if (reads === null) continue;

    const walk = async (dir: string): Promise<void> => {
      for (const entry of await reads.listDir(dir)) {
        const relPath = dir === "" ? entry.name : `${dir}/${entry.name}`;
        if (entry.type === "tree") {
          if (files.has(relPath)) {
            throw new Error(
              `workflow_run_restore_conflict: ${relPath} is both a file and directory across restored refs`,
            );
          }
          directories.add(relPath);
          await walk(relPath);
          continue;
        }
        if (entry.type === "commit") {
          throw new Error(
            `workflow_run_restore_invalid: submodule entry at ${relPath} is unsupported`,
          );
        }
        if (directories.has(relPath)) {
          throw new Error(
            `workflow_run_restore_conflict: ${relPath} is both a directory and file across restored refs`,
          );
        }
        const existing = files.get(relPath);
        if (existing !== undefined && existing.oid !== entry.oid) {
          throw new Error(
            `workflow_run_restore_conflict: ${relPath} differs across restored refs`,
          );
        }
        if (existing === undefined) {
          files.set(relPath, {
            oid: entry.oid,
            read: () => reads.readBlobByOid(entry.oid),
          });
        }
      }
    };
    await walk("");
  }

  const repoDir = path.resolve(substrate.getRepoDir(repoId));
  const repoPrefix = `${repoDir}${path.sep}`;
  const destinations = new Map<string, string>();
  for (const relPath of files.keys()) {
    const destination = path.resolve(repoDir, ...relPath.split("/"));
    if (!destination.startsWith(repoPrefix)) {
      throw new Error(
        `workflow_run_restore_invalid: path ${relPath} escapes the workflow-run repository`,
      );
    }
    destinations.set(relPath, destination);
  }
  for (const entry of await fs.readdir(repoDir)) {
    if (entry === ".git") continue;
    await fs.rm(path.join(repoDir, entry), { recursive: true, force: true });
  }
  for (const [relPath, file] of files) {
    const destination = destinations.get(relPath);
    if (destination === undefined) {
      throw new Error(
        `workflow_run_restore_invalid: missing validated destination for ${relPath}`,
      );
    }
    await fs.mkdir(path.dirname(destination), { recursive: true });
    await fs.writeFile(destination, await file.read());
  }
}

/**
 * Build the sidecar boundary that installs Hub-authoritative workflow-run
 * history before a replacement supervisor starts. Packs land on the
 * unwrapped substrate so applying restored history cannot echo it back to the
 * Hub as a new sidecar-authored update.
 */
export function createWorkflowRunPackRestorer(args: {
  substrate: RepoStore;
  markRestored(repoId: RepoId, ref: string, commitSha: string): void;
}): WorkflowRunPackRestorer {
  const { substrate, markRestored } = args;
  const hubPrincipal = { kind: "hub" } as const;

  return async ({ agentAddress, repoId, pack, ref, commitSha }) => {
    if (repoId.kind !== "workflow-run") {
      throw new Error(
        `workflow_run_restore_invalid: expected workflow-run repo, got ${repoId.kind}`,
      );
    }
    const expectedRepoId = deriveWorkflowRunRepoId(agentAddress);
    if (repoId.id !== expectedRepoId) {
      throw new Error(
        `workflow_run_restore_invalid: ${agentAddress} maps to ${expectedRepoId}, not ${repoId.id}`,
      );
    }
    if (!RESTORABLE_REFS.includes(ref)) {
      throw new Error(
        `workflow_run_restore_invalid: unsupported workflow-run ref ${ref}`,
      );
    }

    // `RepoStore` initializes a signed `.gitignore` genesis on main. Do that
    // before resolving the CAS pre-image; resolving an absent on-disk repo to
    // null and then letting `receivePack` initialize it would observe a new
    // genesis inside the receive lock and reject the stale null pre-image.
    await substrate.initRepo(repoId);
    const expectedOldSha = await substrate.resolveRef(
      hubPrincipal,
      repoId,
      ref,
    );
    if (expectedOldSha !== commitSha) {
      await substrate.receivePack(
        hubPrincipal,
        repoId,
        ref,
        pack,
        commitSha,
        expectedOldSha,
      );
    }
    await materializeRestoredRefs(substrate, repoId);

    // `markRestored` updates both the substrate's incremental-pack cursor and
    // the pack client's acknowledged-tip shadow. It runs only after the ref is
    // known to equal `commitSha`, so a failed receive never suppresses a later
    // retry.
    markRestored(repoId, ref, commitSha);
  };
}
