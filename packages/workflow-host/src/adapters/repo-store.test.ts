import { describe, test, expect, afterAll, beforeAll } from "bun:test";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";
import type { KeyPair } from "@intx/types/runtime";
import {
  createRepoStore,
  workflowRunKindHandler,
  WORKFLOW_RUN_GITIGNORE_PATH,
} from "@intx/hub-sessions";
import type {
  AuthorizeFn,
  KindHandler,
  Principal,
  RepoId,
  ValidatePushResult,
} from "@intx/hub-sessions";
import type { WorkflowEvent } from "@intx/workflow";

import { createWorkflowRunRepoStore } from "./repo-store";

const tempDirs: string[] = [];

async function makeTempDir(prefix: string): Promise<string> {
  const d = await fs.promises.mkdtemp(path.join(os.tmpdir(), prefix));
  tempDirs.push(d);
  return d;
}

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

const REF = "refs/heads/main";
const allowAll: AuthorizeFn = () => ({ allowed: true });

// The kind handler does principal-vs-event-origin checks only on
// `CancelRequested` events; the bare `{ kind: "workflow-process" }`
// principal is enough for the RunStarted/StepStarted shapes the tests
// here exercise, and the substrate's allow-all authorize callback
// gates every action.
const WORKFLOW_PROCESS_PRINCIPAL: Principal = { kind: "workflow-process" };

function freshRunStarted(runId: string, seq = 1): WorkflowEvent {
  return {
    kind: "RunStarted",
    seq,
    at: new Date(0).toISOString(),
    runId,
    definitionHash: "definition-hash",
    trigger: { type: "manual", payload: {} },
  };
}

function freshStepStarted(seq: number): WorkflowEvent {
  return {
    kind: "StepStarted",
    seq,
    at: new Date(0).toISOString(),
    stepId: "step-a",
    attempt: 1,
    input: { ref: "inline:{}" },
  };
}

describe("workflow-host RepoStore adapter — happy path", () => {
  test("append followed by read returns the events in seq order", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-happy-");
    const repoId: RepoId = { kind: "workflow-run", id: "deployment-happy" };
    const principal = WORKFLOW_PROCESS_PRINCIPAL;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    // The kind handler accepts the `.gitignore`-only genesis; seed it
    // so the first append has a coherent prior tree to extend.
    await substrate.writeTree({ kind: "hub" }, repoId, REF, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    });

    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "run-1";
    await adapter.append(runId, freshRunStarted(runId, 1));
    await adapter.append(runId, freshStepStarted(2));

    const events = await adapter.read(runId);
    expect(events).toHaveLength(2);
    expect(events[0]?.kind).toBe("RunStarted");
    expect(events[0]?.seq).toBe(1);
    expect(events[1]?.kind).toBe("StepStarted");
    expect(events[1]?.seq).toBe(2);
  });

  test("on-disk envelope carries top-level seq and type matching the workflow-run kind handler contract", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-envelope-");
    const repoId: RepoId = { kind: "workflow-run", id: "deployment-envelope" };
    const principal = WORKFLOW_PROCESS_PRINCIPAL;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    await substrate.writeTree({ kind: "hub" }, repoId, REF, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    });
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "run-envelope";
    await adapter.append(runId, freshRunStarted(runId, 1));

    const eventPath = path.join(
      substrate.getRepoDir(repoId),
      "runs",
      runId,
      "events",
      "1.json",
    );
    const raw = await fs.promises.readFile(eventPath, "utf8");
    const parsed: Record<string, unknown> = JSON.parse(raw);
    expect(parsed.seq).toBe(1);
    expect(parsed.type).toBe("RunStarted");
    // The on-disk shape does not carry `kind`; the discriminator on
    // disk is `type` per the workflow-run kind handler's contract.
    expect(parsed.kind).toBeUndefined();
  });

  test("multiple appends preserve seq monotonicity across distinct events", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-monotonic-");
    const repoId: RepoId = { kind: "workflow-run", id: "deployment-monotonic" };
    const principal = WORKFLOW_PROCESS_PRINCIPAL;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    await substrate.writeTree({ kind: "hub" }, repoId, REF, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    });
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "run-monotonic";
    for (let seq = 1; seq <= 5; seq += 1) {
      const event: WorkflowEvent =
        seq === 1 ? freshRunStarted(runId, 1) : freshStepStarted(seq);
      await adapter.append(runId, event);
    }
    const events = await adapter.read(runId);
    expect(events.map((e) => e.seq)).toEqual([1, 2, 3, 4, 5]);
  });
});

describe("workflow-host RepoStore adapter — append-result error translation", () => {
  test("seq_conflict: caller-supplied seq diverges from the prior-tree seq", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-seq-conflict-");
    const repoId: RepoId = {
      kind: "workflow-run",
      id: "deployment-seq-conflict",
    };
    const principal = WORKFLOW_PROCESS_PRINCIPAL;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    await substrate.writeTree({ kind: "hub" }, repoId, REF, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    });
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "run-seq-conflict";
    // First append lands at seq 1 (the runtime body's first emit is
    // `emptyState.lastSeq + 1`).
    await adapter.append(runId, freshRunStarted(runId, 1));
    // The next append's caller-supplied seq is 1 (stale); the adapter
    // sees the prior tree already has seq 1 and the next seq is 2.
    // The single-writer-invariant violation must surface as a thrown
    // Error naming both the expected and supplied seqs.
    const stale = freshStepStarted(1);
    await expect(adapter.append(runId, stale)).rejects.toThrow(
      /seq conflict on append to run-seq-conflict.*single-writer invariant violated.*expected seq 2.*caller supplied 1/,
    );
  });

  test("validate_failed: kind handler rejection surfaces with the handler's reason", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-validate-failed-");
    const repoId: RepoId = {
      kind: "agent-state",
      id: "deployment-validate-failed",
    };
    const principal: Principal = { kind: "test" };

    // Custom kind handler that accepts everything except a `kind`
    // equal to "RunStarted". The adapter writes the on-disk envelope
    // with `type: "RunStarted"`; the handler reads the file via
    // `readBlob`, parses, and rejects with a known reason. The
    // adapter must translate the substrate's `path_violation:`
    // surface into a thrown Error carrying the handler's reason.
    const failingHandler: KindHandler = {
      kind: "agent-state",
      directoryPrefix: "validate-failed",
      async validatePush({ topLevelTreePaths, listDir, readBlob }) {
        if (!topLevelTreePaths.includes("runs")) return { ok: true };
        const runIds = await listDir("runs");
        for (const r of runIds) {
          const filenames = await listDir(`runs/${r}/events`);
          for (const name of filenames) {
            const blob = await readBlob(`runs/${r}/events/${name}`);
            const parsed: unknown = JSON.parse(new TextDecoder().decode(blob));
            if (
              typeof parsed === "object" &&
              parsed !== null &&
              (parsed as { type?: unknown }).type === "RunStarted"
            ) {
              return {
                ok: false,
                reason: "rejected-for-test: RunStarted is forbidden",
              };
            }
          }
        }
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
    };

    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": failingHandler },
      authorize: allowAll,
    });
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "run-validate-failed";
    await expect(
      adapter.append(runId, freshRunStarted(runId, 1)),
    ).rejects.toThrow(/rejected-for-test: RunStarted is forbidden/);
    // The thrown error must NOT still carry the substrate's
    // `path_violation:` prefix; the adapter strips it so the runtime
    // sees a clean reason.
    let capturedMessage = "";
    try {
      await adapter.append(runId, freshRunStarted(runId, 1));
    } catch (err) {
      capturedMessage = err instanceof Error ? err.message : String(err);
    }
    expect(capturedMessage.startsWith("path_violation:")).toBe(false);
  });
});

describe("workflow-host RepoStore adapter — read coherence", () => {
  test("read at HEAD returns an empty list when no events exist for the run", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-empty-read-");
    const repoId: RepoId = { kind: "workflow-run", id: "deployment-empty" };
    const principal = WORKFLOW_PROCESS_PRINCIPAL;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    // No genesis write -- the substrate's on-disk repo does not yet
    // exist. The adapter's read must surface an empty list rather than
    // throw.
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });
    const events = await adapter.read("unknown-run");
    expect(events).toEqual([]);
  });
});

describe("workflow-host RepoStore adapter — first-seq contract against runtime body", () => {
  test("append with seq=1 on an empty events tree (the runtime body's actual first event)", async () => {
    // The runtime body emits events at `state.lastSeq + 1` and
    // `emptyState.lastSeq = 0`, so the first append on a fresh run
    // carries seq=1. The adapter must accept that against an empty
    // events tree rather than rejecting on a seq-conflict against an
    // off-by-one baseline.
    const dataDir = await makeTempDir("repo-store-adapter-first-seq-");
    const repoId: RepoId = {
      kind: "workflow-run",
      id: "deployment-first-seq",
    };
    const principal = WORKFLOW_PROCESS_PRINCIPAL;
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "workflow-run": workflowRunKindHandler },
      authorize: allowAll,
    });
    await substrate.writeTree({ kind: "hub" }, repoId, REF, {
      files: { [WORKFLOW_RUN_GITIGNORE_PATH]: "" },
      message: "genesis",
    });
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "run-first-seq";
    await expect(
      adapter.append(runId, freshRunStarted(runId, 1)),
    ).resolves.toBeUndefined();
    const events = await adapter.read(runId);
    expect(events.map((e) => e.seq)).toEqual([1]);
    expect(events[0]?.kind).toBe("RunStarted");
  });
});

describe("workflow-host RepoStore adapter — permissive handler smoke", () => {
  test("write+read round-trip via permissive handler preserves event fields", async () => {
    const dataDir = await makeTempDir("repo-store-adapter-smoke-");
    const repoId: RepoId = { kind: "agent-state", id: "deployment-smoke" };
    const principal: Principal = { kind: "test" };
    const permissive: KindHandler = {
      kind: "agent-state",
      directoryPrefix: "smoke",
      validatePush(): ValidatePushResult {
        return { ok: true };
      },
      onRefUpdated() {
        /* no-op */
      },
    };
    const substrate = createRepoStore({
      dataDir,
      signingKey,
      handlers: { "agent-state": permissive },
      authorize: allowAll,
    });
    const adapter = createWorkflowRunRepoStore({
      substrate,
      repoId,
      principal,
      ref: REF,
    });

    const runId = "smoke-run";
    const original = freshRunStarted(runId, 1);
    await adapter.append(runId, original);
    const events = await adapter.read(runId);
    expect(events).toHaveLength(1);
    const restored = events[0];
    if (restored === undefined) throw new Error("unreachable");
    expect(restored.kind).toBe("RunStarted");
    expect(restored.seq).toBe(1);
    if (restored.kind !== "RunStarted") throw new Error("unreachable");
    expect(restored.runId).toBe(runId);
    expect(restored.definitionHash).toBe("definition-hash");
  });
});
