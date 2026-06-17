// Shared helpers for the FIFO mail integration tests.
//
// The 3-mail correctness test (`fifo-mail.test.ts`) and the under-load
// regression test (`fifo-mail-load.test.ts`) both observe the same
// supervisor surface: per-runId event chains via the workflow-run repo
// and per-message consumed-envelope entries via the workflow-run
// claim-check ref. The walking and decoding logic is identical across
// both; this module is its single home.
//
// The under-load test lives in a separate file because the test
// runtime exceeds the per-iteration latency the operator is willing
// to pay on every `make test`. Both files reference these helpers so
// the split does not duplicate the walk logic.

import git from "isomorphic-git";

import type { RepoId } from "@intx/hub-sessions";

import {
  listRunIds,
  readClaimCheckDir,
  readWorkflowRunEvents,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

/**
 * Walk every `runs/<runId>/events/` subtree on the deployment's
 * workflow-run repo until each of `messageIds` is observed in some
 * run's `RunStarted.consumedMessageId`. Returns one
 * `{ messageId, runId }` per supplied `messageId`, preserving the
 * input order so the caller can assert FIFO mail-fire ordering
 * downstream.
 */
export async function waitForRunsByMessageIds(
  env: DeployFlowEnv,
  deploymentId: string,
  workflowRunRepoId: RepoId,
  messageIds: readonly string[],
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<{ messageId: string; runId: string }[]> {
  const { timeoutMs = 30_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const runIds = await listRunIds(env, workflowRunRepoId);
    const byMessageId = new Map<string, string>();
    for (const runId of runIds) {
      const events = await readWorkflowRunEvents(env, deploymentId, runId);
      for (const event of events) {
        if (event.type !== "RunStarted") continue;
        const consumed = event.body["consumedMessageId"];
        if (typeof consumed !== "string") continue;
        byMessageId.set(consumed, runId);
      }
    }
    const allObserved = messageIds.every((mid) => byMessageId.has(mid));
    if (allObserved) {
      return messageIds.map((messageId) => {
        const runId = byMessageId.get(messageId);
        if (runId === undefined) throw new Error("unreachable");
        return { messageId, runId };
      });
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      const observed = [...byMessageId.keys()].join(", ");
      const deploymentMailAddress =
        env.deployments.get(deploymentId)?.mailAddress ?? "";
      const inbox = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "inbox",
      );
      const processing = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "processing",
      );
      const consumed = await readClaimCheckDir(
        env,
        workflowRunRepoId,
        deploymentMailAddress,
        "consumed",
      );
      const eventsByRun: string[] = [];
      for (const runId of runIds) {
        const evs = await readWorkflowRunEvents(env, deploymentId, runId);
        eventsByRun.push(`  ${runId}: ${evs.map((e) => e.type).join(" -> ")}`);
      }
      throw new Error(
        `waitForRunsByMessageIds timed out after ${String(timeoutMs)}ms; expected ${messageIds.join(", ")}; observed ${observed || "<none>"};\n` +
          `inbox: ${inbox.map((e) => e.filename).join(", ") || "<empty>"}\n` +
          `processing: ${processing.map((e) => e.filename).join(", ") || "<empty>"}\n` +
          `consumed: ${consumed.map((e) => e.filename).join(", ") || "<empty>"}\n` +
          `runs:\n${eventsByRun.join("\n") || "<no runs>"}` +
          ctx,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

/**
 * Read the `consumed/` dedup index for the deployment's mail
 * address on the workflow-run repo's claim-check ref
 * (`refs/heads/events`). Returns one entry per consumed message in
 * the order the substrate's tree iteration surfaces them (which is
 * filename-sorted by `isomorphic-git`).
 */
export async function readConsumedEntries(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
): Promise<{ messageId: string; receivedAt: number }[]> {
  const entries = await readClaimCheckDir(
    env,
    workflowRunRepoId,
    address,
    "consumed",
  );
  const out: { messageId: string; receivedAt: number }[] = [];
  for (const entry of entries) {
    const m = /^(.+)\.json$/.exec(entry.filename);
    if (m === null || m[1] === undefined) continue;
    const messageId = m[1];
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- the workflow-run kind handler validates the consumed envelope shape at push time; readers downstream of validatePush observe Record<string, unknown>
    const parsed = JSON.parse(new TextDecoder().decode(entry.bytes)) as Record<
      string,
      unknown
    >;
    const receivedAt = parsed["receivedAt"];
    if (typeof receivedAt !== "number") {
      throw new Error(
        `readConsumedEntries: ${entry.filename} envelope is missing a numeric receivedAt`,
      );
    }
    out.push({ messageId, receivedAt });
  }
  return out;
}

/**
 * Poll `consumed/` for the deployment's mail address on the
 * workflow-run claim-check ref until every supplied messageId is
 * present, then return the consumed entries. The supervisor's
 * dispatch loop writes `markConsumed` AFTER the run's terminal
 * event lands -- a hub-side observation of `RunCompleted` for the
 * final run of a burst is therefore strictly earlier than that
 * run's `markConsumed` pack push. The pack-push wrap on the
 * sidecar awaits hub ack on every write, so the writes happen in
 * order, but the last write of the burst still has to traverse
 * the dispatch loop's terminal-watcher -> markConsumed ->
 * pack-push pipeline before the test can observe it.
 */
export async function waitForConsumedEntries(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  messageIds: readonly string[],
  opts: { timeoutMs?: number; diagnostics?: () => string } = {},
): Promise<{ messageId: string; receivedAt: number }[]> {
  const { timeoutMs = 30_000, diagnostics } = opts;
  const start = Date.now();
  for (;;) {
    const entries = await readConsumedEntries(env, workflowRunRepoId, address);
    const seen = new Set(entries.map((e) => e.messageId));
    if (messageIds.every((mid) => seen.has(mid))) {
      return entries;
    }
    if (Date.now() - start > timeoutMs) {
      const diag = diagnostics?.();
      const ctx = diag ? `\n${diag}` : "";
      const observed = [...seen].join(", ") || "<none>";
      throw new Error(
        `waitForConsumedEntries timed out after ${String(timeoutMs)}ms; expected ${messageIds.join(", ")}; observed ${observed}` +
          ctx,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }
}

// `git` is imported above so downstream files don't need to. Re-export
// in case a consumer wants to drive isomorphic-git directly off the
// same module rather than re-importing.
export { git };
