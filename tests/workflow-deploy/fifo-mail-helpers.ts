// Shared helpers for the FIFO mail integration tests.
//
// The 3-mail correctness test (`fifo-mail.test.ts`), the under-load
// regression test (`fifo-mail-load.test.ts`), the crash-respawn test
// (`crash-respawn-fifo.test.ts`), and the hub-link reconnect test
// (`hub-link-reconnect.test.ts`) all observe the same supervisor surface:
// per-message consumed-envelope entries on the workflow-run claim-check
// ref. The decoding logic is identical across them; this module is its
// single home.
//
// The under-load test lives in a separate file because the test
// runtime exceeds the per-iteration latency the operator is willing
// to pay on every `make test`. Every one of those files references these
// helpers so the split does not duplicate the decode logic.

import type { RepoId } from "@intx/hub-sessions";

import {
  readClaimCheckDir,
  waitFor,
  type DeployFlowEnv,
} from "../hub-agent/lib/deploy-flow-env";

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
): Promise<
  {
    messageId: string;
    receivedAt: number;
    rejection?: { code: string; message: string };
  }[]
> {
  const entries = await readClaimCheckDir(
    env,
    workflowRunRepoId,
    address,
    "consumed",
  );
  const out: {
    messageId: string;
    receivedAt: number;
    rejection?: { code: string; message: string };
  }[] = [];
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
    const rejection = parsed["rejection"];
    if (rejection !== undefined) {
      if (
        typeof rejection !== "object" ||
        rejection === null ||
        !("code" in rejection) ||
        typeof rejection.code !== "string" ||
        !("message" in rejection) ||
        typeof rejection.message !== "string"
      ) {
        throw new Error(
          `readConsumedEntries: ${entry.filename} envelope has an invalid rejection`,
        );
      }
      out.push({
        messageId,
        receivedAt,
        rejection: { code: rejection.code, message: rejection.message },
      });
    } else {
      out.push({ messageId, receivedAt });
    }
  }
  return out;
}

/**
 * Poll `consumed/` for the deployment's mail address on the
 * workflow-run claim-check ref until every supplied messageId is
 * present, then return the consumed entries. The supervisor's
 * first dispatch writes `markConsumed` AFTER the stable run's terminal
 * event lands. Later queued messages are then consumed with terminal rejection
 * receipts. The pack-push wrapper awaits Hub acknowledgement on every write,
 * so the writes happen in order, but the last receipt still has to traverse
 * the dispatch loop's markConsumed -> pack-push pipeline before the test can
 * observe it.
 *
 * The poll carries no deadline of its own: the test runner's budget is the
 * failsafe for a receipt that never lands, and the harness `waitFor` it runs
 * through is what puts the sidecar's output on the env teardown's report when
 * that happens. `diagnostics` renders when the `consumed/` read itself fails --
 * a malformed envelope is a real fault rather than a not-yet, and the sidecar's
 * output is the context that explains it.
 */
export async function waitForConsumedEntries(
  env: DeployFlowEnv,
  workflowRunRepoId: RepoId,
  address: string,
  messageIds: readonly string[],
  opts: { diagnostics?: () => string } = {},
): Promise<
  {
    messageId: string;
    receivedAt: number;
    rejection?: { code: string; message: string };
  }[]
> {
  const { diagnostics } = opts;
  let entries: Awaited<ReturnType<typeof readConsumedEntries>> = [];
  await waitFor(async () => {
    try {
      entries = await readConsumedEntries(env, workflowRunRepoId, address);
    } catch (cause) {
      const diag = diagnostics?.();
      throw new Error(
        `waitForConsumedEntries: reading consumed/ for ${address} failed${diag === undefined ? "" : `\n${diag}`}`,
        { cause },
      );
    }
    const seen = new Set(entries.map((e) => e.messageId));
    return messageIds.every((mid) => seen.has(mid));
  });
  return entries;
}
