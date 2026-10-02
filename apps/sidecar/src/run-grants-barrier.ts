// The join between a message and the authorization that governs it.
//
// On the hub control socket these two things travel together and in order: the
// hub writes a `run.grants` frame and then the trigger mail onto one FIFO
// channel, so the grants provably precede the mail. A deployment that receives
// mail over IMAP has no shared channel and therefore no ordering, and the cost
// of losing it is not a delayed run. Two separate failures follow, depending on
// which half of the frame is missing:
//
//   - No cached sender key: the admission gate resolves a known sender as
//     `unknown`, the default policy rejects, and the message is dropped. The run
//     never fires and nothing reports why.
//   - No grants file: the message is admitted and reaches the supervisor, whose
//     pre-trigger barrier synthesizes `RunFailed`. `RunFailed` is terminal and a
//     terminal run refuses every later trigger, so the deployment is dead.
//
// One `run.grants` frame carries both halves, so waiting for it closes both.
// This barrier is that wait: before a message is allowed to reach the
// supervisor, the ingress blocks here until the prerequisites are in place,
// asking the hub for them if they are not.
//
// The readiness question is answered from DURABLE state, never from a memo of
// frames this process has seen. A process-local memo would be wrong in both
// directions: it reports "not ready" for a deployment restored from disk whose
// grants are already committed (costing a pointless round trip, and wedging the
// message outright if the hub declines to re-materialize a run it considers
// finished), and it reports "ready" for an address redeployed after a teardown
// that discarded the run's repo. Reading the two durable facts has neither
// failure mode and needs no invalidation.

import { getLogger } from "@intx/log";

const logger = getLogger(["sidecar", "run-grants-barrier"]);

/**
 * How long one `ensure` waits for the hub's answer before giving up.
 *
 * Giving up is not a failure of the message: `ensure` rejects, the ingress
 * leaves the message unconsumed in its mailbox, and the next ingest attempt
 * re-enters here and re-asks. The deadline exists so a hub that never answers
 * produces a retry rather than an unbounded wait holding the message.
 */
const DEFAULT_WAIT_MS = 15_000;

export type RunGrantsBarrierDeps = {
  /** Sends the request frame. Fire-and-forget; the hub answers with `run.grants`. */
  request: (args: {
    agentAddress: string;
    runId: string;
    senderAddress: string;
  }) => void;
  /**
   * Whether this run's grants are committed to its workflow-run repo -- the
   * same file, read the same way, that the supervisor's pre-trigger barrier
   * reads. Asking the question the supervisor will ask is what makes a `ready`
   * answer here mean the run can actually start.
   */
  hasDurableGrants: (args: {
    agentAddress: string;
    runId: string;
  }) => Promise<boolean>;
  /**
   * Whether the sender-key cache holds a key for this address -- the same
   * resolver the admission gate verifies against, so a `ready` answer means the
   * gate will not report a known sender as `unknown`.
   */
  hasSenderKey: (senderAddress: string) => boolean;
  waitMs?: number;
};

export type RunGrantsBarrier = {
  /**
   * Record that a `run.grants` frame for this run has been applied -- its
   * grants written and any co-delivered key cached. Wakes every waiter.
   *
   * Called from the sidecar's grants-router path AFTER the apply resolves, so a
   * waiter released by it observes the applied state rather than the frame's
   * arrival. This is a wake-up only: readiness itself is re-derived from durable
   * state, so a missed signal costs a timeout and a retry, never a message
   * admitted without its prerequisites.
   */
  noteApplied(agentAddress: string, runId: string): void;

  /**
   * Resolve once this run's grants are committed and the sender's key is
   * cached. Asks the hub when either is missing.
   *
   * Rejects when the deadline passes with neither in place. The caller must
   * treat a rejection as "not yet" and retry, never as a refusal: a refusal is
   * the hub declining to materialize, which is indistinguishable here and is
   * equally answered by leaving the message unconsumed.
   */
  ensure(args: {
    agentAddress: string;
    runId: string;
    senderAddress: string;
  }): Promise<void>;
};

type Waiter = { resolve: () => void; reject: (cause: Error) => void };

export function createRunGrantsBarrier(
  deps: RunGrantsBarrierDeps,
): RunGrantsBarrier {
  const waitMs = deps.waitMs ?? DEFAULT_WAIT_MS;
  const waiters = new Map<string, Set<Waiter>>();

  // Keyed on the pair, not the runId alone: a runId is the local part of its
  // deployment address, so two deployments in different domains can share one.
  const keyFor = (agentAddress: string, runId: string): string =>
    `${agentAddress} ${runId}`;

  async function ready(args: {
    agentAddress: string;
    runId: string;
    senderAddress: string;
  }): Promise<boolean> {
    if (!deps.hasSenderKey(args.senderAddress)) return false;
    return await deps.hasDurableGrants({
      agentAddress: args.agentAddress,
      runId: args.runId,
    });
  }

  return {
    noteApplied(agentAddress, runId) {
      const key = keyFor(agentAddress, runId);
      const pending = waiters.get(key);
      if (pending === undefined) return;
      waiters.delete(key);
      for (const waiter of pending) waiter.resolve();
      logger.debug`grants applied for ${agentAddress} run ${runId}; released ${String(pending.size)} waiter(s)`;
    },

    async ensure(args) {
      const { agentAddress, runId, senderAddress } = args;
      const key = keyFor(agentAddress, runId);

      let settle: Waiter | undefined;
      const settled = new Promise<void>((resolve, reject) => {
        const waiter: Waiter = { resolve, reject };
        const timer = setTimeout(() => {
          const current = waiters.get(key);
          current?.delete(waiter);
          if (current !== undefined && current.size === 0) waiters.delete(key);
          reject(
            new Error(
              `no run.grants for ${agentAddress} run ${runId} within ${String(waitMs)}ms of asking the hub`,
            ),
          );
        }, waitMs);
        waiter.resolve = () => {
          clearTimeout(timer);
          resolve();
        };
        waiter.reject = (cause) => {
          clearTimeout(timer);
          reject(cause);
        };
        settle = waiter;
        let pending = waiters.get(key);
        if (pending === undefined) {
          pending = new Set();
          waiters.set(key, pending);
        }
        pending.add(waiter);
      });
      if (settle === undefined) {
        throw new Error("run-grants barrier: waiter was not registered");
      }
      const waiter = settle;

      // Registered BEFORE the readiness read, so an apply that lands during the
      // read wakes a waiter that already exists rather than firing into nothing.
      let satisfied: boolean;
      try {
        satisfied = await ready(args);
      } catch (cause) {
        // A malformed grants file throws rather than reporting absence, and that
        // is a boundary bug the message must not paper over by asking for a
        // fresh copy. Surface it; the ingress leaves the message unconsumed.
        waiter.reject(
          cause instanceof Error ? cause : new Error(String(cause)),
        );
        return await settled;
      }
      if (satisfied) {
        waiter.resolve();
        return await settled;
      }

      logger.info`waiting on run.grants for ${agentAddress} run ${runId}; asking the hub`;
      deps.request({ agentAddress, runId, senderAddress });
      await settled;
    },
  };
}
