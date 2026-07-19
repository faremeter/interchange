// Enumerate a child's currently-parked approval correlations from durable
// state, so the child can answer a supervisor `parked-correlations.request`.
//
// Enumeration keys on REDUCED step state, never on raw `SignalAwaited` log
// events. `parkOnSignal` commits `SignalAwaited` to the durable log before it
// checks for the approval snapshot, so a snapshot-less correlated suspend (a
// director `caps.suspend`, an unwired authz gate) leaves a control-plane
// `SignalAwaited` in the log yet reduces to `phase === "failed"` -- never
// `awaiting-signal`, and never a hub row. Filtering on the reduced
// `awaiting-signal` phase therefore surfaces only the parks that carry a
// durable snapshot by construction; a snapshot-less enumerated step is a
// disagreement between the log and the step store, which this module surfaces
// loudly rather than dropping.

import type {
  RepoId,
  RepoStore as SubstrateRepoStore,
} from "@intx/hub-sessions/substrate";
import type { RepoStore as RuntimeRepoStore } from "@intx/workflow";
import type { SignalKind } from "@intx/types";
import { correlationIdFromSignalName } from "@intx/types";
import type { ApprovalSnapshot } from "@intx/types/runtime";

import { discoverInFlightRuns } from "./self-discovery";

/**
 * Recover the durable approval snapshot for one parked control-plane
 * correlation. The child owns enumeration; the host owns the per-step
 * on-disk layout (cold vs warm), so the snapshot read is a host binding.
 * Returns `undefined` when no pending operation for the correlation carries
 * a snapshot.
 */
export type LoadParkedApproval = (args: {
  runId: string;
  stepId: string;
  attempt: number;
  correlationId: string;
}) => Promise<ApprovalSnapshot | undefined>;

/**
 * One parked approval correlation: the child-supplied half of a suspension
 * registration. `kind` is the literal `"approval"` -- the enumeration filter
 * (a control-plane `signalName(correlationId)` channel) already establishes
 * that this is an approval, and reading `kind` back from the durable record
 * (a bare `"approval"` literal) would risk a future signal kind silently
 * inheriting approval's snapshot policy instead of declaring its own.
 */
export interface ParkedApprovalCorrelation {
  runId: string;
  correlationId: string;
  kind: SignalKind;
  snapshot: ApprovalSnapshot;
}

export interface CollectParkedApprovalCorrelationsOpts {
  substrate: SubstrateRepoStore;
  repoId: RepoId;
  runtimeRepoStore: RuntimeRepoStore;
  loadParkedApproval?: LoadParkedApproval;
}

/**
 * Enumerate every in-flight run's reduced state and return one entry per step
 * parked on a control-plane approval channel. Throws when a park is found but
 * no `loadParkedApproval` binding is wired to recover its snapshot, or when
 * the binding returns no snapshot for an enumerated park -- both are
 * disagreements between the reduced state and the durable store that must not
 * silently drop a correlation the hub is waiting to register.
 */
export async function collectParkedApprovalCorrelations(
  opts: CollectParkedApprovalCorrelationsOpts,
): Promise<ParkedApprovalCorrelation[]> {
  const discovered = await discoverInFlightRuns({
    substrate: opts.substrate,
    repoId: opts.repoId,
    runtimeRepoStore: opts.runtimeRepoStore,
  });
  const out: ParkedApprovalCorrelation[] = [];
  for (const run of discovered) {
    for (const step of run.resumedState.steps.values()) {
      if (step.phase !== "awaiting-signal") continue;
      const name = step.awaitingSignal?.name;
      if (name === undefined) continue;
      const correlationId = correlationIdFromSignalName(name);
      if (correlationId === undefined) continue;
      if (opts.loadParkedApproval === undefined) {
        throw new Error(
          `workflow-child parked-correlations: run ${run.runId} step ${step.stepId} is parked on control-plane correlation ${correlationId}, but no loadParkedApproval binding is wired to recover its snapshot`,
        );
      }
      const snapshot = await opts.loadParkedApproval({
        runId: run.runId,
        stepId: step.stepId,
        attempt: step.currentAttempt,
        correlationId,
      });
      if (snapshot === undefined) {
        throw new Error(
          `workflow-child parked-correlations: reduced state shows run ${run.runId} step ${step.stepId} awaiting control-plane correlation ${correlationId} (attempt ${String(step.currentAttempt)}), but durable storage carries no approval snapshot for it; the run log and the step store disagree`,
        );
      }
      out.push({
        runId: run.runId,
        correlationId,
        kind: "approval",
        snapshot,
      });
    }
  }
  return out;
}
