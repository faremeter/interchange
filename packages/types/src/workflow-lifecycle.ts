import { type } from "arktype";
import { sidecarAllocationStatuses } from "./sidecar-allocation";

// Fixed policy range with ample headroom for deployment and terminal timestamps.
const MAX_LIFECYCLE_DURATION_MS = 36_500 * 86_400_000;

function parseDuration(value: string): number | null {
  const match = /^(0|[1-9][0-9]*)(s|m|h|d)$/.exec(value);
  if (match === null) return null;
  const amount = Number(match[1]);
  const unit = match[2];
  const multiplier =
    unit === "d"
      ? 86_400_000
      : unit === "h"
        ? 3_600_000
        : unit === "m"
          ? 60_000
          : 1_000;
  const milliseconds = amount * multiplier;
  return Number.isSafeInteger(milliseconds) &&
    milliseconds <= MAX_LIFECYCLE_DURATION_MS
    ? milliseconds
    : null;
}

export const LifecycleDuration = type("string").narrow(
  (value, ctx) =>
    parseDuration(value) !== null ||
    ctx.mustBe(
      "a non-negative whole duration ending in s, m, h, or d, at most 36500d",
    ),
);
export type LifecycleDuration = typeof LifecycleDuration.infer;

export function lifecycleDurationMs(value: LifecycleDuration): number {
  const milliseconds = parseDuration(value);
  if (milliseconds === null)
    throw new Error(`Invalid lifecycle duration: ${value}`);
  return milliseconds;
}

export function lifecycleDeadline(
  start: Date,
  duration: LifecycleDuration,
): Date {
  const deadline = new Date(start.getTime() + lifecycleDurationMs(duration));
  if (!Number.isFinite(deadline.getTime())) {
    throw new Error(
      "Lifecycle deadline is outside the supported timestamp range",
    );
  }
  return deadline;
}

const PositiveLifecycleDuration = LifecycleDuration.narrow(
  (value, ctx) =>
    lifecycleDurationMs(value) > 0 || ctx.mustBe("greater than zero"),
);

// The Hub checks a busy deployment once per `standbyAfter`, so this bounds how
// often it sends each deployment an idle check.
const MIN_STANDBY_AFTER_MS = 10_000;

const StandbyAfterDuration = LifecycleDuration.narrow(
  (value, ctx) =>
    lifecycleDurationMs(value) >= MIN_STANDBY_AFTER_MS ||
    ctx.mustBe("at least 10s"),
);

export const WorkflowLifecyclePolicy = type({
  "maxLifetime?": PositiveLifecycleDuration,
  "maxDisconnected?": PositiveLifecycleDuration,
  "standbyAfter?": StandbyAfterDuration,
  "capacityRetention?": type({
    "completed?": LifecycleDuration,
    "failed?": LifecycleDuration,
    "cancelled?": LifecycleDuration,
  }).onUndeclaredKey("reject"),
}).onUndeclaredKey("reject");
export type WorkflowLifecyclePolicy = typeof WorkflowLifecyclePolicy.infer;

/**
 * A deployment's effective policy: every field that has a platform default is
 * set. `standbyAfter` has none, and without it the deployment is never put in
 * standby unasked.
 */
export type ResolvedWorkflowLifecyclePolicy = {
  maxLifetime: LifecycleDuration;
  maxDisconnected: LifecycleDuration;
  standbyAfter?: LifecycleDuration;
  capacityRetention: Required<
    NonNullable<WorkflowLifecyclePolicy["capacityRetention"]>
  >;
};

/** The platform defaults a deployment's policy falls back on. */
export type WorkflowLifecycleDefaults = Omit<
  ResolvedWorkflowLifecyclePolicy,
  "standbyAfter"
>;

export const workflowStandbyStates = ["entering", "standby", "waking"] as const;
export type WorkflowStandbyState = (typeof workflowStandbyStates)[number];

export const WorkflowLifecycleResponse = type({
  runId: "string",
  status: "'deployed' | 'running' | 'completed' | 'failed' | 'cancelled'",
  policy: WorkflowLifecyclePolicy,
  expiresAt: "string | null",
  cancellationRequestedAt: "string | null",
  cancellationDeadline: "string | null",
  cancellationReason: "string | null",
  capacityReleaseAt: "string | null",
  standby: type({
    state: type.enumerated("awake", ...workflowStandbyStates),
    changedAt: "string | null",
    wakeAt: "string | null",
    reason: "string | null",
  })
    .or("null")
    .describe(
      "Null once the run is terminal. `state` is `awake` while the workflow child runs and no standby is asked for, `entering` until the sidecar stops the child, whether a standby request or the policy's `standbyAfter` asked for it, `standby` once the Hub holds the history it left, and `waking` until the child runs again. `wakeAt` is the earliest timer a child in standby wakes for by itself. `reason` says why the state last changed other than as requested: the sidecar refused a standby or wake request, something other than a wake request woke the child in standby, such as mail or a restart that found new history, the sidecar reported the child running, it reported the child in standby though no standby was asked for, the Hub put the child in standby automatically because every run had waited for the policy's `standbyAfter`, a cancellation ended the standby, or the deployment moved to replacement capacity. While a request or an automatic standby is pending it says why the Hub's last attempt was put off. When none was, it is null for a request, which includes a sidecar that is not connected and history still on its way to the Hub, and a request stays pending for as long as that takes; for an automatic standby it says that every run waited long enough for the Hub to start it.",
    ),
  allocation: type({
    id: "string",
    generation: "number",
    status: type.enumerated(...sidecarAllocationStatuses),
    failureCode: "string | null",
    failureMessage: "string | null",
  }).or("null"),
});

// Names the allocation generation the caller inspected, so a request based on
// an outdated status cannot release a later binding.
export const RecoverWorkflowCapacityRequest = type({
  generation: "number.integer >= 0",
});

export type WorkflowLifecyclePolicyResolution =
  | { ok: true; policy: WorkflowLifecyclePolicy }
  | { ok: false; field: string; requested: string; limit: string };

type LifecycleLimitConflict = Extract<
  WorkflowLifecyclePolicyResolution,
  { ok: false }
>;

function foldWorkflowLifecyclePolicies(
  policies: readonly WorkflowLifecyclePolicy[],
): { policy: WorkflowLifecyclePolicy; conflict?: LifecycleLimitConflict } {
  const effective: WorkflowLifecyclePolicy = {};
  let conflict: LifecycleLimitConflict | undefined;
  for (const policy of policies) {
    for (const field of [
      "maxLifetime",
      "maxDisconnected",
      "standbyAfter",
    ] as const) {
      const requested = policy[field];
      if (requested === undefined) continue;
      const limit = effective[field];
      if (
        limit !== undefined &&
        lifecycleDurationMs(requested) > lifecycleDurationMs(limit)
      ) {
        conflict ??= { ok: false, field, requested, limit };
        continue;
      }
      effective[field] = requested;
    }
    for (const status of ["completed", "failed", "cancelled"] as const) {
      const requested = policy.capacityRetention?.[status];
      if (requested === undefined) continue;
      const limit = effective.capacityRetention?.[status];
      if (
        limit !== undefined &&
        lifecycleDurationMs(requested) > lifecycleDurationMs(limit)
      ) {
        conflict ??= {
          ok: false,
          field: `capacityRetention.${status}`,
          requested,
          limit,
        };
        continue;
      }
      effective.capacityRetention ??= {};
      effective.capacityRetention[status] = requested;
    }
  }
  return conflict === undefined
    ? { policy: effective }
    : { policy: effective, conflict };
}

/** Resolve root-to-leaf policies; a descendant can only shorten an inherited limit. */
export function resolveWorkflowLifecyclePolicy(
  policies: readonly WorkflowLifecyclePolicy[],
): WorkflowLifecyclePolicyResolution {
  const { policy, conflict } = foldWorkflowLifecyclePolicies(policies);
  return conflict ?? { ok: true, policy };
}

/**
 * Resolve root-to-leaf policies, capping any value above an inherited limit.
 * An ancestor tightened after a descendant was saved must not strand it.
 */
export function clampWorkflowLifecyclePolicy(
  policies: readonly WorkflowLifecyclePolicy[],
): WorkflowLifecyclePolicy {
  return foldWorkflowLifecyclePolicies(policies).policy;
}
