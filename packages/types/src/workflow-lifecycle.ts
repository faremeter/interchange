import { type } from "arktype";

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
    milliseconds <= 8_640_000_000_000_000
    ? milliseconds
    : null;
}

export const LifecycleDuration = type("string").narrow(
  (value, ctx) =>
    parseDuration(value) !== null ||
    ctx.mustBe("a non-negative whole duration ending in s, m, h, or d"),
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

export const WorkflowLifecyclePolicy = type({
  "maxLifetime?": LifecycleDuration.narrow(
    (value, ctx) =>
      lifecycleDurationMs(value) > 0 || ctx.mustBe("greater than zero"),
  ),
  "capacityRetention?": type({
    "completed?": LifecycleDuration,
    "failed?": LifecycleDuration,
    "cancelled?": LifecycleDuration,
  }).onUndeclaredKey("reject"),
}).onUndeclaredKey("reject");
export type WorkflowLifecyclePolicy = typeof WorkflowLifecyclePolicy.infer;

export type WorkflowLifecyclePolicyResolution =
  | { ok: true; policy: WorkflowLifecyclePolicy }
  | { ok: false; field: string; requested: string; limit: string };

/** Resolve root-to-leaf policies; a descendant can only shorten an inherited limit. */
export function resolveWorkflowLifecyclePolicy(
  policies: readonly WorkflowLifecyclePolicy[],
): WorkflowLifecyclePolicyResolution {
  const effective: WorkflowLifecyclePolicy = {};
  for (const policy of policies) {
    if (policy.maxLifetime !== undefined) {
      if (
        effective.maxLifetime !== undefined &&
        lifecycleDurationMs(policy.maxLifetime) >
          lifecycleDurationMs(effective.maxLifetime)
      ) {
        return {
          ok: false,
          field: "maxLifetime",
          requested: policy.maxLifetime,
          limit: effective.maxLifetime,
        };
      }
      effective.maxLifetime = policy.maxLifetime;
    }
    for (const status of ["completed", "failed", "cancelled"] as const) {
      const requested = policy.capacityRetention?.[status];
      if (requested === undefined) continue;
      const limit = effective.capacityRetention?.[status];
      if (
        limit !== undefined &&
        lifecycleDurationMs(requested) > lifecycleDurationMs(limit)
      ) {
        return {
          ok: false,
          field: `capacityRetention.${status}`,
          requested,
          limit,
        };
      }
      effective.capacityRetention ??= {};
      effective.capacityRetention[status] = requested;
    }
  }
  return { ok: true, policy: effective };
}
