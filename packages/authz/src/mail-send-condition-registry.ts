import { timeWindowEvaluator } from "./time-window";
import type { ConditionRegistry } from "./types";

/**
 * The conditions the hub's mail send check can evaluate: only the clock. The
 * check has no consumer, so a `tool` condition could never hold there. The
 * check evaluates `mail.send` grants with this registry, and any other
 * condition key throws there, so the send fails closed.
 */
export const MAIL_SEND_CONDITION_REGISTRY: Readonly<ConditionRegistry> =
  Object.freeze({
    time_window: timeWindowEvaluator,
  });
