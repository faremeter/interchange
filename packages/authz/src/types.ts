export type {
  ConditionContext,
  ConditionEvaluator,
  ConditionRegistry,
  Effect,
  GrantRule,
  GrantStore,
} from "@interchange/types/authz";

import type { Effect } from "@interchange/types/authz";

export type MatchedGrant = {
  id: string;
  resource: string;
  action: string;
  effect: Effect;
  origin: "system" | "role" | "creator" | "invoker";
  specificity: number;
};

export type AuthzResult = {
  /** The resolved effect, or null if no grants matched at all. */
  effect: Effect | null;
  /** All grants that matched the resource/action query. */
  matchingGrants: MatchedGrant[];
  /** The specific grant that determined the outcome, if any. */
  resolvedBy: MatchedGrant | null;
};
