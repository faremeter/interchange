export { authorize, evaluateGrants } from "./evaluate";
export { matchPattern } from "./patterns";
export { patternSpecificity, grantSpecificity } from "./specificity";
export { evaluateConditions } from "./conditions";
export { timeWindowEvaluator } from "./time-window";
export { createInMemoryGrantStore } from "./memory-store";
export type {
  AuthzResult,
  ConditionContext,
  ConditionEvaluator,
  ConditionRegistry,
  Effect,
  GrantRule,
  GrantStore,
  MatchedGrant,
} from "./types";
