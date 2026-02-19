export { authorize, evaluateGrants } from "./evaluate";
export { matchPattern } from "./patterns";
export { patternSpecificity, grantSpecificity } from "./specificity";
export { evaluateConditions } from "./conditions";
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
