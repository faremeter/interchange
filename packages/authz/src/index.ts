export { authorize, evaluateGrants } from "./evaluate";
export type { EvalOptions } from "./evaluate";
export { authorizeAction } from "./authorize-action";
export type { AuthorizeDecision } from "./authorize-action";
export { matchPattern } from "./patterns";
export { patternSpecificity, grantSpecificity } from "./specificity";
export { evaluateConditions } from "./conditions";
export { timeWindowEvaluator } from "./time-window";
export {
  toolConsumer,
  toolConsumerEvaluator,
  CREDENTIAL_USE_CONDITIONS,
} from "./tool-consumer";
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
