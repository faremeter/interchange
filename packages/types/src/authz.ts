export type Effect = "allow" | "deny" | "ask";

export type GrantRule = {
  id: string;
  resource: string;
  action: string;
  effect: Effect;
  origin: "system" | "role" | "creator" | "invoker";
  conditions: Record<string, unknown> | null;
  expiresAt: Date | null;
  roleId: string | null;
  principalId: string | null;
};

export type GrantStore = {
  collectGrants(principalId: string, tenantId: string): Promise<GrantRule[]>;
};

export type ConditionContext = {
  now: Date;
  resource: string;
  action: string;
  principalId: string;
  tenantId: string;
};

export type ConditionEvaluator = (
  value: unknown,
  ctx: ConditionContext,
) => boolean | Promise<boolean>;

export type ConditionRegistry = Record<string, ConditionEvaluator>;
