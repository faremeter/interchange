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
  /**
   * Like `collectGrants`, but unions the principal's grants across the tenant
   * ancestor chain (the acting tenant plus every ancestor up to the root)
   * rather than a single tenant. Only the source-resolution credential-use
   * check uses this: it mirrors the ancestor-chain reach of credential
   * resolution so a `credential:{id}` / `use` grant stamped with an inherited
   * credential's own (ancestor) tenant still authorizes use. The general RBAC
   * path stays on the single-tenant `collectGrants`.
   */
  collectGrantsInChain(
    principalId: string,
    tenantId: string,
  ): Promise<GrantRule[]>;
};

export type ConditionContext = {
  now: Date;
  resource: string;
  action: string;
  principalId: string;
  tenantId: string;
  // Identity of the capability consumer the decision is being made for
  // (e.g. a `tool:<handle>` consumer). Empty when no consumer is in scope;
  // a consumer-scoped condition fails closed against an empty consumer.
  consumer: string;
};

/**
 * A condition's shape check and its evaluation. Both read the same rules, so
 * a boundary that accepts a condition body through `validate` stores only
 * bodies that `evaluate` can parse.
 */
export type ConditionEvaluator = {
  /** Returns null for a valid condition body, otherwise the problem. */
  validate(value: unknown): string | null;
  /** Throws when `validate` would reject the value. */
  evaluate(value: unknown, ctx: ConditionContext): boolean | Promise<boolean>;
};

export type ConditionRegistry = Record<string, ConditionEvaluator>;

/**
 * Whether a grant resource lies inside `namespace`: the bare namespace or any
 * `<namespace>:<...>` resource. A broader grant such as `*` or `mail.*`
 * matches a resource in the namespace under pattern matching but is not in
 * the namespace itself, so a gate that evaluates only this set never lets
 * such a grant decide.
 */
export function isNamespaceResource(
  namespace: string,
  resource: string,
): boolean {
  return resource === namespace || resource.startsWith(`${namespace}:`);
}
