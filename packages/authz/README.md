# @intx/authz

Grant evaluation engine. Given a principal, a tenant, a resource,
and an action, `authorize` collects the relevant grants from a
`GrantStore`, picks the most specific match, applies condition
evaluators, and returns the resolved effect.

Pattern matching uses colon-segmented patterns with `*` and `**`
wildcards; specificity scoring picks the most specific matching
grant; condition evaluators gate matches on runtime facts such as
time windows. An in-memory grant store is included for tests and
single-process deployments; `@intx/db` provides the database-backed
implementation.

Consumed by `@intx/hub-api` for HTTP request authorization and by
`@intx/harness` for tool-call gating.

```ts
import { authorize, createInMemoryGrantStore } from "@intx/authz";

const store = createInMemoryGrantStore([
  {
    id: "g1",
    principalId: "user-1",
    roleId: null,
    effect: "allow",
    origin: "system",
    resource: "tenant:*:agent:*",
    action: "read",
    conditions: null,
    expiresAt: null,
  },
]);

const result = await authorize(
  store,
  "user-1",
  "acme",
  "tenant:acme:agent:abc",
  "read",
);

if (result.effect !== "allow") throw new Error("forbidden");
```

`authorize` returns an `AuthzResult` whose `effect` is one of
`allow`, `deny`, `ask`, or `null` when no grant matches. Callers
translate `null` into a refusal (HTTP 403, tool-call block) because
evaluation is fail-closed.

## Surface

- `authorize` — collect grants from a `GrantStore` and resolve a
  principal/tenant/resource/action query to an `AuthzResult`.
- `evaluateGrants` — the core resolution logic operating on a supplied
  `GrantRule[]` rather than a store, so it can be driven with synthetic
  grant lists. `authorize` is a thin wrapper that collects grants and
  delegates here.
- `timeWindowEvaluator` — a `ConditionEvaluator` for time-of-day
  windows. Register it under the `time_window` key in a
  `ConditionRegistry` to gate grants on `{ after, before, timezone }`;
  cross-midnight windows are supported.
- `evaluateConditions` — run a grant's `conditions` object against a
  `ConditionRegistry`.
- `createInMemoryGrantStore` — a `GrantStore` backed by an in-memory
  grant list for tests and single-process deployments.
- `matchPattern`, `patternSpecificity`, `grantSpecificity` — the
  colon-segmented wildcard matcher and the specificity scoring used to
  pick the winning grant.
- Types: `AuthzResult`, `ConditionContext`, `ConditionEvaluator`,
  `ConditionRegistry`, `Effect`, `GrantRule`, `GrantStore`,
  `MatchedGrant`.
