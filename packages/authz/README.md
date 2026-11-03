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
import {
  authorize,
  createInMemoryGrantStore,
  timeWindowEvaluator,
} from "@intx/authz";

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
  { conditions: { time_window: timeWindowEvaluator } },
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
  delegates here. Every call names its `ConditionMode`: a
  `ConditionRegistry` evaluates each condition, and `"pessimistic"`
  applies a conditioned `deny` or `ask` as if it held and skips a
  conditioned `allow`, for a check that has no request.
- `createConditionEvaluator` — build a `ConditionEvaluator` from a
  condition name, an arktype schema for the condition body, and an
  evaluation callback. The evaluator's `validate` reports what the
  schema rejects, and its `evaluate` parses with the same schema and
  throws on the same input, so a boundary check and evaluation share
  one set of shape rules.
- `timeWindowEvaluator` — a `ConditionEvaluator` for time-of-day
  windows. Register it under the `time_window` key in a
  `ConditionRegistry` to gate grants on `{ after, before, timezone }`;
  cross-midnight windows are supported.
- `toolConsumerEvaluator` — a `ConditionEvaluator` for credential
  consumers. Register it under the `tool` key to narrow a grant to one
  consumer identity.
- `HUB_CONDITION_REGISTRY` — the hub's standard `ConditionRegistry`,
  registering `time_window` and `tool`.
- `RUN_CHILD_CONDITION_REGISTRY` — the `ConditionRegistry` a workflow
  run's child process evaluates step grants with: `time_window` only,
  a subset of the hub registry.
- `evaluateConditions` — run a grant's `conditions` object against a
  `ConditionRegistry`.
- `validateConditions` — check a grant's `conditions` object against a
  `ConditionRegistry` without evaluating it, returning one problem per
  unregistered key or rejected body. A boundary that stores conditions
  calls it to refuse what `evaluateConditions` would later throw on.
- `validateGrantConditions` — check a grant's conditions against every
  registry that will evaluate it: the hub registry, and for a row on a
  workflow run's principal also the run child registry and the
  credential-use registry, where the row's patterns reach them.
- `createInMemoryGrantStore` — a `GrantStore` backed by an in-memory
  grant list for tests and single-process deployments.
- `matchPattern`, `patternSpecificity`, `grantSpecificity` — the
  colon-segmented wildcard matcher and the specificity scoring used to
  pick the winning grant.
- Types: `AuthzResult`, `ConditionContext`, `ConditionEvaluator`,
  `ConditionMode`, `ConditionRegistry`, `Effect`, `GrantConditionTarget`,
  `GrantRule`, `GrantStore`, `MatchedGrant`.
