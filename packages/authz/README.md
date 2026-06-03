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
