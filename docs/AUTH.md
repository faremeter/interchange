# Authentication and Authorization

## Overview

Interchange uses a unified principal model where users and agents share the same authorization system. Authentication establishes global identity. Authorization is tenant-scoped and evaluated through capability grants attached to principals.

## Authentication

Authentication is handled by better-auth and is tenant-independent. A user authenticates once and receives a session token. That token identifies them globally -- it does not imply any tenant context or authorization.

Supported auth methods:

- Email and password
- Google OAuth

## Tenant Context

A user can belong to many tenants. Tenant context is always encoded in the URL path as `/api/tenants/:tenantId/...` -- no headers, no implicit context.

| Path scope                 | Example                        | Behavior                                                                             |
| -------------------------- | ------------------------------ | ------------------------------------------------------------------------------------ |
| Tenant-scoped              | `/api/tenants/tnt_abc/agents`  | Principal resolved from `(user_id, tnt_abc)`, grants evaluated                       |
| User-scoped (cross-tenant) | `/api/me/principals`           | All of the user's principals resolved, results aggregated and tagged with `tenantId` |
| Global                     | `/api/tenants`, `/api/auth/**` | No tenant context needed                                                             |

## Principals

A principal represents an identity within a tenant. It is the universal join between an entity (user or agent) and a tenant. A principal does not grant authorization by itself -- it establishes that an entity exists in a tenant and tracks their membership status.

A user in three tenants has three principal rows. An agent in a tenant has one principal row. Every authorization question starts by resolving the principal.

```
principal
  id              text PK        -- prn_...
  tenant_id       text FK -> tenant
  kind            text NOT NULL  -- 'user' | 'agent'
  ref_id          text NOT NULL  -- user.id or agent id (run_...)
  status          text NOT NULL  -- 'active' | 'suspended' | 'invited' | 'deactivated'
  created_at      timestamptz
  updated_at      timestamptz
  UNIQUE(tenant_id, kind, ref_id)
```

### Request resolution flow

```
Request to /api/tenants/:tenantId/...
  -> better-auth validates session token -> user ID
  -> extract tenant_id from URL path
  -> resolve principal for (user_id, tenant_id) -> principal ID + status
  -> if no principal row exists: 403 "not a member of this tenant"
  -> if status != active: 403 "membership suspended/deactivated"
  -> collect capability grants for this principal (direct + role-based)
  -> evaluate grants against the requested operation
  -> allow / deny / ask

Request to /api/me/...
  -> better-auth validates session token -> user ID
  -> resolve all principals for user_id
  -> aggregate results across tenants, each tagged with tenantId
```

Agent requests follow the same flow. The principal is resolved by `(run_id, tenant_id)` rather than `(user_id, tenant_id)`. The agent's materialized grants are evaluated against the requested operation.

## Roles

Roles are named bundles of capability grants scoped to a tenant. Both users and agents can be assigned roles.

```
role
  id              text PK        -- rol_...
  tenant_id       text FK -> tenant
  name            text NOT NULL
  description     text
  is_system       bool DEFAULT false
  created_at      timestamptz
  updated_at      timestamptz
  UNIQUE(tenant_id, name)
```

System roles (owner, admin, member) are created automatically when a tenant is created. They cannot be deleted or renamed. Tenant admins can define additional custom roles.

```
principal_role
  principal_id    text FK -> principal
  role_id         text FK -> role
  created_at      timestamptz
  PRIMARY KEY(principal_id, role_id)
```

## Grant Requirements on Definitions

Agent definitions declare grant requirements — the capabilities an agent needs to function. Requirements are not live grants. They are a manifest that the control plane resolves at launch time to produce materialized grants on the agent's principal. This mirrors the credential requirement model described in CREDENTIALS.md.

Each requirement specifies:

```
{
  resource: string     -- glob pattern: "tool:bash", "wallet:*", etc.
  action: string       -- "invoke", "spend", "read", etc.
  source: "creator" | "invoker"
  effect?: "allow" | "ask" | "deny"    -- default: "allow"
  conditions?: object  -- optional constraints
}
```

The `source` field declares where the delegated authority should come from:

- `source: "creator"` — The definition author must delegate this. Resolved at launch against the creator's own grants (identified by `creatorPrincipalId` on the definition). The control plane validates that the creator currently holds the authority being delegated — a creator cannot delegate what they don't have. Materializes as a `grant` with `origin = 'creator'`. This is the setuid model: the definition author's authority travels with the definition.
- `source: "invoker"` — The person launching the agent must provide this. Resolved at launch against the invoker's grants. Materializes as a `grant` with `origin = 'invoker'` and a fixed 24-hour `expires_at` (`INVOKER_GRANT_TTL_MS`).

Tenant-owned credential use is not a `source` on a grant requirement: it is authorized by ownership at resolution, and its consumer-scoping `credential:{id}` / `use` grant (`origin = 'system'`) is stamped directly rather than materialized from a requirement. See CREDENTIALS.md.

### Creator Tracking

The definition stores a `creatorPrincipalId` field identifying the definition author's principal. Creator-sourced requirements resolve against this principal at every launch. If the original creator leaves the organization, ownership can be transferred to another principal. Without transfer, the definition becomes un-launchable for any creator-sourced requirements (the control plane cannot validate delegation authority).

### Resolution at Launch

When an agent is launched, the control plane processes each grant requirement:

1. Look at the `source` field
2. Resolve the authority against the requirement's `source`: the creator's or the invoker's own grants
3. Confirm the delegating principal holds the capability being delegated
4. Create a `grant` row on the agent's new principal with the appropriate `origin` value
5. Ship the effective grant set to the harness in the deploy frame

The `initialGrants` field on `CreateAgent` is a grant requirements manifest — it specifies requirements with source annotations, not live grants. Each launch resolves these requirements against the current state of creator, tenant, and invoker authority.

## Definition Approval and Re-Verification

A code-sourced workflow definition is approved by **content hash**, not by asset id. Approval computes `computeWireDefinitionHash` over the definition's **inert wire projection** (`projectLiveToInert` — tool factories reified to plain data, model sources canonicalized to `(provider, model)`, credentials stripped) and records that hash as the version's approved identity. The inert projection is a deliberately non-executable approval surface; the runtime evaluates the pinned code to the live definition and runs that.

### The re-verify barrier

At every definition **load boundary** — the top-level run (fresh, resumed, or restored), and each referenced-definition spawn — the loader recomputes the wire hash and refuses to proceed unless it matches the approved hash. A mismatch throws; there is no fallback. This catches a definition that diverged from what was approved (a redeploy/resume divergence, or a post-approval mutation of the on-disk asset that the substrate's hub-writes / sidecar-reads authorization does not itself cover for a direct filesystem write).

**The barrier is load-bearing only where the approved hash arrives _out-of-band_ from the bytes being checked** — from the hub's signed deploy frame or the child's spawn env, which a filesystem-level tamperer of the on-disk definition cannot also forge. Where no such out-of-band pin exists, a gate could only fail-closed-always, so it is deliberately absent and the definition's integrity rests on the asset repo's write-authorization plus push-time envelope validation. Applying this rule per load site:

- **Source-ref closure evaluation** — the airlock. Author code sourced from a registry is evaluated to a live definition and re-verified by project-then-hash against `SpawnTimeEnv.definitionHash`. Load-bearing; this is the whole point of the content-hash approval.
- **Top-level inert read** — re-verified against `SpawnTimeEnv.definitionHash` (the hub-approved hash on the signed frame). Load-bearing.
- **onTrigger bodies** — a body is a section extracted from the parent's own approved definition, so the parent's approval carries the body's hash on the signed frame (`referencedDefinitionHashes[bodyId]`). The body spawn re-verifies against it. Load-bearing.
- **childWorkflow spawns** — a `childWorkflow{definitionRef}` references a **separately-approved** workflow asset by id; the parent holds no hash for it and has no authority over it. There is no out-of-band pin, so the in-process spawn reads, envelope-validates, **and runs** the inert definition **without** a re-verify gate — there is no re-verify step between the read and execution on this path, so nothing here guards execution against post-approval drift. The child asset re-verifies against **its own** approved hash only when it is itself deployed as a top-level run (a separate execution), not from a parent that merely references it.

Both hub-approved pins (the top-level `approvedWireHash` and the per-body `referencedDefinitionHashes`) are persisted on the sidecar's deployment record so that a **sidecar restart** re-threads them into the restored child's spawn env and the same barriers hold across the restart — rather than a restored definition or body failing closed for want of a hash.

## Capability Grants

Capability grants are the atomic unit of authorization. Every authorization decision is resolved by evaluating grants. Grants can be attached to a role (applying to all principals with that role) or directly to a principal.

```
grant
  id              text PK        -- grt_...
  tenant_id       text FK -> tenant

  -- Target: who receives this grant. Exactly one of role_id / principal_id is
  -- non-null -- enforced by the grant_target_exactly_one CHECK constraint
  -- (num_nonnulls(principal_id, role_id) = 1), not merely a write-path
  -- convention.
  role_id         text FK -> role
  principal_id    text FK -> principal

  -- What is being authorized
  resource        text NOT NULL  -- glob pattern: "agent:*", "wallet:wal_abc", "tool:bash"
  action          text NOT NULL  -- glob pattern: "invoke", "read", "spend", "*"
  effect          text NOT NULL  -- 'allow' | 'deny' | 'ask'

  -- Constraints
  conditions      jsonb          -- e.g. { "time_window": { "after": "09:00", "before": "17:00", "timezone": "America/Los_Angeles" } }

  -- Provenance
  origin          text NOT NULL  -- 'system' | 'role' | 'creator' | 'invoker'
  expires_at      timestamptz    -- null = permanent

  created_at      timestamptz
  updated_at      timestamptz
```

### Resource and action patterns

Resources use a `type:identifier` format with glob support:

- `agent:*` -- all agents
- `agent:agt_abc123` -- a specific agent
- `wallet:wal_*` -- all wallets
- `tool:bash` -- the bash tool
- `tool:*` -- all tools
- `credential:crd_stripe` -- a specific credential
- `api:stripe:*` -- all Stripe API operations
- `*` -- everything

Actions are operation verbs:

- `invoke` -- call/execute
- `read` -- view/list
- `create` -- create new resources
- `spend` -- financial operations
- `manage` -- update/delete/configure
- `*` -- all actions

### Evaluation

For a given principal attempting an operation:

1. Collect all grants: direct grants on the principal + grants from all assigned roles.
2. Filter to grants matching the resource and action patterns.
3. Order by specificity (more specific patterns beat less specific), and at equal specificity by effect priority (`deny` > `ask` > `allow`).
4. The strongest matching grant under that ordering wins.
5. A no match returns no decision (`effect: null`), not an explicit `deny`; the enforcement layer fails closed when no grant allows the action.

The `ask` effect blocks execution and surfaces an approval request to the appropriate human. The human can respond with `once` (allow this instance), `always` (create a persistent grant), or `reject` (deny with optional feedback).

### Conditions

The `conditions` JSONB field constrains when a grant applies. A condition key is evaluated only if an evaluator is registered for it; today the sole registered evaluator is `time_window`:

- `{ "time_window": { "after": "09:00", "before": "17:00", "timezone": "America/Los_Angeles" } }` -- time-based access (implemented). `timezone` is required; `after` and `before` are `HH:MM` in 24-hour format.

Spending limits and threshold-based escalation are planned conditions with no evaluator yet:

- `{ "max_spend_per_day": 100, "currency": "USD" }` -- spending limits (not yet available).
- `{ "require_approval_above": 50 }` -- threshold-based escalation (not yet available).

Conditions are evaluated at runtime by the authorization engine: a grant whose registered condition is unmet is skipped, but a grant carrying a condition key with no registered evaluator throws and fails the authorization to surface the misconfiguration. Do not author grants with the planned conditions above until their evaluators ship — a grant referencing an unregistered condition will fail authorization with an error rather than silently passing.

## Grant Revocation

Grant revocation is policy-driven with a default of fail-secure.

**Creator grant revocation**: If the creator's authority is revoked after agents have been launched with creator-sourced grants, running agents must lose the affected grants. The harness authorizes each tool call against its live materialized grant set before the call executes, so a revoked capability is blocked once that set no longer contains it; a tool call already in flight is not interrupted. Propagating a revocation to an already-running deployment is not currently implemented — the earlier `grants.update` wire mechanism has been retired, and its supervised replacement is designed separately. Until then, the change takes effect when the deployment next loads its grants (the deploy pack at spawn and the supervisor's IPC credentials snapshot at recycle). Tenants can configure grace periods or notification-only behavior for specific grant types.

**Invoker grant revocation**: Invoker-granted capabilities carry a fixed 24-hour TTL from launch (`INVOKER_GRANT_TTL_MS`), not a lifetime tied to when the agent stops. A capability the user explicitly persists (approving with `always`) becomes a non-expiring grant.

**Tenant policy changes**: When tenant policies change (role modifications, system role updates), the control plane re-evaluates affected agents. Propagating the resulting grant changes to already-running deployments shares the retired-mechanism gap described above; a change takes effect when each deployment next loads its grants.

This parallels the credential revocation model described in CREDENTIALS.md — both follow the same fail-secure default with configurable tenant policies.

## Smart-HTTP Git Tokens

The hub exposes asset and agent-state repositories over the smart-HTTP wire (`info/refs`, `git-upload-pack`, `git-receive-pack`). Stock `git` clients authenticate to those endpoints with an opaque bearer token rather than a better-auth session cookie. See `docs/GIT_ACCESS.md` for the operator walkthrough — credential-helper setup, URL grammar, `refPattern` grammar, and worked clone/push examples.

### Token model

Tokens are minted as plaintext strings of the form `itx_pat_<base64>` (personal access) or `itx_svc_<base64>` (tenant-bound service token). The hub stores only the token's SHA-256 digest in `git_token.token_hash_sha256`; the plaintext is returned exactly once in the mint response and never persisted. There is no recovery flow — a lost token is revoked and replaced.

Every token row is owned by a user (`user_id`). `kind: "pat"` is user-scoped and may optionally restrict to a single tenant (`tenant_id` non-null) or remain cross-tenant (`tenant_id` null). `kind: "svc"` is always tenant-bound and additionally carries a `principal_id` so the token speaks as a specific tenant member.

```
git_token
  id                    text PK        -- gtk_...
  tenant_id             text FK -> tenant (nullable; non-null for kind = 'svc')
  user_id               text FK -> user  NOT NULL
  principal_id          text FK -> principal (nullable; set for kind = 'svc')
  name                  text NOT NULL
  kind                  text NOT NULL   -- 'pat' | 'svc'
  token_hash_sha256     bytea NOT NULL UNIQUE
  resource              text NOT NULL   -- 'asset:*', 'asset:def_xxx', 'agent-state:run_xxx', ...
  ref_pattern           text NOT NULL   -- simple-glob
  actions               text[] NOT NULL -- RepoActions
  expires_at            timestamptz NOT NULL
  revoked_at            timestamptz     -- soft revocation
  created_at            timestamptz
  UNIQUE(user_id, name) WHERE revoked_at IS NULL
```

The partial unique on `(user_id, name)` filtered by `revoked_at IS NULL` lets a user reuse a friendly name (e.g. `"laptop"`) after revoking the old token bearing that name.

### Scoping claims

Three columns bound a token's authority:

- `resource` — a single substrate authz resource string, e.g. `asset:*`, `asset:def_xxx`, `agent-state:run_xxx`. Glob patterns are honored by the substrate; a token with `resource: "asset:*"` reaches every asset row in the tenant the token is bound to.
- `ref_pattern` — a glob restricting which refs within the resource the token may read or write. Grammar: `*` matches within a `/`-segment, `**` crosses segments. Worked examples appear in `docs/GIT_ACCESS.md`.
- `actions` — the `RepoAction` vocabulary the token is allowed to invoke (`receivePack`, `createPack`, `resolveRef`, ...). The mint API accepts the user-facing aliases `can_read` (expands to `["createPack", "resolveRef"]`) and `can_push` (expands to `["receivePack"]`), and stores the canonical names so the lookup path never re-runs the alias table.
- `expires_at` — required, server-enforced floor of one minute. The bearer middleware checks `expires_at > now()` on every request.

### Composition with the grant model

Tokens and grants are independent authorization layers — **both must allow** the operation.

| Layer       | Vocabulary                                                     | Scope                   | Resolved at                            |
| ----------- | -------------------------------------------------------------- | ----------------------- | -------------------------------------- |
| Grant       | grant verbs (`read`, `write`, `create`, ...)                   | Tenant-scoped principal | Request time, by substrate `authorize` |
| Token claim | `RepoAction`s (`createPack`, `receivePack`, `resolveRef`, ...) | Token row               | Mint time, checked at request time     |

The bearer middleware translates the inbound smart-HTTP request to a `RepoAction` via `httpToRepoAction`, then to a grant verb via `repoActionToGrantVerb` (e.g. `createPack` → `read`, `receivePack` → `write`). The verb is what `authorize` evaluates against the resolved principal's grants. The `RepoAction` is what gets checked against the token's `actions` claim. A request passes only when both layers agree.

This composition is deliberate. A token cannot grant authority the underlying principal does not have — narrowing only. And a principal with broad grants cannot accidentally exercise them through a token whose `actions` claim does not cover the operation. The narrower of the two layers always wins.

### Revocation

`DELETE /api/me/git-tokens/:tokenId` (for personal tokens) and `DELETE /api/tenants/:tenantId/git-tokens/:tokenId` (for service tokens) set `revoked_at`. The bearer middleware returns `403` with `code: "token_revoked"` on the next request bearing that secret. The row is preserved for audit.

Token revocation is independent of grant revocation. Revoking the underlying principal's grant denies the operation through the grant layer; revoking the token denies it through the token layer. Either is sufficient; operators choose the layer that matches the intent (revoke the principal's authority entirely, or just this token).

## Mapping to Interchange Concepts

This table shows how Interchange authorization concepts map to materialized grant forms in `grant`. Definitions carry requirements (see Grant Requirements on Definitions above); this is what those requirements produce after resolution at launch.

| Interchange concept          | Implementation                                                                                                                                                                                                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Creator-granted capabilities | Grant requirements with `source: "creator"`, resolved against creator's principal, materialized with `origin = 'creator'`                                                                                                                                                             |
| Invoker-granted capabilities | Grant requirements with `source: "invoker"`, resolved against invoker's principal, materialized with `origin = 'invoker'`, a fixed 24-hour `expires_at`                                                                                                                               |
| Tool-call gates              | Grants where `resource = 'tool:...'`                                                                                                                                                                                                                                                  |
| Wallet access                | Grants where `resource = 'wallet:...'`, `action = 'spend'` (spending-limit conditions are planned — see Conditions)                                                                                                                                                                   |
| Credential use               | A tenant-owned credential's use is authorized by ownership at resolution (no grant requirement); a consumer-scoping `credential:{id}` / `use` grant with `origin = 'system'` is stamped for the runtime gate. Principal-owned credential use is not yet supported. See CREDENTIALS.md |
| User roles (RBAC)            | Grants attached to roles, roles assigned to user principals                                                                                                                                                                                                                           |
| Human approval gates         | Grants with `effect = 'ask'`                                                                                                                                                                                                                                                          |
| Agent delegation chain       | Child agent's grants are a subset of parent agent's grants, enforced at launch time                                                                                                                                                                                                   |

## Personal Tenant

On user registration:

1. Create a tenant with a slug derived from the username.
2. Create a principal for the user in that tenant (`kind = 'user'`).
3. Assign the system `owner` role.
4. The owner role includes a broad default grant: `resource = "*", action = "*", effect = "allow"`.

The personal tenant has the same authorization machinery as any other tenant. When the user creates agents there, those agents get principals and grants through the same system. The onboarding UX is simple, but the underlying model is uniform.

## Tenant Schema

```
tenant
  id              text PK        -- tnt_...
  name            text NOT NULL
  slug            text UNIQUE
  domain          text UNIQUE    -- SMTP domain (slug.interchange.network)
  parent_id       text FK -> tenant (nullable, for hierarchy)
  config          jsonb
  created_at      timestamptz
  updated_at      timestamptz
```

Tenants can be organized hierarchically. Child tenants inherit policies from their parent (additive restrictions only). Federation between sibling tenants requires explicit trust establishment.
