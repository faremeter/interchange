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
| User-scoped (cross-tenant) | `/api/me/agents`               | All of the user's principals resolved, results aggregated and tagged with `tenantId` |
| Global                     | `/api/tenants`, `/api/auth/**` | No tenant context needed                                                             |

## Principals

A principal represents an identity within a tenant. It is the universal join between an entity (user or agent) and a tenant. A principal does not grant authorization by itself -- it establishes that an entity exists in a tenant and tracks their membership status.

A user in three tenants has three principal rows. An agent in a tenant has one principal row. Every authorization question starts by resolving the principal.

```
principal
  id              text PK        -- prn_...
  tenant_id       text FK -> tenant
  kind            text NOT NULL  -- 'user' | 'agent'
  ref_id          text NOT NULL  -- user.id or agent id (ins_...)
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

Agent requests follow the same flow. The principal is resolved by `(ins_id, tenant_id)` rather than `(user_id, tenant_id)`. The agent's materialized grants are evaluated against the requested operation.

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
  source: "tenant" | "creator" | "invoker"
  effect?: "allow" | "ask" | "deny"    -- default: "allow"
  conditions?: object  -- optional constraints
}
```

The `source` field declares where the authority should come from:

- `source: "tenant"` — The tenant's organizational policies must allow this. Resolved from system roles and tenant-configured role grants, walking up the tenant hierarchy. Materializes as a `grant` with `source = 'system'` or `source = 'role'`.
- `source: "creator"` — The definition author must delegate this. Resolved at launch against the creator's own grants (identified by `creatorPrincipalId` on the definition). The control plane validates that the creator currently holds the authority being delegated — a creator cannot delegate what they don't have. Materializes as a `grant` with `source = 'creator'`. This is the setuid model: the definition author's authority travels with the definition.
- `source: "invoker"` — The person launching the agent must provide this. Resolved at launch against the invoker's grants. Materializes as a `grant` with `source = 'invoker'` and a short `expires_at` (session-scoped to the agent's lifetime).

### Creator Tracking

The definition stores a `creatorPrincipalId` field identifying the definition author's principal. Creator-sourced requirements resolve against this principal at every launch. If the original creator leaves the organization, ownership can be transferred to another principal. Without transfer, the definition becomes un-launchable for any creator-sourced requirements (the control plane cannot validate delegation authority).

### Resolution at Launch

When an agent is launched, the control plane processes each grant requirement:

1. Look at the `source` field
2. Resolve against the appropriate principal (tenant policies, creator's grants, or invoker's grants)
3. Validate that the source has the authority to delegate
4. Create a `grant` row on the agent's new principal with the appropriate `source` value
5. Ship the effective grant set to the harness in the deploy frame

The `initialGrants` field on `CreateAgent` is a grant requirements manifest — it specifies requirements with source annotations, not live grants. Each launch resolves these requirements against the current state of creator, tenant, and invoker authority.

## Capability Grants

Capability grants are the atomic unit of authorization. Every authorization decision is resolved by evaluating grants. Grants can be attached to a role (applying to all principals with that role) or directly to a principal.

```
grant
  id              text PK        -- grt_...
  tenant_id       text FK -> tenant

  -- Target: who receives this grant (exactly one is non-null)
  role_id         text FK -> role
  principal_id    text FK -> principal

  -- What is being authorized
  resource        text NOT NULL  -- glob pattern: "agent:*", "wallet:wal_abc", "tool:bash"
  action          text NOT NULL  -- glob pattern: "invoke", "read", "spend", "*"
  effect          text NOT NULL  -- 'allow' | 'deny' | 'ask'

  -- Constraints
  conditions      jsonb          -- e.g. { "max_spend_per_day": 100, "currency": "USD" }

  -- Provenance
  source          text NOT NULL  -- 'system' | 'role' | 'creator' | 'invoker'
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
3. Order by specificity (more specific patterns beat less specific).
4. Last matching grant wins.
5. No match defaults to `deny` (fail-closed).

The `ask` effect blocks execution and surfaces an approval request to the appropriate human. The human can respond with `once` (allow this instance), `always` (create a persistent grant), or `reject` (deny with optional feedback).

### Conditions

The `conditions` JSONB field constrains when a grant applies:

- `{ "max_spend_per_day": 100, "currency": "USD" }` -- spending limits
- `{ "time_window": { "start": "09:00", "end": "17:00" } }` -- time-based access
- `{ "require_approval_above": 50 }` -- threshold-based escalation

Conditions are evaluated at runtime by the authorization engine. A grant with unmet conditions is skipped during evaluation.

## Grant Revocation

Grant revocation is policy-driven with a default of fail-secure.

**Creator grant revocation**: If the creator's authority is revoked after agents have been launched with creator-sourced grants, running agents lose the affected grants immediately. The control plane pushes a `grants.update` frame to the harness, which re-evaluates its materialized grants and stops exercising revoked capabilities regardless of in-flight work. Tenants can configure grace periods or notification-only behavior for specific grant types.

**Invoker grant revocation**: Invoker-granted capabilities expire when the agent stops unless explicitly persisted. They are session-scoped by default.

**Tenant policy changes**: When tenant policies change (role modifications, system role updates), the control plane re-evaluates affected agents and pushes grant updates to their harnesses.

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
  resource              text NOT NULL   -- 'asset:*', 'asset:def_xxx', 'agent-state:ins_xxx', ...
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

- `resource` — a single substrate authz resource string, e.g. `asset:*`, `asset:def_xxx`, `agent-state:ins_xxx`. Glob patterns are honored by the substrate; a token with `resource: "asset:*"` reaches every asset row in the tenant the token is bound to.
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

| Interchange concept          | Implementation                                                                                                                                |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Tenant-granted capabilities  | Grant requirements with `source: "tenant"`, resolved from tenant policies, materialized with `source = 'system'` or `source = 'role'`         |
| Creator-granted capabilities | Grant requirements with `source: "creator"`, resolved against creator's principal, materialized with `source = 'creator'`                     |
| Invoker-granted capabilities | Grant requirements with `source: "invoker"`, resolved against invoker's principal, materialized with `source = 'invoker'`, short `expires_at` |
| Tool-call gates              | Grants where `resource = 'tool:...'`                                                                                                          |
| Wallet access                | Grants where `resource = 'wallet:...'`, `action = 'spend'`, with spending limit conditions                                                    |
| Credential access via grant  | Grants where `resource = 'credential:...'`, `action = 'use'`. See also CREDENTIALS.md for the credential requirement model                    |
| User roles (RBAC)            | Grants attached to roles, roles assigned to user principals                                                                                   |
| Human approval gates         | Grants with `effect = 'ask'`                                                                                                                  |
| Agent delegation chain       | Child agent's grants are a subset of parent agent's grants, enforced at launch time                                                           |

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
