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
  granted_by      text FK -> principal
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

- `source: "tenant"` — The tenant's organizational policies must allow this. Resolved from system roles and tenant-configured role grants, walking up the tenant hierarchy. Materializes as a `capability_grant` with `source = 'system'` or `source = 'role'`.
- `source: "creator"` — The definition author must delegate this. Resolved at launch against the creator's own grants (identified by `creatorPrincipalId` on the definition). The control plane validates that the creator currently holds the authority being delegated — a creator cannot delegate what they don't have. Materializes as a `capability_grant` with `source = 'creator'`. This is the setuid model: the definition author's authority travels with the definition.
- `source: "invoker"` — The person launching the agent must provide this. Resolved at launch against the invoker's grants. Materializes as a `capability_grant` with `source = 'invoker'` and a short `expires_at` (session-scoped to the agent's lifetime).

### Creator Tracking

The definition stores a `creatorPrincipalId` field identifying the definition author's principal. Creator-sourced requirements resolve against this principal at every launch. If the original creator leaves the organization, ownership can be transferred to another principal. Without transfer, the definition becomes un-launchable for any creator-sourced requirements (the control plane cannot validate delegation authority).

### Resolution at Launch

When an agent is launched, the control plane processes each grant requirement:

1. Look at the `source` field
2. Resolve against the appropriate principal (tenant policies, creator's grants, or invoker's grants)
3. Validate that the source has the authority to delegate
4. Create a `capability_grant` row on the agent's new principal with the appropriate `source` value
5. Ship the effective grant set to the harness in the deploy frame

The `initialGrants` field on `CreateAgent` is a grant requirements manifest — it specifies requirements with source annotations, not live grants. Each launch resolves these requirements against the current state of creator, tenant, and invoker authority.

## Capability Grants

Capability grants are the atomic unit of authorization. Every authorization decision is resolved by evaluating grants. Grants can be attached to a role (applying to all principals with that role) or directly to a principal.

```
capability_grant
  id              text PK        -- cap_...
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
5. No match defaults to `ask` (escalate to human).

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

## Mapping to Interchange Concepts

This table shows how Interchange authorization concepts map to materialized grant forms in `capability_grant`. Definitions carry requirements (see Grant Requirements on Definitions above); this is what those requirements produce after resolution at launch.

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
