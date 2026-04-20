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
  ref_id          text NOT NULL  -- user.id or agent.id
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

## Mapping to Interchange Concepts

| Interchange concept          | Implementation                                                                                           |
| ---------------------------- | -------------------------------------------------------------------------------------------------------- |
| Creator-granted capabilities | Grants with `source = 'creator'`, attached to the agent's principal                                      |
| Invoker-granted capabilities | Grants with `source = 'invoker'`, attached to the agent's principal, short `expires_at` (session-scoped) |
| Tool-call gates              | Grants where `resource = 'tool:...'`                                                                     |
| Wallet access                | Grants where `resource = 'wallet:...'`, `action = 'spend'`, with spending limit conditions               |
| Credential binding           | Grants where `resource = 'credential:...'`, `action = 'use'`                                             |
| User roles (RBAC)            | Grants attached to roles, roles assigned to user principals                                              |
| Human approval gates         | Grants with `effect = 'ask'`                                                                             |
| Agent delegation chain       | Child agent's grants are a subset of parent agent's grants, enforced at creation time                    |

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
