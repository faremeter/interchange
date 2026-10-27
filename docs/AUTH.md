# Authentication and Authorization

## Overview

Interchange uses a unified principal model where users and workflow runs share the same authorization system. Authentication establishes global identity. Authorization is tenant-scoped and evaluated through capability grants attached to principals.

## Authentication

Authentication is handled by better-auth and is tenant-independent. A user authenticates once and receives a session token. That token identifies them globally -- it does not imply any tenant context or authorization.

Supported auth methods:

- Email and password
- Google OAuth

## Tenant Context

A user can belong to many tenants. Tenant context is always encoded in the URL path as `/api/tenants/:tenantId/...` -- no headers, no implicit context.

| Path scope                 | Example                          | Behavior                                                                             |
| -------------------------- | -------------------------------- | ------------------------------------------------------------------------------------ |
| Tenant-scoped              | `/api/tenants/tnt_abc/workflows` | Principal resolved from `(user_id, tnt_abc)`, grants evaluated                       |
| User-scoped (cross-tenant) | `/api/me/principals`             | All of the user's principals resolved, results aggregated and tagged with `tenantId` |
| Global                     | `/api/tenants`, `/api/auth/**`   | No tenant context needed, except a tenant created under a parent (see Tenant Schema) |

## Principals

A principal represents an identity within a tenant. It is the universal join between an entity (user or workflow run) and a tenant. A principal does not grant authorization by itself -- it establishes that an entity exists in a tenant and tracks their membership status.

A user in three tenants has three principal rows. A workflow run in a tenant has one principal row. Every authorization question starts by resolving the principal.

```
principal
  id              text PK        -- prn_...
  tenant_id       text FK -> tenant
  kind            text NOT NULL  -- 'user' | 'agent' | 'workflow'
  ref_id          text NOT NULL  -- user.id, or a workflow_run (run_...) or workflow_definition (wfd_...) id
  status          text NOT NULL  -- 'active' | 'suspended' | 'invited' | 'deactivated'
  created_at      timestamptz
  updated_at      timestamptz
  UNIQUE(tenant_id, kind, ref_id)
```

### Signing keys

The `principal_key` table holds a principal's Ed25519 signing key, giving it a stable cryptographic identity. The public key is a stable handle a verifier can check a signature against; the hub custodies the private key.

```
principal_key
  id            text PK        -- pky_...
  principal_id  text FK -> principal (ON DELETE CASCADE)
  public_key    text NOT NULL UNIQUE  -- hex-encoded 32-byte Ed25519 public key
  private_key   text NOT NULL         -- hex-encoded 32-byte seed, sealed by the cipher
  status        text NOT NULL         -- 'active' | 'retired'
  created_at    timestamptz
  updated_at    timestamptz
  UNIQUE(principal_id) WHERE status = 'active'   -- one active key per principal
```

The private key seed is sealed at rest with the same AEAD cipher that protects credential secrets, bound to its row and column so a sealed key cannot be transplanted to another row. See [CREDENTIALS.md](./CREDENTIALS.md) for the encryption-at-rest mechanics.

Custody is an **attribution** model, not non-repudiation. A signature proves the action came from a holder of the principal's private key, which is enough to attribute the action to the principal within the system. It is **not** proof against the hub operator, who also holds the key and could sign as the principal. Non-repudiation would require the private key to live somewhere the hub cannot reach, which the hub-custodied model does not provide.

Every principal is minted a signing key when it is created, in the same transaction as the principal row, so a principal never exists without a key. The hub seals the seed under `PRINCIPAL_KEY_ENCRYPTION_KEY` (32 bytes, hex) -- a separate key from `CREDENTIAL_ENCRYPTION_KEY`, so the two rotate independently -- and refuses to start without it.

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

### Workflow run tokens

A workflow run calls the tenant routes with a run token instead of a session. A run token is an opaque bearer of the form `itx_run_<base64url>`, held in the `run_token` table as a SHA-256 hash and as a copy sealed with the credential cipher. Each token belongs to one run principal and one consumer: a tool package (`tool:<package>`) or an action handler (`action:<handler>`).

```
Request to /api/tenants/:tenantId/... with Authorization: Bearer itx_run_...
  -> hash the bearer, look up the run_token row; unknown -> 401
  -> resolve the token's principal in the URL tenant; not there -> 403
  -> if the principal is not active (the run ended), or is not a workflow principal: 401
  -> if the route is not on the run-token allow-list: 403
  -> set principal, tenant and the token's consumer; resolveTenant skips the session
  -> evaluate the run principal's grants against the requested operation
```

A request with no run token, or with a git token, takes the session flow above unchanged.

A consumer opts into hub access by declaring the reserved credential handle `interchange.hub` (`HUB_CREDENTIAL_HANDLE`): a tool in `defineTool({ credentials })`, an action handler in `action({ credentials })`. Deploy records the declaring consumers on the deployment's anchor run, across the root and every nested body. When the hub creates the run principal, in the same transaction, it mints one token for each of them and stamps a `credential:hub:<consumer>` / `use` grant scoped to that consumer by a `{ tool }` condition. The hub delivers the tokens on the run's `run.grants` barrier, as a credential delivery that binds the reserved handle to an `http` credential pinned to `HUB_API_ORIGIN`. Every barrier ahead of a trigger mail carries them, including a replay after a reconnect, and the reconnect credential reconcile sends them again. The reconcile never revokes them: a run with no tokens may not have minted them yet, and a deleted token already fails at the hub. The sidecar merges them into the supervisor's credential mirror before it writes the grants. It never writes them in the grants file: it keeps them in the mirror, which it seals in the run record (see _Restoring a run's authority in a new process_). A consumer resolves `interchange.hub` to a mediated credential and calls the hub through it, as with any other credential. Mediation is not containment: a consumer can read the header it sends, so a token that leaks works until its run principal is deactivated.

Every path that deactivates a run principal -- run termination, a stop or cancel, a terminal capacity failure, and `PATCH /principals/:principalId` -- deletes its tokens in the same transaction.

The token's consumer goes into authorization. A grant with a `{ "tool": "<consumer>" }` condition, allow or deny, applies only to requests from that consumer. A session or git token has no consumer, so such a grant never matches it. `POST /api/tenants/:tenantId/principals/:principalId/evaluate` accepts an optional `consumer` to show the decision a consumer's token gets.

The allow-list (`RUN_TOKEN_ROUTES` in `@intx/hub-api`) limits a run token to reading runs, deployments, definitions and assets, deploying, signaling and stopping runs. Every other route refuses a run token whatever grants the run holds. A run therefore cannot resolve approvals, change grants, roles, principals, credentials, OAuth clients, tokens or federation trusts, send REST mail, or manage the tenant.

## Roles

Roles are named bundles of capability grants scoped to a tenant. Both users and workflow runs can be assigned roles.

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

Workflow definitions declare grant requirements — the capabilities a workflow run needs to function. Requirements are not live grants. They are a manifest that the control plane resolves at launch time to produce materialized grants on the run principal. This mirrors the credential requirement model described in CREDENTIALS.md.

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

- `source: "creator"` — The definition author must delegate this. Resolved at launch against the creator's own grants (identified by `creatorPrincipalId` on the definition). The control plane validates that the creator currently holds the authority being delegated — a creator cannot delegate what they don't have. Materializes as a `grant` with `origin = 'creator'`. This is the setuid model: the definition author's authority travels with the definition. A definition sourced from a package-registry tarball is refused at freeze if it declares any creator requirement: the asset's `creatorPrincipalId` names whoever created the registry asset, not whoever published the tarball into it, and anyone holding `asset:<id>` write can publish, so no publisher provenance exists to delegate from. A definition sourced from a `workflow` asset is not refused. A `workflow` asset is its creator's own codebase, and write on it makes the holder a contributor to that codebase, so it delegates the creator's authority: any principal that holds `asset:<id>` write can change the code that a deploy runs with the creator's grants. Grant that write only to principals the creator trusts with that authority.
- `source: "invoker"` — The invoker must provide this. The invoker of record is the deployment's **deployer**: the principal that deployed it, recorded as the launch spec's `sourceAuthorityPrincipalId`. It is not whoever sends the trigger mail, so the trigger path cannot change whose authority a run delegates, and a run that deploys another workflow is that workflow's invoker. Resolved at launch against the deployer's delegatable grants, on the HTTP trigger and the mail-triggered path alike. Materializes as a `grant` with `origin = 'invoker'` and a fixed 24-hour `expires_at` (`INVOKER_GRANT_TTL_MS`). A deployment with invoker-sourced requirements whose deployer is not recorded or no longer active cannot launch a run: the trigger is refused with 409 `deployer_unavailable`. A deployment that a run deployed can outlive that run, so a later first trigger is refused this way once the run has ended. A deployment without invoker-sourced requirements needs no deployer. The hub reads the deployer's grants once, at a run's first trigger. The run keeps the materialized rows until they expire, even when the deployer later loses the grant or is deactivated, and nothing renews them, so an invoker grant on a long-lived run lapses 24 hours after its first trigger. Deploy does not check that the deployer can delegate the invoker requirements; the first trigger refuses a deployer that cannot.

A workflow's `mailAccept` `invoker` rule admits mail from the same principal; see INBOUND_MAIL_POLICY.md.

Delegating to a deployment is a trust decision, as it is for a creator. Every principal that can trigger a run of the deployment starts a run that holds the deployer's delegated grants: a principal with `manage` on its anchor run, and every mail sender that inbound admission accepts.

A requirement can name the run it is materialized for as `workflow-run:self` (`SELF_RUN_RESOURCE` in `@intx/types`, re-exported from `@intx/workflow`). Launch binds it to `workflow-run:<runId>`, the top-level run, which child bodies share, before the authority check. The delegator must therefore hold authority over that run id, for example `workflow-run:*` or the run itself. A deployer holds `workflow-run:<anchor>` / `read` from the deploy, so an invoker-sourced `workflow-run:self` / `read` resolves without a wider grant. The approval records the literal `workflow-run:self`. `self` is valid only as that whole resource. The probe gate refuses a definition whose requirement uses it anywhere else, at install and approve, ignoring case and surrounding whitespace.

Tenant-owned credential use is not a `source` on a grant requirement: it is authorized by ownership at resolution, and its consumer-scoping `credential:{id}` / `use` grant (`origin = 'system'`) is stamped directly rather than materialized from a requirement. See CREDENTIALS.md.

### Creator Tracking

The definition stores a `creatorPrincipalId` field identifying the definition author's principal. Creator-sourced requirements resolve against this principal at every launch. If the original creator leaves the organization, ownership can be transferred to another principal. Without transfer, the definition becomes un-launchable for any creator-sourced requirements (the control plane cannot validate delegation authority). For a definition sourced from a package-registry tarball this principal is the registry asset's creator, not the tarball's publisher, so creator-sourced requirements on such a definition are refused at freeze.

### Resolution at Launch

When a workflow run is launched, the control plane processes each grant requirement:

1. Look at the `source` field
2. Resolve the authority against the requirement's `source`: the creator's or the deployer's own grants
3. Confirm the delegating principal holds the capability being delegated
4. Create a `grant` row on the run's new principal with the appropriate `origin` value
5. Ship the effective grant set to the harness in the deploy frame

The `grantRequirements` field on a workflow definition is a grant requirements manifest — it specifies `GrantRequirement` entries with source annotations, not live grants. Each launch resolves these requirements against the current state of creator, tenant, and invoker authority.

## Definition Approval and Re-Verification

A code-sourced workflow definition is approved by **content hash**, not by asset id. Approval computes `computeWireDefinitionHash` over the definition's **inert wire projection** (`projectLiveToInert` — tool factories reified to plain data, model sources canonicalized to `(provider, model)`, credentials stripped) and records that hash as the version's approved identity. The inert projection is a deliberately non-executable approval surface; the runtime evaluates the pinned code to the live definition and runs that.

A declared plugin package projects as its name plus the tool declarations (name and approval mark) that the closure's copy of the package exports. The definition names only the package, so the probe, the sidecar and the run child each load those declarations from their closure before they project. A plugin version that adds a tool, drops a tool or changes a tool's approval mark therefore changes the wire hash, even when the workflow code is unchanged.

### The re-verify barrier

At every definition **load boundary** — the top-level run (fresh, resumed, or restored), and each referenced-definition spawn — the loader recomputes the wire hash and refuses to proceed unless it matches the approved hash. A mismatch throws; there is no fallback. This catches a definition that diverged from what was approved (a redeploy/resume divergence, or a post-approval mutation of the on-disk asset that the substrate's hub-writes / sidecar-reads authorization does not itself cover for a direct filesystem write).

**The barrier is load-bearing only where the approved hash arrives _out-of-band_ from the bytes being checked** — from the hub's signed deploy frame or the child's spawn env, which a filesystem-level tamperer of the on-disk definition cannot also forge. Where no such out-of-band pin exists, a gate could only fail-closed-always, so it is deliberately absent and the definition's integrity rests on the asset repo's write-authorization plus push-time envelope validation. Applying this rule per load site:

- **Source-ref closure evaluation** — the airlock. Author code sourced from a registry is evaluated to a live definition and re-verified by project-then-hash against `SpawnTimeEnv.definitionHash`. Load-bearing; this is the whole point of the content-hash approval.
- **Top-level inert read** — re-verified against `SpawnTimeEnv.definitionHash` (the hub-approved hash on the signed frame). Load-bearing.
- **onTrigger bodies** — a body is a section extracted from the parent's own approved definition, so the parent's approval carries the body's hash on the signed frame (`referencedDefinitionHashes[bodyId]`). The body spawn re-verifies against it. Load-bearing.
- **childWorkflow spawns** — a `childWorkflow{definitionRef}` references a **separately-approved** workflow asset by id; the parent holds no hash for it and has no authority over it. There is no out-of-band pin, so the in-process spawn reads, envelope-validates, **and runs** the inert definition **without** a re-verify gate — there is no re-verify step between the read and execution on this path, so nothing here guards execution against post-approval drift. The child asset re-verifies against **its own** approved hash only when it is itself deployed as a top-level run (a separate execution), not from a parent that merely references it.

Both hub-approved pins (the top-level `approvedWireHash` and the per-body `referencedDefinitionHashes`) are persisted on the sidecar's deployment record so that a **sidecar restart** re-threads them into the restored child's spawn env and the same barriers hold across the restart — rather than a restored definition or body failing closed for want of a hash.

## The Sidecar Is Not an Authorization Authority

A code-sourced workflow's package code is installed and **evaluated on the sidecar** — untrusted, author-controlled code runs there in an airlocked child to produce the inert projection and the capability walk. So it matters that **the sidecar is not trusted to decrypt credentials or to determine which code runs.** Those are the two things that grant a workflow real power, and both are anchored hub-side:

**Credentials are decrypted and delivered by the hub.** The secrets a consumer (a tool package or a workflow action handler) uses to reach an external system are held encrypted in the tenant vault and decrypted **hub-side** (`buildCredentialDelivery`). The sidecar cannot decrypt the vault. It keeps the deliveries it received in each run record, so a restart can restore the run, and it seals every secret there under its own credential cipher, so no credential is plaintext at rest. The hub decides _which_ credentials to deliver from its **own** content-addressed workflow asset plus the operator's approval — never from anything the sidecar supplies — and each delivered credential grant is scoped `credential:{id}/use` to a specific consumer, resolving only a **tenant-owned** credential. A consumer whose secret the hub did not send is inert.

**The code that runs is the code the operator's approval was bound to.** The dependency closure is hub-resolved and SRI-pinned; at every load boundary the child re-materializes it, re-projects, and re-checks the projection's content hash against the hub-approved hash, failing closed on any mismatch (see _Definition Approval and Re-Verification_ above). A sidecar cannot substitute different code without failing this barrier.

Given those two anchors, the sidecar's **capability walk is advisory, not enforcement.** It exists to show the operator what the workflow appears to need, so the operator's `ApprovalSet` — the single human decision — can gate it at freeze time. A compromised or buggy sidecar can only:

- **over-report** grants → the operator's `ApprovalSet` declines;
- **under-report** grants → the workflow is under-provisioned and fails closed at the child's authorize check, never over-privileged;
- **lie about the projection** → the re-verify barrier catches it and fails closed;
- **declare a grant requirement the operator did not approve** → the gate declines, on the same `ApprovalSet` decision.

The fourth path is different in kind from the first three and is worth stating plainly. A declared `grantRequirement` is **not advisory**: the freeze persists it and both run-time materialization sites mint real grant rows on the run principal from it, which ship to the child as the run's `stepGrants`. It also rides `grantWalkSnapshot`, a sibling of `projection` on the probe frame, so it sits **outside the wire-hash preimage** — tamper-evidence says nothing about it. The operator's decision is therefore the only thing gating it, which is why `gateAndFreezeProbeResult` holds every declared requirement to the `ApprovalSet` alongside the walk's grant strings, comparing the whole requirement record (`source`, `resource`, `action`, `effect`, `conditions`) rather than the resource alone. The approval vocabulary carries both kinds of item for this reason: a requirement is multi-axis and cannot be expressed as a grant-shape string.

The one decision has two representations, and the difference is layering rather than policy. Persisted, it is a flat list of approval items — `FrozenApprovalBundle.approvedGrants`, each entry either a grant string or a requirement record — because a stored row owes compatibility to rows already written, and a flat list is append-friendly and order-independent. In memory it is `ApprovalSet`, a struct holding the grant strings in a `Set` and the requirements in a list, because the two kinds are tested by two different operations: a string by exact membership, a record only by structural comparison. `approvalSetFromItems` is the single boundary between the two forms, and it is also where each requirement is parsed, so the structural comparison at the gate can trust the records it receives. Neither form is a second approval channel; both are the same one operator decision.

Even so, a forged requirement **delegates** authority rather than fabricating it: `resolveGrantMaterialization` resolves every requirement against the creator's or the invoker's own grants and rejects `insufficient_grants` on a miss. The ceiling is whatever that party already holds — which is why a wildcard requirement is the maximal case and not a universal bypass. Note that a wildcard row is nonetheless stronger than any walk-derived row: the walk emits bare `tool:<name>` grants, which are inert against a pinned tool's namespaced `tool:<bundleId>:<name>` gate, while a `*` resource matches that gate. Step scoping, not the capability ceiling, is what an unapproved wildcard would defeat.

None of the four escalate privilege beyond that delegation, because forging the advisory list yields **no new credential material** (hub-delivered; the sidecar cipher only seals what the hub delivered) and **no new code** (closure-pinned). The list's _accuracy_ still matters — it is the single artifact the operator's one decision is made against, so an under-stated list could win an approval whose true grant implications the operator did not see. That is **informed-consent** integrity, worth keeping honest, not a privilege-escalation boundary.

### The child-side grant gate is defense-in-depth

At tool-invoke time the child evaluates the step's grants (`grants.json`, the operator-approved `frame.config.grants`) through a _gated capability_ before yielding credential material. That is the honest enforcement path for a **healthy** sidecar — but the sidecar is itself the process that _writes_ `grants.json` and assembles the snapshot the child reads. So the child-side gate is **defense-in-depth, not a boundary against a compromised sidecar**: a compromised sidecar could forge `grants.json`, but it gains nothing — the real boundaries are the hub's credential cipher and the closure pin, not the grant evaluation the sidecar feeds the child.

### Restoring a run's authority in a new process

A run needs three kinds of authority: its grants snapshot, its credential material, and its Hub tokens. Several paths start a new workflow child for a deployment. Before a new child resumes any run, the supervisor sends it one `authority.set` control frame with the run's grants snapshot and the supervisor's merged credential mirror. The child stops if its first control frame is not `authority.set`.

- **Grants snapshot.** The supervisor reads it from the run's grants file, `runs/<runId>/grants.json`. Before the run's first trigger there is no file, and a read that fails gives no snapshot. In both cases the frame carries no snapshot, so a step that needs grants fails closed.
- **Credential material and Hub tokens.** The mirror is the deploy delivery plus every later `credentials.update`, with its revokes applied. The Hub tokens join the mirror when the first `run.grants` barrier brings them. After each update, the sidecar seals the mirror in the run record under its credential cipher (see CREDENTIALS.md).

| Path                                                      | Grants snapshot                                                       | Credential material and Hub tokens                                                                            |
| --------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Deploy                                                    | None until the first trigger; the `run.grants` barrier then brings it | The deploy frame. The Hub tokens arrive on the first `run.grants` barrier                                     |
| Sidecar restart                                           | The run's grants file                                                 | The sealed mirror in the run record. On reconnect the Hub replays `run.grants` and reconciles the credentials |
| Crash-respawn, and every recycle (operator, policy, self) | The run's grants file                                                 | The supervisor's mirror                                                                                       |
| Hub replacement deploy                                    | The run's grants file, after the Hub restores the run's history       | The deploy frame, which also carries the run's existing Hub tokens                                            |

A credential that the Hub revokes while the sidecar is down stays usable by the restored run until the reconnect reconcile revokes it.

The child starts its timer scheduler before it applies the frame, so a timer that is already due can fire first. Only a resumed run uses grants and credentials, and the child resumes runs only after it applies the frame. The child also holds every control frame it sends until it has sent `ready` and `resumed.runs`, because the supervisor accepts nothing else first.

### What the grant list does NOT bound

Grants gate `tool:` / `effect:` / `credential:` resources. They are **not** a compute or network sandbox: raw computation, filesystem access within the child's data dir, and outbound network to public endpoints are bounded by **host/process isolation of the child**, not by the grant list. Do not mistake the grant model for a syscall sandbox.

### Trusted components

The authorization model rests on exactly these anchors — not the sidecar:

1. **The hub** — holds the only credential cipher; decides credential delivery from its own content-addressed asset plus the operator's approval; authors the workflow asset repo (hub-writes / sidecar-reads).
2. **The operator's `ApprovalSet`** — the one human decision, gating the advisory grant set at freeze.
3. **The tenant credential vault and tenant ownership** — a binding can only ever resolve a tenant-owned credential.
4. **The closure pin + SRI + child re-verify** — only approved code runs; fail closed otherwise.
5. **Host / process isolation of the child** — bounds the non-credential capabilities the grant list does not.

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
  resource        text NOT NULL  -- glob pattern: "workflow-run:*", "wallet:wal_abc", "tool:bash"
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

- `workflow-run:*` -- all workflow runs
- `workflow-run:run_abc123` -- a specific workflow run
- `wallet:wal_*` -- all wallets
- `tool:bash` -- the bash tool
- `tool:*` -- all tools
- `credential:crd_stripe` -- a specific credential
- `api:stripe:*` -- all Stripe API operations
- `federation:*` -- all federation trusts of the tenant
- `federation:tnt_abc` -- the federation trust with tenant `tnt_abc` (keyed on the target tenant id, not the trust row id). Establishing a trust checks this resource with `create`, and revoking it checks `manage`. Listing the trusts checks `federation:*` / `read`.
- `*` -- everything

Actions are operation verbs:

- `invoke` -- call/execute
- `read` -- view/list
- `create` -- create new resources
- `spend` -- financial operations
- `manage` -- update/delete/configure
- `recover` -- release a workflow run's capacity whose cleanup stopped, on the caller's word that its copy is gone; only the owner role holds it by default
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

Every check names how it treats a conditioned grant. A check on a request evaluates each condition with the hub's condition registry. A check with no request to evaluate a condition against is **pessimistic**: a conditioned `deny` or `ask` applies as if its condition held, and a conditioned `allow` does not apply. Delegation is the pessimistic check:

- **Delegation at launch.** Creator and invoker requirements resolve against the delegator's stored grants, not a request. A delegator's conditioned `deny` or `ask` refuses the requirement with 403 `insufficient_grants`, and a conditioned `allow` does not satisfy it. This applies to a user's time-window grant and to a run's consumer-scoped grant the same way, so a run never receives more than its delegator holds without conditions.

The sidecar's step grant check evaluates `time_window` against the sidecar's clock, so a run's declared time-window grant applies inside its window, as on the hub. A grant with any other condition key fails the check closed, because that key needs request context the sidecar does not have.

Specificity still decides between grants: a broad conditioned `deny` loses to a more specific unconditioned `allow`, as it would on a request where its condition holds.

## Grant Revocation

Grant revocation is policy-driven with a default of fail-secure.

**Creator grant revocation**: If the creator's authority is revoked after workflow runs have been launched with creator-sourced grants, those running workflow runs must lose the affected grants. The harness authorizes each tool call against its live materialized grant set before the call executes, so a revoked capability is blocked once that set no longer contains it; a tool call already in flight is not interrupted. Propagating a revocation to an already-running deployment is not currently implemented — the earlier `grants.update` wire mechanism has been retired, and its supervised replacement is designed separately. Until then, the change takes effect when the deployment next loads its grants (the deploy pack at spawn, and the `authority.set` frame that every new child receives; see _Restoring a run's authority in a new process_). Tenants can configure grace periods or notification-only behavior for specific grant types.

**Invoker grant revocation**: Invoker-granted capabilities carry a fixed 24-hour TTL from the run's first trigger (`INVOKER_GRANT_TTL_MS`), not a lifetime tied to when the run stops. Approving an invoker-granted `ask` with `always` changes its effect to `allow` and keeps its expiry.

**Tenant policy changes**: When tenant policies change (role modifications, system role updates), the control plane re-evaluates affected runs. Propagating the resulting grant changes to already-running deployments shares the retired-mechanism gap described above; a change takes effect when each deployment next loads its grants.

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
  resource              text NOT NULL   -- 'asset:*', 'asset:ast_xxx', 'agent-state:run_xxx', ...
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

- `resource` — a single substrate authz resource string, e.g. `asset:*`, `asset:ast_xxx`, `agent-state:run_xxx`. Glob patterns are honored by the substrate; a token with `resource: "asset:*"` reaches every asset row in the tenant the token is bound to.
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

| Interchange concept          | Implementation                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Creator-granted capabilities | Grant requirements with `source: "creator"`, resolved against creator's principal, materialized with `origin = 'creator'`. Refused at freeze on a definition sourced from a package-registry tarball                                                                                                                                                                        |
| Invoker-granted capabilities | Grant requirements with `source: "invoker"`, resolved against the deployer's principal (the invoker of record), materialized with `origin = 'invoker'`, a fixed 24-hour `expires_at`                                                                                                                                                                                        |
| Tool-call gates              | Grants where `resource = 'tool:...'`                                                                                                                                                                                                                                                                                                                                        |
| Wallet access                | Grants where `resource = 'wallet:...'`, `action = 'spend'` (spending-limit conditions are planned — see Conditions)                                                                                                                                                                                                                                                         |
| Credential use               | A tenant-owned credential's use is authorized by ownership at resolution (no grant requirement); a consumer-scoping `credential:{id}` / `use` grant with `origin = 'system'` is stamped for the runtime gate. Principal-owned credential use is not yet supported. See CREDENTIALS.md                                                                                       |
| User roles (RBAC)            | Grants attached to roles, roles assigned to user principals                                                                                                                                                                                                                                                                                                                 |
| Human approval gates         | Grants with `effect = 'ask'`                                                                                                                                                                                                                                                                                                                                                |
| Delegation containment       | A delegator cannot delegate authority it does not currently hold — each creator/invoker requirement is validated at launch against that principal's own grants. Invoker grants are non-re-delegatable and expire in 24h. Creator requirements are refused at freeze for a definition sourced from a package-registry tarball, whose publisher provenance does not yet exist |

## Personal Tenant

> **Planned / Not Yet Implemented.** Nothing creates a personal tenant on registration. `createAuth` in `@intx/hub-api` registers no post-signup hook, and the only code that creates the tenant, principal, and owner-role trio is the tenant-creation route, which an authenticated user calls with a slug it supplies. The sequence below records the intended design.

On user registration:

1. Create a tenant whose slug is derived from the user's email address and reduced to a single DNS label. The address itself is not a legal slug, because `TenantSlug` and `tenant_slug_dns_label_check` both reject `@` and `.` (see Tenant Schema).
2. Create a principal for the user in that tenant (`kind = 'user'`).
3. Assign the system `owner` role.
4. The owner role includes a broad default grant: `resource = "*", action = "*", effect = "allow"`.

The personal tenant has the same authorization machinery as any other tenant. When the user creates workflows there, those runs get principals and grants through the same system. The onboarding UX is simple, but the underlying model is uniform.

## Tenant Schema

```
tenant
  id              text PK        -- tnt_...
  name            text NOT NULL
  slug            text NOT NULL UNIQUE
                                 -- CHECK: a single DNS label, at most 63 chars
  domain          text NOT NULL  -- SMTP domain; no grammar constraint
                                 -- unique on lower(domain), via index
                                 -- create route derives <slug>.localhost
  parent_id       text FK -> tenant (nullable, for hierarchy)
  config          jsonb
  created_at      timestamptz
  updated_at      timestamptz
```

`tenant_slug_dns_label_check` constrains the slug to a single DNS label under RFC 1035 section 2.3.1 as relaxed by RFC 1123 section 2.1. The create route applies the same grammar through the `TenantSlug` validator, and the constraint covers every write path to `slug`, including the ones that bypass that route.

What mail keys on is `domain`, not `slug`: `domain` is the sender stamp on outbound mail and the recipient run address, and the recipient address is the key into the inbound mail policy lookup and the mail router. The create route derives `domain` as the lowercased slug plus a suffix, but that derivation is the route's own convention -- nothing relates the two columns, and `domain` carries no grammar constraint. So the check bounds what the route derives a domain from; a write path that bypasses the route sets `domain` freely.

Tenants can be organized hierarchically. Child tenants inherit policies from their parent (additive restrictions only). Federation between sibling tenants requires explicit trust establishment.

A child tenant inherits use of its parent's tenant-owned credentials, providers, catalogs, assets and OAuth clients without a grant of its own. So creating a tenant with a `parent_id` takes authority in the parent: the caller must have an active principal there that is allowed `create` on `tenant:*`. The system owner and admin roles hold it; the member role does not. Every refusal is the same 403, whether the parent does not exist, the caller is not an active member, or the grant is missing, so the route does not reveal whether a tenant id exists or who belongs to it. A tenant created with no parent needs only an authenticated user.
