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

The `principal_key` table holds a principal's Ed25519 signing key, giving it a stable cryptographic identity. The public key is a stable handle a verifier can check a signature against. For a hub-custodied key the hub also holds the private key; for a public-only key another party holds it.

```
principal_key
  id            text PK        -- pky_...
  principal_id  text FK -> principal (ON DELETE CASCADE)
  public_key    text NOT NULL UNIQUE  -- hex-encoded 32-byte Ed25519 public key
  private_key   text NULL             -- hex-encoded 32-byte seed, sealed by the cipher;
                                      -- NULL when another party holds the private key
  status        text NOT NULL         -- 'active' | 'retired'
  created_at    timestamptz
  updated_at    timestamptz
  UNIQUE(principal_id) WHERE status = 'active'   -- one active key per principal
```

A row with a NULL `private_key` is public-only: another party holds the private key, and the hub holds only the public key. The hub can check the principal's signatures against it, but `principalKeyStore.sign` refuses to sign with it.

Only the active row signs or answers for a principal. Retiring a key sets its row to `retired`; the row stays, `sign` and `getPublicKey` ignore it, and another key can then become active. Recording a retired public-only key again for the same principal makes that row active again; recording the public key of a hub-custodied row is refused, so a key recorded as public-only never signs.

The private key seed is sealed at rest with the same AEAD cipher that protects credential secrets, bound to its row and column so a sealed key cannot be transplanted to another row. See [CREDENTIALS.md](./CREDENTIALS.md) for the encryption-at-rest mechanics.

Hub custody is an **attribution** model, not non-repudiation. A signature with a hub-custodied key proves the action came from a holder of the principal's private key, which is enough to attribute the action to the principal within the system. It is **not** proof against the hub operator, who also holds the key and could sign as the principal. Non-repudiation would require the private key to live somewhere the hub cannot reach, which the hub-custodied model does not provide.

Every user principal is minted a signing key when it is created, in the same transaction as the principal row, so a user principal never exists without a key. A workflow run principal is different: deploy creates it with no hub-held key, and the run's first trigger links it to the run. The hub seals a hub-custodied key's seed under `PRINCIPAL_KEY_ENCRYPTION_KEY` (32 bytes, hex) -- a separate key from `CREDENTIAL_ENCRYPTION_KEY`, so the two rotate independently -- and refuses to start without it.

A workflow run signs its mail with a key that the sidecar running its deployment mints. The sidecar holds the private key. When the deployment finishes initializing, the hub records the public key as the active, public-only key of the run principal. When the hub replaces the sidecar, it retires that key, and the key the replacement sidecar reports becomes active when the replacement's initialization completes. A replacement that reuses the replaced sidecar's disk reports the same key, and that key becomes active again. The hub resolves no retired key, so while the key stays retired it vouches for no key for mail that the replaced sidecar signed. A recipient sidecar that cached the retired key keeps it until the run's next mail carries the replacement's key.

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

A workflow run calls the tenant routes with a run token instead of a session. A run token is an opaque bearer of the form `itx_run_<base64url>`, held in the `run_token` table as a SHA-256 hash and as a copy sealed with the credential cipher. Each token belongs to one run principal and one consumer: a tool factory (`tool:<factory id>`) or an action handler (`action:<handler>`).

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

A consumer opts into hub access by declaring the reserved credential handle `interchange.hub` (`HUB_CREDENTIAL_HANDLE`): a tool in `defineTool({ credentials })`, an action handler in `action({ credentials })`. Deploy records the declaring consumers on the deployment's anchor run, across the root and every nested body. When the run's first trigger links the run principal to the run, in the same transaction, the hub mints one token for each of them and stamps a `credential:hub:<consumer>` / `use` grant scoped to that consumer by a `{ tool }` condition. The hub delivers the tokens on the run's `run.grants` barrier, as a credential delivery that binds the reserved handle to an `http` credential pinned to `HUB_API_ORIGIN`. Every barrier ahead of a trigger mail carries them, including a replay after a reconnect, and the reconnect credential reconcile sends them again. The reconcile never revokes them: a run with no tokens may not have minted them yet, and a deleted token already fails at the hub. The sidecar merges them into the supervisor's credential mirror before it writes the grants. It never writes them in the grants file: it keeps them in the mirror, which it seals in the run record (see _Restoring a run's authority in a new process_). A consumer resolves `interchange.hub` to a mediated credential and calls the hub through it, as with any other credential. Mediation is not containment: a consumer can read the header it sends, so a token that leaks works until its run principal is deactivated.

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
  conditions?: object  -- optional constraints; only conditions a run can evaluate (see Conditions)
}
```

The `source` field declares where the delegated authority should come from:

- `source: "creator"` — The definition author must delegate this. Resolved at launch against the creator's own grants (identified by `creatorPrincipalId` on the definition). The control plane validates that the creator currently holds the authority being delegated — a creator cannot delegate what they don't have. Materializes as a `grant` with `origin = 'creator'`; an `allow` row expires when the creator's grants stop allowing the requirement (see _Grant Revocation_). This is the setuid model: the definition author's authority travels with the definition. A definition sourced from a package-registry tarball is refused at freeze if it declares any creator requirement: the asset's `creatorPrincipalId` names whoever created the registry asset, not whoever published the tarball into it, and anyone holding `asset:<id>` write can publish, so no publisher provenance exists to delegate from. A definition sourced from a `workflow` asset is not refused. A `workflow` asset is its creator's own codebase, and write on it makes the holder a contributor to that codebase, so it delegates the creator's authority: any principal that holds `asset:<id>` write can change the code that a deploy runs with the creator's grants. Grant that write only to principals the creator trusts with that authority.
- `source: "invoker"` — The invoker must provide this. The invoker of record is the deployment's **deployer**: the principal that deployed it, recorded as the launch spec's `sourceAuthorityPrincipalId`. It is not whoever sends the trigger mail, so the trigger path cannot change whose authority a run delegates, and a run that deploys another workflow is that workflow's invoker. Resolved at launch against the deployer's delegatable grants, on the HTTP trigger and the mail-triggered path alike. Materializes as a `grant` with `origin = 'invoker'`. An `allow` row has an `expires_at` no later than 24 hours after deploy (`INVOKER_GRANT_TTL_MS`); a `deny` or `ask` row has none. A deployment with invoker-sourced requirements whose deployer is not recorded or no longer active cannot launch a run: the trigger is refused with 409 `deployer_unavailable`. A deployment that a run deployed can outlive that run, so a later first trigger is refused this way once the run has ended. A deployment without invoker-sourced requirements needs no deployer. The hub reads the deployer's grants at a run's first trigger. After that, the run's invoker rows follow the deployer through re-derivation (see _Grant Revocation_): a row lapses when the deployer loses the grant or is not active. The 24-hour expiry is an upper bound. Nothing renews it, so an invoker `allow` row on a long-lived run lapses 24 hours after deploy at the latest. Deploy does not check that the deployer can delegate the invoker requirements; the first trigger refuses a deployer that cannot.

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

At the top-level definition **load boundary** — a fresh, resumed, or restored run — the loader recomputes the wire hash and refuses to proceed unless it matches the approved hash. A mismatch throws; there is no fallback. This catches a definition that diverged from what was approved (a redeploy/resume divergence, or a post-approval mutation of the on-disk asset that the substrate's hub-writes / sidecar-reads authorization does not itself cover for a direct filesystem write).

**The barrier is load-bearing only where the approved hash arrives _out-of-band_ from the bytes being checked** — from the hub's deploy frame or the child's spawn env, which a filesystem-level tamperer of the on-disk definition cannot also forge. Where no such out-of-band pin exists, a gate could only fail-closed-always, so it is deliberately absent and the definition's integrity rests on the asset repo's write-authorization plus push-time envelope validation. Applying this rule per load site:

- **Source-ref closure evaluation** — the airlock. Author code sourced from a registry is evaluated to a live definition and re-verified by project-then-hash against `SpawnTimeEnv.definitionHash`. Load-bearing; this is the whole point of the content-hash approval.
- **Top-level inert read** — re-verified against `SpawnTimeEnv.definitionHash` (the hub-approved hash on the deploy frame). Load-bearing.
- **onTrigger bodies** — a body is a section of the parent's own approved definition, resolved in memory from the parent's re-verified closure, so the top-level re-verify covers it. The body spawn runs no separate check.
- **childWorkflow spawns** — a `childWorkflow{definitionRef}` references a **separately-approved** workflow asset by id; the parent holds no hash for it and has no authority over it. There is no out-of-band pin, so the in-process spawn reads, envelope-validates, **and runs** the inert definition **without** a re-verify gate — there is no re-verify step between the read and execution on this path, so nothing here guards execution against post-approval drift. The child asset re-verifies against **its own** approved hash only when it is itself deployed as a top-level run (a separate execution), not from a parent that merely references it.

The hub-approved `approvedWireHash` is persisted on the sidecar's deployment record so that a **sidecar restart** re-threads it into the restored child's spawn env and the same barrier holds across the restart — rather than a restored definition failing closed for want of a hash.

## The Sidecar Is Not an Authorization Authority

A code-sourced workflow's package code is installed and **evaluated on the sidecar** — untrusted, author-controlled code runs there in an airlocked child to produce the inert projection and the capability walk. So it matters that **the sidecar is not trusted to decrypt credentials or to determine which code runs.** Those are the two things that grant a workflow real power, and both are anchored hub-side:

**Credentials are decrypted and delivered by the hub.** The secrets a consumer (a tool factory or a workflow action handler) uses to reach an external system are held encrypted in the tenant vault and decrypted **hub-side** (`buildCredentialDelivery`). The sidecar cannot decrypt the vault. It keeps the deliveries it received in each run record, so a restart can restore the run, and it seals every secret there under its own credential cipher, so no credential is plaintext at rest. The hub decides _which_ credentials to deliver from its **own** content-addressed workflow asset plus the operator's approval — never from anything the sidecar supplies — and each delivered credential grant is scoped `credential:{id}/use` to a specific consumer, resolving only a **tenant-owned** credential. A consumer whose secret the hub did not send is inert.

**The code that runs is the code the operator's approval was bound to.** The dependency closure is hub-resolved and SRI-pinned; at every load boundary the child re-materializes it, re-projects, and re-checks the projection's content hash against the hub-approved hash, failing closed on any mismatch (see _Definition Approval and Re-Verification_ above). A sidecar cannot substitute different code without failing this barrier. The long-lived sidecar process never evaluates the code itself: each workflow child reports the projection it verified, and the supervisor checks that report against the same hash before it uses it, because the child runs author code and could misreport it.

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

At tool-invoke time the child evaluates the step's grants (`grants.json`, the run's committed grants from its `run.grants` frames) through a _gated capability_ before yielding credential material. That is the honest enforcement path for a **healthy** sidecar — but the sidecar is itself the process that _writes_ `grants.json` and assembles the snapshot the child reads. So the child-side gate is **defense-in-depth, not a boundary against a compromised sidecar**: a compromised sidecar could forge `grants.json`, but it gains nothing — the real boundaries are the hub's credential cipher and the closure pin, not the grant evaluation the sidecar feeds the child.

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

The child starts its timer scheduler before it applies the frame, so a timer that is already due can fire first. Only a resumed run uses grants and credentials, and the child resumes runs only after it applies the frame. The child also holds every control frame it sends until it has sent `definition`, `ready` and `resumed.runs`, because the supervisor accepts nothing else first.

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

An `ask` decision on a tool call in a workflow run suspends the call and creates an approval request for a human. On a hub REST route, `ask` does not allow the request: the route returns 403 and creates no approval. The human approves or rejects the tool call, with a scope:

- Approve with `once` allows this call only. Approve with `always` also changes the run principal's `ask` grant for that tool to `allow`. The run then does not ask again for the rest of the run.
- Reject denies this call, with an optional message as feedback. Its scope defaults to `once`. Reject with `always` also changes the grant to `deny`. The run then blocks the tool and does not ask again.

A standing decision changes the effect of an existing grant whose resource is the exact `tool:<name>`. It never creates a grant. It applies across every trigger of the deployment and in its child runs. Two grant sends for one run can arrive out of order: a send that loaded the grants earlier can arrive later. The run then holds the older grants until the next send, and it can ask again.

### Conditions

The `conditions` JSONB field constrains when a grant applies. Each key names a condition evaluator in the hub's condition registry. Two evaluators are registered:

- `{ "time_window": { "after": "09:00", "before": "17:00", "timezone": "America/Los_Angeles" } }` -- time-based access. `timezone` is required and is an IANA zone name or a UTC offset. `after` and `before` are `HH:MM` in 24-hour format and must differ; `after` later than `before` gives a window that crosses midnight. No other key is allowed.
- `{ "tool": "tool:<factory id>" }` -- a consumer-scoped grant, which applies only when the named credential consumer makes the request. The value is a non-empty consumer identity.

Spending limits and threshold-based escalation are planned conditions with no evaluator yet:

- `{ "max_spend_per_day": 100, "currency": "USD" }` -- spending limits (not yet available).
- `{ "require_approval_above": 50 }` -- threshold-based escalation (not yet available).

Every grant is evaluated on the hub with this registry. A grant that sits directly on a workflow run's principal is also evaluated where the run is, with narrower registries:

| Where               | Evaluates                                                                                       | Registry           |
| ------------------- | ----------------------------------------------------------------------------------------------- | ------------------ |
| The run child       | a row whose resource pattern can match a resource outside `credential:`                         | `time_window` only |
| The credential gate | a row whose resource pattern can match `credential:<id>` and whose action pattern matches `use` | `tool` only        |

A role grant never reaches a run, because a run loads only the rows on its own principal. So a run-principal row may carry `time_window` on a non-credential resource and `tool` on credential use. A row whose patterns can reach both, such as resource `*` with an action pattern that matches `use`, may carry no condition, because neither key is evaluable in both places.

The hub checks conditions against every registry that will evaluate the row before it stores the row:

- The probe gate (`gateAndFreezeProbeResult`) checks a workflow definition's declared grant requirements, which become rows on the run principal, at install and approve. It refuses the definition with the reason `grant_requirement_conditions_invalid`, before the operator approval checks and before anything is frozen.
- Grant create (`POST`) and update (`PATCH`) check `conditions` for the grant's target, resource, and action. An unregistered key, such as the planned conditions above, a body that does not match its evaluator's shape, or a key that a run cannot evaluate on a run-principal row, gives a 400 that names each problem, and the grant is not stored. A `PATCH` that omits `conditions` does not check or change the stored value.

The authorization engine evaluates conditions at runtime: a grant whose registered condition is unmet is skipped. A stored grant that carries an unregistered key or a malformed body still throws and fails the authorization, to surface the misconfiguration. Such a row comes from a path that does not check conditions: a grant materialized (at launch, or when a change to the delegator's grants re-derives the run) from a declared grant requirement that was frozen before the probe gate checked conditions, a direct database write, or a row stored before the check existed.

Every check names how it treats a conditioned grant. A check on a request evaluates each condition with the hub's condition registry. A check with no request to evaluate a condition against is **pessimistic**: a conditioned `deny` or `ask` applies as if its condition held, and a conditioned `allow` does not apply. Delegation is the pessimistic check:

- **Delegation at launch.** Creator and invoker requirements resolve against the delegator's stored grants, not a request. A delegator's conditioned `deny` or `ask` refuses the requirement with 403 `insufficient_grants`, and a conditioned `allow` does not satisfy it. This applies to a user's time-window grant and to a run's consumer-scoped grant the same way, so a run never receives more than its delegator holds without conditions.

The sidecar's step grant check evaluates `time_window` against the sidecar's clock, so a run's declared time-window grant applies inside its window, as on the hub. A grant with any other condition key fails the check closed, because that key needs request context the sidecar does not have.

Specificity still decides between grants: a broad conditioned `deny` loses to a more specific unconditioned `allow`, as it would on a request where its condition holds.

## Grant Revocation

Grant revocation is policy-driven with a default of fail-secure.

**Delegated grants follow their delegators**: A run's grant requirements are delegated from its creator (the creator of the definition's asset) or its invoker (the deployment's deployer). At the run's first trigger the hub writes one row on the run principal for each requirement the delegator allows. The run principal exists from deploy, so an operator can grant to it before the first trigger; those rows stay, the first trigger sends them with the rows it writes, and an operator `mail.accept` deny among them refuses the first mail. An operator who suspends or deactivates the run principal before the first trigger makes the trigger fail with `workflow_run_principal_inactive`. After that, the hub re-derives those rows whenever a delegator's grants change, so a running run holds what its delegators hold now:

- A requirement the delegator no longer allows loses the run's `allow` rows for it. This includes an `ask` that a standing approval changed to `allow`, because that `allow` exceeds what the delegator holds. The run's `deny` and `ask` rows for it stay as floors, with no expiry.
- A requirement the delegator allows again gets its row back. A regained invoker `allow` row ends no later than 24 hours after deploy, so once that time has passed the row does not come back. A regained `tool:` `ask` row gets back the latest `always` decision the run's approver made for that tool.
- A row the delegator still allows stays, with its id and any standing decision, and takes the expiry its effect gets from the delegator's grants now (see _Row expiry_).

**Row expiry**: A requirement's `allow` row expires at the first expiry of a delegator grant after which the delegator's grants no longer allow the requirement, or never if no such expiry exists. When the delegator's `allow` grants for the requirement stand alone, that is the latest of their expiries. A specific `allow` that overrides a broader `deny` ends the row when the specific `allow` ends. A role grant with an expiry counts the same as a direct grant. An invoker `allow` row also ends 24 hours after deploy at the latest: the limit counts from the creation of the run principal, which deploy creates. A run whose first trigger comes more than 24 hours after deploy therefore gets no invoker `allow` rows. A `deny` or `ask` row is a floor: it restricts the run, so it has no expiry. After the 24-hour limit an invoker requirement counts as lapsed: its `ask` row stays, and an `allow` row for it is deleted at the next re-derivation. A regained `tool:` row that a standing `always` approval makes `allow` gets the `allow` expiry. A standing (`always`) resolution that changes a requirement row's `ask` to `allow` or `deny` re-derives the run in the same request, so the row gets the expiry of its new effect at once. The hub's REST checks and the run's child both skip a row after its expiry; the child compares the expiry with its own clock. Re-derivation deletes a requirement's expired rows, so a delegator grant renewed after it expired gives the row back. A run that was live before the hub gave delegated rows these expiries keeps its earlier row expiries until its next re-derivation: a creator `allow` row with no expiry, and an invoker row, `allow` or floor, that ends 24 hours after the first trigger.

Only the rows that grant requirements produce depend on a delegator. The runtime `tool:` and `effect:` rows, the `mail.accept` rows and the credential-use rows come from the deploy approval, and a delegator change does not touch them. A requirement whose row has the same key as a runtime row is also left alone.

These routes re-derive the runs that delegate from the principals they change:

| Change                                                           | Runs re-derived                                  |
| ---------------------------------------------------------------- | ------------------------------------------------ |
| `POST`, `PATCH` or `DELETE` of a grant on a principal            | The runs that delegate from that principal       |
| `POST`, `PATCH` or `DELETE` of a grant on a role (system or not) | The runs that delegate from a member of the role |
| Role assign or unassign                                          | The runs that delegate from that principal       |
| Principal status `PATCH`                                         | The runs that delegate from that principal       |
| Principal `DELETE`                                               | The runs whose asset the principal created       |

A run whose rows change is itself the delegator of the deployments it deployed and of the assets it created, so those runs are re-derived too.

The change and the re-derivation commit in one transaction, so the hub's REST checks see the new rows at once. After the commit the hub sends each affected live run its grants in a `run.grants` frame, and also each live run whose own rows the route changed. A run whose sidecar is not connected gets its grants when the sidecar reconnects; a send that fails is corrected by the next signal, trigger or reconnect. Until the frame arrives the run's child uses its old grants. The harness checks each tool call against the run's grants before the call starts, and it does not interrupt a call that is in progress. Two grant sends for one run can arrive out of order, and then the sidecar holds the older grants until the next send.

The routes in the table, role delete and member invite take one tenant lock, so their changes in a tenant run one at a time.

Points to know:

- An invoker `allow` row expires 24 hours after deploy at the latest (`INVOKER_GRANT_TTL_MS`), not when the run stops. Approving a requirement row's `ask` with `always` changes its effect to `allow`, and the row gets the `allow` expiry at once.
- A creator that is suspended or deactivated still delegates; only the deployer must be active. A status change on a creator changes nothing. A deployer that is not active lapses the run's invoker rows.
- A principal that deployed a workflow cannot be deleted (409). Deleting an asset's creator removes the creator link, and the run's creator rows lapse.
- An operator `DELETE` of a run's requirement row is undone at the next re-derivation. An operator `PATCH` of its conditions changes its key, so the next re-derivation adds the original row next to it. An operator `PATCH` from `allow` to `deny` stays. It keeps the `allow` row's expiry until the next re-derivation removes it, and it ends at that expiry if no re-derivation comes first. To keep the `deny` past that expiry, set `expiresAt` to `null` in the same `PATCH`.
- A run keeps its `ask` row for a lapsed requirement, and for an invoker requirement after the 24-hour limit. An approver can still allow the tool for one call (`once`). An `always` approval removes the row in the same request, because its `allow` exceeds what the delegator holds, so the run's next call on the tool finds no row and is denied, not asked. An `always` rejection turns the row into a `deny` floor.
- A standing (`always`) resolution takes the tenant lock that the routes above take, before it locks the deployment, so it and a re-derivation run one at a time. On a provisioned deployment it holds the lock while it reads the run's lifecycle from workflow Git, so the tenant's grant, role and principal routes wait for that read. It re-derives the run and the deployments the run deployed. A `once` resolution changes no grant and does not take the lock.
- A delegator grant that expires with time does not start a re-derivation. The run's `allow` row for it expires at the same time (see _Row expiry_). When the delegator's grants allow a requirement again because a `deny` or `ask` grant expired, the run gets the row back only at the next re-derivation. A direct database write (a migration or a seed) does not start a re-derivation. The sweep in the next point still finds a deployer that a direct write made not active.
- A run that deploys a workflow is the deployer of that deployment. When the run ends, its principal is deactivated, so the deployment's invoker rows lapse. No route makes that change, so a sweep finds it: every 30 seconds the hub re-derives each live run that holds an unexpired invoker `allow` row while its deployer is not active, and sends its grants to each run whose rows changed. Each run is re-derived in its own transaction, so a run that fails does not stop the others. A run whose invoker `allow` row no requirement produces (an operator added it, or an operator `PATCH` changed its conditions) keeps the row, so the sweep selects it again at each pass and changes nothing.
- A running child body authorizes against the root's current grants under its spawn-time cap, from its next tool call or step. Each rung of a deeper child caps the current grants of the rung that spawned it.

**Credential deletion** is not a re-derivation: deleting a credential deletes every `credential:<id>` row, the run principals' rows included, and evicts its secret (see CREDENTIALS.md).

This parallels the credential revocation model described in CREDENTIALS.md — both follow the same fail-secure default with configurable tenant policies.

## Smart-HTTP Git Tokens

The hub exposes asset repositories over the smart-HTTP wire (`info/refs`, `git-upload-pack`, `git-receive-pack`). Stock `git` clients authenticate to those endpoints with an opaque bearer token rather than a better-auth session cookie. See `docs/GIT_ACCESS.md` for the operator walkthrough — credential-helper setup, URL grammar, `refPattern` grammar, and worked clone/push examples.

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
  resource              text NOT NULL   -- 'asset:*', 'asset:ast_xxx', ...
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

- `resource` — a single substrate authz resource string, e.g. `asset:*`, `asset:ast_xxx`. Glob patterns are honored by the substrate; a token with `resource: "asset:*"` reaches every asset row in the tenant the token is bound to.
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

| Interchange concept          | Implementation                                                                                                                                                                                                                                                                                                                                                                                           |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Creator-granted capabilities | Grant requirements with `source: "creator"`, resolved against creator's principal, materialized with `origin = 'creator'`. Refused at freeze on a definition sourced from a package-registry tarball                                                                                                                                                                                                     |
| Invoker-granted capabilities | Grant requirements with `source: "invoker"`, resolved against the deployer's principal (the invoker of record), materialized with `origin = 'invoker'`, an `allow` row's `expires_at` no later than 24 hours                                                                                                                                                                                             |
| Tool-call gates              | Grants where `resource = 'tool:...'`                                                                                                                                                                                                                                                                                                                                                                     |
| Wallet access                | Grants where `resource = 'wallet:...'`, `action = 'spend'` (spending-limit conditions are planned — see Conditions)                                                                                                                                                                                                                                                                                      |
| Credential use               | A tenant-owned credential's use is authorized by ownership at resolution (no grant requirement); a consumer-scoping `credential:{id}` / `use` grant with `origin = 'system'` is stamped for the runtime gate. Principal-owned credential use is not yet supported. See CREDENTIALS.md                                                                                                                    |
| User roles (RBAC)            | Grants attached to roles, roles assigned to user principals                                                                                                                                                                                                                                                                                                                                              |
| Human approval gates         | Grants with `effect = 'ask'`                                                                                                                                                                                                                                                                                                                                                                             |
| Delegation containment       | A delegator cannot delegate authority it does not currently hold — each creator/invoker requirement is validated at launch against that principal's own grants. Invoker grants are non-re-delegatable, and an invoker `allow` row expires within 24h. Creator requirements are refused at freeze for a definition sourced from a package-registry tarball, whose publisher provenance does not yet exist |

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
