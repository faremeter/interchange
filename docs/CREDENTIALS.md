# Faremeter Interchange

_Credential Storage_

## Overview

Credentials enable agents to interact with third-party services on behalf of users and organizations. The credential system handles storage, resolution, access control, and lifecycle management for secrets that agents need to operate in the world.

## Design Principles

Agents never discover or look up credentials at runtime. When an agent is launched into a harness, the control plane resolves the credentials the agent needs and provides them to the harness as part of the launch process. The harness then mediates the agent's access to those credentials, either by proxying requests (attaching authentication headers transparently) or by injecting credentials into the agent's runtime environment, depending on the credential type and the provider's integration model.

This means credential resolution, access control, and scope validation all happen at launch time, not at runtime. The harness receives ready-to-use credentials and is responsible for their secure handling during the agent's lifecycle.

## Providers

A provider represents any third-party service that agents interact with. GitHub, OpenAI, Stripe, Slack, an internal deployment service, an SSH bastion host -- all are providers. The provider definition describes how Interchange integrates with that service.

Every credential belongs to a provider. There are no provider-less credentials. Even internal services or custom integrations are modeled as providers because the provider definition is what tells the system how to handle the credential (refresh behavior, authentication method, scope model).

Providers have a `plugin` field that references the code module responsible for provider-specific behavior. An API key provider uses a simple plugin that knows the credential is static. An OAuth provider plugin handles the authorization code flow, token exchange, and refresh. Provider-specific quirks (PKCE support, non-standard token formats, custom scope semantics) are encapsulated in the plugin.

Providers are tenant-scoped and inherit through the tenant hierarchy. A parent tenant can define a "github" provider, and all child tenants inherit that definition unless they shadow it with their own "github" provider. This lets organizations define their integrations once and have them available to all sub-teams.

## OAuth Clients

For providers that use OAuth2, the tenant needs a client registration -- the `client_id` and `client_secret` that identify the Interchange application to the provider. This is stored separately from both the provider definition and the credentials because it has a different lifecycle and purpose.

The provider definition describes the service (URLs, scopes, behavior). The OAuth client describes the tenant's registered application with that service. Multiple tenants might use the same provider definition but have different OAuth client registrations.

OAuth clients inherit through the tenant hierarchy the same way providers do. If a child tenant doesn't have its own OAuth client for a given provider, it uses the parent's.

OAuth clients only exist for OAuth-type providers. API key providers don't need client registrations because there's no OAuth flow involved.

## Credentials

A credential is a runtime secret that agents use to authenticate with a provider. The credential type (API key, OAuth token, certificate) is a property of the credential, not something the agent specifies. From the agent's perspective, it needs "access to service X" and the system figures out the authentication mechanism.

### Tenant Scoping

Every credential belongs to exactly one tenant. The tenant is the isolation boundary. Credentials inherit through the tenant hierarchy via walk-up resolution: when looking for a credential, the system checks the current tenant first, then walks up the parent chain until it finds a match or reaches the root.

This means organizational credentials can be stored at the parent tenant and automatically become available to child tenants. A child tenant can shadow a parent's credential by creating one with the same name, which takes precedence within that child's scope.

### Principal Ownership

A credential can optionally be owned by a specific principal within the tenant. This distinguishes between organizational credentials (shared across the tenant, no principal owner) and personal credentials (owned by a specific user or agent).

When the principal owner is null, the credential is an organizational credential managed by tenant administrators. When the principal owner is set, the credential belongs to that specific principal -- typically created through an OAuth flow where a user authorized Interchange to act on their behalf with a third-party service.

This distinction matters at resolution time when determining which credential to use based on the agent definition's source specification.

### Unique Names

Credential names are unique within a tenant. This enables name-based resolution and shadowing across the tenant hierarchy. A credential named "github-deploy-key" in a child tenant shadows a credential with the same name in the parent tenant.

### Scopes

All credentials can carry scope information, not just OAuth tokens. For OAuth tokens, scopes are populated automatically from the token response. For API keys and other credential types, scopes are specified by the administrator when creating the credential. This allows the control plane to match credential requirements accurately regardless of credential type.

If an agent requires specific scopes and a credential doesn't declare any, the credential doesn't match. Administrators should specify scopes on API keys when those keys have limited permissions.

### Status and Lifecycle

Credentials track their health via a status field: active, expired, revoked, or error. Expired or errored credentials are not resolved for agent launches. Proactive refresh of OAuth tokens before they expire (and the error-status transition on refresh failure) is **Planned / Not Yet Implemented** — see "Credential Updates and Refresh" below.

## Agent Credential Requirements

Agent definitions live in git repositories and declare what credentials the agent needs to operate. Requirements are expressed as capability specifications, not direct references to specific credentials.

A credential requirement specifies:

- **Provider**: which service the agent needs access to (by provider name)
- **Scopes**: what permissions are needed (optional, matched against the credential's scopes)
- **Source**: whose credential to use -- the tenant's organizational credential, the agent creator's personal credential, or the invoking user's personal credential
- **Name**: an optional tiebreaker when multiple credentials match the same provider, source, and scope requirements

The source field maps to the architecture document's three-source model. A definition binds a consumer's declared credential handle to a concrete provider and source through a **credential binding** (`credentialBindings` on the definition). The consumer is a tool package or a workflow action handler. At launch the control plane resolves each binding to a specific tenant-owned credential and materializes a consumer-scoped `credential:{id}` / `use` grant on the agent's principal, authorized by tenant ownership within the hierarchy — the walk-up resolution already proves it, so the grant is stamped `origin = 'system'` directly and scoped to the consumer by a `{ tool }` condition, not delegated from a creator or invoker. The resolved secret is then **delivered** to the consumer over the sidecar control channel (decrypted hub-side at launch, never put in the spawn environment, and written to disk only in the sidecar's run record, sealed under the sidecar's credential cipher) and **gated at runtime by a per-consumer capability**: a tool resolves its declared handle to an origin-pinned mediated credential only if its own package holds the `credential:{id}` / `use` grant, and an action handler resolves a handle bound to `action:<handler>` only if the action step's grants hold that grant for `action:<handler>`. The scope is per handler: steps that share a handler share the handles their declarations name. The secret is read fresh on each use, so a rotation — or a revocation delivered by omission — reaches an already-shaped handle. An action handler's capability is built for each invocation and disposed when the handler settles; an action that declares a handle with no binding fails before its handler runs. The handler reaches the capability only inside `perform`'s `run`, so every authenticated call that `run` awaits is gated by the effect grant and recorded in the ledger; the scope and its handles refuse use once `run` settles. General provider/scope requirement resolution outside the binding mechanism is not yet built.

A binding names exactly one consumer:

- `package`: a tool package, given as the tool factory's bundle id. The stamped `{ tool }` condition is `tool:` plus that value.
- `action`: a workflow action handler, given as the handler's export name in the workflow's `interchange.actions` module. The stamped `{ tool }` condition is `action:` plus that value.

The condition key is `tool` for both kinds. The prefix of the value names the kind, so a tool package and an action handler with the same name never share a grant.

A tool declares the handles it resolves in `defineTool({ ..., credentials: [{ handle, scopes? }, ...] })`. The declaration is part of the approved projection and its hash. The check works per tool package, which is also the consumer a binding names: the declarations of every factory with that package id form one set, and every tool of the package can resolve every handle bound to it. A package none of whose factories declares credentials is not checked: its bindings may name any handle. A pinned package declares in its `package.json` `interchange.credentials`; that list covers all of its factories. A factory in it that also declares through `defineTool` may declare only handles the list declares, with the same scopes, or the loader refuses the package. So a package whose factories use different handles lists them all in `interchange.credentials`, and each factory declares its own.

An action declares the handles its handler may resolve: `action({ handler, credentials: [handle, ...], effect: { requires: [capability, ...] } })`. The handles resolve only inside `perform`'s `run`, so `action()` refuses `credentials` without an `effect.requires` capability. The declaration is part of the approved projection and its hash. The bindings for one handler may name only the union of the handles its action steps declare.

The reserved handle `interchange.hub` is not bound by an authored binding. Deploy records each consumer that declares it, and the hub binds the handle for that consumer to the run's token for the hub REST API, delivered on the run's grants barrier; see "Workflow run tokens" in AUTH.md.

Bindings belong on the root definition; deploy resolves no other. When a definition is installed and approved, the hub's probe gate refuses it, before any credential is resolved, if:

- an action declares a handle that no root binding names for its handler;
- a tool factory declares a handle that no root binding names for it;
- a nested body (a `loop`, `childWorkflow`, or `onTrigger` body, at any depth) carries `credentialBindings`;
- a `package` binding names no tool factory the workflow runs, or an `action` binding names no action handler that declares credentials;
- an `action` binding names a handle its handler does not declare, or a `package` binding names a handle that its declaring tool factory does not declare;
- a binding names the reserved handle `interchange.hub`;
- two bindings name the same consumer and handle.

The check runs at install and approve, not when a deployment respawns, so a deployment approved earlier keeps running.

## Resolution at Launch Time

The control plane resolves each **credential binding** at launch by the algorithm below, materializes the resulting authorization grant, and delivers the resolved secret to the harness (step 7). The harness gates use at runtime through a per-consumer capability that enforces the materialized grant's consumer scope: each tool package and each action handler receives a capability scoped to its own consumer identity, so one consumer's grant cannot authorize another's resolve. General provider/scope requirement resolution outside the binding mechanism is not yet built.

When the control plane launches an agent into a harness, it processes each credential binding:

1. Resolve the provider by name, walking up the tenant hierarchy
2. Find credentials matching the provider, source (principal filter), and scopes
3. If a name is specified, narrow to that name
4. If multiple credentials still match, the launch fails -- the administrator must resolve the ambiguity
5. Authorize use: a **tenant-owned** credential (`principalId IS NULL`) is authorized by ownership within the hierarchy — the walk-up in step 1–2 already proves it — and the launch stamps a `credential:{id}` / `use` grant with `origin = 'system'` directly (scoped to the binding's consumer). A **principal-owned** credential is not a tenant resource: it is not usable through a shared catalog offering or a credential binding today, and owner-delegated use is Planned / Not Yet Implemented
6. If zero matches, the launch fails (missing required credential)
7. If all checks pass, the resolved credential is included in the harness's launch payload

Step 5 is the ownership boundary: a tenant's own credentials are usable by ownership, with no personal grant to hold. Principal-owned credential use — whether owner-delegated or invoker-_brought_ (an invoker supplying their own credential material for an allowed provider) — remains Planned / Not Yet Implemented.

The whole tool-credential rail is exercised end-to-end by `tests/workflow-deploy/single-step-credential-tool.test.ts`: a real credential-consuming tool package is pinned into a deployed step, its credential delivered on the deploy frame (and, in a second case, over the live rotation push), resolved through the per-consumer capability, and used to authenticate an http request to a mock origin — asserting the exact delivered secret arrives as a bearer, that a rotated secret supersedes it, and that a run lacking the `credential:{id}` / `use` grant fails the resolve closed so nothing reaches the origin.

The action-credential rail is exercised end-to-end by `tests/workflow-deploy/action-step-credential.test.ts`. A top-level action, an action inside a childWorkflow body and an action inside a top-level loop body each resolve their declared handle and authenticate to a mock origin with the exact delivered secret. The test also asserts that deploy stamps each binding with `action:<handler>`, that each body run's grants keep only the use grant for the body's own handler, that a handler no binding names cannot resolve the handle, that a run without the use grant fails closed, and that deploy refuses a declared handle with no binding.

The hub handle is exercised end-to-end by `tests/workflow-deploy/action-hub-access.test.ts`. An action that declares `interchange.hub` calls the real hub REST app over HTTP with the token the trigger minted. It reads its own run with a `workflow-run:self` requirement, and gets 403 for another run, for a route off the allow-list, for another tenant, and for its own run when it has no requirement. Its token never appears in the run's events, and stops working when the caller deactivates the run principal. `tests/workflow-deploy/tool-hub-access.test.ts` shows the same for a tool: an agent step's tool that declares `interchange.hub` reads its own run with the token minted for its `tool:` consumer.

The per-handler consumer is a least-privilege limit, not an isolation boundary. All action handlers of a deployment run in one workflow process and one module scope, so a handler that is hostile can reach state that another handler holds. The capability keeps a correct handler to the handles its own bindings name, and deploy holds those bindings to the handles that the handler's steps declare. It does not contain a handler that is hostile. The rule that credentials work only inside `perform`'s `run` has the same limit: it stops an author mistake, but a hostile handler can call `fetch` without a credential handle.

A local run (`runLocal` in `@intx/workflow`) gives action handlers credentials through its `actionCredentials` option. `createActionCredentialsFactory` in `@intx/harness` builds that factory over a material cell, a grants resolver and a provider registry; the deployed host uses the same builder. The grants resolver receives the run id and the step id, so one factory can serve a run and its child runs. `tests/workflow/run-local-action-credentials.test.ts` exercises it.

### Credential-Use Authorization

Credential use — whether by the inference layer (a tenant-catalog model key) or through a credential binding (a tool package or an action handler) — is authorized the same way: by **tenant ownership within the hierarchy**. A tenant-owned credential (`principalId IS NULL`) is usable by the tenant that owns it and by any descendant tenant, resolved through the same ancestor walk-up that credential, provider, and OAuth resolution already run on. There is no role grant, no per-principal grant, and no seeded authority: a tenant may use what it (or an ancestor) owns, and descendants inherit. The check is fail-closed — a credential that is not tenant-owned, or not reachable in the launching tenant's chain, withholds — and runs at launch and again on rotation, so a rotated or newly-eligible credential is picked up live without materializing any grant.

One ownership rule, two transports. The inference gate (`buildSource`) and the credential-binding path resolve ownership through the same rule — a tenant-owned credential reachable in the launching tenant's ancestor chain — and neither consults a creator, role, or seeded grant. They differ only in what a resolved credential feeds. The inference gate emits the secret straight into a launchable source: its consumer is the harness itself, so no durable grant is needed. A credential binding delivers the secret to its consumer (a tool package or an action handler) and stamps a `credential:{id}` / `use` grant carrying a `{ tool }` condition, which the runtime per-consumer gate reads to decide which consumer may resolve the handle. Only that runtime gate needs a durable grant; ownership itself is settled at resolution and never re-checked. Because ownership is the authority, an agent definition needs no creator to launch a tenant-owned inference source.

A **principal-owned** credential is not a tenant resource: it is not usable through a shared catalog offering or a credential binding today, and it fails closed. Owner-delegated use of a principal-owned credential is Planned / Not Yet Implemented.

Authorization is distinct from secret transport. How the secret is resolved, pushed to the sidecar, and bounded in confidentiality over time is a separate concern — see "Credential Updates and Refresh" for the rotation push. Because tenant-owned credential use is ownership-derived, it is revoked by removing or re-owning the credential rather than by withdrawing a grant.

Revoking authority does not retract a secret already delivered to a running instance. For a tenant-owned credential, authority is ownership, so revocation means removing the credential or moving it out of the subtree. The change pushes nothing to running sidecars and takes effect only when the deployment next re-resolves — the delivered secret is refreshed on the sidecar only by a rotation or catalog edit, which re-resolves and re-pushes the instance's sources. Re-resolution re-checks ownership live, so no re-push delivers a secret the tenant no longer owns. Until a re-resolution occurs, a running agent retains a secret delivered earlier — a confidentiality-bounded revocation latency, not an authorization hole.

## Walk-up Resolution

All three concepts (providers, OAuth clients, credentials) use the same walk-up resolution pattern through the tenant hierarchy. Starting from the current tenant, the system checks for a match, then moves to the parent tenant, and continues until it finds a match or reaches the root.

For providers: resolution is by name. A child tenant's "github" provider shadows the parent's.

For OAuth clients: resolution is by provider. If a child tenant doesn't have an OAuth client for a given provider, the parent's client is used.

For credentials: the live model-source gate resolves a specific credential by ID through this walk-up — the first tenant in the hierarchy with a matching credential wins. Matching a binding's requirement by provider, source, scopes, and optional name is the credential-binding resolution now wired at launch (see "Resolution at Launch Time"); general credential-requirement resolution outside the binding mechanism is not yet built.

This model is consistent with the architecture document's statement that child tenants inherit policies from their parent. The walk-up resolution extends this inheritance to integration configuration and credentials.

## Credential Updates and Refresh

### Credential Rotation Push (Current Behavior)

When a tenant administrator rotates a credential's secret, the control plane pushes the updated inference sources to every running instance in the tenant. The rotation is not filtered to the affected provider — the push takes only the tenant, re-resolves each running instance's full sources array, and sends a `sources.update` frame to the instance's sidecar over the persistent bidirectional connection that sidecars maintain with the control plane (described in the architecture document). The sidecar hot-swaps the active source on the harness and re-persists the agent config.

This push is triggered only by an administrator rotating the secret. It is not an automatic token-refresh loop: the control plane does not, on its own, detect an expiring or invalid token and obtain a new one. The planned proactive and reactive refresh flows below would add that capability; today the only way an updated credential reaches a running instance is an administrator rotation.

### Proactive Refresh

**Planned / Not Yet Implemented.** The behavior in this subsection describes a future design; it is not built today. There is no background process that refreshes OAuth tokens, and the control plane performs no token exchange against provider endpoints.

The planned design: the control plane runs a background process that refreshes OAuth tokens before they expire. It queries credentials approaching expiration, resolves the associated OAuth client and provider, and performs a token refresh using the provider's token endpoint. The refreshed access token and expiry are updated in place. If the provider rotates refresh tokens, the new refresh token is stored as well.

When a credential is refreshed, the control plane pushes the updated credential to every harness that currently holds it. This happens over the persistent bidirectional connection that harnesses maintain with the control plane. The control plane tracks which harnesses hold which credentials based on agent launch records.

### Runtime Token Failure

**Planned / Not Yet Implemented.** The reactive flow in this subsection describes a future design; it is not built today. The harness does not request a refresh from the control plane, and there is no harness-to-control-plane refresh-request frame on the wire.

The planned design: proactive refresh handles the happy path where tokens expire on schedule. The unhappy path -- a provider invalidating a token unexpectedly (service restart, manual revocation, security incident) -- requires a reactive flow.

When the harness encounters an authentication failure (e.g., a 401 response from a provider), it recognizes this as a credential failure and requests a refresh from the control plane over the persistent connection. The control plane receives the request, attempts to refresh the credential using the refresh token and OAuth client, and responds:

- **Refresh succeeds**: the control plane sends the new credential back to the harness. The harness updates its in-memory credential and retries the failed request once.
- **Refresh fails**: the control plane marks the credential as errored and notifies the harness. The harness surfaces the error to the agent as a tool failure. The control plane notifies the credential owner (the principal, or tenant administrators for organizational credentials) that reauthorization is needed.

If multiple harnesses hold the same credential and report failures simultaneously, the control plane deduplicates refresh attempts -- only one actual refresh per credential within a short time window. All requesting harnesses receive the result.

The harness enforces a retry budget: one retry after a successful refresh. If the retried request also fails, the harness surfaces the error without further retry attempts.

### Bidirectional Credential Channel

**Planned / Not Yet Implemented.** The two-directional refresh channel described here is a future design. Today the credential channel carries only the control-plane-to-sidecar rotation push described under "Credential Rotation Push" above; there is no harness-to-control-plane refresh request.

The planned design: the persistent connection between harnesses and the control plane serves both directions of credential lifecycle management:

- **Control plane to harness**: proactive credential updates when the control plane refreshes a token on schedule.
- **Harness to control plane**: reactive refresh requests when the harness encounters an authentication failure at runtime.

This is not two separate mechanisms but two uses of the same bidirectional channel. The control plane pushes updates when it knows a credential has changed. The harness requests updates when it discovers a credential is no longer valid.

## Access Control

Credential access is governed by the existing grant-based authorization system. Grants determine which principals can use which credentials. The agent definition references credentials by capability, but the grant system validates access by credential ID after resolution.

The key constraint is that a definition creator cannot grant an agent access to credentials the creator doesn't have access to themselves. For a **principal-owned** credential this is the setuid delegation check at launch: the resolved `credential:{id}` / `use` requirement is validated against the binding authority's own grants (see "Resolution at Launch Time"). A **tenant-owned** credential needs no such delegation — it is authorized by tenant ownership within the hierarchy (see "Credential-Use Authorization"), so any agent launched in the owning subtree may use it, uniformly for the inference layer and for tools. Invoker-_brought_ credential material remains Planned / Not Yet Implemented.

## OAuth2 Implementation and better-auth

Interchange uses better-auth for user authentication (signing into Interchange itself). The credential system uses better-auth's infrastructure for a different purpose: OAuth2 integration flows that obtain tokens for agents to use with third-party services.

These are two distinct concerns. Authentication proves "I am Alice so I can sign into Interchange." Integration proves "Alice authorizes Interchange to act on her behalf with GitHub so her agents can access her repos." They use the same OAuth2 protocol machinery but have different lifecycles, different storage, and different consumers.

### Reusing better-auth's OAuth2 Primitives

better-auth ships OAuth2 protocol primitives in `@better-auth/core/oauth2`: authorization URL construction with PKCE and state management, authorization code exchange, and token refresh. These are pure functions that implement the OAuth2 spec without any coupling to better-auth's identity or session model.

better-auth also ships provider-specific implementations in `@better-auth/core/social-providers`. Each provider (GitHub, Google, Slack, Linear, Discord, Notion, Spotify, and many others) is a self-contained object that knows the provider's endpoints, default scopes, token exchange quirks, and user info API. These provider objects use the core OAuth2 primitives internally but add the provider-specific wiring that makes each integration work correctly.

The credential system uses these provider objects directly as standalone OAuth2 clients. When a user connects a service, the system instantiates the appropriate provider object with the `client_id` and `client_secret` from the tenant's OAuth client registration, then calls its methods to drive the authorization flow. The resulting tokens are stored in the credential table, not in better-auth's account table.

### How the Provider Plugin Field Maps

The `plugin` field on the provider table determines which implementation handles the OAuth2 flow:

For well-known providers, the plugin field maps to a better-auth social provider factory. A provider with `plugin: "github"` uses the GitHub provider from `@better-auth/core/social-providers`, which knows GitHub's authorization endpoint, token endpoint, default scopes, and API quirks. The same applies for Google, Slack, Linear, Discord, Notion, Figma, Atlassian, Dropbox, and the rest of better-auth's provider catalog.

For custom OAuth2 providers not in better-auth's list, the plugin field is set to `"oauth2"` and the provider table's `authorizationUrl`, `tokenUrl`, and other fields are used directly with the core `@better-auth/core/oauth2` primitives.

For non-OAuth providers (API key services, certificate-based auth), the plugin field indicates the authentication method (e.g., `"api-key"`). No OAuth flow is involved; credentials are created manually and stored directly.

### Separation of Concerns

better-auth owns user authentication: sign-in, sign-up, session management, the `user`, `session`, and `account` tables. The credential system does not read from or write to these tables.

The credential system owns integration credentials: the `provider`, `oauth_client`, and `credential` tables, tenant hierarchy walk-up, principal ownership, grant-based access control, the rotation push to running instances, and (as planned, not-yet-implemented work) proactive refresh. It uses better-auth's OAuth2 protocol machinery as a library dependency, not as a framework it runs inside of.

This separation means the two systems can evolve independently. Adding a new OAuth provider to better-auth's catalog automatically makes it available for integration credentials. Changes to better-auth's session or account model do not affect credential storage. The credential system's tenant-scoped, principal-aware storage model is entirely its own concern.

### Token Refresh

**Planned / Not Yet Implemented.** Token refresh is not built today; the control plane performs no token exchange against provider endpoints. The design below records how the better-auth primitives are intended to be used once the proactive and reactive refresh flows (see "Credential Updates and Refresh") are implemented.

For proactive refresh, the control plane would use the same provider objects that performed the original OAuth flow. The provider's `refreshAccessToken` method handles provider-specific refresh behavior. The control plane reads the refresh token from the credential table, calls the provider's refresh method, and updates the credential table with the new access token and expiry.

For reactive refresh (when a harness reports a 401), the same path applies: the control plane resolves the provider, calls its refresh method, and pushes the result back to the harness.

## Encryption at Rest

Credential secrets are encrypted at rest: the credential `secret` and `refresh_secret` columns and the OAuth client's `client_secret` column are stored as AES-256-GCM ciphertext, never plaintext. Each ciphertext is bound to its row id and column as authenticated data, so a ciphertext cannot be transplanted between rows or columns and still decrypt. Encryption happens at the write boundary and decryption at the single point of use (an inference source's `apiKey`); the decrypt is strict, so a value that is not a ciphertext fails closed rather than being delivered as a key.

Encryption is a pluggable seam, the `CredentialCipher`. The built-in implementation encrypts under a single operator-provided key, `CREDENTIAL_ENCRYPTION_KEY` (32 bytes, hex); the hub refuses to start without it. A future KMS or envelope-encryption plugin implements the same seam and can keep key material inside the KMS without touching any call site.

The same cipher seam seals per-principal signing keys at rest, under a separate key `PRINCIPAL_KEY_ENCRYPTION_KEY` so the two rotate independently. See [AUTH.md](./AUTH.md) for the principal signing-key custody and attribution model.

The threat model this addresses is **database-only compromise** — a stolen backup, a replica leak, a SQL-injection read, a snoop over the table. It does **not** defend against full host compromise, where the attacker holds both the key and the data; that requires a KMS/envelope plugin.

Deploying encryption over a database whose rows predate it requires a one-time re-key of the legacy plaintext. With the hub **stopped** (so no write races an un-re-keyed row against the strict read path), run `bin/rekey-credential-secrets.ts` with the hub's environment sourced, then start the hub. The pass is idempotent — already-encrypted rows are skipped — so it is safe to re-run or resume.

## What This Design Does Not Cover

- **Harness injection protocol**: the mechanism by which the control plane transmits resolved credentials to harnesses is not specified here. It depends on the harness implementation and deployment environment. In the prototype sidecar implementation, credentials arrive in the initial `agent.deploy` frame and are persisted in the allocation-scoped deployment record for same-capacity restoration. See HARNESS_DESIGN.md for the wire protocol.
