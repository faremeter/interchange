# API Routes

Design philosophy and conventions for the Interchange control plane REST API. This document explains the _why_ behind the route structure. For the authoritative route reference with request/response types, see the generated OpenAPI spec at `/openapi.json`.

## Scope

This API is the control plane's HTTP interface for human users and client applications (web, mobile). Clients authenticate, discover agents, deploy instances, send messages, manage wallets, and observe system state through this API.

This API is not:

- **The harness's internal API.** Harnesses talk to the control plane via a separate persistent connection.
- **Agent-to-agent communication.** That goes through SMTP/IMAP.
- **The instance channel protocol.** That uses WebSocket with an SSE fallback (documented below since WebSocket semantics are not fully expressible in OpenAPI).

## Path Structure

Every route falls into one of three scopes:

- **`/api/tenants/:tenantId/...`** -- Tenant-scoped operations. All reads and writes within a tenant.
- **`/api/me/...`** -- User-scoped, cross-tenant reads. Dashboard views aggregating across all tenants the user belongs to.
- **`/api/...`** -- Global operations. Auth, tenant creation, system health, model catalog.

### Why tenant is always in the path

Tenant context is encoded in the URL, not in a header. This was a deliberate choice:

- **No ambiguity.** The URL is the full context. There is no implicit state to manage, forget, or misconfigure.
- **Loggable and cacheable.** Every request's scope is visible in access logs, cache keys, and browser history without inspecting headers.
- **Mobile-friendly.** Native mobile clients don't need to manage header state across navigation. The URL carries everything.
- **RESTful.** Resources are identified by their path. A tenant-scoped agent lives at a tenant-scoped path.

The tradeoff is longer URLs. `/api/tenants/tnt_abc/agents/agt_xyz` is more verbose than `/api/agents/agt_xyz`. This is acceptable -- IDs are globally unique and URLs are for machines, not humans.

### Why agent definitions and instances are separate route groups

Agent definitions (`/agents/definitions`) and agents (`/agents/instances`) are sibling route groups under the tenant's `/agents` namespace rather than agents being nested under definitions (`/agents/definitions/:agentId/instances`).

Definitions are catalog entries and blueprints — they describe what an agent can do. Agents are the live running entities with state, addresses, principals, and offerings. They are siblings because they are different resource types under the same namespace: one is data, the other is a runtime entity.

Nesting would mean listing all agents in a tenant requires knowing all definition IDs first. It also creates awkward paths when a definition is retired but its historical agents are still queryable. Agents are first-class tenant resources alongside wallets, credentials, and approvals.

Runtime state — data, history, branches, health, logs, metrics — lives on agent paths (`/agents/instances/:instanceId/...`), not on definition paths. This follows from the model: runtime state belongs to the running agent, not the blueprint. Definition paths carry versioning, rollback, and catalog-level offerings.

### Why cross-tenant reads are separate endpoints

A user who belongs to multiple tenants needs dashboard views: "all my definitions across all orgs", "all my running agents", "all pending approvals". Rather than making the tenant-scoped endpoints optionally cross-tenant (via an absent header or special parameter), we provide explicit endpoints under `/api/me/...`.

This is clearer for clients: `/api/me/agents/definitions` is a different operation from `/api/tenants/:tenantId/agents/definitions`. The former aggregates across tenants and tags each result with `tenantId`. The latter is scoped to one tenant. No mode-switching, no ambiguity about what "list definitions" means in a given context.

Only resources that benefit from cross-tenant aggregation have `/api/me/...` endpoints: definitions, agents, and approvals. These are the "dashboard" resources -- what's defined, what's running, what needs my attention.

## Conventions

### Authentication

All endpoints except `/status` and `/openapi.json` require authentication via better-auth session token (cookie or `Authorization: Bearer`).

### Principal resolution

Every tenant-scoped request resolves the authenticated user's principal within the target tenant. The principal determines membership status and, combined with its capability grants and roles, what the user is authorized to do. See `docs/AUTH.md` for the full authorization model.

### IDs

All IDs are globally unique with typed prefixes:

- `tnt_` -- tenant
- `prn_` -- principal
- `agt_` -- agent definition
- `rol_` -- role
- `ofr_` -- offering
- `ins_` -- agent
- `ses_` -- session (internal, not exposed in the API)
- `mail_` -- session mail record
- `turn_` -- inference turn
- `wal_` -- wallet
- `crd_` -- credential
- `apr_` -- approval

IDs embed timestamps for natural ordering. Prefixes make it impossible to confuse a session ID for an agent ID in logs, URLs, or bug reports.

### Pagination

All list endpoints use cursor-based pagination: `?cursor=X&limit=N`. Cursors are opaque strings. The response includes `nextCursor` (null when no more results). Default limit is 50, maximum is 100.

Cursor-based pagination avoids the consistency problems of offset-based pagination (inserted/deleted rows shifting pages) and is efficient for the underlying queries.

### Error envelope

All errors follow a consistent shape:

```json
{ "error": { "code": "not_found", "message": "Agent not found" } }
```

Error codes are machine-readable strings (not HTTP status codes). The message is human-readable. HTTP status codes are used correctly (400 for validation, 403 for authorization, 404 for not found, 501 for unimplemented stubs) but the error code in the body provides finer granularity.

### Validation

Request validation uses ArkType types shared between server and clients via the `@intx/types` package. Validation is enforced at the route level -- invalid requests receive a 400 response before reaching business logic.

Response types are documented in the OpenAPI spec via the same ArkType types but are not validated at runtime (the server is trusted to produce correct responses).

## Authorization Through Capability Grants

There are no dedicated "binding" endpoints for connecting resources to principals. No `POST /wallets/:walletId/agents/:agentId` to give an agent wallet access. No `POST /credentials/:credentialId/agents/:agentId` to bind a credential.

Instead, all authorization flows through capability grants. The grant endpoints (`/api/tenants/:tenantId/grants`) manage tenant-level policies and role-based grants. These are the materialized grants that live on principals. The evaluate endpoint (`/api/tenants/:tenantId/principals/:principalId/evaluate`) lets operators debug authorization by asking "what would happen if this principal tried to do X?"

Agent definitions declare grant requirements with source annotations (tenant, creator, invoker). These requirements are resolved at launch time by the control plane, which materializes grants on the agent's new principal. Creator-sourced and invoker-sourced grants are resolved against the respective principal's authority at launch time. Tenant-sourced grants are resolved from tenant role and system policies. The grant API endpoints manage the tenant-level policy layer; the launch flow handles per-agent materialization.

## Approvals as First-Class Resources

When an agent encounters a capability grant with `effect: "ask"`, execution blocks and an approval request is created. These approvals are not buried inside instance state. They are top-level resources under `/api/tenants/:tenantId/approvals` with their own cross-tenant view at `/api/me/approvals`.

This design supports mobile clients that need notification-driven approval flows. A user gets a push notification, opens the app, sees the pending approval with full context (what action, which agent, which instance), and approves or rejects. The approval flow is independent of the instance UI.

When a user approves with `scope: "always"`, the system creates a persistent capability grant so the agent won't need to ask again for the same operation. This is the bridge between interactive approval and long-term authorization policy.

## Mail and Streaming

Mail is the first-class communication primitive. The hub stores raw MIME bytes at routing time and serves parsed views following the JMAP Email object model (RFC 8621).

`POST .../agents/instances/:instanceId/mail` persists the user's message as a mail record and dispatches it to the running agent. The agent's response does not come back in the HTTP response. Instead, it streams over the agent's channel (WebSocket or SSE).

`GET .../agents/instances/:instanceId/mail` returns cursor-paginated JMAP Email objects for the instance, in reverse chronological order.

`GET .../agents/instances/:instanceId/turns` returns cursor-paginated inference turns with their parts. One turn per inference cycle. Turns capture the agent's internal reasoning trace separately from the mail record.

`GET .../blobs/:blobId` returns raw bytes for a MIME attachment part. Blob IDs are embedded in JMAP Email responses by the mail parsing layer, using the format `blob_<mailId>_<partPath>` where `partPath` is an IMAP-style section specifier (e.g. `1.3`). The caller must hold `read` access on the containing instance.

This "fire-and-forget via REST, stream via channel" pattern matches the architecture's "persist first, stream second" principle. The durable record (mail in the database) is always ahead of or equal to the stream. If the client disconnects, nothing is lost -- they catch up by fetching mail via the REST endpoint.

## Agent Channel Protocol

The agent channel is a real-time overlay for interactive use cases. It is not fully expressible in the OpenAPI spec, so the protocol details are documented here.

### SSE Event Stream

The current implementation uses Server-Sent Events at `GET .../api/tenants/:tenantId/agents/instances/:instanceId/events`. Client-to-server messages use the REST `POST .../mail` endpoint.

Event format: JSON objects with a `type` field and `data` payload.

```json
{"type": "inference.start", "data": {"model": "claude-sonnet-4-20250514"}}
{"type": "inference.text.delta", "data": {"token": "Hello"}}
{"type": "inference.thinking.delta", "data": {"token": "Let me think..."}}
{"type": "inference.tool_call.start", "data": {"name": "web_search", "callId": "call_xyz"}}
{"type": "inference.done", "data": {"message": {...}, "usage": {...}}}
{"type": "tool.start", "data": {"callId": "call_xyz"}}
{"type": "tool.done", "data": {"result": {"callId": "call_xyz", "content": "..."}}}
{"type": "connector.reply", "data": {}}
{"type": "reactor.done", "data": {}}
{"type": "reactor.error", "data": {"error": "...", "fatal": true}}
```

Reconnection: Agent channels are ephemeral. On disconnect, the client reconnects and fetches missed mail via the REST `GET .../mail` endpoint. No token-level resume -- tokens are ephemeral previews of content that is persisted as complete mail records.

### WebSocket Agent Channel (Future)

The architecture specifies a WebSocket agent channel at `wss://.../api/tenants/:tenantId/agents/instances/:instanceId/stream` with JWT authentication. This is not yet implemented. The SSE endpoint serves all current user-facing streaming needs.

### Debug and Telemetry Streams

Debug and telemetry data (state inspection, trace output, log tailing) flows only over the agent channel. It is not part of the durable message record. Requires explicit authorization -- not all clients are permitted to attach debuggers to agents.
