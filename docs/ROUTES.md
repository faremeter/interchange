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

Agent definitions (`/agents/definitions`) and agent instances (`/agents/instances`) are sibling route groups under the tenant's `/agents` namespace rather than instances being nested under definitions (`/agents/definitions/:agentId/instances`).

Nesting would mean listing all instances in a tenant requires knowing all agent IDs first. It also creates awkward paths when an agent definition is retired but its historical instances are still queryable. Instances are first-class tenant resources alongside wallets, credentials, and approvals.

### Why cross-tenant reads are separate endpoints

A user who belongs to multiple tenants needs dashboard views: "all my agents across all orgs", "all pending approvals". Rather than making the tenant-scoped endpoints optionally cross-tenant (via an absent header or special parameter), we provide explicit endpoints under `/api/me/...`.

This is clearer for clients: `/api/me/agents` is a different operation from `/api/tenants/:tenantId/agents`. The former aggregates across tenants and tags each result with `tenantId`. The latter is scoped to one tenant. No mode-switching, no ambiguity about what "list agents" means in a given context.

Only resources that benefit from cross-tenant aggregation have `/api/me/...` endpoints: agents, instances, and approvals. These are the "dashboard" resources -- what's running, what's active, what needs my attention.

## Conventions

### Authentication

All endpoints except `/status` and `/openapi.json` require authentication via better-auth session token (cookie or `Authorization: Bearer`).

### Principal resolution

Every tenant-scoped request resolves the authenticated user's principal within the target tenant. The principal determines membership status and, combined with its capability grants and roles, what the user is authorized to do. See `docs/AUTH.md` for the full authorization model.

### IDs

All IDs are globally unique with typed prefixes:

- `tnt_` -- tenant
- `prn_` -- principal
- `agt_` -- agent
- `rol_` -- role
- `ofr_` -- offering
- `ins_` -- instance
- `ses_` -- session (internal, not exposed in the API)
- `msg_` -- message
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

Request validation uses ArkType types shared between server and clients via the `@interchange/types` package. Validation is enforced at the route level -- invalid requests receive a 400 response before reaching business logic.

Response types are documented in the OpenAPI spec via the same ArkType types but are not validated at runtime (the server is trusted to produce correct responses).

## Authorization Through Capability Grants

There are no dedicated "binding" endpoints for connecting resources to principals. No `POST /wallets/:walletId/agents/:agentId` to give an agent wallet access. No `POST /credentials/:credentialId/agents/:agentId` to bind a credential.

Instead, all authorization flows through capability grants. Granting an agent wallet access is creating a grant: `{ resource: "wallet:wal_abc", action: "spend", principal_id: "prn_xyz" }`. Revoking it is deleting that grant. This is one mechanism for all authorization -- users, agents, tools, wallets, credentials, APIs.

This means the grant endpoints (`/api/tenants/:tenantId/grants`) are the universal authorization management surface. The evaluate endpoint (`/api/tenants/:tenantId/principals/:principalId/evaluate`) lets operators debug authorization by asking "what would happen if this principal tried to do X?"

## Approvals as First-Class Resources

When an agent encounters a capability grant with `effect: "ask"`, execution blocks and an approval request is created. These approvals are not buried inside instance state. They are top-level resources under `/api/tenants/:tenantId/approvals` with their own cross-tenant view at `/api/me/approvals`.

This design supports mobile clients that need notification-driven approval flows. A user gets a push notification, opens the app, sees the pending approval with full context (what action, which agent, which instance), and approves or rejects. The approval flow is independent of the instance UI.

When a user approves with `scope: "always"`, the system creates a persistent capability grant so the agent won't need to ask again for the same operation. This is the bridge between interactive approval and long-term authorization policy.

## Messages and Streaming

`POST .../agents/instances/:instanceId/messages` persists the user's message and returns. The agent's response does not come back in the HTTP response. Instead, it streams over the instance channel (WebSocket or SSE).

This "fire-and-forget via REST, stream via channel" pattern matches the architecture's "persist first, stream second" principle. The durable record (message in the database) is always ahead of or equal to the stream. If the client disconnects, nothing is lost -- they catch up by fetching messages via the REST endpoint.

## Instance Channel Protocol

The instance channel is a real-time overlay for interactive use cases. It is not fully expressible in the OpenAPI spec, so the protocol details are documented here.

### SSE Event Stream

The current implementation uses Server-Sent Events at `GET .../api/tenants/:tenantId/agents/instances/:instanceId/events`. Client-to-server messages use the REST `POST .../messages` endpoint.

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

Reconnection: Instance channels are ephemeral. On disconnect, the client reconnects and fetches missed messages via the REST `GET .../messages` endpoint. No token-level resume -- tokens are ephemeral previews of content that is persisted as complete messages.

### WebSocket Instance Channel (Future)

The architecture specifies a WebSocket instance channel at `wss://.../api/tenants/:tenantId/agents/instances/:instanceId/stream` with JWT authentication. This is not yet implemented. The SSE endpoint serves all current user-facing streaming needs.

### Debug and Telemetry Streams

Debug and telemetry data (state inspection, trace output, log tailing) flows only over the instance channel. It is not part of the durable message record. Requires explicit authorization -- not all clients are permitted to attach debuggers to agents.
