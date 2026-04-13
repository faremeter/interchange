# API Routes

Design philosophy and conventions for the Interchange control plane REST API. This document explains the _why_ behind the route structure. For the authoritative route reference with request/response types, see the generated OpenAPI spec at `/openapi.json`.

## Scope

This API is the control plane's HTTP interface for human users and client applications (web, mobile). Clients authenticate, discover agents, start sessions, send messages, manage wallets, and observe system state through this API.

This API is not:

- **The harness's internal API.** Harnesses talk to the control plane via a separate persistent connection.
- **Agent-to-agent communication.** That goes through SMTP/IMAP.
- **The session channel protocol.** That uses WebSocket with an SSE fallback (documented below since WebSocket semantics are not fully expressible in OpenAPI).

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

### Why sessions are tenant-scoped, not agent-scoped

Sessions are created _for_ an agent, but they are owned by the tenant. A session is a conversation between a principal and an agent, within a tenant context. The agent ID is a property of the session (specified at creation, filterable on list), not a path component.

Putting sessions under agents (`/api/tenants/:tenantId/agents/:agentId/sessions`) would mean listing all sessions in a tenant requires knowing all agent IDs first. It also creates awkward paths when an agent is retired but its sessions are still queryable for history. Sessions belong alongside wallets, credentials, and approvals as first-class tenant resources.

### Why cross-tenant reads are separate endpoints

A user who belongs to multiple tenants needs dashboard views: "all my agents across all orgs", "all pending approvals". Rather than making the tenant-scoped endpoints optionally cross-tenant (via an absent header or special parameter), we provide explicit endpoints under `/api/me/...`.

This is clearer for clients: `/api/me/agents` is a different operation from `/api/tenants/:tenantId/agents`. The former aggregates across tenants and tags each result with `tenantId`. The latter is scoped to one tenant. No mode-switching, no ambiguity about what "list agents" means in a given context.

Only resources that benefit from cross-tenant aggregation have `/api/me/...` endpoints: agents, sessions, and approvals. These are the "dashboard" resources -- what's running, what's active, what needs my attention.

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
- `ses_` -- session
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

When an agent encounters a capability grant with `effect: "ask"`, execution blocks and an approval request is created. These approvals are not buried inside session state. They are top-level resources under `/api/tenants/:tenantId/approvals` with their own cross-tenant view at `/api/me/approvals`.

This design supports mobile clients that need notification-driven approval flows. A user gets a push notification, opens the app, sees the pending approval with full context (what action, which agent, which session), and approves or rejects. The approval flow is independent of the session UI.

When a user approves with `scope: "always"`, the system creates a persistent capability grant so the agent won't need to ask again for the same operation. This is the bridge between interactive approval and long-term authorization policy.

## Messages and Streaming

`POST .../sessions/:sessionId/messages` persists the user's message and returns. The agent's response does not come back in the HTTP response. Instead, it streams over the session channel (WebSocket or SSE).

This "fire-and-forget via REST, stream via channel" pattern matches the architecture's "persist first, stream second" principle. The durable record (message in the database) is always ahead of or equal to the stream. If the client disconnects, nothing is lost -- they catch up by fetching messages via the REST endpoint.

## Session Channel Protocol

The session channel is a real-time overlay for interactive use cases. It is not fully expressible in the OpenAPI spec, so the protocol details are documented here.

### WebSocket

Primary transport at `wss://.../api/tenants/:tenantId/sessions/:sessionId/stream`.

Authentication: The client sends the session token (JWT from session creation) in the first message after connection.

Message format: JSON objects with a `type` field and `data` payload.

```json
{"type": "inference.start", "data": {"model": "claude-sonnet-4-20250514"}}
{"type": "inference.token", "data": {"token": "Hello", "seq": 1}}
{"type": "inference.done", "data": {"seq": 42, "messageId": "msg_abc123"}}
{"type": "user.message", "data": {"content": "What's the weather?"}}
{"type": "tool.start", "data": {"toolId": "web_search", "callId": "call_xyz"}}
{"type": "tool.result", "data": {"callId": "call_xyz", "output": "..."}}
{"type": "approval.requested", "data": {"approvalId": "apr_xyz", "action": "..."}}
{"type": "debug.state", "data": {"contextTokens": 4096}}
{"type": "system.ping", "data": {"ts": 1699999999}}
{"type": "system.pong", "data": {"ts": 1699999999}}
```

Heartbeats: Both sides send `system.ping`/`system.pong` every 30 seconds. Connections without activity for 30 seconds are terminated.

Reconnection: Session channels are ephemeral. On disconnect, the client reconnects, re-authenticates, and fetches missed messages via the REST endpoint. No token-level resume -- tokens are ephemeral previews of content that is persisted as complete messages.

### SSE Fallback

For clients that cannot use WebSocket (some embedded environments, restrictive firewalls): `GET .../api/tenants/:tenantId/sessions/:sessionId/events`.

Same event types as WebSocket, server-to-client only. Client-to-server messages use the REST `POST .../messages` endpoint.

### Debug and Telemetry Streams

Debug and telemetry data (state inspection, trace output, log tailing) flows only over the session channel. It is not part of the durable message record. Requires explicit authorization -- not all clients are permitted to attach debuggers to agents.
