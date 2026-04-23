# Interchange Hub API

## Endpoint Index

| Method | Path | Summary |
| ------ | ---- | ------- |
| GET | /api/me | Get current user profile |
| GET | /api/me/principals | List principals across all tenants |
| GET | /api/me/agents | List agents across all tenants |
| GET | /api/me/instances | List instances across all tenants |
| GET | /api/me/approvals | List pending approvals across all tenants |
| POST | /api/tenants | Create a tenant |
| GET | /api/tenants/:tenantId | Get tenant details |
| PATCH | /api/tenants/:tenantId | Update tenant config |
| GET | /api/tenants/:tenantId/federation | List federation trust relationships |
| POST | /api/tenants/:tenantId/federation | Establish federation trust |
| DELETE | /api/tenants/:tenantId/federation/:targetTenantId | Revoke federation trust |
| GET | /api/models | List available models |
| GET | /api/tenants/:tenantId/principals | List principals in the tenant |
| GET | /api/tenants/:tenantId/principals/:principalId | Get principal details |
| PATCH | /api/tenants/:tenantId/principals/:principalId | Update principal status |
| DELETE | /api/tenants/:tenantId/principals/:principalId | Remove principal from tenant |
| POST | /api/tenants/:tenantId/members/invite | Invite a user to the tenant |
| GET | /api/tenants/:tenantId/roles | List roles in the tenant |
| POST | /api/tenants/:tenantId/roles | Create a custom role |
| GET | /api/tenants/:tenantId/roles/:roleId | Get role details |
| PATCH | /api/tenants/:tenantId/roles/:roleId | Update a role |
| DELETE | /api/tenants/:tenantId/roles/:roleId | Delete a custom role |
| POST | /api/tenants/:tenantId/principals/:principalId/roles/:roleId | Assign a role to a principal |
| DELETE | /api/tenants/:tenantId/principals/:principalId/roles/:roleId | Remove a role from a principal |
| GET | /api/tenants/:tenantId/grants | List capability grants in the tenant |
| POST | /api/tenants/:tenantId/grants | Create a capability grant |
| GET | /api/tenants/:tenantId/grants/:grantId | Get grant details |
| PATCH | /api/tenants/:tenantId/grants/:grantId | Update a grant |
| DELETE | /api/tenants/:tenantId/grants/:grantId | Revoke a grant |
| POST | /api/tenants/:tenantId/principals/:principalId/evaluate | Evaluate grants for a principal |
| GET | /api/tenants/:tenantId/agents/definitions | List agent definitions |
| POST | /api/tenants/:tenantId/agents/definitions | Create an agent definition |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId | Get agent definition details |
| PATCH | /api/tenants/:tenantId/agents/definitions/:agentId | Update agent definition |
| DELETE | /api/tenants/:tenantId/agents/definitions/:agentId | Retire an agent |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/versions | List agent versions |
| POST | /api/tenants/:tenantId/agents/definitions/:agentId/rollback | Rollback to a previous version |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/health | Get agent health status |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/offerings | List agent offerings |
| POST | /api/tenants/:tenantId/agents/instances | Deploy an agent instance |
| GET | /api/tenants/:tenantId/agents/instances | List agent instances |
| GET | /api/tenants/:tenantId/agents/instances/:instanceId | Get instance detail |
| DELETE | /api/tenants/:tenantId/agents/instances/:instanceId | Stop an instance |
| POST | /api/tenants/:tenantId/agents/instances/:instanceId/messages | Send a message to the agent |
| GET | /api/tenants/:tenantId/agents/instances/:instanceId/messages | List messages for an instance |
| POST | /api/tenants/:tenantId/agents/instances/:instanceId/abort | Abort current operation |
| GET | /api/tenants/:tenantId/agents/instances/:instanceId/events | SSE event stream |
| GET | /api/tenants/:tenantId/approvals | List pending approvals in the tenant |
| GET | /api/tenants/:tenantId/approvals/:approvalId | Get approval details |
| POST | /api/tenants/:tenantId/approvals/:approvalId/approve | Approve an action |
| POST | /api/tenants/:tenantId/approvals/:approvalId/reject | Reject an action |
| GET | /api/tenants/:tenantId/wallets | List wallets in the tenant |
| POST | /api/tenants/:tenantId/wallets | Create a wallet |
| GET | /api/tenants/:tenantId/wallets/:walletId | Get wallet details |
| PATCH | /api/tenants/:tenantId/wallets/:walletId | Update wallet config |
| DELETE | /api/tenants/:tenantId/wallets/:walletId | Deactivate a wallet |
| GET | /api/tenants/:tenantId/wallets/:walletId/transactions | List transactions |
| GET | /api/tenants/:tenantId/credentials | List credentials |
| POST | /api/tenants/:tenantId/credentials | Store a credential |
| GET | /api/tenants/:tenantId/credentials/:credentialId | Get credential metadata |
| PATCH | /api/tenants/:tenantId/credentials/:credentialId | Rotate or update a credential |
| DELETE | /api/tenants/:tenantId/credentials/:credentialId | Revoke a credential |
| GET | /api/tenants/:tenantId/offerings | Search offerings |
| GET | /api/tenants/:tenantId/offerings/:offeringId | Get offering details |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/logs | Get agent logs |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/metrics | Get agent metrics |
| GET | /api/tenants/:tenantId/traces | Query distributed traces |
| GET | /api/tenants/:tenantId/traces/:traceId | Get a full trace |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/data | List files in agent working directory |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/data/* | Read a file from agent storage |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/history | List commits and checkpoints |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/history/:ref | Show changes in a commit |
| GET | /api/tenants/:tenantId/agents/definitions/:agentId/branches | List branches |
| POST | /api/tenants/:tenantId/agents/definitions/:agentId/history/:ref/restore | Restore agent data to a previous state |

## User

### GET /api/me
Get current user profile

200: UserProfile -- User profile
401: ErrorResponse -- Not authenticated

### GET /api/me/principals
List principals across all tenants

Returns all of the authenticated user's principals across tenants, with tenant name, roles, and status in each.

200: PrincipalSummary[] -- List of principals across tenants
401: ErrorResponse -- Not authenticated

### GET /api/me/agents
List agents across all tenants

Aggregates agents from all tenants the user belongs to. Each result is tagged with tenantId.

200: AgentSummary[] -- Agents across tenants

### GET /api/me/instances
List instances across all tenants

Aggregates running agent instances from all tenants the user belongs to. Each result is tagged with tenantId.

200: InstanceSummary[] -- Instances across tenants

### GET /api/me/approvals
List pending approvals across all tenants

Aggregates pending approval requests from all tenants the user belongs to. Each result is tagged with tenantId.

200: ApprovalSummary[] -- Approvals across tenants

## Tenants

### POST /api/tenants
Create a tenant

Creates a new tenant. The authenticated user becomes the owner with a principal and default owner role.

Body: CreateTenant

201: TenantResponse -- Tenant created
400: ErrorResponse -- Validation error

### GET /api/tenants/:tenantId
Get tenant details

200: TenantResponse -- Tenant details
403: ErrorResponse -- Not a member of this tenant
404: ErrorResponse -- Tenant not found

### PATCH /api/tenants/:tenantId
Update tenant config

Requires admin or higher grant within the tenant.

Body: UpdateTenant

200: TenantResponse -- Tenant updated
403: ErrorResponse -- Insufficient grants

### GET /api/tenants/:tenantId/federation
List federation trust relationships

200: FederationTrust[] -- Federation trusts

### POST /api/tenants/:tenantId/federation
Establish federation trust

Creates a trust relationship with another tenant for cross-tenant agent discovery and interaction.

Body: CreateFederationTrust

201: FederationTrust -- Trust established
400: ErrorResponse -- Validation error

### DELETE /api/tenants/:tenantId/federation/:targetTenantId
Revoke federation trust

204: (no content) -- Trust revoked
404: ErrorResponse -- Trust not found

## Discovery

### GET /api/models
List available models

Lists available models across configured providers with capabilities, pricing, and limits.

200: ModelInfo[] -- List of models

### GET /api/tenants/:tenantId/offerings
Search offerings

Searches offerings across discoverable agents in the tenant and federated tenants. Filterable by offering name, pricing range, and payment method.

Query: name?, minPrice?, maxPrice?, paymentMethod?

200: OfferingDetail[] -- List of offerings

### GET /api/tenants/:tenantId/offerings/:offeringId
Get offering details

Returns pricing, agent info, and request/response type information.

200: OfferingDetail -- Offering details
404: ErrorResponse -- Offering not found

## Principals

### GET /api/tenants/:tenantId/principals
List principals in the tenant

Lists all principals (users and agents) in the tenant. Filterable by kind and status.

Query: kind?: user|agent, status?: active|suspended|invited|deactivated

200: PrincipalResponse[] -- List of principals

### GET /api/tenants/:tenantId/principals/:principalId
Get principal details

Returns principal details including kind, status, assigned roles, and effective grants.

200: PrincipalResponse -- Principal details
404: ErrorResponse -- Principal not found

### PATCH /api/tenants/:tenantId/principals/:principalId
Update principal status

Activate, suspend, or deactivate a principal.

Body: UpdatePrincipal

200: PrincipalResponse -- Principal updated
403: ErrorResponse -- Insufficient grants

### DELETE /api/tenants/:tenantId/principals/:principalId
Remove principal from tenant

Removes a user or agent principal from the tenant. For agents, use agent deletion instead.

204: (no content) -- Principal removed
403: ErrorResponse -- Insufficient grants

### POST /api/tenants/:tenantId/members/invite
Invite a user to the tenant

Invites a user by email. Creates a principal with invited status and optionally assigns a role.

Body: InviteMember

201: PrincipalResponse -- Invitation sent
400: ErrorResponse -- Validation error

## Roles

### GET /api/tenants/:tenantId/roles
List roles in the tenant

Lists both system roles (owner, admin, member) and custom roles.

200: RoleResponse[] -- List of roles

### POST /api/tenants/:tenantId/roles
Create a custom role

Body: CreateRole

201: RoleResponse -- Role created
400: ErrorResponse -- Validation error

### GET /api/tenants/:tenantId/roles/:roleId
Get role details

Returns role details including attached capability grants.

200: RoleResponse -- Role details
404: ErrorResponse -- Role not found

### PATCH /api/tenants/:tenantId/roles/:roleId
Update a role

Update name or description. System roles cannot be modified.

Body: UpdateRole

200: RoleResponse -- Role updated
403: ErrorResponse -- Cannot modify system role

### DELETE /api/tenants/:tenantId/roles/:roleId
Delete a custom role

Deletes a custom role. Fails if principals are currently assigned to it. System roles cannot be deleted.

204: (no content) -- Role deleted
400: ErrorResponse -- Role still assigned to principals
403: ErrorResponse -- Cannot delete system role

### POST /api/tenants/:tenantId/principals/:principalId/roles/:roleId
Assign a role to a principal

Assigns a role to a user or agent principal within the tenant.

204: (no content) -- Role assigned
404: ErrorResponse -- Principal or role not found

### DELETE /api/tenants/:tenantId/principals/:principalId/roles/:roleId
Remove a role from a principal

204: (no content) -- Role removed
404: ErrorResponse -- Assignment not found

## Grants

### GET /api/tenants/:tenantId/grants
List capability grants in the tenant

Lists all capability grants. Filterable by principalId, roleId, resource pattern, and effect.

Query: principalId?, roleId?, resource?, effect?: allow|deny|ask

200: GrantResponse[] -- List of grants

### POST /api/tenants/:tenantId/grants
Create a capability grant

Creates a grant targeting either a role or a principal directly. Exactly one of roleId or principalId must be provided.

Body: CreateGrant

201: GrantResponse -- Grant created
400: ErrorResponse -- Validation error

### GET /api/tenants/:tenantId/grants/:grantId
Get grant details

200: GrantResponse -- Grant details
404: ErrorResponse -- Grant not found

### PATCH /api/tenants/:tenantId/grants/:grantId
Update a grant

Update effect, conditions, or expiry on an existing grant.

Body: UpdateGrant

200: GrantResponse -- Grant updated
404: ErrorResponse -- Grant not found

### DELETE /api/tenants/:tenantId/grants/:grantId
Revoke a grant

204: (no content) -- Grant revoked
404: ErrorResponse -- Grant not found

### POST /api/tenants/:tenantId/principals/:principalId/evaluate
Evaluate grants for a principal

Evaluates what would happen if a principal attempted an operation. Returns the resolved effect and all matching grants. Useful for debugging authorization.

Body: EvaluateRequest

200: EvaluateResult -- Evaluation result
404: ErrorResponse -- Principal not found

## Agent Definitions

### GET /api/tenants/:tenantId/agents/definitions
List agent definitions

Filterable by offering and status.

Query: offering?, status?: deployed|stopped

200: AgentResponse[] -- List of agent definitions

### POST /api/tenants/:tenantId/agents/definitions
Create an agent definition

Creates an agent definition and its corresponding principal. Accepts the definition and optional initial capability grants for the agent's principal.

Body: CreateAgent

201: AgentResponse -- Agent created
400: ErrorResponse -- Validation error

### GET /api/tenants/:tenantId/agents/definitions/:agentId
Get agent definition details

Returns the agent definition, status, capabilities, and principal ID.

200: AgentResponse -- Agent details
404: ErrorResponse -- Agent not found

### PATCH /api/tenants/:tenantId/agents/definitions/:agentId
Update agent definition

Updates the agent definition and creates a new version. Running instances are not automatically updated; redeploy to pick up the new version.

Body: UpdateAgent

200: AgentResponse -- Agent updated
400: ErrorResponse -- Validation error

### DELETE /api/tenants/:tenantId/agents/definitions/:agentId
Retire an agent

Deactivates the agent's principal and stops any running instances.

204: (no content) -- Agent retirement initiated
404: ErrorResponse -- Agent not found

### GET /api/tenants/:tenantId/agents/definitions/:agentId/versions
List agent versions

Lists all versions with their deployment status.

200: AgentVersion[] -- List of versions

### POST /api/tenants/:tenantId/agents/definitions/:agentId/rollback
Rollback to a previous version

Shifts traffic back to the specified version. The current version is stopped.

Body: RollbackRequest

200: AgentResponse -- Rollback initiated
400: ErrorResponse -- Invalid version

### GET /api/tenants/:tenantId/agents/definitions/:agentId/health
Get agent health status

Returns liveness and readiness status.

200: AgentHealth -- Health status
404: ErrorResponse -- Agent not found

### GET /api/tenants/:tenantId/agents/definitions/:agentId/offerings
List agent offerings

Returns the agent's exposed offerings with pricing metadata.

200: Offering[] -- List of offerings

## Instances

### POST /api/tenants/:tenantId/agents/instances
Deploy an agent instance

Creates a new running instance of the specified agent definition. Resolves credentials, provisions the agent on a sidecar, and starts the agent. At most one running instance per agent is permitted.

Body: CreateAgentInstance

201: AgentInstanceResponse -- Instance deployed
404: ErrorResponse -- Agent definition not found
409: ErrorResponse -- Agent not launchable or already has an active instance
502: ErrorResponse -- Sidecar unavailable

### GET /api/tenants/:tenantId/agents/instances
List agent instances

Lists agent instances in the tenant. Filterable by agentId and status.

Query: agentId?, status?: deployed|running|updating|error|stopped

200: AgentInstanceResponse[] -- List of instances

### GET /api/tenants/:tenantId/agents/instances/:instanceId
Get instance detail

Returns instance runtime state including status, public key, sidecar assignment, and runtime status (idle, busy, waiting_approval) when available.

200: AgentInstanceResponse -- Instance detail
404: ErrorResponse -- Instance not found

### DELETE /api/tenants/:tenantId/agents/instances/:instanceId
Stop an instance

Stops the running instance and undeploys the agent from the sidecar.

204: (no content) -- Instance stopped
404: ErrorResponse -- Instance not found
409: ErrorResponse -- Instance already stopped
502: ErrorResponse -- Sidecar unavailable

### POST /api/tenants/:tenantId/agents/instances/:instanceId/messages
Send a message to the agent

Persists the user message and dispatches it to the running agent. The agent's response streams over the instance SSE channel, not in this HTTP response.

Body: SendMessage

201: MessageResponse -- Message sent
400: ErrorResponse -- Validation error
404: ErrorResponse -- Instance not found
409: ErrorResponse -- Instance not running
502: ErrorResponse -- Sidecar unavailable

### GET /api/tenants/:tenantId/agents/instances/:instanceId/messages
List messages for an instance

Returns messages with all parts (text, reasoning, tool calls, etc.). Cursor-paginated. Only includes messages created during this instance's lifetime.

Query: cursor?, limit?

200: MessageResponse[] -- List of messages
404: ErrorResponse -- Instance not found

### POST /api/tenants/:tenantId/agents/instances/:instanceId/abort
Abort current operation

Aborts the agent's current inference or tool execution.

204: (no content) -- Abort signal sent
404: ErrorResponse -- Instance not found
409: ErrorResponse -- Instance not running
502: ErrorResponse -- Sidecar unavailable

### GET /api/tenants/:tenantId/agents/instances/:instanceId/events
SSE event stream

Server-Sent Events stream for agent events. Server-to-client only; use POST .../messages for client-to-server messaging.

200: SSE stream -- SSE event stream
404: ErrorResponse -- Instance not found
410: ErrorResponse -- Instance stopped

## Approvals

### GET /api/tenants/:tenantId/approvals
List pending approvals in the tenant

Returns pending approval requests for the authenticated user within this tenant.

200: ApprovalResponse[] -- List of approvals

### GET /api/tenants/:tenantId/approvals/:approvalId
Get approval details

Returns the proposed action, context, originating agent, and instance.

200: ApprovalResponse -- Approval details
404: ErrorResponse -- Approval not found

### POST /api/tenants/:tenantId/approvals/:approvalId/approve
Approve an action

Approves the pending action. With scope 'once', the approval is one-time. With scope 'always', a persistent capability grant is created so the agent won't need to ask again.

Body: ApproveAction

200: ApprovalResponse -- Action approved
404: ErrorResponse -- Approval not found

### POST /api/tenants/:tenantId/approvals/:approvalId/reject
Reject an action

Rejects the pending action. An optional message provides feedback to the agent.

Body: RejectAction

200: ApprovalResponse -- Action rejected
404: ErrorResponse -- Approval not found

## Wallets

### GET /api/tenants/:tenantId/wallets
List wallets in the tenant

200: WalletResponse[] -- List of wallets

### POST /api/tenants/:tenantId/wallets
Create a wallet

Creates a wallet with the specified payment backend and currency. Access for agents is managed through capability grants.

Body: CreateWallet

201: WalletResponse -- Wallet created
400: ErrorResponse -- Validation error

### GET /api/tenants/:tenantId/wallets/:walletId
Get wallet details

Returns wallet details including current balance.

200: WalletResponse -- Wallet details
404: ErrorResponse -- Wallet not found

### PATCH /api/tenants/:tenantId/wallets/:walletId
Update wallet config

Body: UpdateWallet

200: WalletResponse -- Wallet updated
404: ErrorResponse -- Wallet not found

### DELETE /api/tenants/:tenantId/wallets/:walletId
Deactivate a wallet

204: (no content) -- Wallet deactivated
404: ErrorResponse -- Wallet not found

### GET /api/tenants/:tenantId/wallets/:walletId/transactions
List transactions

Transaction history for a wallet. Filterable by agent, date range, and status.

Query: agentId?, startTime?, endTime?, status?: pending|completed|failed

200: TransactionResponse[] -- List of transactions

## Credentials

### GET /api/tenants/:tenantId/credentials
List credentials

Lists credential metadata. Secrets are never returned. Access for agents is managed through capability grants.

200: CredentialResponse[] -- List of credentials

### POST /api/tenants/:tenantId/credentials
Store a credential

Stores a credential (API key, OAuth token, etc.). The secret is stored securely and never returned in subsequent reads.

Body: CreateCredential

201: CredentialResponse -- Credential stored
400: ErrorResponse -- Validation error

### GET /api/tenants/:tenantId/credentials/:credentialId
Get credential metadata

Returns credential metadata. The secret is never included.

200: CredentialResponse -- Credential metadata
404: ErrorResponse -- Credential not found

### PATCH /api/tenants/:tenantId/credentials/:credentialId
Rotate or update a credential

Body: UpdateCredential

200: CredentialResponse -- Credential updated
404: ErrorResponse -- Credential not found

### DELETE /api/tenants/:tenantId/credentials/:credentialId
Revoke a credential

204: (no content) -- Credential revoked
404: ErrorResponse -- Credential not found

## Observability

### GET /api/tenants/:tenantId/agents/definitions/:agentId/logs
Get agent logs

Structured logs for an agent. Filterable by level and time range.

Query: level?: debug|info|warn|error, startTime?, endTime?

200: LogEntry[] -- Log entries
404: ErrorResponse -- Agent not found

### GET /api/tenants/:tenantId/agents/definitions/:agentId/metrics
Get agent metrics

Returns throughput, latency, error rates, token usage, and cost metrics.

200: MetricsResponse -- Agent metrics
404: ErrorResponse -- Agent not found

### GET /api/tenants/:tenantId/traces
Query distributed traces

Searches traces within the tenant. Filterable by agent, instance, time range, and trace ID.

Query: agentId?, instanceId?, traceId?, startTime?, endTime?

200: SpanResponse[] -- List of traces

### GET /api/tenants/:tenantId/traces/:traceId
Get a full trace

Returns all spans in a trace across agent boundaries.

200: TraceResponse -- Trace with spans
404: ErrorResponse -- Trace not found

## Agent Data

### GET /api/tenants/:tenantId/agents/definitions/:agentId/data
List files in agent working directory

200: FileEntry[] -- File listing
404: ErrorResponse -- Agent not found

### GET /api/tenants/:tenantId/agents/definitions/:agentId/data/*
Read a file from agent storage

Reads a file by path from the agent's local storage.

200: FileContent -- File content
404: ErrorResponse -- File or agent not found

### GET /api/tenants/:tenantId/agents/definitions/:agentId/history
List commits and checkpoints

Returns the agent's change history with commit messages and timestamps.

200: HistoryEntry[] -- History entries

### GET /api/tenants/:tenantId/agents/definitions/:agentId/history/:ref
Show changes in a commit

Returns the files changed in a specific commit with additions/deletions counts.

200: CommitDetail -- Commit details
404: ErrorResponse -- Commit not found

### GET /api/tenants/:tenantId/agents/definitions/:agentId/branches
List branches

Lists branches in the agent's data repository.

200: BranchInfo[] -- List of branches

### POST /api/tenants/:tenantId/agents/definitions/:agentId/history/:ref/restore
Restore agent data to a previous state

Restores the agent's working directory to the state at the specified commit.

204: (no content) -- Data restored
404: ErrorResponse -- Commit not found

## Type Reference

### AgentHealth
`{ liveness: "ok" | "unhealthy", readiness: "not_ready" | "ok" | "unhealthy", lastCheckedAt?: string | null }`
Source: packages/types/src/agents.ts

### AgentInstanceResponse
`{ id: string, agentId: string, tenantId: string, address: string, status: "deployed" | "running" | "updating" | "error" | "stopped", createdAt: string, updatedAt: string, publicKey?: string | null, kernelId?: string | null, sidecarId?: string | null, endedAt?: string | null, runtimeStatus?: "idle" | "busy" | "waiting_approval" }`
Source: packages/types/src/agents.ts

### AgentResponse
`{ createdAt: string, currentVersion: string, id: string, name: string, principalId: string, status: "deployed" | "stopped", tenantId: string, updatedAt: string, capabilities?: { [string]: unknown }, contextConfig?: { [string]: unknown }, description?: string | null, initialState?: { [string]: unknown }, modelConfig?: { [string]: unknown }, skills?: { [string]: unknown }, systemPrompt?: string | null }`
Source: packages/types/src/agents.ts

### AgentSummary
`{ id: string, name: string, status: "deployed" | "stopped", tenantId: string, tenantName: string, description?: string | null }`
Source: packages/types/src/me.ts

### AgentVersion
`{ createdAt: string, status: "active" | "failed" | "inactive", version: string }`
Source: packages/types/src/agents.ts

### ApprovalResponse
`{ action: string, agentId: string, createdAt: string, id: string, principalId: string, resource: string, sessionId: string, status: "approved" | "pending" | "rejected", tenantId: string, context?: { [string]: unknown } | null, resolvedAt?: string | null }`
Source: packages/types/src/approvals.ts

Note: `sessionId` is an internal FK to the session channel. The approval was created during an instance's execution; the instance ID can be resolved via the session relationship.

### ApprovalSummary
`{ action: string, agentId: string, agentName: string, createdAt: string, id: string, resource: string, sessionId: string, tenantId: string, tenantName: string }`
Source: packages/types/src/me.ts

Note: `sessionId` is an internal FK, as with ApprovalResponse above.

### ApproveAction
`{ scope: "always" | "once" }`
Source: packages/types/src/approvals.ts

### BranchInfo
`{ name: string, isCurrent?: boolean, lastCommitAt?: string | null, lastCommitMessage?: string | null, lastCommitRef?: string | null }`
Source: packages/types/src/agent-data.ts

### Offering
`{ agentId: string, id: string, name: string, description?: string | null, pricing?: { base?: { amount: string, currency: string }, bounds?: { max?: string, min?: string }, methods?: string[], negotiable?: boolean } }`
Source: packages/types/src/agents.ts

### OfferingDetail
`{ agentId: string, agentName: string, id: string, name: string, tenantId: string, description?: string | null, pricing?: { base?: { amount: string, currency: string }, bounds?: { max?: string, min?: string }, methods?: string[], negotiable?: boolean }, schema?: { [string]: unknown } | null }`
Source: packages/types/src/offerings.ts

### CommitDetail
`{ author: string, changes: { path: string, status: "added" | "deleted" | "modified", additions?: number, deletions?: number }[], message: string, ref: string, timestamp: string }`
Source: packages/types/src/agent-data.ts

### CreateAgent
`{ name: string, capabilities?: { [string]: unknown }, contextConfig?: { [string]: unknown }, description?: string, initialGrants?: { action: string, effect: "allow" | "ask" | "deny", resource: string, conditions?: { [string]: unknown } | null }[], initialState?: { [string]: unknown }, modelConfig?: { [string]: unknown }, skills?: { [string]: unknown }, systemPrompt?: string }`
Source: packages/types/src/agents.ts

### CreateCredential
`{ name: string, secret: string, type: "api_key" | "certificate" | "oauth_token" | "other", description?: string, metadata?: { [string]: unknown } }`
Source: packages/types/src/credentials.ts

### CreateFederationTrust
`{ direction: "bilateral" | "inbound" | "outbound", targetTenantId: string }`
Source: packages/types/src/tenants.ts

### CreateGrant
`{ action: string, effect: "allow" | "ask" | "deny", resource: string, source: "creator" | "invoker" | "role" | "system", conditions?: { [string]: unknown } | null, expiresAt?: string | null, principalId?: string | null, roleId?: string | null }`
Source: packages/types/src/grants.ts

### CreateRole
`{ name: string, description?: string }`
Source: packages/types/src/roles.ts

### CreateAgentInstance
`{ agentId: string }`
Source: packages/types/src/agents.ts

### CreateTenant
`{ name: string, slug: string, parentId?: string | null }`
Source: packages/types/src/tenants.ts

### CreateWallet
`{ backendType: "credits" | "crypto" | "fiat", currency: string, name: string, config?: { [string]: unknown } }`
Source: packages/types/src/wallets.ts

### CredentialResponse
`{ createdAt: string, id: string, name: string, tenantId: string, type: "api_key" | "certificate" | "oauth_token" | "other", updatedAt: string, description?: string | null, metadata?: { [string]: unknown } | null }`
Source: packages/types/src/credentials.ts

### ErrorResponse
`{ error: { code: string, message: string } }`
Source: packages/types/src/common.ts

### EvaluateRequest
`{ action: string, resource: string }`
Source: packages/types/src/grants.ts

### EvaluateResult
`{ effect: "allow" | "ask" | "deny", matchingGrants: { action: string, effect: "allow" | "ask" | "deny", id: string, resource: string, source: "creator" | "invoker" | "role" | "system" }[] }`
Source: packages/types/src/grants.ts

### FederationTrust
`{ createdAt: string, direction: "bilateral" | "inbound" | "outbound", tenantDomain: string, tenantId: string, tenantName: string }`
Source: packages/types/src/tenants.ts

### FileContent
`{ content: string, path: string, encoding?: "base64" | "utf-8" }`
Source: packages/types/src/agent-data.ts

### FileEntry
`{ path: string, type: "directory" | "file", modifiedAt?: string | null, size?: number | null }`
Source: packages/types/src/agent-data.ts

### GrantResponse
`{ action: string, createdAt: string, effect: "allow" | "ask" | "deny", id: string, resource: string, source: "creator" | "invoker" | "role" | "system", tenantId: string, updatedAt: string, conditions?: { [string]: unknown } | null, expiresAt?: string | null, principalId?: string | null, roleId?: string | null }`
Source: packages/types/src/grants.ts

### HistoryEntry
`{ author: string, message: string, ref: string, timestamp: string, filesChanged?: number }`
Source: packages/types/src/agent-data.ts

### InviteMember
`{ email: string, roleId?: string }`
Source: packages/types/src/principals.ts

### LogEntry
`{ level: "debug" | "error" | "info" | "warn", message: string, timestamp: string, metadata?: { [string]: unknown } | null }`
Source: packages/types/src/observability.ts

### MessageResponse
`{ createdAt: string, id: string, parts: { id: string, type: "file" | "patch" | "reasoning" | "snapshot" | "step-finish" | "step-start" | "text" | "tool", content?: string | null, metadata?: { [string]: unknown } | null }[], role: "assistant" | "user", sessionId: string, status: "pending" | "delivered" | "failed" }`
Source: packages/types/src/sessions.ts

Note: `sessionId` refers to the internal session channel identifier, not the removed session API resource. This field is retained for backward compatibility with the underlying message storage model.

### MetricsResponse
`{ agentId: string, avgLatencyMs?: number, cost?: string, errorRate?: number, messageCount?: number, tokenUsage?: { input?: number, output?: number, total?: number } }`
Source: packages/types/src/observability.ts

### ModelInfo
`{ id: string, name: string, providerId: string, capabilities?: string[], description?: string | null, limits?: { context?: number, output?: number }, pricing?: { cacheRead?: string, cacheWrite?: string, input?: string, output?: string } }`
Source: packages/types/src/models.ts

### PrincipalResponse
`{ createdAt: string, id: string, kind: "agent" | "user", refId: string, roles: { id: string, name: string }[], status: "active" | "deactivated" | "invited" | "suspended", tenantId: string, updatedAt: string }`
Source: packages/types/src/principals.ts

### PrincipalSummary
`{ kind: "agent" | "user", principalId: string, roles: { id: string, name: string }[], status: "active" | "deactivated" | "invited" | "suspended", tenantId: string, tenantName: string, tenantSlug: string }`
Source: packages/types/src/me.ts

### RejectAction
`{ message?: string }`
Source: packages/types/src/approvals.ts

### RoleResponse
`{ createdAt: string, id: string, isSystem: boolean, name: string, tenantId: string, updatedAt: string, description?: string | null }`
Source: packages/types/src/roles.ts

### RollbackRequest
`{ version: string }`
Source: packages/types/src/agents.ts

### SendMessage
`{ content: string, attachments?: { type: string, url: string, mimeType?: string }[] }`
Source: packages/types/src/sessions.ts

### InstanceSummary
`{ id: string, agentId: string, agentName: string, address: string, status: "deployed" | "running" | "updating" | "error" | "stopped", tenantId: string, tenantName: string, createdAt: string }`
Source: packages/types/src/me.ts

### SpanResponse
`{ name: string, spanId: string, startTime: string, traceId: string, agentId?: string | null, attributes?: { [string]: unknown } | null, durationMs?: number | null, endTime?: string | null, parentSpanId?: string | null, status?: "error" | "ok" }`
Source: packages/types/src/observability.ts

### TenantResponse
`{ createdAt: string, domain: string, id: string, name: string, slug: string, updatedAt: string, config?: { [string]: unknown }, parentId?: string | null }`
Source: packages/types/src/tenants.ts

### TraceResponse
`{ spans: { name: string, spanId: string, startTime: string, traceId: string, agentId?: string | null, attributes?: { [string]: unknown } | null, durationMs?: number | null, endTime?: string | null, parentSpanId?: string | null, status?: "error" | "ok" }[], traceId: string }`
Source: packages/types/src/observability.ts

### TransactionResponse
`{ amount: string, createdAt: string, currency: string, direction: "inbound" | "outbound", id: string, status: "completed" | "failed" | "pending", walletId: string, agentId?: string | null, recipientId?: string | null, requestId?: string | null, senderId?: string | null }`
Source: packages/types/src/wallets.ts

### UpdateAgent
`{ capabilities?: { [string]: unknown }, contextConfig?: { [string]: unknown }, description?: string, initialState?: { [string]: unknown }, modelConfig?: { [string]: unknown }, name?: string, skills?: { [string]: unknown }, systemPrompt?: string }`
Source: packages/types/src/agents.ts

### UpdateCredential
`{ description?: string, metadata?: { [string]: unknown }, name?: string, secret?: string }`
Source: packages/types/src/credentials.ts

### UpdateGrant
`{ conditions?: { [string]: unknown } | null, effect?: "allow" | "ask" | "deny", expiresAt?: string | null }`
Source: packages/types/src/grants.ts

### UpdatePrincipal
`{ status: "active" | "deactivated" | "suspended" }`
Source: packages/types/src/principals.ts

### UpdateRole
`{ description?: string, name?: string }`
Source: packages/types/src/roles.ts

### UpdateTenant
`{ config?: { [string]: unknown }, name?: string }`
Source: packages/types/src/tenants.ts

### UpdateWallet
`{ config?: { [string]: unknown }, name?: string }`
Source: packages/types/src/wallets.ts

### UserProfile
`{ createdAt: string, email: string, emailVerified: boolean, id: string, name: string, updatedAt: string, image?: string | null }`
Source: packages/types/src/me.ts

### WalletResponse
`{ backendType: "credits" | "crypto" | "fiat", balance: string, createdAt: string, currency: string, id: string, name: string, tenantId: string, updatedAt: string, config?: { [string]: unknown } }`
Source: packages/types/src/wallets.ts

