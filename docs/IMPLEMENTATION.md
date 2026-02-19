# Faremeter Interchange

_Implementation_

## Message Bus: SMTP/IMAP Transport

The Interchange message bus is built on SMTP and IMAP as wire protocols, leveraging existing global messaging infrastructure as the transport layer for agent communication. SMTP/IMAP provide the durable, federated message routing that Interchange needs — but Interchange clients are not email clients.

Interchange clients (kernels, UIs, CLIs) speak SMTP/IMAP to send and receive messages, but they are purpose-built for Interchange semantics: structured message payloads, conversation threading, offering negotiation, and real-time session channels. A traditional email client _could_ connect to an agent's inbox and see messages, but the experience would be like reading raw HTTP traffic — technically possible, not practically useful.

### Why SMTP/IMAP

SMTP and IMAP provide a battle-tested, globally distributed message passing system with decades of operational maturity:

- **Universal addressing** - Every agent gets an SMTP address as its network identity
- **Federated by default** - No central authority required; agents on different domains communicate seamlessly
- **Delivery guarantees** - SMTP provides store-and-forward semantics with retry logic
- **Asynchronous** - Agents don't need to be online simultaneously to exchange messages
- **Existing infrastructure** - DNS MX records, TLS, spam filtering, and authentication (SPF, DKIM, DMARC) already exist
- **Firewall-friendly** - SMTP traverses network boundaries that block other protocols

The choice of SMTP/IMAP is pragmatic: we get a global, federated message bus without building one from scratch. The protocols are the implementation detail; the Interchange message semantics are what matter.

### Agent Addressing

Each agent has an SMTP address that serves as its network identifier:

```
agent-name@domain.interchange.network
```

The local part identifies the agent; the domain identifies the tenant. Tenant boundaries map directly to SMTP domains, providing natural isolation and federation semantics.

### Message Transport

**Outbound (SMTP)**
When an agent sends a message to another agent or human, the kernel composes and submits it via SMTP. The payload is serialized into the body. Structured data uses MIME multipart encoding.

**Inbound (IMAP)**
The kernel maintains an IMAP connection to the agent's inbox. Incoming messages are fetched, parsed, and converted into events that the kernel routes to internal handlers.

### Message Topologies

SMTP/IMAP naturally supports the topologies defined in the architecture:

- **1:1** - Direct message between two addresses
- **1:N** - Distribution lists or CC/BCC for broadcast
- **M:N** - Distribution lists where multiple agents can post and receive

### Authentication and Trust

Interchange uses standard SMTP authentication mechanisms:

- **SPF** - Sender Policy Framework validates sending servers
- **DKIM** - DomainKeys Identified Mail provides cryptographic signatures
- **DMARC** - Domain-based Message Authentication ties SPF and DKIM together

Agent identity is bound to its SMTP address and verified through these mechanisms. Additional payload-level signatures can provide end-to-end verification when required.

### Content Safety

Beyond authenticating message senders, the kernel applies content-level safety measures:

**Context Framing**
All input assembled into the agent's context uses consistent structural delimiters. System prompts and skill instructions are clearly marked as trusted. External content (messages, tool responses, user input) is framed as data with explicit boundaries. The framing format is designed to be recognizable by models and resistant to delimiter injection.

**Action Validation**
Before executing tool invocations, the kernel validates the action against the agent's tool policy. Validation checks include:

- Is this tool permitted for this agent?
- Does the invocation match expected patterns?
- Are there anomalous parameters or frequencies?

Failed validations are logged and can trigger alerts.

**Human Approval Gates**
Operators can configure approval requirements for sensitive actions. When an agent attempts a gated action, execution blocks and an approval request is sent via the message bus. The request includes the proposed action and relevant context. Execution resumes only after explicit human approval.

**Audit Logging**
All inputs, outputs, and policy decisions are logged. Logs are partitioned by tenant (as described in Tenant Isolation Enforcement). Anomaly detection can flag unusual patterns for review.

### Encryption

- **Transport encryption** - TLS for SMTP and IMAP connections
- **Payload encryption** - S/MIME or PGP for end-to-end encrypted messages between agents

### Cryptographic Identity: Key Formats

Interchange uses Ed25519 as the single key algorithm for all cryptographic identity operations. Ed25519 is compact, fast, and supported by SSH (since OpenSSH 6.5), PGP/GnuPG (since 2.1), and TLS 1.3 (RFC 8446), allowing one key algorithm to serve across all protocol contexts. For encryption operations, the corresponding Curve25519 (X25519) is used.

The kernel generates one Ed25519 key pair per kernel and per agent. The same key material is used across protocols, with SSH, PGP, and X.509 serving as wire formats depending on context:

- **SSH format** - Used for kernel-to-kernel communication, control plane interactions, and general identity verification. Public keys are published in OpenSSH format.
- **PGP format** - Used for message-level signatures and encryption over the SMTP/IMAP transport, where PGP integration with email infrastructure is a natural fit. Agent PGP keys can be published via DNS (DANE/OPENPGPKEY records) or through the control plane.
- **X.509 format** - Used for TLS client certificates enabling mutual authentication. The control plane issues X.509 certificates wrapping the agent's Ed25519 public key, signed by a tenant certificate authority. This allows agents to authenticate via standard TLS mutual auth without introducing a separate key type.

**Protocol mapping:**

- **Agent/kernel identity and control plane** - Ed25519 in SSH format
- **Message payload signatures** - Ed25519 in PGP format, integrated with SMTP/IMAP transport
- **End-to-end message encryption** - X25519 in PGP format (S/MIME as an alternative where required)
- **TLS mutual authentication** - Ed25519 in X.509 format, certificates issued by the control plane

The kernel manages key generation, storage, rotation, and revocation. Key rotation follows a grace period model: the new key is published alongside the old key, both are accepted for verification during the overlap window, and the old key is retired after the grace period.

### Spam and Abuse Prevention

Existing SMTP infrastructure handles abuse:

- Rate limiting at the SMTP level
- Reputation systems for sending domains
- Content filtering where appropriate

Agents that misbehave lose SMTP deliverability, which maps directly to losing network access.

### IMAP Inbox as State

The agent's IMAP inbox provides a natural persistence layer:

- Unprocessed messages remain in the inbox
- Processed messages can be archived or deleted
- Folders organize different message types or conversations
- Search capabilities (IMAP SEARCH) allow retrieval of historical context

### Limitations and Mitigations

**Latency**
SMTP is not designed for real-time communication. For latency-sensitive workloads — streaming LLM responses, interactive chat, debugging — session channels provide a real-time alternative (see Session Channels below).

**Message size**
Large payloads may hit size limits. Agents can signal through the bus and exchange large data through separate channels (e.g., signed URLs).

**Ordering**
SMTP does not guarantee message ordering. Agents that require ordering must implement sequencing at the application layer.

## Session Channels: WebSocket Transport

Session channels provide an optional real-time overlay on the message bus for interactive use cases. The message bus (SMTP/IMAP) remains the canonical, durable transport — complete messages always land in the recipient's IMAP inbox. Session channels add real-time token streaming for clients that want to see inference output as it's generated.

### Protocol

Session channels use WebSocket (RFC 6455) as the transport protocol. WebSocket provides:

- Full-duplex communication over a single TCP connection
- Low overhead for small messages (2-6 byte framing vs. SMTP/MIME overhead)
- Wide support in browsers, mobile platforms, and server environments
- Compatible with standard load balancers and proxies

For clients that cannot use WebSocket (some embedded environments, restrictive firewalls), Server-Sent Events (SSE) for server-to-client streaming combined with HTTP POST for client-to-server messages provides a fallback.

### Message Format

Session channel messages are JSON objects with a type field and payload:

```json
{"type": "inference.token", "data": {"token": "Hello", "seq": 1}}
{"type": "inference.done", "data": {"seq": 42, "message_id": "<abc123@example.com>"}}
{"type": "user.message", "data": {"content": "What's the weather?"}}
```

Message types are namespaced by category (inference, user, debug, system). The kernel and client negotiate supported message types during session establishment.

### Connection Establishment

**Production (via control plane):**

1. Client authenticates with the control plane via HTTPS, providing identity credentials and target agent
2. Control plane returns a session token (JWT signed by the control plane, containing agent ID, client identity, capabilities, expiry)
3. Client opens WebSocket to control plane's session endpoint: `wss://controlplane.example/session`
4. Client sends session token in the first message
5. Control plane validates token and routes connection to the kernel hosting the target agent
6. Kernel sends `session.ready` message; bidirectional streaming begins

**Development (direct to kernel):**

1. Client opens WebSocket directly to kernel: `ws://localhost:8080/session`
2. Client sends authentication (API key, or signed challenge)
3. Kernel validates and sends `session.ready`

### Authentication

Session tokens issued by the control plane are JWTs containing:

- `sub` — client identity
- `aud` — target agent ID
- `cap` — invoker-granted capabilities for this session
- `exp` — token expiry (short-lived, typically minutes)
- `jti` — unique token ID for replay prevention

Tokens are signed with the control plane's Ed25519 key. Kernels verify the signature against the control plane's published public key.

### Streaming Inference

When an agent performs inference, the kernel streams tokens to the session channel as they arrive from the model backend:

```
← {"type": "inference.start", "data": {"model": "claude-3"}}
← {"type": "inference.token", "data": {"token": "The", "seq": 1}}
← {"type": "inference.token", "data": {"token": " weather", "seq": 2}}
← {"type": "inference.token", "data": {"token": " today", "seq": 3}}
...
← {"type": "inference.done", "data": {"seq": 47, "usage": {"input": 120, "output": 47}}}
```

Sequence numbers allow clients to detect dropped messages. The `inference.done` message includes final metadata (token counts, model info).

When inference completes, the complete message is also delivered via SMTP to the client's IMAP inbox. Clients connected to both the session channel and IMAP will see the content twice — once as streaming tokens, once as the complete message. The client is responsible for deduplication, typically by matching the message ID included in `inference.done` with the IMAP message.

### Heartbeats and Timeouts

Both client and kernel send periodic heartbeat messages to detect dead connections:

```json
{"type": "system.ping", "data": {"ts": 1699999999}}
{"type": "system.pong", "data": {"ts": 1699999999}}
```

Connections without activity or heartbeat responses for 30 seconds are terminated. Clients are expected to reconnect and resume if needed.

### Reconnection and Durability

Session channels are ephemeral — there is no built-in resume capability for token streams. If a connection drops mid-stream, those tokens are lost. However, conversation durability is guaranteed by the message bus:

1. Client reconnects and re-authenticates
2. Client fetches any messages that arrived via IMAP while disconnected
3. New streaming resumes from the current point

The message bus is the source of truth for conversation history. Session channels are a real-time optimization, not a replacement for IMAP. A client that loses its session connection can always recover the complete conversation from its inbox.

### Debug and Telemetry Streams

Debug and telemetry data (state inspection, trace output, log tailing) flows only over session channels — it is not delivered via IMAP. This data is captured for auditing purposes with standard retention policies but is not part of the durable conversation record.

```json
{"type": "debug.state", "data": {"context_tokens": 4096, "tools_invoked": 3}}
{"type": "debug.trace", "data": {"span_id": "abc123", "event": "tool_start", "tool": "web_search"}}
{"type": "telemetry.log", "data": {"level": "info", "message": "Processing request"}}
```

Debug streams require explicit authorization — not all clients are permitted to attach debuggers to agents.

## Authorization: OpenAPI-Based Capability Scoping

When agents are granted access to external APIs, the full API surface often exceeds what the agent needs. Interchange uses OpenAPI specifications to define and enforce fine-grained authorization subsets.

### Subset Definitions

An operator provides an OpenAPI spec (or a reference to one) alongside a subset policy that restricts which operations the agent may invoke. The subset policy specifies:

- Allowed paths and HTTP methods
- Parameter constraints (e.g., only certain query parameter values)
- Required headers or authentication scopes

The kernel loads the OpenAPI spec, applies the subset policy, and generates a filtered view of the API that the agent can discover and invoke through the standard tool interface.

### Dynamic Skill and Tool Generation

From the filtered OpenAPI spec, the kernel dynamically generates tools that the agent can invoke. Each permitted endpoint becomes a tool with typed parameters derived from the spec's schema definitions. The agent sees only the operations it is authorized to use, and the kernel validates every invocation against the subset policy before forwarding the request to the external API.

This approach allows operators to grant access to complex APIs without writing custom tool definitions. The OpenAPI spec serves as both documentation and enforcement boundary.

### Credential Binding

Subset policies are paired with the delegated credentials for the API. The kernel injects authentication (OAuth tokens, API keys) into outbound requests transparently. The agent never sees raw credentials; it invokes tools and the kernel handles authentication.

## Wallets: Payment Tools and Plugins

Wallets provide agents with the ability to send and receive payments. The kernel exposes wallet functionality as tools, with a plugin architecture for payment backends.

### Payment Tools

The kernel provides standard payment tools that agents invoke like any other capability:

- **`wallet.pay`** — Send payment to a recipient
- **`wallet.request_payment`** — Request payment from a caller, blocking until received or timeout
- **`wallet.check_balance`** — Query available balance by currency
- **`wallet.verify_payment`** — Verify that a payment was received
- **`wallet.list_transactions`** — Query recent transaction history

These tools abstract over payment protocols and backends. The agent specifies what to pay (amount, currency, recipient); the kernel handles how (signing, broadcasting, proof generation).

### Payment Framework

The kernel defines a payment integration interface behind the payment tools. This interface is what connects the agent-facing tools (`wallet.pay`, etc.) to actual payment implementations. Multiple implementations can be active simultaneously — the kernel selects the appropriate one based on the payment requirements of a given transaction.

### Faremeter

[Faremeter](https://github.com/faremeter/faremeter) is the primary payment framework. It provides a pluggable, standards-agnostic payment system that supports a superset of emerging payment protocols including x402, L402, and Cloudflare's Pay-Per-Crawl.

**Agent as consumer (outbound payments):**
The kernel uses Faremeter's client library (`@faremeter/fetch`) to handle outbound payments. When an agent invokes a remote tool that requires payment, the kernel uses Faremeter to negotiate and execute the payment using the appropriate protocol and wallet backend. Faremeter's plugin system handles scheme and network selection — the kernel connects the agent's configured wallet plugins and Faremeter handles the rest.

**Agent as provider (inbound payments):**
For agents that charge for their services, the kernel uses Faremeter's middleware to handle payment collection. The middleware intercepts inbound requests, enforces pricing, and verifies payment before allowing the agent to process the request. This separates payment handling from agent logic — the agent focuses on its work while Faremeter handles the payment mechanics.

Faremeter's plugin architecture is wallet and blockchain agnostic. Tenants configure which payment plugins are available (Solana, EVM chains, fiat processors) and bind them to wallets. New payment standards can be supported by adding plugins without changes to the kernel or agent.

### Third-Party Integrations

The payment integration interface is open. Payment implementations outside of Faremeter's ecosystem can plug into the kernel's wallet layer, allowing tenants to connect proprietary payment systems, custom billing platforms, or emerging protocols that haven't yet been adopted by Faremeter. Third-party integrations implement the same interface and are available to agents through the same payment tools.

### Policy Enforcement

The kernel enforces spending policy before executing any payment:

- **Spending limits** — Per-transaction, per-day, per-recipient caps
- **Approved recipients** — Whitelist of addresses/accounts permitted to receive funds
- **Approved currencies** — Which currencies the agent may spend
- **Approval thresholds** — Transactions above a threshold require human approval (via the message bus)

Policy violations are logged and the payment is rejected. The agent receives an error indicating the policy failure.

### Pricing Advertisement

Agents that charge for their services publish pricing in their offering metadata:

```json
{
  "offering": "web-search",
  "pricing": {
    "base": { "amount": "0.001", "currency": "USD" },
    "methods": ["ethereum", "lightning", "stripe"],
    "negotiable": true,
    "bounds": { "min": "0.0005", "max": "0.01" }
  }
}
```

When `negotiable` is true, callers can propose alternative pricing within the specified bounds. The agent (or its policy) decides whether to accept.

### Receipt Logging

All payment transactions are logged to the audit trail:

- Transaction ID / hash
- Amount and currency
- Payer and recipient identities
- Associated request (tool invocation, offering call)
- Timestamp
- Success/failure status

This provides a complete financial audit trail for compliance and debugging.

## Tenant Mapping

Multi-tenancy maps naturally onto SMTP infrastructure, with tenant boundaries corresponding to message domains.

### Domain-Based Tenancy

Each tenant owns one or more SMTP domains:

```
agent@acme.interchange.network      # Tenant: Acme Corp
agent@research.acme.interchange.network  # Tenant: Acme Research (child tenant)
agent@startup.interchange.network   # Tenant: Startup Inc
```

Subdomains support hierarchical tenancy - child tenants inherit the parent domain as a suffix.

### Tenant-Local Communication

Messages between agents in the same tenant (same domain) are routed internally without traversing external SMTP infrastructure. The kernel can optimize tenant-local delivery for lower latency and higher throughput.

### Cross-Tenant Federation

Cross-tenant communication uses standard SMTP federation:

- Agents in different tenants communicate by sending messages across domains
- DNS MX records route messages to the correct tenant's mail infrastructure
- SPF, DKIM, and DMARC authenticate cross-tenant messages at the domain level
- Additional tenant-level trust policies determine whether to accept or reject cross-tenant requests

### Tenant Discovery

Tenants publish their federated agents and offerings through DNS:

- **SRV records** - Advertise the tenant's Interchange endpoints
- **TXT records** - Publish federation policies and offering manifests
- **Well-known URIs** - HTTPS endpoints for detailed offering and discovery queries (discovered via DNS)

This allows agents to discover other tenants and their available services using standard DNS resolution.

### Tenant Isolation Enforcement

The SMTP/IMAP server infrastructure enforces tenant boundaries:

- Each tenant operates its own message domain (or subdomain)
- Server configuration prevents agents from impersonating other tenants
- Rate limits and quotas are applied per-tenant
- Audit logs are partitioned by tenant for compliance and debugging

## Observability: OpenTelemetry

Interchange uses OpenTelemetry as the observability framework, providing a vendor-neutral interface for logs, metrics, and traces.

### Instrumentation

The kernel instruments key operations:

- **Inference calls** - Span per model invocation, recording model ID, token counts, latency
- **Tool invocations** - Span per tool call, recording tool name, parameters (redacted as needed), result status
- **Message send/receive** - Span per message, recording sender, recipient, message type
- **Policy decisions** - Log entry per authorization check, recording action, policy, and outcome

### Trace Propagation

Trace context propagates through the message bus:

- Outbound messages include trace context in a custom MIME header (`X-Trace-Context`)
- Inbound message parsing extracts trace context and resumes the trace
- Cross-tenant traces work the same way; trace context is just data in the message header

### Export

Telemetry is exported via the OpenTelemetry Protocol (OTLP). Operators configure collectors and backends (e.g., Jaeger, Prometheus, Grafana) according to their infrastructure.

### Tenant Scoping

Telemetry is tagged with tenant identifiers. Backends can enforce tenant-level access control so operators only see telemetry for their own agents.

## Lifecycle: Versioning and Health

Agent lifecycle operations are managed through versioned packages and health protocols.

### Agent Packages

An agent package is an immutable artifact built from the agent's definition repository with resolved dependencies. It contains:

- Skills and their dependencies
- System prompt
- Context builder configuration
- Initial state
- Tool policy

Packages are versioned with semantic versioning. The control plane tracks which versions are deployed, active, and available for rollback.

### Health Protocol

The kernel exposes health endpoints:

- **Liveness** - Returns OK if the kernel process is running and responsive
- **Readiness** - Returns OK if the agent is ready to accept work (connections established, initialization complete)

Health checks are polled by the control plane. Agents that fail liveness are restarted. Agents that fail readiness are removed from discovery until they recover.

Health can also be reported via periodic heartbeat messages to the control plane. Missed heartbeats trigger the same failure handling.

### Deployment Procedure

1. **Package upload** - New version is uploaded to the control plane
2. **Staging** - New version starts alongside existing version
3. **Health gate** - New version must pass health checks before receiving traffic
4. **Traffic shift** - Registry updates discovery to point to new version
5. **Drain** - Old version stops accepting new work, completes in-flight operations
6. **Retirement** - Old version shuts down; package remains available for rollback

### Rollback

If a new version fails health checks or exhibits problems in production:

1. Operator or agent triggers rollback via control plane
2. Traffic shifts back to previous version
3. Failed version is marked unhealthy and stopped

Rollback is fast because the previous version's package is still available and can be restarted immediately.

## Agent Versioning: Git-Backed Definitions

Agent definitions are stored in git repositories, providing version control for the resources that constitute an agent.

### Repository Contents

The repository contains agent-specific resources:

- Skills and skill configuration
- System prompt
- Context builder configuration
- Initial state
- Tool policy
- Dependency references (resolved at deployment)

External packages and libraries are not stored directly. Configuration files reference them, and the kernel resolves these references when building the agent package for deployment.

### Deployment

When an agent is deployed:

1. The agent definition is checked out from the repository at the specified version
2. External dependencies are resolved and bundled into the agent package
3. The package is deployed to the execution environment
4. A separate worktree is initialized for the agent's working data

The agent's definition environment and working data worktree are distinct git contexts.

### Repository Organization

Repository structure is flexible:

- **Tenant repository** - A single repository contains definitions for all agents within a tenant, organized by directory
- **Agent repository** - Each agent has a dedicated repository

Both approaches support standard git workflows for managing agent versions.

## Change History: Git-Backed Storage

Agent-local data is stored in git repositories, providing revision control as a native capability.

### Storage Backends

The git repository can be backed by different storage implementations depending on the execution environment:

- **Filesystem** - Native git on disk for Docker containers, VMs, local processes, and mobile/embedded devices with filesystem access. Full git functionality with standard tooling.
- **In-memory with remote sync** - For JavaScript runtimes (Cloudflare Workers, web workers) and mobile/embedded environments that lack persistent filesystem access. Uses isomorphic-git or similar pure-JS implementations with object storage (R2, S3) as the backing store. Repository state is loaded on initialization and synced on commits.
- **Virtual filesystem** - For environments with partial filesystem semantics. Adapts git operations to the available storage primitives.

The kernel abstracts these differences - agents interact with the same history tools regardless of the underlying backend.

### Repository Structure

Each agent has a git repository for its local data:

```
/agent-data/
  .git/                 # Git repository
  workspace/            # Agent's working directory
    notes.md
    artifacts/
    ...
```

The kernel initializes this repository when the agent is created and manages it throughout the agent's lifecycle.

### Commit Policy

The kernel uses a hybrid commit strategy:

- **Auto-commits** - The kernel commits automatically only on lifecycle boundaries:
  - On agent suspension or shutdown (preserving state before the agent goes offline)
  - On context window compaction (preserving state before truncation loses information)

  Auto-commits use generated messages describing the triggering event.

- **Agent checkpoints** - Agents explicitly create checkpoints when they want to mark meaningful points in their work. The agent controls the commit message, making history readable and intentional. This is the primary mechanism for building useful change history.

The hybrid approach ensures no work is lost across session boundaries while keeping the agent in control of what constitutes a meaningful checkpoint during active operation.

### Branch Management

Agents create and switch branches through kernel-provided tools:

- `branch create <name>` - Create a new branch from current state
- `branch switch <name>` - Switch to an existing branch
- `branch merge <name>` - Merge another branch into current
- `branch delete <name>` - Remove a branch

The kernel tracks branch metadata (creation time, purpose, parent branch) for garbage collection and auditing.

### Worktree Support

For concurrent access to multiple states:

- `worktree add <path> <branch>` - Create a new worktree for a branch
- `worktree remove <path>` - Clean up a worktree

Worktrees share history but have independent working directories, enabling agents to compare states or run parallel experiments.

### History Queries

Agents query history through structured tools:

- `history log` - List recent commits with messages and timestamps
- `history show <ref>` - Show changes in a specific commit
- `history diff <ref1> <ref2>` - Compare two points in history
- `history restore <ref> <path>` - Restore a file from a previous state

### Garbage Collection

The kernel periodically cleans up:

- Merged branches older than a retention threshold
- Orphaned worktrees
- Unreachable commits (standard git gc)

Retention policies are configurable per-tenant or per-agent.
