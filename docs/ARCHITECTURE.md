# Faremeter Interchange

_Architecture_

## Agent Composition

### Agent Definition

An agent definition is a blueprint — the catalog entry that describes what an agent can do, what it needs, and how it should behave. Definitions are pure data with no runtime state, no principal, and no address. They declare requirements; the control plane resolves those requirements when an agent is launched.

A definition contains:

**Skills**
Executable capabilities that define what the agent can do. Skills are executed by the local harness, which handles interaction with the local environment on behalf of the skill.

**System Prompt**
The agent's identity and behavioral instructions. Defines the persona, goals, and constraints that guide the agent's reasoning.

**Context Building**
Logic for constructing and managing the agent's context window. The context builder assembles the agent's working context from multiple sources: system prompt, skill instructions, initial state, conversation history, and runtime state. It handles compaction strategies to keep context within model limits while preserving relevant information. Skills contribute their own context fragments, which the builder integrates into the overall context hierarchy. The context builder distinguishes between trusted content (system prompt, skill instructions) and untrusted content (external messages, tool responses, user input), applying structural boundaries that help prevent untrusted content from being interpreted as instructions.

**Initial State**
Pre-populated data that forms part of the agent's starting context. Layered on top of the system prompt and other initial context elements.

**Grant Requirements**
Authorization requirements declaring which capabilities the agent needs and where each should come from. Each requirement specifies a resource, action, and source — one of `tenant`, `creator`, or `invoker`. The definition does not carry live grants; it carries a requirements manifest that the control plane resolves at launch time. See Authorization Delegation below.

**Credential Requirements**
External service credentials the agent needs, declared with a source annotation indicating whether the tenant, definition creator, or invoker should provide them. See CREDENTIALS.md for the resolution model.

**Model Requirements**
The models the agent needs for inference, declared by canonical name with optional capability filters and provider preferences. Models, the providers that serve them, and their pairings live in a tenant-scoped catalog that inherits through the tenant hierarchy (see Model Provider Management); a tenant defines its own canonical model names, including custom names for private or self-hosted deployments.

A model provider is a service that offers access to one or more models — each with its own endpoint and authentication method. A model provider authenticates via either a credential (API key, OAuth token) or a wallet (pay-per-use), but not both. This is distinct from the credential system's "provider" concept (which represents any third-party service); model providers are specifically the services that serve inference.

For each model, the definition can specify model provider preferences and fallback order with source annotations, following the same three-source delegation model as grants and credentials:

- **Tenant-sourced** — Tenant policies define which model providers are available and set baseline priorities. This is the outer boundary; no agent can use a model provider the tenant has not registered.
- **Creator-sourced** — The definition author sets model provider preferences that travel with the definition. A creator can pin a definition to specific model providers (e.g., requiring Anthropic direct for a safety-critical agent) or set a preferred fallback order.
- **Invoker-sourced** — The user launching the agent can reorder or restrict the model providers for the session, within the set the tenant catalog already makes available. Bringing a provider the tenant has not registered is not supported; the invoker preference can only narrow or reprioritize, never extend, the available set.

The definition does not carry model provider configurations directly; it declares model needs and preferences that the control plane resolves against the catalog at launch, producing an ordered per-model source list. The head of that list is the active source and the tail is the failover chain. If a required model resolves to no launchable source, the agent fails to launch — the same behavior as an unresolvable credential requirement. See INFERENCE.md for the provider adapter architecture.

**Version History**
Definitions are tracked in git repositories on the hub. The repository contains the agent-specific resources: skills, system prompt, context builder configuration, and initial state. External skill dependencies are referenced in configuration files and resolved when the agent is deployed.

**Update Policy**
The definition declares how updates to it affect running agents — auto-redeploy, notify-only, staged, or manual.

**Creator Reference**
The definition stores a `creatorPrincipalId` identifying the principal of the definition author. Creator-sourced grant and credential requirements resolve against this principal. Ownership can be transferred to another principal if the original creator leaves the organization.

### Agent at Launch

When an agent is launched from a definition, the control plane creates a new runtime entity with its own identity and state. Each launched agent is independent — agents from the same definition do not share principals, addresses, or keys.

An agent gains at launch:

- **Principal** — A new principal is created for the agent. All grants are materialized on this principal.
- **Address** — An SMTP address (`ins_xxxxx@tenant.interchange.network`) for receiving messages.
- **Key pair** — An Ed25519 key pair for cryptographic identity and content signing.
- **Materialized grants** — The control plane resolves the definition's grant requirements against the appropriate sources (tenant policies, creator's grants, invoker's grants) and materializes the effective grant set on the agent's principal.
- **Resolved credentials** — The control plane resolves the definition's credential requirements and provisions the agent with the credentials it needs.
- **Resolved model providers** — The control plane resolves the definition's model requirements against all three sources (tenant, creator, invoker), producing an ordered model provider list per model. The harness uses this list for inference routing.
- **Inbox** — The agent's SMTP mailbox for durable message delivery.
- **Effective offerings** — The set of offerings the agent can actually provide, determined by which credentials and grants were successfully resolved.

### Repository Layout

Each agent on a sidecar has a single git repository that contains both the deployed definition and the runtime state, separated by path:

- `deploy/` — hub-managed content (skills, prompt, configuration). The hub assembles this tree from the agent's source repository and any referenced skill libraries via subtree merge, producing a single flat tree with no submodule metadata.
- `state/` — sidecar-managed content (conversation context, audit records). The sidecar commits here during normal operation.

Path disjointness is enforced: the hub only writes commits that modify `deploy/` paths, the sidecar only writes commits that modify `state/` paths. This guarantees conflict-free merges when the hub pushes new deploy versions to a running agent.

The git DAG encodes provenance. State commits on the sidecar descend from the initial repository commit, and the `refs/heads/deploy` ref tracks the latest deploy version received from the hub. On redeploy, the sidecar force-checks-out the new deploy tree — path disjointness between `deploy/` and `state/` makes merge unnecessary and the simpler operation is correct.

Repository organization on the hub is flexible — a single repository may contain definitions for multiple agents within a tenant, or agents may have dedicated repositories depending on operational needs. The assembly step normalizes any source layout into the standard `deploy/` tree structure before pushing to sidecars.

## Agent Harness

The agent harness is the core runtime component deployed for each agent. It acts as the glue layer that binds together all the capabilities an agent needs to operate autonomously within the Interchange ecosystem.

### Responsibilities

The harness orchestrates five primary concerns:

**Inference**
The harness manages inference through a layered abstraction. Agent definitions declare which models they need; the control plane resolves these requirements at launch into an ordered list of model providers per model.

The reactor director selects which model to use for each inference call (per-call, not per-session). The harness maps that model to the highest-priority available model provider, which determines everything needed for the call: protocol adapter, endpoint, and credentials. Different model providers for the same model may use entirely different protocols — one may speak the Anthropic API while another speaks an OpenAI-compatible API — so failover between model providers can mean switching the protocol adapter, not just the endpoint. The inference layer is stateless with respect to model provider selection; it executes against whatever configuration the harness provides.

Model provider selection follows the priority order established at launch. The highest-priority source is active; on a source-specific failure — an authentication failure, a protocol mismatch, or a transient network or timeout error the inference harness has already retried — the reactor fails over to the next source in the list. A rate limit is the exception: it waits out a short same-source backoff first, since it clears with time, and only then fails over. The error surfaces only once the list is exhausted. Source-invariant failures (a context-window overflow, a fatal request error, or an abort) do not trigger failover, since no other source would serve the call differently. Each new inference cycle restarts at the most-preferred source. v1 uses strict priority; equal-priority load-balancing and configurable selection strategies (weighted round-robin, latency-based) are a planned extension.

The model provider list is not fixed for the agent's lifetime. The control plane can push model provider updates to a running deployment — adding newly available model providers, removing revoked ones, or adjusting priorities. The sidecar routes the update to the deployment's supervisor, which swaps the running agent's sources in place. The harness applies updates without interrupting in-flight inference calls; the updated list takes effect on the next selection.

The agent is unaware of which model provider is serving a given inference call.

**Tools**
The harness consumes a single `ToolRunner` interface; the host (e.g. the sidecar) composes that runner from one or more tool packages — `@intx/tools-mail`, `@intx/tools-posix`, `@intx/tools-lsp`, plus any operator-supplied additions — and merges them through a generic name-collision-checked merger before passing the result to the harness. Each tool package receives the host services it needs (e.g. the bound `MessageTransport`) through a typed runtime-capability resolver, so packages are not statically wired to any one host. Local tools run within the agent's runtime — file system access, code execution, network requests, or custom tools the operator composes in. Remote tools are discovered through the control plane and invoke offerings exposed by other agents or services on the Interchange network. From the agent's perspective, local and remote tools share the same interface; the harness handles protocol negotiation, request routing, and wallet-based payment transparently. All tool invocations are subject to authorization policies.

**Local Data**
The harness manages persistent state on behalf of the agent. Conversation context, pending operations, and audit records are stored in a git repository under `state/` — but this is an implementation detail invisible to the agent. The agent interacts with the outside world exclusively through tools and messages; it has no direct access to the storage layer, the git history, or any harness metadata. Data is isolated per-agent. Credentials and authorization grants are managed by the harness and explicitly hidden from the agent (see Trust Boundary below).

**Environment Integration**
Separate harness implementations exist for each execution environment:

- **Cloudflare Workers** - Edge deployment with global distribution, limited to stateless compute
- **Docker Containers** - Full OS-level isolation with network and filesystem access
- **Virtual Machines** - Complete machine-level isolation for untrusted workloads
- **Local Processes** - Direct execution on the host for development and personal use
- **Mobile** - iOS and Android devices, using local process or web worker execution
- **Embedded** - IoT and edge devices, using local process or web worker execution

Each harness variant adapts to its environment's constraints while exposing a consistent interface to the agent.

**Message Passing**
The harness handles all communication with external entities through a durable message bus:

- _Agent-to-agent_ - Discovering other agents, sending requests, receiving responses
- _Agent-to-human_ - Surfacing questions, receiving instructions, reporting status
- _Agent-to-system_ - Registering offerings, reporting health, receiving control signals

Agents subscribe to message buses with different topologies:

- _1:1_ - Direct communication between two agents
- _1:N_ - Broadcast from one agent to many subscribers
- _M:N_ - Many-to-many communication for collaborative workloads

Messages are routed through the Interchange network with delivery guarantees and observability. The message bus uses SMTP/IMAP as its wire protocol (see Implementation), but Interchange clients are not email clients — they are purpose-built for Interchange message semantics, structured payloads, and conversation threading.

**Session Channels**
Session channels provide an optional real-time overlay on top of the message bus. Session channels are a protocol-layer concept — the streaming transport between client and harness — distinct from the instance lifecycle exposed in the API. The message bus (SMTP/IMAP) is always the canonical, durable transport for conversations — every complete message lands in the recipient's inbox regardless of whether a session channel is open. Session channels add real-time streaming for clients that want to see tokens as they arrive rather than waiting for complete messages.

When a client has a session channel open:

- _Streaming tokens_ - The client sees inference output token-by-token as it's generated
- _Complete messages_ - The finished message also lands in the client's IMAP inbox as usual
- _Client deduplication_ - The client is responsible for reconciling the streamed tokens with the complete message (typically by message ID or sequence number)

When a client is disconnected:

- Messages queue in the IMAP inbox as normal
- On reconnect, the client fetches missed messages via IMAP
- No tokens are lost — they were ephemeral previews of content that was persisted anyway

Session channels also support debugging and telemetry streams — attaching to a running agent to inspect state, watch events, or tail logs. Unlike conversation messages, debug and telemetry data does not flow through IMAP. It streams only over the session channel and is captured separately for auditing with standard retention policies.

Not every agent needs session channel support. Background agents, batch processors, and agents without interactive users operate purely through the message bus. Session channels are enabled when real-time interaction is required.

**Content Safety**
The harness mediates all external input before it reaches the agent's context. Inbound messages, tool responses, and user input are treated as data, not instructions. The harness applies structural framing to clearly delineate trusted and untrusted content. For sensitive operations, the harness can require human approval before proceeding - approval requests are surfaced through the message passing system and block execution until resolved.

### Event Handling

The harness is event-driven. Incoming events - messages from other agents, tool responses, inference completions, system signals - are received by the harness and routed to the appropriate internal handler. This decouples the external interface from the internal implementation; components register interest in event types and the harness dispatches accordingly.

### Lifecycle

1. **Initialization** - Harness starts, loads the agent package (skills, system prompt, context builder, initial state, materialized grants), establishes connections to inference and storage backends
2. **Registration** - Harness announces the agent's presence and offerings to the control plane
3. **Operation** - Harness enters the event loop, receiving and routing events to internal handlers
4. **Shutdown** - Harness deregisters, flushes state, and terminates cleanly

**Versioning**
Agent packages are immutable and versioned. When an agent is updated, the new version is deployed alongside the old. Traffic shifts to the new version after health checks pass. The previous version remains available for rollback.

**Health Checks**
The harness reports health status to the control plane. Liveness checks confirm the harness is responsive. Readiness checks confirm the agent can accept work. Unhealthy agents are removed from discovery until they recover.

**Graceful Updates**
When an agent is updated or retired, the harness drains in-flight work before shutting down. Messages are held at the bus until the new version is ready. Long-running operations complete or checkpoint before handoff.

### Authorization Delegation

Agents acquire capabilities from three sources: the tenant's organizational policies, the definition author (creator), and the user who launches the agent (invoker). These three authority domains combine to determine what the agent can actually do at runtime.

**Tenant-granted capabilities** come from organizational policies in the tenant hierarchy. These are baseline permissions that the tenant allows for agents of this type — resource access, network policies, spending limits. Tenant grants are resolved by walking up the tenant hierarchy, with child tenants able to add restrictions but not remove them.

**Creator-granted capabilities** are delegated by the definition author. The creator is the principal who authored the agent definition. Their authority is a persistent property of the definition — analogous to the setuid model in UNIX. The agent runs with capabilities granted by its creator, not limited to what the invoking user could do themselves. A creator can build an agent that performs privileged operations on behalf of users who lack direct access to those operations. These capabilities travel with the definition regardless of who later launches agents from it.

**Invoker-granted capabilities** are provided at launch time by the user who creates the agent. These are additional permissions the invoker delegates for the duration of the agent's lifetime — access to the invoker's data, authorization to act on the invoker's behalf with specific services, or credentials the agent needs to complete work for that particular user.

**Effective capabilities** are the union of tenant, creator, and invoker grants, materialized on the agent's principal. The control plane resolves all grant requirements at launch time and ships the effective grant set to the harness in the deploy frame. The harness enforces these materialized grants throughout the agent's lifetime. See AUTH.md for the resolution algorithm.

This three-source model enables important patterns:

- A creator builds an agent with access to a production database. Users invoke the agent to query data they couldn't access directly — the agent mediates access according to its own logic and policy.
- A creator builds an agent with deployment credentials. Users invoke the agent to trigger deployments without holding deployment keys themselves.
- An invoker grants an agent OAuth tokens to their personal accounts. The agent uses both its creator-granted infrastructure access and the invoker's personal credentials to complete a task.
- A tenant policy grants all agents in the tenant access to a shared logging service. Individual definitions don't need to declare this requirement.

**Capability scoping** applies across all three sources. Creator-granted capabilities can be broad but constrained by grants (the agent may hold database credentials but only be permitted to run read-only queries). Invoker-granted capabilities can be narrowed by the harness when an external API's permission model is coarser than what the agent needs. Tenant policies set the outer boundary that neither creator nor invoker grants can exceed.

**Inherited capabilities** apply when agents create other agents. The parent agent can grant its child a subset of its own creator-sourced capabilities, establishing a delegation chain. Children cannot exceed their parent's authority, but they can carry creator-sourced capabilities that future invokers of the child would not have on their own.

**Grant revocation** is policy-driven with a default of fail-secure. If the creator's grants are revoked after agents have been launched with creator-sourced grants, running agents must lose the affected grants. Propagating a revocation to an already-running deployment is not currently implemented — the earlier grants-update wire mechanism has been retired, and its supervised replacement is designed separately; a change takes effect when the deployment next loads its grants. Tenants can configure grace periods or notification-only behavior for specific grant types. Invoker-granted capabilities expire when the agent stops unless explicitly persisted.

Authorization grants are part of the agent's auditable state. The harness logs what was granted, by whom (tenant, creator, or invoker), when, and tracks all usage of delegated credentials.

### Isolation Model

Each harness runs in its own isolated context. The degree of isolation depends on the deployment environment, but the harness always enforces:

- Separate memory spaces between agents
- Explicit permission grants for cross-agent communication
- Wallet-based accounting for resource consumption
- Audit logging for all external interactions
- Alternate identity tracking for external services
- Content safety boundaries between trusted and untrusted input

### Trust Boundary

The harness is a security boundary between the agent (untrusted code) and the platform. The agent sees tool definitions, tool results, messages, and inference responses. It does not see:

- **Grant rules** — The agent never learns what actions are allowed, denied, or require approval. Exposing policy would let the agent reason about circumventing restrictions. When a tool call is blocked, the agent receives a generic refusal, not the grant rule that triggered it.
- **Private keys** — The agent's Ed25519 key pair is managed by the harness and used for signing on the agent's behalf. The agent cannot access, export, or influence key material.
- **Audit records** — Tool invocation audit records are written by the harness to the git storage layer. The agent has no read or write access to audit data.
- **Storage internals** — The harness persists conversation context and audit records in a git repository under `state/`. The agent has no access to the git layer, the filesystem, or any harness metadata. The agent cannot create checkpoints, branches, or inspect its own history.
- **Authorization decisions** — The agent receives the outcome of a tool call (result or refusal), never the authorization evaluation that produced it (which grants matched, what effect was applied, which principal was evaluated).

This separation is fundamental to the security model. The harness enforces policy precisely because the agent cannot observe or influence the enforcement mechanism.

### Cryptographic Identity

Every harness and every agent has its own asymmetric key pair. These keys serve as the foundation for identity and content provenance within Interchange.

**Per-harness keys** identify the runtime instance. The harness signs system-level messages (health reports, registration announcements, telemetry) with its key. This allows other components to verify that a message originated from a specific harness instance, not just a specific agent.

**Per-agent keys** identify the agent across its lifecycle, independent of which harness instance is running it. The agent's key pair is generated at launch and persists for the agent's lifetime. When an agent produces content — messages, tool invocations, checkpoints — the harness signs it with the agent's key. Recipients can verify that a specific agent generated specific content, providing a chain of provenance.

Key pairs are generated at agent launch time and managed by the harness. Private keys are stored alongside the agent's persistent data and never exposed to agents or external systems. Public keys are published to the control plane and included in the agent's discovery metadata. The control plane stores agent public keys so it can verify ownership claims when a harness reconnects after a restart.

**Commit signing** extends per-agent keys to the git layer. Every state commit (context checkpoints, audit records) is signed with the agent's Ed25519 key using SSH signature format. This means standard `git verify-commit` works with no custom tooling. The control plane verifies signatures when the sidecar pushes state, rejecting any commit not signed by the registered key. Deploy commits are signed by the hub's own key; sidecars verify deploy signatures before accepting content. See Implementation for the wire protocol details.

The control plane maintains a key validity history per agent — a list of `(publicKey, validFrom, validUntil)` tuples — so that historical commits remain verifiable after key rotation. When a key is retired (due to compromise, migration, or routine rotation), the old key is retained for signature verification but no longer accepted for new pushes.

### Agent Continuity

Agents survive harness restarts. The harness persists agent state (conversation context, pending operations, key pairs) in the agent's local storage. When the harness restarts, it discovers previously managed agents, proves ownership of each agent address by signing a cryptographic challenge with the agent's private key, and resumes operation from the persisted state. Continuity refers to a single agent surviving its own harness restart, not portability across agents from the same definition.

The authority model for agent continuity is:

- **Harness local storage is authoritative** for agent inference context — conversation history, pending operations, and token usage. This is the source of truth for what the agent knows.
- **Control plane is a delivery queue** for user messages. Messages sent while the harness is disconnected are queued and flushed to the harness on successful reconnect. The harness incorporates delivered messages into the agent's context through the normal message handling path.

The reconnection protocol requires the harness to prove it holds the private key for each agent address it claims to manage. This prevents a rogue harness from hijacking agents.

Signatures are attached to:

- Outbound messages (agent-to-agent, agent-to-human)
- Tool invocation requests and responses
- State commits (context checkpoints, audit records) — signed by the agent's key
- Deploy commits (skill code, prompts, configuration) — signed by the hub's key
- Registry announcements

The harness verifies inbound signatures automatically. Messages with invalid or missing signatures are flagged and can be rejected according to policy. Cross-tenant messages require valid signatures as a baseline trust requirement.

### Observability

The harness emits telemetry for all significant events:

- **Logs** - Structured records of agent activity, tool invocations, and policy decisions
- **Metrics** - Quantitative measurements: message throughput, inference latency, tool invocation counts, error rates
- **Traces** - Distributed traces that follow work across agent boundaries, linking cause to effect across multi-agent interactions

Telemetry is tagged with agent identity, tenant, and correlation IDs. The harness propagates trace context through the message bus so that downstream agents can continue traces started upstream.

### Alternate Identity Tracking

The harness maintains a record of all external identities that participate in an agent's work. This includes:

- **Model backends** - Which LLMs were invoked, including canonical model name, model provider identity, and model provider-specific model identifier
- **Model provider routing decisions** - Which model provider was selected for each inference call, why (priority, fallback after failure, load balancing), and any fallback events that occurred. This data supports debugging (why did this call go to model provider X?), cost attribution (how much traffic went to each model provider?), and reliability analysis (how often does model provider Y fail?)
- **Third-party services** - External APIs, tools, or data sources accessed during operation

Each external interaction is logged with the identity of the external service, timestamp, and sufficient context to reconstruct the interaction for debugging. This record is scoped to the agent and available for audit queries. The tracking is transparent to the agent itself - the harness handles it as part of its mediation layer for all external calls.

## Tenant Model

A tenant is a generalized isolation boundary within Interchange. Tenants can represent different organizational units depending on deployment context: an enterprise, a team, an individual operator, or a logical grouping of related agents. The tenant abstraction is flexible by design, allowing the same infrastructure to serve different multi-tenancy patterns.

### Tenant Scope

All resources in Interchange are scoped to a tenant:

- **Agents** - Each agent belongs to exactly one tenant. The tenant defines the administrative boundary for the agent's lifecycle, configuration, and policies.
- **Wallets** - Financial resources are tenant-scoped. Agents within a tenant may share wallet access according to tenant policy, but wallets do not cross tenant boundaries without explicit federation.
- **Data** - Persistent storage is isolated per tenant. Agents cannot access data belonging to other tenants unless explicitly shared through federation.
- **Message Buses** - Tenant-internal message channels are private by default. Cross-tenant messaging requires federation.
- **Control Plane** - Each tenant's slice of the control plane manages its agents, harnesses, credentials, and discovery data. The control plane handles federation with other tenants.

### Federation

Tenants are not siloed - they can federate to enable cross-tenant discovery and interaction:

- **Discovery** - Tenants can publish selected agents and offerings through the control plane, making them visible to other tenants.
- **Trust establishment** - Before cross-tenant interaction, tenants establish trust relationships. Trust can be bilateral (mutual agreement) or follow a hierarchical model (parent tenant grants access to child tenants).
- **Cross-tenant invocation** - Once trust is established, agents in one tenant can invoke tools and services provided by agents in another tenant. Authorization policies govern what cross-tenant actions are permitted.
- **Message routing** - Federated message buses allow agents in different tenants to communicate while maintaining tenant-level observability and control.

### Tenant Hierarchy

Tenants can be organized hierarchically:

- A parent tenant can contain child tenants
- Child tenants inherit policies from their parent (with the ability to add restrictions, not remove them)
- Resource quotas and permissions flow down the hierarchy
- Federation between sibling tenants still requires explicit trust establishment

The harness enforces tenant boundaries at runtime, ensuring that all resource access, message routing, and tool invocations respect tenant scope and federation policies.

## Control Plane

The control plane is the central orchestration and management layer for Interchange. It is tenant-aware, serving multiple tenants from shared infrastructure while maintaining strict isolation between them.

### Responsibilities

**Harness Management**
The control plane tracks all available harnesses within a tenant. Harnesses can be provisioned directly by the control plane (spinning up containers, VMs, or workers as needed) or registered externally (an operator brings their own compute and registers it with the control plane). The control plane maintains a pool of available harness capacity and assigns agents to harnesses based on resource requirements, affinity rules, and availability.

**Definition Management**
The control plane manages agent definitions — creating, versioning, and retiring blueprints. Definitions are catalog entries that describe what an agent can do. The control plane tracks version history, supports rollback to previous versions, and enforces update policies that govern how definition changes affect running agents.

**Agent Lifecycle**
The control plane launches agents from definitions onto harnesses. When an agent is launched, the control plane selects an appropriate harness, resolves the definition's grant, credential, and model requirements, transfers the agent package with materialized grants and resolved model providers, and instructs the harness to initialize the agent. The control plane tracks which agents are running on which harnesses, handles redeployment when harnesses fail, and coordinates graceful shutdown during updates or retirement.

**Discovery**
The control plane is the source of truth for discovery. It maintains a two-tier registry within each tenant: agent definitions as catalog entries (potential offerings that can be launched on demand) and running agents as live providers (immediately invocable with agent address and health status). The offerings endpoint returns both tiers, tagged with availability. The control plane also handles federation — publishing selected definitions and agents to other tenants and incorporating federated entries from trusted tenants into local discovery results.

**Health Monitoring**
The control plane continuously monitors harness and agent health. It polls health endpoints, processes heartbeat messages, and maintains the operational status of all components. Unhealthy agents are removed from discovery. Unhealthy harnesses trigger agent migration to healthy harnesses. Health data feeds into the observability layer for dashboards and alerting.

**Authorization Distribution**
The control plane manages how authority flows to agents via harnesses. Agent definitions declare grant requirements with source annotations (tenant, creator, invoker). When an agent is launched, the control plane resolves each requirement against the appropriate source — tenant policies, the definition creator's grants, or the invoker's grants — validates that the source has the authority to delegate, and materializes the effective grant set on the agent's principal. The harness receives the materialized grants in the deploy frame and enforces them at runtime.

**Credential Storage**
The control plane stores API keys, OAuth tokens, and other credentials that agents need to access external services. Operators configure integrations at the tenant or agent level; the control plane securely stores credentials and distributes them to harnesses at agent launch time. Agents never access credentials directly; the harness retrieves them from the control plane and injects them into outbound requests.

**Model Provider Management**
The control plane maintains the model provider catalog for each tenant — the set of model providers available and their configurations. Operators register model providers at the tenant level, specifying the model provider's name, endpoint, supported models (mapped to canonical model names), and authentication method (credential or wallet). The catalog follows the tenant hierarchy with the same shadowing semantics as credentials (see CREDENTIALS.md): child tenants inherit their parent's models, providers, and offerings, and can add new entries, disable an inherited entry for themselves and their descendants, or shadow an inherited entry with their own configuration, pricing, or restrictions. Resolution walks the tenant ancestor chain leaf-to-root; the nearest tenant to define an entry wins, and a disabled entry suppresses it for that tenant and its descendants. An offering's identity for inheritance is its canonical model name paired with its provider name, so disabling or shadowing a model or provider also disables or reroutes the inherited offerings that depend on it. The control plane also maintains the global model registry — canonical identifiers for well-known models — which tenants can extend with custom names for private or self-hosted models. The control plane resolves agent definitions' model requirements against this catalog at launch, producing the ordered model provider list shipped to the harness. The control plane monitors model provider health and can push model provider updates to running agents when model providers become unavailable or new ones are added, following the same push model as grant updates.

**Message Bus Management**
The control plane manages the message bus infrastructure for each tenant. It configures routing rules, manages distribution lists, enforces rate limits, and handles cross-tenant federation for messaging. The message bus itself may be implemented as a separate service, but the control plane provides the configuration and policy layer.

**Channel Routing**
For agents that support session channels, the control plane brokers connections between clients and harnesses. In production deployments, clients do not connect directly to harnesses — they connect to the control plane, which routes channel traffic to the appropriate harness. This keeps harnesses from needing public addresses and enables NAT traversal for harnesses running on mobile, embedded, or firewalled networks. Harnesses maintain a persistent outbound connection to the control plane; channel traffic tunnels through this connection when a client connects to an agent.

Session channels are optional. Agents that do not require real-time streaming (background processors, batch workers, agent-to-agent workflows) operate purely through the message bus and do not expose streaming endpoints.

Agent launch and channel attachment flow:

1. Client authenticates with the control plane and launches an agent from a definition via `POST /agents/instances`
2. Control plane validates the client's identity and authorization, resolves grant and credential requirements, and deploys the agent to a sidecar
3. Control plane returns the running agent with its address and public key
4. Client opens a session channel to the agent's SSE endpoint (`GET /agents/instances/:instanceId/events`)
5. Control plane routes the channel to the harness hosting the agent
6. Harness enforces the materialized grants for the agent's lifetime

In development environments, clients may connect directly to a locally-running harness, bypassing the control plane for convenience.

**Observability and Debugging**
The control plane aggregates telemetry from harnesses and agents, providing a unified view of system behavior. Operators use the control plane to trace agent interactions, inspect message flows, debug failures, and audit activity. The control plane correlates distributed traces across agent boundaries and surfaces anomalies for investigation.

**Certificate Authority**
The control plane operates a certificate authority for each tenant. It issues X.509 certificates wrapping agent Ed25519 keys, enabling TLS mutual authentication. Certificate issuance, renewal, and revocation are managed through the control plane.

### Relationship to Harnesses

The control plane and harnesses have a clear division of responsibility:

- **Control plane** — orchestration, configuration, credential distribution, discovery, health monitoring, observability aggregation
- **Harness** — execution, tool invocation, message sending/receiving, policy enforcement, local data management

The harness is the runtime; the control plane is the management layer. Harnesses register with the control plane on startup and maintain a persistent connection for receiving commands and reporting status. The control plane can instruct a harness to launch an agent, update credentials, or shut down gracefully.

### Deployment Models

The control plane supports multiple deployment configurations:

- **Hosted** — Faremeter operates the control plane as a service; tenants connect their harnesses or use Faremeter-provisioned compute
- **Self-hosted** — Organizations run their own control plane infrastructure, maintaining full control over their agent ecosystem
- **Hybrid** — A self-hosted control plane federates with the hosted service for cross-organization agent discovery and interaction

## Wallets

Wallets enable agents to send and receive assets — cryptocurrency, fiat currency, or platform credits. They provide the economic layer that makes agents accountable participants in a transactional ecosystem.

### Wallet as Abstraction

A wallet is an abstraction over payment backends. Interchange does not hold funds directly; instead, tenants configure wallet plugins that connect to their chosen payment infrastructure:

- Cryptocurrency wallets (Ethereum, stablecoins, etc.)
- Fiat payment processors (Stripe, bank integrations)
- Platform credit systems (internal ledgers)

The wallet interface is uniform regardless of backend. Agents interact with wallets through the harness without knowing or caring whether they're spending ETH or USD.

### Wallet Scope and Sharing

Wallets are tenant resources managed through the control plane:

- A tenant can have multiple wallets (different currencies, different purposes)
- Multiple agents can share access to the same wallet
- A single agent can have access to multiple wallets
- Wallet assignment is flexible and policy-controlled

### Agent Access to Wallets

Agents never hold wallet keys or direct access to funds. Wallet access is a capability granted through the authorization model:

**Creator-granted wallet access** — The definition declares wallet access as a grant requirement with `source: "creator"`. At launch, the control plane resolves this against the creator's authority and materializes the wallet grant on the agent's principal. For example, an agent might have spending authority from a tenant's API-costs wallet to pay for external services it uses.

**Invoker-granted wallet access** — A user grants the agent temporary access to their wallet for a session. For example, a user might authorize an agent to make purchases on their behalf.

The harness mediates all wallet operations. When an agent needs to pay for something, it requests payment through the harness's tool interface. The harness enforces policy (spending limits, approved recipients, transaction types) and executes the transaction using the wallet's backend plugin. The agent sees only success or failure, never keys or account details.

### Spending and Earning

Wallet operations are exposed to agents as tools. The harness provides payment tools that agents invoke like any other capability:

- **pay** — Send funds to a recipient
- **request_payment** — Request payment from a caller before proceeding
- **check_balance** — Query available funds
- **verify_payment** — Confirm a payment was received

These tools abstract over the underlying payment protocols. The harness handles protocol-specific details (transaction signing, proof generation, receipt verification) while the agent works with simple pay/receive semantics.

**Agent as consumer:**

1. Agent invokes a remote tool or service
2. Service requests payment (protocol-specific)
3. Agent invokes the `pay` tool with payment details
4. Harness evaluates against policy — is this spend authorized?
5. If authorized, harness executes payment via wallet plugin
6. Agent resubmits request with proof of payment

**Agent as provider:**

1. Remote caller invokes the agent's capability
2. Agent invokes `request_payment` tool to charge the caller
3. Caller pays
4. Agent invokes `verify_payment` to confirm receipt
5. Agent performs the work

The x402 protocol (HTTP 402 Payment Required) is one supported payment flow, useful for HTTP-based tool invocations where the harness can handle payment negotiation transparently. But agents can also use wallet tools directly for arbitrary payment scenarios — tipping, subscriptions, escrow, or custom billing logic.

### Pricing

When agents provide services, pricing is controlled through policy:

- **Operator-configured** — The operator sets base prices, acceptable payment methods, and pricing rules
- **Agent-negotiable** — Within operator-defined bounds, agents can adjust pricing dynamically (demand-based pricing, bulk discounts, etc.)

Pricing metadata is published as part of the agent's offering advertisement in the control plane.

### Payment Failure

When a wallet cannot complete a payment:

- **Default behavior** — The request fails immediately. The agent receives an error indicating insufficient funds or payment failure.
- **Overdraft policy** — Operators can configure controlled overdraft to prevent service interruption. Policy defines overdraft limits and conditions.

Payment failures are logged and can trigger alerts. Persistent payment failures may result in the agent being suspended or removed from discovery.

### Cross-Tenant Payments

Payments between agents in different tenants work through standard x402 flows. The paying agent's wallet plugin handles the actual transfer; the receiving agent's wallet plugin handles receipt verification. Federation trust policies can restrict which tenants an agent is permitted to transact with.

## Change History

Runtime state lives at the repository root alongside the deployed definition under `deploy/`. This unified repository means the full history — what the agent was running and what it did — is available in a single DAG.

The reactor auto-commits at every cycle boundary (after each inference completes, after a batch of tool calls completes, after a compaction runs). The director's `checkpoint` action no longer decides _whether_ a commit happens; it supplies the human-meaningful message for the next auto-commit. Audit records (`state/audit/`) and error records (`state/errors/`) are still committed separately by the audit store on its existing schedule (`afterCheckpoint` and `onShutdown`). The agent has no awareness of or control over when commits occur (see Trust Boundary).

### Per-Cycle Working Tree

The context store records every cycle's content as a snapshot of these top-level files:

```
turns.jsonl              durable conversation history
prompt.jsonl             what was sent to inference for the most recent cycle
response.jsonl           what came back for the most recent cycle
manifest.jsonl           ordered transform records produced for the most recent cycle
metadata.json            pending operations, cumulative token usage, connector thread state
tool-output/             spill blobs from oversized tool results, keyed by tool call id
```

Per-cycle files are overwritten each cycle; prior versions live in git commits. `git log -- prompt.jsonl` shows every inference call's input; `git log -- turns.jsonl` shows when the durable history changed. Spill blobs live in `tool-output/` and migrate with the conversation; the agent reaches them through the `BlobReader` capability that resolves `tool-output:///{callId}` URIs to the working-tree path.

`ContextStore` exposes per-cycle writers (`writeTurns`, `writePrompt`, `writeResponse`, `writeManifest`, `writeBlob`, `writeMetadata`) and a single `commit({ message })` overload that snapshots the working tree. The reactor writes through each transform's output immediately so the working tree is the live state, then issues one commit per cycle. The legacy `commit(turns, ops, usage, message)` signature and the `state/context.json` serializer are gone.

### Operator Inspection

The control plane exposes the agent's change history to operators through the Agent Data API. Operators can list files, read content, browse commit history, and inspect individual commits. This provides visibility into what the agent was doing and when, useful for debugging, auditing, and incident response. The agent itself cannot access this history — it is a platform-level inspection surface, not an agent-facing capability.

### Garbage Collection

Agent repos grow without bound: every accepted state or deploy pack advances a ref and strands the prior tip's objects, and the sidecar's reactor commits loose objects every cycle. iso-git has no `git gc` equivalent, so the storage layer reclaims explicitly. The collector (`runGC` in `@intx/storage-isogit`) repacks everything reachable from the repo's refs — the keep set unions reachability over `refs/heads/main` and `refs/heads/deploy`, since collecting one head alone would discard the other's live objects — into a single pack published through the same atomic staging dance receives use, then drops the superseded packs and loose objects.

**Cadence is owned by the writers, on the write path.** There is no GC daemon and no external operator job. After a successful write, the writer samples the repo's object counts — two cheap directory reads — and reclaims once it crosses a configured pack-count or loose-object threshold. When a reclaim runs it checks the `.git` byte size against a budget and warns if the repo is still over it, surfacing runaway accumulation that survives a reclaim; the recursive byte walk rides the reclaim rather than every write. The hub triggers this from its repo substrate (state-pack receives and deploy-ref commits), while it holds its higher-level repo lock; the sidecar triggers it from the context store's commits, while it holds the per-directory lock the commit already took. Either way the reclaim runs under a lock held against concurrent writers.

**Concurrency.** A sidecar agent repo is written by several drivers that share no higher-level lock — the reactor's context commits, the mail-audit commits, and deploy applies, with the state-pack producer reading alongside. The storage layer owns the serialization: a per-directory lock keyed by the resolved working-directory path is acquired by every object-store mutation and by the collector, so GC always observes a quiescent store. It nests harmlessly under the hub's higher-level repo lock.

**Retention is configurable per environment.** The hub defaults to `keep-history`: it is the long-term archive of an agent's state graph, so the commit ancestry is preserved. The sidecar defaults to `tip-only`: it treats the repo as the agent's current state, so dropping ancestry keeps the repo small for reactor read and commit latency. This is safe because the durable conversation lives in the working-tree files at the tip (`turns.jsonl` is the full conversation, rewritten each cycle), not in commit history — the reactor's own loop only ever reads the tip. Commit history on the sidecar is therefore a best-effort time-travel surface: the history-read paths (`log`, `readManifestHistory`, the agent's `checkpoints` tool) walk from the tip and stop at the first pruned parent, returning the surviving slice rather than failing. An operator that needs full sidecar history sets retention to `keep-history`. Thresholds and retention are resolved at each boot edge: `HUB_AGENT_GC_PACK_THRESHOLD`, `HUB_AGENT_GC_LOOSE_THRESHOLD`, `HUB_AGENT_GC_WARN_BYTES`, `HUB_AGENT_GC_RETENTION` on the hub, and the `SIDECAR_AGENT_GC_*` equivalents on the sidecar.

Reclaiming a rejected pack is separate and immediate: `receivePackObjects` and `applyPack` unpublish a caller-rejected pack at the moment of rejection rather than waiting for the next GC threshold, so a flood of bad packs cannot accumulate in the window before reclaim.
