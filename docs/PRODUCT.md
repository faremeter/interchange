# Faremeter Interchange

_Product Brief_

**The infrastructure layer for the agentic era.**

We are witnessing an explosion of AI agents - autonomous systems that can reason, act, and collaborate. But the infrastructure to run them securely, connect them reliably, and hold them accountable doesn't exist. Today's agents run in ad-hoc environments with no isolation, no standard way to discover each other's offerings, and no mechanism for economic accountability. As agents become more powerful and prevalent, this isn't just inconvenient - it's dangerous.

Faremeter Interchange is the foundational platform for deploying, connecting, and governing AI agents. It provides the primitives necessary for agents to operate as trusted, autonomous participants in a networked economy.

## Core Capabilities

**Secure Runtimes**
Interchange provides isolated execution environments - language runtimes, containers, or virtual machines - where agents can operate without risking the host system or each other. Operators define security policies; Interchange enforces them.

**Model Flexibility**
Agents can use any model backend - local models running on-device, remote APIs, or self-hosted inference servers. Interchange abstracts the model layer so agents remain portable across deployments.

**Agent Builder**
Interchange provides tools to construct agents and skills with properly assembled context and prompts. The builder handles composition of system prompts, skill instructions, initial state, and runtime context into coherent agent configurations. Operators define the building blocks; the builder assembles them into deployable agents.

**Tool Discovery**
Agent definitions declare their offerings in a catalog. Running agents provide live offerings that are immediately invocable. Discovery returns both: live agents you can call now, and definitions that can be launched on demand. Need an agent that can search the web, execute code, or process payments? Discover it, verify its permissions, and invoke it - all through standard protocols.

**Human Interface**
Humans interact with agents through the same messaging and session infrastructure that agents use with each other. On desktop and mobile, users connect to agents through session channels that stream responses in real-time. When disconnected, messages queue and are available on reconnect. The interface is native to each deployment platform - not a bolted-on afterthought. Agents don't distinguish between human and agent callers at the protocol level; the harness handles both through the same message bus and session channel primitives.

**Message Passing**
Interchange handles communication between agents and humans with built-in routing and delivery guarantees. The hub acts as a mail server: it stores raw MIME bytes at routing time and serves parsed JMAP Email objects (RFC 8621) to clients. The UI acts as a mail client, consuming these structured views. This design is forward-compatible with JMAP (RFC 8620/8621) for future federation and external mail client support. Agents can collaborate on complex tasks without bespoke integration work.

**Wallets**
Every agent has a wallet. Agents spend to use tools and resources; agents earn by providing services to others. This creates accountability - an agent that misbehaves loses economic access - and enables entirely new interaction patterns where agents can autonomously transact, bid for work, and allocate resources.

**Authorization**
Agent definitions declare what capabilities they need and where the authority should come from — the tenant's policies, the definition author, or the person launching the agent. At launch, the control plane resolves these requirements into effective grants. Identity is cryptographically verifiable. Trust is explicit, auditable, and revocable.

**Multi-Tenancy**
Interchange supports flexible isolation boundaries for agents and resources. A tenant can represent an organization, a team, an individual operator, or a logical grouping of agents - the boundary is defined by whoever deploys and manages the infrastructure. All resources - agents, wallets, data, message channels, and registries - are scoped to tenants by default. Tenants can federate, allowing agents to discover and interact with agents in other tenants when explicitly permitted. This enables shared infrastructure to host multiple independent parties while maintaining strong isolation guarantees.

**Lifecycle Management**
Agent definitions and agents have distinct lifecycles. Definitions are created, versioned, and retired as catalog entries. Agents are launched from definitions, monitored for health, and stopped when their work is done. Definitions can be rolled back to previous versions. The definition's update policy governs how changes affect running agents — from automatic redeployment to manual control.

**Observability**
Operators see what their agents are doing. Interchange provides structured logs, metrics, and distributed tracing across agent interactions. Dashboards surface resource usage, message flow, and anomalies. When something goes wrong, operators can trace the chain of events that led there.

**Content Safety**
Agents receive input from many sources - other agents, external tools, files, and humans. Interchange provides layered defenses to prevent malicious or malformed input from hijacking agent behavior. External content is isolated from trusted instructions, actions are validated against policy before execution, and sensitive operations can require human approval.

**Change History**
Agents working with local data need to understand what changed and when. Interchange provides built-in change tracking for agent-local storage - every file the agent creates or modifies is tracked automatically. Agents can create named checkpoints, branch to explore alternatives, and recover previous states. This enables safe experimentation: an agent can try an approach, and if it doesn't work, roll back cleanly. For debugging and auditing, the complete history of an agent's local modifications is available.

**Interaction Recording**
Every conversation - human-to-agent and agent-to-agent - is durably recorded through the message bus. Token-level streaming, tool invocations, policy decisions, and inference metadata are captured alongside the conversation content. This data is tenant-scoped, queryable, and available for downstream analysis: cost attribution, performance evaluation, compliance reporting, or custom analytics tooling built on top. Interchange captures the raw interaction data; what you build on it is up to you.

**Audit Trail**
Agents interact with external systems - LLMs for inference, APIs for tools and data, locally hosted services. Interchange tracks which external services participated in an agent's work. When something goes wrong or needs auditing, operators can see exactly which models and services were involved, enabling effective debugging and compliance reporting.

## Standards & Integration

Interchange builds on established protocols rather than inventing new ones:

- **Offering discovery** - Agents expose and discover offerings using MCP, OpenAPI, or A2A
- **Payments** - Wallets send and receive using [Faremeter](https://github.com/faremeter/faremeter), supporting a superset of payment protocols (x402, L402, and others) with an open integration layer for third-party payment systems
- **Identity** - Cryptographic identity based on existing PKI and DID standards
- **Messaging** - Built on SMTP/IMAP for agent-to-agent communication; JMAP Email (RFC 8621) for the client-facing API

This approach reduces integration friction and allows Interchange to leverage existing tooling, libraries, and operational knowledge.

## Deployment

- **Local** - Desktop or laptop for personal agent infrastructure
- **Mobile** - iOS or Android for agents on the go
- **Embedded** - IoT and edge devices for agents in the physical world
- **Network** - Deployed as a service for teams or organizations
- **Hybrid** - Combine any of the above; mobile and embedded devices can interact with agents running anywhere

## The Vision

Interchange is the foundation for a world where agents are first-class participants - discoverable, accountable, and economically active. Not just tools we invoke, but collaborators we deploy into a shared infrastructure designed for their unique needs.
