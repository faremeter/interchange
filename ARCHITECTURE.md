# Faremeter Interchange

*Implementation*

## Agent Kernel

The agent kernel is the core runtime component deployed for each agent instance. It acts as the glue layer that binds together all the capabilities an agent needs to operate autonomously within the Interchange ecosystem.

### Responsibilities

The kernel orchestrates five primary concerns:

**Inference**
The kernel manages the connection to the agent's model backend - whether a local model, remote API, or self-hosted inference server. It handles request/response cycles, streaming, context management, and model-specific protocol translation. The agent's reasoning happens through this interface.

**Tools**
The kernel exposes a standardized interface for invoking tools, whether local or remote. Local tools run within the agent's runtime - file system access, code execution, network requests, or custom tools registered by the operator. Remote tools are discovered through the registry and invoke capabilities exposed by other agents or services on the Interchange network. From the agent's perspective, local and remote tools share the same interface; the kernel handles protocol negotiation, request routing, and wallet-based payment transparently. All tool invocations are subject to authorization policies.

**Local Data**
The kernel provides access to persistent storage scoped to the agent. This includes working memory, cached artifacts, and any other state the agent accumulates during operation. Data is isolated per-agent unless explicitly shared. Credentials are managed separately by the kernel and not exposed as agent-accessible data.

**Environment Integration**
Kernels deploy into execution environments of varying capability:

- **Cloudflare Workers** - Edge deployment with global distribution, limited to stateless compute
- **Docker Containers** - Full OS-level isolation with network and filesystem access
- **Virtual Machines** - Complete machine-level isolation for untrusted workloads
- **Local Processes** - Direct execution on the host for development and personal use

The kernel adapts to the constraints and capabilities of its environment while exposing a consistent interface to the agent.

**Message Passing**
The kernel handles all communication with external entities:

- *Agent-to-agent* - Discovering other agents, sending requests, receiving responses
- *Agent-to-human* - Surfacing questions, receiving instructions, reporting status
- *Agent-to-system* - Registering capabilities, reporting health, receiving control signals

Messages are routed through the Interchange network with delivery guarantees and observability.

### Lifecycle

1. **Initialization** - Kernel starts, loads configuration, establishes connections to inference and storage backends
2. **Registration** - Kernel announces the agent's presence and capabilities to the registry
3. **Operation** - Kernel enters the event loop, processing incoming messages and inference requests
4. **Shutdown** - Kernel deregisters, flushes state, and terminates cleanly

### Isolation Model

Each kernel runs in its own isolated context. The degree of isolation depends on the deployment environment, but the kernel always enforces:

- Separate memory spaces between agents
- Explicit permission grants for cross-agent communication
- Wallet-based accounting for resource consumption
- Audit logging for all external interactions
