# Faremeter Interchange

_Implementation_

## Message Bus: SMTP/IMAP Transport

The Interchange message bus is built on SMTP and IMAP as wire protocols, leveraging existing global messaging infrastructure as the transport layer for agent communication. SMTP/IMAP provide the durable, federated message routing that Interchange needs — but Interchange clients are not email clients.

Interchange clients (harnesses, UIs, CLIs) speak SMTP/IMAP to send and receive messages, but they are purpose-built for Interchange semantics: structured message payloads, conversation threading, offering negotiation, and real-time session channels. A traditional email client _could_ connect to an agent's inbox and see messages, but the experience would be like reading raw HTTP traffic — technically possible, not practically useful.

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
ins_xxxxx@domain.interchange.network
```

The local part identifies the agent; the domain identifies the tenant. Tenant boundaries map directly to SMTP domains, providing natural isolation and federation semantics. Multiple agents launched from the same definition each receive their own address.

### Message Transport

**Outbound (SMTP)**
When an agent sends a message to another agent or human, the harness composes and submits it via SMTP. The payload is serialized into the body. Structured data uses MIME multipart encoding.

**Inbound (IMAP)**
The harness maintains an IMAP connection to the agent's inbox. Incoming messages are fetched, parsed, and converted into events that the harness routes to internal handlers.

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

Beyond authenticating message senders, the harness applies content-level safety measures:

**Context Framing**
All input assembled into the agent's context uses consistent structural delimiters. System prompts and skill instructions are clearly marked as trusted. External content (messages, tool responses, user input) is framed as data with explicit boundaries. The framing format is designed to be recognizable by models and resistant to delimiter injection.

**Action Validation**
Before executing tool invocations, the harness evaluates the action against the agent's materialized grants (entries where `resource` matches `tool:*`). Validation checks include:

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

The harness generates one Ed25519 key pair per harness and per agent. The same key material is used across protocols, with SSH, PGP, and X.509 serving as wire formats depending on context:

- **SSH format** - Used for harness-to-harness communication, control plane interactions, and general identity verification. Public keys are published in OpenSSH format.
- **PGP format** - Used for message-level signatures and encryption over the SMTP/IMAP transport, where PGP integration with email infrastructure is a natural fit. Agent PGP keys can be published via DNS (DANE/OPENPGPKEY records) or through the control plane.
- **X.509 format** - Used for TLS client certificates enabling mutual authentication. The control plane issues X.509 certificates wrapping the agent's Ed25519 public key, signed by a tenant certificate authority. This allows agents to authenticate via standard TLS mutual auth without introducing a separate key type.

**Protocol mapping:**

- **Agent/harness identity and control plane** - Ed25519 in SSH format
- **Message payload signatures** - Ed25519 in PGP format, integrated with SMTP/IMAP transport
- **End-to-end message encryption** - X25519 in PGP format (S/MIME as an alternative where required)
- **TLS mutual authentication** - Ed25519 in X.509 format, certificates issued by the control plane

The harness manages key generation, storage, rotation, and revocation. Key rotation follows a grace period model: the new key is published alongside the old key, both are accepted for verification during the overlap window, and the old key is retired after the grace period.

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

Session channel messages are JSON objects with a type field, sequence number, and payload. The event types are defined in INFERENCE.md and shared end-to-end — the inference layer emits the same events that session channel subscribers receive.

```json
{"type": "inference.start", "seq": 1, "data": {"model": "claude-4"}}
{"type": "inference.text.delta", "seq": 2, "data": {"token": "The", "partial": {...}}}
{"type": "inference.thinking.delta", "seq": 3, "data": {"token": "Let me think", "partial": {...}}}
{"type": "inference.text.delta", "seq": 4, "data": {"token": " weather", "partial": {...}}}
{"type": "inference.usage", "seq": 47, "data": {"input": 120, "output": 47, "cacheRead": 0, "cacheWrite": 80, "thinking": 15}}
{"type": "inference.done", "seq": 48, "data": {"message_id": "<abc123@example.com>", "usage": {"input": 120, "output": 47}}}
{"type": "user.message", "data": {"content": "What's the weather?"}}
```

Event types are namespaced by category: `inference.*` for model streaming, `tool.*` for tool execution, `reactor.*` for lifecycle, `fork.*` for fork management, `message.*` for inbound messages, `custom.*` for plugin-defined events, and `user.*`, `debug.*`, `system.*` for session-level events. See INFERENCE.md for the complete event type reference.

### Connection Establishment

**Production (via control plane):**

1. Client authenticates with the control plane via HTTPS, providing identity credentials and target agent
2. Control plane returns a session token (JWT signed by the control plane, containing agent ID, client identity, capabilities, expiry)
3. Client opens WebSocket to control plane's session endpoint: `wss://controlplane.example/session`
4. Client sends session token in the first message
5. Control plane validates token and routes connection to the harness hosting the target agent
6. Harness sends `session.ready` message; bidirectional streaming begins

**Development (direct to harness):**

1. Client opens WebSocket directly to harness: `ws://localhost:8080/session`
2. Client sends authentication (API key, or signed challenge)
3. Harness validates and sends `session.ready`

### Authentication

Session tokens issued by the control plane are JWTs containing:

- `sub` — client identity
- `aud` — target agent ID
- `cap` — invoker-granted capabilities for this session
- `exp` — token expiry (short-lived, typically minutes)
- `jti` — unique token ID for replay prevention

Tokens are signed with the control plane's Ed25519 key. Harnesses verify the signature against the control plane's published public key.

### Streaming Inference

When an agent performs inference, the harness streams events to the session channel as they arrive from the model backend. Events use the protocol defined in INFERENCE.md:

```
← {"type": "inference.start", "seq": 1, "data": {"model": "claude-4"}}
← {"type": "inference.text.delta", "seq": 2, "data": {"token": "The", "partial": {...}}}
← {"type": "inference.text.delta", "seq": 3, "data": {"token": " weather", "partial": {...}}}
← {"type": "inference.text.delta", "seq": 4, "data": {"token": " today", "partial": {...}}}
...
← {"type": "inference.usage", "seq": 47, "data": {"input": 120, "output": 47, "cacheRead": 0, "cacheWrite": 80, "thinking": 15}}
← {"type": "inference.done", "seq": 48, "data": {"usage": {"input": 120, "output": 47}, "message_id": "<abc123@example.com>"}}
```

Sequence numbers are monotonic across the session (not per-inference-call), enabling gap detection and ordered replay. The `inference.usage` event fires before `inference.done`, allowing clients to process token accounting before the terminal event. `inference.done` also carries a usage summary for clients that only need totals. Reasoning tokens stream as `inference.thinking.delta` events, separate from `inference.text.delta`.

When inference completes, the complete message is also delivered via SMTP to the client's IMAP inbox. Clients connected to both the session channel and IMAP will see the content twice — once as streaming tokens, once as the complete message. The client is responsible for deduplication, typically by matching the message ID included in `inference.done` with the IMAP message.

### Heartbeats and Timeouts

Both client and harness send periodic heartbeat messages to detect dead connections:

```json
{"type": "system.ping", "data": {"ts": 1699999999}}
{"type": "system.pong", "data": {"ts": 1699999999}}
```

Connections without activity or heartbeat responses for 30 seconds are terminated. Clients are expected to reconnect and resume if needed.

### Reconnection and Durability

Session channels are ephemeral — there is no built-in resume capability for token streams. If a connection drops mid-stream, those tokens are lost. However, conversation durability is guaranteed through two mechanisms depending on the transport layer:

**Production (IMAP transport):**

1. Client reconnects and re-authenticates
2. Client fetches any messages that arrived via IMAP while disconnected (using QRESYNC for efficient sync)
3. New streaming resumes from the current point

The IMAP inbox is the source of truth for conversation history. Session channels are a real-time optimization, not a replacement for IMAP.

**Prototype (hub-mediated sidecar transport):**

1. Sidecar reconnects to the hub and proves ownership of agent addresses via signed challenge (see HARNESS_DESIGN.md)
2. Hub refreshes grants via `grants.update` frame — the sidecar must have current grants before processing any messages
3. Hub flushes queued undelivered messages as `message.send` frames for verified agents
4. Sidecar loads agent context from isogit and resumes operation

In the prototype, the hub's database serves as the delivery queue for messages sent while the sidecar is disconnected. The sidecar's isogit repository is the source of truth for agent inference context. The hub maintains a sidecar-to-agent mapping so it knows which sidecar to route messages to for a given agent address. See HARNESS_DESIGN.md for the reconnection wire protocol.

### Sidecar Agent Lifecycle Frames

The sidecar WebSocket protocol includes frames for agent deployment, reconnection, and address verification.

**Agent deployment:**

| Direction     | Frame                | Purpose                                                |
| ------------- | -------------------- | ------------------------------------------------------ |
| Hub → Sidecar | `agent.deploy`       | Provision an agent (keys, directory, ephemeral config) |
| Sidecar → Hub | `agent.deploy.ack`   | Confirm provisioning, provide agent public key         |
| Hub → Sidecar | `session.start`      | Start the inference harness for a provisioned agent    |
| Sidecar → Hub | `session.start.ack`  | Confirm the harness is running                         |
| Hub → Sidecar | `agent.undeploy`     | Remove an agent from the sidecar                       |
| Sidecar → Hub | `agent.undeploy.ack` | Confirm teardown (includes state push status)          |
| Sidecar → Hub | `agent.error`        | Report a failure at any stage                          |

Agent deployment is a three-phase operation: provision, pack delivery, session start. The hub sends `agent.deploy` to provision the agent (generate keys, create directory, persist config). After receiving `agent.deploy.ack` with the agent's public key, the hub streams the deploy tree via pack frames. After `pack.ack`, the hub sends `session.start` to start the inference harness. This ordering ensures the deploy tree (prompt, skills) is on disk before the harness reads it.

Undeploy is an acknowledged operation. The sidecar stops the harness, pushes state to the hub (best-effort), deletes the agent directory, and responds with `agent.undeploy.ack`. The `statePushed` field indicates whether the state push was attempted. The hub defers routing table cleanup until the ack arrives.

**Reconnection:**

| Direction     | Frame                | Purpose                                          |
| ------------- | -------------------- | ------------------------------------------------ |
| Sidecar → Hub | `reconnect`          | Announce agent addresses after WebSocket connect |
| Hub → Sidecar | `challenge`          | Per-address nonces to sign                       |
| Sidecar → Hub | `challenge.response` | Signed proofs of key ownership                   |
| Hub → Sidecar | `challenge.failed`   | Verification failure for a specific address      |

**Grant management:**

| Direction     | Frame           | Purpose                                              |
| ------------- | --------------- | ---------------------------------------------------- |
| Hub → Sidecar | `grants.update` | Push updated grants to a running agent (request/ack) |

On first connection (no existing agents), the sidecar sends a `register` frame. On reconnection (agents in data directory), it sends `reconnect`. After successful challenge verification, verified addresses are provisionally added to the routing table so the `grants.update` round-trip can reach the sidecar. If grant refresh fails, the address is rolled back from the routing table and its queued messages are preserved for the next reconnect attempt.

### Debug and Telemetry Streams

Debug and telemetry data (state inspection, trace output, log tailing) flows only over session channels — it is not delivered via IMAP. This data is captured for auditing purposes with standard retention policies but is not part of the durable conversation record.

```json
{"type": "debug.state", "data": {"context_tokens": 4096, "tools_invoked": 3}}
{"type": "debug.trace", "data": {"span_id": "abc123", "event": "tool_start", "tool": "web_search"}}
{"type": "telemetry.log", "data": {"level": "info", "message": "Processing request"}}
```

Debug streams require explicit authorization — not all clients are permitted to attach debuggers to agents.

## Workflow Process: Supervisor/Child IPC

A deployed workflow runs in two processes per active deployment. The supervisor lives in the sidecar — it holds the mail-bus identity for the deployment's addresses, owns the per-deployment Ed25519 signing key the hub trusts, and reads the per-step credential snapshots out of each step's `agent-state` repo. The workflow-process is a Bun child the supervisor spawns; it loads operator-supplied workflow code (tools, directors, agent prompts) and executes runs against the runtime adapters. The supervisor is the authoritative side; the workflow-process is the untrusted side.

The IPC between them is the airlock between those two halves. Per-frame crypto is what makes the airlock seal: nothing the child receives is acted on until its signature verifies under the supervisor's public key, and nothing the supervisor accepts from the child is acted on until its HMAC verifies under the shared key. The implementation lives at `packages/workflow-host/src/ipc/` with the threat model at the top of `packages/workflow-host/src/ipc/index.ts`.

### Why This Boundary Exists

The workflow-process runs code the operator deploys but the sidecar does not fully control: tool implementations, director prompts, agent harnesses talking to external APIs. The supervisor must continue to be trusted by the hub regardless of what happens inside the child, because the supervisor holds the deployment's credentials and signing keys. The IPC channels exist so the supervisor can hand the child everything it needs to run while keeping the credentials and the signing key inside the supervisor's own address space.

### Two Channels, Split By Cost And Rate

Each deployment gets two channels:

- **Control channel.** NDJSON over stdio, Ed25519-signed by the supervisor. Carries trigger fires, signal deliveries, drain, recycle, shutdown, grants-updated, sources-updated, and the child's `ready` signal back. Low rate, high authority. Implementation lives in `packages/workflow-host/src/ipc/control-channel.ts`.
- **Event channel.** A UNIX socketpair, HMAC-SHA256-authenticated with a 32-byte symmetric key derived at spawn time. Carries `InferenceEvent`s from the reactor (including the per-message `message.run.started` / `message.run.ended` brackets). High rate. Implementation lives in `packages/workflow-host/src/ipc/event-channel.ts`.

Asymmetric crypto on control is correct because only the supervisor signs and the child must not forge supervisor commands. Symmetric HMAC on events is correct because both sides must authenticate every frame at reactor cadence — Ed25519 per frame at that rate would dominate runtime cost. The two channels' payload unions are disjoint by construction; the typed `ControlPayload` validator does not accept inference-event shapes and the typed `EventPayload` validator does not accept control-plane shapes, so a confused-deputy attack that smuggles a `drain` or `recycle` over the event channel fails at validation.

### Frame Envelope

Every frame on either channel carries `{ seq, channelId, payload }` inside the bytes that are authenticated. `seq` is a monotonic u64 counter the sender maintains per channel. `channelId` is the supervisor-minted channel identity. `payload` is the channel-specific JSON value. The wire format wraps the envelope with either `sig` (hex-encoded Ed25519, 64 bytes) on the control channel or `mac` (hex-encoded HMAC-SHA256 tag, 32 bytes) on the event channel. The envelope schema and canonical-JSON encoder live in `packages/workflow-host/src/ipc/envelope.ts`.

### Replay Protection

Two complementary mechanisms run side by side:

- **Monotonic seq per channelId.** The receiver tracks the highest seq seen and requires the next frame's seq to equal `highestSeq + 1` exactly. A repeat or decrease is a replay; a gap is a drop. Either crashes the receiver.
- **ChannelId rotation on every spawn and every recycle.** The supervisor mints a fresh 16-byte channelId via `crypto.randomBytes` (hex-encoded) at every spawn and at every recycle (the `generateChannelId` helper in `packages/workflow-host/src/ipc/crypto.ts`). The receiver compares the incoming channelId against the current one byte for byte; any mismatch crashes the receiver. A frame from a recycled child's predecessor, or a frame captured from a previous channelId's lifetime, carries the wrong channelId and is rejected loudly rather than silently dropped.

The combination is what defends against frame replay across the spawn/recycle boundary that monotonic seq alone cannot defend against. The seq counter resets per channelId.

### Trust-Anchor Bootstrap

The spawn-time environment is the only channel the supervisor uses to hand trust anchors to the child. It carries exactly three values: `HOST_PUBKEY` (the supervisor's Ed25519 32-byte public key, hex), `IPC_HMAC_KEY` (the freshly minted 32-byte HMAC key, hex), and `IPC_CHANNEL_ID` (the freshly minted channelId, hex). The supervisor's Ed25519 private key never appears in env, never appears in any IPC payload, never appears in a log line, and never appears in an audit-log frame — it lives only as a 32-byte `Uint8Array` held in closure by the signing-key callback the supervisor wraps the control channel sender around. The env is constructed as a fresh object containing only those three variables, with no `...process.env` spread that could accidentally propagate a private value the supervisor's parent put in its own env.

### Failure Stance

The child verifies every control frame against `HOST_PUBKEY`; the supervisor verifies every event frame against `IPC_HMAC_KEY`. The contract on both sides is the same: any frame whose signature or MAC fails, whose channelId does not match the current one, or whose seq violates the monotonicity check is treated as a violation. The receiver calls a caller-supplied crash callback and ends the iterator. The supervisor's wiring promotes that crash to a process kill; the child does the same. Recycling brings the deployment back up cleanly with a fresh channelId.

The event channel's userspace buffer is bounded (default 1024 frames, exported as `DEFAULT_EVENT_BUFFER_LIMIT`). On overrun the supervisor logs the saturation and the workflow-process kills itself. The audit chain is built from forwarded `InferenceEvent`s; a silent drop of even one event would corrupt the chain in a way no downstream consumer could detect. Between corrupt-but-quiet and crash-and-be-noticed, crash wins.

### Supervisor Lifecycle And Bindings

The host-side supervisor implementation lives at
`packages/workflow-host/src/supervisor/`. The factory
`createWorkflowSupervisor(bindings)` takes a `WorkflowSupervisorBindings`
object whose shape is:

- `repoStore` — substrate-shaped `RepoStore` handle. The supervisor
  reads grants and commits events through this one handle; per-
  principal write-sites pass the principal kind explicitly rather
  than going through pre-minted "RepoStore views".
- `signAsPrincipal: (kind, payload) => SignedPayload` — host-owned
  per-principal signing callback. The supervisor never sees the
  principal's private key.
- `mailBus` — `MailBusBindings` (`registerAddress`,
  `unregisterAddress`, `subscribeMailForAddress`).
- `subprocessSpawner` — invoked once per spawn against the
  package-owned `packages/workflow-host/bin/workflow-child` script.
  Production wires it against `Bun.spawn`; tests inject a mock.
- Per-deployment configuration: `binaryPath`, `substrateEnv`,
  `workflowRunRepoId`, `workflowRunRef`, `deploymentId`,
  `deploymentMailAddress`, `readPrincipal`, `deriveStepAddress`.

The supervisor's `spawn(opts)` method:

1. Mints a fresh channelId, HMAC key, and IPC Ed25519 keypair.
2. Builds the spawn-time env containing `IPC_CHANNEL_ID`,
   `IPC_HMAC_KEY`, `HOST_PUBKEY`, `DEPLOYMENT_ID`,
   `DEFINITION_HASH`, `MAILBOX_ADDRESS`, plus substrate-config keys
   the host injected. Never the private key, never the principal-
   signing key.
3. Invokes `bindings.subprocessSpawner` and constructs the control-
   channel sender + event-channel receiver against the returned
   handle's stdio / event-socket fds.
4. Awaits the child's `ready` frame on the control channel.
5. Assembles the per-deployment `credentialsSnapshot` from each
   step's `agent-state` repo (see next subsection) and registers
   the deployment's mail address on the bus.
6. Forwards inbound mail as `trigger.fire` control frames; mail that
   arrives during `starting` buffers and drains in arrival order
   after `ready`.

Implementation lives in
`packages/workflow-host/src/supervisor/supervisor.ts`. The recycle
sequence is library code in
`packages/workflow-host/src/supervisor/recycle.ts`; see the Recycle
subsection below for the six-step contract and the three trigger
origins.

### Workflow-Process Child

The workflow-process child's runtime body is `runWorkflowChild`. It
lives in `packages/workflow-host/src/child/run-child.ts` and is the
single function each host's thin entry script invokes via
`runWorkflowChildFromProcessEnv` (the process-boundary helper in
the same package). The signature is:

```ts
runWorkflowChild(opts: {
  env: SpawnTimeEnv;             // parsed spawn-time env
  controlReader: NdjsonReader;   // supervisor -> child
  controlWriter: NdjsonWriter;   // child -> supervisor
  eventWriter: FrameWriter;      // child -> supervisor (InferenceEvents)
  bindings: RunWorkflowChildBindings;
}): Promise<RunWorkflowChildResult>
```

Every I/O stream and every substrate handle is injected. Nothing
inside the function reads `process.env` or reaches into a singleton;
the binary is the only component that crosses that boundary.
Integration tests bypass the binary and exercise `runWorkflowChild`
directly with mock streams and an in-memory substrate.

#### Lifecycle

1. **Open the IPC channels.** The control channel is the
   `receiveControlChannel` iterator the supervisor's `createControl
ChannelSender` writes against; the event channel is the
   `createEventChannelSender` the child publishes to.
2. **Construct `WorkflowRuntimeEnv`.** Each in-flight run gets a
   per-run `BlobSubstrate` and `SignalChannel`; the per-deployment
   `RepoStore` adapter and the host-process `Scheduler` singleton
   are shared. The `authorize` slot is the credentialsSnapshot-backed
   closure documented below.
3. **Self-discovery.** The child enumerates `runs/<runId>/` under
   the workflow-run repo's working tree, calls `resumeFromLog` for
   each run whose log lacks a terminal event
   (`RunCompleted`/`RunFailed`/`RunCancelled`), and invokes
   `runtimeRun(env, definition, { resumeFromEvents })` for each one
   it surfaces. The runtime body's resume path re-arms unfired
   timers, pending awaits, and uncancelled children from the seed log.
   Self-discovery runs before `ready` so a fresh `trigger.fired`
   cannot race ahead of a resume.
4. **`ready`.** The child emits a signed `ready` control frame back
   to the supervisor via the same `createControlChannelSender` shape
   the downstream uses, signed under the IPC keypair the supervisor
   minted at spawn time.
5. **Control-loop.** The child consumes the receiver iterator:
   - `trigger.fire` -> open a new run via `runtimeRun`.
   - `grants-updated` -> swap the active credentialsSnapshot.
   - `drain` -> forward to the drain controller (no-op placeholder
     in this commit; the real controller lands separately).
   - `recycle` -> log-only notification on the child side; the
     supervisor's recycle path drives the kill/respawn from outside
     the child, so the downstream `recycle` payload exists for
     symmetry with the protocol but is not the trigger.
   - `shutdown` -> stop accepting new triggers and exit the loop.

#### Trust Anchors

The spawn-time env (`SpawnTimeEnv`) carries:

- `IPC_CHANNEL_ID` -- supervisor-minted channelId per spawn.
- `IPC_HMAC_KEY` -- 32-byte shared HMAC key for the event channel.
- `HOST_PUBKEY` -- the supervisor's Ed25519 public key for verifying
  inbound control frames. **The supervisor's private key never
  appears in env, never appears in any IPC payload, never appears
  in a log line.** It lives only in the supervisor's address space.
- `DEPLOYMENT_ID`, `DEFINITION_HASH`, `MAILBOX_ADDRESS` --
  per-deployment identifiers the runtime body uses to attribute
  every committed event back to the owning deployment.

`parseSpawnTimeEnv` validates the env at the boundary and returns
the typed struct; missing keys, malformed hex, or off-size byte
payloads throw before the child opens IPC.

#### CredentialsSnapshot-Backed Authorize

The child's `WorkflowAuthorizeFn` closure captures a mutable
`CredentialsSnapshotRef`; every authorize call looks up the
originating step's grants by `stepId` and delegates to a
host-supplied `GrantEvaluator`. A `grants-updated` control frame
swaps the snapshot in place, so subsequent steps see the fresh
grants without reconstructing the runtime env. The initial
snapshot can arrive as a binding (multi-step deploys whose host
wires it at spawn time) or via the first `grants-updated` frame;
the closure raises if a step's authorize fires before any snapshot
has landed.

#### Channel-To-Loop Mapping

- **Control channel.** Supervisor -> child. Carries every
  `ControlPayload` variant (`trigger.fire`, `signal.deliver`,
  `drain`, `recycle`, `shutdown`, `grants-updated`,
  `sources-updated`) plus the child's upstream `ready` ride. Every
  inbound frame is verified against `HOST_PUBKEY`; the receiver
  iterator crashes the loop on any signature/channelId/seq
  violation.
- **Event channel.** Child -> supervisor. Carries `EventPayload`
  (InferenceEvents the harness emits). The child authenticates
  every outbound frame with HMAC under `IPC_HMAC_KEY`; the
  supervisor's `receiveEventChannel` verifies the same way.

#### Thin binary per host

`@intx/workflow-host` ships the runtime logic as a library; the
package itself does not ship a `bin` entry. Each host owns the
binary the supervisor's `subprocessSpawner` invokes, and the
binary is a thin entry script (~5 lines: a `bun` shebang, an
`import` of `runWorkflowChildFromProcessEnv`, an `import` of the
host's substrate factory, an `await` of the helper).

The substrate factory is the host-owned slot. It is a callback
typed `(env: SubstrateFactoryEnv) => Promise<RunWorkflowChildBindings>`
that closes over the host's concrete infrastructure: the
substrate-shaped `RepoStore`, the host-process scheduler
singleton (adapted via `adaptHostScheduler` to the runtime's
`Scheduler` shape), the production `invokeStep` and `spawnChild`
adapters, the host's grant evaluator, and the IPC upstream
signing key. `SubstrateFactoryEnv` is a narrow typed struct (the
parsed `SpawnTimeEnv` plus a `substrateConfig` record listing the
keys the host's binary called out); the factory never sees
`NodeJS.ProcessEnv` directly.

The supervisor's `binaryPath` binding resolves to the host's own
binary statically. In the sidecar's wiring
(`apps/sidecar/src/workflow-host-wiring.ts`) the resolution lives
in a wiring-module-load-time constant computed via
`import.meta.resolve("../bin/workflow-child")`; the same pattern
applies to any alternative-sidecar implementation.

The in-tree sidecar's substrate factory is
`apps/sidecar/src/workflow-substrate-factory.ts` and the binary
is `apps/sidecar/bin/workflow-child`. An alternative-sidecar
implementer ships its own factory + binary and points its
supervisor's `binaryPath` at the alternative binary.

#### Drain semantics

The runtime body observes drain at exactly four sites in
`packages/workflow/src/runtime/run.ts`. Each site reads
`shouldAbortForDrain(env.drain, stepId)` from
`packages/workflow/src/runtime/drain.ts` and aborts the step's local
abort controller when (a) the drain signal has aborted AND (b) the
step's declared `drainBehavior` is `"cancel"`. A step whose behavior
is `"wait"` ignores drain; only an explicit cancel can abort it.

The four observation points and why they are the four:

1. **Main loop entry.** Per iteration, the runtime checks every
   in-flight step. A drain that lands after a step was already
   scheduled (the step's combined abort signal was wired through
   `createStepAbort` at schedule time) is re-evaluated against the
   live `behaviorFor` on every tick. This is where a drain that
   fires while the workflow is paused on a `Promise.race` is first
   observed.
2. **Retry-between-attempts inside `runStep`.** A long-running
   workflow that hit `StepFailed` and committed `AttemptScheduled`
   is asleep inside `waitForTimer`. The site after `waitForTimer`
   returns and before the next attempt's `invokeStep` runs is the
   point where the runtime can short-circuit cleanly without
   re-entering a fresh agent invocation that the supervisor would
   then wait on to wind down.
3. **`waitForTimer`.** The event-sourced timer wait subscribes to
   the run's log tail. A drain that lands while the subscription
   is live aborts the local controller so the `for await` ends
   cleanly. The site is observed at entry and again as a
   `drain.signal` listener attached during the wait.
4. **`runAwaitSignal`.** The signal-channel await is the
   human-in-the-loop site. `awaitSignal` defaults to `"wait"` so
   the typical pause sits through drain untouched; an author who
   explicitly opts in via `drainBehavior: "cancel"` gets the
   short-circuit at entry and again on `drain.signal` mid-await.

The four sites cover every place the runtime body blocks long
enough for drain to matter. The state-machine primitives that do
not block (gate, escalation, the outer map) settle without ever
yielding to the event loop, so a drain observed at the next
main-loop tick takes effect before they could matter.

`"wait"` vs `"cancel"` is declared per primitive. The constructors
in `packages/workflow/src/definition/primitives.ts` set the
defaults: `step`, `sleep`, and `childWorkflow` default to
`"cancel"` (long compute should not block a redeploy past the
drainTimeout); `awaitSignal` defaults to `"wait"` (human-in-the-
loop pauses are the canonical case the spec calls out, and
operators do not want them silently cancelled at redeploy). `map`'s
outer node carries no `drainBehavior` -- its inner step carries
its own. `gate` and `escalation` are non-blocking and resolve as
`"cancel"` for short-circuit purposes only.

The supervisor's drainTimeout accumulator lives in
`packages/workflow-host/src/supervisor/drain-timeout.ts`. Default
`drainTimeout` is 60 seconds (`DEFAULT_DRAIN_TIMEOUT_MS`),
operator-overridable per deployment. The accumulator ticks against
wall-clock time only while the supervisor believes at least one
`"cancel"`-behavior step is in flight; it pauses when every live
step transitions to `"wait"` behavior and resumes when cancel-mode
work reappears. Time spent paused does not consume the deadline.

On `drainTimeout` expiry the accumulator invokes the supervisor's
injected `signAsPrincipal("supervisor", ...)` callback to mint a
signature over a canonical `CancelRequested{origin:
"supervisor-drain"}` event and commits it through the supervisor's
substrate handle. The runtime body observes the commit through its
existing cancel-cascade path; no special drain-aware cancel branch
is needed in `run.ts`.

The canonical observable sequence at the in-process layer (asserted
in this package's tests via `subscribeKind` against a fake
`RepoStore`, and at the end-to-end layer by the I3 integration
test) is:

1. `drain` issued (host command, not a workflow-run event).
2. Steps in `"wait"` mode commit nothing during the drain window.
3. Steps in `"cancel"` mode: `drainTimeout` accumulates; on
   expiry, `CancelRequested{origin: "supervisor-drain"}` appended
   to the workflow-run log.
4. `CancelPropagated` appended for each non-terminal step
   (runtime-body cascade).
5. `RunFailed{reason: "cancelled"}` / `RunCancelled` appended
   (terminal).
6. Workflow-process exit observed by the supervisor.

Child-workflow drain coordination splits two ways:

- **Same-deployment children** run inside the same workflow-process
  as the parent. They share the parent's `DrainController` instance
  through the runtime env; no extra mechanism is required. The
  parent's main loop iteration over its own in-flight steps
  includes the spawn step, and the existing
  `ChildCancelRequested` emission path in `runtime/run.ts` covers
  child propagation.
- **Cross-deployment children** run under a different deployment
  with its own supervisor and its own drain accumulator. The
  parent's `ChildCancelRequested` event lands on the parent run's
  log; the cross-deployment child's supervisor handles the
  resulting cancel mail as `supervisor-operator` origin against
  its own runtime, which then drains through its own four
  observation points.

#### Recycle

Recycle is the supervisor's "same deploy tree, fresh process" path,
implemented as library code in
`packages/workflow-host/src/supervisor/recycle.ts` and consumed by
the supervisor object's `recycle()` method.

**Strict orthogonality with redeploy.** Recycle uses the SAME deploy
tree -- the same `workflow.json`, the same workflow-asset repo, the
same per-step `agent-state` repos. Redeploy is the other shape: a
new deploy tree, a new workflow definition, possibly different
agents. The recycle module never refetches the deploy tree, never
consults an updated workflow definition, never re-resolves agents.
The module header documents the invariant loudly: if a deploy-tree
change is needed the host runs redeploy, which is a different code
path with different authorization and a different rollback shape.
The recycle module must not grow a "maybe also refetch the deploy
tree" mode -- that would erase the orthogonality and let a recycle
silently turn into a redeploy.

**Six-step sequence (locked):**

1. **`drain`** -- the supervisor sends the existing `drain` control
   mail (from the drain-controller commit). The child's
   `DrainController` flips its signal; the runtime body's four
   observation points pick up the change on their next tick.
   `drainTimeout` escalation applies normally -- the supervisor's
   drain-timeout accumulator runs orthogonally and commits a signed
   `CancelRequested{origin: "supervisor-drain"}` if cancel-mode work
   outlasts the deadline. The recycle path does NOT reimplement
   drain; it consumes the supervisor's existing `drain()` primitive.
2. **`kill`** -- the supervisor terminates the workflow-process child
   via the injected subprocess spawner's `SubprocessHandle.kill`
   surface. SIGTERM first; if the child does not exit within
   `DEFAULT_KILL_TIMEOUT_MS` (operator-overridable), the recycle
   path escalates to SIGKILL. The injected spawner owns the Node
   primitives; the recycle path never reaches into them directly.
3. **`respawn`** -- the supervisor mints a fresh 16-byte hex
   channelId via the same `generateChannelId` helper the initial
   spawn uses, generates a new 32-byte HMAC key, mints a fresh
   Ed25519 IPC keypair, re-reads per-step credentials via the
   injected `RepoStore`, and spawns a new Bun child via the same
   subprocess-spawner binding. The new IPC anchors flow through
   spawn-time env exactly as the initial spawn's anchors did.
4. **`self-discover`** -- the new child runs its existing
   self-discovery on spawn (the same code path the initial-spawn
   case uses). The recycle path does not coordinate this step.
5. **`resume`** -- self-discovery resumes any in-flight runs from
   the workflow-run log. The runtime body's seed-events path
   re-arms timers, pending awaits, and uncancelled children.
6. **Mail buffer drain** -- the supervisor drains its buffered mail
   into the new child over the control IPC. Inbound mail that
   arrived during the kill/respawn gap was buffered in the
   supervisor's queue; this step replays it in arrival order as
   `trigger.fire` control frames. Subsequent inbound mail bypasses
   the buffer and forwards immediately.

**Mail address ownership across the gap.** The supervisor holds the
mail-bus registration across the recycle via the injected mail-bus
binding. No re-register, no unregister, no subscription cycling.
Inbound mail during the kill/respawn gap buffers in the supervisor's
bounded queue (`MAX_BUFFERED_MAIL`, 256 messages). On saturation the
recycle path surfaces a loud error rather than silently dropping
inbound mail -- a saturated buffer indicates either an upstream
stuck on the deployment or a recycle stuck partway through, and
either case is one the operator must see. Once the new child emits
`ready`, the supervisor drains the buffer over control IPC.

**ChannelId rotation per the IPC contract.** Every respawn mints a
new channelId via `generateChannelId` (16 bytes from
`crypto.randomBytes`, hex-encoded). The new channel's receiver
crashes on any frame carrying the predecessor channelId -- a stale
frame is by construction either a replay or a programming bug, and
crashing on mismatch is the only honest response. The seq counter
resets per channelId because the IPC contract pins seq monotonicity
to the channelId's lifetime; the rotation is what defends against
frame replay across the recycle boundary that monotonic seq alone
cannot defend against.

**Three trigger origins funnel through `triggerRecycle(reason)`:**

- **Operator command.** The host receives a `recycle` request via
  its caller-facing API and routes it to the supervisor's
  `recycle()` method, which delegates here with origin `operator`.
- **Supervisor policy.** A periodic check inside the supervisor
  (`createRecyclePolicy`) wakes on the configured interval
  (default `DEFAULT_POLICY_INTERVAL_MS`, ~1 minute) and evaluates
  the configured bounds: `maxUptimeMs`, `maxRssBytes`,
  `maxGrantsAgeMs`. Defaults are unlimited (the bound is disabled
  unless the operator sets a threshold). On a threshold trip the
  policy calls `triggerRecycle` with origin `policy` and a reason
  naming the tripped bound.
- **Workflow-process self-initiated.** The child sends a
  `recycle.request` payload over control IPC when its own self-
  check decides it needs to be recycled (an internal consistency
  error it can't recover from, a watchdog tripping). The
  supervisor's upstream control-channel reader recognises the
  variant and funnels into `triggerRecycle` with origin `self`. The
  IPC payload union extension is the only contract change recycle
  introduces; the envelope, signature, and channelId/seq rules are
  unchanged.

All three origins land in the same code path. The reason string is
the only origin-specific data the path carries forward.

#### Placeholders And Future Work

The signal-channel seam is constructed per run with an empty
`RunState` reader -- a pre-await signal still resolves through the
live `subscribeKind` tail, but a resume-rehydrated queued signal
needs the runtime body's `RunState` reader plumbing, which lands
when the per-run state-machine reader is wired into the child.

### Deploy Routing (Option Z)

The supervisor is the single ingress for inbound `agent.deploy`
frames. The host-side handler calls `supervisor.deploy(frame)` and
the supervisor decides between the trivial (1-step) passthrough
and the multi-step IPC-backed spawn. Routing lives on the
supervisor side of the seam; the host does not re-decide. The
sidecar's hub-link surface lives at
`packages/hub-agent/src/ws/hub-link.ts`; it consumes a
`DeployRouter` binding whose production implementation is the
workflow-host supervisor.

Four invariants on the routing model are locked:

1. **Supervisor owns `agent.deploy` framing.** The hub-side handler
   in `packages/hub-agent/src/ws/hub-link.ts` calls
   `deployRouter.deploy(frame)`; the production router is the
   workflow-host supervisor's `supervisor.deploy(frame)`. No routing
   logic leaks back into `hub-agent` or `hub-sessions`.
2. **Trivial branch is a process-topology passthrough.** The
   supervisor invokes the host-injected `trivialLaunch` callback
   directly. No IPC channel opens. No workflow-process child is
   spawned. No mail-bus subscription registers. The bytes flowing
   through the deploy-flow gate path are bit-identical to the
   pre-supervisor surface.
3. **Trivial deploys emit the canonical workflow-run event chain.**
   The supervisor commits `RunStarted` / `StepStarted` /
   `StepCompleted` / `RunCompleted` to the workflow-run repo from
   the supervisor process itself, signed via the host's
   `signAsPrincipal("supervisor", ...)` callback. The chain fires
   per inbound mail trigger (one run per fire) and is driven by
   the host calling `bindings.recordRunEvent(...)` from the
   trivialLaunch callback's reactor / harness lifecycle moments
   (`message.run.started` / `message.run.ended` brackets). The
   on-disk envelope is byte-identical to the one a multi-step
   deployment's workflow-process child produces. Trivial and
   multi-step deployments differ in process topology, not in
   observability. The supervisor's signing key never leaves the
   supervisor's address space; `signAsPrincipal` returns the raw
   signature bytes for the canonical payload only after the
   supervisor has serialized them.
4. **`credentialsSnapshot` is multi-step-only.** The trivial branch
   does not assemble a snapshot; `getCredentialsSnapshot()`
   continues to return `null` after a trivial deploy. The
   multi-step branch (`steps.length >= 2`) provisions per-step
   `agent-state` repos, mints keys, spawns the workflow-process
   child via `subprocessSpawner`, registers the deployment's mail
   address, waits for `ready`, and assembles the
   `credentialsSnapshot` -- this is the body of `spawn(opts)`,
   which the multi-step branch is the worker for. The
   `agent.deploy` wire frame today carries only a `HarnessConfig`
   (no workflow definition); every frame is therefore trivial. The
   seam exists now so the frame-format extension that carries a
   `WorkflowDefinition` lands as a pure data-shape change.

The supervisor's per-event commit primitive lives in
`packages/workflow-host/src/supervisor/run-event-signing.ts`
alongside the analogous `commitCancelRequested` path. The trivial
branch's `recordRunEvent` is a thin closure over that primitive that
the supervisor hands into the trivialLaunch bindings; the multi-step
branch composes the same primitive from inside its in-supervisor
event-channel receiver.

The workflow-run repo's substrate `repoId.id` is constrained to
`/^[a-zA-Z0-9_-]+$/` (`SAFE_REPO_ID` in
`packages/hub-sessions/src/repo-store/types.ts`), which the
agent-address shape (`ins_<id>@<domain>`) does not satisfy. The
sidecar wiring derives the trivial branch's deployment id by
substituting disallowed characters with `-`; see
`deriveDeploymentId` in
`apps/sidecar/src/workflow-host-wiring.ts`. The supervisor
principal's `deploymentId` and the workflow-run `repoId.id` are kept
equal so the workflow-run kind handler's principal-vs-repo authz
check holds for every supervisor-authored event commit.

The sidecar production wiring lives in
`apps/sidecar/src/workflow-host-wiring.ts`:
`createSidecarDeployRouter` constructs a fresh per-deployment
supervisor on every inbound frame whose `trivialLaunch` closes
over `SessionManager.provisionAgent` plus the hub-pairing-key
recording the legacy handler performed inline. The `HubTransport`
mail-bus adapter the supervisor consumes lives in the
`@intx/workflow-host` package proper
(`packages/workflow-host/src/mail-bus/`) so an alternative-sidecar
implementation can reuse it without forking the wiring.

### CredentialsSnapshot Assembly

Per the Q6.4 discovery decision, each workflow step gets its own
`agent-state` repo, and the supervisor enumerates the workflow
definition's `stepOrder` at spawn time to assemble a per-step grant
snapshot. The implementation lives in
`packages/workflow-host/src/supervisor/credentials.ts`:

- `defaultStepRepoId({ deploymentId, stepId })` returns the
  `agent-state` repo identity for the step. The default convention
  is `<deploymentId>-<stepId>`.
- The grants file for a step rides at `state/grants.json` under the
  step's repo working tree (the substrate's `getRepoDir` is a pure
  path computation, mirroring the sibling production adapters'
  working-tree-read pattern).
- `assembleCredentialsSnapshot` reads each step's grants, hashes
  them with sha256 over the canonical JSON serialization, and
  returns a `CredentialsSnapshot` with per-step `address`, `grants`,
  and `contentHash`. The hash is stable across processes so the
  child can detect a stale snapshot pushed after a fresher one.
- A missing grants file is treated as an empty grant array (so the
  trivial path with no operator-supplied grants does not crash); a
  malformed file fails loudly at the boundary.

### Signed CancelRequested Authority

Per interface decisions Q3, every CancelRequested origin flows
through the same supervisor-signed path; the workflow-process has
no asymmetric keypair of its own. The implementation lives in
`packages/workflow-host/src/supervisor/cancel-signing.ts`:

- The supervisor builds the event payload from the runId, origin,
  reason, and ISO-8601 timestamp.
- The canonical JSON bytes (without the `signature` field) are
  passed to `bindings.signAsPrincipal("supervisor", payload)`,
  which returns a `SignedPayload` carrying the raw 64-byte
  Ed25519 signature plus the principal kind.
- The supervisor attaches the signature to the on-disk blob and
  commits via `RepoStore.writeTreePreservingPrefix` under a
  `WorkflowRunSupervisorPrincipal`. The workflow-run kind handler
  enforces the Q3 principal-vs-origin map at push validation;
  routing every supervisor-signed origin through the same
  principal kind keeps the trust anchor inventory at one signing
  key per deployment.

The `self`-origin case is the workflow-process forwarding its
stated reason to the supervisor over the control IPC; the
supervisor wraps it into a signed event without consulting the
child for the signature.

### Host Wiring (Sidecar Reference Implementation)

The in-tree sidecar's wiring lives at
`apps/sidecar/src/workflow-host-wiring.ts`. The module is
intentionally thin -- it composes the sidecar's existing
`HubTransport`, signing keypair, and substrate `RepoStore` handle
into the bindings shape the supervisor expects, and exposes
`createSidecarWorkflowSupervisor(opts)` for the deploy handler to
invoke per workflow deployment. Anything that would benefit a
future alternative-sidecar implementer belongs inside the
`@intx/workflow-host` package, not in the wiring.

### How This Differs From The Hub-Sidecar WebSocket Boundary

The session-channels transport documented above runs between two trusted services across a network. Mutual TLS plus per-sidecar tokens plus challenge/response cover the authentication problem; framing is JSON over WebSocket; reconnection handles transport flakiness with sequence numbers carried on the resume; lifecycle frames travel alongside event frames on the same wire.

The supervisor/child IPC runs between two processes on the same host where one of them is the trust anchor and the other is not. The threat model is "compromised user code in the child," not "lossy network in the middle." That changes three things in shape:

- **Per-frame crypto, not per-session.** A long-lived authenticated session is not enough when the untrusted side could fabricate a single frame at any moment. Every frame carries its own authentication tag.
- **Two separate transports, not one multiplexed wire.** Control and event differ in cost (asymmetric vs symmetric), in rate (low vs high), and in authority (one-way supervisor-to-child vs bidirectional). Multiplexing them onto a single wire would force the worst-case discipline of either, with no upside.
- **Crash-on-violation, not retry-on-violation.** The WebSocket transport reconnects on transport faults. The IPC channels do not — a violation is treated as either a compromise or a programming bug, both of which the supervisor handles by killing the child and respawning with a fresh channelId. There is no "try again on the same wire" path.

## Authorization: OpenAPI-Based Capability Scoping

When agents are granted access to external APIs, the full API surface often exceeds what the agent needs. Interchange uses OpenAPI specifications to define and enforce fine-grained authorization subsets.

### Subset Definitions

An operator provides an OpenAPI spec (or a reference to one) alongside a subset policy that restricts which operations the agent may invoke. The subset policy specifies:

- Allowed paths and HTTP methods
- Parameter constraints (e.g., only certain query parameter values)
- Required headers or authentication scopes

The harness loads the OpenAPI spec, applies the subset policy, and generates a filtered view of the API that the agent can discover and invoke through the standard tool interface.

### Dynamic Skill and Tool Generation

From the filtered OpenAPI spec, the harness dynamically generates tools that the agent can invoke. Each permitted endpoint becomes a tool with typed parameters derived from the spec's schema definitions. The agent sees only the operations it is authorized to use, and the harness validates every invocation against the subset policy before forwarding the request to the external API.

This approach allows operators to grant access to complex APIs without writing custom tool definitions. The OpenAPI spec serves as both documentation and enforcement boundary.

### Credential Binding

Subset policies are paired with the delegated credentials for the API. The harness injects authentication (OAuth tokens, API keys) into outbound requests transparently. The agent never sees raw credentials; it invokes tools and the harness handles authentication.

## Wallets: Payment Tools and Plugins

Wallets provide agents with the ability to send and receive payments. The harness exposes wallet functionality as tools, with a plugin architecture for payment backends.

### Payment Tools

The harness provides standard payment tools that agents invoke like any other capability:

- **`wallet.pay`** — Send payment to a recipient
- **`wallet.request_payment`** — Request payment from a caller, blocking until received or timeout
- **`wallet.check_balance`** — Query available balance by currency
- **`wallet.verify_payment`** — Verify that a payment was received
- **`wallet.list_transactions`** — Query recent transaction history

These tools abstract over payment protocols and backends. The agent specifies what to pay (amount, currency, recipient); the harness handles how (signing, broadcasting, proof generation).

### Payment Framework

The harness defines a payment integration interface behind the payment tools. This interface is what connects the agent-facing tools (`wallet.pay`, etc.) to actual payment implementations. Multiple implementations can be active simultaneously — the harness selects the appropriate one based on the payment requirements of a given transaction.

### Faremeter

[Faremeter](https://github.com/faremeter/faremeter) is the primary payment framework. It provides a pluggable, standards-agnostic payment system that supports a superset of emerging payment protocols including x402, L402, and Cloudflare's Pay-Per-Crawl.

**Agent as consumer (outbound payments):**
The harness uses Faremeter's client library (`@faremeter/fetch`) to handle outbound payments. When an agent invokes a remote tool that requires payment, the harness uses Faremeter to negotiate and execute the payment using the appropriate protocol and wallet backend. Faremeter's plugin system handles scheme and network selection — the harness connects the agent's configured wallet plugins and Faremeter handles the rest.

**Agent as provider (inbound payments):**
For agents that charge for their services, the harness uses Faremeter's middleware to handle payment collection. The middleware intercepts inbound requests, enforces pricing, and verifies payment before allowing the agent to process the request. This separates payment handling from agent logic — the agent focuses on its work while Faremeter handles the payment mechanics.

Faremeter's plugin architecture is wallet and blockchain agnostic. Tenants configure which payment plugins are available (Solana, EVM chains, fiat processors) and bind them to wallets. New payment standards can be supported by adding plugins without changes to the harness or agent.

### Third-Party Integrations

The payment integration interface is open. Payment implementations outside of Faremeter's ecosystem can plug into the harness's wallet layer, allowing tenants to connect proprietary payment systems, custom billing platforms, or emerging protocols that haven't yet been adopted by Faremeter. Third-party integrations implement the same interface and are available to agents through the same payment tools.

### Policy Enforcement

The harness enforces spending policy before executing any payment:

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

Messages between agents in the same tenant (same domain) are routed internally without traversing external SMTP infrastructure. The harness can optimize tenant-local delivery for lower latency and higher throughput.

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

The harness instruments key operations:

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

An agent package is a git tree assembled from the agent's definition repository with resolved skill dependencies (see Agent Versioning above). It contains:

- Skills and their dependencies
- System prompt
- Context builder configuration
- Initial state
- Capability grants

Each deploy commit represents an immutable version. The control plane tracks which deploy refs are active on which sidecars, and retains historical deploy tags for rollback.

### Health Protocol

The harness exposes health endpoints:

- **Liveness** - Returns OK if the harness process is running and responsive
- **Readiness** - Returns OK if the agent is ready to accept work (connections established, initialization complete)

Health checks are polled by the control plane. Agents that fail liveness are restarted. Agents that fail readiness are removed from discovery until they recover.

Health can also be reported via periodic heartbeat messages to the control plane. Missed heartbeats trigger the same failure handling.

### Deployment Procedure

1. **Provision** - Hub sends `agent.deploy` with ephemeral config; sidecar creates directory, keys, and returns the public key
2. **Assembly** - Hub resolves skill dependencies and assembles the deploy tree
3. **Pack transfer** - Hub streams the packfile to the sidecar (see Agent Deployment)
4. **Session start** - Hub sends `session.start`; sidecar reads deploy tree and starts the harness
5. **Health gate** - New version must pass health checks before receiving traffic
6. **Traffic shift** - Registry updates discovery to point to new version
7. **Drain** - Old version stops accepting new work, completes in-flight operations
8. **Retirement** - Old version shuts down; deploy tag remains for rollback

### Rollback

If a new version fails health checks or exhibits problems in production:

1. Operator or agent triggers rollback via control plane
2. Traffic shifts back to previous version
3. Failed version is marked unhealthy and stopped

Rollback is fast because the previous version's package is still available and can be restarted immediately.

## Agent Versioning: Git-Backed Definitions

Agent definitions are stored in git repositories on the hub, providing version control for the resources that constitute an agent.

### Repository Contents

The repository contains agent-specific resources:

- Skills and skill configuration
- System prompt (`prompt.md`)
- Context builder configuration
- Initial state
- Capability grants
- Dependency references (resolved at deployment)

External skill libraries are not stored directly. Configuration files reference them, and the hub resolves these references when assembling the deploy tree.

### Source Composition

At deploy time, the hub assembles a flat tree from multiple sources via subtree merge:

- The agent's own repository provides the prompt, configuration, and agent-specific skills
- Referenced skill library repositories are merged under `deploy/skills/<name>/` prefixes

The result is a single commit containing everything the harness needs. The sidecar receives this flat tree with no submodule metadata or external references to resolve. Provenance (which source repos and refs contributed) is recorded in the assembly commit on the hub side.

How the hub discovers and resolves skill dependencies (registry format, version resolution, authentication to source repos) is not yet specified.

### Skill Structure

A skill is a directory within the deploy tree:

```
deploy/skills/<name>/
  tool.json               (required: ToolDefinition schema)
  handler.ts              (optional: executable implementation)
  ...                     (additional assets as needed)
```

`tool.json` declares the tool's name, description, and input schema. The harness reads all `deploy/skills/*/tool.json` files at startup to build the tool list for inference calls.

A skill with `tool.json` but no `handler.ts` is backed by the harness's built-in tool runner (e.g., `read_file`, `write_file`, `run_shell`). The tool name in `tool.json` maps to the internal implementation.

Skills with `handler.ts` contain executable code loaded by the harness. The degree of sandboxing depends on the execution environment (see Architecture, Environment Integration). At minimum, skill handlers run in a separate isolate/process from the harness itself.

### Repository Organization

Repository structure is flexible:

- **Tenant repository** - A single repository contains definitions for all agents within a tenant, organized by directory
- **Agent repository** - Each agent has a dedicated repository

Both approaches support standard git workflows for managing agent versions.

## Agent Deployment: Git Pack Transport

Deploy content travels from the hub to sidecars as git packfiles streamed over the existing WebSocket connection. State travels back from sidecars to the hub using the same mechanism. This avoids requiring sidecars to have network access to external git hosts and reuses the authenticated, encrypted channel already in place.

### Wire Frames

The following frames are additions to the hub-sidecar protocol:

| Frame          | Direction      | Purpose                                             |
| -------------- | -------------- | --------------------------------------------------- |
| `pack.push`    | Either         | Chunked packfile data with sequence number          |
| `pack.done`    | Either         | End of transfer; carries target refs and commit SHA |
| `pack.ack`     | Receiver       | Refs accepted                                       |
| `pack.reject`  | Receiver       | Transfer rejected (with reason code)                |
| `sync.request` | Hub to sidecar | Request state push for a specific agent             |

Each pack transfer is scoped to an `agentAddress` and carries a `transferId` for correlation. Multiple transfers for different agents can be in flight concurrently.

Additionally, `ReconnectFrame` gains an optional `deployRefs` field: a mapping of agent addresses to their current deploy commit SHA. This allows the hub to determine whether a pack transfer is needed on reconnect.

### Encoding and Flow Control

Packfile chunks are base64-encoded within JSON frames, consistent with how mail bytes are encoded in the protocol. Each `pack.push` frame carries a bounded chunk (64 KiB before encoding) to avoid blocking the frame parser.

Pack transfers share the WebSocket with live session traffic. To prevent interference:

- Pack chunks are interleaved with other frames at the WebSocket message level (messages are atomic; the receiver processes each independently)
- The sender limits unacknowledged pack data to a configurable window before pausing
- Session-critical frames (`mail.inbound`, `message.send`, `session.abort`) are never delayed by pack transfers
- The receiver buffers incoming chunks in memory (or a temp file for large transfers) and only unpacks atomically on `pack.done`

### Rejection Reasons

`pack.reject` carries a `reason` field distinguishing failure modes:

| Reason              | Meaning                                          | Sender action                  |
| ------------------- | ------------------------------------------------ | ------------------------------ |
| `signature_invalid` | Commit signature verification failed             | Do not retry with same content |
| `path_violation`    | Commit modifies paths outside sender's ownership | Do not retry                   |
| `backpressure`      | Receiver is overwhelmed                          | Pause and retry after a delay  |
| `conflict`          | Ref update conflicts with receiver state         | Reconcile before retrying      |
| `corrupt`           | Packfile failed index verification               | Resend from scratch            |

### Deploy Flow

Deployment is a three-phase operation: provision, pack delivery, session start.

**Phase 1: Provision**

1. Hub sends `agent.deploy` with ephemeral config (credentials, materialized grants, providers, session ID)
2. Sidecar creates the agent directory, generates an Ed25519 key pair, and persists the config
3. Sidecar responds with `agent.deploy.ack` containing the agent's public key (hex-encoded)
4. Hub stores the public key for future challenge/response verification

The sidecar is now provisioned but not running. It can receive pack data.

**Phase 2: Pack delivery**

5. Hub assembles the deploy tree and produces a packfile
6. Hub sends `pack.push` frames (chunked, interleaved with other traffic)
7. Hub sends `pack.done` with target ref (`refs/heads/deploy`) and commit SHA
8. Sidecar validates the packfile integrity
9. Sidecar verifies the deploy commit signature against the hub's public key
10. Sidecar unpacks objects into the git object store
11. Sidecar updates `refs/heads/deploy` to the new commit
12. Sidecar merges `deploy` into its agent branch (trivial merge)
13. Sidecar checks out only `deploy/` paths via partial checkout (`filepaths: ["deploy/"], noUpdateHead: true`) — `state/` working-tree files are untouched because `filepaths` restricts the scope, `noUpdateHead` prevents moving the branch pointer
14. Sidecar responds with `pack.ack`

**Phase 3: Session start**

15. Hub sends `session.start`
16. Sidecar reads the deploy tree from disk (prompt, skills/tools)
17. Sidecar creates the inference harness with the deploy tree and ephemeral config
18. Sidecar starts the harness and responds with `session.start.ack`

The agent is now running and can receive messages.

On first deploy a full packfile is sent. On subsequent deploys, if the sidecar advertises its current deploy ref in `ReconnectFrame.deployRefs`, the hub can send a thin pack containing only the delta.

### Undeploy Flow

1. Hub sends `agent.undeploy` with a reason string
2. Sidecar stops the inference harness (or removes the agent from the provisioned set if session has not started)
3. Sidecar pushes state to the hub via `pack.push`/`pack.done` (best-effort — the `statePushed` field in the ack indicates whether this was attempted, not whether the hub confirmed receipt)
4. Sidecar deletes the agent directory
5. Sidecar responds with `agent.undeploy.ack`
6. Hub removes the agent from the routing table

If the sidecar disconnects before sending the ack, the hub removes the agent from the routing table on disconnect. If `startSession` fails, the agent remains provisioned and can be retried or undeployed.

### State Push Flow

1. Sidecar commits context/audit under `state/` on its agent branch (every commit is signed with the agent's Ed25519 key using SSH signature format)
2. On policy trigger (see State Push Policy):
   - Sidecar produces a packfile of new commits since last successful push
   - Sidecar sends `pack.push` / `pack.done` with its agent ref
3. Hub verifies commit signatures against the stored public key for that agent
4. Hub verifies path ownership (no commits modify `deploy/` paths)
5. Hub responds with `pack.ack` or `pack.reject`

### Partial Transfer Recovery

If the WebSocket disconnects mid-transfer, no git state is corrupted: the sidecar buffers chunks in memory and only unpacks on `pack.done`. On reconnect, the sidecar advertises its current deploy ref. The hub detects it is behind and initiates a fresh transfer.

### Reconnect Sequencing

Pack transfers are sequenced after identity verification:

1. Sidecar sends `reconnect` with `agentAddresses` and `deployRefs`
2. Hub sends `challenge` per address
3. Sidecar sends `challenge.response` per address
4. Hub verifies signatures — only verified agents proceed
5. Hub sends `grants.update` per verified agent (sidecar must have current grants before processing messages)
6. For agents whose deploy ref is behind: hub initiates pack transfer, waits for `pack.ack`
7. Hub sends `session.start` per agent
8. After `session.start.ack`: hub flushes queued messages

Pack content is never sent to a sidecar that has not proved ownership of the agent's key.

On first deploy (no prior key exists), the sidecar is authenticated by its registration token but cannot prove agent key ownership (the key does not exist yet). The hub sends `agent.deploy` to provision the agent, and the sidecar generates the key and returns it in `agent.deploy.ack`. The registration token and the authenticated WebSocket channel bound the trust for first-deploy; challenge/response protects all subsequent interactions.

### State Push Policy

The sidecar pushes state to the hub based on configurable policy:

| Trigger                                          | Use Case                                 |
| ------------------------------------------------ | ---------------------------------------- |
| Debounced timer (e.g., 10-30s after last commit) | Limits data loss window on process crash |
| Session end / graceful shutdown                  | Clean handoff before instance stops      |
| Hub-initiated `sync.request`                     | On-demand observability, pre-migration   |
| Per-audit-commit (no debounce)                   | Strict compliance requirements           |

The push policy is a per-agent or per-tenant configuration concern, not a protocol-level one.

**Data loss characteristics:** The debounce timer bounds data loss for process crashes (sidecar dies but disk survives — recoverable on restart). It does not bound data loss for disk loss (ephemeral container eviction). Deployments with ephemeral storage should use aggressive push policies. Deployments with durable local storage can rely on the sidecar pushing on restart.

### SSH Commit Signatures

State commits are signed using SSH signature format (compatible with `git verify-commit` since git 2.34). The signature is embedded in the commit object's `gpgsig` header via isomorphic-git's `onSign` callback.

The signature structure:

```
-----BEGIN SSH SIGNATURE-----
<base64 of SSHSIG binary:
  "SSHSIG" magic (6 bytes)
  uint32 version = 1
  string publickey (SSH wire: "ssh-ed25519" + 32-byte key)
  string namespace = "git"
  string reserved = ""
  string hash_algorithm = "sha512"
  string signature (Ed25519 sig over prescribed hash)>
-----END SSH SIGNATURE-----
```

State commits carry an author identity of `<agentAddress> <agent-id@interchange.local>` rather than a shared harness identity, making commits attributable in standard git tooling.

### Deploy Content Integrity

The deploy tree can contain executable code (`handler.ts` files in skills). This makes it the primary attack surface in the deployment path. Deploy commit signing is mandatory:

- Deploy commits are signed by the hub's Ed25519 key (or an operator-delegated key)
- The sidecar verifies the signature before unpacking
- Unsigned or incorrectly signed deploy content is rejected

The hub's signing public key is provisioned to the sidecar at registration time. A compromised hub can still push malicious code (it holds the signing key) — that threat is bounded by hub security and access controls, not by the wire protocol.

### Credential Isolation

The deploy tree never contains credentials. API keys, OAuth tokens, and other secrets travel in the `sources` field of the wire-format `HarnessConfig` from `@intx/types/runtime` on the `agent.deploy` frame, which is ephemeral (not persisted to git). The `keys/` directory is excluded from git via `.gitignore`.

## Change History: Git-Backed Storage

Agent-local data is stored in the same git repository as the deployed definition, providing revision control as a native capability. State lives under `state/` paths on a per-agent branch, while the deployed definition lives under `deploy/` (see Agent Deployment below).

### Storage Backends

The git repository can be backed by different storage implementations depending on the execution environment:

- **Filesystem** - Native git on disk for Docker containers, VMs, local processes, and mobile/embedded devices with filesystem access. Full git functionality with standard tooling.
- **In-memory with remote sync** - For JavaScript runtimes (Cloudflare Workers, web workers) and mobile/embedded environments that lack persistent filesystem access. Uses isomorphic-git or similar pure-JS implementations with object storage (R2, S3) as the backing store. Repository state is loaded on initialization and synced on commits.
- **Virtual filesystem** - For environments with partial filesystem semantics. Adapts git operations to the available storage primitives.

The harness abstracts these differences - agents interact with the same history tools regardless of the underlying backend.

### Repository Structure

Each agent has a single git repository:

```
<agent-dir>/
  .git/
  deploy/               # Hub-managed (read-only on sidecar)
    prompt.md
    config.json
    skills/
      <name>/
        tool.json
        handler.ts
  state/                # Sidecar-managed (per-agent journal)
    context.json
    audit/
      <session-id>/
        <call-id>.json
  keys/                 # Not tracked in git (.gitignore)
    id_ed25519
    id_ed25519.pub
```

The `deploy/` subtree is checked out from the deploy ref. The `state/` subtree is the agent's working data. The `keys/` directory holds the agent's Ed25519 keypair and is excluded from git via `.gitignore`.

### Ref Structure

```
refs/
  heads/
    deploy                  (latest deploy commit, hub-managed)
  tags/
    deploy/v1               (historical deploy versions)
    deploy/v2
  agents/
    <agent-id>              (per-agent state branch)
```

The agent branch forks from the deploy ref at launch time. On redeploy, the new deploy commit is merged into the agent branch (always conflict-free due to path disjointness).

### Commit Policy

The harness uses a hybrid commit strategy:

- **Auto-commits** - The harness commits automatically only on lifecycle boundaries:
  - On agent suspension or shutdown (preserving state before the agent goes offline)
  - On context window compaction (preserving state before truncation loses information)

  Auto-commits use generated messages describing the triggering event.

- **Agent checkpoints** - Agents explicitly create checkpoints when they want to mark meaningful points in their work. The agent controls the commit message, making history readable and intentional. This is the primary mechanism for building useful change history.

The hybrid approach ensures no work is lost across session boundaries while keeping the agent in control of what constitutes a meaningful checkpoint during active operation.

### Branch Management

Agents create and switch branches through harness-provided tools:

- `branch create <name>` - Create a new branch from current state
- `branch switch <name>` - Switch to an existing branch
- `branch merge <name>` - Merge another branch into current
- `branch delete <name>` - Remove a branch

The harness tracks branch metadata (creation time, purpose, parent branch) for garbage collection and auditing.

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

The harness periodically cleans up:

- Merged branches older than a retention threshold
- Orphaned worktrees
- Unreachable commits (standard git gc)

Retention policies are configurable per-tenant or per-agent.
