# Agent Harness Design - Implementation

## Overview

The sidecar manages agent workloads on behalf of the hub. Each deployment runs in its own supervised workflow child process: the sidecar's deploy router (`@intx/hub-agent`) creates one `WorkflowSupervisor` (`@intx/workflow-host`) per deployment, which spawns an isolated `bin/workflow-child` OS process, and each agent step runs in-process inside that child via `@intx/agent`. Each workload has an isogit repository for persistent storage and an Ed25519 key pair for identity.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Hub                                       │
│  - Agent definitions, credentials                                │
│  - Session management, message persistence                       │
│  - Harness registration and lifecycle management                 │
│  - Allocation-authenticated sidecar WebSocket handler             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Persistent WebSocket (outbound from sidecar)
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│              Sidecar (apps/sidecar/)                              │
│  - Created or reused only through a provisioner                   │
│  - Pure WebSocket client (no HTTP server)                        │
│  - Spawns a supervised workflow child process per deployment     │
│  - Self-restores agent sessions from disk on restart             │
│  - Bound to one allocation anchor and generation                 │
└─────────────────────────────────────────────────────────────────┘
```

## Sidecar Package Structure

The sidecar app is a thin wiring file that composes building blocks
out of `@intx/hub-agent`, `@intx/workflow-host`, and `@intx/agent`.
The per-deployment disk layout, the hub WebSocket protocol, and the
deploy router live in `@intx/hub-agent`; `@intx/workflow-host` owns
the per-deployment `WorkflowSupervisor` that spawns and supervises the
`bin/workflow-child` process; the in-process agent runtime
(`createAgent(def, env)` wrapping the reactor exactly once) lives in
`@intx/agent` and runs inside that child. The sidecar draws runtime
capabilities, step tools, and conversation state from `@intx/harness`.
The app supplies the concrete crypto / tool / storage / authz plugins.

```
apps/sidecar/
├── src/
│   ├── index.ts             # Entry point: wires the stores, the harness builder, and the hub link
│   └── default-harness.ts   # HarnessBuilder source-admission seam (canBuildSource) the deploy router consults before spawning
├── package.json
└── tsconfig.json
```

## Hub ↔ Sidecar Communication

All communication between hub and sidecar is over a single persistent WebSocket connection. The provisioner supplies the full Hub WebSocket URL, normally `ws://<hub>/api/sidecars/ws`, and the sidecar connects outbound to it. There are no REST endpoints on the sidecar.

### Deployment Frames

**Hub to Sidecar:**

| Frame            | Fields                                                                                                    | Description                       |
| ---------------- | --------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `agent.deploy`   | `agentAddress`, `agentId`, `config` (full `HarnessConfig`), `hubPublicKey`, `workflow?`, `provisionStep?` | Deploy an agent to this sidecar   |
| `agent.undeploy` | `agentAddress`, `reason`                                                                                  | Remove an agent from this sidecar |

**Sidecar to Hub:**

| Frame              | Fields                      | Description                            |
| ------------------ | --------------------------- | -------------------------------------- |
| `agent.deploy.ack` | `agentAddress`, `publicKey` | Agent deployed, here is its public key |
| `agent.error`      | `agentAddress`, `error`     | Deployment failed                      |

When the Hub sends `agent.deploy`, the sidecar spawns a supervised **workflow-process child** to host the deployment and responds with `agent.deploy.ack`. The sidecar records the Hub key used for deploy-pack verification and returns the supervisor public key. The Hub publishes that key only after initialization completes under the current allocation lock; reconnect authority remains the allocation credential, not the projected public key. Before the child is spawned, inputs a restart cannot otherwise recover are written to a per-deployment record.

When the hub sends `agent.undeploy`, the sidecar shuts the deployment's supervisor down (killing the workflow-process child and releasing its IPC pipes and event-channel handle), unregisters the deployment address from the transport and from the mail/signal/drain routers, reclaims the deployment's per-step scratch, and deletes the `deployment.json` record so a later boot does not re-spawn a torn-down deployment. The agent's key pair and its durable agent-state / conversation repositories are left in place so a redeploy on the same address resumes them.

Credentials travel in the `agent.deploy` frame's inference **sources** — `config.sources`, and the per-step `workflow.sources` failover chains — where each `InferenceSource` carries its own API key. There is no separate credential push endpoint.

## Per-Agent Key Pairs

Each agent has its own Ed25519 key pair, generated when the agent is first deployed to the sidecar and stored alongside the agent's isogit repository. The key pair persists across sidecar restarts. The public key is transmitted to the Hub in the initial `agent.deploy.ack` frame for the deployment's published identity and signed-content provenance. Allocation credentials, not agent keys, authorize reconnect routing.

Keys are stored as raw 32-byte binary files under a `keys/` directory within the agent's data directory.

Directory layout under `SIDECAR_DATA_DIR`:

```
SIDECAR_DATA_DIR/
  <sanitized-agent-address>/     # per-agent key custody + head deploy-tree repo
    .git/                        # isogit repository (deploy tree, context, audit records)
    keys/
      id_ed25519                 # agent private key (raw 32 bytes, mode 0600)
      id_ed25519.pub             # agent public key (raw 32 bytes)
  workflow-runs/
    <runId>/                     # workflow-run substrate for one run
      deployment.json            # per-run restore record (mode 0600); see below
  workflow-step-state/
    <runId>/                     # ephemeral per-step scratch, reclaimed on undeploy
  agent-conversation-state/      # durable per-agent conversation, survives undeploy
```

The per-agent key directory is keyed by the sanitized run address; the workflow subtrees are keyed by the derived run id.

The `deployment.json` record stores only what a restart cannot otherwise recover: the deployment's `agentAddress`, the `definitionId` naming its workflow definition on disk, each step's ordered inference-**sources** failover chain (`sources`), the optional inference `sessionId`, and — for a single-step deployment — the `hubPublicKey`. A `version` field guards the schema so a stale record can be rejected rather than parsed blindly. The record deliberately does **not** duplicate the workflow definition (kept on disk under its `definitionId` and re-read at restore) or the step grants (kept in each step's agent-state repo). Because each source embeds its API key, the record is written owner-only (mode 0600).

A live source rotation for a single-step deployment overwrites this record's `sources` before it takes effect. Persistence is what makes a rotation durable: a rotation whose write fails is not durable, and the deployment falls back to the last durably-recorded source list on the next recycle or restart.

The directory name is the run address with `@` replaced by `_at_` and non-alphanumeric characters (except `-` and `_`) replaced by `_`.

## Agent Deployment vs User Sessions

The sidecar manages agents, not user sessions. When the hub deploys an agent to a sidecar, the sidecar spawns a supervised **workflow-process child** for that deployment. The child runs continuously, receiving messages from any source — other agents, users, system signals — and builds the agent harness inside its own process. User sessions are a hub-side concept: the hub tracks which users are interacting with which agents and routes user messages to the agent's address accordingly, but the sidecar does not know or care about individual user sessions.

The hub maintains a sidecar-to-agent mapping in its database. This mapping determines where to route messages for a given run address. When a sidecar disconnects, the hub knows which agents are affected and queues messages for them until the sidecar reconnects.

### Connector threads and user sessions

The connector is **one durable thread per agent**. Anyone who sends conversational mail to the agent — a human via a hub session, a parent agent that launched this one as a sub-agent, a peer agent that initiates a conversation — joins the thread by stamping threading headers the harness recognizes. Participants accumulate; no one is displaced. The thread persists for the lifetime of the agent.

The connector router classifies each inbound message as:

- **`start`** when no thread is active. The sender becomes the first participant; the message-id becomes `threadRoot`; the subject is recorded and preserved for the life of the thread.
- **`continue`** when the message's `references` includes the active `threadRoot`, or its `inReplyTo` equals the active `lastMessageId`. The sender is added to the participant set; the previous most-recent speaker moves into `cc` (deduplicated against re-entry).
- **`passthrough`** for everything else — non-conversational mail (structured payloads, system notifications) and conversational mail without threading headers while a thread is active. The reactor still sees it, but the harness leaves it in the INBOX and the connector state is untouched.

Connector state has four parts: `threadRoot` (the first message's id), `lastMessageId` (the most recent message in either direction), `replyTo` (the most recent speaker — the primary recipient on the next outbound reply), and `cc` (every other participant who has spoken on the thread, deduplicated, in arrival order). When the reactor emits `connector.reply`, the outbound mail is addressed to `replyTo` with `cc` carrying everyone else — whoever spoke most recently gets the direct reply and the rest stay in the loop.

When a hub user composes mail to an agent, the hub decides what threading headers to stamp:

1. **Session history wins.** If the user already has prior mail in this session, the hub stamps `inReplyTo` and `references` from that session-history chain. The harness routes the message as `continue` against whatever thread the user's prior session message was part of.
2. **Connector cache fallback.** With no session history, the hub looks up the agent's cached connector state. If a thread is active, the hub stamps `inReplyTo = lastMessageId` and `references = [threadRoot]` — regardless of who else is on the thread. The user joins whatever conversation is in progress.
3. **No threading.** With no session history and no active connector, the hub sends the message threading-less. The harness routes it as `start`, establishing this user as the first participant on a new thread.

The hub learns the cached connector state from a `connector.state.changed` frame the sidecar emits whenever the router's state mutates. Cache entries are dropped on sidecar disconnect.

On reconnect, agents whose persisted state is non-null re-bootstrap the cache automatically: the router's `restore()` call from the reactor's first `wrappedStore.load()` fires `onStateChanged` because the state transitions from the cold-start `null` to the persisted value, and the sidecar lifts that callback onto a wire frame. Agents whose persisted state is null emit no frame — the cache stays absent until the harness produces its first real state change. The route handler treats absent and null identically.

The bootstrap restore happens **only on the first `wrappedStore.load()`**. Subsequent loads return the store's payload but do not restore from disk; once the router emits its first state change, the harness flips an `inMemoryStateAuthoritative` bit and refuses to clobber in-memory state with a stale disk value. This closes a race where the reactor's startup `load()` lands on the same microtask boundary as the watch callback's `commit()`: without the dirty bit, the second load would reset the router's freshly committed thread state to disk's null and the next `connector.reply` would fail to compose.

Two observable windows where the cache may be empty or stale, both of which fall through to threading-less mail and self-heal on the next state change:

1. **Between WebSocket connection and the reactor's first `wrappedStore.load()`.** A user message composed in this window finds an absent cache entry. After the load, a bootstrap frame populates the cache.
2. **Between a sidecar disconnect and the same sidecar's next `wrappedStore.load()` on reconnect.** The disconnect clears the cache. A user message composed in this window also finds an absent entry. If the cache was ahead of the persisted store at disconnect (a state mutation fired between the last `writeMetadata` cycle and the drop), the bootstrap will restore the persisted snapshot rather than the prior in-memory cache value. The cache reflects the freshest source of truth available, not a continuous history.

## Registration and Reconnection

A provisioner creates the sidecar identity before starting capacity. The bearer token resolves to one durable workflow probe or allocation and generation; there is no ambient registration pool. Probe identities carry no workflow address and may register only an empty address list.

Possession of the raw token is sufficient to authenticate as that durable owner while it remains active. Provisioners and workers must never log it. A provisioner that must persist the token for restartable capacity must use access-controlled secret storage and delete it when the durable owner becomes terminal. Connections crossing a non-loopback or otherwise untrusted transport must use `wss://`; plaintext `ws://` is only appropriate for local loopback development.

On first connection, the worker sends an empty `register` frame. After restoring a live supervisor from its allocation storage, it sends `reconnect` with that allocation's deployment address. The Hub accepts either frame only when the token resolves to the currently fenced generation. A reconnect may announce only the anchor address carried by that identity; a stale generation or unrelated address closes the socket.

The Hub restores that one address directly after validation. Frames following the reconnect are serialized behind it on the same socket, so a workflow-run pack re-driven by `onWorkflowAddressesRoutable` cannot overtake route restoration. Trigger and signal durability lives in `workflow_run_dispatch`; the Hub does not maintain an unscoped, in-memory queue for arbitrary disconnected sidecars.

| Direction     | Frame       | Fields                                       | Description                                      |
| ------------- | ----------- | -------------------------------------------- | ------------------------------------------------ |
| Sidecar → Hub | `register`  | `sidecarId`, `token`, empty `agentAddresses` | Register probe or undeployed allocation capacity |
| Sidecar → Hub | `reconnect` | `sidecarId`, `token`, the allocation address | Restore the current generation's route           |

## Self-Restoration

At boot, before opening the WebSocket connection, the in-tree sidecar scans its allocation data directory for a deployment record. The record is validated and restored through the same supervised workflow-child spawn path used by a fresh deploy. A provisioner may preserve or discard that storage according to the isolation and recovery guarantees it advertises.

## Authority Model

The sidecar's isogit repository is the source of truth for agent inference context (conversation history, pending operations, token usage). The hub's database is a delivery queue for user messages that have not yet reached the agent. On reconnect, the hub delivers queued messages to the sidecar, which incorporates them into the agent's context via the normal message handling path.

## Security Model

Credentials travel in the `agent.deploy` frame's inference `sources` (each `InferenceSource` carries its own API key) and are held in memory by the running deployment. They are also persisted to disk in the deployment's `deployment.json` record — the `sources` field embeds those API keys — so the sidecar can restore a deployment on restart without re-receiving them from the hub. The record is written owner-only (mode 0600), but storing provider API keys on the sidecar's disk at all is a known limitation of the prototype that should be addressed before production use.

## Key Rotation

Key rotation is not yet implemented. The architecture supports it: the sidecar would send a `key.rotated` frame with the new public key, and the hub would accept both old and new keys during a grace period. This is deferred until there is a concrete need.

## Failure Paths

If the Hub rejects a reconnect because its token is stale or its announced address does not match the allocation anchor, it closes the socket and leaves that capacity unroutable. The provisioner and allocation reconciler own recovery; the worker cannot mint a new identity or claim another address.

If the sidecar discovers agent repositories but has no key pairs for them (for example, keys were deleted), it skips those agents and logs a warning rather than generating a replacement identity that would break signed-content continuity.

## Mail and Event Flow

Mail is the first-class communication primitive. The sidecar persists outbound mail from agents via `mail.outbound` frames sent to the hub. The hub persists inbound mail sent by users via `POST .../workflows/runs/:runId/mail` and dispatches it to the sidecar as a `mail.delivered` agent event.

The composition-layer harness exposes the agent's reactor event stream as `harness.stream()`, an `AsyncIterable<ReactorEmittedEvent>`. The sidecar's `HarnessBuilder` drains that stream and adapts each event into an `onEvent(event)` callback for the hub session channel. The stream carries inference activity, tool execution, reactor lifecycle, and fork events. `message.received` is a `ReactorInboundEvent` — it is delivered directly to the reactor director and is not forwarded to session channel subscribers. This keeps the external event stream focused on observable inference activity rather than internal routing signals.

Inference traces are stored separately from mail. The hub records one `inference_turn` per inference cycle and one or more `turn_part` rows per turn. The `/turns` endpoint serves these to UI clients independently of the `/mail` endpoint.

## Prototype Scope

This document describes the current prototype implementation. It diverges from the production architecture described in ARCHITECTURE.md in several ways: it uses WebSocket for hub-sidecar communication instead of SMTP/IMAP, uses SSE for user-facing event streaming instead of WebSocket session channels, and uses a simplified credential model where credentials travel in deploy frames rather than through a separate credential management channel.
