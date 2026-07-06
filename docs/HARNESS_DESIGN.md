# Agent Harness Design - Implementation

## Overview

The sidecar manages agent harnesses on behalf of the hub. Each agent gets its own harness instance backed by `@intx/harness`, with an isogit repository for persistent storage and an Ed25519 key pair for identity.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Hub                                       │
│  - Agent definitions, credentials                                │
│  - Session management, message persistence                       │
│  - Harness registration and lifecycle management                 │
│  - Sidecar WebSocket handler (challenge/response, deploy/undeploy)│
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Persistent WebSocket (outbound from sidecar)
                            │
┌───────────────────────────┴─────────────────────────────────────┐
│              Sidecar (apps/sidecar/)                              │
│  - One per machine                                                │
│  - Pure WebSocket client (no HTTP server)                        │
│  - Creates @intx/harness instances per agent              │
│  - Self-restores agent sessions from disk on restart             │
│  - Proves agent ownership via Ed25519 challenge/response         │
└─────────────────────────────────────────────────────────────────┘
```

## Sidecar Package Structure

The sidecar app is a thin wiring file that composes building blocks
out of `@intx/hub-agent`, `@intx/harness`, and `@intx/agent`. The
per-agent disk layout, harness lifecycle, and the hub WebSocket
protocol live in `@intx/hub-agent`; the in-process agent runtime
(`createAgent(def, env)` wrapping the reactor exactly once) lives in
`@intx/agent`; `@intx/harness` narrows to a composition layer that
calls `createAgent` and adds transport subscription, the connector
router, the INBOX watch, and connector-reply forwarding. The app
supplies the concrete crypto / tool / storage / authz plugins.

```
apps/sidecar/
├── src/
│   ├── index.ts             # Entry point, wires the stores, SessionManager, and HubLink
│   └── default-harness.ts   # HarnessBuilder; constructs the AgentDefinition and env, calls createHarness(def, env)
├── package.json
└── tsconfig.json
```

## Configuration

Environment variables:

```env
HUB_WS_URL=ws://localhost:3000/api/sidecars/ws
SIDECAR_ID=dev-sidecar-1
SIDECAR_TOKEN=dev-token
SIDECAR_DATA_DIR=./tmp/sidecar-data
```

## Hub ↔ Sidecar Communication

All communication between hub and sidecar is over a single persistent WebSocket connection. The sidecar connects outbound to the hub. There are no REST endpoints on the sidecar.

### Deployment Frames

**Hub to Sidecar:**

| Frame            | Fields                                                     | Description                       |
| ---------------- | ---------------------------------------------------------- | --------------------------------- |
| `agent.deploy`   | `agentAddress`, `agentId`, `config` (full `HarnessConfig`) | Deploy an agent to this sidecar   |
| `agent.undeploy` | `agentAddress`, `reason`                                   | Remove an agent from this sidecar |

**Sidecar to Hub:**

| Frame              | Fields                      | Description                            |
| ------------------ | --------------------------- | -------------------------------------- |
| `agent.deploy.ack` | `agentAddress`, `publicKey` | Agent deployed, here is its public key |
| `agent.error`      | `agentAddress`, `error`     | Deployment failed                      |

When the hub sends `agent.deploy`, the sidecar spawns a supervised **workflow-process child** to host the deployment and responds with `agent.deploy.ack`. For a single-step (launched-agent) deployment the sidecar generates the agent's key pair (if new) or loads the existing one, records the hub's public key for later deploy-pack verification, initializes the head's on-disk deploy-tree repository (the narrow `initRepo`, not the retired `provisionAgent`), and acks the agent's hex-encoded public key — the hub stores it in `agent_instance.publicKey` for reconnect verification. A multi-step deployment's address is workflow-derived and has no `agent_instance` row, so the ack carries the deployment address's own key — the same Ed25519 key the sidecar signs reconnect challenges with — and the hub stores it on the deployment's `workflow_deployment` row (keyed by address), mirroring the launched-agent path so both are reconnect-verifiable. Before the child is spawned, the inputs a restart cannot otherwise recover — the per-step inference sources, the session id, and (single-step only) the hub public key — are written to a per-deployment `deployment.json` record.

When the hub sends `agent.undeploy`, the sidecar shuts the deployment's supervisor down (killing the workflow-process child and releasing its IPC pipes and event-channel handle), unregisters the deployment address from the transport and from the mail/signal/drain routers, reclaims the deployment's per-step scratch, and deletes the `deployment.json` record so a later boot does not re-spawn a torn-down deployment. The agent's key pair and its durable agent-state / conversation repositories are left in place so a redeploy on the same address resumes them.

Credentials travel in the `agent.deploy` frame's inference **sources** — `config.sources`, and the per-step `workflow.sources` failover chains — where each `InferenceSource` carries its own API key. There is no separate credential push endpoint.

## Per-Agent Key Pairs

Each agent has its own Ed25519 key pair, generated when the agent is first deployed to the sidecar and stored alongside the agent's isogit repository. The key pair persists across sidecar restarts. The public key is transmitted to the hub in the initial `agent.deploy.ack` frame so the hub can verify ownership on reconnect.

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
    <deploymentId>/              # workflow-run substrate for one deployment
      deployment.json            # per-deployment restore record (mode 0600); see below
  workflow-step-state/
    <deploymentId>/              # ephemeral per-step scratch, reclaimed on undeploy
  agent-conversation-state/      # durable per-agent conversation, survives undeploy
```

The per-agent key directory is keyed by the sanitized agent address; the workflow subtrees are keyed by the derived deployment id.

The `deployment.json` record stores only what a restart cannot otherwise recover: the deployment's `agentAddress`, the `definitionId` naming its workflow definition on disk, each step's ordered inference-**sources** failover chain (`sources`), the optional inference `sessionId`, and — for a single-step deployment — the `hubPublicKey`. A `version` field guards the schema so a stale record can be rejected rather than parsed blindly. The record deliberately does **not** duplicate the workflow definition (kept on disk under its `definitionId` and re-read at restore) or the step grants (kept in each step's agent-state repo). Because each source embeds its API key, the record is written owner-only (mode 0600).

A live source rotation for a single-step deployment overwrites this record's `sources` before it takes effect. Persistence is what makes a rotation durable: a rotation whose write fails is not durable, and the deployment falls back to the last durably-recorded source list on the next recycle or restart.

The directory name is the agent address with `@` replaced by `_at_` and non-alphanumeric characters (except `-` and `_`) replaced by `_`.

## Agent Deployment vs User Sessions

The sidecar manages agents, not user sessions. When the hub deploys an agent to a sidecar, the sidecar spawns a supervised **workflow-process child** for that deployment. The child runs continuously, receiving messages from any source — other agents, users, system signals — and builds the agent harness inside its own process. User sessions are a hub-side concept: the hub tracks which users are interacting with which agents and routes user messages to the agent's address accordingly, but the sidecar does not know or care about individual user sessions.

The hub maintains a sidecar-to-agent mapping in its database. This mapping determines where to route messages for a given agent address. When a sidecar disconnects, the hub knows which agents are affected and queues messages for them until the sidecar reconnects.

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

## Registration

On first connection (no restorable agents in `SIDECAR_DATA_DIR`), the sidecar sends a `register` frame to identify itself to the hub. The hub responds by sending `agent.deploy` frames for any agents assigned to this sidecar.

| Direction     | Frame      | Fields                                           | Description                 |
| ------------- | ---------- | ------------------------------------------------ | --------------------------- |
| Sidecar → Hub | `register` | `sidecarId`, `token`, `agentAddresses: string[]` | Identify sidecar to the hub |

On reconnection (agents successfully restored from `SIDECAR_DATA_DIR`), the sidecar sends a `reconnect` frame instead, which triggers the challenge/response verification flow described below.

## Reconnection Protocol

### Self-Restoration

At boot, **before** opening the WebSocket connection to the hub, the sidecar scans `SIDECAR_DATA_DIR/workflow-runs/` for `deployment.json` records (`scanWorkflowDeploymentRecords`). Each record is validated at the trust boundary, its stored `agentAddress` is cross-checked against its directory name, its workflow definition is re-read and re-validated off disk with the same gates a fresh deploy applies, and its pinned inference sources are re-admitted against the providers this sidecar can build; a record that fails any check is logged and skipped so one bad deployment cannot strand the rest. Each surviving record is restored through the **same** spawn path a live deploy uses — a supervised workflow-process child, with the single-step head's key pair and recorded hub key re-established. Restoration completes before the `register` or `reconnect` frame is sent, and happens entirely on the sidecar side; the hub is not involved in restoration.

### Challenge/Response Verification

After self-restoration, the sidecar connects to the hub and proves ownership of each agent address:

1. Sidecar sends a `reconnect` frame listing the addresses it restored and their current deploy commit SHAs (`deployRefs`)
2. Hub generates a 32-byte random nonce per address and sends a `challenge` frame
3. Sidecar signs `nonce || agent_address` (concatenated bytes) with each agent's private key and sends a `challenge.response` frame
4. Hub verifies each signature against the stored public key for that address
5. Verified addresses are provisionally added to the routing table so the hub's `agent.reconnected` reaction can run for each
6. Hub compares each agent's advertised deploy ref against its own. For agents whose ref is stale or absent, the hub creates and sends a fresh deploy pack (fire-and-forget, does not block reconnect completion)
7. On a successful reaction, the address remains in the routing table and queued messages are flushed
8. On failure (the reconnect reaction rejected by governance), the address is rolled back from the routing table — its queue is preserved for the next reconnect attempt and the sidecar receives `challenge.failed`
9. For addresses that fail cryptographic verification, hub sends `challenge.failed` with the address and reason

A supervised deployment carries its grants in the deploy pack and refreshes them over the supervisor's IPC credentials snapshot at spawn and recycle, so reconnect no longer performs a grant-refresh round-trip over the wire.

### Reconnection Frames

**Sidecar to Hub:**

| Frame                | Fields                                                                                  | Description                                         |
| -------------------- | --------------------------------------------------------------------------------------- | --------------------------------------------------- |
| `reconnect`          | `sidecarId`, `token`, `agentAddresses: string[]`, `deployRefs?: Record<string, string>` | Sidecar announces addresses and current deploy SHAs |
| `challenge.response` | `responses: { address, signature }[]`                                                   | Signed proof of key ownership per address           |

**Hub to Sidecar:**

| Frame              | Fields                             | Description                         |
| ------------------ | ---------------------------------- | ----------------------------------- |
| `challenge`        | `challenges: { address, nonce }[]` | One nonce per address to be signed  |
| `challenge.failed` | `address`, `reason`                | Verification failed for one address |

### Nonce Security

Nonces are single-use. The hub marks each nonce as consumed after verification and rejects any reuse. The signing surface is `nonce || agent_address` (concatenated bytes), which prevents a signature for one address from being replayed for a different address.

### Partial Failure

Verification is per-address. If a sidecar presents three addresses and one fails verification, the hub accepts the two verified addresses and rejects the failed one. The sidecar logs the rejection and continues serving the verified agents. A failed address does not affect other addresses on the same connection.

## Hub Message Queuing

While a sidecar is disconnected, the hub queues messages in memory. These messages are flushed to the sidecar immediately after successful challenge verification. The queue has a configurable TTL (default 5 minutes) and maximum size (default 100 frames per agent address). Messages that exceed the TTL or queue size are dropped.

When a sidecar sends a `register` frame (first connection, no prior state), any existing disconnect queue for addresses on that sidecar is discarded — `register` bypasses challenge verification, so queued messages cannot be delivered without ownership proof.

## Authority Model

The sidecar's isogit repository is the source of truth for agent inference context (conversation history, pending operations, token usage). The hub's database is a delivery queue for user messages that have not yet reached the agent. On reconnect, the hub delivers queued messages to the sidecar, which incorporates them into the agent's context via the normal message handling path.

## Security Model

Credentials travel in the `agent.deploy` frame's inference `sources` (each `InferenceSource` carries its own API key) and are held in memory by the running deployment. They are also persisted to disk in the deployment's `deployment.json` record — the `sources` field embeds those API keys — so the sidecar can restore a deployment on restart without re-receiving them from the hub. The record is written owner-only (mode 0600), but storing provider API keys on the sidecar's disk at all is a known limitation of the prototype that should be addressed before production use.

## Key Rotation

Key rotation is not yet implemented. The architecture supports it: the sidecar would send a `key.rotated` frame with the new public key, and the hub would accept both old and new keys during a grace period. This is deferred until there is a concrete need.

## Failure Paths

If the hub rejects all addresses on reconnect, the sidecar logs the failure and does not serve any agents. It does not attempt fresh deployments for rejected addresses, since that would bypass the ownership proof. The operator must investigate the key mismatch.

If the sidecar discovers agent repositories but has no key pairs for them (e.g., keys were deleted), it skips those agents and logs a warning. It does not generate new keys, since the hub would reject signatures from unknown keys.

## Mail and Event Flow

Mail is the first-class communication primitive. The sidecar persists outbound mail from agents via `mail.outbound` frames sent to the hub. The hub persists inbound mail sent by users via `POST .../instances/:instanceId/mail` and dispatches it to the sidecar as a `mail.delivered` agent event.

The composition-layer harness exposes the agent's reactor event stream as `harness.stream()`, an `AsyncIterable<ReactorEmittedEvent>`. The sidecar's `HarnessBuilder` drains that stream and adapts each event into an `onEvent(event)` callback for the hub session channel. The stream carries inference activity, tool execution, reactor lifecycle, and fork events. `message.received` is a `ReactorInboundEvent` — it is delivered directly to the reactor director and is not forwarded to session channel subscribers. This keeps the external event stream focused on observable inference activity rather than internal routing signals.

Inference traces are stored separately from mail. The hub records one `inference_turn` per inference cycle and one or more `turn_part` rows per turn. The `/turns` endpoint serves these to UI clients independently of the `/mail` endpoint.

## Prototype Scope

This document describes the current prototype implementation. It diverges from the production architecture described in ARCHITECTURE.md in several ways: it uses WebSocket for hub-sidecar communication instead of SMTP/IMAP, uses SSE for user-facing event streaming instead of WebSocket session channels, and uses a simplified credential model where credentials travel in deploy frames rather than through a separate credential management channel.
