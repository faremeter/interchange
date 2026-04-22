# Agent Harness Design - Implementation

## Overview

The sidecar manages agent harnesses on behalf of the hub. Each agent gets its own harness instance backed by `@interchange/harness`, with an isogit repository for persistent storage and an Ed25519 key pair for identity.

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
│  - Creates @interchange/harness instances per agent              │
│  - Self-restores agent sessions from disk on restart             │
│  - Proves agent ownership via Ed25519 challenge/response         │
└─────────────────────────────────────────────────────────────────┘
```

## Sidecar Package Structure

```
apps/sidecar/
├── src/
│   ├── main.ts              # Entry point, wires session manager + ws client
│   ├── ws-client.ts         # WebSocket client to hub (frame protocol)
│   ├── session-manager.ts   # Creates/destroys harness instances per agent
│   ├── key-store.ts         # Per-agent Ed25519 key pairs and config persistence
│   └── ws-client.test.ts    # Tests
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

When the hub sends `agent.deploy`, the sidecar generates a key pair (if new) or loads the existing one, persists the `HarnessConfig` to `agent.json`, initializes the harness, and responds with `agent.deploy.ack` including the agent's hex-encoded public key. The hub stores this public key for reconnect verification.

When the hub sends `agent.undeploy`, the sidecar stops the harness, unregisters the agent from the transport, and clears the persisted config.

Credentials travel in the `agent.deploy` frame's `config.providers` array. There is no separate credential push endpoint.

## Per-Agent Key Pairs

Each agent has its own Ed25519 key pair, generated when the agent is first deployed to the sidecar and stored alongside the agent's isogit repository. The key pair persists across sidecar restarts. The public key is transmitted to the hub in the initial `agent.deploy.ack` frame so the hub can verify ownership on reconnect.

Keys are stored as raw 32-byte binary files under a `keys/` directory within the agent's data directory.

Directory layout under `SIDECAR_DATA_DIR`:

```
SIDECAR_DATA_DIR/
  agent-name_at_tenant_interchange_network/
    .git/              # isogit repository (context, audit records)
    agent.json         # persisted HarnessConfig for session restore
    keys/
      id_ed25519       # agent private key (raw 32 bytes)
      id_ed25519.pub   # agent public key (raw 32 bytes)
```

The `agent.json` file stores the full harness configuration (system prompt, model, providers, session ID) so that the sidecar can restore agent sessions on restart without re-receiving the config from the hub.

The directory name is the agent address with `@` replaced by `_at_` and non-alphanumeric characters (except `-` and `_`) replaced by `_`.

## Agent Deployment vs User Sessions

The sidecar manages agents, not user sessions. When the hub deploys an agent to a sidecar, the sidecar starts a harness for that agent. The harness runs continuously, receiving messages from any source — other agents, users, system signals. User sessions are a hub-side concept: the hub tracks which users are interacting with which agents and routes user messages to the agent's address accordingly, but the sidecar does not know or care about individual user sessions.

The hub maintains a sidecar-to-agent mapping in its database. This mapping determines where to route messages for a given agent address. When a sidecar disconnects, the hub knows which agents are affected and queues messages for them until the sidecar reconnects.

## Registration

On first connection (no restorable agents in `SIDECAR_DATA_DIR`), the sidecar sends a `register` frame to identify itself to the hub. The hub responds by sending `agent.deploy` frames for any agents assigned to this sidecar.

| Direction     | Frame      | Fields                                           | Description                 |
| ------------- | ---------- | ------------------------------------------------ | --------------------------- |
| Sidecar → Hub | `register` | `sidecarId`, `token`, `agentAddresses: string[]` | Identify sidecar to the hub |

On reconnection (agents successfully restored from `SIDECAR_DATA_DIR`), the sidecar sends a `reconnect` frame instead, which triggers the challenge/response verification flow described below.

## Reconnection Protocol

### Self-Restoration

When the WebSocket connection to the hub opens, the sidecar scans `SIDECAR_DATA_DIR` for agent directories containing both a key pair and an `agent.json` config. For each valid directory, the sidecar restores the harness from the persisted config. The `register` or `reconnect` frame is held until restoration completes. This restoration happens entirely on the sidecar side — the hub is not involved in session restoration.

### Challenge/Response Verification

After self-restoration, the sidecar connects to the hub and proves ownership of each agent address:

1. Sidecar sends a `reconnect` frame listing the addresses it restored and their current deploy commit SHAs (`deployRefs`)
2. Hub generates a 32-byte random nonce per address and sends a `challenge` frame
3. Sidecar signs `nonce || agent_address` (concatenated bytes) with each agent's private key and sends a `challenge.response` frame
4. Hub verifies each signature against the stored public key for that address
5. Verified addresses are provisionally added to the routing table (required so the `grants.update` request/ack round-trip can reach the sidecar)
6. Hub sends `grants.update` with current grants for each verified address
7. Hub compares each agent's advertised deploy ref against its own. For agents whose ref is stale or absent, the hub creates and sends a fresh deploy pack (fire-and-forget, does not block reconnect completion)
8. On grant refresh success, the address remains in the routing table and queued messages are flushed
9. On failure (grant refresh rejected or timed out), the address is rolled back from the routing table — its queue is preserved for the next reconnect attempt and the sidecar receives `challenge.failed`
10. For addresses that fail cryptographic verification, hub sends `challenge.failed` with the address and reason

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

Credentials travel in the `agent.deploy` frame's `config.providers` array and are held in memory by the harness. They are never persisted to disk (the `agent.json` file contains the full config including provider entries with API keys — this is a known limitation of the prototype that should be addressed before production use).

## Key Rotation

Key rotation is not yet implemented. The architecture supports it: the sidecar would send a `key.rotated` frame with the new public key, and the hub would accept both old and new keys during a grace period. This is deferred until there is a concrete need.

## Failure Paths

If the hub rejects all addresses on reconnect, the sidecar logs the failure and does not serve any agents. It does not attempt fresh deployments for rejected addresses, since that would bypass the ownership proof. The operator must investigate the key mismatch.

If the sidecar discovers agent repositories but has no key pairs for them (e.g., keys were deleted), it skips those agents and logs a warning. It does not generate new keys, since the hub would reject signatures from unknown keys.

## Prototype Scope

This document describes the current prototype implementation. It diverges from the production architecture described in ARCHITECTURE.md in several ways: it uses WebSocket for hub-sidecar communication instead of SMTP/IMAP, uses SSE for user-facing event streaming instead of WebSocket session channels, and uses a simplified credential model where credentials travel in deploy frames rather than through a separate credential management channel.
