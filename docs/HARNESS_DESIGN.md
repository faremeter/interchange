# Agent Harness Design - Implementation Plan

## Overview

The Agent Harness uses OpenCode as the execution engine, managed by a sidecar service that bridges to the Hub control plane.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Hub (existing)                           │
│  - Agent definitions, credentials, wallets                      │
│  - Offerings/discovery                                         │
│  - Harness registration and lifecycle management               │
│  - Credential refresh (pushes updates to sidecars)             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ REST API + Credential Push
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Sidecar (packages/sidecar/)                        │
│  - One per machine                                              │
│  - Spawns ONE OpenCode process                                 │
│  - Manages agents deployed by the Hub                          │
│  - Receives credentials from Hub (pushed at instantiation)     │
│  - Exposes HTTP API for tool invocation                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Spawns + manages
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              OpenCode (execution engine)                        │
│  - Runs agent harnesses                                         │
│  - Subagents are forked sessions                                │
│  - Calls sidecar to invoke tools (credentials never exposed)    │
└─────────────────────────────────────────────────────────────────┘
```

## Sidecar Package Structure

```
packages/sidecar/
├── src/
│   ├── index.ts           # Entry point, starts HTTP server
│   ├── sidecar.ts         # Core sidecar logic
│   ├── opencode.ts        # Spawns/manages OpenCode process
│   ├── registry.ts        # Registers with Hub
│   ├── routes/
│   │   ├── agent.ts       # Agent lifecycle endpoints
│   │   └── tools.ts       # Tool proxy to Hub (adds credentials)
│   └── types.ts           # TypeScript types
├── bin/sidecar            # Start script (sources .env)
├── package.json
└── tsconfig.json
```

## API Design

### Hub → Sidecar

| Method   | Path                            | Description                               |
| -------- | ------------------------------- | ----------------------------------------- |
| `GET`    | `/health`                       | Health check                              |
| `POST`   | `/agents`                       | Create agent session in OpenCode          |
| `GET`    | `/agents/:id`                   | Get agent status                          |
| `DELETE` | `/agents/:id`                   | Stop agent                                |
| `POST`   | `/agents/:id/message`           | Send message to agent                     |
| `PUT`    | `/credentials/:agentId`         | Push credentials to sidecar (instantiate) |
| `PUT`    | `/credentials/:agentId`         | Push updated credentials (refresh)        |
| `POST`   | `/credentials/:agentId/refresh` | Request credential refresh from Hub       |

### Sidecar → Hub (for proxying)

| Method | Path                                  | Description                |
| ------ | ------------------------------------- | -------------------------- |
| `POST` | `/api/sidecars`                       | Register sidecar           |
| `GET`  | `/api/sidecars/:id`                   | Heartbeat/status           |
| `POST` | `/api/sidecars/:id/agents`            | Deploy agent               |
| `GET`  | `/api/agents/:id/capabilities`        | Get capabilities           |
| `POST` | `/api/tools/:id/invoke`               | Invoke tool                |
| `POST` | `/api/agents/:id/credentials/refresh` | Request credential refresh |

### Sidecar → OpenCode

OpenCode runs on a port (e.g., 4096). Sidecar proxies requests through.

| Method | Path                                        | Description    |
| ------ | ------------------------------------------- | -------------- |
| `POST` | `http://localhost:PORT/session`             | Create session |
| `POST` | `http://localhost:PORT/session/:id/message` | Send message   |
| `GET`  | `http://localhost:PORT/session/:id/events`  | SSE stream     |

### OpenCode → Sidecar (callbacks)

OpenCode calls these endpoints when the agent needs to invoke tools. Credentials are NEVER exposed to OpenCode - the sidecar acts as a trusted proxy.

| Method | Path                    | Description                             |
| ------ | ----------------------- | --------------------------------------- |
| `POST` | `/tools/:toolId/invoke` | Execute tool (sidecar adds credentials) |

## Implementation Sequence

### Phase 1: Sidecar Skeleton

1. Create `packages/sidecar/` with basic structure
2. Hono HTTP server with health endpoint
3. Start script that loads config

### Phase 2: Hub Integration

1. Add sidecar registration to Hub (`POST /api/sidecars`)
2. Add agent deployment endpoints
3. Add credential/capability endpoints

### Phase 3: OpenCode Lifecycle

1. Sidecar spawns OpenCode process
2. Sidecar creates sessions (agents) in OpenCode
3. Proxy messages through sidecar → OpenCode

### Phase 4: Tool Invocation

1. Sidecar exposes `/tools/:toolId/invoke` endpoint
2. OpenCode calls this when agent wants to use a tool
3. Sidecar looks up credentials in memory
4. Sidecar adds credentials, invokes tool via Hub
5. Result returned to OpenCode (credentials never exposed)

### Phase 5: Credential Push

1. Hub pushes credentials via `PUT /credentials/:agentId` at instantiation
2. Hub pushes updates via same endpoint on refresh
3. Sidecar handles `POST /credentials/:agentId/refresh` for runtime failures

## Configuration

Environment variables:

```env
# Sidecar
SIDECAR_PORT=4097
SIDECAR_ID=<generated UUID>

# Hub connection
HUB_URL=http://localhost:3000

# OpenCode
OPENCODE_PORT=4096
OPENCODE_SERVER_PASSWORD=<generated>
```

## OpenCode Session Flow

1. Hub calls `POST /sidecar/agents` with agent config
2. Sidecar creates session in OpenCode:
   ```typescript
   // POST http://localhost:4096/session
   {
     agent: agentConfig.systemPrompt,
     skills: agentConfig.skills
   }
   ```
3. Hub **pushes** credentials to sidecar:
   ```
   PUT http://localhost:4097/credentials/{agentId}
   { credentials: [...] }
   ```
4. Sidecar stores credentials in memory (never exposed to OpenCode)
5. When agent needs to call a tool, OpenCode calls:
   ```
   POST http://localhost:4097/tools/{toolId}/invoke
   ```
6. Sidecar adds credentials internally, invokes the tool, returns result

## Credential Lifecycle

### Instantiation (Push)

When an agent is instantiated:

1. Hub creates the agent session in sidecar
2. Hub pushes credentials to sidecar via `PUT /credentials/:agentId`
3. Sidecar stores credentials in memory, scoped to that agent

### Proactive Refresh

Hub runs a background process that:

1. Refreshes OAuth tokens before they expire
2. Pushes updated credentials to all sidecars holding that credential:
   ```
   PUT /credentials/:agentId { credentials: [...updated...] }
   ```

### Runtime Refresh (Pull)

If sidecar encounters a 401 from external API:

1. Sidecar requests refresh from Hub:
   ```
   POST /credentials/:agentId/refresh
   ```
2. Hub attempts refresh, returns new credential
3. Sidecar updates stored credentials, retries request

## Security Model

**Credentials are NEVER exposed to OpenCode.** The sidecar acts as a trusted execution proxy.

### Credential Lifecycle

- **Instantiation**: Hub pushes credentials to sidecar when agent starts
- **Storage**: Credentials stored in sidecar memory (not persisted)
- **Refresh**: Hub proactively pushes updated credentials before expiry
- **Runtime**: If 401, sidecar requests refresh from Hub

### Credential Isolation

- Credentials are stored only in the sidecar (pushed from Hub)
- OpenCode never sees any credentials - not via env vars, not via API
- When agent needs to call an external API, OpenCode asks sidecar to invoke the tool
- Sidecar adds credentials internally and makes the actual call
- Only the result (not credentials) is returned to OpenCode

### Tool Invocation Flow

```
Agent → OpenCode: "call tool X with params Y"
      → Sidecar: "invoke tool X with params Y" (no credentials)
      → Sidecar: looks up credentials for this agent internally
      → External API: actual call with credentials
      → Sidecar: returns result (no credentials exposed)
      → OpenCode → Agent
```

### Security Considerations

- Sidecar validates all requests from OpenCode
- Tool invocations validated against agent capabilities
- Credentials scoped to specific agent, never exposed
- OpenCode server password scoped per-session

## Sidecar Reconnection Protocol

The sidecar persists agent state in isogit repositories under `SIDECAR_DATA_DIR`, one repository per agent address. On restart, the sidecar scans this directory to discover agents it previously managed, then reconnects to the hub and proves ownership of each agent address.

### Per-Agent Key Pairs

Each agent has its own Ed25519 key pair, generated when the agent is first deployed to the sidecar and stored alongside the agent's isogit repository. The key pair persists across sidecar restarts. The public key is transmitted to the hub in the initial `agent.deploy.ack` frame so the hub can verify ownership on reconnect.

Key format follows the project convention: Ed25519 in SSH format.

Directory layout under `SIDECAR_DATA_DIR`:

```
SIDECAR_DATA_DIR/
  agent-name_at_tenant_interchange_network/
    .git/              # isogit repository (context, audit records)
    context.json       # conversation state
    keys/
      id_ed25519       # agent private key
      id_ed25519.pub   # agent public key
```

The directory name is the agent address with `@` replaced by `_at_` and non-alphanumeric characters (except `-` and `_`) replaced by `_`.

### Agent Deployment vs User Sessions

The sidecar manages agents, not user sessions. When the hub deploys an agent to a sidecar, the sidecar starts a harness for that agent. The harness runs continuously, receiving messages from any source — other agents, users, system signals. User sessions are a hub-side concept: the hub tracks which users are interacting with which agents and routes user messages to the agent's address accordingly, but the sidecar does not know or care about individual user sessions.

The hub maintains a sidecar-to-agent mapping in its database. This mapping determines where to route messages for a given agent address. When a sidecar disconnects, the hub knows which agents are affected and queues messages for them until the sidecar reconnects.

### Registration

On first connection (no existing agents in `SIDECAR_DATA_DIR`), the sidecar sends a `register` frame to identify itself to the hub. The hub responds by sending `agent.deploy` frames for any agents assigned to this sidecar.

| Direction     | Frame      | Fields                                           | Description                 |
| ------------- | ---------- | ------------------------------------------------ | --------------------------- |
| Sidecar → Hub | `register` | `sidecarId`, `token`, `agentAddresses: string[]` | Identify sidecar to the hub |

On reconnection (existing agents in `SIDECAR_DATA_DIR`), the sidecar sends a `reconnect` frame instead, which triggers the challenge/response verification flow described below.

### Deployment Frames

**Hub to Sidecar:**

| Frame            | Fields                                                                | Description                       |
| ---------------- | --------------------------------------------------------------------- | --------------------------------- |
| `agent.deploy`   | `agentAddress`, `agentId`, `config` (system prompt, model, providers) | Deploy an agent to this sidecar   |
| `agent.undeploy` | `agentAddress`, `reason`                                              | Remove an agent from this sidecar |

**Sidecar to Hub:**

| Frame              | Fields                      | Description                            |
| ------------------ | --------------------------- | -------------------------------------- |
| `agent.deploy.ack` | `agentAddress`, `publicKey` | Agent deployed, here is its public key |
| `agent.error`      | `agentAddress`, `error`     | Deployment failed                      |

When the hub sends `agent.deploy`, the sidecar generates a key pair (if new) or loads the existing one, initializes the harness, and responds with `agent.deploy.ack` including the agent's public key. The hub stores this public key for reconnect verification.

When the hub sends `agent.undeploy`, the sidecar tears down the harness and may clean up local state depending on the reason (e.g., permanent retirement vs temporary rebalancing).

### Reconnection Flow

1. Sidecar starts and scans `SIDECAR_DATA_DIR` for agent repositories
2. For each repository with a key pair, sidecar loads the private key and records the agent address
3. Sidecar connects to the hub via WebSocket and sends a `reconnect` frame listing agent addresses it has locally
4. Hub generates a cryptographically random nonce (minimum 32 bytes) per address and sends a `challenge` frame
5. Sidecar signs `nonce || agent_address` with each agent's private key and sends a `challenge.response` frame
6. Hub verifies each signature against the stored public key for that address
7. For each verified address that the hub still wants running, hub sends `agent.deploy` (with `restored: true`)
8. For each verified address the hub no longer wants running, hub sends `agent.undeploy`
9. For each failed address, hub sends a `challenge.failed` frame with the address and reason
10. Sidecar loads isogit context for each restored agent, starts the harness, and sends `agent.deploy.ack`
11. Hub flushes queued undelivered messages as `message.send` frames after receiving the ack

This reconciliation handles all cases:

- **Agent still active**: hub sends `agent.deploy` with `restored: true`, sidecar resumes
- **Agent was undeployed while disconnected**: hub sends `agent.undeploy`, sidecar cleans up
- **Agent unknown to hub**: hub sends `challenge.failed`, sidecar cleans up orphaned state
- **Hub has new agents for this sidecar**: hub sends `agent.deploy` (without `restored`) after reconciliation completes

### Reconnection Frames

**Sidecar to Hub:**

| Frame                | Fields                                           | Description                                |
| -------------------- | ------------------------------------------------ | ------------------------------------------ |
| `reconnect`          | `sidecarId`, `token`, `agentAddresses: string[]` | Sidecar announces addresses it has locally |
| `challenge.response` | `responses: { address, signature }[]`            | Signed proof of key ownership per address  |

**Hub to Sidecar:**

| Frame              | Fields                             | Description                         |
| ------------------ | ---------------------------------- | ----------------------------------- |
| `challenge`        | `challenges: { address, nonce }[]` | One nonce per address to be signed  |
| `challenge.failed` | `address`, `reason`                | Verification failed for one address |

The `agent.deploy` frame includes an optional `restored: boolean` field. When `true`, the sidecar loads existing isogit context rather than initializing fresh state.

### Nonce Security

Nonces are single-use. The hub marks each nonce as consumed after verification and rejects any reuse. The signing surface is `nonce || agent_address` (concatenated bytes), which prevents a signature for one address from being replayed for a different address.

### Partial Failure

Verification is per-address. If a sidecar presents three addresses and one fails verification, the hub accepts the two verified addresses and rejects the failed one. The sidecar logs the rejection and continues serving the verified agents. A failed address does not affect other addresses on the same connection.

### Hub Message Queuing

While a sidecar is disconnected, the hub queues user messages in its database. These messages are not lost. On successful reconnection and address verification, the hub flushes queued messages as `message.send` frames in chronological order.

The hub does not queue messages indefinitely. A configurable TTL determines when queued messages expire. When an agent is undeployed from a sidecar, the hub stops queuing and drops any remaining messages for that address.

### Authority Model

The sidecar's isogit repository is the source of truth for agent inference context (conversation history, pending operations, token usage). The hub's database is a delivery queue for user messages that have not yet reached the agent. On reconnect, the hub delivers queued messages to the sidecar, which incorporates them into the agent's context via the normal message handling path.

### Key Rotation

When an agent's key pair is rotated, the sidecar sends a `key.rotated` frame containing the agent address and the new public key. The hub stores the new key alongside the old one and begins a grace period during which both keys are accepted for challenge verification. The grace period duration is configured at the hub. After the grace period, the old key is retired and only the new key is accepted.

| Direction     | Frame         | Fields                      | Description            |
| ------------- | ------------- | --------------------------- | ---------------------- |
| Sidecar → Hub | `key.rotated` | `agentAddress`, `publicKey` | Announce a rotated key |

### Failure Paths

If the hub rejects all addresses on reconnect, the sidecar logs the failure and does not serve any agents. It does not attempt fresh deployments for rejected addresses, since that would bypass the ownership proof. The operator must investigate the key mismatch.

If the sidecar discovers agent repositories but has no key pairs for them (e.g., keys were deleted), it skips those agents and logs a warning. It does not generate new keys, since the hub would reject signatures from unknown keys.

## Prototype Scope

- Single machine (sidecar + OpenCode + Hub)
- Credentials pushed from Hub to sidecar at instantiation
- Tool proxy with credentials (no OAuth refresh for prototype)

This document describes the prototype sidecar implementation. It diverges from the production architecture described in ARCHITECTURE.md in several ways: it uses REST/WebSocket for hub-sidecar communication instead of SMTP/IMAP, and it uses a simplified credential push model instead of the full bidirectional credential channel. The reconnection protocol above is designed for the prototype transport and will be adapted when the production transport is implemented.
