# Agent Kernel Design - Implementation Plan

## Overview

The Agent Kernel uses OpenCode as the execution engine, managed by a sidecar service that bridges to the Hub control plane.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Hub (existing)                           │
│  - Agent definitions, credentials, wallets                      │
│  - Offerings/discovery                                         │
│  - Kernel registration and lifecycle management                │
│  - Credential refresh (pushes updates to sidecars)             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ REST API + Credential Push
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              Sidecar (packages/sidecar/)                        │
│  - One per machine                                              │
│  - Spawns ONE OpenCode process                                 │
│  - Manages sessions (agents) in OpenCode                       │
│  - Receives credentials from Hub (pushed at instantiation)     │
│  - Exposes HTTP API for tool invocation                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            │ Spawns + manages
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              OpenCode (execution engine)                        │
│  - Runs agents as sessions                                      │
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

## Prototype Scope

- Single machine (sidecar + OpenCode + Hub)
- Credentials pushed from Hub to sidecar at instantiation
- Tool proxy with credentials (no OAuth refresh for prototype)
- No persistence (sessions lost on restart)
