# Package Layout

The system has three deployment contexts: a shared foundation consumed by everything, a control plane that runs as a server application, and an agent runtime that runs in many environments. Packages are organized into these three trees. The trees have different versioning cadences, different portability constraints, and different consumers.

## Shared Foundation

Consumed by both the control plane and the agent runtime. Pure logic and type definitions. No environment assumptions, no I/O beyond what the caller provides.

| Package       | Purpose                                                                                                                                                                                             |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@intx/types` | ArkType schemas, API contract, event protocol types, and the runtime interfaces (`ContextStore`, `CryptoProvider`, `MessageTransport`, `ToolRunner`). The stability contract between the two trees. |
| `@intx/authz` | Grant evaluation engine. Pattern matching, specificity ordering, condition evaluation. The hub uses it for API authorization; the harness uses it for tool-call gate enforcement.                   |
| `@intx/log`   | Logging abstraction.                                                                                                                                                                                |

Breaking changes to shared foundation packages cascade into both trees. These packages carry the strictest versioning discipline.

## Control Plane

A server application backed by Postgres. The hub packages and `db` are versioned and deployed together; `hub-client` is a browser library shipped separately to UI consumers.

| Package              | Purpose                                                                                                                                                                                                              |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@intx/hub-api`      | Hono app factory, context middleware, route group factories, per-request middleware, the better-auth wrapper, request/response helpers, the request context and session contract types, and the timeline read model. |
| `@intx/hub-sessions` | Sidecar websocket wire layer and typed event emitter, session service, event collector and registry, agent repository, hub session orchestrator, and the credential-push pipeline.                                   |
| `@intx/hub-common`   | Utilities genuinely shared across hub packages. Holds the id generator and its prefix table; see the package README for the rules that gate additions.                                                               |
| `@intx/hub-client`   | Browser/UI client library. API transport, SSE event stream transforms, instance session management. Consumed by the UI, not the hub itself.                                                                          |
| `@intx/db`           | Drizzle schema, connection pooling, credential resolution, grant store, tenant hierarchy queries. Postgres-specific.                                                                                                 |

`apps/hub` wires `hub-api` and `hub-sessions` together and starts the Hono server. `apps/admin-ui` is the browser SPA built against `hub-client`.

## Agent Runtime

Distributed across many environments: containers, VMs, local processes, Cloudflare Workers, browsers, mobile devices, embedded systems. `apps/sidecar` is the canonical host process for hub-orchestrated agents. Packages are split along two axes: portable core logic that runs everywhere, and environment-specific implementations that provide I/O capabilities.

### Portable Core

Uses `fetch` and `ReadableStream` exclusively. No Node APIs, no filesystem assumptions, no native bindings. Ships once, works everywhere.

| Package                | Purpose                                                                                                                                                                                                                                                                                                                  |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@intx/inference`      | Provider adapters (Anthropic, OpenAI-compatible), streaming harness, reactor, director system, context management, compaction, error classification, token accounting, cross-provider message transformation, test provider, and the `transforms/` subpath (`createSizeCapTransform` and friends). The reasoning engine. |
| `@intx/harness`        | Agent lifecycle, event routing, tool dispatch, content safety, session channel logic. Composes inference and authz with environment-specific implementations injected at startup.                                                                                                                                        |
| `@intx/agent`          | In-process agent runtime. Peer driver to the harness — the harness drives the reactor from a mail transport; the agent drives it from in-process calls. Bundles `storage-isogit` for context storage.                                                                                                                    |
| `@intx/mime`           | MIME message construction, multipart assembly, PGP detached signature generation, RFC 2822 formatting, and MIME parsing. Used by message transports.                                                                                                                                                                     |
| `@intx/pack-transport` | Git pack protocol chunking and reassembly. Transfers git object data between the hub and sidecars over WebSocket or HTTP.                                                                                                                                                                                                |

### Environment-Specific Implementations

Each package implements an interface defined in `@intx/types`. The harness accepts any conforming implementation. Different environments compose different implementations.

**Storage** (implements `ContextStore`):

| Package                | Environment                  | Implementation                                                                                 |
| ---------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------- |
| `@intx/storage-isogit` | Constrained (Worker/browser) | isomorphic-git with object storage (R2/S3/IndexedDB) for persistence. Pure JS, no native deps. |

**Cryptographic Identity** (implements `CryptoProvider`):

| Package             | Environment | Implementation                                                                                      |
| ------------------- | ----------- | --------------------------------------------------------------------------------------------------- |
| `@intx/crypto-node` | Server      | Node `crypto` module. Ed25519 key generation, signing, verification. SSH/PGP/X.509 format handling. |

**Message Transport** (implements `MessageTransport`):

| Package             | Environment | Implementation                                                                                                                       |
| ------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `@intx/mail-memory` | Testing     | In-memory mail transport for single-process and test environments. Addresses register and exchange mail locally via local mailboxes. |

**Tool Sets**:

| Package             | Environment | What it provides                                                                                                                            |
| ------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `@intx/tools-posix` | Server      | File read/write/edit, shell execution, git operations. Requires filesystem and process spawning.                                            |
| `@intx/tools-lsp`   | Server      | Language-server diagnostics. Composes with `tools-posix` and spawns LSP servers over JSON-RPC. Requires process spawning and `which` shell. |

### Why the splits exist

A package that pulls in native git bindings has no business shipping to a Cloudflare Worker. A package that bundles isomorphic-git has no business shipping to a server with native git available. The environment splits exist because the dependency trees are genuinely different — different native modules, different APIs, different capabilities. These are not configuration differences. They are different code.

The portable core packages (`inference`, `harness`, `agent`) never import environment-specific code. They operate through interfaces defined in `@intx/types`. The harness is wired at startup with concrete implementations chosen by the embedder.

## Development and Testing

| Package                                  | Purpose                                                                                                                                                                                                                                                                  |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@intx/inference-testing`                | Deterministic test harness for the `@intx/inference` streaming pipeline. Virtual-clock-driven simulated fetch, scripted provider wire bytes, matcher suite. Used by tests across the agent runtime tree.                                                                 |
| `@intx/inference-discovery`              | Shared runtime for the wire-capture rig: provider plug-in contract, capture runner, capability catalog, support matrix, and the parser behind the `bin/discover.ts` CLI. Produces the fixture bundles under `packages/inference-testing/wire/` that the harness replays. |
| `@intx/inference-discovery-google-genai` | Google GenAI provider plug-in for the discovery rig. Captures Gemini wire responses (text, multimodal, function calling, code execution, grounding, files API) across streaming and non-streaming variants.                                                              |
| `@intx/inference-discovery-openai`       | OpenAI-protocol provider plug-in for the discovery rig. Carries the protocol layer plus per-deployment wiring; OpenCode Zen (Moonshot, Z.AI, DeepSeek, Alibaba, Xiaomi MiMo) is the first deployment.                                                                    |

## Dependency Rules

1. Agent runtime packages never depend on control-plane packages.
2. Control-plane packages never depend on agent runtime packages.
3. Both trees depend on the shared foundation (`types`, `authz`, `log`).
4. Environment-specific packages depend on `types` for interfaces, never on each other (`tools-lsp` on `tools-posix` is the documented exception — see the tool-set table).
5. The harness depends on `types` for interfaces and `inference` for portable logic. It does not depend on any environment-specific package.
