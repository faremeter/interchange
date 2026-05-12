# Package Layout

The system has three deployment contexts: a shared foundation consumed by everything, a control plane that runs as a server application, and an agent runtime that runs in many environments. Packages are organized into these three trees. The trees have different versioning cadences, different portability constraints, and different consumers.

## Shared Foundation

Consumed by both the control plane and the agent runtime. Pure logic and type definitions. No environment assumptions, no I/O beyond what the caller provides.

| Package              | Purpose                                                                                                                                                                                                              |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@interchange/types` | ArkType schemas, API contract, event protocol types, and all runtime interfaces (`ContextStore`, `CryptoProvider`, `MessageTransport`, `ToolRunner`, `WalletBackend`). The stability contract between the two trees. |
| `@interchange/authz` | Grant evaluation engine. Pattern matching, specificity ordering, condition evaluation. The hub uses it for API authorization; the harness uses it for tool-call gate enforcement.                                    |
| `@interchange/log`   | Logging abstraction.                                                                                                                                                                                                 |

Breaking changes to shared foundation packages cascade into both trees. These packages carry the strictest versioning discipline.

## Control Plane

A server application. One instance (or cluster), backed by Postgres. No portability concern. Packages are versioned and deployed together as a coordinated set.

| Package                        | Purpose                                                                                                                                                   |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@interchange/hub-api`         | Hono routes, middleware, request/response handling. The HTTP API surface.                                                                                 |
| `@interchange/hub-db`          | Drizzle schema, connection pooling, credential resolution, grant store, tenant hierarchy queries. Postgres-specific.                                      |
| `@interchange/hub-client`      | Browser/UI client library. API transport, SSE event stream transforms, instance session management. Consumed by the UI, not the hub itself.               |
| `@interchange/hub-credentials` | Background credential refresh workers, proactive push of updated credentials to harnesses. Long-running process, not request/response.                    |
| `@interchange/hub-sessions`    | WebSocket proxy between clients and harnesses, session brokering, NAT traversal for harnesses behind firewalls. Stateful, long-lived connections.         |
| `@interchange/hub-ca`          | Certificate authority. Ed25519 certificate issuance, renewal, and revocation for agent and harness identity. Security-sensitive, benefits from isolation. |

`apps/hub` is the entry point that wires the hub packages together and starts the server.

### Why separate hub packages

The splits are driven by operational concerns, not portability. `hub-credentials` runs background workers on timers. `hub-sessions` holds long-lived WebSocket connections. `hub-ca` manages private key material. These have different process models, security boundaries, and scaling characteristics than stateless route handlers. Whether they run as separate services or as modules within one process is a deployment decision, but the package boundaries make either option available.

## Agent Runtime

Distributed across many environments: containers, VMs, local processes, Cloudflare Workers, browsers, mobile devices, embedded systems. Packages are split along two axes: portable core logic that runs everywhere, and environment-specific implementations that provide I/O capabilities.

### Portable Core

Uses `fetch` and `ReadableStream` exclusively. No Node APIs, no filesystem assumptions, no native bindings. Ships once, works everywhere.

| Package                       | Purpose                                                                                                                                                                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `@interchange/inference`      | Provider adapters (Anthropic, OpenAI-compatible), streaming harness, reactor, director system, context management, compaction, error classification, token accounting, cross-provider message transformation, test provider. The reasoning engine. |
| `@interchange/wallet`         | Payment tool definitions (`wallet.pay`, `wallet.request_payment`, etc.), spending policy enforcement, payment backend plugin interface. Policy logic is arithmetic and pattern matching.                                                           |
| `@interchange/harness`        | Agent lifecycle, event routing, tool dispatch, content safety, session channel logic. Composes inference, wallet, and authz with environment-specific implementations injected at startup.                                                         |
| `@interchange/mime`           | MIME message construction, multipart assembly, PGP detached signature generation, RFC 2822 formatting, and MIME parsing. Used by message transports.                                                                                               |
| `@interchange/pack-transport` | Git pack protocol chunking and reassembly. Transfers git object data between the hub and sidecars over WebSocket or HTTP.                                                                                                                          |

### Environment-Specific Implementations

Each package implements an interface defined in `@interchange/types`. The harness accepts any conforming implementation. Different environments compose different implementations.

**Storage** (implements `ContextStore`, `ChangeHistory`):

| Package                       | Environment                       | Implementation                                                                                 |
| ----------------------------- | --------------------------------- | ---------------------------------------------------------------------------------------------- |
| `@interchange/storage-git`    | Server (Node/Bun with filesystem) | Native git. Shell out to `git` or libgit2 bindings.                                            |
| `@interchange/storage-isogit` | Constrained (Worker/browser)      | isomorphic-git with object storage (R2/S3/IndexedDB) for persistence. Pure JS, no native deps. |

**Cryptographic Identity** (implements `CryptoProvider`):

| Package                    | Environment | Implementation                                                                                      |
| -------------------------- | ----------- | --------------------------------------------------------------------------------------------------- |
| `@interchange/crypto-node` | Server      | Node `crypto` module. Ed25519 key generation, signing, verification. SSH/PGP/X.509 format handling. |
| `@interchange/crypto-web`  | Constrained | WebCrypto API (`subtle.crypto`). Same operations, different primitives.                             |

**Message Transport** (implements `MessageTransport`):

| Package                       | Environment | Implementation                                                                                                   |
| ----------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------- |
| `@interchange/message-smtp`   | Server      | SMTP/IMAP client over TCP. TLS, MIME parsing, SPF/DKIM/DMARC. Requires raw socket access.                        |
| `@interchange/message-http`   | Constrained | HTTP relay through the hub. For environments that cannot open TCP sockets.                                       |
| `@interchange/message-memory` | Testing     | In-memory message transport for single-process and test environments. Agents register and exchange mail locally. |

**Payment Backends** (implements `WalletBackend`):

| Package                         | Environment            | Implementation                                                                    |
| ------------------------------- | ---------------------- | --------------------------------------------------------------------------------- |
| `@interchange/wallet-faremeter` | Portable (fetch-based) | Faremeter client library for outbound payments, Faremeter middleware for inbound. |

**Tool Sets**:

| Package                    | Environment | What it provides                                                                                 |
| -------------------------- | ----------- | ------------------------------------------------------------------------------------------------ |
| `@interchange/tools-posix` | Server      | File read/write/edit, shell execution, git operations. Requires filesystem and process spawning. |
| `@interchange/tools-http`  | Portable    | Web search, API calls, remote tool proxy. fetch-only.                                            |

### Distribution Bundles

Convenience packages that compose a harness with the right implementations for a target environment. One install instead of picking individual packages.

| Package                       | Composition                                                                          |
| ----------------------------- | ------------------------------------------------------------------------------------ |
| `@interchange/harness-server` | harness + storage-git + crypto-node + message-smtp + tools-posix + wallet-faremeter  |
| `@interchange/harness-worker` | harness + storage-isogit + crypto-web + message-http + tools-http + wallet-faremeter |

### Why the splits exist

A package that pulls in native git bindings has no business shipping to a Cloudflare Worker. A package that bundles isomorphic-git has no business shipping to a server with native git available. The environment splits exist because the dependency trees are genuinely different — different native modules, different APIs, different capabilities. These are not configuration differences. They are different code.

The portable core packages (`inference`, `wallet`, `harness`) never import environment-specific code. They operate through interfaces defined in `@interchange/types`. The harness is wired at startup:

```typescript
import { createHarness } from "@interchange/harness";
import { FilesystemStore } from "@interchange/storage-git";
import { NodeCrypto } from "@interchange/crypto-node";
import { SMTPTransport } from "@interchange/message-smtp";

const harness = createHarness({
  storage: new FilesystemStore({ path: "/agent-data" }),
  crypto: new NodeCrypto(),
  message: new SMTPTransport({ host: "mail.example.com" }),
});
```

## Dependency Rules

1. Agent runtime packages never depend on `hub-*` packages.
2. `hub-*` packages never depend on agent runtime packages.
3. Both trees depend on the shared foundation (`types`, `authz`, `log`).
4. Environment-specific packages depend on `types` for interfaces, never on each other.
5. The harness depends on `types` for interfaces, `inference` and `wallet` for portable logic, and `authz` for grant evaluation. It does not depend on any environment-specific package.
6. Distribution bundles depend on the harness and a specific set of environment packages. They contain no logic of their own.
