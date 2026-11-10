# Agent Harness Design - Implementation

## Overview

The sidecar manages agent workloads on behalf of the hub. Each deployment runs in its own supervised workflow child process: the deploy router in `@intx/workflow-host` creates one `WorkflowSupervisor` per deployment, which spawns an isolated `bin/workflow-child` OS process, and each agent step runs in-process inside that child via `@intx/agent`. Each workload has an isogit repository for persistent storage and an Ed25519 key pair for identity.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Hub                                       │
│  - Agent definitions, credentials                                │
│  - Session management, message persistence                       │
│  - Harness registration and lifecycle management                 │
│  - Token-authenticated sidecar WebSocket handler                  │
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
│  - Hosts every probe and allocation placed on it                 │
└─────────────────────────────────────────────────────────────────┘
```

## Sidecar Package Structure

The sidecar app is a thin wiring layer that composes building blocks
out of `@intx/hub-agent`, `@intx/workflow-host`, and `@intx/agent`.
The per-deployment disk layout and the hub WebSocket protocol live in
`@intx/hub-agent`, which takes the deploy router as an injected
factory. `@intx/workflow-host` owns that router, the per-deployment
`WorkflowSupervisor`, the child substrate factory, the probe protocol,
durable agent step state (`conversation-state.ts`), and step tool
attachment (`child/step-tools.ts`). The in-process agent runtime
(`createAgent(def, env)` wrapping the reactor exactly once) lives in
`@intx/agent` and runs inside that child. The sidecar draws runtime
capabilities from `@intx/harness`. The app supplies the concrete
crypto / tool / storage / authz plugins, the Bun spawners, and the
closure-materialization binding.

"Thin" describes the app's layering, not its file count: the app owns every
binding that names a concrete dependency, so the wiring surface is wide even
though the reusable machinery lives in the packages above.

```
apps/sidecar/
├── bin/
│   ├── workflow-child        # Workflow-process child binary; the only reader of process.env on that path
│   └── workflow-probe-child  # Airlocked one-shot probe child binary
├── src/
│   ├── index.ts                           # Entry point: wires the stores, the harness builder, and the hub link
│   ├── config.ts                          # Boundary readers for the sidecar's env-config inputs
│   ├── default-harness.ts                 # HarnessBuilder source-admission seam (canBuildSource) the deploy router consults before spawning
│   ├── sidecar-materialization-config.ts  # Shared registry-map and host-platform resolution for closure materialization
│   ├── signing-keypair.ts                 # Loads or mints the sidecar's on-disk Ed25519 signing keypair
│   ├── workflow-child-bindings.ts         # Closes the child factory over the grant cap
│   ├── workflow-child-spawner.ts          # Bun spawner and resolved path for bin/workflow-child
│   └── workflow-probe-spawner.ts          # Bun spawner and resolved path for bin/workflow-probe-child
├── package.json
├── README.md
└── tsconfig.json
```

Tests are co-located as `*.test.ts` beside each module and are omitted above.

## Hub ↔ Sidecar Communication

All communication between hub and sidecar is over a single persistent WebSocket connection. The provisioner supplies the full Hub WebSocket URL, normally `ws://<hub>/api/sidecars/ws`, and the sidecar connects outbound to it. There are no REST endpoints on the sidecar.

### Deployment Frames

**Hub to Sidecar:**

| Frame            | Fields                                                                        | Description                             |
| ---------------- | ----------------------------------------------------------------------------- | --------------------------------------- |
| `agent.deploy`   | `requestId`, `agentAddress`, `generation`, `config` (`sessionId`), `workflow` | Deploy an incarnation to this sidecar   |
| `agent.undeploy` | `requestId`, `agentAddress`, `generation`, `reason`                           | Remove an incarnation from this sidecar |

**Sidecar to Hub:**

| Frame                  | Fields                                                 | Description                                  |
| ---------------------- | ------------------------------------------------------ | -------------------------------------------- |
| `agent.deploy.ack`     | `requestId`, `agentAddress`, `generation`, `publicKey` | Incarnation deployed, here is its public key |
| `agent.deploy.error`   | `requestId`, `agentAddress`, `generation`, `error`     | Deployment failed                            |
| `agent.undeploy.ack`   | `requestId`, `agentAddress`, `generation`              | Incarnation removed, or it was never here    |
| `agent.undeploy.error` | `requestId`, `agentAddress`, `generation`, `error`     | Teardown failed                              |

An incarnation is a deployment address plus the allocation generation it was deployed for. Every reply echoes the `requestId` and `generation` of the request it answers. A sidecar holds at most one incarnation of an address, and an address is deployed once: a deploy of an address it holds is refused. An undeploy of a generation older than the one it holds is acknowledged without touching it.

Deploy errors carry `error: { code, message }`. `capacity_full` confirms that
the request was refused before accepting or starting a deployment;
`deployment_failed` leaves the outcome uncertain. The Hub uses the code for
that distinction and the message for diagnostics.

When the Hub sends `agent.deploy`, the sidecar spawns a supervised **workflow-process child** to host the deployment and responds with `agent.deploy.ack`. The sidecar returns the supervisor public key. The Hub publishes that key only after initialization completes under the current allocation lock; reconnect authority remains the sidecar credential, not the projected public key. Before the child is spawned, inputs a restart cannot otherwise recover are written to a per-deployment record.

When the hub sends `agent.undeploy`, the sidecar shuts the deployment's supervisor down (killing the workflow-process child and releasing its IPC pipes and event-channel handle), unregisters the deployment address from the transport and from the mail/signal/drain routers, reclaims the deployment's per-step scratch, removes its workflow-run repository, and deletes the `deployment.json` record so a later boot does not re-spawn a torn-down deployment. It marks the record `tearing-down` first and deletes it only once every other step succeeded, so a crash mid-undeploy or a step that fails leaves the deployment reported as tearing down, and the Hub retries removal while the allocation's cleanup policy permits it. A cleanup disconnect timeout resumes after reconnect; exhausted retries or a permanent cleanup rejection require operator recovery and keep a cleanup-only binding for inventory. These terminal failures do not trigger another undeploy on hello. A copy stops occupying an active slot once removal or retained storage is confirmed, even if the provider obligation remains failed. It also deletes the deployment's agent directory, key pair included, and its local conversation copy, which carries a conversation only across child respawns and sidecar restarts of the deployment that wrote it.

An `InferenceSource` names its credential by `credentialId`. The secrets travel separately: in the `agent.deploy` frame's `workflow.credentials`, and later in `credentials.update` frames.

## Per-Agent Key Pairs

Each agent has its own Ed25519 key pair, generated when the agent is first deployed to the sidecar and stored in the agent's data directory. The key pair persists across sidecar restarts. The public key is transmitted to the Hub in the initial `agent.deploy.ack` frame for the deployment's published identity and signed-content provenance. Sidecar credentials, not agent keys, authorize reconnect routing.

Keys are stored as raw 32-byte binary files under a `keys/` directory within the agent's data directory.

Directory layout under `SIDECAR_DATA_DIR`:

```
SIDECAR_DATA_DIR/
  .sidecar-signing/              # the sidecar's own signing keypair
  <sanitized-agent-address>/     # per-agent key custody
    keys/
      id_ed25519                 # agent private key (raw 32 bytes, mode 0600)
      id_ed25519.pub             # agent public key (raw 32 bytes)
  workflow-runs/
    <runId>/                     # workflow-run substrate for one run
      deployment.json            # per-run restore record (mode 0600); see below
  workflow-step-state/
    <runId>/                     # per-step scratch and working stores, reclaimed on undeploy
  agent-conversation-state/
    <runId>/                     # local conversation copy, reclaimed on undeploy
  workflow-definition-sources/
    <runId>/
      source-assets/<assetId>/   # mounted tarball asset
  workflow-definition-source-gits/
    <runId>/                     # retained `.git` of each source-format asset
  workflow-definition-closures/
    <runId>/                     # laid-out closure the workflow child imports from
  workflow-definition-closure-cache/  # tarball cache the deploy closure applies share
  cache/tarballs/                # the probe's tarball cache (`SIDECAR_CACHE_DIR`)
  workflow-probe/closures/       # per-probe scratch
```

The per-agent key directory is keyed by the sanitized run address; the workflow subtrees are keyed by the derived run id.

Nothing creates or reads `agents/` under `SIDECAR_DATA_DIR` or `HUB_DATA_DIR`. Earlier versions kept agent-state repositories there; one left behind is safe to delete.

The `deployment.json` record stores only what a restart cannot otherwise recover: the deployment's `agentAddress` and the allocation `generation` it was deployed for, the `definitionId` naming its workflow definition, the `sourceRef` pin a restore re-materializes the closure from, the sealed credential cell, each step's ordered inference-**sources** failover chain (`sources`), and the optional inference `sessionId`. A `version` field guards the schema so a stale record can be rejected rather than parsed blindly. A deployment that no longer runs keeps its record until the Hub undeploys it, marked `stopped`, with the error that ended it unless the Hub stopped it, or `tearing-down`, so a restart reports it instead of spawning it. Its optional `retention` marker records whether keeping the copy was accepted (`kept`) or refused (`refused`); only durably kept copies leave the active capacity pool. The record deliberately does **not** duplicate the workflow definition (re-materialized from its `sourceRef` closure at restore). The record is written owner-only (mode 0600).

A live source rotation for a single-step deployment overwrites this record's `sources` before it takes effect. Persistence is what makes a rotation durable: a rotation whose write fails is not durable, and the deployment falls back to the last durably-recorded source list on the next recycle or restart.

The directory name is the run address with `@` replaced by `_at_` and non-alphanumeric characters (except `-` and `_`) replaced by `_`.

## Agent Deployment vs User Sessions

The sidecar manages agents, not user sessions. When the hub deploys an agent to a sidecar, the sidecar spawns a supervised **workflow-process child** for that deployment. The child runs continuously, receiving messages from any source — other agents, users, system signals — and builds the agent harness inside its own process. User sessions are a hub-side concept: the hub tracks which users are interacting with which agents and routes user messages to the agent's address accordingly, but the sidecar does not know or care about individual user sessions.

The hub maintains a sidecar-to-agent mapping in its database. This mapping determines where to route messages for a given run address. When a sidecar disconnects, the hub knows which agents are affected: mail sent to them comes back undelivered, while triggers and signals wait in dispatch rows until the sidecar reconnects.

### Connector threads and user sessions

The connector is **one durable thread per agent**. Anyone who sends conversational mail to the agent — a human via a hub session, a parent agent that launched this one as a sub-agent, a peer agent that initiates a conversation — joins the thread by stamping threading headers the harness recognizes. Participants accumulate on an anchored thread; no one is displaced. An anchored thread persists for the lifetime of the agent.

The connector router classifies each inbound message as:

- **`start`** when no thread is active, or when the active thread holds no anchor. The sender becomes the first participant; the message-id becomes `threadRoot` and `lastMessageId`; the subject is recorded and preserved for the life of the thread. A message that named no message-id still starts a thread, because it still names a reply address, but that thread opens with neither `threadRoot` nor `lastMessageId`. Nothing can reference a message that named no id, so such a thread is anchorless and no arrival continues it. The next arrival that names a reply address re-anchors it as a fresh `start`, which replaces `replyTo` and empties `cc`; an arrival naming no reply address is the only one that still routes as `passthrough`. The agent replying is the other way out: its own reply-id becomes `lastMessageId`, which anchors the thread without displacing anyone on it.
- **`continue`** when the message's `references` includes the active `threadRoot`, or its `inReplyTo` equals the active `lastMessageId`. The sender is added to the participant set; the previous most-recent speaker moves into `cc` (deduplicated against re-entry).
- **`passthrough`** for everything else — mail carrying no threading headers the active anchored thread recognizes, and mail carrying no `From` header, which names nobody to reply to and so has no thread to start or continue. The reactor still sees it, but the harness leaves it in the INBOX and the connector state is untouched. In a warm agent turn, the agent's reply to a `passthrough` message goes to that message's sender alone, with `In-Reply-To` set to its message-id and `References` set to its own `References` plus that id. Composing the outbound message drops each `References` entry that is not a valid message id. That reply does not change the connector state. A `passthrough` message that names no sender, or whose sender the router cannot parse, gets no reply in a warm agent turn: the reply drain drops the reply and logs it. The router holds this per-turn reply target in memory. The next `start` or `continue` clears it, and so does the start of the next warm agent turn.

A warm agent turn that parks on an approval gate keeps its reply target. The durable conversation store files the target under the gate's correlation id and persists it with the conversation, so the target survives a child respawn. The approval resume takes the target back, and its reply goes to the same sender, or nowhere when that sender had no address. A resume of a turn that had no reply target replies on the thread. `createHarness` sets no reply target: its reply to a `passthrough` message goes to the connector thread, as any other reply does. It routes a message when it delivers it, not when the agent starts the turn for it, so it cannot bind a per-message target to the turn that answers that message. A `passthrough` message that arrived during another sender's turn would take that turn's reply.

The router does not read `Interchange-Type`. A structured payload that names a `From` starts or continues the connector thread exactly as a conversation message does, so its sender can become `replyTo` and stay on `cc`. Only conversational mail should join the thread. The router cannot enforce that until a `passthrough` message has something that removes it from the INBOX, because the `passthrough` arm does not consume.

Connector state has four parts: `threadRoot` (the first message's id), `lastMessageId` (the most recent message on the thread in either direction; a `passthrough` message does not change it, and neither does a warm agent's reply to one), `replyTo` (the most recent speaker — the primary recipient on the next outbound reply), and `cc` (every other participant who has spoken on the thread, deduplicated, in arrival order). `threadRoot` and `lastMessageId` are each present only when the message that set them named an id; `replyTo` is always present, because a message that names no reply address starts no thread. When the reactor emits `connector.reply` in a turn with no `passthrough` reply target, the outbound mail is addressed to `replyTo` with `cc` carrying everyone else — whoever spoke most recently gets the direct reply and the rest stay in the loop. The reply carries `In-Reply-To` whenever the thread holds an identified ancestor — `lastMessageId`, or `threadRoot` when the most recent message named no id. With neither present the header is omitted, since RFC 5322 defines it as one or more message ids and admits no empty one.

Mail a hub user sends to a run carries no threading headers (`packages/hub-api/src/workflow-run-trigger.ts`).

The bootstrap restore happens **only on the first `wrappedStore.load()`**. Subsequent loads return the store's payload but do not restore from disk; once the router emits its first state change, the harness flips an `inMemoryStateAuthoritative` bit and refuses to clobber in-memory state with a stale disk value. This closes a race where the reactor's startup `load()` lands on the same microtask boundary as the watch callback's `commit()`: without the dirty bit, the second load would reset the router's freshly committed thread state to disk's null and the next `connector.reply` would fail to compose.

## Registration and Reconnection

The Hub mints a sidecar identity for each probe or allocation generation and hands it to the provisioner, which either starts capacity that authenticates with it or places the work on a sidecar it already runs. The bearer token resolves to that sidecar and every workflow probe and allocation generation it currently hosts, including cleanup bindings; there is no ambient registration pool. A sidecar hosting only probes has no incarnation to report.

Possession of the raw token is sufficient to authenticate as that sidecar, for every probe and allocation it currently hosts, including cleanup bindings. Provisioners and workers must never log it. A provisioner that must persist the token for restartable capacity must use access-controlled secret storage and keep it with any worker state that can be restarted, deleting it only when the worker and its restorable state are permanently removed. Connections crossing a non-loopback or otherwise untrusted transport must use `wss://`; plaintext `ws://` is only appropriate for local loopback development.

Every connection opens with a `hello` frame reporting each incarnation the worker holds and what it is doing with it: `deploying`, `live`, `stopped`, or `tearing-down`. A `stopped` incarnation no longer runs and keeps its local state for inspection until the Hub undeploys it. One the Hub did not stop, because its workflow child ended itself or the worker could not restore it, carries the `error` that ended it, and the Hub fails its deployment. The worker also reports such a stop in a `deployment.stopped` frame when it happens and after each `welcome` until retention is decided, with the tips of the deployment's history branches, and the Hub fails the deployment once it holds that history, or after a bounded wait. The worker restores its deployments from storage before it first connects, so that `hello` already lists them. The Hub accepts the frame when the token resolves to current work or a cleanup obligation (a release, a replacement, or a failed cleanup); a cleanup-only connection receives no routes. It routes a reported incarnation only when it is live, its generation is the current generation of an allocation the sidecar hosts, that generation has finished initializing, and its run has not ended. It keeps current stopped copies and initialized terminal copies unrouted. After welcome it stops terminal live copies, requesting retention only while their policy allows it. Durable kept copies use the retained pool. Cleanup bindings leave removal to allocation reconciliation; the Hub sends `agent.undeploy` for other unwanted incarnations instead of closing the socket, so one stale deployment cannot disconnect the sidecar's other work, and then answers `welcome` with the incarnations it routes.

The sidecar defaults to 128 active reservations and 256 kept records, configured at startup with `SIDECAR_MAX_ACTIVE_DEPLOYMENTS` and `SIDECAR_MAX_RETAINED_DEPLOYMENTS`. It advertises those limits and reports both pools in full; the Hub uses the advertised active limit for placement. The combined protocol ceiling is 2,048 records, and the complete `hello` must fit the 64 MiB WebSocket frame-size limit. Hello errors use the same 4,096-character limit as individual stop reports. Restoration exceeding either configured limit fails startup; it does not omit records from `hello`.

The worker sends nothing the Hub must receive before `welcome`. It queues the reports it owes (outbound mail and signal correlation registrations), drops best-effort events, and holds back workflow-run pack pushes. On `welcome` it sends the queue and re-drives each routed incarnation's pending pack and parked correlations, so nothing it re-sends can overtake route restoration. The Hub answers every `hello`: with `welcome`, or by closing the socket when the frame is invalid or registration fails before the welcome. A worker that gets no `welcome` within 30 seconds reconnects. A worker whose token is valid but that hosts neither current work nor cleanup obligations gets a `welcome` with nothing routed after the Hub sends `agent.undeploy` for everything it reports, and stays connected, idle, until its provisioner places new work on it or stops it. Trigger and signal durability lives in `workflow_run_dispatch`; the Hub does not maintain an unscoped, in-memory queue for arbitrary disconnected sidecars.

| Direction     | Frame                | Fields                                                                     | Description                                                     |
| ------------- | -------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------------- |
| Sidecar → Hub | `hello`              | `sidecarId`, `token`, `capacity`, `incarnations`, `cachedSenderAddresses?` | Authenticate and report every incarnation the sidecar holds     |
| Hub → Sidecar | `welcome`            | `routed`                                                                   | The incarnations the Hub routes on this connection              |
| Sidecar → Hub | `deployment.stopped` | `agentAddress`, `generation`, `error`, `refTips?`                          | Report a deployment that stopped though the Hub did not stop it |

## Self-Restoration

At boot, the sidecar validates its stored deployment records and restores runnable copies through the normal supervised spawn path, eight at a time, before connecting. Retained, stopped and tearing-down copies stay unspawned. Failed restores are recorded as stopped with their error until the Hub undeploys them. Storage survival depends on the provisioner's advertised isolation and recovery guarantees.

If all `N` respawns use their full ready timeout, their waits take roughly `ceil(N / 8) × readyTimeout`: 8 minutes for the default 128 active deployments, or 64 minutes for 1,024, at the default 30 seconds. Allow additional startup time when setting [`maxDisconnected`](workflow-lifecycle-policy.md), especially when raising `SIDECAR_MAX_ACTIVE_DEPLOYMENTS`; capacity changes do not extend the deadline.

## Authority Model

The sidecar's isogit repository is the source of truth for agent inference context (conversation history, pending operations, token usage). The Hub keeps triggers and signals in dispatch rows until they are delivered, and holds mail the sidecar had not acknowledged for a limited time, redelivering it on reconnect. The sidecar incorporates delivered messages into the agent's context via the normal message handling path.

## Security Model

Credential secrets travel in the `agent.deploy` frame's `workflow.credentials` and in `credentials.update` frames, and are held in memory by the running deployment. The `deployment.json` record also keeps them, sealed with `SIDECAR_CREDENTIAL_ENCRYPTION_KEY`, so the sidecar can restore a deployment on restart without re-receiving them from the hub. The record is written owner-only (mode 0600).

## Key Rotation

Key rotation is not yet implemented. The architecture supports it: the sidecar would send a `key.rotated` frame with the new public key, and the hub would accept both old and new keys during a grace period. This is deferred until there is a concrete need.

## Failure Paths

If a reconnecting sidecar's token resolves to no current probe, allocation, or cleanup binding, the Hub asks it to undeploy every incarnation it reports, then closes the socket and leaves that capacity unroutable. A reported incarnation the Hub neither routes nor keeps unrouted is undeployed instead, and the sidecar's other work stays connected. Cleanup bindings keep the connection open without workflow routes, and their copies are left to the reconciler's cleanup policy. The provisioner and allocation reconciler own recovery; the worker cannot mint a new identity or claim another address.

If a deployment the sidecar restores has no key pair on disk (for example, keys were deleted), the sidecar generates a new one without warning. The new key does not match the public key the Hub recorded at deploy, so the restored deployment loses signed-content continuity.

## Mail and Event Flow

Mail is the first-class communication primitive. The sidecar persists outbound mail from agents via `mail.outbound` frames sent to the hub. The hub persists inbound mail sent by users via `POST .../workflows/runs/:runId/mail` and dispatches it to the sidecar as a `mail.delivered` agent event.

The composition-layer harness exposes the agent's reactor event stream as `harness.stream()`, an `AsyncIterable<ReactorEmittedEvent>`. The workflow child sends those events to the supervisor over the event channel, and the supervisor forwards them to the hub session channel. The stream carries inference activity, tool execution, reactor lifecycle, and fork events. `message.received` is a `ReactorInboundEvent` — it is delivered directly to the reactor director and is not forwarded to session channel subscribers. This keeps the external event stream focused on observable inference activity rather than internal routing signals.

Inference traces are stored separately from mail. The hub records one `inference_turn` per inference cycle and one or more `turn_part` rows per turn. The `/turns` endpoint serves these to UI clients independently of the `/mail` endpoint.

## Prototype Scope

This document describes the current prototype implementation. It diverges from the production architecture described in ARCHITECTURE.md in several ways: it uses WebSocket for hub-sidecar communication instead of SMTP/IMAP, uses SSE for user-facing event streaming instead of WebSocket session channels, and uses a simplified credential model where credentials travel in deploy frames rather than through a separate credential management channel.
