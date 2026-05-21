# Faremeter Interchange

_Inference_

The inference package (`@intx/inference`) is the provider-agnostic LLM layer that powers all agent reasoning in Interchange. It handles streaming, tool execution, context management, and token accounting across multiple model providers without depending on any provider's SDK.

## Design Principles

**No upstream SDKs.** Every provider is just HTTP POST + SSE. The package uses `fetch` and `ReadableStream` exclusively, keeping it runtime-agnostic (Bun, Node, browser) and free of SDK bloat.

**Errors are data, not exceptions.** Errors flow through the event stream as typed events. Consumers always see a complete event sequence (start through done or error), even during failures. This makes streaming UI and multiplayer fanout reliable.

**One event protocol.** The inference event stream is the session channel protocol. Events emitted by the inference layer are the same events that flow to session channel subscribers. There is no internal format that gets translated into an external format. One protocol, end to end.

**Event-driven, not request-driven.** The agent reactor processes events from multiple sources — humans, other agents, the system, tool completions — and asks the director what to do next. It can suspend for external reasons (approval gates, payment, credential refresh) and resume from where it left off. Suspension is a first-class concept, not an error.

**Pluggable by default.** The reactor, tool execution, context management, and compaction are all director-driven. The inference package provides the machinery; consumers provide the policy. But the reactor enforces safety invariants regardless of director behavior — it validates actions, catches director exceptions, and prevents resource leaks.

## Providers

The inference layer supports multiple providers through thin adapters built on a shared streaming harness. Each adapter provides two functions: build a request from the internal format, and parse the provider's SSE events into internal events. The harness handles connection management, SSE parsing, partial state accumulation, abort propagation, and error reporting.

This inverts the typical approach where each provider implements the full streaming lifecycle independently. The harness eliminates the structural duplication that plagues multi-provider libraries, where every adapter copies the same try/catch/cleanup/emit boilerplate with minor variations.

**Day-one providers:**

- **Anthropic** — Messages API with streaming, extended thinking, prompt caching
- **OpenAI-compatible** — Covers OpenAI, OpenRouter, OpenCode Go/Zen, and self-hosted endpoints (Ollama, vLLM)

### Provider Registry

Providers register as adapter pairs (request builder + response parser). Registration is a map keyed by provider identifier. Custom providers register the same way built-in providers do. No lazy loading ceremony, no class hierarchy.

### Capability Detection

Not all OpenAI-compatible endpoints support the same features. "Speaks the OpenAI protocol" is not a single capability — it's a family of overlapping feature sets.

The adapter detects per-endpoint:

- Whether the `developer` role is supported (reasoning models only)
- Whether `reasoning_effort` is accepted
- Which `max_tokens` field name the endpoint expects (`max_tokens` vs `max_completion_tokens`)
- What thinking/reasoning format the endpoint uses (at least five known variants)
- Whether the endpoint supports response caching or the `store` parameter

Detection uses a combination of base URL pattern matching and explicit configuration. When detection is ambiguous, the adapter fails closed (omits unsupported parameters) rather than sending them and hoping.

### Streaming Harness

The harness is the shared infrastructure that all provider adapters build on:

1. Opens an HTTP connection with the adapter's built request
2. Parses the SSE byte stream into `data:` lines, handling `[DONE]` sentinels
3. Passes each parsed event to the adapter's response parser
4. Accumulates partial message state from parser output
5. Emits events on the common event protocol
6. Checks AbortSignal between chunks
7. On error: classifies, emits error event, cleans up the connection
8. On completion: emits terminal event with final message and usage

Provider adapters never touch SSE parsing, connection lifecycle, abort handling, or event emission. They translate request/response shapes. The harness does everything else.

## Event Protocol

All inference activity — streaming tokens, tool execution, reactor state changes — emits events on a single protocol. This protocol is what session channel subscribers receive. There is no internal event format.

### Event Types

```
inference.start           — Model call begins (carries model ID)
inference.thinking.delta  — Reasoning token
inference.text.delta      — Output token
inference.tool_call.start — Tool call detected (name, partial args)
inference.tool_call.delta — Tool call argument fragment
inference.tool_call.end   — Tool call complete (full args)
inference.usage           — Token accounting for this call (fires before inference.done)
inference.done            — Model call completed normally (carries final message and usage summary)
inference.error           — Model call failed (carries error classification and partial message)

tool.start                — Tool execution begins
tool.update               — Partial tool output (streaming tools)
tool.done                 — Tool execution completed

message.received          — Inbound message arrived (reactor-internal; not forwarded to session channel subscribers)
message.queued            — Inbound message queued for later processing
message.correlated        — Inbound message matched to pending outbound

connector.reply           — Agent reply sent to the conversation (turn complete)

reactor.start             — Reactor initialized
reactor.gate.blocked      — Reactor suspended (reason, gate type)
reactor.gate.cleared      — Suspension resolved, reactor resuming
reactor.done              — Reactor completed normally
reactor.error             — Reactor failed

fork.created              — New fork spawned (mode, fork ID, parent ID)
fork.done                 — Fork completed (with result if child mode)
fork.error                — Fork failed
fork.aborted              — Fork was aborted
```

### Usage Event Ordering

`inference.usage` fires before `inference.done`. This allows subscribers to process token accounting before the terminal event. `inference.done` also carries a final usage summary for convenience, so clients that only care about totals can ignore `inference.usage` and read usage from `inference.done`.

### Partial State

Every `inference.*` event carries a `partial` field: the full assistant message accumulated so far. Subscribers that connect mid-stream receive current state immediately without replaying deltas.

This is critical for multiplayer: multiple clients watching the same agent conversation see consistent state regardless of when they connect. Late joiners get the partial message, not a gap.

### Sequence Numbers

Every event carries a monotonic sequence number scoped to the session. The sequence is globally monotonic across all event types within a session — inference events, tool events, reactor events, and message events all share the same counter. This enables:

- Client-side gap detection (missed events during brief disconnection)
- Deduplication when the same content arrives via both session channel and message bus
- Ordered replay from a buffer for late-joining subscribers
- Total ordering of all session activity for audit

Fork events carry both a session-scoped sequence number and a fork ID, so subscribers can route events to the correct conversation view.

## Message Format

The internal message format is provider-agnostic. Messages are converted to and from provider wire formats at the adapter boundary.

### Content Types

- **Text** — Plain text output
- **Thinking** — Reasoning content with optional redaction and opaque signatures for multi-turn continuity
- **Image** — Base64-encoded image data with MIME type
- **Tool call** — Function invocation with name, ID, and arguments
- **Tool result** — Execution result with content blocks, optional detail data, and error flag

### Cross-Provider Message Transformation

When conversations cross provider boundaries (model switch, agent handoff, session replay to a different provider), the message history must be transformed. This is a core architectural layer, not a utility, because interchange agents routinely cross provider boundaries — different tenants use different providers, model selection changes mid-conversation based on cost or availability, and agent-to-agent handoffs may involve different backends.

Transformations:

- **Tool call ID normalization** — Provider ID formats vary (OpenAI Responses API generates 450+ character IDs with pipes; Anthropic has strict format requirements). IDs are normalized to a portable format with a bidirectional mapping for round-trip fidelity.
- **Thinking block handling** — Encrypted/redacted reasoning blocks are valid only for the originating model. Stripped when replaying to a different provider.
- **Thinking signature preservation** — Opaque signatures for multi-turn reasoning continuity are kept for same-model, dropped for cross-model.
- **Orphaned tool call recovery** — Interrupted conversations show tool calls without results. Synthetic error results are injected so the target model sees a complete tool sequence.
- **Incomplete turn filtering** — Error/aborted assistant messages are filtered during replay to prevent "reasoning without output" errors on the target model.

Transformation runs automatically when the target model differs from a message's originating model. The originating model is tracked per-message, not per-conversation, because model switches can happen mid-conversation.

## Agent Reactor

The inference package provides an event-driven reactor, not a request-response loop. An interchange agent is a long-lived entity that receives messages from multiple sources, reasons about them, and acts. The reactor processes events as they arrive and asks the director what to do next.

### Terminology

This document uses "context" in three distinct senses:

- **Message history** — The array of messages (user, assistant, tool results, system) that form the conversation. This is what gets sent to the model and stored in git.
- **Reactor state** — The full state visible to the director: message history plus active forks, pending gates, async operations, and token accounting. This is the `state` parameter to the director decision function.
- **Context store** — The git-backed persistent store that holds the message history and reactor metadata. This is the storage layer.

When the distinction matters, this document uses the specific term. When it doesn't, "context" refers to the message history.

### Relationship to the Harness

The harness (described in ARCHITECTURE.md) is the agent's runtime. The inference package is a library the harness uses. The harness:

- Instantiates the reactor with a director, context store, provider configuration, and director policy
- Delivers inbound messages from the message bus to the reactor
- Routes reactor events to session channel subscribers
- Manages the agent's credential lifecycle (the reactor requests credentials via gates; the harness fulfills them)
- Handles agent lifecycle (startup, shutdown, health checks) — the reactor handles inference lifecycle

The reactor does not own the message bus, the session channel, or the credential store. It owns the inference loop, tool execution, context management, and event emission. The harness is the integration layer.

### Why Not a Loop

A traditional agent loop assumes one input source (the user), one conversation at a time, and synchronous tool execution. Interchange agents face a fundamentally different environment:

- A **collaborative agent** sends a message to Agent X, continues working on something else, and processes Agent X's response whenever it arrives — potentially mid-task.
- A **customer-facing bot** handles User A's conversation when User B sends a message. That's a separate conversation, not a follow-up.
- A **background worker** has no "conversation" to loop through. It sits idle and reacts to inbound work.
- A **coding assistant** works like a traditional loop but may receive system messages (credential refresh, admin commands) at any time.

A loop that checks a queue "between turns" can't serve these use cases without fighting its own structure. The reactor handles all of them with one model.

### Reactor Structure

The reactor processes one event at a time and asks the director for the next action:

```
Event arrives → Director decides → Reactor executes → Next event
```

**Inbound events** (things that happen to the agent):

- `message.received` — A message arrived (from a human, another agent, the system)
- `inference.done` — A model call completed (with assistant message)
- `inference.error` — A model call failed (with error classification)
- `tool.done` — A tool execution completed (with result)
- `reactor.gate.cleared` — A suspension condition resolved (with gate result)
- `abort` — The reactor should shut down (with reason)

**Actions** (things the reactor can do, as directed by the director):

- `infer` — Call a model with a message history, model, and options
- `execute_tools` — Execute tool calls (sequential or parallel)
- `suspend` — Wait for a gate to clear (approval, payment, credential, custom)
- `fork` — Create a new reactor with copied context
- `emit` — Send a custom event to session channel subscribers (see below)
- `checkpoint` — Commit current state to the context store without suspending
- `wait` — Return to the event loop without shutting down (idle until next event)
- `done` — Reactor is finished (terminal, no further events processed)

The reactor is a thin dispatch layer. It doesn't decide what to do — the director does. The reactor executes actions reliably: manages the streaming harness for inference, dispatches tool calls, handles suspension mechanics, manages fork lifecycle.

### Event Ordering

Events from multiple sources can land in the reactor's queue simultaneously. During an `executeInfer` call, an inbound message may arrive (via `deliver`) before the inference completes and enqueues its `inference.done` event. Naive FIFO processing would hand the `message.received` event to the director first, which appends a user text message to the conversation history — but the preceding assistant message contains tool calls whose results have not been appended yet. The provider rejects the next inference call because the tool call / tool result pairing is broken.

The reactor enforces a priority invariant: when the last message in the conversation history is an assistant message with tool calls (meaning tool results have not yet been appended), inference-cycle events (`inference.done`, `inference.error`, `tool.done`) are dequeued before other events. This ensures the tool-call cycle always completes before inbound messages are interleaved into the history. Once tool results are in place, the reactor reverts to FIFO ordering so inbound messages are processed promptly.

`abort` events are always processed immediately regardless of cycle state.

### The `emit` Action

The director can emit custom events to session channel subscribers via the `emit` action. Custom events use a `custom.*` type namespace and carry arbitrary data. They receive sequence numbers like all other events. They are ephemeral — not persisted to the context store. Use cases: progress indicators, debug information, UI hints.

The director cannot emit events in the `inference.*`, `tool.*`, `reactor.*`, or `fork.*` namespaces. Those are reserved for the reactor.

### Director Decision Function

The director is a single function:

```
(event, state, capabilities) → action | action[]
```

The director receives:

- **event** — What just happened
- **state** — The current message history, active forks, pending gates, async operations, token accounting
- **capabilities** — What the reactor can do (infer, execute tools, fork, suspend, emit, checkpoint, compact, wait, done)

It returns one or more actions. Multiple actions execute concurrently where possible (parallel tool calls, fork + continue).

**If the director throws an exception**, the reactor catches it, emits `reactor.error` with the exception details, and initiates graceful shutdown. The director is user-provided code and must not be able to crash the reactor without a clean terminal event.

### Action Validation

The reactor validates the action set returned by the director before executing:

- **No conflicting actions** — At most one `infer` action. At most one `done`. `infer` + `done` is invalid.
- **Fork is composable** — `fork` can appear alongside `infer` or `execute_tools` (fork happens concurrently).
- **Multiple tool executions collapse** — Multiple `execute_tools` actions are merged into a single parallel batch.
- **Suspend is exclusive** — `suspend` cannot appear alongside `infer` or `execute_tools`. You either do work or you wait.
- **Checkpoint is composable** — At most one `checkpoint` per action set; it can appear alongside any other action (it fires before the other action executes). The checkpoint carries a `message` string used as the git commit message.
- **Compact selects a registered compactor** — The `compact` action carries a `compactor` name (resolved against the reactor's compactor registry) and a `reason` string recorded in the manifest. Composition rules with other actions are defined in [Context Transforms and Compactors](#context-transforms-and-compactors); the Phase 1 type surface declares the action but does not yet execute it.
- **Wait is exclusive** — `wait` returns to the event loop immediately. It cannot appear alongside `infer` or `execute_tools`.

Invalid action sets produce a `reactor.error` event with a diagnostic message. The reactor does not guess intent.

### Message Handling

Inbound messages arrive at the reactor regardless of current state. Correlated messages (responses to pending outbound requests) are matched and resolved by the reactor before the director sees them (see Correlation). Non-correlated messages are delivered to the director as `message.received` events. The director decides how to handle them:

**Queue** — Add to the message history for the next model call. The model sees the message when the current action completes. This is the simple case: a human sends a follow-up while the model is generating.

**Inject as steering** — Abort the current inference call, add the message to the history, and re-infer. The model sees the message immediately. Use case: an admin sends a priority redirect.

**Fork** — Create a new reactor instance to handle the message independently. The current work continues uninterrupted. Use case: a second user starts a conversation while the agent is already busy.

**Ignore** — Drop the message. Use case: duplicate delivery, irrelevant system notification.

### INBOX Consumption

The harness consumes messages from the agent's INBOX after delivering them to the reactor. Messages that belong to the active connector thread or that are responses to agent-initiated outbound sends are fetched, delivered via `reactor.deliver()`, and then flagged as deleted and expunged from the INBOX. Unsolicited messages (new threads, untracked threads) remain in the INBOX for the agent to discover via mail tools.

This means messages routed through the connector or matched as agent-initiated responses are not visible to `mail_search` after delivery. The message content enters the conversation history through the reactor's event processing, not through the INBOX. The model cannot poll for these messages — they arrive as `message.received` events and are appended to the history by the reactor when the event is processed.

### Correlation

Correlation connects outbound async tool calls to inbound responses. The reactor owns correlation matching — it is mechanical protocol behavior, not a policy decision.

**Registration.** When a tool returns a pending marker, the reactor registers the correlation ID in its async state. The pending marker may include additional matching criteria beyond the correlation ID; these are protocol-specific and opaque to the reactor's core. The reactor delegates validation of those criteria to a correlation validator provided at startup.

**Matching.** When an inbound event carries a correlation ID, the reactor checks it against registered correlations before delivering it to the director. The reactor calls the correlation validator with the registered state and the inbound event. The validator returns whether the match is authentic. If validation fails, the event is delivered to the director as a regular uncorrelated event.

For message correlation specifically, the validator enforces sender identity and cryptographic signature verification. See MESSAGE.md for the full security model.

**On match.** The reactor clears the corresponding gate, injects the response as a resolution into the message history, and emits a `message.correlated` event. The director sees the correlated event and decides the next action (infer, done, fork) but does not participate in the matching itself.

**Duplicate responses** (same correlation ID, already resolved) are delivered to the director as regular uncorrelated events. The reactor does not deduplicate — the director decides whether to process or ignore.

**Orphaned correlations** (pending operations whose responses never arrive) are cleaned up by gate timeouts.

### Forking

When the director returns a `fork` action, the reactor creates a new reactor instance. Two fork modes are supported:

**Independent** — The new reactor gets a full copy of the message history up to the fork point. After that, the two reactors share nothing. Each has its own message history, its own director state, its own lifecycle, its own token accounting. In git terms, this is a branch that diverges immediately. The harness manages both reactors. Use case: handling a completely separate conversation.

**Child** — The new reactor gets a copy of the message history and reports results back to the parent. The parent can wait for the child (`suspend` on a child-completion gate) or continue working. When the child completes, its result is delivered to the parent as a `fork.done` event. The child's token usage is tracked independently but the parent can query aggregate usage. In git terms, this is a branch that gets merged back. Use case: delegating a subtask to a cheaper model or a specialized tool set.

In both modes, the fork receives:

- A copy of the message history (not a reference — mutations are independent)
- A fresh token accounting counter (starting from zero)
- A fresh async state (no inherited pending operations)
- Director state initialized by the fork policy callback

Fork lifecycle is managed by the reactor. Forks emit their own events (tagged with the fork ID for session channel routing). Forks can be aborted independently. The director tracks active forks via the state object.

### Tool Execution Semantics

**All tools return synchronously from the reactor's perspective.** A tool execution is an async function that resolves with a result. The reactor never blocks on a tool that's waiting for an external event.

Tools that need to wait for something external (a response from another agent, a payment confirmation, human approval) follow the **pending marker pattern**:

1. The tool does its immediate work (sends the message, submits the payment request).
2. The tool returns immediately with a pending marker: `{ status: "pending", correlationId: "abc123" }`.
3. The reactor registers the correlation ID in its async state.
4. The `tool.done` event fires with the pending result.
5. The director sees the pending marker and decides what to do.

The director has several options when it sees a pending result:

**Suspend** — Enter a gate that waits for the correlated response. The reactor suspends but continues receiving messages. When the reactor resolves the correlation, it clears the gate and injects the resolution into the message history.

**Continue** — Infer with partial results. The model sees "message sent, awaiting response" as the tool result and can do other work while waiting. When the reactor resolves the correlation later, it emits a `message.correlated` event and the director handles it normally (queue, inject, fork).

**Fork** — Spawn a child fork that suspends at the gate while the parent continues working. When the reactor resolves the correlation, the child processes it and reports the result back to the parent.

This pattern is uniform across all async operations: message passing, payment requests, approval gates, credential refresh. The tool does the immediate action; the reactor owns matching responses to pending correlations; the director manages the wait strategy.

**Parallel execution with mixed sync/async tools** works naturally. If the model calls `read_file`, `send_message`, and `grep` in one turn, all three execute concurrently. The sync tools complete and return results. The async tool returns a pending marker. The reactor registers the correlation. All three `tool.done` events fire. The director sees two complete results and one pending, and decides how to proceed.

### Async State Awareness

The model needs to know what async operations are pending and when they resolve. Two mechanisms work together:

**Synthetic resolution messages** — When an async operation resolves (a response arrives, a payment confirms, an approval is granted), the director injects a synthetic message into the conversation history. This is a real message, stored in git, part of the persistent context. The model sees it in the natural conversation flow and can reason about it:

```
[system: async_resolution] Agent X responded to your review request (r1):
"The auth module looks good, but the token refresh logic has a race condition on line 42."
```

**Pending status injection (optional)** — A `ContextTransform` can inject a compact summary of still-pending operations before each inference call. This is ephemeral — generated from the reactor's live async state, never written to `turns.jsonl`. It lands in the materialized prompt (`prompt.jsonl`) and is regenerated on each cycle:

```
[pending: agent-y "check database schema" (sent 4m ago)]
```

Pending status injection is opt-in via the context transform chain. Directors that don't need it (coding assistants with no async operations) pay no context window cost.

**Persistence model:**

| Data                                  | In git?                | Survives compaction?          |
| ------------------------------------- | ---------------------- | ----------------------------- |
| Pending marker (original tool result) | Yes                    | Summarized with conversation  |
| Resolution message (synthetic)        | Yes                    | Summarized with conversation  |
| Pending status line                   | No — ephemeral         | Regenerated from async state  |
| Async state (pending ops list)        | Yes — reactor metadata | Yes (metadata, not compacted) |

After compaction, the original pending marker and resolution message are summarized together ("Sent review request to agent-x, received feedback about race condition"). The pending status injection is unaffected — it's regenerated from the reactor's async state metadata, which persists independently of conversation history.

On reactor resume after suspension or restart, the async state is restored from git. Pending operations that haven't resolved are re-injected as status lines (if a context transform is active for that role). Resolved operations are already in the conversation history as synthetic messages.

### Gates

Before executing an action, the reactor checks **gates**. A gate can pass or block.

When a gate blocks, the reactor emits `reactor.gate.blocked` and suspends. It resumes when the gate clears (`reactor.gate.cleared`). Gates handle platform-level concerns:

- **Approval** — Tool call requires human approval. The reactor suspends, the request flows through the message bus, resumes on approval or terminates on denial.
- **Payment** — Next action requires payment. Suspends until wallet is funded.
- **Credential** — Provider credential expired. Suspends while sidecar refreshes via control plane.
- **Budget** — Cost/turn/token threshold exceeded. Director decides: compact, pause, or terminate.
- **Child completion** — Parent waiting for a child fork to finish.
- **Message response** — Waiting for a correlated response from another agent or human. The gate holds a correlation ID; when a `message.received` event matches, the gate clears with the response.

### Gate Timeouts

Every gate has a timeout. The director sets the timeout when defining the gate. If no timeout is specified, the reactor enforces a default maximum (configurable at reactor initialization, default 1 hour).

When a gate times out:

1. The reactor emits `reactor.gate.cleared` with a `reason: "timeout"` field
2. The director receives the event and decides the response — retry, fail, switch to a different strategy
3. The timed-out gate is removed from the active gates list

Gates without timeouts are resource leaks. The reactor prevents them.

### Gate Behavior During Suspension

Gates are checked before action execution, not polled. The reactor yields and is resumed by the gate's resolution mechanism.

Inbound messages are still delivered to the director during gate suspension. The director can queue them, fork to handle them, or ignore them. Suspension doesn't mean deaf.

If multiple gates are active simultaneously (possible when the director suspends at a compound gate or multiple gates from different operations), they resolve independently. The reactor delivers `reactor.gate.cleared` events in the order gates clear. If two clear simultaneously, delivery order is unspecified but both are delivered.

### Suspension, Checkpoint, and Resumption

**Suspend** and **checkpoint** are distinct operations:

- **Checkpoint** commits the current reactor state (message history, async state, token accounting) to the context store without stopping the reactor. The reactor continues processing events after the commit completes.
- **Suspend** stops the reactor at a gate. The reactor is idle until the gate clears. Suspension does not automatically checkpoint — if the director wants durability across process restarts, it should return `[checkpoint, suspend]` as a compound action.

When the reactor suspends, its resumable state is:

- The message history
- The pending action (what was about to happen when the gate blocked)
- Active gates and their context (including timeouts)
- Active forks and their state
- Async operations (pending ops list)
- Token accounting

If this state has been checkpointed, the harness process can restart and the reactor can resume from the checkpoint. This supports the lifecycle patterns in ARCHITECTURE.md — agents that survive restarts, migrate between harnesses, or hibernate when idle.

### Shutdown

When the reactor receives an `abort` event or the director returns `done` (note: `wait` does _not_ trigger shutdown — it returns to the event loop):

1. The director receives the terminal event and returns cleanup actions
2. In-flight inference calls are aborted (AbortSignal fires)
3. In-flight tool executions are aborted (AbortSignal fires) — tools must handle this gracefully
4. Active gates are cleared with `reason: "shutdown"`
5. Child forks receive abort propagation (the director can override per-fork — independent forks may outlive their parent)
6. The reactor emits `reactor.done` or `reactor.error` as the terminal event
7. No further events are processed after the terminal event

**Cleanup time limit**: the reactor enforces a maximum shutdown duration (configurable, default 30 seconds). If cleanup exceeds this, remaining operations are force-killed and the terminal event fires immediately.

**Cleanup actions cannot trigger new gates.** If the director returns a `suspend` during shutdown, it is ignored. Shutdown is not interruptible.

### Keeping It Simple

The reactor is small. It is not a workflow engine, a state machine library, or an actor framework. It is a dispatch loop:

1. Wait for an event
2. Give it to the director (catch exceptions)
3. Validate the returned action(s)
4. Execute the action(s)
5. Repeat

The complexity lives in directors, not in the reactor. A coding assistant's director is roughly 50 lines of decision logic. A collaborative agent's director is larger. The reactor itself is the same either way.

## Reactor Director

The reactor director is the consumer's orchestration policy. One core director is required; optional extension hooks layer on top.

### Core Director

The core director is a decision function. It receives an event and returns action(s):

- On `message.received` — Decide: infer, fork, queue, ignore. Choose the model.
- On `inference.done` with tool calls — Decide: execute tools (which ones, sequential or parallel), or stop.
- On `inference.done` without tool calls — Decide: reply, wait, or done.
- On `inference.error` — Decide: retry, compact, switch model, or fail.
- On `tool.done` — Decide: infer again, execute more tools, wait, or done.
- On `reactor.gate.cleared` — Resume the suspended action, or take a different action based on the gate result.
- On `message.received` while suspended — Decide: queue, fork, or ignore.

The director also provides:

- **Tool execution** — The actual tool runner. Receives tool calls, returns results. How and where tools run is the consumer's problem.
- **Gate definitions** — Which gates to check before which actions. Can be static or dynamic.
- **Model selection** — Which model for each inference call. Per-call, not per-session. Enables cost-based routing, fallback, and capability matching.
- **Fork policy** — How to initialize a new fork's director state. Called when a fork action is executed.
- **Commit policy** — When to auto-commit to the context store. Default: lifecycle boundaries (suspension, compaction, shutdown). Can be configured to per-turn or per-message for agents that need more granularity.

### Extension Hooks

Extensions layer on top of the core director without replacing it. Extensions are guard-style hooks that block or allow rather than mutate content. Content-mutating roles use the Transform / Compactor surface described in [Context Transforms and Compactors](#context-transforms-and-compactors) — the term `Extension` is reserved for hooks that gate.

- **Before tool execution** — Can block a tool call with a reason (authorization, policy). The first extension that blocks wins — the chain short-circuits and the tool call produces an error result with the blocking reason. Later extensions in the chain do not see blocked calls.
- **After tool execution** — Can modify tool result content, details, or error flag. Extensions run in order; each sees the output of the previous. Enables redaction, enrichment, or audit logging.
- **Provider request intercept** — Inspect or modify the raw provider request before sending. Enables header injection, payload logging, or tenant-specific modifications.
- **Message routing** — Intercept inbound messages before the core director sees them. Can filter, transform, or drop messages. Enables cross-cutting concerns like logging, rate limiting, or content safety filtering. The first extension that drops a message prevents further processing.

### Context Transforms and Compactors

Three role-specific abstractions describe how content flowing into and out of the conversation is mutated. They share a common shape — each takes a typed input, produces a typed output, and emits a `TransformRecord` documenting what was done — but differ in where they fire and which file in the context store's working tree their output lands in. Type definitions live in `@intx/types/runtime` and the runtime wiring is in `@intx/inference`.

- **`ToolResultTransform`** — Runs on each tool result entering history. The default size-cap policy (`createSizeCapTransform`) ships out of the box: oversized payloads are spilled to `tool-output/{callId}` via `ContextStore.writeBlob` and the inline content is replaced with a marker carrying a `tool-output:///{callId}` URI. The reactor calls the transform chain between `toolRunner.run` and `createToolResultTurn`; the transformed output is appended to `turns.jsonl`.
- **`ContextTransform`** — Runs in order before every inference call, producing the materialized prompt. Output is written to `prompt.jsonl` for the cycle; the durable history in `turns.jsonl` is untouched. Pending status injection, aged-result clearing, and post-success collapse are all describable as `ContextTransform`s.
- **`Compactor`** — Named, registered in a registry, invoked explicitly by the director via the `compact` action. Output overwrites `turns.jsonl`; the `TransformRecord` is appended to the per-cycle manifest. The director chooses _when_ to compact based on signals it observes (capacity, cost, overflow recovery); the reactor enforces the mechanics.

Every invocation produces a `TransformRecord` carrying the strategy name, version, parameters, reason, and decision details. Records are written to `manifest.jsonl` so a future operator can `git log` the manifest and reconstruct exactly which strategy made which change to any past prompt.

`ToolResultTransform` and `ContextTransform` are configured as ordered chains on the reactor. `Compactor`s are configured as a name-keyed registry; the director selects one by name when returning a `compact` action.

## Tool Interaction Patterns

The model interacts with the world through tools. How those tools accept input, report results, handle failures, and manage state has measurable impact on agent performance, cost, and reliability. The patterns described here are general — they apply to any tool that mutates state, validates content, or produces results the model must reason about.

### Mutation Formats

When a model needs to modify structured content through a tool (a file, a configuration, a database record, a document), it must express the change in some format. The format affects both reliability and cost.

Research across multiple benchmarks and production systems has identified four primary mutation formats. Each has different reliability characteristics depending on the model and the direction of the task.

**Search/replace blocks.** The model specifies an exact string to find and a replacement. No line numbers, no position arithmetic. The model is good at reproducing a chunk of content it recently read (pattern matching) and bad at counting lines or characters. Search/replace plays to the model's strengths. Benchmark data (Kamoi et al., "Diff-XYZ," arxiv 2510.12487, 2025) shows frontier models achieve 0.74-0.95 exact match for generating search/replace edits — substantially higher than unified diff generation (0.43-0.82 exact match) across the same models and tasks.

**Unified diff.** Standard patch format with hunk headers, context lines, and +/- prefixes. Models are better at applying diffs (reading and executing them: 0.90-0.95 exact match) than generating them. Hunk header arithmetic is a consistent source of errors. The asymmetry matters: unified diff is most useful when the harness generates diffs for the model to review or apply, not when the model generates them for the harness.

**Whole content.** The model produces the entire updated content. Highest token cost, highest reliability for small targets (under ~400 lines or equivalent). No format compliance issues — the model generates text. A variant uses a fine-tuned apply model that takes the model's output and merges it with existing content using speculative decoding, enabling high throughput without sacrificing accuracy.

**Lazy edit with apply model.** The model uses placeholder markers (e.g., `[... unchanged ...]`) to indicate regions that should be preserved from the original. A purpose-trained smaller model resolves the placeholders against the original content. This separates the planning model (which decides what changes) from the apply model (which produces the merged output). Requires an additional model endpoint and adds latency for the apply step.

The inference layer does not mandate a mutation format. Tool definitions choose the format that matches their mutation target and the model's capabilities. The patterns above inform that choice.

**Format compliance varies by model.** From the Aider code editing benchmark, search/replace format compliance rates range from 84% (o1-preview) to 98% (Claude 3.7 Sonnet). Models that cannot reliably produce a structured format should use whole-content tools instead. The tool definition should match the model's demonstrated reliability, not an ideal format. Format compliance should be measured per-model as part of tool development, not assumed.

**Layered matching.** Tools that accept search/replace input should implement fallback matching: exact match, then whitespace-normalized, then fuzzy (Levenshtein/difflib), then progressively looser heuristics. Every production system that achieves high reliability uses this pattern. A single exact-match strategy causes avoidable failures when the model introduces minor whitespace differences.

**Structured error feedback on match failure.** When a mutation target cannot be found (the search string doesn't match the file), the tool result must include what the content actually looks like near the expected match point. Telling the model "not found" is insufficient. Showing the model "you said X, but the content at that location is Y" enables immediate correction.

### Validation Authorities

A validation authority is any external system that can evaluate the correctness of a mutation and return structured diagnostics. The inference layer does not care what the authority is — only that it returns structured feedback with severity, location, and message.

Examples: a language server (LSP) validating code, a JSON schema validator checking configuration, a SQL linter checking queries, a Kubernetes admission controller validating manifests, a prose grammar checker reviewing documentation. The pattern is identical across all of them: tool mutates content, authority validates, diagnostics enrich the tool result.

**Integration through the after-tool-execution extension hook.** Validation does not belong inside individual tool implementations. It belongs in the extension layer. An after-tool-execution extension inspects the mutation (file path, content type, change summary), submits it to the appropriate authority, waits for diagnostics, and enriches the tool result with structured error information. The tool itself handles the mutation; the extension handles validation. This separation means new validation authorities can be added without modifying tool code.

**Diagnostic format.** Diagnostics from any authority are normalized to a common structure: severity (error, warning, info), location (file/path/line/column or equivalent), message, and optionally a machine-readable code and suggested fix. The tool result includes diagnostics in a format the model can read and act on. Raw protocol output (JSON-RPC responses, XML validation reports) is not useful to the model. Formatted, human-readable diagnostics are.

**Severity filtering.** Not all diagnostics warrant model attention. Errors should always be reported. Warnings may or may not be relevant depending on the task. Informational diagnostics are noise. The extension filters by severity and caps the count of diagnostics per result to a model-manageable number configured by the extension. Too few diagnostics and the model misses problems; too many and the context is overwhelmed with validation output that displaces useful information.

**Cross-target diagnostics.** A mutation to one target can cause errors in other targets. Changing a field name in a schema causes validation failures in every document that references it. Modifying an API contract breaks every client that depends on it. The extension should report diagnostics from the mutated target and from a bounded set of affected targets. Reporting every error across all targets is counterproductive; reporting only the mutated target misses cascading failures. The bound is configured by the extension based on the target topology — a system with 5 dependents needs different limits than one with 500.

**Mechanical fixes.** Some validation authorities provide not just diagnostics but also suggested fixes (auto-corrections in linters, suggested edits from schema validators, quick-fix actions from language servers). When a diagnostic includes a fix that can be applied mechanically, the tool can apply the fix internally and re-validate without involving the model. This is the highest-leverage optimization: the error is caught, fixed, and verified in a single tool execution. The model never sees the failure, the context is not polluted, and the cache prefix is not affected.

### Tool Result Lifecycle

Tool results enter the message history and stay there. Over a long session, old tool results accumulate — file contents that have since changed, search results that are no longer relevant, error messages from issues that were resolved turns ago. This has two costs: token spend (every old result is re-sent on each inference call) and context degradation (model performance measurably worsens as context grows — see Context Window Tracking for measurements). Old tool results sitting in the middle of a long conversation are in the worst position for both cost and retrieval quality.

**Tool result clearing.** A `ContextTransform` can clear old tool results before each inference call. Cleared results are replaced with a placeholder indicating that the result existed but was removed. The model knows the tool was called and completed, but does not see the full output. Clearing is ephemeral — it affects the materialized prompt for the cycle but not the durable history in `turns.jsonl`.

Clearing strategy: prioritize clearing the oldest and largest tool results first. Results from the current step (the most recent tool call/response cycle) should never be cleared. Results from older steps that have been superseded by newer information (a file was read again, a search was re-run) are safe to clear. The director controls the clearing policy.

**Post-success collapse.** When a tool fails and the model retries, the failure sequence (error result, model apology, retry call, success result) pollutes the conversation. After a successful retry, the failure sequence can be collapsed into a compact annotation on the successful result: "Applied successfully. 2 prior attempts failed: missing required field, invalid reference format." The model retains the lesson without the verbose error/retry/apology turns consuming context.

Collapse should happen before the next inference call so the collapsed form becomes part of the stable cache prefix. The alternative — leaving the full failure sequence — gives better immediate cache hits (the prefix is unchanged) but worse long-term cost (the verbose sequence is re-sent on every future turn at 10% of base price per cached token).

**Cache invalidation from collapse.** All current providers use prefix-based caching. Modifying content at position N in the conversation invalidates the cache from position N through the end of the request — including content after the collapsed region that is byte-identical to its previous version. The tokens are at different offsets in the sequence, so the KV-cache entries are invalid. This is a fundamental property of how attention-based caching works, not a provider limitation.

The implication: collapse is cheapest when the failure sequence is near the end of the conversation (small tail to rebuild) and most expensive when it is near the beginning (most of the context must be re-cached). Since failure sequences typically occur in the most recent turns, the typical case is favorable. If a failure sequence is deep in history, it may be better to defer its collapse until the next full compaction rather than pay a large cache rebuild for a small token savings.

**Tool-internal retry.** The strongest pattern for handling tool failures is to never let them enter the conversation. When a tool detects a failure it can fix mechanically (validation authority provides a fix, format error has an obvious correction, retry with different parameters), it should retry internally and return only the final result. The model sees one tool call and one successful result. No context pollution, no cache impact, no wasted tokens on error/retry cycles.

Tool-internal retry works for: format validation failures, mechanical fixes from validation authorities, transient errors (network timeouts, lock contention), and idempotent operations that can be re-attempted safely. It does not work for failures that require the model to reason about a different approach — those must enter the conversation so the model can adapt.

**Tool-output spills.** When a tool result exceeds the inline budget, the default `createSizeCapTransform` spills the full payload to the context store and substitutes a pointer for the inline content. The pointer takes the form `tool-output:///{callId}` — three slashes, empty authority, callId in the path so case is preserved. The agent's read tool recognizes this URI scheme and resolves it through a `BlobReader` capability that the harness backs with the context store. Any other scheme, missing or non-empty authority, extra path components, query string, or fragment is rejected; missing blobs throw. The agent has no direct path into the context store's working tree — the URI is the only handle, and it does not encode a filesystem location. The transform also emits a `TransformRecord` (`strategy: "size-cap"`, `reason: "exceeded-cap" | "within-cap"`) so the manifest captures every invocation.

### Mutation Transactions

Tools that mutate state should support transactional semantics: checkpoint before mutation, roll back on failure. The mechanism depends on the mutation target:

- **Filesystem** — Git commit or snapshot before modification; `git restore` or equivalent on failure
- **Database** — SQL transaction; rollback on error
- **API state** — Record the prior state; issue compensating calls on failure
- **Configuration** — Copy-on-write; restore the copy on failure

These are application-level transactions, managed by the tool implementation. The reactor's `checkpoint` action (described in Suspension, Checkpoint, and Resumption) is a separate concern: it commits the conversation state (message history, async state, token accounting) to the context store. A reactor checkpoint does not create a savepoint of the external system being mutated. Combining `[checkpoint, execute_tools]` means the reactor can resume the conversation from a known state if the process crashes — but rolling back the external mutation (the database, the filesystem, the API) is the tool's responsibility, not the reactor's.

Transaction granularity is the tool's decision. Per-call transactions maximize safety but add overhead. Batch transactions (one savepoint before a sequence of mutations) balance safety and performance. The inference layer provides conversation checkpointing; tools provide mutation rollback. The two are orthogonal.

## Context Management

### Inference Trace Storage

Inference activity is stored separately from mail. Each inference cycle produces one `inference_turn` row, with its streaming parts stored in `turn_part` rows ordered by `ordinal`. The turn captures the model, status, and time bounds of the cycle. Parts capture individual content blocks: text, reasoning, tool calls, tool results, and step markers.

This separation means mail records are pure RFC 5322 MIME objects, and inference traces are queryable independently. The `/turns` endpoint serves these records to UI clients.

### Context Store

The context store is a git-backed persistent store for the message history and reactor metadata. The inference package defines the context store interface; implementations live in separate packages, matching the storage backend pattern from ARCHITECTURE.md:

- **Filesystem git** — Native git on disk for Bun/Node environments. Fast, full git functionality.
- **In-memory git with remote sync** — For browser/worker environments using isomorphic-git or equivalent. Loaded on initialization, synced on commits.
- **Virtual filesystem** — For environments with partial filesystem semantics.

The reactor accepts any implementation that satisfies the interface. This keeps the inference package runtime-agnostic while allowing each deployment environment to use the appropriate backend.

### Context Store Failures

If the context store fails to read on reactor startup, the reactor cannot initialize. It emits `reactor.error` and terminates.

If the context store fails to write during a checkpoint or commit, the reactor continues operating with in-memory state. The failure is reported as an event (`reactor.error` with a non-fatal flag). The director decides whether to retry the commit, continue without persistence, or shut down. The reactor does not silently lose data — if a commit fails, the director knows.

For the in-memory git backend with remote sync: if the sync fails, the commit succeeds locally (data is not lost) and the sync failure is reported. The reactor continues. The next commit retries the sync.

### Git as Context

The message history is stored in the agent's git-backed local storage. This is not an afterthought — git is the context mechanism, not just a persistence layer.

**Forking is branching.** When the reactor forks, it creates a git branch. Independent forks branch and diverge. Child forks branch, do work, and their results can be delivered back to the parent. The fork modes map directly to git branch operations.

**Audit is built in.** Every context mutation is tracked by git. Who added what message, when, in response to what event — it's in the commit history. No separate audit trail for context changes; git is the audit trail. This satisfies the observability requirements in ARCHITECTURE.md without additional infrastructure.

**Compaction is visible.** Compacting context replaces older messages with a summary. The old messages remain in git history (reachable via log), but the working state is compact. Auditors can see what was compacted, when, and why.

**Suspension with checkpoint is durable.** When the reactor checkpoints and suspends, the context is persisted in git. The reactor can be killed and restarted; it reads context from git on resume.

**Migration works.** If an agent migrates from one harness to another, context travels as a git repo. Push and pull.

### Working State and Commits

During active operation, the reactor works with an in-memory representation of the message history for performance. Git captures snapshots at commit points. On resume after suspension, the reactor reconstructs state from the last git commit.

Commit frequency is controlled by the director's commit policy. The default commits on lifecycle boundaries (suspension, compaction, shutdown), matching the commit policy described in ARCHITECTURE.md's Change History section. Directors can configure more frequent commits (per-turn, per-message) when durability requirements are higher.

### Context Window Tracking

Token usage is tracked per-message from provider usage reports. The context store maintains a running total of consumption alongside the message history.

When usage data is unavailable (errored or aborted requests), the package falls back to character-based estimation (characters / 4) calibrated against the last successful response. This heuristic is imprecise — it undercounts for CJK text and overcounts for single-character-heavy code — but it provides a usable floor for compaction decisions and wallet accounting. Providers that return usage data on error responses are preferred.

**Context length degrades performance.** Research (Liu et al., "Lost in the Middle," TACL 2024; arxiv 2510.05381) demonstrates that model performance degrades as context length increases, even when the model can perfectly retrieve all relevant information. The effect is task-dependent and model-dependent — frontier models on simple retrieval tasks show single-digit degradation, while smaller models on complex reasoning tasks show degradation exceeding 50%. The degradation follows a U-shaped curve: information at the beginning and end of context is retrieved most reliably; information in the middle suffers a roughly 20 percentage point accuracy drop relative to the ends. This is not purely a retrieval problem — it is a property of attention mechanisms under long inputs.

The implication for context window tracking is that the token count is not just a budget to stay under. It is a quality signal. A conversation consuming 80% of the model's context window is measurably worse at reasoning than the same conversation at 40%, even if no information has been lost. Context window tracking should inform compaction decisions not just at the overflow boundary but at quality thresholds well before it. The cost-based compaction trigger (described below) serves this purpose indirectly — high input token cost correlates with long context and degraded quality.

### Compaction Triggers

Compaction is invoked by the director via the `compact` action, which selects a named `Compactor` from the reactor's registry (see [Context Transforms and Compactors](#context-transforms-and-compactors)). The director chooses _when_ to compact based on signals it observes:

- **Capacity** — Context usage approaches the model's window limit. This is the standard trigger.
- **Cost** — The per-request input token cost exceeds a threshold. A conversation spending $0.50/request on input tokens when it could spend $0.10 after compaction is wasting wallet balance. The cost threshold is configured by the tenant or agent policy.
- **Overflow recovery** — The model rejects a request as too long. One automatic compaction attempt, then fail. No infinite loops.
- **Quality** — Context length has crossed a threshold where model performance is measurably degraded. This is distinct from capacity (which triggers near the hard limit) and cost (which triggers on per-request spend). A quality trigger fires earlier, compacting proactively to maintain reasoning accuracy rather than waiting for the context to approach overflow. The threshold is model-dependent and configured by the agent policy.
- **Explicit** — The director requests compaction directly.

### Compaction Contract

The compaction director receives the current message history, trigger reason, and wallet balance context. It returns a compacted message history that must satisfy:

- Tool call / tool result pairing is preserved. Every tool call in the compacted output has a corresponding tool result. Orphaned calls or results are not permitted.
- Message ordering is preserved. The compacted output is a valid conversation that the target model can process.
- Pending async operations (tool results with pending markers that have not resolved) are preserved in the compacted output. They cannot be removed because the reactor still holds active gates for them.
- The compacted output is shorter (in tokens) than the input. A compaction that produces equal or longer output is a no-op and the reactor logs a warning.

Compaction itself costs tokens (the summary generation is an inference call). If the wallet is nearly empty, a cheaper compaction strategy (or simply truncating old messages without summarization) may be better than generating an expensive summary.

## Error Classification

Provider errors are classified into categories that determine the reactor's response. Classification is provider-aware because each provider reports errors differently.

### Error Categories

- **Retryable** — Rate limits (429), server errors (500, 502, 503, 504), overload, network failures. Response: exponential backoff with retry.
- **Context overflow** — Request exceeds the model's context window. Each provider phrases this differently (20+ known patterns). Response: trigger compaction, not retry.
- **Credential failure** — Authentication rejected, token expired. Response: emit a credential gate, suspend the reactor for credential refresh. In a platform with managed credentials, expiry is expected and recoverable.
- **Quota exhausted** — Provider-level usage limit hit (distinct from transient rate limits). Response: fail or switch model, depending on director policy.
- **Fatal** — Invalid request, unsupported model, malformed content. Response: fail immediately with diagnostic information.
- **Aborted** — Caller cancelled via AbortSignal. Response: clean termination.
- **Timeout** — Per-call inactivity or total wall-clock cap fired (see Per-Call Timeouts). The call produced no usable response. Response: treat as transient infrastructure failure and retry per director policy rather than as a model decision.
- **Protocol mismatch** — Upstream emitted a chunk that violates the provider's streaming protocol: malformed JSON, schema validation failure, or an out-of-order event sequence; the offending chunk is preserved in `error.raw`. Response: fail rather than retry — retrying the same request won't fix the bytes.

The classifier inspects HTTP status codes, error response bodies, and provider-specific error message patterns. The classified error is delivered to the director as an `inference.error` event, and the director decides the response — retry, compact, switch model, suspend, or fail. The error categories inform the director's decision but do not hardcode the response.

### Retry Behavior

Retryable errors use exponential backoff: `baseDelay * 2^attempt`. The director controls maximum attempts and total budget. Failed attempts are removed from the message history before retrying — the model should not see its own error responses.

## Per-Call Timeouts

Every `runInference` call arms two timers: an inactivity timer reset on every chunk the harness receives from the wire, and a total wall-clock cap that starts at `fetch()`. When either timer fires, the harness aborts the underlying fetch's combined `AbortController` and emits an `inference.error` with category `"timeout"` and a message naming which timer fired and its threshold value. The combined controller wires together the caller-supplied signal (if any) and the harness's internal timeout signal, so a fetch implementation that respects `AbortSignal` sees both.

"Wire chunk" means any SSE data line the harness reads, not strictly the events the harness yields downstream — heartbeat frames, sentinel-only chunks (e.g. `[DONE]`), and trailing usage events all count as activity. This is the more permissive of the two natural rules: it treats any sign of provider liveness as defeat of the inactivity timer, which is what "stalled SSE stream" intuitively means and what the safety property actually guards against.

The defaults are conservative:

- `inactivityTimeoutMs: 120_000` (2 min). Reasoning-heavy models emit tokens regularly when actually thinking; two minutes of pure silence is real stall, not legitimate reasoning. Tune higher for models with extended pure-silence thinking stretches — the `interchange-demo-dispatch` planner agent was observed reasoning for roughly seven minutes between tool calls during early benchmarking, which would require a higher inactivity threshold.
- `totalTimeoutMs: 600_000` (10 min). Matches Anthropic's documented per-call recommendation and fits within typical CI budgets. Backstop for streams that keep emitting forever without terminating.

Both can be overridden per call via `InferenceOptions.inactivityTimeoutMs` and `InferenceOptions.totalTimeoutMs`. Set to a smaller number to fail fast; set to a larger number to accommodate slower model behaviour.

A timeout always classifies as `"timeout"`, never as `"aborted"` — even though the underlying mechanism is an `AbortController` firing. Distinguishing the two at the category level lets directors apply different policies: a caller-initiated abort is the user's decision and should not be retried, while a timeout is an infrastructure failure and may be retried.

## Abort Handling

AbortSignal is threaded through every async boundary: stream parsing, tool execution, retry delays, compaction, and the reactor itself.

When abort fires, every layer produces clean termination: streams emit error events, tool executions return error results, gates resolve as denied, and the reactor exits with a terminal event. No resource leaks, no dangling promises.

### Abort Reasons

Interchange has more abort sources than a single-user tool, and the reason determines cleanup behavior:

- **User disconnect** — May or may not mean "stop." A background agent should continue. An interactive session should pause. The director decides.
- **Wallet exhaustion** — Hard stop. No more inference calls or paid tool invocations. Checkpoint state for resumption when funded.
- **Admin kill** — Hard stop. The harness is shutting down. No checkpoint.
- **Session timeout** — The session channel has been idle too long. Checkpoint and suspend.
- **Credential revocation** — The creator revoked a capability. Stop exercising it immediately.

The reactor receives abort as an `abort` event with a reason. The director returns the appropriate cleanup action: checkpoint and suspend, terminate immediately, or signal forks to abort.

## Token Accounting

Every inference call reports full token breakdown:

- **Input tokens** — Tokens in the request
- **Output tokens** — Tokens generated
- **Cache read tokens** — Tokens served from prompt cache
- **Cache write tokens** — Tokens written to prompt cache
- **Thinking tokens** — Tokens consumed by reasoning

The inference package reports raw counts and emits them as `inference.usage` events. Cost calculation is the wallet system's responsibility — the inference layer does not know token prices.

When a request fails or aborts before returning usage data, the package emits estimated counts. The wallet must be able to account for every request, successful or not. Provider billing does not forgive failed requests, and neither should the accounting layer.

Token accounting is per-reactor. When a reactor forks, each fork tracks its own usage independently (starting from zero). The parent can query aggregate usage across itself and its children via the state object.

## Prompt Caching

Prompt caching is a provider-level optimization where identical prefixes of the request (tools, system prompt, message history) are cached and reused across turns. The inference layer must structure requests to maximize cache hits, because caching dominates the cost profile of multi-turn agent sessions.

### Provider Mechanics

Caching is prefix-based. The provider matches the incoming request against previously cached content from the start of the request forward. The longest matching prefix is served from cache; everything after the first divergence is processed fresh.

The cache hierarchy is strict and ordered: **Tools → System → Messages**. A change at any level invalidates that level and everything downstream. Modifying a tool definition invalidates tools, system, and all messages. Modifying the system prompt invalidates system and messages but preserves the tools cache. Modifying messages invalidates only the message cache.

This hierarchy has a critical implication for tool management: adding or removing a tool mid-session invalidates the entire cache. Dynamic tool sets (tools discovered from remote registries, tools that connect and disconnect during a session) break the prefix every time they change.

**Anthropic.** Explicit cache breakpoints via `cache_control` markers, maximum 4 per request. Minimum cacheable length varies by model (1,024-4,096 tokens). Default TTL is 5 minutes, refreshed on each hit; extended TTL of 1 hour is available at 2x write cost. Cache writes cost 1.25x base input price (5-minute) or 2.0x (1-hour). Cache reads cost 0.1x base input price — a 90% discount.

**OpenAI.** Fully automatic prefix caching, no explicit markers. Minimum 1,024 tokens, cached in 128-token increments. TTL varies by usage pattern and load — generally minutes to an hour of inactivity; consult current documentation for specifics as retention policies have changed over time. Cache reads cost 0.5x base input price — a 50% discount. No surcharge for cache writes.

### Request Structure for Cache Performance

Stable content goes first, variable content goes last. This is a hard constraint — prefix matching cannot skip content.

1. **Tool definitions** — Most stable. Change rarely or never during a session. Place cache breakpoints on the last tool in the array.
2. **System prompt** — Stable for the session duration. Do not inject per-request data (timestamps, mode flags, dynamic context) into the system prompt. Put variable context into messages instead.
3. **Message history** — Grows each turn. The existing prefix is cached; new messages extend it. Modifications to earlier messages (compaction, collapse, clearing) invalidate the cache from the modification point forward.

The reactor should not inject ephemeral data (pending status, dynamic context) into the system prompt. The context transform chain handles ephemeral injection by modifying the message array, leaving the system prompt cache intact.

### Tool Definition Stability

Tool definitions are part of the cached prefix. Their token cost compounds: a set of 15 tools with moderately complex parameters consumes roughly 5,000-6,000 tokens, sent and cached on every turn.

For agents with dynamic capabilities (tools from remote registries, extension-provided tools), the mitigation is a stable core with deferred loading: a small, fixed set of always-available tools lives in the tool definition array (stable, always cached). Tools that are not always needed are excluded from the array entirely. When the model needs a deferred tool, it discovers it through a search/discovery tool in the core set, and the discovered tool's definition is injected inline as a tool reference in the message history. The tool prefix stays stable; discovered tools are part of the message stream, not the tool array.

The cost of dynamic tool sets without these mitigations is severe. Each MCP server adds 1,500-12,000 tokens to the tool prefix. Three to four servers can add 10,000-18,000 tokens of overhead per turn. Every connect/disconnect event invalidates the entire cache.

### Cache-Aware Compaction

Compaction (described in Context Management) replaces older messages with a summary. Because caching is strictly prefix-based, any modification to the message history invalidates the cache from the modification point through the end of the request. Content after the compacted region is rebuilt even if it is unchanged — it now sits at different token offsets. The cost of compaction is not just the summarization inference call — it includes a full cache rebuild of everything from the compaction point forward.

For compaction to be worthwhile, the tokens saved per turn must recoup the one-time cache rebuild cost within the remaining session. The rebuild cost depends on the tail: everything from the compaction point forward must be re-cached, regardless of whether it changed. At Anthropic's pricing, each re-cached token costs 1.25x base price instead of the 0.1x it would have cost as a cache read — a per-token premium of 1.15x base price for the rebuild turn.

Consider a conversation at 60,000 tokens where compaction at the midpoint removes 10,000 tokens. The post-compaction conversation is 50,000 tokens. The tail from the compaction point is 25,000 tokens (the second half minus the removed content plus the summary). Rebuild premium: 25,000 tokens at 1.15x base price. Per-turn savings: 10,000 fewer cached tokens at 0.1x base price = 1,000 base-price-equivalents per turn. Break-even depends on the ratio of tail size to tokens removed — the larger the tail relative to the savings, the longer until break-even. Compaction near the end of the conversation (small tail) pays back quickly. Compaction near the beginning (large tail) takes many turns to recoup.

The failure collapse pattern (described in Tool Interaction Patterns) is the favorable case: the failure sequence is typically near the end of the conversation, so the tail is small. The stable prefix before the failure point is preserved; only the collapsed region and whatever follows it are rebuilt.

### Parallel Requests and Cache Sharing

Cache entries are only available after the first response begins. Parallel requests sent simultaneously cannot share a fresh cache — each pays full input price for the shared prefix. Sequential requests benefit from caching because each subsequent request finds the prefix already cached.

This affects the fork architecture. When the reactor forks, each fork makes independent inference calls. If forks share the same tool definitions and system prompt (typical), the prefix cache is shared automatically — these are content-addressed, not session-addressed. But if two forks send requests concurrently, only one benefits from a cache write by the other.

For cost-sensitive deployments, sequential subagent invocations that share a prefix are cheaper than parallel invocations. The tradeoff is latency vs. cost.

## Thinking and Reasoning

Extended thinking (Anthropic) and reasoning tokens (OpenAI) are supported from day one.

### Configuration

Reasoning is configured per-agent as part of the agent definition, not per-request. The agent's creator decides the reasoning posture:

- **Off** — No reasoning tokens. Cheapest, fastest.
- **On** — Reasoning enabled with a token budget.

The budget is a ceiling, not a target. The provider adapter translates the on/off + budget into the provider-specific mechanism (Anthropic's `thinking` parameter, OpenAI's `reasoning_effort`, provider-variant formats for OpenRouter, z.ai, Qwen).

The reactor director can override reasoning configuration per-call if needed (for example, enabling reasoning for a complex planning step and disabling it for simple tool result processing). But the default comes from the agent definition, not from the inference package.

**Cache interaction.** Changing the thinking budget between turns invalidates the message cache. Tool and system prompt caches survive, but all message-level cache entries are rebuilt. If the director toggles reasoning on and off per-call, each toggle pays a cache rebuild cost. For cost-sensitive agents, holding a consistent thinking budget across the session is preferable to dynamic adjustment. If variable reasoning depth is needed, achieve it through prompt engineering (instructing the model to think briefly vs. deeply) rather than through the API parameter, preserving the cache.

### Thinking Content

Thinking content flows through the event stream as `inference.thinking.delta` events. The partial message accumulates thinking blocks alongside text blocks.

Opaque signatures for multi-turn reasoning continuity are preserved per-message and handled by cross-provider transformation (kept for same-model, stripped for cross-model).

Redacted thinking blocks (provider chose not to expose reasoning) are preserved for same-model replay but dropped during cross-provider transformation.

## Test Provider

The package includes a deterministic test provider that faithfully implements the provider adapter contract without making network calls.

### Capabilities

- **Response queuing** — Static messages or factory functions that receive context and return dynamic responses
- **Streaming simulation** — Variable-size chunking with configurable delays for realistic streaming behavior
- **Session-aware caching** — Tracks per-session context prefixes and reports cache hit/miss token counts
- **Token estimation** — Character-count heuristics matching the fallback estimation used for real errors
- **Abort support** — Respects AbortSignal, produces proper error events
- **Gate simulation** — Can trigger approval/payment/credential gates for testing suspension and resumption
- **Message injection** — Can simulate inbound messages arriving mid-inference for testing reactor message handling and forking
- **Full event protocol** — Emits the complete event sequence identical to real providers

The test provider exercises the same code paths as real providers, including the streaming harness, partial state accumulation, and error classification. Tests that pass against the test provider will pass against real providers, modulo provider-specific parsing edge cases that have their own targeted tests.

## Future Considerations

**Branched forking.** A third fork mode where the new reactor shares the message history with the parent up to the fork point (like a git branch) and both append independently. This would enable exploring alternative approaches while preserving the ability to compare or merge results. It is deferred because the semantics are unresolved: compaction interaction with a shared prefix, merge semantics for divergent conversation branches, and shared mutable state between concurrent reactors all require design work that would delay implementation of the core fork modes. If a concrete use case emerges that cannot be served by independent or child forks, branched forking can be designed with the benefit of production experience.
