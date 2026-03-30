# Faremeter Interchange

_Inference_

The inference package (`@interchange/inference`) is the provider-agnostic LLM layer that powers all agent reasoning in Interchange. It handles streaming, tool execution, context management, and token accounting across multiple model providers without depending on any provider's SDK.

## Design Principles

**No upstream SDKs.** Every provider is just HTTP POST + SSE. The package uses `fetch` and `ReadableStream` exclusively, keeping it runtime-agnostic (Bun, Node, browser) and free of SDK bloat.

**Errors are data, not exceptions.** Errors flow through the event stream as typed events. Consumers always see a complete event sequence (start through done or error), even during failures. This makes streaming UI and multiplayer fanout reliable.

**One event protocol.** The inference event stream is the session channel protocol. Events emitted by the inference layer are the same events that flow to session channel subscribers. There is no internal format that gets translated into an external format. One protocol, end to end.

**Event-driven, not request-driven.** The agent reactor processes events from multiple sources — humans, other agents, the system, tool completions — and asks the plugin what to do next. It can suspend for external reasons (approval gates, payment, credential refresh) and resume from where it left off. Suspension is a first-class concept, not an error.

**Pluggable by default.** The reactor, tool execution, context management, and compaction are all plugin-driven. The inference package provides the machinery; consumers provide the policy. But the reactor enforces safety invariants regardless of plugin behavior — it validates actions, catches plugin exceptions, and prevents resource leaks.

## Providers

The inference layer supports multiple providers through thin adapters built on a shared streaming harness. Each adapter provides two functions: build a request from the internal format, and parse the provider's SSE events into internal events. The harness handles connection management, SSE parsing, partial state accumulation, abort propagation, and error reporting.

This inverts the typical approach where each provider implements the full streaming lifecycle independently. The harness eliminates the structural duplication that plagues multi-provider libraries, where every adapter copies the same try/catch/cleanup/emit boilerplate with minor variations.

**Day-one providers:**

- **Anthropic** — Messages API with streaming, extended thinking, prompt caching
- **OpenAI-compatible** — Covers OpenAI, OpenRouter, and self-hosted endpoints (Ollama, vLLM)
- **OpenCode Go/Zen** — OpenCode's inference backends

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

message.received          — Inbound message arrived (from human, agent, system)
message.queued            — Inbound message queued for later processing
message.correlated        — Inbound message matched to pending outbound

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

The inference package provides an event-driven reactor, not a request-response loop. An interchange agent is a long-lived entity that receives messages from multiple sources, reasons about them, and acts. The reactor processes events as they arrive and asks the plugin what to do next.

### Terminology

This document uses "context" in three distinct senses:

- **Message history** — The array of messages (user, assistant, tool results, system) that form the conversation. This is what gets sent to the model and stored in git.
- **Reactor state** — The full state visible to the plugin: message history plus active forks, pending gates, async operations, and token accounting. This is the `state` parameter to the plugin decision function.
- **Context store** — The git-backed persistent store that holds the message history and reactor metadata. This is the storage layer.

When the distinction matters, this document uses the specific term. When it doesn't, "context" refers to the message history.

### Relationship to the Kernel

The kernel (described in ARCHITECTURE.md) is the agent's runtime. The inference package is a library the kernel uses. The kernel:

- Instantiates the reactor with a plugin, context store, and provider configuration
- Delivers inbound messages from the message bus to the reactor
- Routes reactor events to session channel subscribers
- Manages the agent's credential lifecycle (the reactor requests credentials via gates; the kernel fulfills them)
- Handles agent lifecycle (startup, shutdown, health checks) — the reactor handles inference lifecycle

The reactor does not own the message bus, the session channel, or the credential store. It owns the inference loop, tool execution, context management, and event emission. The kernel is the integration layer.

### Why Not a Loop

A traditional agent loop assumes one input source (the user), one conversation at a time, and synchronous tool execution. Interchange agents face a fundamentally different environment:

- A **collaborative agent** sends a message to Agent X, continues working on something else, and processes Agent X's response whenever it arrives — potentially mid-task.
- A **customer-facing bot** handles User A's conversation when User B sends a message. That's a separate conversation, not a follow-up.
- A **background worker** has no "conversation" to loop through. It sits idle and reacts to inbound work.
- A **coding assistant** works like a traditional loop but may receive system messages (credential refresh, admin commands) at any time.

A loop that checks a queue "between turns" can't serve these use cases without fighting its own structure. The reactor handles all of them with one model.

### Reactor Structure

The reactor processes one event at a time and asks the plugin for the next action:

```
Event arrives → Plugin decides → Reactor executes → Next event
```

**Inbound events** (things that happen to the agent):

- `message.received` — A message arrived (from a human, another agent, the system)
- `inference.done` — A model call completed (with assistant message)
- `inference.error` — A model call failed (with error classification)
- `tool.done` — A tool execution completed (with result)
- `reactor.gate.cleared` — A suspension condition resolved (with gate result)
- `abort` — The reactor should shut down (with reason)

**Actions** (things the reactor can do, as directed by the plugin):

- `infer` — Call a model with a message history, model, and options
- `execute_tools` — Execute tool calls (sequential or parallel)
- `suspend` — Wait for a gate to clear (approval, payment, credential, custom)
- `fork` — Create a new reactor with copied context
- `emit` — Send a custom event to session channel subscribers (see below)
- `checkpoint` — Commit current state to the context store without suspending
- `done` — Reactor is finished

The reactor is a thin dispatch layer. It doesn't decide what to do — the plugin does. The reactor executes actions reliably: manages the streaming harness for inference, dispatches tool calls, handles suspension mechanics, manages fork lifecycle.

### The `emit` Action

The plugin can emit custom events to session channel subscribers via the `emit` action. Custom events use a `custom.*` type namespace and carry arbitrary data. They receive sequence numbers like all other events. They are ephemeral — not persisted to the context store. Use cases: progress indicators, debug information, UI hints.

The plugin cannot emit events in the `inference.*`, `tool.*`, `reactor.*`, or `fork.*` namespaces. Those are reserved for the reactor.

### Plugin Decision Function

The plugin is a single function:

```
(event, state, capabilities) → action | action[]
```

The plugin receives:

- **event** — What just happened
- **state** — The current message history, active forks, pending gates, async operations, token accounting
- **capabilities** — What the reactor can do (infer, execute tools, fork, suspend, emit, checkpoint)

It returns one or more actions. Multiple actions execute concurrently where possible (parallel tool calls, fork + continue).

**If the plugin throws an exception**, the reactor catches it, emits `reactor.error` with the exception details, and initiates graceful shutdown. The plugin is user-provided code and must not be able to crash the reactor without a clean terminal event.

### Action Validation

The reactor validates the action set returned by the plugin before executing:

- **No conflicting actions** — At most one `infer` action. At most one `done`. `infer` + `done` is invalid.
- **Fork is composable** — `fork` can appear alongside `infer` or `execute_tools` (fork happens concurrently).
- **Multiple tool executions collapse** — Multiple `execute_tools` actions are merged into a single parallel batch.
- **Suspend is exclusive** — `suspend` cannot appear alongside `infer` or `execute_tools`. You either do work or you wait.
- **Checkpoint is composable** — `checkpoint` can appear alongside any other action (it fires before the other action executes).

Invalid action sets produce a `reactor.error` event with a diagnostic message. The reactor does not guess intent.

### Message Handling

Inbound messages arrive at the reactor regardless of current state. The plugin decides how to handle them:

**Queue** — Add to the message history for the next model call. The model sees the message when the current action completes. This is the simple case: a human sends a follow-up while the model is generating.

**Inject as steering** — Abort the current inference call, add the message to the history, and re-infer. The model sees the message immediately. Use case: an admin sends a priority redirect.

**Fork** — Create a new reactor instance to handle the message independently. The current work continues uninterrupted. Use case: a second user starts a conversation while the agent is already busy.

**Correlate** — Match the message to a pending outbound by correlation ID. Deliver it as a synthetic resolution message and clear the corresponding message-response gate if one is active. Use case: Agent X responds to a message we sent earlier.

**Ignore** — Drop the message. Use case: duplicate delivery, irrelevant system notification.

### Message Correlation

Correlation connects outbound async tool calls to inbound responses. The mechanics:

- **Correlation IDs** are assigned by the tool implementation when it sends a message. The format is opaque to the reactor — typically a UUID, but the tool and the external protocol determine the format.
- The tool returns the correlation ID in its pending marker: `{ status: "pending", correlationId: "abc123" }`.
- The plugin registers the correlation ID with the reactor's async state.
- When a `message.received` event arrives, the plugin checks its correlation ID (carried in the message metadata) against registered pending operations.
- If matched, the plugin handles it as a correlated response (inject as resolution, clear gate, etc.).
- **Duplicate responses** (same correlation ID) are delivered to the plugin. The plugin decides whether to process or ignore. The reactor does not deduplicate.
- **Orphaned correlations** (pending operation whose response never arrives) are cleaned up by gate timeouts.

### Forking

When the plugin returns a `fork` action, the reactor creates a new reactor instance. Two fork modes are supported:

**Independent** — The new reactor gets a full copy of the message history up to the fork point. After that, the two reactors share nothing. Each has its own message history, its own plugin state, its own lifecycle, its own token accounting. In git terms, this is a branch that diverges immediately. The kernel manages both reactors. Use case: handling a completely separate conversation.

**Child** — The new reactor gets a copy of the message history and reports results back to the parent. The parent can wait for the child (`suspend` on a child-completion gate) or continue working. When the child completes, its result is delivered to the parent as a `fork.done` event. The child's token usage is tracked independently but the parent can query aggregate usage. In git terms, this is a branch that gets merged back. Use case: delegating a subtask to a cheaper model or a specialized tool set.

In both modes, the fork receives:

- A copy of the message history (not a reference — mutations are independent)
- A fresh token accounting counter (starting from zero)
- A fresh async state (no inherited pending operations)
- Plugin state initialized by the fork policy callback

Fork lifecycle is managed by the reactor. Forks emit their own events (tagged with the fork ID for session channel routing). Forks can be aborted independently. The plugin tracks active forks via the state object.

### Tool Execution Semantics

**All tools return synchronously from the reactor's perspective.** A tool execution is an async function that resolves with a result. The reactor never blocks on a tool that's waiting for an external event.

Tools that need to wait for something external (a response from another agent, a payment confirmation, human approval) follow the **pending marker pattern**:

1. The tool does its immediate work (sends the message, submits the payment request)
2. The tool returns immediately with a pending marker: `{ status: "pending", correlationId: "abc123" }`
3. The `tool.done` event fires with the pending result
4. The plugin sees the pending marker and decides what to do

The plugin has several options when it sees a pending result:

**Suspend** — Enter a message-response gate that waits for the correlated response. The reactor suspends but continues receiving messages. When the response arrives, the gate clears and the plugin injects the response into the message history.

**Continue** — Infer with partial results. The model sees "message sent, awaiting response" as the tool result and can do other work while waiting. When the response arrives later as a `message.received` event, the plugin handles it normally (queue, inject, fork).

**Fork** — Spawn a child fork that suspends at the gate while the parent continues working. When the response arrives, the child processes it and reports the result back to the parent.

This pattern is uniform across all async operations: message passing, payment requests, approval gates, credential refresh. The tool does the immediate action; the plugin manages the wait.

**Parallel execution with mixed sync/async tools** works naturally. If the model calls `read_file`, `send_message`, and `grep` in one turn, all three execute concurrently. The sync tools complete and return results. The async tool returns a pending marker. All three `tool.done` events fire. The plugin sees two complete results and one pending, and decides how to proceed.

### Async State Awareness

The model needs to know what async operations are pending and when they resolve. Two mechanisms work together:

**Synthetic resolution messages** — When an async operation resolves (a response arrives, a payment confirms, an approval is granted), the plugin injects a synthetic message into the conversation history. This is a real message, stored in git, part of the persistent context. The model sees it in the natural conversation flow and can reason about it:

```
[system: async_resolution] Agent X responded to your review request (r1):
"The auth module looks good, but the token refresh logic has a race condition on line 42."
```

**Pending status injection (optional)** — The context transform extension can inject a compact summary of still-pending operations before each inference call. This is ephemeral — generated from the reactor's live async state, never stored in git, never committed. It is stripped before messages are persisted:

```
[pending: agent-y "check database schema" (sent 4m ago)]
```

Pending status injection is opt-in via the context transform extension. Plugins that don't need it (coding assistants with no async operations) pay no context window cost.

**Persistence model:**

| Data                                  | In git?                | Survives compaction?          |
| ------------------------------------- | ---------------------- | ----------------------------- |
| Pending marker (original tool result) | Yes                    | Summarized with conversation  |
| Resolution message (synthetic)        | Yes                    | Summarized with conversation  |
| Pending status line                   | No — ephemeral         | Regenerated from async state  |
| Async state (pending ops list)        | Yes — reactor metadata | Yes (metadata, not compacted) |

After compaction, the original pending marker and resolution message are summarized together ("Sent review request to agent-x, received feedback about race condition"). The pending status injection is unaffected — it's regenerated from the reactor's async state metadata, which persists independently of conversation history.

On reactor resume after suspension or restart, the async state is restored from git. Pending operations that haven't resolved are re-injected as status lines (if the context transform extension is active). Resolved operations are already in the conversation history as synthetic messages.

### Gates

Before executing an action, the reactor checks **gates**. A gate can pass or block.

When a gate blocks, the reactor emits `reactor.gate.blocked` and suspends. It resumes when the gate clears (`reactor.gate.cleared`). Gates handle platform-level concerns:

- **Approval** — Tool call requires human approval. The reactor suspends, the request flows through the message bus, resumes on approval or terminates on denial.
- **Payment** — Next action requires payment. Suspends until wallet is funded.
- **Credential** — Provider credential expired. Suspends while sidecar refreshes via control plane.
- **Budget** — Cost/turn/token threshold exceeded. Plugin decides: compact, pause, or terminate.
- **Child completion** — Parent waiting for a child fork to finish.
- **Message response** — Waiting for a correlated response from another agent or human. The gate holds a correlation ID; when a `message.received` event matches, the gate clears with the response.

### Gate Timeouts

Every gate has a timeout. The plugin sets the timeout when defining the gate. If no timeout is specified, the reactor enforces a default maximum (configurable at reactor initialization, default 1 hour).

When a gate times out:

1. The reactor emits `reactor.gate.cleared` with a `reason: "timeout"` field
2. The plugin receives the event and decides the response — retry, fail, switch to a different strategy
3. The timed-out gate is removed from the active gates list

Gates without timeouts are resource leaks. The reactor prevents them.

### Gate Behavior During Suspension

Gates are checked before action execution, not polled. The reactor yields and is resumed by the gate's resolution mechanism.

Inbound messages are still delivered to the plugin during gate suspension. The plugin can queue them, fork to handle them, or ignore them. Suspension doesn't mean deaf.

If multiple gates are active simultaneously (possible when the plugin suspends at a compound gate or multiple gates from different operations), they resolve independently. The reactor delivers `reactor.gate.cleared` events in the order gates clear. If two clear simultaneously, delivery order is unspecified but both are delivered.

### Suspension, Checkpoint, and Resumption

**Suspend** and **checkpoint** are distinct operations:

- **Checkpoint** commits the current reactor state (message history, async state, token accounting) to the context store without stopping the reactor. The reactor continues processing events after the commit completes.
- **Suspend** stops the reactor at a gate. The reactor is idle until the gate clears. Suspension does not automatically checkpoint — if the plugin wants durability across process restarts, it should return `[checkpoint, suspend]` as a compound action.

When the reactor suspends, its resumable state is:

- The message history
- The pending action (what was about to happen when the gate blocked)
- Active gates and their context (including timeouts)
- Active forks and their state
- Async operations (pending ops list)
- Token accounting

If this state has been checkpointed, the kernel process can restart and the reactor can resume from the checkpoint. This supports the lifecycle patterns in ARCHITECTURE.md — agents that survive restarts, migrate between kernels, or hibernate when idle.

### Shutdown

When the reactor receives an `abort` event or the plugin returns `done`:

1. The plugin receives the terminal event and returns cleanup actions
2. In-flight inference calls are aborted (AbortSignal fires)
3. In-flight tool executions are aborted (AbortSignal fires) — tools must handle this gracefully
4. Active gates are cleared with `reason: "shutdown"`
5. Child forks receive abort propagation (the plugin can override per-fork — independent forks may outlive their parent)
6. The reactor emits `reactor.done` or `reactor.error` as the terminal event
7. No further events are processed after the terminal event

**Cleanup time limit**: the reactor enforces a maximum shutdown duration (configurable, default 30 seconds). If cleanup exceeds this, remaining operations are force-killed and the terminal event fires immediately.

**Cleanup actions cannot trigger new gates.** If the plugin returns a `suspend` during shutdown, it is ignored. Shutdown is not interruptible.

### Keeping It Simple

The reactor is small. It is not a workflow engine, a state machine library, or an actor framework. It is a dispatch loop:

1. Wait for an event
2. Give it to the plugin (catch exceptions)
3. Validate the returned action(s)
4. Execute the action(s)
5. Repeat

The complexity lives in plugins, not in the reactor. A coding assistant's plugin is roughly 50 lines of decision logic. A collaborative agent's plugin is larger. The reactor itself is the same either way.

## Reactor Plugin

The reactor plugin is the consumer's orchestration policy. One core plugin is required; optional extension hooks layer on top.

### Core Plugin

The core plugin is a decision function. It receives an event and returns action(s):

- On `message.received` — Decide: infer, fork, queue, ignore. Choose the model.
- On `inference.done` with tool calls — Decide: execute tools (which ones, sequential or parallel), or stop.
- On `inference.done` without tool calls — Decide: done, or wait for more events.
- On `inference.error` — Decide: retry, compact, switch model, or fail.
- On `tool.done` — Decide: infer again, execute more tools, or stop.
- On `reactor.gate.cleared` — Resume the suspended action, or take a different action based on the gate result.
- On `message.received` while suspended — Decide: queue, fork, or ignore.

The plugin also provides:

- **Tool execution** — The actual tool runner. Receives tool calls, returns results. How and where tools run is the consumer's problem.
- **Gate definitions** — Which gates to check before which actions. Can be static or dynamic.
- **Model selection** — Which model for each inference call. Per-call, not per-session. Enables cost-based routing, fallback, and capability matching.
- **Fork policy** — How to initialize a new fork's plugin state. Called when a fork action is executed.
- **Commit policy** — When to auto-commit to the context store. Default: lifecycle boundaries (suspension, compaction, shutdown). Can be configured to per-turn or per-message for agents that need more granularity.

### Extension Hooks

Extensions layer on top of the core plugin without replacing it:

- **Before tool execution** — Can block a tool call with a reason (authorization, policy). The first extension that blocks wins — the chain short-circuits and the tool call produces an error result with the blocking reason. Later extensions in the chain do not see blocked calls.
- **After tool execution** — Can modify tool result content, details, or error flag. Extensions run in order; each sees the output of the previous. Enables redaction, enrichment, or audit logging.
- **Context transform** — Modify the message array before each inference call. This is where pending status injection lives (if enabled). Extensions run in order; each sees the output of the previous. Modifications are ephemeral — they affect the inference call but are not committed to the context store.
- **Provider request intercept** — Inspect or modify the raw provider request before sending. Enables header injection, payload logging, or tenant-specific modifications.
- **Message routing** — Intercept inbound messages before the core plugin sees them. Can filter, transform, or drop messages. Enables cross-cutting concerns like logging, rate limiting, or content safety filtering. The first extension that drops a message prevents further processing.

## Context Management

### Context Store

The context store is a git-backed persistent store for the message history and reactor metadata. The inference package defines the context store interface; implementations live in separate packages, matching the storage backend pattern from ARCHITECTURE.md:

- **Filesystem git** — Native git on disk for Bun/Node environments. Fast, full git functionality.
- **In-memory git with remote sync** — For browser/worker environments using isomorphic-git or equivalent. Loaded on initialization, synced on commits.
- **Virtual filesystem** — For environments with partial filesystem semantics.

The reactor accepts any implementation that satisfies the interface. This keeps the inference package runtime-agnostic while allowing each deployment environment to use the appropriate backend.

### Context Store Failures

If the context store fails to read on reactor startup, the reactor cannot initialize. It emits `reactor.error` and terminates.

If the context store fails to write during a checkpoint or commit, the reactor continues operating with in-memory state. The failure is reported as an event (`reactor.error` with a non-fatal flag). The plugin decides whether to retry the commit, continue without persistence, or shut down. The reactor does not silently lose data — if a commit fails, the plugin knows.

For the in-memory git backend with remote sync: if the sync fails, the commit succeeds locally (data is not lost) and the sync failure is reported. The reactor continues. The next commit retries the sync.

### Git as Context

The message history is stored in the agent's git-backed local storage. This is not an afterthought — git is the context mechanism, not just a persistence layer.

**Forking is branching.** When the reactor forks, it creates a git branch. Independent forks branch and diverge. Child forks branch, do work, and their results can be delivered back to the parent. The fork modes map directly to git branch operations.

**Audit is built in.** Every context mutation is tracked by git. Who added what message, when, in response to what event — it's in the commit history. No separate audit trail for context changes; git is the audit trail. This satisfies the observability requirements in ARCHITECTURE.md without additional infrastructure.

**Compaction is visible.** Compacting context replaces older messages with a summary. The old messages remain in git history (reachable via log), but the working state is compact. Auditors can see what was compacted, when, and why.

**Suspension with checkpoint is durable.** When the reactor checkpoints and suspends, the context is persisted in git. The reactor can be killed and restarted; it reads context from git on resume.

**Migration works.** If an agent migrates from one kernel to another, context travels as a git repo. Push and pull.

### Working State and Commits

During active operation, the reactor works with an in-memory representation of the message history for performance. Git captures snapshots at commit points. On resume after suspension, the reactor reconstructs state from the last git commit.

Commit frequency is controlled by the plugin's commit policy. The default commits on lifecycle boundaries (suspension, compaction, shutdown), matching the commit policy described in ARCHITECTURE.md's Change History section. Plugins can configure more frequent commits (per-turn, per-message) when durability requirements are higher.

### Context Window Tracking

Token usage is tracked per-message from provider usage reports. The context store maintains a running total of consumption alongside the message history.

When usage data is unavailable (errored or aborted requests), the package falls back to character-based estimation (characters / 4) calibrated against the last successful response. This heuristic is imprecise — it undercounts for CJK text and overcounts for single-character-heavy code — but it provides a usable floor for compaction decisions and wallet accounting. Providers that return usage data on error responses are preferred.

### Compaction Triggers

Compaction can be triggered by multiple conditions:

- **Capacity** — Context usage approaches the model's window limit. This is the standard trigger.
- **Cost** — The per-request input token cost exceeds a threshold. A conversation spending $0.50/request on input tokens when it could spend $0.10 after compaction is wasting wallet balance. The cost threshold is configured by the tenant or agent policy.
- **Overflow recovery** — The model rejects a request as too long. One automatic compaction attempt, then fail. No infinite loops.
- **Explicit** — The reactor plugin or an extension requests compaction directly.

### Compaction Contract

The compaction plugin receives the current message history, trigger reason, and wallet balance context. It returns a compacted message history that must satisfy:

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
- **Quota exhausted** — Provider-level usage limit hit (distinct from transient rate limits). Response: fail or switch model, depending on plugin policy.
- **Fatal** — Invalid request, unsupported model, malformed content. Response: fail immediately with diagnostic information.
- **Aborted** — Caller cancelled via AbortSignal. Response: clean termination.

The classifier inspects HTTP status codes, error response bodies, and provider-specific error message patterns. The classified error is delivered to the plugin as an `inference.error` event, and the plugin decides the response — retry, compact, switch model, suspend, or fail. The error categories inform the plugin's decision but do not hardcode the response.

### Retry Behavior

Retryable errors use exponential backoff: `baseDelay * 2^attempt`. The plugin controls maximum attempts and total budget. Failed attempts are removed from the message history before retrying — the model should not see its own error responses.

## Abort Handling

AbortSignal is threaded through every async boundary: stream parsing, tool execution, retry delays, compaction, and the reactor itself.

When abort fires, every layer produces clean termination: streams emit error events, tool executions return error results, gates resolve as denied, and the reactor exits with a terminal event. No resource leaks, no dangling promises.

### Abort Reasons

Interchange has more abort sources than a single-user tool, and the reason determines cleanup behavior:

- **User disconnect** — May or may not mean "stop." A background agent should continue. An interactive session should pause. The plugin decides.
- **Wallet exhaustion** — Hard stop. No more inference calls or paid tool invocations. Checkpoint state for resumption when funded.
- **Admin kill** — Hard stop. The kernel is shutting down. No checkpoint.
- **Session timeout** — The session channel has been idle too long. Checkpoint and suspend.
- **Credential revocation** — The creator revoked a capability. Stop exercising it immediately.

The reactor receives abort as an `abort` event with a reason. The plugin returns the appropriate cleanup action: checkpoint and suspend, terminate immediately, or signal forks to abort.

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

## Thinking and Reasoning

Extended thinking (Anthropic) and reasoning tokens (OpenAI) are supported from day one.

### Configuration

Reasoning is configured per-agent as part of the agent definition, not per-request. The agent's creator decides the reasoning posture:

- **Off** — No reasoning tokens. Cheapest, fastest.
- **On** — Reasoning enabled with a token budget.

The budget is a ceiling, not a target. The provider adapter translates the on/off + budget into the provider-specific mechanism (Anthropic's `thinking` parameter, OpenAI's `reasoning_effort`, provider-variant formats for OpenRouter, z.ai, Qwen).

The reactor plugin can override reasoning configuration per-call if needed (for example, enabling reasoning for a complex planning step and disabling it for simple tool result processing). But the default comes from the agent definition, not from the inference package.

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
