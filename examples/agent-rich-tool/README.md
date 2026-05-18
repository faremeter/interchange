# agent-rich-tool

Demonstrate the `pendingMarker` field on `ToolResult` — the
mechanism that lets a tool open an approval gate, a payment-pending
gate, or any other "wait for an external event before treating this
operation as complete" pattern, without blocking the agent.

A tool that needs out-of-band confirmation returns a `ToolResult`
whose `pendingMarker.correlationId` registers a gate on the
reactor's pending-operations table. The conversation continues
immediately (the model still sees the tool's `content` and can
respond to the user). Whenever someone — a human approver, a
payment webhook, another agent — delivers an `InboundMessage` whose
`interchangeCorrelationId` header matches, the reactor correlates
the message, removes the pending operation, and emits a
`message.correlated` event. Watchers can then continue the
conversation, or surface the resolution to the user.

## What it shows

- A "full handler" tool (via `tool({ definition, handler })`, not
  `stringTool`) returning a `ToolResult` with `pendingMarker`.
- The reactor registering a gate and continuing the conversation
  even though the operation is technically incomplete.
- Building an inbound approval message with
  `createInboundMessage({ correlationId })` from
  `@interchange/mime` — the same builder a production approver
  service would use.
- Delivering the message via `agent.deliver(message)` and watching
  `agent.stream()` for the matching `message.correlated` event.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-rich-tool
bun run start "transfer \$1000 to alice"
```

The prompt is required; the example does not substitute a default
because the demonstration only makes sense for a request the model
will route through `request_approval`. An empty prompt prints a
one-line usage message and exits non-zero.

Output looks like:

```
assistant: I have submitted the transfer for approval; waiting for the approver to confirm.

pending operation registered:
  correlationId: approval-toolcall-abc-9e2a...
  action:        transfer $1000 to alice

correlation resolved:
  event:         message.correlated
  correlationId: approval-toolcall-abc-9e2a...
```

To start fresh:

```bash
rm -rf ../../tmp/agent-rich-tool
```

## Walkthrough

`src/approval-tool.ts` defines `createApprovalTool`. Two things make
it a "rich" tool rather than a plain one:

1. It uses the **full** registration shape — `tool({ definition,
handler })` — instead of `stringTool`. The full form lets the
   handler return a complete `ToolResult` rather than just a string.
   The string sugar cannot set `pendingMarker`.
2. The returned `ToolResult` carries:
   - `content` — a human-readable explanation of what the tool did.
     The model sees this and can respond to the user accordingly.
   - `detail` — structured metadata. The example stamps the
     correlation ID here so the CLI can read it back without parsing
     the content text.
   - `pendingMarker` — the reactor-side handshake. `status` is
     always `"pending"` for now; `correlationId` is the routing key;
     `expectedFrom` is an optional hint about who should resolve
     the gate.

`src/cli.ts` drives the full flow:

1. **Send the prompt.** The model invokes `request_approval`; the
   handler returns the pending result; the reactor:
   - Registers the gate keyed by `pendingMarker.correlationId`.
   - Adds the operation to the persistent pending-operations list
     so a restart picks it up (see the resume example for the
     general durability story).
   - Re-prompts the model with the tool result, which produces the
     "waiting for approval" text reply.
2. **Pull the correlation ID** out of the recorded `tool_result`
   block's `detail` field. The CLI does this through `agent.history()`
   rather than relying on the tool's own state, because the model is
   the user-facing source of truth and the detail field is the
   audited record.
3. **Synthesise the approval.** `createInboundMessage` produces a
   well-formed `InboundMessage` with the correlation ID stamped into
   the `interchangeCorrelationId` header. This is the same builder
   the structured-payload example uses — see
   [`agent-structured-payload`](../agent-structured-payload/README.md).
4. **Deliver and wait.** `agent.deliver(approval)` hands the message
   to the reactor; `agent.stream()` is consumed concurrently for a
   `message.correlated` event matching the correlation ID. The
   example subscribes to the stream **before** sending so there is
   no window in which the correlation could fire and be dropped.

## Why correlate rather than block?

The agent stays responsive while operations are pending. The model
can answer follow-up questions, kick off other tools, or summarise
the in-flight requests, all while one or more `pendingMarker`-bearing
results are still outstanding. Blocking on every external
confirmation would prevent any of that.

## Beyond approval gates

The same shape covers any external-event handshake:

- **Payment pending.** Tool returns `pendingMarker` with a payment
  request ID; the payment processor's webhook handler delivers a
  `payment.completed` inbound when funds clear.
- **Human-in-the-loop tagging.** Tool requests a label; an annotator
  UI delivers a tagged inbound when the human submits.
- **Long-running compute.** Tool kicks off a job and registers a
  marker; the job runner posts back when complete.

All of these route through the same two primitives: a tool that
returns `pendingMarker`, and an inbound with a matching
`interchangeCorrelationId`. The agent surface does not need a
dedicated mechanism for each; the correlation table is the
mechanism.
