# agent-structured-payload

Build an `InboundMessage` carrying a typed `InterchangeType` payload
(here `offering.request`), deliver it to an `@intx/agent`,
and confirm via the reactor's event stream that the typed envelope
landed intact.

This example uses `offering.request` because it has the most
concrete fields; the same shape covers `payment.required`,
`approval.request`, `system.credential.refresh`, and every other
`InterchangeType` defined in `@intx/types`.

## What it shows

- Building an `InboundMessage` with `createInboundMessage` from
  `@intx/mime`, passing `payload: { type, body }` instead of
  `content: string`.
- The mail-builder's defaults — `interchangeType` is auto-derived
  from `payload.type`, `messageId` is generated, `signatureStatus`
  defaults to `"missing"`, `ref` defaults to a stub IMAP-ish UID —
  so the caller only supplies the fields that actually vary.
- Delivering the message with `agent.deliver(message)` and watching
  `agent.stream()` for the `message.received` event the reactor
  emits. The payload arrives at the reactor with its `type`,
  `version`, and `body` intact, ready for any consumer that knows
  the schema.

## Running

```bash
export ANTHROPIC_API_KEY=sk-...
cd examples/agent-structured-payload
bun run start                                # default offering
bun run start --offering-id offer-42 \
              --description "Limited beta seat" \
              --price-cents 4999 \
              --currency USD
```

Output looks like:

```
delivering offering.request:
  offeringId:  demo-offering-001
  description: Premium widget — limited release
  price:       USD 19.99
  type:        offering.request
  messageId:   <1747...@merchant>

reactor received:
  type:        offering.request
  from:        merchant@local
  messageId:   <1747...@merchant>
  payload.type:    offering.request
  payload.version: 1
  payload.body:    {"offeringId":"demo-offering-001","description":"Premium widget — limited release","priceCents":1999,"currency":"USD"}
```

To start fresh:

```bash
rm -rf ../../tmp/agent-structured-payload
```

## Walkthrough

`buildOfferingRequest` in `src/cli.ts` is the only piece of code
that touches the mail builder:

```typescript
return createInboundMessage({
  from: "merchant@local",
  to: "agent@local",
  payload: {
    type: "offering.request",
    body: {
      offeringId: args.offeringId,
      description: args.description,
      priceCents: args.priceCents,
      currency: args.currency,
    },
  },
  offeringId: args.offeringId,
});
```

A few things to notice:

1. **`payload` and `content` are mutually exclusive.** Conversation
   types (`conversation.message`, `conversation.thread.start`, etc.)
   use `content: string`. Non-conversation types use `payload:
{ type, body, version? }`. The builder rejects passing both and
   rejects passing `payload` with a conversation type.
2. **`payload.version` defaults to `"1"`.** Bump it explicitly via
   `payload.version` when an application owns a custom payload that
   has incremented.
3. **`interchangeType` is auto-derived.** The builder copies
   `payload.type` into the `Interchange-Type` header so audit
   tooling can route on a single header without parsing the body.
4. **`offeringId` is set both inside `body` and as a top-level
   field.** The top-level `offeringId` becomes the
   `Interchange-Offering-Id` header — useful for routing and
   reply-threading without unpacking the JSON body.

## Why `deliver` and not `send`?

`send(message)` waits for `connector.reply`. The default director
in `@intx/harness` calls `infer()` whenever a
`message.received` event arrives, regardless of whether the message
carries text content or a structured payload, so `send` would happily
park waiting for a reply.

The catch is that the default rendering — `createInboundTurn` in
`packages/inference/src/turns.ts` — drops structured payloads from
the prompt, because it only knows how to render `message.content`
(the text body). The model therefore never sees the payload, never
produces text in response, and `send` waits indefinitely.

This example uses `agent.deliver(message)` instead. `deliver`
forwards the message into the reactor without queuing it as a
request/response pair. The reactor:

1. Persists the inbound message in its tracking state (used by the
   correlation table, audit, and any downstream consumer that walks
   the event stream).
2. Emits a `message.received` event so subscribers can observe the
   typed payload directly.

`deliver` is the right primitive for any "drop this message on the
agent and inspect what happens" workflow — webhook ingestion, event
bus consumers, correlation resolution
([`agent-rich-tool`](../agent-rich-tool/README.md)), or, as here,
auditing the typed-message contract.

## Making the model react to a structured payload

The system intentionally separates **delivery** (what this example
demonstrates) from **rendering** (what the default director chose
not to take a position on). To make the model react to a structured
payload, supply a custom director that, on `message.received`, projects
the payload's `body` into a user turn before calling
`capabilities.infer`. Author-defined directors register via
`defineDirector(...)` and are named on `AgentDefinition.director` as a
`DirectorRef`; the env's `directors` registry resolves the ref at
`createAgent(def, env)` time.

A complete custom-director example is out of scope here, but the
shape is:

```typescript
import type {
  ReactorDirector,
  ReactorInboundEvent,
  ReactorState,
  ReactorCapabilities,
} from "@intx/types/runtime";

const renderPayloadDirector: ReactorDirector = {
  async decide(event, _state, caps): Promise<ReactorAction | ReactorAction[]> {
    if (
      event.type === "message.received" &&
      event.message.payload !== undefined
    ) {
      // Compose a system-prompt overlay that surfaces the payload.
      const body = JSON.stringify(event.message.payload.body);
      return caps.infer({
        systemPrompt: `${BASE_PROMPT}\n\nIncoming ${event.message.payload.type} payload: ${body}`,
        tools: TOOLS,
      });
    }
    return defaultDecide(event, _state, caps);
  },
};
```

That is policy code; it lives in the application, not in
`@intx/agent`.

## Beyond offering.request

The same delivery pattern handles every `InterchangeType`:

```typescript
// approval.request
createInboundMessage({
  from: "approver@local",
  to: "agent@local",
  payload: {
    type: "approval.request",
    body: { action: "transfer $1000 to alice", justification: "..." },
  },
  correlationId,
});

// payment.required
createInboundMessage({
  from: "billing@local",
  to: "agent@local",
  payload: {
    type: "payment.required",
    body: { amountCents: 1999, currency: "USD", invoice: "..." },
  },
  correlationId,
});

// system.credential.refresh
createInboundMessage({
  from: "ops@local",
  to: "agent@local",
  payload: {
    type: "system.credential.refresh",
    body: { provider: "anthropic", reason: "rotation" },
  },
});
```

Combine with [`agent-rich-tool`](../agent-rich-tool/README.md) to see
how a tool can open a gate that a `correlationId`-carrying inbound
later closes — the typed payload then doubles as the resolution
artifact.
