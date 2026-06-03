# @intx/mail-memory

In-memory `MessageTransport` for single-process and test
environments. A single transport instance hosts every address in
the process; each registered address gets its own per-recipient
transport handle that signs outbound mail with the provided
`CryptoProvider` and delivers inbound mail straight to the
recipient's INBOX.

Consumed by examples, tests, and the in-process `@intx/agent`
runtime. The on-the-wire format matches `@intx/mime` so a fixture
captured here can be replayed against a real transport without
modification.

```ts
import { createInMemoryTransport } from "@intx/mail-memory";
import { createNodeCrypto, generateKeyPair } from "@intx/crypto-node";

const transport = createInMemoryTransport();
const alpha = createNodeCrypto(await generateKeyPair());
const beta = createNodeCrypto(await generateKeyPair());

transport.register("alpha@local.interchange", alpha);
transport.register("beta@local.interchange", beta);

const alphaMail = transport.getTransportFor("alpha@local.interchange");
await alphaMail.send({
  to: "beta@local.interchange",
  type: "conversation.message",
  content: "hello",
});
```

Install a `RemoteSendHandler` via `transport.setRemoteSendHandler`
to forward outbound mail for addresses the transport does not host
locally, and add one or more `MessageSentHandler`s via
`transport.addMessageSentHandler` for post-send observability.
