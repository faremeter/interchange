# @intx/tools-mail

Mail tool runner for the agent harness. Exposes `mail_send`,
`mail_reply`, `mail_search`, `mail_read`, and `mail_wait` against
the `MessageTransport` resolved from the harness's
`RuntimeCapabilities` registry.

Consumed by `apps/sidecar` and the demo examples; pairs with
`mergeToolRunners` from `@intx/harness` when composing the full
tool set the reactor sees.

```ts
import { createMailTools } from "@intx/tools-mail";
import { mergeToolRunners } from "@intx/harness";

const mail = createMailTools({ capabilities });
const tools = mergeToolRunners([mail, posixTools]);
```

The transport is resolved once at handler-init and held for the
deploy lifetime; the handlers do not re-consult capabilities on
each call.
