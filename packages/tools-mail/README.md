# @intx/tools-mail

Mail tool runner for the agent harness. Exposes `mail_send`,
`mail_reply`, `mail_search`, `mail_read`, and `mail_wait` against
the `MessageTransport` resolved from the harness's
`RuntimeCapabilities` registry.

Consumed by `apps/sidecar` and the demo examples; the harness
package's `defineMailTools` wraps this runner as an
`AnnotatedToolFactory` so the agent's `resolveTools` aggregates
its definitions alongside other bundles.

```ts
import { createMailTools } from "@intx/tools-mail";
import { defineMailTools } from "@intx/harness";

const mailTools = createMailTools({ capabilities });
const mailFactory = defineMailTools(
  () => mailTools,
  mailTools.definitions.map((def) => ({ name: def.name })),
);
const def = defineAgent({ ..., tools: [mailFactory, posixFactory] });
```

The transport is resolved once at handler-init and held for the
deploy lifetime; the handlers do not re-consult capabilities on
each call.
