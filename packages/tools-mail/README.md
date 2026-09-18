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

## Attachments

`mail_send` and `mail_reply` take an optional `attachments` array of
`{ name, contentType, content, encoding? }` on conversation messages.
`content` is plain text for text-like content types and base64 for
everything else; `encoding` (`"utf-8"` or `"base64"`) overrides that
default, except that a type which is not text-like must be base64. Attachments are validated with `validateAttachments` from
`@intx/types` (allowlist, filename, and size limits) before the
message is sent. `mail_read` surfaces received attachments as
`{ name, contentType, size, part }` in its `"full"` and `"payload"`
responses; a follow-up `mail_read` with that `part` path returns the
attachment as text (`encoding: "utf-8"`) for text-like types whose
bytes are valid UTF-8 and as base64 (`encoding: "base64"`) otherwise.
