# @intx/hub-client

Browser-side client for talking to the Interchange hub. Provides a
typed `Transport` for the hub REST API, an `InstanceSession` that
streams agent activity over SSE and exposes a normalized
`InstanceEvent` history, and a small library of transforms that map
mail and turn payloads into displayable events.

`apps/admin-ui` is the only consumer today. The package is
side-effect-free at import time so it is safe to bundle into other
browser UIs.

```ts
import {
  createBrowserTransport,
  createInstanceSession,
} from "@intx/hub-client";

const transport = createBrowserTransport();

const session = createInstanceSession({
  tenantId: "tnt_1",
  instanceId: "ins_1",
  transport,
  onChange: () => render(session.events),
});

const stop = session.start();
await session.sendMail("hello");
// later: stop(); session.destroy();
```
