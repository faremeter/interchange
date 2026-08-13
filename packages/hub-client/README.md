# @intx/hub-client

Browser-side client for talking to the Interchange hub. Provides a
typed `Transport` for the hub REST API, a `RunSession` that polls a
workflow run's committed event log and exposes a seq-ordered
`WorkflowRunEvent` timeline, and a small library of transforms over
that event log (terminal detection, awaited-signal resolution).

`apps/admin-ui` is the only consumer today. The package is
side-effect-free at import time so it is safe to bundle into other
browser UIs.

```ts
import { createBrowserTransport, createRunSession } from "@intx/hub-client";

const transport = createBrowserTransport();

const session = createRunSession({
  tenantId: "tnt_1",
  runId: "run_1",
  transport,
  onChange: () => render(session.events),
});

const stop = session.start();
// session.events grows as the run commits events; polling stops once
// the run reaches a terminal event (session.terminal === true).
// later: stop(); session.destroy();
```
