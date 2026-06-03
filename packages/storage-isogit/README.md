# @intx/storage-isogit

Isomorphic-git backed implementation of `ContextStore` and
`AuditStore`. Each agent gets its own git repository on disk;
inference state lives on a working branch, the tool-authorization
audit log lives on its own branch, and mail history lives in a
dedicated audit store that commits each inbound and outbound
message.

Consumed by `@intx/agent` for in-process persistence, and by
`@intx/hub-sessions` and `@intx/hub-agent` for the agent
repositories that move between the hub and the sidecar as packs.

```ts
import { createIsogitStore } from "@intx/storage-isogit";

const store = await createIsogitStore("./tmp/agent-repo", signer);

// store implements both ContextStore and AuditStore -- hand it to
// the inference and tool layers as appropriate.
```

The pack-send and pack-receive helpers (`createDeployPack`,
`createNegotiatedPack`, `applyPack`, `receivePackObjects`) produce
and consume the wire bytes that `@intx/pack-transport` chunks
across the WebSocket. A `CommitSigner` is optional but required
when the consumer needs every commit to carry a verifiable
signature.
