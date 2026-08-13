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
import { createIsogitStore } from "@intx/storage-isogit/node";

const store = await createIsogitStore("./tmp/agent-repo", signer);

// store implements both ContextStore and AuditStore -- hand it to
// the inference and tool layers as appropriate.
```

Node and Bun consumers use the `/node` entry point above. The package root is
runtime-neutral and exports `createIsogitStorage(runtime)` for hosts that
provide their own filesystem:

```ts
import type { FsClient } from "isomorphic-git";
import { createIsogitStorage } from "@intx/storage-isogit";

type IsogitRuntime = {
  fs: FsClient;
  rename(oldPath: string, newPath: string): Promise<void>;
  path: {
    join(...parts: string[]): string;
    relative(from: string, to: string): string;
    resolve(filepath: string): string;
  };
  flush?: () => Promise<void>;
};
```

The storage layer derives its other filesystem operations from `FsClient`,
including recursive directory creation and removal, existence checks, and
UTF-8 text reads. `rename` remains explicit because completed packs require an
atomic publish primitive. Persistent backends can implement `flush` to make
completed mutations durable before the API call resolves.

Audit and error record paths are append-only. Repeating a byte-identical
record is an idempotent retry, including after an uncertain persistence
failure; reusing the same path for different content is rejected. Mail commits
reconcile an uncertain ref publication before the same store accepts another
message, so a committed message cannot have its ordinal reused.

The pack-send and pack-receive helpers (`createDeployPack`,
`createNegotiatedPack`, `applyPack`, `receivePackObjects`) produce
and consume the wire bytes that `@intx/pack-transport` chunks
across the WebSocket. A `CommitSigner` is optional but required
when the consumer needs every commit to carry a verifiable
signature.

Pack receivers flush an accepted pack before checkout or ref promotion. If a
later ref or persistence operation fails, the call rejects but retains the
pack because the ref update may already be observable. Callers must treat that
error as an uncertain promotion outcome rather than assuming the ref is
unchanged; whichever ref value is visible remains backed by durable objects.
