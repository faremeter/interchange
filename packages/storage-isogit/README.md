# @intx/storage-isogit

Isomorphic-git backed implementation of `ContextStore` and
`AuditStore`. Each agent gets its own git repository in the host filesystem;
inference state lives on a working branch, and tool-authorization audit
records live under `state/audit/`.

Consumed by `@intx/agent` and `@intx/workflow-host` for a step's
persistence, and by `@intx/hub-sessions` and `@intx/hub-agent` for the pack
helpers that move repositories between the hub and the sidecar.

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
failure; reusing the same path for different content is rejected.

Conversation checkpoints stage the context files and tool-output directory
in one Git index update. Tool-output files are processed sequentially; every
output is re-read, hashed, and compressed on each checkpoint. Spills and
checkpoints fail when tool-output is a symlink or a file rather than a
directory, and a checkpoint fails when the directory cannot be listed.

Browsers can use the included LightningFS IndexedDB adapter:

```ts
import { createBrowserIsogitStorage } from "@intx/storage-isogit/browser";

const storage = createBrowserIsogitStorage("agent-storage");
const store = await storage.createIsogitStore("/agents/example");
```

The browser adapter supplies isomorphic-git's Buffer compatibility, atomic
rename, POSIX path operations, and IndexedDB-backed file storage. Each call to
`createBrowserIsogitStorage(name)` claims a fresh volume and clears any prior
contents for that name. Call it exactly once per name during a page or worker
lifetime, then pass the returned storage object to every consumer. Reusing the
same name from another factory call, bundle, tab, or worker is unsupported and
may clear active data.

Browser storage is intentionally disposable: a page reload, tab close, or
worker restart ends the logical session, and the next owner starts from an
empty volume instead of attempting recovery. `flush()` writes the current
filesystem state to IndexedDB, but does not promise that state will survive a
new owner. IndexedDB is the storage medium, not a restart-persistence
guarantee. Use absolute repository paths; relative browser paths resolve from
`/`. Applications that require restart persistence must inject a durable
runtime through the package root and own that runtime's recovery guarantees.

The pack-send and pack-receive helpers (`createDeployPack`,
`createNegotiatedPack`, `stagePack`) produce
and consume the wire bytes that `@intx/pack-transport` chunks
across the WebSocket. A `CommitSigner` is optional; when a consumer passes
one, every commit is signed. Nothing verifies those signatures at runtime.

`stagePack` writes and indexes a received pack outside `objects/pack/` and
returns a handle. The handle's `fs` reads the staged objects, so a receiver
validates the pack before it publishes it; a rejected pack is discarded and
never enters `objects/pack/`. A crash can leave a staged pack behind, which
no reader scans; it stays until a caller runs `removeStagedPacks`. The hub's
repo store does this before the first receive for a repository in each
process.
