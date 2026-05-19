# @intx/tools-posix

POSIX-flavored tool runner for the agent harness. Provides `read_file`,
`write_file`, `edit_file`, `run_shell`, `search_files`, and `grep` with a
plugin/middleware surface for extensions.

## Construction

```ts
import { createPosixTools } from "@intx/tools-posix";

const tools = createPosixTools({
  cwd: "/path/to/agent/workspace",
});
```

Pass `plugins` to register extra tools, middleware, or dispose callbacks.

## Reading tool-output spills

`createPosixTools` accepts an optional `blobReader` that resolves
`tool-output:///{callId}` URIs to the underlying blob bytes. When a tool
result is too large to inline in the conversation, a context transform
spills the full output to the context store and returns a pointer of the
form `tool-output:///{callId}`. Pass that URI as the `path` argument to
`read_file` and the runner dispatches through the blob reader instead of
the filesystem.

```ts
import { createHarness } from "@intx/harness";
import { createPosixTools } from "@intx/tools-posix";

const harness = createHarness(harnessConfig);
const tools = createPosixTools({
  cwd: workDir,
  blobReader: harness.blobReader,
});
```

The URI scheme is strict:

- **Scheme** `tool-output:`
- **Authority** empty (three slashes)
- **Path** `/{callId}` — case is preserved
- **Query, fragment** rejected

`tool-output://abc` (two slashes) is rejected because the URL parser
lowercases hostnames, which would silently corrupt provider-assigned call
ids containing uppercase letters. Use the three-slash form
`tool-output:///abc` to keep the callId in the URI path.

A binary blob (containing a NUL byte) is refused with the same error as a
binary filesystem read. Missing blobs and malformed URIs surface as tool
errors with clear messages.

When no `blobReader` is configured, attempting to read a `tool-output:`
URI returns an error explaining that the runner is not wired for blob
reads; filesystem reads are unaffected.
