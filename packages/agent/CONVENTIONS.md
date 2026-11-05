# `@intx/agent` conventions

This document specifies the conventions external packages follow when
they contribute tools to an agent runtime. The runtime (`@intx/agent`)
consumes anything that fits these shapes; this file fixes the shapes.

## Tool-package convention

A package that contributes tools exports `AnnotatedToolFactory` values,
built via `defineTool({ id, requires?, definitions, factory })`. A workflow
definition imports them and lists them in an agent's `toolFactories`:

```ts
import { defineTool } from "@intx/agent";

export const search = defineTool({
  id: "@vendor/my-tools/search",
  requires: ["mail.transport"],
  definitions: [{ name: SEARCH_DEFINITION.name }],
  factory: (env) => ({
    definitions: [SEARCH_DEFINITION],
    run: makeSearchRunner(env),
  }),
});

export const fetch = defineTool({
  id: "@vendor/my-tools/fetch",
  definitions: [{ name: FETCH_DEFINITION.name }],
  factory: (env) => ({
    definitions: [FETCH_DEFINITION],
    run: makeFetchRunner(env),
  }),
});
```

`definitions` statically declares the tool names the factory
contributes so callers (e.g. the deploy-time capability walk) can
enumerate them without instantiating the factory.

A package may export one or many factories. Each factory's `id` must be
package-namespaced (`@vendor/pkg/name` or `pkg/name`); `defineTool`
enforces this via `validateNamespacedId`.

### Per-instance isolation

A factory is invoked **once per agent instance**, with an env scoped to
that instance. The bundle it returns — and any handler state closed
over by the bundle's `run` — belongs to that one instance.

The underlying ESM module is **safely shared** across instances on the
same process. Mutable per-instance state lives in handler closures
produced at factory invocation time, not in module-level bindings. A
package author who needs per-instance state assigns it inside `factory`
(closures captured by the returned `run`), never at module top level.

This means two agent instances on one sidecar can load the same
`package@version` without sharing a mutable module cache, even though
the module graph itself is shared. The factory is the isolation
boundary; the module is not.

### Tool names

A tool's model-facing name is the bare name in its `ToolDefinition`
(e.g. `read_file`), and grants match it as `tool:<name>`.

## Env requirements

A factory's `requires: readonly string[]` enumerates the env keys it
reads at construction time, beyond `BaseEnv`'s core fields. The agent
runtime's `validateEnv` (in `@intx/agent`'s `env-validation.ts`)
asserts every declared key is present on the env before the factory
is invoked. A missing key raises `AgentEnvError` at agent-construction
time.

The keys may name either:

- **Capability registry keys** consumed via a `RuntimeCapabilities`
  lookup (e.g. `mail.transport`, owned by `@intx/harness`'s
  `createHarnessRuntimeCapabilities`). New capabilities are added to
  `RuntimeCapabilityMap` in `@intx/types/runtime-capabilities`.
- **BaseEnv-extension fields** the host populates directly on the env
  object (e.g. the `transport` and `address` fields `@intx/harness`'s
  `MailEnv` adds for mail tools).

Both shapes coexist by design — the runtime only checks that the key
is present on env; it does not distinguish how the host produces the
value.

## Plugin factories

Some tool packages contribute extra capabilities to other tool packages
(e.g. LSP's diagnostics middleware decorates posix's edit tools)
rather than producing a self-contained `ToolBundle`. These declare an
entry in `package.json` via the `interchange.tools` field, and its named
exports are `AnnotatedPluginFactory` values, built via `definePlugin`:

```json
{
  "name": "@vendor/my-plugin",
  "version": "1.2.3",
  "interchange": {
    "tools": "./dist/tools.js"
  }
}
```

The workflow child imports the entry of each package an agent lists in
`plugins` and surfaces the results to tool factories via `env.plugins`
before any tool factory is invoked.

Tool packages that consume plugins read `env.plugins` and filter by
shape — the agent runtime does not interpret the plugin's return
type. The plugin contract between producer and consumer (what shape
the plugin returns, what key names the consumer matches on) is
package-to-package, not part of the convention itself.

**Host control over plugin chaining.** The agent runtime treats
plugin factories as opaque: it accepts whatever the host hands to
`createAgent` and surfaces the collected results to tool factories
via `env.plugins`. Hosts MAY invoke plugin factories one at a time
and re-feed each factory the prior results on `env.plugins`,
producing a chain in which each successive plugin sees its
predecessors. The workflow child in this repository does exactly that
(`packages/workflow-host/src/child/step-tools.ts`), so packages authored
against the in-tree sidecar can assume the chained shape.

Package authors that intend to consume sibling plugin results must
inspect `env.plugins` at construction and fail loudly with a clear
message when a prerequisite is missing — the consumer cannot tell
whether the host is chaining or batching, and a silently-absent
plugin would degrade the consumer to half-built state. Likewise,
consumers must not assume any particular factory order beyond
"prerequisites appear before me when the host chains"; the contract
between producer and consumer is package-to-package, not part of
this convention.

## Versioning

Changes to the entry-module shape (what `interchange.tools` may
export) are breaking. A package's declared `interchange.tools` module
must remain compatible with the `@intx/agent` major it depends on.
