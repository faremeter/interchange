# `@intx/agent` conventions

This document specifies the conventions external packages follow when
they contribute tools to an agent runtime. The runtime (`@intx/agent`)
consumes anything that fits these shapes; this file fixes the shapes.

## Tool-package convention

A package that contributes tools to an agent declares its tool entry in
`package.json` via the `interchange.tools` field:

```json
{
  "name": "@vendor/my-tools",
  "version": "1.2.3",
  "interchange": {
    "tools": "./dist/tools.js"
  }
}
```

The path resolves against the package root. The module it names is the
**tool entry module**.

### Tool entry module shape

The tool entry module's **named exports** are `AnnotatedToolFactory`
values, built via `defineTool({ id, requires?, definitions, factory })`:

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

The default export is not consumed. Any non-`AnnotatedToolFactory`
named export is ignored by the loader. The loader rejects an entry
module that has no `AnnotatedToolFactory` named exports.

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

### Tool-name namespacing

The loader synthesizes the model-facing tool name as
`<bundle.id>:<def.name>`. Package authors write bare tool names inside
the `ToolDefinition` (e.g. `read_file`); the loader prefixes with the
bundle's `id`.

Grants in the existing authz system match the matching shape:
`tool:<bundle.id>/<def.name>`. The grant evaluator does not need to
know about packages.

The model-facing form uses `:` and the grant form uses `/` on
purpose: the model never sees the `tool:` resource prefix the grant
evaluator works with, so reusing `:` in both would either force the
model-facing form to also carry the `tool:` prefix (verbose, leaks
authz internals) or have the grant form drop its `tool:` discriminator
(loses the prefix that lets the evaluator route resources by kind).
The two-character split keeps each form unambiguous within its own
layer.

### Audit provenance

Every tool invocation is tagged with the providing bundle's `id`. The
package does not need to participate; the loader records provenance
against the bundle it dispatched to.

## Env requirements

A factory's `requires: readonly string[]` enumerates the env keys it
reads at construction time, beyond `BaseEnv`'s core fields. The agent
runtime's `validateEnv` (in `@intx/agent`'s `env-validation.ts`)
asserts every declared key is present on the env before the factory
is invoked. A missing key raises `AgentEnvError` at agent-construction
time, which the loader's atomic-apply layer surfaces as a
`factory.construct.failed` deploy-apply error.

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
rather than producing a self-contained `ToolBundle`. These export
`AnnotatedPluginFactory` values, built via `definePlugin`. The loader
collects them and surfaces their results to tool factories via
`env.plugins` before any tool factory is invoked.

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
predecessors. The default sidecar harness in this repository does
exactly that (`apps/sidecar/src/default-harness.ts`), so packages
authored against the in-tree sidecar can assume the chained shape.

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
export) are breaking. A tool package's declared `interchange.tools`
module must remain compatible with the loader version it ships
against. The loader version is the `@intx/agent` major it depends on.
