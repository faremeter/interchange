# @intx/types

Foundational types for the Interchange monorepo. ArkType runtime
validators, API contract types, runtime interfaces, and sidecar
wire frames. Nearly every other package imports from here, which
makes this the canonical home for any shape that crosses a package
boundary.

Each entry point pairs an ArkType validator with its inferred
TypeScript type so consumers can validate at the boundary and
trust the resulting value internally.

## Surface

The package is split into several entry points so consumers only
pull in the shapes they need:

- `@intx/types` — domain validators and shared primitives: tenants,
  principals, roles, grants, agents, sessions, approvals, wallets,
  providers, oauth clients, credentials, offerings, the model catalog
  (models, model providers, offerings, and append-only pricing, plus
  the model-requirement and invoker-preference shapes and the model
  discovery view), observability, sidecar status enums (distinct from
  the wire frames under `@intx/types/sidecar` below), agent addresses,
  hex and base64 helpers, and the `hasCode` error guard.
- `@intx/types/authz` — grant rules, condition contexts, and
  authorization result shapes shared between `@intx/authz` and the
  hub.
- `@intx/types/audit` — the `AuditAuthz`, `AuditRecord`, and
  `ErrorRecord` shapes for tool-authorization and error records.
- `@intx/types/runtime` — inference and harness contracts:
  `ContextStore`, `ToolRunner`, `ToolDefinition`, `AuditStore`,
  `InferenceSource` (the resolved provider/model/credential a call
  executes against), retry policy, director and reactor types.
- `@intx/types/runtime-capabilities` — the capability-registry
  contract harness extensions resolve against (e.g. mail transport,
  blob reader).
- `@intx/types/sidecar` — hub-sidecar WebSocket wire frames.
- `@intx/types/grant-wire` — grant-update wire frames pushed from
  the hub to the sidecar.
- `@intx/types/tool-packages` — schemas for the tool-package
  distribution path: pin shapes (`ToolPackagePin`,
  `ToolPackagePinArray`, `ToolPackagePinName`), source variants
  (`ToolPackageAssetSource`, `ToolPackageRegistrySource`,
  `ToolPackageSource`), and the deploy-pack manifest
  (`ToolPackageManifestEntry`, `ToolPackageManifest`).
- `@intx/types/package-json` — the `PackageJSON` validator for the
  subset of `package.json` fields the asset substrate and tool-package
  builders read, including the `interchange.tools` extension.
