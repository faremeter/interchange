# Package-registry asset layout

Packages distributed through the asset substrate live in a
`package-registry` asset. The asset row sits on a tenant; the asset's
backing git repo holds one tarball per name+version under `tarballs/`.

```
<asset-root>/
└── tarballs/
    ├── @acme-workflow-1.0.0.tgz
    └── @acme-tools-1.2.3.tgz
```

A filename cannot hold a `/`, so a scoped package flattens its scope with
a `-` by convention (`@scope/tail` → `@scope-tail`), which keeps two scoped
packages whose tails collide distinguishable on disk.

## Asset shape

- **Asset kind**: `package-registry`.
- **Asset name**: lowercase-kebab, addressable by REST and resolvable
  via the tenant-inheritance walker. Operators choose the name.
- **Contents**: `tarballs/<basename>-<version>.tgz` files at the root
  of the `tarballs/` directory. Filenames must match the
  `<basename>-<version>.tgz` pattern the `package-registry` kind
  handler validates at push time. Each tarball's `package.json` is
  parsed and validated by the kind handler against the workspace's
  `PackageJSON` schema before the push is accepted.
- **Mount path on the sidecar**: `source-assets/<assetId>/`
  (`workflowSourceAssetMountPath` in `@intx/types/workflow-sources`). Asset
  packs land at `<assetRoot>/<mountPath>/`, so the sidecar finds the
  tarballs at `<assetRoot>/source-assets/<assetId>/tarballs/`.

## Where the manifest lives

The closure resolver walks a list of top-level packages, resolves
transitive dependencies, and produces a `ToolPackageManifest` (validated
by `@intx/types/tool-packages`). A deploy from source resolves its
workflow's closure this way and hands the manifest to the sidecar.

Manifest entries whose `source.kind` is `"asset"` carry the asset's id
plus a `package` envelope. The envelope's `format` discriminates where
the bytes live inside the asset: `"tarball"` (this document) points at a
prepackaged `.tgz` blob and carries its `path` and `integrity`;
`"source"` points at a git subtree checked out in place (used by
source-format workflow definitions, see the tool-packaging README). A
tarball entry looks like:

```json
{
  "name": "@acme/workflow",
  "version": "1.0.0",
  "source": {
    "kind": "asset",
    "assetId": "ast_tool_registry",
    "package": {
      "format": "tarball",
      "path": "tarballs/@acme-workflow-1.0.0.tgz",
      "integrity": "sha512-..."
    }
  }
}
```

The tarball fetcher joins `source.package.path` against the materialized asset
root on the sidecar to locate the bytes.

## Mount table

Materialization takes a `Map<assetId, mountPath>`, for example
`ast_tool_registry` to `source-assets/ast_tool_registry/`. It uses
the map to resolve every `kind: "asset"` entry — looking up the mount
path by `source.assetId`, then joining it with `source.package.path`
against the workspace's asset root.

## Per-instance apply state on the sidecar

Each apply materializes a workflow deployment's frozen closure into a
fixed directory under the deployment's instance directory
(`<dataDir>/workflow-definition-closures/<deployment-id>/`):

```
<instanceDir>/
└── packages/
    └── store/<name>/<version>/...
```

The workflow child evaluates the closure from
`packages/store/<name>/<version>/`. Nothing moves the tree once it is laid
out, so a package that resolves files relative to its own on-disk location
at run time — `import.meta.url`, `require.resolve()`, a call-time
`await import("./sibling.js")` — keeps resolving for the life of the
deployment.

**Retention.** A deployment keeps one closure on disk. An apply clears
`packages/` before it lays anything out.

**Crash safety.** Restoring a deployment never reads a closure back. It
applies the closure again, which clears `packages/` first, so a
half-written directory left by a crash cannot reach a child. See `packages/tool-packaging/src/workflow-closure-apply.ts`
for the apply.

## Tarballs

- Bytes are exactly what an npm registry would serve: the package tree
  rooted under `package/`, gzipped tar.
- Integrity is verified by the sidecar against the entry's
  `source.package.integrity` field before extraction. That SRI hash, in the
  closure the hub froze, is what the sidecar trusts; the asset repo's commit
  signatures are not verified at runtime.

## REST surface

| Operation            | Endpoint                                                                                |
| -------------------- | --------------------------------------------------------------------------------------- |
| Create the asset row | `POST /api/tenants/:tid/assets` with `{ kind: "package-registry", name }`               |
| Upload a tarball     | `PUT /api/tenants/:tid/assets/:assetId/tarballs/:filename` (`application/octet-stream`) |
| List tarballs        | `GET /api/tenants/:tid/assets/:assetId/tarballs`                                        |
| Delete a tarball     | `DELETE /api/tenants/:tid/assets/:assetId/tarballs/:filename`                           |

## Registry vs asset

| Concern         | Asset-sourced                                                  | Registry-sourced                                                      |
| --------------- | -------------------------------------------------------------- | --------------------------------------------------------------------- |
| Distribution    | Operator commits the tarball to the asset repo.                | Sidecar fetches at apply time from a configured registry.             |
| Trust           | Manifest integrity pin; commit signatures are not verified.    | Registry trust + manifest integrity pin.                              |
| Reproducibility | Strong — the bytes are vendored.                               | Strong as long as the registry honors immutability of `name@version`. |
| Use case        | Operator-curated tools, anything the operator wants to vendor. | Public packages, third-party tools, anything not vendored.            |

Either source kind is valid for any entry. A closure may mix both: the
top-level pin can be asset-sourced while a transitive dep is fetched
from a registry.
