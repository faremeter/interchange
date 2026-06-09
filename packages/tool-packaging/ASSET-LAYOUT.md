# Tool-package asset layout

Tool packages distributed through the asset substrate live in a
`package-registry` asset. The asset row sits on a tenant; the asset's
backing git repo holds one tarball per name+version under `tarballs/`.

```
<asset-root>/
└── tarballs/
    ├── @intx-tools-mail-0.1.2.tgz
    ├── @intx-tools-posix-0.1.2.tgz
    └── @intx-tools-lsp-0.1.2.tgz
```

Filenames for scoped packages flatten the scope with a `-` separator
(`@scope/tail` → `@scope-tail`) so two scoped packages whose tails
collide remain distinguishable on disk.

## Asset shape

- **Asset kind**: `package-registry`.
- **Asset name**: lowercase-kebab, addressable by REST and resolvable
  via the tenant-inheritance walker. Operators choose the name; the
  workspace's built-ins ship under `workspace-builtins`.
- **Contents**: `tarballs/<basename>-<version>.tgz` files at the root
  of the `tarballs/` directory. Filenames must match the
  `<basename>-<version>.tgz` pattern the `package-registry` kind
  handler validates at push time. Each tarball's `package.json` is
  parsed and validated by the kind handler against the workspace's
  `PackageJSON` schema before the push is accepted.
- **Mount path on the sidecar**: `package-registries/<asset.name>/`,
  resolved by the `package-registry` kind handler. Asset packs land at
  `<workspaceRoot>/<mountPath>/`, so the sidecar's loader finds the
  tarballs at `<workspaceRoot>/package-registries/<asset.name>/tarballs/`.

## Where the manifest lives

The pinned closure is the hub's responsibility: the closure resolver
walks every agent-pinned tool package, resolves transitive dependencies,
and writes a `ToolPackageManifest` (validated by
`@intx/types/tool-packages`) into the deploy pack at
`deploy/tool-packages-manifest.json`. The sidecar reads it from there
at apply time.

Manifest entries whose `source.kind` is `"asset"` carry the asset's id
plus the tarball path inside the asset tree:

```json
{
  "name": "@intx/tools-mail",
  "version": "0.1.2",
  "integrity": "sha512-...",
  "source": {
    "kind": "asset",
    "assetId": "ast_workspace_builtins",
    "path": "tarballs/@intx-tools-mail-0.1.2.tgz"
  }
}
```

The loader joins `source.path` against the materialized asset root on
the sidecar to locate the bytes.

## Mount table in the deploy pack

The hub-side session service writes a sidecar-readable mapping from
asset id to mount path into the deploy pack at
`deploy/asset-mounts.json`:

```json
{
  "assetMounts": {
    "ast_workspace_builtins": "package-registries/workspace-builtins/"
  }
}
```

The sidecar's `readDeployTree` parses this file into a
`Map<assetId, mountPath>` and hands it to the loader. The loader uses
the map to resolve every `kind: "asset"` entry — looking up the mount
path by `source.assetId`, then joining it with `source.path` against
the workspace's asset root.

## Tarballs

- Bytes are exactly what an npm registry would serve: the package tree
  rooted under `package/`, gzipped tar.
- Integrity is verified by the sidecar loader against the manifest's
  `integrity` field before extraction. The asset substrate's git-commit
  signature provides the trust root; the SRI hash provides byte-level
  integrity at materialization time.
- The workspace's bundled tarballs (`bin/build-builtins.ts`) emit a
  packed `package.json` with `dependencies`, `devDependencies`,
  `optionalDependencies`, and `exports` deleted: every workspace and
  catalog dependency is inlined into a single bundled entry, and the
  tarball carries no `exports` so it cannot be loaded by anything
  other than the tool-package loader. A consumer attempting
  `import "@intx/tools-mail"` from outside the loader would fail to
  resolve a public subpath. Operators publishing their own packages
  through the asset substrate can preserve `exports` if they want
  standalone consumability; the workspace builtins deliberately do
  not.

## REST surface

| Operation            | Endpoint                                                                                |
| -------------------- | --------------------------------------------------------------------------------------- |
| Create the asset row | `POST /api/tenants/:tid/assets` with `{ kind: "package-registry", name }`               |
| Upload a tarball     | `PUT /api/tenants/:tid/assets/:assetId/tarballs/:filename` (`application/octet-stream`) |
| List tarballs        | `GET /api/tenants/:tid/assets/:assetId/tarballs`                                        |
| Delete a tarball     | `DELETE /api/tenants/:tid/assets/:assetId/tarballs/:filename`                           |

`bin/publish-tool-packages.ts` is the reference REST client for this
surface.

## Registry vs asset

| Concern         | Asset-sourced                                                  | Registry-sourced                                                      |
| --------------- | -------------------------------------------------------------- | --------------------------------------------------------------------- |
| Distribution    | Operator commits the tarball to the asset repo.                | Sidecar fetches at apply time from a configured registry.             |
| Trust           | Git-commit signature on the asset.                             | Registry trust + manifest integrity pin.                              |
| Reproducibility | Strong — the bytes are vendored.                               | Strong as long as the registry honors immutability of `name@version`. |
| Use case        | Operator-curated tools, anything the operator wants to vendor. | Public packages, third-party tools, anything not vendored.            |

Either source kind is valid for any entry. A closure may mix both: the
top-level pin can be asset-sourced while a transitive dep is fetched
from a registry.
