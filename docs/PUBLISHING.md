# Publishing

How the `@intx/*` packages are built for and published to npm, and why the
distribution model is shaped the way it is.

## Distribution model: compiled ESM, not source

Published `@intx/*` packages ship **compiled ESM** — `.js` plus `.d.ts`
under `dist/` — not TypeScript source. A consumer on any ESM runtime
(Node, Deno, Bun) can install a package and run it without a TypeScript
toolchain of their own. Source is never shipped.

This is a deliberate, enforced policy, not an accident of the build. The
repository itself type-checks with `noEmit` and consumes each package as
`.ts` source, so the compiled `dist/` exists only to be published.

## The `intx-src` exports condition

Each package resolves to two different things depending on who imports it:
inside the repo it must resolve to `src/*.ts` (the dev loop builds no
`dist`), and for a consumer it must resolve to the compiled `dist`. A
custom [exports condition](https://nodejs.org/api/packages.html#conditional-exports)
named `intx-src` is the switch. Every non-private package's `exports`
carries three conditions, in order:

```json
"exports": {
  ".": {
    "intx-src": "./src/index.ts",
    "types": "./dist/index.d.ts",
    "default": "./dist/index.js"
  }
}
```

- **In-repo tooling** activates `intx-src`, so `@intx/*` resolves to
  TypeScript source and no build step is needed to run tests or the stack.
- **A consumer** — or any tool that does not know `intx-src` — falls
  through to `types` (for TypeScript) and `default` (compiled `dist`).

The name is deliberately custom rather than the conventional
`development`. `Bun.build` (and potentially other tools) apply
`development` by default, which would make an external `bun build` of a
dist-only package resolve to `./src/*` that is not in the tarball.
`intx-src` is selected by nothing unless explicitly configured, so
external Node/Deno/Bun consumers always get `dist`.

`bin/exports-shape.ts` is the transform that produces this shape and the
`make lint` check that keeps it: a package added later without the
three-condition shape fails the gate rather than silently regressing the
dev loop to publish semantics.

### Running the dev loop

Because the condition must be turned on explicitly, the dev loop runs
through tooling that sets it:

- `make` targets set `--conditions=intx-src` for every `bun` call (via the
  `BUN` variable) and `customConditions` for `tsc`.
- `bin/dev` runs the stack orchestrator with the condition; its child
  processes (and the sidecar's `workflow-child`) carry it too.
- `apps/admin-ui`'s vite config sets `resolve.conditions`.

Run through `make` and `bin/dev`. A bare `bun test <file>` without the flag
will not resolve `@intx/*` to source.

## Building the compiled output

`bin/build-dist.ts` emits each non-private package's `dist/`: it runs `tsc`
to produce `.js` + `.d.ts`, then rewrites the emitted relative import
specifiers to carry explicit extensions (`./x` -> `./x.js`), which Node's
ESM loader requires and `tsc` under `moduleResolution: "bundler"` does not
add. `dist/` is gitignored; it is a build artifact, produced at publish
time, never committed.

Source uses extensionless relative imports and a bundler would be the
obvious alternative, but bundling corrupts the arktype value+type
dual-name exports these packages use, so the emit is plain `tsc` plus the
rewriter, preserving module structure.

## Tarball contents

Each package declares `"files": ["dist", "README.md", "LICENSE"]` — an
allowlist, so the tarball carries only compiled output and legal text,
never source, tests, `tsconfig`, or `.tsbuildinfo`. Two packages that read
a package-root data directory at runtime add it: `@intx/db` ships
`migrations`, `@intx/inference-discovery` ships `media`.

Packages also declare `"publishConfig": { "access": "public" }` (scoped
packages default to restricted) and `"sideEffects"` (`false` so bundlers
may tree-shake, except `@intx/log`, whose entry points install a console
sink at load). `bin/publish-metadata.ts` is the transform and the
`make lint` check for all three fields.

## Releasing and publishing

Publishing is a manual, credentialed operation. The flow:

1. `bin/release [major|minor|patch]` bumps every workspace package's
   `version`, refreshes the version `bun.lock` records for each workspace
   member to match (via `bin/sync-workspace-lockfile`), commits both, and
   creates a signed `v*` tag. The lockfile refresh matters because
   `bun pm pack` resolves each `workspace:*` dependency to the version
   `bun.lock` records for the sibling — not the on-disk `package.json`
   version, which a plain `bun install` does not write back. A stale
   lockfile would freeze internal dependencies at the previous version and
   404 on npm even when every `package.json` version is correct.
2. `bin/publish` is the guarded publish path. By default it is a dry run.
   It refuses to proceed unless the working tree is clean, `HEAD` is
   exactly the release tag, every non-private package's `version` equals
   the tag, and every internal `@intx/*` dependency is expressed as
   `workspace:` or `catalog:` (so pack rewrites it to the release
   version). It then packs one internal package and confirms the rewrite
   landed on the release version — a fast check that `bun.lock` is not
   stale — before emitting `dist`, packing every target, installing the
   whole set into a scratch consumer, and loading each package. The load
   smoke asserts every package loads under Node, Bun, and Deno and that
   default resolution lands on `dist` (never the inert `intx-src` source).
   Deno runs under `--node-modules-dir=manual` so it resolves the
   npm-installed packages; a runtime not on PATH is skipped, not failed.
   Only `bin/publish --execute` runs `bun publish`, leaf-first, under the
   `faremeter-dist` npm credentials.

The version-sync-before-publish guard exists because the live `0.1.2`
packages shipped broken — published from a tree whose sibling versions
were not yet the release version, so every internal dependency froze at an
unpublished `0.0.0`.

## Tool packages

The `@intx/tools-*` packages double as loadable interchange tool packages:
a sidecar configured with a public-npm tool registry pulls one and its
`@intx/*` dependency closure and loads the compiled `dist/sidecar-bundle.js`
it ships. They are ordinary compiled packages — the closure resolver
fetches the real dependency graph from npm — with no special bundling.
`make verify-tool-load` proves a tool package and its packed closure
install and load with the factories intact.
