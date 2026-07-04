// Boundary readers for the sidecar's env-config inputs.
//
// Centralizing the readers here keeps env-validation rules in one
// place — `SIDECAR_CACHE_MAX_BYTES` must be a positive finite
// number, with a 10 GiB default — even though the sidecar only
// invokes the reader at one site today (the orchestrator boot in
// `apps/sidecar/src/index.ts`). The harness builder receives the
// resolved value through `DefaultHarnessBuilderConfig` rather than
// re-reading env, so the boundary stays at the boot edge.

import { AdapterManifest } from "@intx/inference";

const DEFAULT_CACHE_MAX_BYTES = 10 * 1024 * 1024 * 1024;

export function readCacheMaxBytes(): number {
  const raw = process.env["SIDECAR_CACHE_MAX_BYTES"];
  if (raw === undefined || raw.trim() === "") return DEFAULT_CACHE_MAX_BYTES;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(
      `SIDECAR_CACHE_MAX_BYTES must be a positive number; got ${raw}`,
    );
  }
  return n;
}

// Mirrors the hub's `DEFAULT_HUB_MAX_TARBALL_BYTES`. The sidecar's
// HTTP-registry fetcher enforces this cap on every upstream registry
// tarball pull. An operator pointing the sidecar at a third-party
// registry whose curated tarballs run larger should raise the cap
// explicitly via `SIDECAR_REGISTRY_MAX_TARBALL_BYTES`.
const DEFAULT_REGISTRY_MAX_TARBALL_BYTES = 10 * 1024 * 1024;

export function readRegistryMaxTarballBytes(): number {
  const raw = process.env["SIDECAR_REGISTRY_MAX_TARBALL_BYTES"];
  if (raw === undefined || raw.trim() === "")
    return DEFAULT_REGISTRY_MAX_TARBALL_BYTES;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(
      `SIDECAR_REGISTRY_MAX_TARBALL_BYTES must be a positive number; got ${raw}`,
    );
  }
  return n;
}

// Operator-configured custom inference adapter manifest. The value is
// TRUSTED operator input read only from this process's environment;
// `import(specifier)` is arbitrary code execution, so a specifier must
// never originate from deploy or tenant data — the agent deploy tree
// carries only a `provider` key, never a specifier. The shape is
// arktype-validated here (and re-validated at the workflow-child spawn
// boundary as defense in depth), but the value itself is trusted.
//
// Unset or whitespace-only means "no custom adapters", a valid
// configuration — the sidecar then resolves only the statically-linked
// built-ins. A present-but-malformed value fails loud at boot: an
// opaque `JSON.parse` SyntaxError is rethrown naming the env key, and
// the parsed value is asserted against `AdapterManifest`.
export function readAdapterManifest(): AdapterManifest {
  const raw = process.env["SIDECAR_ADAPTER_MANIFEST"];
  if (raw === undefined || raw.trim() === "") return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error("SIDECAR_ADAPTER_MANIFEST is not valid JSON", { cause });
  }
  return AdapterManifest.assert(parsed);
}
