// Boundary readers for the sidecar's env-config inputs. Validation
// rules live here so the boot edge resolves each value once and passes
// it down instead of re-reading env at non-boundary sites.

import { AdapterManifest } from "@intx/inference";
import { hexDecode } from "@intx/types";

const DEFAULT_CACHE_MAX_BYTES = 10 * 1024 * 1024 * 1024;

// The operator key the sidecar seals its at-rest credential material under
// (inference-source apiKeys, and the tool credential material store).
// Deliberately separate from the hub's `CREDENTIAL_ENCRYPTION_KEY`: the
// sidecar runs on a host it does not control as tightly, so a sidecar
// disk-plus-key compromise must not also decrypt the hub's credential
// database. Required at boot; 32 bytes, hex -- e.g. `openssl rand -hex 32`.
export function readCredentialEncryptionKey(): Uint8Array {
  const raw = process.env["SIDECAR_CREDENTIAL_ENCRYPTION_KEY"];
  if (raw === undefined || raw.trim() === "") {
    throw new Error(
      "SIDECAR_CREDENTIAL_ENCRYPTION_KEY environment variable is required",
    );
  }
  return hexDecode(raw);
}

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

// Mirrors the hub's `DEFAULT_HUB_MAX_TARBALL_BYTES`. Cap on every upstream
// registry tarball pull; raise it via `SIDECAR_REGISTRY_MAX_TARBALL_BYTES`
// for registries with larger curated tarballs.
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

// Hub-link reconnect backoff, in milliseconds. Absent or whitespace-only
// yields `undefined`, leaving the delay to the hub link's own default. A
// present value must be a positive integer; anything else throws so a typo
// fails the boot instead of silently reverting to the default. `Number`
// rejects a whole string rather than reading a leading numeric prefix, so
// "3000ms" throws instead of arriving as 3000.
//
// Production never sets this; the deploy-flow test harness sets a short
// value so reconnect-survival tests do not burn 3s of wall clock per
// dropped link. Takes the raw string rather than reading `process.env`
// itself so the validation rule is reachable from a test as a pure
// function of its input.
export function parseReconnectDelayMs(
  raw: string | undefined,
): number | undefined {
  if (raw === undefined || raw.trim() === "") return undefined;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(
      `SIDECAR_RECONNECT_DELAY_MS must be a positive integer (milliseconds), got ${raw}`,
    );
  }
  return n;
}

// Operator-configured custom inference adapter manifest. Trusted operator
// input read only from this process's environment: `import(specifier)` is
// arbitrary code execution, so a specifier must never originate from
// deploy or tenant data — the agent deploy tree carries only a `provider`
// key. Arktype-validated here and re-validated at the workflow-child
// spawn boundary as defense in depth.
//
// Unset or whitespace-only means "no custom adapters" (valid); a
// present-but-malformed value fails loud at boot.
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
