// Registry-map and host-platform boundary for tool-package apply.
//
// `parseToolRegistries` owns the `SIDECAR_TOOL_REGISTRIES` rules,
// including the unset-versus-empty distinction. `hostPlatform` owns
// the npm `os`/`cpu` allowlists. Callers pass the raw env string and
// the process platform and arch; neither function reads the process.

import { type } from "arktype";

import type { HostPlatform } from "./loader";
import type { RegistryConfig } from "./resolver";

// Boundary validator for the SIDECAR_TOOL_REGISTRIES env var. The
// env-wire shape carries `name` alongside the registry config so an
// operator can author the JSON as a flat array; the boundary collapses
// the array into a Map keyed by name before handing it to the loader.
const RegistryConfigEnvEntry = type({
  name: "string",
  url: "string",
  "auth?": type({
    "token?": "string",
    "basic?": type({ user: "string", pass: "string" }),
  }),
});
const RegistryConfigEnvArray = RegistryConfigEnvEntry.array();

export function parseToolRegistries(
  raw: string | undefined,
): ReadonlyMap<string, RegistryConfig> {
  if (raw === undefined) {
    return new Map([["npmjs", { url: "https://registry.npmjs.org" }]]);
  }
  // Distinguish unset from empty: an empty string almost always
  // indicates misconfig (failed CI secret expansion, a dropped
  // template value), and falling through to the npmjs default would
  // silently route tool packages through public npm. Fail loudly; the
  // recovery is `unset SIDECAR_TOOL_REGISTRIES`, not `=""`.
  if (raw.trim() === "") {
    throw new Error(
      "SIDECAR_TOOL_REGISTRIES is set but empty — unset the variable to use the default npmjs registry",
    );
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(
      `SIDECAR_TOOL_REGISTRIES is not valid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  const validated = RegistryConfigEnvArray(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `SIDECAR_TOOL_REGISTRIES failed validation: ${validated.summary}`,
    );
  }
  const out = new Map<string, RegistryConfig>();
  for (const entry of validated) {
    if (out.has(entry.name)) {
      throw new Error(
        `SIDECAR_TOOL_REGISTRIES has duplicate registry name ${JSON.stringify(entry.name)}`,
      );
    }
    const config: RegistryConfig = {
      url: entry.url,
      ...(entry.auth !== undefined ? { auth: entry.auth } : {}),
    };
    out.set(entry.name, config);
  }
  return out;
}

// npm's `os` token namespace, mirrored from Node's `process.platform`
// enum. An unknown value would silently mis-route the loader's
// platform filter, so validate at the boundary: an unknown platform
// fails boot instead of producing a quiet mis-resolution at apply
// time. Node periodically adds platforms, so a runtime bump that
// lands a new `process.platform` value fails boot until this
// allowlist is refreshed.
const KNOWN_PROCESS_PLATFORMS = new Set<string>([
  "aix",
  "android",
  "darwin",
  "freebsd",
  "haiku",
  "linux",
  "openbsd",
  "sunos",
  "win32",
  "cygwin",
  "netbsd",
]);

// npm's `cpu` token namespace, mirrored from Node's `process.arch`
// enum. Same rationale as KNOWN_PROCESS_PLATFORMS — an unknown arch
// would mis-route the loader's filter without surfacing the gap.
const KNOWN_PROCESS_ARCHS = new Set<string>([
  "arm",
  "arm64",
  "ia32",
  "loong64",
  "mips",
  "mipsel",
  "ppc64",
  "riscv64",
  "s390x",
  "x64",
]);

function assertKnownHostPlatform(platform: string): void {
  if (!KNOWN_PROCESS_PLATFORMS.has(platform)) {
    throw new Error(
      `sidecar boot: process.platform ${JSON.stringify(platform)} is not a recognized npm \`os\` token; tool-package platform filtering would be unreliable`,
    );
  }
}

function assertKnownHostArch(arch: string): void {
  if (!KNOWN_PROCESS_ARCHS.has(arch)) {
    throw new Error(
      `sidecar boot: process.arch ${JSON.stringify(arch)} is not a recognized npm \`cpu\` token; tool-package platform filtering would be unreliable`,
    );
  }
}

/**
 * Resolve the host platform token pair the loader filters manifest
 * entries against, asserting each token is one npm recognizes before
 * returning it.
 */
export function hostPlatform(platform: string, arch: string): HostPlatform {
  assertKnownHostPlatform(platform);
  assertKnownHostArch(arch);
  return { os: platform, cpu: arch };
}
