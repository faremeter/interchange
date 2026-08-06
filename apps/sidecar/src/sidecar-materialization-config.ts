// Shared sidecar-local materialization config helpers.
//
// The sidecar's per-step tool-package apply path
// (`tool-materialization.ts`) and the workflow-probe closure
// materializer (`workflow-closure-materialization.ts`) both resolve the
// same two boundary inputs before they construct a `@intx/tool-packaging`
// loader: the operator's registry map, and the host platform token pair
// asserted against npm's `os`/`cpu` namespaces. Centralizing them here
// keeps both call sites reading one source of truth rather than
// duplicating the env parsing and the platform allowlists.

import { type } from "arktype";
import type { HostPlatform, RegistryConfig } from "@intx/tool-packaging";

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

export function readRegistries(): ReadonlyMap<string, RegistryConfig> {
  const raw = process.env["SIDECAR_TOOL_REGISTRIES"];
  if (raw === undefined) {
    return new Map([["npmjs", { url: "https://registry.npmjs.org" }]]);
  }
  // Distinguish unset from empty. An operator setting the var to an
  // empty string almost always indicates misconfig (CI secret
  // expansion failed, a templater dropped the value). Falling through
  // to the npmjs default at that point silently routes tool packages
  // through public npm, which is precisely the misroute a custom
  // registry pin was meant to prevent. Surface the gap loudly; the
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
// enum. Any value outside this set means the host is running a Node
// build the loader's platform filter would silently mis-route — a
// pinned package whose `os` list excludes the host would not be
// excluded if `process.platform` is a token npm has never heard of.
// Validate at the boundary so an unknown platform fails the boot
// instead of producing a quiet, host-shaped mis-resolution at apply
// time.
//
// UPGRADE TAX: Node periodically adds platforms (and Bun ships
// extensions of its own). A Node/Bun major bump that lands a new
// `process.platform` value will fail boot until this allowlist is
// refreshed against the upstream enum. Sidecar operators upgrading
// the runtime should expect this as part of the cutover, not as a
// surprise regression.
const KNOWN_PROCESS_PLATFORMS = new Set<NodeJS.Platform>([
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
// Same upgrade tax applies: Node has added `loong64` and `riscv64`
// in recent releases, and future arch additions will need to be
// added here when the sidecar is rebuilt against them.
const KNOWN_PROCESS_ARCHS = new Set<NodeJS.Architecture>([
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

function assertKnownHostPlatform(platform: NodeJS.Platform): void {
  if (!KNOWN_PROCESS_PLATFORMS.has(platform)) {
    throw new Error(
      `sidecar boot: process.platform ${JSON.stringify(platform)} is not a recognized npm \`os\` token; tool-package platform filtering would be unreliable`,
    );
  }
}

function assertKnownHostArch(arch: NodeJS.Architecture): void {
  if (!KNOWN_PROCESS_ARCHS.has(arch)) {
    throw new Error(
      `sidecar boot: process.arch ${JSON.stringify(arch)} is not a recognized npm \`cpu\` token; tool-package platform filtering would be unreliable`,
    );
  }
}

/**
 * Resolve the host platform token pair the `@intx/tool-packaging` loader
 * filters manifest entries against, asserting each token is one npm
 * recognizes before returning it. Both consumers (`tool-materialization.ts`
 * and `workflow-closure-materialization.ts`) resolve the host through this
 * one helper so the allowlists and the fail-loud gate live in a single
 * place.
 */
export function resolveHostPlatform(): HostPlatform {
  assertKnownHostPlatform(process.platform);
  assertKnownHostArch(process.arch);
  return { os: process.platform, cpu: process.arch };
}
