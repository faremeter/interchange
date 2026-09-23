import { type } from "arktype";

import { ModelProviderPlugin } from "./catalog";

// Describes one custom adapter: the provider identifier it serves, the module
// specifier to import, and the named export within that module to use as its
// factory. Specifiers are operator-config-only and resolve to arbitrary code
// via `import()`, so they must originate solely from trusted operator
// configuration, never from tenant or deploy data. The shape is validated at
// every deserialization boundary; the value is trusted operator input.
export const AdapterManifestEntry = type({
  provider: ModelProviderPlugin,
  specifier: "string",
  export: "string",
});
export type AdapterManifestEntry = typeof AdapterManifestEntry.infer;

export const AdapterManifest = AdapterManifestEntry.array();
export type AdapterManifest = typeof AdapterManifest.infer;

// Parses the raw `SIDECAR_ADAPTER_MANIFEST` environment value. Absent or
// whitespace-only yields `undefined`; the caller decides what an omitted
// declaration means at its own boundary. A present value must be a JSON
// array of manifest entries, and anything else throws naming the variable
// so a typo fails the boot rather than silently dropping adapters.
export function parseAdapterManifestEnv(
  raw: string | undefined,
): AdapterManifest | undefined {
  if (raw === undefined || raw.trim() === "") return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    throw new Error("SIDECAR_ADAPTER_MANIFEST is not valid JSON", { cause });
  }
  const validated = AdapterManifest(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `SIDECAR_ADAPTER_MANIFEST failed validation: ${validated.summary}`,
    );
  }
  return validated;
}
