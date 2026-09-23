import { type } from "arktype";

// Describes one custom adapter: the provider identifier it serves, the module
// specifier to import, and the named export within that module to use as its
// factory. Specifiers are operator-config-only and resolve to arbitrary code
// via `import()`, so they must originate solely from trusted operator
// configuration, never from tenant or deploy data. The shape is validated at
// every deserialization boundary; the value is trusted operator input.
export const AdapterManifestEntry = type({
  provider: "string",
  specifier: "string",
  export: "string",
});
export type AdapterManifestEntry = typeof AdapterManifestEntry.infer;

export const AdapterManifest = AdapterManifestEntry.array();
export type AdapterManifest = typeof AdapterManifest.infer;
