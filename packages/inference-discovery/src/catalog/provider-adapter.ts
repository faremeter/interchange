// The catalog names providers by brand (`SupportEntry.provider`,
// `manifest.json`'s `provider`): `anthropic`, `openai`, `opencode-zen`,
// `google-genai`. Two other per-brand facts live downstream of that name and
// are needed wherever a capture is produced or replayed:
//
//   - the adapter-registry key the replay harness drives `runInference`
//     against, which `CaptureManifest.source.provider` records. `openai` and
//     `opencode-zen` both speak the OpenAI protocol through the one
//     `openai-compatible` adapter, so the map is not injective.
//   - the API base URL the brand is captured against, which
//     `CaptureManifest.source.baseURL` records. `openai` and `opencode-zen`
//     resolve to the same adapter but hit different endpoints, so this map is
//     keyed by brand and is distinct from the adapter map above.
//
// Both are looked up by brand. Callers own their miss policy — the converter
// throws on an unknown brand, the compat replayer skips it — so these
// accessors return `undefined` rather than deciding for them. The adapter
// name is returned as a bare `string`: the adapter-registry key space lives in
// the inference adapter package, and the catalog does not take a type
// dependency on it just to name one field.

const CATALOG_TO_ADAPTER: Record<string, string> = {
  anthropic: "anthropic",
  openai: "openai-compatible",
  "opencode-zen": "openai-compatible",
  "google-genai": "google-genai",
};

// Canonical base URLs, mirrored here because their defining modules do not
// export them for reuse: ANTHROPIC_BASE / GEMINI_BASE live in the discovery
// packages' endpoint.ts, the first-party OpenAI base in the openai deployment,
// and the OpenCode Zen relay bases in bin/lib/catalog-seed-data.ts. The wire
// never recorded which endpoint served each capture, so every opencode-zen
// cell takes the primary zen/v1 relay base.
const CATALOG_TO_BASE_URL: Record<string, string> = {
  anthropic: "https://api.anthropic.com",
  // Origin only: the google-genai adapter's request path already carries the
  // `/v1beta` version segment, and `resolveURL` concatenates base + path, so a
  // base that also carried `/v1beta` would double it. This matches the runtime
  // source the production seed data configures.
  "google-genai": "https://generativelanguage.googleapis.com",
  openai: "https://api.openai.com/v1",
  "opencode-zen": "https://opencode.ai/zen/v1",
};

export function adapterForCatalogProvider(name: string): string | undefined {
  return CATALOG_TO_ADAPTER[name];
}

export function baseURLForCatalogProvider(name: string): string | undefined {
  return CATALOG_TO_BASE_URL[name];
}
