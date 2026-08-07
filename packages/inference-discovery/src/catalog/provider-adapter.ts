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
// Both are looked up by brand. Callers own their miss policy — every consumer
// today throws on an unknown brand — so these accessors return `undefined`
// rather than deciding for them. The adapter
// name is returned as a bare `string`: the adapter-registry key space lives in
// the inference adapter package, and the catalog does not take a type
// dependency on it just to name one field.

const CATALOG_TO_ADAPTER: Record<string, string> = {
  anthropic: "anthropic",
  openai: "openai-compatible",
  "opencode-zen": "openai-compatible",
  "google-genai": "google-genai",
};

// Canonical base URLs, mirrored here rather than imported. `ANTHROPIC_BASE`
// and `GEMINI_BASE` come from the discovery packages' endpoint.ts. All four
// brand bases are also declared by @intx/inference-catalog's providers, a
// zero-dependency upstream package, so those values could be imported from it —
// but the mirror is kept deliberately, to hold @intx/inference-discovery free
// of any dependency on the catalog (whose own guard already depends on
// discovery; importing it back would knot the two together).
// @intx/inference-catalog's guard asserts these bases agree, so the duplication
// is checked rather than load-bearing. (`GEMINI_BASE` would also be the wrong
// value to import: it carries the `/v1beta` segment this origin-only map
// deliberately omits, see below.) The wire never recorded which endpoint served
// each capture, so every opencode-zen cell takes the primary zen/v1 relay base.
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
