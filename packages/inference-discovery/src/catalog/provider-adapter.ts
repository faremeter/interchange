// Per-brand facts needed wherever a capture is produced or replayed:
//
//   - adapter-registry key the replay harness drives `runInference` against
//     (`CaptureManifest.source.provider`). `openai` and `opencode-zen` both
//     speak the OpenAI protocol through the one `openai-compatible` adapter,
//     so the map is not injective.
//   - API base URL the brand is captured against (`source.baseURL`). `openai`
//     and `opencode-zen` hit different endpoints, so this map is keyed by
//     brand and distinct from the adapter map.
//
// Both are looked up by brand; callers own the miss policy, so these return
// `undefined` rather than deciding. The adapter name is a bare `string`: the
// key space lives in the inference adapter package and the catalog does not
// take a type dependency on it.

const CATALOG_TO_ADAPTER: Record<string, string> = {
  anthropic: "anthropic",
  openai: "openai-compatible",
  "opencode-zen": "openai-compatible",
  xai: "openai-compatible",
  "google-genai": "google-genai",
};

// Canonical base URLs, mirrored here deliberately rather than imported, to
// hold @intx/inference-discovery free of any dependency on the catalog
// (whose guard already depends on discovery; importing it back would knot
// the two together). The catalog's guard asserts the bases agree, so the
// duplication is checked, not load-bearing. The wire never recorded which
// endpoint served each capture, so every opencode-zen cell takes the primary
// zen/v1 relay base.
const CATALOG_TO_BASE_URL: Record<string, string> = {
  anthropic: "https://api.anthropic.com",
  // Origin only: the adapter's request path already carries `/v1beta`, and
  // resolveURL concatenates base + path, so a base with `/v1beta` would
  // double it.
  "google-genai": "https://generativelanguage.googleapis.com",
  openai: "https://api.openai.com/v1",
  "opencode-zen": "https://opencode.ai/zen/v1",
  xai: "https://api.x.ai/v1",
};

export function adapterForCatalogProvider(name: string): string | undefined {
  return CATALOG_TO_ADAPTER[name];
}

export function baseURLForCatalogProvider(name: string): string | undefined {
  return CATALOG_TO_BASE_URL[name];
}
