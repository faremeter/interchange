// Fixture factory for a manifest entry whose provider is not a built-in.
// The module body only declares the export; loading it must not open a
// network connection.
import type { AdapterFactory } from "../../adapter";

export const makeAdapter: AdapterFactory = () => ({
  buildRequest: () => ({ url: "fixture://system-one", headers: {}, body: "" }),
  parseResponse: () => [],
  parseJSONResponse: () => [],
});
