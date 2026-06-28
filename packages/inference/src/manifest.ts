import { type } from "arktype";
import type { AdapterFactory } from "./adapter";

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

// Imports a module by specifier. The production importer is `import()`; tests
// inject a synthetic importer so they can exercise the loader without fixture
// modules on disk.
export type ModuleImporter = (specifier: string) => Promise<unknown>;

/**
 * Imports each manifest entry's module and narrows its named export to an
 * {@link AdapterFactory}, returning a record keyed by provider identifier.
 * Later entries override earlier ones sharing a provider key.
 *
 * Fails loud (naming the specifier, and the export where relevant) when a
 * module does not resolve to an object, the named export is missing, or the
 * named export is not a function. The importer seam defaults to `import()` and
 * is injectable for testing.
 *
 * @param manifest - Validated manifest entries to load
 * @param opts - Optional injected module importer
 * @returns A record of provider identifier to adapter factory
 */
export async function loadAdapterFactories(
  manifest: AdapterManifest,
  opts?: { import?: ModuleImporter },
): Promise<Record<string, AdapterFactory>> {
  const importer = opts?.import ?? ((specifier: string) => import(specifier));
  const factories: Record<string, AdapterFactory> = {};

  for (const entry of manifest) {
    const mod = await importer(entry.specifier);
    if (typeof mod !== "object" || mod === null) {
      throw new Error(
        `Adapter module did not resolve to an object: ${entry.specifier}`,
      );
    }

    const exported: unknown = Reflect.get(mod, entry.export);
    if (typeof exported !== "function") {
      throw new Error(
        `Adapter export is not a function: ${entry.export} from ${entry.specifier}`,
      );
    }

    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- dynamically imported factory; the call signature cannot be verified at runtime, enforced by the AdapterFactory contract
    factories[entry.provider] = exported as AdapterFactory;
  }

  return factories;
}
