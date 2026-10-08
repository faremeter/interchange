import { type } from "arktype";
import type { AdapterFactory } from "./adapter";

// One custom adapter entry: provider id, module specifier, and named export.
// Specifiers resolve to arbitrary code via `import()`, so they must come only
// from trusted operator config, never from tenant or deploy data.
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
 * Load each manifest entry's module and narrow its named export to an
 * {@link AdapterFactory}. Later entries override earlier ones on the same
 * provider key. Throws with the specifier (and export) on any resolution
 * failure.
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
