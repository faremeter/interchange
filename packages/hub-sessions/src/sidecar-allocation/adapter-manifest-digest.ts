import { sha256 } from "@intx/crypto";
import { hexEncode, type AdapterManifest } from "@intx/types";

/**
 * Identifies the adapter manifest a sidecar boots with, so the Hub can tell
 * whether a sidecar it already runs loaded the manifest a new placement needs.
 * The sidecar treats an absent manifest as an empty one, so both hash alike.
 * Entry order is kept because a later entry overrides an earlier one for the
 * same provider. Each entry is rebuilt with its keys in a fixed order, so the
 * key order of the parsed input does not change the digest.
 */
export async function adapterManifestDigest(
  manifest: AdapterManifest | undefined,
): Promise<string> {
  const entries = (manifest ?? []).map((entry) => ({
    provider: entry.provider,
    specifier: entry.specifier,
    export: entry.export,
  }));
  return hexEncode(await sha256(JSON.stringify(entries)));
}
