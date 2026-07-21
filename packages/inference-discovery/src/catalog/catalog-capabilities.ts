import { WIRE_CAPABILITIES, type Capability } from "@intx/types";

import { isFixtureBearing, SUPPORT_MATRIX } from "./support-matrix";
import type { SupportEntry } from "./support-matrix";

const STREAMING_SUFFIX = "-streaming";

/**
 * Expands the discovery support matrix for a single `(provider, model)` into the
 * catalog capability set that tuple has proven on the wire.
 *
 * Only fixture-bearing rows contribute (see `isFixtureBearing`). The streaming
 * variant is a strict superset of its buffered base — a streaming-capable flow
 * can be collected into a buffered one — so a proven `-streaming` row lights up
 * both variants, while a proven base row lights up only itself. The result is
 * projected onto the production wire vocabulary, dropping discovery-only probes
 * (e.g. `safety-classification`) that no catalog offering can advertise.
 *
 * @param entries - The rows to expand. Defaults to the production `SUPPORT_MATRIX`;
 *   override only in tests to exercise the expansion against synthetic rows.
 */
export function catalogCapabilitiesFor(
  provider: string,
  model: string,
  entries: readonly SupportEntry[] = SUPPORT_MATRIX,
): Capability[] {
  // `proven` intentionally holds unvalidated strings: stripping the streaming
  // suffix can produce a name that is not a wire capability. The final
  // WIRE_CAPABILITIES.filter is the validation gate, so nothing here needs to
  // assert membership — do not launder these into `Capability` with a cast.
  const proven = new Set<string>();
  for (const entry of entries) {
    if (entry.provider !== provider || entry.model !== model) continue;
    if (!isFixtureBearing(entry)) continue;
    proven.add(entry.capability);
    if (entry.capability.endsWith(STREAMING_SUFFIX)) {
      proven.add(entry.capability.slice(0, -STREAMING_SUFFIX.length));
    }
  }

  // Returning a filter of WIRE_CAPABILITIES yields the result in vocabulary
  // declaration order (keeping each base beside its streaming variant), dedupes
  // for free, and drops discovery-only probes. The order is intentional, not
  // lexical — do not replace this with a sort of `proven`.
  return WIRE_CAPABILITIES.filter((capability) => proven.has(capability));
}
