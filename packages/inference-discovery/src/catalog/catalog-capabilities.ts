import { WIRE_CAPABILITIES, type Capability } from "@intx/types";

import { isFixtureBearing, SUPPORT_MATRIX } from "./support-matrix";
import type { SupportEntry } from "./support-matrix";

const STREAMING_SUFFIX = "-streaming";

/**
 * Expands the support matrix for a single `(provider, model)` into the
 * catalog capability set that tuple has proven on the wire.
 *
 * Only fixture-bearing rows contribute. A proven `-streaming` row lights up
 * both variants (a streaming flow can be collected into a buffered one); a
 * proven base row lights up only itself. The result is projected onto the
 * production wire vocabulary, dropping discovery-only probes that no catalog
 * offering can advertise.
 *
 * Defaults to SUPPORT_MATRIX; override only in tests.
 */
export function catalogCapabilitiesFor(
  provider: string,
  model: string,
  entries: readonly SupportEntry[] = SUPPORT_MATRIX,
): Capability[] {
  // Holds unvalidated strings on purpose: stripping the streaming suffix can
  // produce a non-wire name. WIRE_CAPABILITIES.filter is the validation gate;
  // do not launder these into `Capability` with a cast.
  const proven = new Set<string>();
  for (const entry of entries) {
    if (entry.provider !== provider || entry.model !== model) continue;
    if (!isFixtureBearing(entry)) continue;
    proven.add(entry.capability);
    if (entry.capability.endsWith(STREAMING_SUFFIX)) {
      proven.add(entry.capability.slice(0, -STREAMING_SUFFIX.length));
    }
  }

  // Filtering WIRE_CAPABILITIES yields declaration order (base beside its
  // streaming variant), dedupes, and drops discovery-only probes. Order is
  // intentional — do not replace with a sort of `proven`.
  return WIRE_CAPABILITIES.filter((capability) => proven.has(capability));
}
