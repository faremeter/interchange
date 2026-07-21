import { catalogCapabilitiesFor } from "@intx/inference-discovery/catalog";
import type { Capability } from "@intx/types";

import type { CatalogOfferingSpec } from "./catalog-seed-data";

// Resolve an offering's advertised capabilities: the wire capabilities the
// discovery matrix proved for its `discoverySource` tuple (empty when the tuple
// has not been probed), plus the hand-curated model capabilities the matrix
// cannot prove. Reading the matrix through the helper here keeps the wire set
// from ever drifting from what discovery captured.
export function offeringCapabilities(
  offering: CatalogOfferingSpec,
): Capability[] {
  const wire = offering.discoverySource
    ? catalogCapabilitiesFor(
        offering.discoverySource.provider,
        offering.discoverySource.model,
      )
    : [];
  return [...wire, ...offering.curatedCapabilities];
}
