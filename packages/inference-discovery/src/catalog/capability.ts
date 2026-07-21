import { type } from "arktype";
import { WIRE_CAPABILITIES } from "@intx/types";

// The discovery probe vocabulary is the production wire vocabulary
// (@intx/types owns it, so production code never depends on this package) plus
// the capabilities the rig probes for but production does not yet support.
// `safety-classification` is the sole such extension today: the runtime models
// no safety-classification content block or event, and the matrix only ever
// records it as `misled`. It lives here, not in @intx/types, precisely because
// no production offering can advertise it.
export const CAPABILITIES = [
  ...WIRE_CAPABILITIES,
  "safety-classification",
  "safety-classification-streaming",
] as const;

export const Capability = type.enumerated(...CAPABILITIES);
export type Capability = typeof Capability.infer;
