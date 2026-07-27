import { type } from "arktype";
import { WIRE_CAPABILITIES } from "@intx/types";

// The discovery probe vocabulary is the production wire vocabulary
// (@intx/types owns it, so production code never depends on this package) plus
// the capabilities the rig probes for but production does not yet support.
// Discovery-only extensions today: `safety-classification` (structured safety
// ratings; no production content block) and `structured-output-refusal-streaming`
// (probe for OpenAI delta.refusal under strict json_schema). Production already
// models refusal events via synthetic coverage; this capability exists so the
// support matrix can retain a fixture-bearing row for the live classifier
// outcome (currently misled). Neither is advertised by catalog offerings.
export const CAPABILITIES = [
  ...WIRE_CAPABILITIES,
  "safety-classification",
  "safety-classification-streaming",
  "structured-output-refusal-streaming",
] as const;

export const Capability = type.enumerated(...CAPABILITIES);
export type Capability = typeof Capability.infer;
