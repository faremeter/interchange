import { type } from "arktype";
import { Capability } from "./capability";

export const FixtureManifest = type({
  provider: "string",
  model: "string",
  capability: Capability,
  capturedAt: "string",
  "observedModelVersion?": "string | null",
  schemaVersion: "'1'",
});
export type FixtureManifest = typeof FixtureManifest.infer;
