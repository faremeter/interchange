import { type } from "arktype";
import { type Capability, FixtureManifest } from "./catalog";

export interface BuildManifestOpts {
  provider: string;
  model: string;
  capability: Capability;
  now?: () => Date;
  observedModelVersion?: string;
}

export function buildManifest(opts: BuildManifestOpts): FixtureManifest {
  const now = opts.now ?? (() => new Date());
  const base: Record<string, unknown> = {
    provider: opts.provider,
    model: opts.model,
    capability: opts.capability,
    capturedAt: now().toISOString(),
    schemaVersion: "1",
  };
  if (opts.observedModelVersion !== undefined) {
    base.observedModelVersion = opts.observedModelVersion;
  }
  const validated = FixtureManifest(base);
  if (validated instanceof type.errors) {
    throw new Error(`Invalid manifest: ${validated.summary}`);
  }
  return validated;
}
