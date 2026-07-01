import type { HarnessRequest } from "@intx/inference-testing";

// Compile-time guard for the harness request contract. HarnessRequest is
// defined in the harness package by extending Bun's internal request
// override interface, because undici's global Request augmentation wins
// global `Request` resolution inside that package. This file is compiled
// in a context where the global `Request` resolves to Bun's platform type,
// so it is the correct place to assert that HarnessRequest stays assignable
// to the platform Request. If a bun-types upgrade drifts the internal
// override interface, this reference fails to type-check here — loudly, at
// the spot that explains why — instead of silently diverging.
type MustExtend<T extends Request> = T;
export type HarnessRequestIsPlatformRequest = MustExtend<HarnessRequest>;
