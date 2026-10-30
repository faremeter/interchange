import {
  type Capability,
  type CapabilityIntent,
} from "@intx/inference-discovery/catalog";
import type {
  CaptureStep,
  CapturedResponse,
  IterateCaptureStepsOpts,
} from "@intx/inference-discovery";
import { buildEndpointURL } from "./endpoint";
import {
  buildMultiTurnTurn1Body,
  buildMultiTurnTurn2Body,
  buildRequestBody,
} from "./body";

const MULTI_TURN_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "function-calling-multi-turn",
]);

// The four wire-specific pieces of a capture iterator: the endpoint URL
// plus the three body builders. The Chat and Responses iterators share
// the multi-turn/single-step flow and differ only in these functions.
export interface CaptureBuilders<TBody> {
  buildURL: (baseUrl: string) => string;
  buildTurn1Body: (opts: { model: string; intent: CapabilityIntent }) => TBody;
  buildTurn2Body: (opts: {
    model: string;
    intent: CapabilityIntent;
    turn1Body: TBody;
    turn1Response: unknown;
  }) => TBody;
  buildSingleBody: (opts: {
    model: string;
    capability: Capability;
    intent: CapabilityIntent;
  }) => unknown;
}

export function createCaptureIterator<TBody>(
  baseUrl: string,
  builders: CaptureBuilders<TBody>,
): (
  opts: IterateCaptureStepsOpts,
) => Generator<CaptureStep, void, CapturedResponse> {
  return function* iterateCaptureSteps(
    opts: IterateCaptureStepsOpts,
  ): Generator<CaptureStep, void, CapturedResponse> {
    const { model, capability, intent } = opts;
    const url = builders.buildURL(baseUrl);

    if (MULTI_TURN_CAPABILITIES.has(capability)) {
      const turn1 = builders.buildTurn1Body({ model, intent });
      const turn1Response = yield {
        kind: "json",
        url,
        body: turn1,
      };
      const turn2 = builders.buildTurn2Body({
        model,
        intent,
        turn1Body: turn1,
        turn1Response: turn1Response.parsed,
      });
      yield {
        kind: "json",
        url,
        body: turn2,
      };
      return;
    }

    yield {
      kind: "json",
      url,
      body: builders.buildSingleBody({ model, capability, intent }),
    };
  };
}

export function createOpenaiIterator(
  baseUrl: string,
): (
  opts: IterateCaptureStepsOpts,
) => Generator<CaptureStep, void, CapturedResponse> {
  return createCaptureIterator(baseUrl, {
    buildURL: buildEndpointURL,
    buildTurn1Body: buildMultiTurnTurn1Body,
    buildTurn2Body: buildMultiTurnTurn2Body,
    buildSingleBody: buildRequestBody,
  });
}
