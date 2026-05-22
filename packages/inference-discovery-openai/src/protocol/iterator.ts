import { type Capability } from "@intx/inference-discovery/catalog";
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

export function createOpenaiIterator(
  baseUrl: string,
): (
  opts: IterateCaptureStepsOpts,
) => Generator<CaptureStep, void, CapturedResponse> {
  return function* iterateCaptureSteps(
    opts: IterateCaptureStepsOpts,
  ): Generator<CaptureStep, void, CapturedResponse> {
    const { model, capability, intent } = opts;
    const url = buildEndpointURL(baseUrl);

    if (MULTI_TURN_CAPABILITIES.has(capability)) {
      const turn1 = buildMultiTurnTurn1Body({ model, intent });
      const turn1Response = yield {
        subdir: "turn-1",
        url,
        body: turn1,
      };
      const turn2 = buildMultiTurnTurn2Body({
        model,
        intent,
        turn1Body: turn1,
        turn1Response: turn1Response.parsed,
      });
      yield {
        subdir: "turn-2",
        url,
        body: turn2,
      };
      return;
    }

    yield {
      subdir: null,
      url,
      body: buildRequestBody({ model, capability, intent }),
    };
  };
}
