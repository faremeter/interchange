import { type Capability } from "@intx/inference-discovery/catalog";
import type {
  CaptureStep,
  CapturedResponse,
  IterateCaptureStepsOpts,
} from "@intx/inference-discovery";
import { buildResponsesURL } from "./responses-endpoint";
import {
  buildResponsesMultiTurnTurn1Body,
  buildResponsesMultiTurnTurn2Body,
  buildResponsesRequestBody,
} from "./responses-body";

const MULTI_TURN_CAPABILITIES: ReadonlySet<Capability> = new Set<Capability>([
  "function-calling-multi-turn",
]);

export function createResponsesIterator(
  baseUrl: string,
): (
  opts: IterateCaptureStepsOpts,
) => Generator<CaptureStep, void, CapturedResponse> {
  return function* iterateCaptureSteps(
    opts: IterateCaptureStepsOpts,
  ): Generator<CaptureStep, void, CapturedResponse> {
    const { model, capability, intent } = opts;
    const url = buildResponsesURL(baseUrl);

    if (MULTI_TURN_CAPABILITIES.has(capability)) {
      const turn1 = buildResponsesMultiTurnTurn1Body({ model, intent });
      const turn1Response = yield {
        kind: "json",
        url,
        body: turn1,
      };
      const turn2 = buildResponsesMultiTurnTurn2Body({
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
      body: buildResponsesRequestBody({ model, capability, intent }),
    };
  };
}
