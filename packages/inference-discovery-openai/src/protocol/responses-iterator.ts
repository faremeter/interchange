import type {
  CaptureStep,
  CapturedResponse,
  IterateCaptureStepsOpts,
} from "@intx/inference-discovery";
import { createCaptureIterator } from "./iterator";
import { buildResponsesURL } from "./responses-endpoint";
import {
  buildResponsesMultiTurnTurn1Body,
  buildResponsesMultiTurnTurn2Body,
  buildResponsesRequestBody,
} from "./responses-body";

export function createResponsesIterator(
  baseUrl: string,
): (
  opts: IterateCaptureStepsOpts,
) => Generator<CaptureStep, void, CapturedResponse> {
  return createCaptureIterator(baseUrl, {
    buildURL: buildResponsesURL,
    buildTurn1Body: buildResponsesMultiTurnTurn1Body,
    buildTurn2Body: buildResponsesMultiTurnTurn2Body,
    buildSingleBody: buildResponsesRequestBody,
  });
}
