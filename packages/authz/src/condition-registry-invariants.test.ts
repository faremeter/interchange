import { describe, test, expect } from "bun:test";

import { HUB_CONDITION_REGISTRY } from "./hub-condition-registry";
import { RUN_CHILD_CONDITION_REGISTRY } from "./run-child-condition-registry";
import { CREDENTIAL_USE_CONDITIONS } from "./tool-consumer";
import type { ConditionRegistry } from "./types";

const REGISTRIES: [string, Readonly<ConditionRegistry>][] = [
  ["HUB_CONDITION_REGISTRY", HUB_CONDITION_REGISTRY],
  ["CREDENTIAL_USE_CONDITIONS", CREDENTIAL_USE_CONDITIONS],
  ["RUN_CHILD_CONDITION_REGISTRY", RUN_CHILD_CONDITION_REGISTRY],
];

describe("condition registries", () => {
  // validateConditions passes an evaluator's message through without adding
  // the key, so each evaluator must name itself by its registry key.
  for (const [name, registry] of REGISTRIES) {
    test(`${name}: each evaluator names itself by its registry key`, () => {
      for (const [key, evaluator] of Object.entries(registry)) {
        expect(evaluator.validate(undefined)).toStartWith(`${key}: `);
      }
    });
  }

  // A run's grant rows are evaluated both on the hub and in the run child, so
  // a condition the child accepts must mean the same thing on the hub.
  test("the run child registry is a subset of the hub registry", () => {
    for (const [key, evaluator] of Object.entries(
      RUN_CHILD_CONDITION_REGISTRY,
    )) {
      expect(HUB_CONDITION_REGISTRY[key]).toBe(evaluator);
    }
  });
});
