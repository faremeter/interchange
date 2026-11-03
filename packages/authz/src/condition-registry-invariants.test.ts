import { describe, test, expect } from "bun:test";

import { HUB_CONDITION_REGISTRY } from "./hub-condition-registry";
import { CREDENTIAL_USE_CONDITIONS } from "./tool-consumer";
import type { ConditionRegistry } from "./types";

const REGISTRIES: [string, Readonly<ConditionRegistry>][] = [
  ["HUB_CONDITION_REGISTRY", HUB_CONDITION_REGISTRY],
  ["CREDENTIAL_USE_CONDITIONS", CREDENTIAL_USE_CONDITIONS],
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
});
