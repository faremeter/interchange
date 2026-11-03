import { describe, test, expect } from "bun:test";

import { validateGrantConditions } from "./grant-conditions";
import { HUB_CONDITION_REGISTRY } from "./hub-condition-registry";

const WINDOW = {
  time_window: { after: "09:00", before: "17:00", timezone: "UTC" },
};
const TOOL = { tool: "tool:pkg" };

function problems(
  resource: string,
  action: string,
  conditions: Record<string, unknown> | null,
  onRunPrincipal: boolean,
) {
  return validateGrantConditions(
    { resource, action, conditions, onRunPrincipal },
    HUB_CONDITION_REGISTRY,
  );
}

describe("validateGrantConditions", () => {
  describe("a grant not on a run principal", () => {
    test("accepts every hub condition on any resource", () => {
      expect(problems("tool:search", "invoke", TOOL, false)).toEqual([]);
      expect(problems("credential:crd_1", "use", WINDOW, false)).toEqual([]);
      expect(problems("*", "*", { ...WINDOW, ...TOOL }, false)).toEqual([]);
    });

    test("reports what the hub registry rejects", () => {
      expect(problems("tool:search", "invoke", { bogus: 1 }, false)).toEqual([
        'Unknown condition: "bogus"',
      ]);
    });
  });

  describe("a grant on a run principal", () => {
    test("accepts no conditions anywhere", () => {
      expect(problems("*", "*", null, true)).toEqual([]);
    });

    test("accepts a time window on a non-credential resource", () => {
      expect(problems("tool:search", "invoke", WINDOW, true)).toEqual([]);
    });

    test("refuses a tool condition on a non-credential resource", () => {
      expect(problems("tool:search", "invoke", TOOL, true)).toEqual([
        'a workflow run cannot evaluate this grant: Unknown condition: "tool"',
      ]);
    });

    test("accepts a tool condition on credential use", () => {
      expect(problems("credential:crd_1", "use", TOOL, true)).toEqual([]);
      expect(problems("credential:*", "use", TOOL, true)).toEqual([]);
    });

    test("refuses a time window on credential use", () => {
      expect(problems("credential:crd_1", "use", WINDOW, true)).toEqual([
        'credential use cannot evaluate this grant: Unknown condition: "time_window"',
      ]);
    });

    test("a credential row with another action meets neither narrower check", () => {
      expect(problems("credential:crd_1", "read", WINDOW, true)).toEqual([]);
      expect(problems("credential:crd_1", "read", TOOL, true)).toEqual([]);
    });

    test("a wildcard resource meets both narrower checks", () => {
      expect(problems("*", "*", WINDOW, true)).toEqual([
        'credential use cannot evaluate this grant: Unknown condition: "time_window"',
      ]);
      expect(problems("*", "use", TOOL, true)).toEqual([
        'a workflow run cannot evaluate this grant: Unknown condition: "tool"',
      ]);
    });

    test("a prefix of credential: can match both kinds of resource", () => {
      expect(problems("cred*", "*", TOOL, true)).toEqual([
        'a workflow run cannot evaluate this grant: Unknown condition: "tool"',
      ]);
    });

    test("reports a hub problem once, without a narrower copy", () => {
      expect(problems("*", "*", { bogus: 1 }, true)).toEqual([
        'Unknown condition: "bogus"',
      ]);
    });
  });
});
