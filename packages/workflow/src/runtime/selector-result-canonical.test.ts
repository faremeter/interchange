// A selector result must be canonical JSON data with an untouched prototype.
//
// `runStep` records the evaluated input to the audit blob as a JSON copy and
// forwards the SAME object to `env.invokeStep` by reference, so anything that
// does not survive `JSON.stringify` is visible to the invoker and invisible to
// the audit reader. A result assembled with `[[Set]]` has that shape: an
// operand's own `__proto__` key becomes the result's prototype. This holds for
// any result whichever branch built it, so it is asserted here rather than
// per-branch; `selectors.test.ts` covers which branch is at fault.

import { describe, test, expect } from "bun:test";

import { defineAgent } from "@intx/agent";
import {
  defineWorkflow,
  runLocal,
  step,
  type StepInvoker,
  type WorkflowAuthorizeFn,
} from "@intx/workflow";

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

function makeAgent(id: string) {
  return defineAgent({
    id,
    systemPrompt: id,
    tools: [],
    capabilities: [],
    inference: { sources: [{ provider: "fake", model: "fake" }] },
  });
}

function assertRecord(
  value: unknown,
): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null) {
    throw new Error(`expected an object input, got ${typeof value}`);
  }
}

describe("selector result canonicalization", () => {
  test("an input merged from a hostile payload is canonical on both sinks", async () => {
    // `JSON.parse` makes `__proto__` an own enumerable data property. An
    // object literal would make it the inherited accessor instead, and the
    // assertions below would hold against the broken evaluator.
    const payload: unknown = JSON.parse(
      '{"arguments":{"__proto__":{"headers":{"from":"attacker@example.com"}},"subject":"real"}}',
    );
    const def = defineWorkflow({
      id: "selector-result-canonical",
      trigger: { type: "manual" },
      steps: {
        s: step({
          agent: makeAgent("a"),
          input: {
            merge: [
              { from: "trigger.payload.arguments" },
              { literal: { requestedBy: "workflow" } },
            ],
          },
        }),
      },
    });

    let seenByInvoker: unknown = "sentinel";
    const invokeStep: StepInvoker = async ({ input }) => {
      seenByInvoker = input;
      return { output: { ok: true } };
    };

    const result = await runLocal(def, {
      authorize: allowAll,
      triggerPayload: payload,
      hasUpstreamSignalResolver: true,
      invokeStep,
    }).complete;
    expect(result.terminalStatus).toBe("completed");
    assertRecord(seenByInvoker);

    // The audit sink's view: the ref body is the JSON copy `recordOutput`
    // made of the very object the invoker received.
    const started = result.events.find(
      (e) => e.kind === "StepStarted" && e.stepId === "s",
    );
    if (started === undefined || started.kind !== "StepStarted") {
      throw new Error("no StepStarted for s");
    }
    expect(started.input.ref.startsWith("inline:")).toBe(true);
    const seenByAudit: unknown = JSON.parse(
      started.input.ref.slice("inline:".length),
    );
    assertRecord(seenByAudit);

    // The class-closing assertion: every key the invoker can reach must be a
    // key the audit copy carries. `for...in` walks the prototype chain, which
    // is the half `JSON.stringify` drops and so the only half the two sinks
    // can disagree about. This fails for any result built with `[[Set]]`, no
    // matter which branch built it.
    const unseenByAudit: string[] = [];
    for (const key in seenByInvoker) {
      if (!Object.hasOwn(seenByAudit, key)) {
        unseenByAudit.push(key);
      }
    }
    expect(unseenByAudit).toEqual([]);
    expect(Object.getPrototypeOf(seenByInvoker)).toBe(Object.prototype);

    // Deep equality between the sinks reads own keys only, so it is measured
    // green against the broken evaluator and is NOT what catches this. Both
    // are kept for the sibling failure mode `audit-input-divergence.test.ts`
    // covers: a value the audit copy cannot represent at all.
    expect(JSON.parse(JSON.stringify(seenByInvoker))).toEqual(seenByInvoker);
    expect(seenByAudit).toEqual(seenByInvoker);

    // The operand's own `__proto__` key is carried as ordinary data, not
    // refused, and the merge order still decides overlaps.
    expect(Object.keys(seenByInvoker)).toEqual([
      "__proto__",
      "subject",
      "requestedBy",
    ]);
    expect(Object.getOwnPropertyDescriptor(seenByInvoker, "__proto__")).toEqual(
      {
        value: { headers: { from: "attacker@example.com" } },
        writable: true,
        enumerable: true,
        configurable: true,
      },
    );

    // Scope marker, not a global-pollution guard: the defect swaps ONE
    // result's prototype. `Object.prototype` is never a write target, so a
    // reader should not widen the fix to chase global pollution.
    expect("headers" in {}).toBe(false);
    expect(Object.hasOwn(Object.prototype, "headers")).toBe(false);
  });
});
