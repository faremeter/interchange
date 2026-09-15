// A pre-aborted signal reaching the local child spawner.
//
// The runtime hands the spawner the parent's per-primitive abort, and four
// awaits separate the child run id allocation from the spawn call -- one of
// them a durable flush. A cancel landing anywhere in that stretch arrives
// before the spawner runs, so the spawner must consult the signal's level on
// entry. Subscribing to the edge alone would miss it, leaving the child
// uncancelled and the parent waiting on a terminal that never comes.
//
// Asserted against the spawner directly rather than through a full run: the
// production adapter's own pre-abort coverage takes the same shape, and it
// removes the race that driving this end to end would otherwise depend on.

import { describe, test, expect } from "bun:test";

import { defineWorkflow, step } from "../definition/index";
import { defineAgent } from "@intx/agent";
import type { WorkflowAuthorizeFn } from "../authorize-context";
import { createInMemorySpawnChild } from "./run-local";

const agent = defineAgent({
  id: "child-agent",
  systemPrompt: "s",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "anthropic", model: "mock-model" }] },
});

const allowAll: WorkflowAuthorizeFn = async () => ({
  effect: "allow",
  matchingGrants: [],
  resolvedBy: null,
});

const childDefinition = defineWorkflow({
  id: "child",
  trigger: { type: "manual" },
  steps: { work: step({ agent }) },
});

describe("the local child spawner", () => {
  test("refuses a pre-aborted signal instead of starting a child run", async () => {
    // The child's one step runs through the default invoker, which calls
    // authorize exactly once. A zero count is the local stand-in for the
    // host adapter's `runCalls`: proof no child run was ever started.
    let authorizeCalls = 0;
    const countingAllow: WorkflowAuthorizeFn = async () => {
      authorizeCalls += 1;
      return { effect: "allow", matchingGrants: [], resolvedBy: null };
    };
    const spawn = createInMemorySpawnChild(
      new Map([["child-ref", childDefinition]]),
      { authorize: countingAllow },
    );

    const ctrl = new AbortController();
    ctrl.abort();

    // `ctrl.abort()` with no argument leaves a DOMException as the signal's
    // reason, and the spawner rethrows that reason as-is. Match on the name
    // rather than the message: the reason-less branch builds its own
    // DOMException with a different message, and only the name is stable.
    await expect(
      spawn({
        definitionRef: "child-ref",
        childRunId: "child-1",
        input: null,
        parentRunId: "parent-1",
        parentStepId: "spawn",
        signal: ctrl.signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(authorizeCalls).toBe(0);
  });

  test("carries the abort reason when the signal supplies one", async () => {
    const spawn = createInMemorySpawnChild(
      new Map([["child-ref", childDefinition]]),
      { authorize: allowAll },
    );

    const ctrl = new AbortController();
    ctrl.abort(new Error("parent cancelled"));

    await expect(
      spawn({
        definitionRef: "child-ref",
        childRunId: "child-2",
        input: null,
        parentRunId: "parent-2",
        parentStepId: "spawn",
        signal: ctrl.signal,
        depth: 1,
        maxChildSpawnDepth: 32,
      }),
    ).rejects.toThrow("parent cancelled");
  });
});
