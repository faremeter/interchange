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

const agent = defineAgent({
  id: "a",
  systemPrompt: "a",
  tools: [],
  capabilities: [],
  inference: { sources: [{ provider: "fake", model: "fake" }] },
});

async function replyByMailSeen(
  replyByMail: boolean | undefined,
): Promise<boolean[]> {
  const seen: boolean[] = [];
  const invokeStep: StepInvoker = async (req) => {
    seen.push(req.replyByMail);
    return { output: "done" };
  };
  const def = defineWorkflow({
    id: "step-reply-by-mail",
    trigger: { type: "manual" },
    steps: {
      s: step({
        agent,
        ...(replyByMail !== undefined ? { replyByMail } : {}),
      }),
    },
  });
  const result = await runLocal(def, {
    authorize: allowAll,
    triggerPayload: {},
    hasUpstreamSignalResolver: true,
    invokeStep,
  }).complete;
  expect(result.terminalStatus).toBe("completed");
  return seen;
}

describe("step replyByMail option", () => {
  test("an absent option reaches the invoker as false", async () => {
    expect(await replyByMailSeen(undefined)).toEqual([false]);
  });

  test("a set option reaches the invoker unchanged", async () => {
    expect(await replyByMailSeen(true)).toEqual([true]);
    expect(await replyByMailSeen(false)).toEqual([false]);
  });
});
