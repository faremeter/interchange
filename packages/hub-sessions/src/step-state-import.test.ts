import { describe, expect, test } from "bun:test";

import type { StepStateSnapshot } from "@intx/types";
import type { ConversationTurn, InferenceSource } from "@intx/types/runtime";
import type { WorkflowProjectionDefinition } from "@intx/types/sidecar";

import {
  buildStepStateSeeds,
  type StepStateSeedsResult,
} from "./step-state-import";
import { workflowRunStepSeedPath } from "./workflow-run-kind";

const RUN_ID = "run_import";
const AGENT = { modelSources: [] };

const projection: WorkflowProjectionDefinition = {
  id: "wf-import",
  triggers: [],
  stepOrder: ["intake", "review", "notify", "fanout", "rounds"],
  steps: {
    intake: { kind: "step", agent: AGENT },
    review: { kind: "step", agent: AGENT, after: ["intake"] },
    notify: { kind: "action", after: ["review"] },
    fanout: { kind: "map", step: { agent: AGENT } },
    rounds: {
      kind: "loop",
      body: {
        stepOrder: ["drafter"],
        steps: { drafter: { kind: "step", agent: AGENT } },
      },
    },
  },
};

function source(model: string): InferenceSource {
  return {
    id: `anthropic:${model}`,
    provider: "anthropic",
    baseURL: "https://inference.test",
    credentialId: "credential",
    model,
  };
}

const sources = {
  intake: [source("claude-a")],
  review: [source("claude-b")],
};

function snapshot(turns: ConversationTurn[]): StepStateSnapshot {
  return {
    version: 1,
    turns,
    tokenUsage: {
      input: 5,
      output: 7,
      cacheRead: 0,
      cacheWrite: 0,
      thinking: 2,
    },
    connectorState: null,
  };
}

const question: ConversationTurn = {
  role: "user",
  content: [{ type: "text", text: "question" }],
  timestamp: 1,
};

function assistant(model: string, text: string): ConversationTurn {
  return {
    role: "assistant",
    content: [
      { type: "thinking", thinking: `${model} thinks`, signature: "sig" },
      { type: "text", text },
    ],
    model,
    timestamp: 2,
  };
}

function seeds(stepState: Record<string, StepStateSnapshot>) {
  const result = buildStepStateSeeds({
    projection,
    sources,
    runId: RUN_ID,
    stepState,
  });
  if (!result.ok) {
    throw new Error(`unexpected refusal: ${result.reason}`);
  }
  return new Map(
    Object.entries(result.files).map(([file, body]): [string, unknown] => [
      file,
      JSON.parse(body),
    ]),
  );
}

describe("buildStepStateSeeds", () => {
  test("files a seed for each imported agent step under the deployment's run", () => {
    const intake = snapshot([question]);
    const review = snapshot([{ ...question, timestamp: 4 }]);

    expect(seeds({ intake, review })).toEqual(
      new Map<string, unknown>([
        [workflowRunStepSeedPath(RUN_ID, "intake"), intake],
        [workflowRunStepSeedPath(RUN_ID, "review"), review],
      ]),
    );
  });

  test("refuses a reply thread, including one for a single-step workflow's agent", () => {
    const withThread = {
      ...snapshot([question]),
      connectorState: {
        threadRoot: "<root@client.test>",
        lastMessageId: "<reply@workflow.test>",
        replyTo: "exfil@attacker.test",
        cc: ["other@attacker.test"],
      },
    };
    const singleStep: WorkflowProjectionDefinition = {
      id: "wf-single",
      triggers: [],
      stepOrder: ["intake"],
      steps: { intake: { kind: "step", agent: AGENT } },
    };
    const refusal = (stepId: string): StepStateSeedsResult => ({
      ok: false,
      reason: `Step state for ${JSON.stringify(stepId)} carries a reply thread; import it with connectorState null, since a step replies only to senders that mailed its own deployment`,
    });

    expect(
      buildStepStateSeeds({
        projection: singleStep,
        sources,
        runId: RUN_ID,
        stepState: { intake: withThread },
      }),
    ).toEqual(refusal("intake"));
    expect(
      buildStepStateSeeds({
        projection,
        sources,
        runId: RUN_ID,
        stepState: { review: withThread },
      }),
    ).toEqual(refusal("review"));
  });

  test("refuses a snapshot whose tool results do not follow their calls", () => {
    const orphanResult: ConversationTurn = {
      role: "user",
      content: [
        {
          type: "tool_result",
          callId: "call-trimmed",
          content: [{ type: "text", text: "found it" }],
        },
      ],
      timestamp: 2,
    };
    const result = buildStepStateSeeds({
      projection,
      sources,
      runId: RUN_ID,
      stepState: { intake: snapshot([question, orphanResult]) },
    });

    if (result.ok) throw new Error("expected the import to be refused");
    expect(result.reason).toMatch(
      /^Step state for "intake" is not a conversation its step can continue: .*no preceding tool_call/,
    );
  });

  test("keeps the thinking of the model the step runs on", () => {
    const imported = snapshot([
      question,
      assistant("claude-a", "first answer"),
      { ...question, timestamp: 3 },
      assistant("claude-a", "second answer"),
    ]);

    expect(
      seeds({ intake: imported }).get(
        workflowRunStepSeedPath(RUN_ID, "intake"),
      ),
    ).toEqual(imported);
  });

  test("refuses a snapshot another model produced", () => {
    const result = buildStepStateSeeds({
      projection,
      sources,
      runId: RUN_ID,
      stepState: {
        intake: snapshot([
          question,
          assistant("claude-a", "first answer"),
          { ...question, timestamp: 3 },
          assistant("gpt-c", "second answer"),
        ]),
      },
    });

    expect(result).toEqual({
      ok: false,
      reason: `Step state for "intake" was produced by "gpt-c", but the step runs on "claude-a"; the Hub does not convert a conversation between models, so import it into a step that runs on the same model`,
    });
  });

  test("refuses an assistant turn that records no model", () => {
    const { model: _model, ...unrecorded } = assistant("claude-a", "answer");

    const result = buildStepStateSeeds({
      projection,
      sources,
      runId: RUN_ID,
      stepState: { intake: snapshot([question, unrecorded]) },
    });

    expect(result).toEqual({
      ok: false,
      reason: `Step state for "intake" has an assistant turn that records no model, but the step runs on "claude-a"; the Hub does not convert a conversation between models, so import it into a step that runs on the same model`,
    });
  });

  test("closes a tool call the snapshot left without a result", () => {
    const call: ConversationTurn = {
      role: "assistant",
      content: [
        {
          type: "tool_call",
          id: "call-1",
          name: "lookup",
          arguments: { query: "status" },
        },
      ],
      model: "claude-a",
      timestamp: 2,
    };

    const seed = seeds({ intake: snapshot([question, call]) }).get(
      workflowRunStepSeedPath(RUN_ID, "intake"),
    );

    expect(seed).toMatchObject({
      turns: [
        question,
        call,
        {
          role: "user",
          content: [{ type: "tool_result", callId: "call-1", isError: true }],
        },
      ],
    });
  });

  test("names every step id that is not a top-level agent step", () => {
    const imported = snapshot([question]);

    expect(
      buildStepStateSeeds({
        projection,
        sources,
        runId: RUN_ID,
        stepState: {
          intake: imported,
          notify: imported,
          fanout: imported,
          drafter: imported,
          missing: imported,
        },
      }),
    ).toEqual({
      ok: false,
      reason: `Step state names "notify", "fanout", "drafter", "missing", which the workflow has no top-level agent step for`,
    });
  });
});
