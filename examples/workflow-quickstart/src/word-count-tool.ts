// The one tool this workflow's step agent may call.
//
// A workflow step's agent gets its tools the same way any other
// `@intx/agent` agent does: an `AnnotatedToolFactory` built by
// `defineTool`, listed in the agent's `tools`. Nothing about being
// inside a workflow -- or inside a loop body -- changes the shape.
//
// `countWords` is exported alongside the factory because the loop's
// `while` predicate measures the same thing the model measures. The
// predicate runs in the workflow runtime and the tool runs in the
// agent, so they are two callers of one function rather than two
// implementations of one rule.

import { defineTool, type BaseEnv } from "@intx/agent";

/** Bundle id for the example's tools. `defineTool` requires a namespaced id. */
export const TOOL_BUNDLE_ID = "@intx/example-workflow-quickstart/tools";

/** The model-facing tool name. */
export const WORD_COUNT_TOOL = "word_count";

/** Number of whitespace-separated words in `text`. */
export function countWords(text: string): number {
  const trimmed = text.trim();
  if (trimmed === "") return 0;
  return trimmed.split(/\s+/).length;
}

export const wordCountTool = defineTool<BaseEnv>({
  id: TOOL_BUNDLE_ID,
  // The static declaration the deploy-time capability walk reads without
  // instantiating the factory. It is what turns into the operator-facing
  // `tool:word_count` grant.
  definitions: [{ name: WORD_COUNT_TOOL }],
  factory: () => ({
    definitions: [
      {
        name: WORD_COUNT_TOOL,
        description:
          "Count the words in a piece of text. Use this to check a tagline's length before answering.",
        inputSchema: {
          type: "object",
          properties: { text: { type: "string" } },
          required: ["text"],
        },
      },
    ],
    run: async (call) => {
      const text = call.arguments["text"];
      if (typeof text !== "string") {
        return {
          callId: call.id,
          content: `${WORD_COUNT_TOOL} needs a string "text" argument`,
          isError: true,
        };
      }
      return { callId: call.id, content: String(countWords(text)) };
    },
  }),
});
