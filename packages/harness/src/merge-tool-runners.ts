// Generic, mail-agnostic merger for tool runners. Takes an arbitrary
// list of (runner + its declared definitions) and produces a single
// runner whose definitions list is the concatenation, with
// name-collision detection at construction.
//
// Retained for external callers that compose multiple bundles outside
// the `defineTool` shape -- the in-tree composition path now flows
// through `defineMailTools` / `AnnotatedToolFactory`, and the agent's
// own `resolveTools` handles definition aggregation and dispatch. No
// in-tree code path calls this helper at run time; it stays in the
// public surface because the harness ships it for hosts that prefer
// the merged-runner shape.

import type {
  ToolCall,
  ToolDefinition,
  ToolResult,
  ToolRunner,
} from "@intx/types/runtime";

/**
 * Merge an arbitrary list of tool runners into a single runner with a
 * combined `definitions` list.
 *
 * Ordering: the combined `definitions` array preserves the order of the
 * input runners, and the order of definitions within each input runner.
 * This is observable by the model through the prompt the director
 * assembles from `definitions`, so callers that care about
 * model-facing ordering control it by sequencing the input array.
 *
 * Collision: a tool name that appears in more than one input runner's
 * `definitions` throws at construction time, naming the two source
 * runner indices and the colliding name. A tool name that appears
 * twice within a single runner's `definitions` is the runner's bug;
 * this function surfaces it with a distinct message.
 *
 * Dispatch: a call whose name is not declared by any input runner
 * resolves to a result with `isError: true` and object-shaped content
 * `{ error: 'Unknown tool: "<name>"' }`. The object shape matches the
 * per-handler error shape that ToolRunner implementations across the
 * codebase use, so callers see one error shape regardless of whether
 * the failure came from dispatch or from a runner's own handler.
 *
 * Empty input throws — `mergeToolRunners` with no runners is almost
 * always a wiring bug. A caller that legitimately wants an empty
 * runner constructs one explicitly at the call site. A runner whose
 * own `definitions` array is empty is accepted: it contributes nothing
 * to the merged dispatch and is treated as the caller's choice.
 */
export function mergeToolRunners(
  runners: readonly (ToolRunner & { definitions: ToolDefinition[] })[],
): ToolRunner & { definitions: ToolDefinition[] } {
  if (runners.length === 0) {
    throw new Error("mergeToolRunners called with no runners");
  }

  const definitions: ToolDefinition[] = [];
  // Per declared name: which runner provides it (for dispatch) and at
  // which input index (so collisions can name both sides).
  const owners = new Map<string, { runner: ToolRunner; index: number }>();

  for (const [i, runner] of runners.entries()) {
    for (const def of runner.definitions) {
      const existing = owners.get(def.name);
      if (existing !== undefined) {
        if (existing.index === i) {
          throw new Error(
            `Tool name collision on "${def.name}": runners[${i}] declares it twice`,
          );
        }
        throw new Error(
          `Tool name collision on "${def.name}": registered by both runners[${existing.index}] and runners[${i}]`,
        );
      }
      owners.set(def.name, { runner, index: i });
      definitions.push(def);
    }
  }

  return {
    definitions,
    async run(call: ToolCall, signal: AbortSignal): Promise<ToolResult> {
      const entry = owners.get(call.name);
      if (entry === undefined) {
        return {
          callId: call.id,
          content: { error: `Unknown tool: "${call.name}"` },
          isError: true,
        };
      }
      return entry.runner.run(call, signal);
    },
  };
}
