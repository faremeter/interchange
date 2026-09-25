// Project a live primitive to the agent it carries, if any.
//
// `step` and `map` are the agent-carrying kinds; every other kind returns
// null. Two consumers need the same projection: the deploy-time capability
// walk (to collect `director:` grants) and the child-loader's
// `collectDirectorIds` (to decide which director packages to import). A
// helper that is not exhaustive fails closed for a newly-added
// agent-carrying kind -- the director is declared and shipped, but the
// loader never loads it and the walk reports `unresolvable director`.
// The switch is exhaustive with a `never` assignment so a new kind fails
// at compile time rather than silently reading as agent-less.

import type { AgentDefinition, BaseEnv } from "@intx/agent";

import type { Primitive } from "./primitives";

/**
 * The agent a primitive carries, or `null` when the kind has none.
 *
 * `step` and `map` return the agent; every other kind returns null. The
 * switch is exhaustive: a newly-added primitive kind fails the `never`
 * assignment below at compile time, forcing the author to decide whether
 * it carries an agent rather than letting it read as agent-less.
 */
export function extractAgent(
  primitive: Primitive,
): AgentDefinition<BaseEnv> | null {
  switch (primitive.kind) {
    case "step":
      return primitive.agent;
    case "map":
      return primitive.step.agent;
    case "action":
    case "loop":
    case "onTrigger":
    case "gate":
    case "awaitSignal":
    case "sleep":
    case "childWorkflow":
    case "escalation":
      return null;
    default: {
      const exhaustive: never = primitive;
      throw new Error(
        `extractAgent: unhandled primitive kind ${JSON.stringify(
          (exhaustive as { kind: string }).kind,
        )}`,
      );
    }
  }
}
