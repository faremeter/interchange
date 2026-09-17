// The `interchange.actions` entry point.
//
// An `action` is a step with no agent in front of it: deterministic host
// TypeScript that the runtime checkpoints like any other step. Like a
// loop's `while`/`carry`, its `handler` is a STRING the host resolves by
// export name against the module named by `interchange.actions` -- so
// `handler: "publishTagline"` binds to the export below.
//
// A deployed handler is a bare module export. It receives exactly
// `(input, ctx, signal)` and nothing else: no closure over host
// configuration, no injected services. Everything it needs has to arrive
// through `input`, which is why this workflow threads `outputPath` all the
// way from the trigger payload rather than reading it from the process.
//
// Three rules the runtime cannot enforce, and that a handler author owns:
//
//   1. Every external effect goes through `ctx.perform`. Nothing stops a
//      handler calling `writeFile` directly -- and a handler that does is
//      invisible to the capability check and replays on every resume.
//   2. Each effect is idempotent under its `effectId`, or atomic with its
//      ledger record. On a crash resume the handler BODY is replayed; only
//      the effects are deduplicated, by `effectId`, against the ledger.
//   3. The returned output is deterministic given its effects' results,
//      because that replay reconstructs it.

import { writeFile } from "node:fs/promises";
import { type } from "arktype";

import type { ActionHandler } from "@intx/workflow";

/**
 * What the `publish` action reads: the loop's carry state, which the
 * definition selects with `{ from: "steps.revise.output.carry" }`.
 * Actions get no default-input convention -- an action with no `input`
 * selector receives nothing -- so the selector is always explicit.
 */
const PublishRequest = type({
  tagline: "string",
  outputPath: "string",
  "+": "ignore",
});

export const publishTagline: ActionHandler = async (input, ctx) => {
  const request = PublishRequest.assert(input);
  await ctx.perform({
    effectId: `write:${request.outputPath}`,
    // Refused unless the action's `effect.requires` lists it. The
    // capability string is the author's own vocabulary; the deploy-time
    // walk turns it into an `effect:fs:write` grant for an operator to
    // approve.
    capability: "fs:write",
    run: async () => {
      await writeFile(request.outputPath, `${request.tagline}\n`, "utf8");
      return null;
    },
  });
  return { published: request.tagline, path: request.outputPath };
};
