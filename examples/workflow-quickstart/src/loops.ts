// The `interchange.loops` entry point.
//
// A `loop` primitive names its `while` and `carry` as STRINGS, not
// functions, because a `WorkflowDefinition` has to stay hashable data the
// deploy substrate can ship and an operator can approve. The host resolves
// each string against the exports of the module named by
// `interchange.loops` in this package's `package.json`, BY EXPORT NAME. So
// `while: "stillTooLong"` binds to the `stillTooLong` export below, and
// renaming the export -- without renaming the ref -- breaks the deployment
// at establish time, not at authoring time.
//
// Both functions must be PURE. The runtime re-runs them on every forward
// pass and again on every crash resume, replaying them over the recorded
// inputs and outputs to re-derive where the loop got to. Their type
// (`LoopFn`) receives only data: no effect context, no authorize, no abort
// signal. There is nowhere to put a side effect, which is the point.

import type { LoopFn } from "@intx/workflow";

import { BodyOutput, RevisionPass } from "./revision-pass";
import { countWords } from "./word-count-tool";

/**
 * The loop's `while`: keep going while the tagline this pass STARTED with
 * is still longer than the target.
 *
 * Note which argument it reads. `LoopFn` is called as
 * `while(iterationOutput, iterationInput)`, and judging the iteration's
 * OUTPUT reads more naturally -- but the loop's own step output is
 * `{ outcome, iterations, carry }`, where `carry` is the converging
 * iteration's INPUT and the converging iteration's output is not exposed
 * at all. A `while` that judged the output would therefore converge on a
 * value no downstream step can read. Judging the carry state is what makes
 * `steps.revise.output.carry` the accepted tagline, which is what the
 * `publish` action consumes.
 *
 * The cost is one extra pass: the loop is a do-while, so the pass that
 * produces the accepted tagline is followed by one more pass that confirms
 * it. The body agent's system prompt tells it to return an already-short
 * tagline unchanged, so that confirming pass is cheap.
 */
export const stillTooLong: LoopFn = (_iterationOutput, carryState) => {
  const pass = RevisionPass.assert(carryState);
  return countWords(pass.tagline) > pass.maxWords;
};

/**
 * The loop's `carry`: the next pass works on what this pass produced.
 * Everything else about the pass (the target length, the publish
 * destination) is threaded through unchanged -- the carry state is the
 * only channel a loop iteration has to the next one.
 */
export const nextPass: LoopFn = (iterationOutput, carryState) => {
  const pass = RevisionPass.assert(carryState);
  const output = BodyOutput.assert(iterationOutput);
  return { ...pass, tagline: output.shorten.reply.trim() };
};
