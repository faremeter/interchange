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
 * The loop's `while`: keep going while the tagline this pass PRODUCED is
 * still longer than the target.
 *
 * `LoopFn` is called as `while(iterationOutput, iterationInput)`, so the
 * pass's result is the first argument and the state it worked from is the
 * second. The loop settles the moment this returns false, and the
 * iteration it judged is the one the loop's step output reports as
 * `final` -- so the accepted tagline is
 * `steps.revise.output.final.shorten.reply` downstream.
 */
export const stillTooLong: LoopFn = (iterationOutput, carryState) => {
  const output = BodyOutput.assert(iterationOutput);
  const pass = RevisionPass.assert(carryState);
  return countWords(output.shorten.reply) > pass.maxWords;
};

/**
 * The loop's `carry`: the next pass works on what this pass produced.
 * Everything else about the pass (the target length, the publish
 * destination) is threaded through unchanged -- the carry state is the
 * only channel a loop iteration has to the next one.
 *
 * It does not run on the converging pass: `while` goes false first and the
 * loop settles. `steps.revise.output.carry` is therefore the state the
 * converging pass STARTED from, which is where `publish` reads the
 * destination while it reads the tagline from `final`.
 */
export const nextPass: LoopFn = (iterationOutput, carryState) => {
  const pass = RevisionPass.assert(carryState);
  const output = BodyOutput.assert(iterationOutput);
  return { ...pass, tagline: output.shorten.reply.trim() };
};
