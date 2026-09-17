// The data that travels through the workflow.
//
// Everything a workflow moves between steps is plain JSON: the runtime
// records it in the run's event log and resolves it through `Selector`
// paths, so it never carries a class, a closure, or a live handle. That
// makes every value crossing a step boundary external data as far as the
// code reading it is concerned, which is why the loop functions and the
// action handler validate with arktype rather than assert a shape.

import { type } from "arktype";

/**
 * One revision pass: the tagline to work on, the length it has to reach,
 * and where the accepted tagline is published. This is both the loop's
 * seed input (`input: { literal: ... }` would pin it at authoring time;
 * this workflow takes it from the trigger payload instead) and the carry
 * state threaded from one iteration to the next.
 */
export const RevisionPass = type({
  tagline: "string",
  maxWords: "number.integer > 0",
  outputPath: "string",
});
export type RevisionPass = typeof RevisionPass.infer;

/**
 * The loop body's output, as the runtime hands it to `while` and `carry`.
 *
 * A loop iteration is a child run of the body workflow, so its output is
 * the body's per-step output record keyed by step id -- not the output of
 * whichever step happened to run last. `shorten` is the body's only step;
 * `{ reply }` is what this example's step invoker returns for an agent
 * step.
 */
export const BodyOutput = type({
  shorten: { reply: "string" },
});
export type BodyOutput = typeof BodyOutput.infer;
