/**
 * Derive an inline body's ref from its owning workflow id and the step id that
 * carries it. This is the SINGLE owner of the `<workflowId>__<stepId>` scheme,
 * shared by every inline-body kind -- onTrigger sections, childWorkflow
 * children, and loop bodies. Refs never collide because each `__`-joined
 * segment is atomic (a step id may not itself contain `__`, enforced in
 * `normalize`) and a given step id is exactly one primitive kind. Atomic
 * segments matter once a ref spans more than two segments: a loop body's
 * `childWorkflow` grandchild is registered under
 * `<workflowId>__<loopStepId>__<childStepId>`, so without the `__`-free rule
 * that ref could alias a top-level step literally named
 * `<loopStepId>__<childStepId>`. The live rewrite/enumeration
 * and the source-ref hub's inert-body enumerator both mint refs through it,
 * and the source-ref run child re-derives the same ref when it rewrites the
 * re-evaluated closure (`run-child`). A hub that records a body's sources under
 * this ref and a run child that looks them up must agree byte-for-byte, so the
 * scheme lives in one place rather than being re-spelled at each site.
 */
export function inlineBodyRef(workflowId: string, stepId: string): string {
  return `${workflowId}__${stepId}`;
}
