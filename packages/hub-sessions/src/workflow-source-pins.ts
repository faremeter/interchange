import type { WorkflowProjectionWithSources } from "@intx/types/sidecar";
import type { HarnessConfig } from "@intx/types/runtime";
import {
  buildInertBodyStepSources,
  enumerateInertBodies,
  type ApprovalSet,
} from "@intx/workflow-deploy";

export function buildReferencedWorkflowSourcePins(args: {
  projection: WorkflowProjectionWithSources["definition"];
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): readonly WorkflowProjectionWithSources[] {
  return enumerateInertBodies(args.projection).map((body) => ({
    definition: body.definition,
    sources: buildInertBodyStepSources({
      definition: body.definition,
      workflowId: body.ref,
      config: args.config,
      operatorApprovals: args.operatorApprovals,
    }),
  }));
}
