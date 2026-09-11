import type { WorkflowProjectionWithSources } from "@intx/types/sidecar";
import type { HarnessConfig } from "@intx/types/runtime";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import {
  buildInertBodyStepSources,
  enumerateInertBodies,
  type ApprovalSet,
} from "@intx/workflow-deploy";

export async function buildReferencedWorkflowSourcePins(args: {
  projection: WorkflowProjectionWithSources["definition"];
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): Promise<readonly WorkflowProjectionWithSources[]> {
  return Promise.all(
    enumerateInertBodies(args.projection).map(async (body) => ({
      definition: body.definition,
      sources: buildInertBodyStepSources({
        definition: body.definition,
        workflowId: body.ref,
        config: args.config,
        operatorApprovals: args.operatorApprovals,
      }),
      approvedWireHash: await computeWireDefinitionHash(body.definition),
    })),
  );
}
