import type { WorkflowProjectionWithSources } from "@intx/types/sidecar";
import type { HarnessConfig } from "@intx/types/runtime";
import { computeWireDefinitionHash } from "@intx/types/wire-definition-hash";
import {
  enumerateInertBodies,
  pickStepInferenceSource,
  pinInertStepSources,
  WorkflowDefinitionInvalidError,
  type ApprovalSet,
} from "@intx/workflow-deploy";

export async function buildReferencedWorkflowSourcePins(args: {
  projection: WorkflowProjectionWithSources["definition"];
  config: HarnessConfig;
  operatorApprovals: ApprovalSet;
}): Promise<readonly WorkflowProjectionWithSources[]> {
  return Promise.all(
    enumerateInertBodies(args.projection).map(async (body) => {
      const sources = pinInertStepSources({
        definition: body.definition,
        workflowId: body.ref,
        context: `deployCodeSourcedWorkflow body ${body.ref}: `,
        resolveLeafSource: ({ stepId, isAgent, preference }) => {
          if (!isAgent) {
            const placeholder = args.config.sources.find(
              (source) => source.id === args.config.defaultSource,
            );
            if (placeholder === undefined) {
              throw new WorkflowDefinitionInvalidError(
                body.ref,
                `non-agent body step ${stepId} needs an inert placeholder source, but the deploy config carries no defaultSource entry to pin`,
              );
            }
            return placeholder;
          }
          return pickStepInferenceSource({
            preferred: preference,
            stepId,
            workflowId: body.ref,
            config: args.config,
            operatorApprovals: args.operatorApprovals,
          });
        },
      });
      return {
        definition: body.definition,
        sources,
        approvedWireHash: await computeWireDefinitionHash(body.definition),
      };
    }),
  );
}
