// Substrate factory the sidecar's `bin/workflow-child` hands to
// `runWorkflowChildFromProcessEnv`. The factory closes over the
// production substrate (`createAgentRepoStore`-backed `RepoStore`),
// the host-process scheduler singleton (adapted to the runtime's
// `Scheduler` shape), and the sidecar's grant-rule evaluator.
//
// The factory consumes the workflow-host's typed `SubstrateFactoryEnv`
// -- the parsed `SpawnTimeEnv` plus a narrow `substrateConfig`
// record carrying only the keys the binary listed in
// `RunWorkflowChildFromProcessEnvOpts.substrateConfigKeys`. The
// factory does not read `process.env` itself; the binary owns the
// only crossing of that boundary.

import { type } from "arktype";

import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  createAgentRepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  adaptHostScheduler,
  createWorkflowHostScheduler,
  createWorkflowSpawnChild,
  createWorkflowStepInvoker,
  type RunWorkflowChildBindings,
  type SubstrateFactory,
} from "@intx/workflow-host";

/**
 * Required substrate-config keys the sidecar's binary forwards into
 * the factory's `substrateConfig` slot. Listed here so the binary
 * passes the same names to the helper; the helper enforces
 * presence-and-non-empty against this allowlist before the factory
 * runs.
 */
export const SIDECAR_SUBSTRATE_CONFIG_KEYS = [
  "SIDECAR_DATA_DIR",
  "WORKFLOW_DEFINITION_REPO_ID",
  "WORKFLOW_DEFINITION_REF",
  "WORKFLOW_RUN_REPO_ID",
  "WORKFLOW_RUN_REF",
  "SIDECAR_SIGNING_PUBLIC_KEY",
  "SIDECAR_SIGNING_PRIVATE_KEY",
] as const;

const SubstrateConfig = type({
  SIDECAR_DATA_DIR: "string > 0",
  WORKFLOW_DEFINITION_REPO_ID: "string > 0",
  WORKFLOW_DEFINITION_REF: "string > 0",
  WORKFLOW_RUN_REPO_ID: "string > 0",
  WORKFLOW_RUN_REF: "string > 0",
  SIDECAR_SIGNING_PUBLIC_KEY: "string > 0",
  SIDECAR_SIGNING_PRIVATE_KEY: "string > 0",
}).onUndeclaredKey("ignore");

function hexDecode(hex: string, name: string): Uint8Array {
  if (hex.length % 2 !== 0) {
    throw new Error(
      `${name} must be even-length hex; got ${String(hex.length)} chars`,
    );
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i += 1) {
    const byte = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    if (Number.isNaN(byte)) {
      throw new Error(`${name} contains non-hex characters`);
    }
    out[i] = byte;
  }
  return out;
}

/**
 * Production substrate factory. The sidecar's
 * `bin/workflow-child` binary calls
 * `runWorkflowChildFromProcessEnv(createSubstrate, { substrateConfigKeys: SIDECAR_SUBSTRATE_CONFIG_KEYS })`
 * and the helper invokes this factory with the parsed env.
 *
 * Construction order:
 *   1. Narrow the `substrateConfig` record against the typed schema.
 *      A missing or empty key already threw inside the helper; this
 *      pass enforces the exact shape the factory consumes.
 *   2. Open the substrate-shaped `RepoStore` via
 *      `createAgentRepoStore` against the sidecar's data dir and
 *      Ed25519 keypair.
 *   3. Start the host-process scheduler singleton against the same
 *      substrate, then adapt it to the runtime's `Scheduler` shape.
 *   4. Construct the production `invokeStep` and `spawnChild`
 *      adapters.
 *   5. Return the `RunWorkflowChildBindings` the runtime body
 *      consumes.
 */
export const createSubstrate: SubstrateFactory = async (env) => {
  const validated = SubstrateConfig(env.substrateConfig);
  if (validated instanceof type.errors) {
    throw new Error(
      `sidecar workflow-child substrate config failed validation: ${validated.summary}`,
    );
  }

  const signingKey = {
    publicKey: hexDecode(
      validated.SIDECAR_SIGNING_PUBLIC_KEY,
      "SIDECAR_SIGNING_PUBLIC_KEY",
    ),
    privateKey: hexDecode(
      validated.SIDECAR_SIGNING_PRIVATE_KEY,
      "SIDECAR_SIGNING_PRIVATE_KEY",
    ),
  };

  const agentRepoStore = createAgentRepoStore({
    dataDir: validated.SIDECAR_DATA_DIR,
    signingKey,
  });
  const substrate = agentRepoStore.repoStore;

  const workflowRunRepoId = {
    kind: "workflow-run" as const,
    id: validated.WORKFLOW_RUN_REPO_ID,
  };
  const workflowDefinitionRepoId = {
    kind: "workflow" as const,
    id: validated.WORKFLOW_DEFINITION_REPO_ID,
  };
  const principal: WorkflowRunWorkflowProcessPrincipal = {
    kind: "workflow-process",
    deploymentId: env.spawn.deploymentId,
  };

  const hostScheduler = createWorkflowHostScheduler({
    repoStore: substrate,
    principal,
    listActiveDeployments: () => [workflowRunRepoId],
    ref: validated.WORKFLOW_RUN_REF,
    clock: () => new Date(),
  });
  await hostScheduler.start();
  const scheduler = adaptHostScheduler(hostScheduler);

  const baseInvokeStep = createWorkflowStepInvoker({
    workflowAuthorize: async () => {
      throw new Error(
        "sidecar workflow-child step invoker: workflow-typed authorize is wired by runWorkflowChild via createCredentialsBackedAuthorize; the adapter should not invoke this slot directly",
      );
    },
    buildEnv: async (_req) => {
      // The per-step inference source is carried by the deployed
      // workflow definition's step entry, but the `agent.deploy`
      // wire frame today does not yet surface a step-keyed source
      // table to this layer. The multi-step deploy-frame extension
      // is what lands the step-to-source plumbing; until then,
      // any step invocation that actually reaches this builder
      // surfaces a precise failure rather than a fabricated source.
      //
      // The substrate factory itself runs at child startup
      // regardless of whether any step ever invokes; this
      // callback only fires when the multi-step branch's
      // `trigger.fire` arrives, which is gated behind the same
      // unimplemented frame-extension.
      throw new Error(
        "sidecar workflow-child step invoker buildEnv: per-step InferenceSource resolution is not wired; the multi-step deploy frame extension lands the source-on-step plumbing",
      );
    },
  });

  // Adapt the workflow-runtime `StepInvoker` shape onto the host's
  // `ChildStepInvoker` shape. The wrapper today drops `onEvent` --
  // the production step-invoker adapter does not yet thread an
  // event firehose through the harness's send path; the event
  // funnel inside the adapter lands when the harness's emit hook is
  // wired. Holding the parameter at this boundary keeps the seam
  // explicit so the wire-up is a single point of edit.
  const invokeStep: RunWorkflowChildBindings["invokeStep"] = async (
    req,
    onEvent,
  ) => {
    void onEvent;
    return baseInvokeStep(req);
  };

  const spawnChild = createWorkflowSpawnChild({
    substrate,
    principal,
    deployRef: validated.WORKFLOW_DEFINITION_REF,
    runChild: async () => {
      throw new Error(
        "sidecar workflow-child spawnChild: child-workflow spawn is not wired; the cross-deployment child path lands with the multi-deployment supervisor wiring",
      );
    },
  });

  const bindings: RunWorkflowChildBindings = {
    substrate,
    workflowRunRepoId,
    workflowRunRef: validated.WORKFLOW_RUN_REF,
    principal,
    workflowDefinitionRepoId,
    workflowDefinitionRef: validated.WORKFLOW_DEFINITION_REF,
    invokeStep,
    spawnChild,
    scheduler,
    evaluateGrants: async ({ resource, action, grants }) => {
      const result = await evaluateGrants(
        // The credentialsSnapshot's grants are typed as
        // `readonly unknown[]` so the workflow-host package does not
        // depend on the sidecar's grant-rule grammar. The sidecar
        // owns that grammar; the cast surfaces here at the boundary
        // where the typed grant shape is known.
        // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- credentialsSnapshot.steps[*].grants is typed unknown[] at the workflow-host boundary; the sidecar owns the GrantRule grammar
        [...(grants as readonly GrantRule[])],
        resource,
        action,
      );
      return {
        effect: result.effect,
        matchingGrants: [],
        resolvedBy: null,
      };
    },
  };
  return bindings;
};
