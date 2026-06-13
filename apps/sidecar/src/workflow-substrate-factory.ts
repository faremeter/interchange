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
//
// The substrate's `RepoStore` is wrapped via
// `createWorkflowRunPackPushingRepoStore` so a successful workflow-run
// `writeTreePreservingPrefix` in the child fires a pack push back to
// the hub. The wrap mirrors the boot-edge facade in
// `apps/sidecar/src/index.ts`; the child-side registry is a
// single-entry map keyed by the deployment's workflow-run repo id.
// Non-`workflow-run` writes flow through unchanged.

import { type } from "arktype";

import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import {
  createAgentRepoStore,
  type RepoId,
  type RepoStore,
  type WorkflowRunWorkflowProcessPrincipal,
} from "@intx/hub-sessions";
import {
  adaptHostScheduler,
  createWorkflowHostScheduler,
  createWorkflowSpawnChild,
  createWorkflowStepInvoker,
  type RunWorkflowChildBindings,
  type SubstrateFactory,
  type SubstrateFactoryEnv,
} from "@intx/workflow-host";

import {
  createDeploymentAddressRegistry,
  createWorkflowRunPackClient,
  createWorkflowRunPackPushingRepoStore,
  type WorkflowRunPackClient,
} from "./workflow-run-pack-client";

/**
 * Required substrate-config keys the sidecar's binary forwards into
 * the factory's `substrateConfig` slot. Listed here so the binary
 * passes the same names to the helper; the helper enforces
 * presence-and-non-empty against this allowlist before the factory
 * runs.
 *
 * `HUB_WS_URL`, `SIDECAR_ID`, and `SIDECAR_TOKEN` carry the
 * hub-connection trust anchors the child needs to ship workflow-run
 * pack pushes back to the hub. The sidecar's deploy router populates
 * these via the supervisor's `substrateEnv` plumbing
 * (`multistepSubstrateEnv` on `createSidecarDeployRouter`), threaded
 * from the boot edge's own env reads.
 */
export const SIDECAR_SUBSTRATE_CONFIG_KEYS = [
  "SIDECAR_DATA_DIR",
  "WORKFLOW_DEFINITION_REPO_ID",
  "WORKFLOW_DEFINITION_REF",
  "WORKFLOW_RUN_REPO_ID",
  "WORKFLOW_RUN_REF",
  "SIDECAR_SIGNING_PUBLIC_KEY",
  "SIDECAR_SIGNING_PRIVATE_KEY",
  "HUB_WS_URL",
  "SIDECAR_ID",
  "SIDECAR_TOKEN",
] as const;

const SubstrateConfig = type({
  SIDECAR_DATA_DIR: "string > 0",
  WORKFLOW_DEFINITION_REPO_ID: "string > 0",
  WORKFLOW_DEFINITION_REF: "string > 0",
  WORKFLOW_RUN_REPO_ID: "string > 0",
  WORKFLOW_RUN_REF: "string > 0",
  SIDECAR_SIGNING_PUBLIC_KEY: "string > 0",
  SIDECAR_SIGNING_PRIVATE_KEY: "string > 0",
  HUB_WS_URL: "string > 0",
  SIDECAR_ID: "string > 0",
  SIDECAR_TOKEN: "string > 0",
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
 * Narrow surface of `HubLink.pushWorkflowRunPack` the substrate
 * factory needs. Lifting the dependency to this shape (instead of a
 * full `HubLink`) keeps the child process from needing to construct
 * the orchestrator-side WebSocket lifecycle that the boot edge owns.
 * Production wiring in the child supplies an implementation backed by
 * a hub WebSocket opened from `HUB_WS_URL` / `SIDECAR_ID` /
 * `SIDECAR_TOKEN`; until that surface lands, the default thrower
 * makes a missed wiring loud at the first workflow-run write.
 */
export type ChildHubPackSink = {
  pushWorkflowRunPack(opts: {
    agentAddress: string;
    repoId: RepoId;
    pack: Uint8Array;
    ref: string;
    commitSha: string;
  }): Promise<void>;
};

/**
 * Dependency overrides accepted by `createSidecarSubstrateFactory`.
 * Production callers omit these to get the default-thrower hub sink;
 * tests inject a recording sink so the wrap behavior is observable
 * without standing up a WebSocket.
 */
export interface SidecarSubstrateFactoryDeps {
  /**
   * Construct the child's hub-link-equivalent for the workflow-run
   * pack push surface. Receives the validated substrate config so an
   * implementation can read `HUB_WS_URL`, `SIDECAR_ID`, and
   * `SIDECAR_TOKEN` to open its own connection.
   *
   * The default returns a sink whose `pushWorkflowRunPack` throws so
   * any production deploy that reaches a workflow-run commit before
   * the child-side hub WebSocket is wired surfaces a structured
   * failure rather than silently dropping the push.
   */
  createHubPackSink?: (config: {
    hubWsUrl: string;
    sidecarId: string;
    sidecarToken: string;
  }) => ChildHubPackSink;
  /**
   * Override the bare-store constructor. Production callers omit this
   * to get the `createAgentRepoStore`-backed `RepoStore` against
   * `SIDECAR_DATA_DIR`; tests inject an in-memory recording stub so
   * the wrap can be exercised without standing up an on-disk
   * substrate.
   */
  createBareRepoStore?: (config: {
    dataDir: string;
    signingKey: { publicKey: Uint8Array; privateKey: Uint8Array };
  }) => RepoStore;
}

function defaultCreateHubPackSink(): ChildHubPackSink {
  return {
    pushWorkflowRunPack(): Promise<void> {
      return Promise.reject(
        new Error(
          "sidecar workflow-child hub pack sink: child-side hub WebSocket is not wired; the multi-step deploy path's workflow-run pack push cannot reach the hub until the child binary constructs a real `pushWorkflowRunPack` implementation from HUB_WS_URL / SIDECAR_ID / SIDECAR_TOKEN",
        ),
      );
    },
  };
}

/**
 * Build a `SubstrateFactory` closed over the supplied dependency
 * overrides. The production export `createSubstrate` is the
 * default-deps call; tests inject a recording `createHubPackSink` to
 * observe the wrap behavior end-to-end without standing up a hub
 * WebSocket.
 *
 * Construction order:
 *   1. Narrow the `substrateConfig` record against the typed schema.
 *      A missing or empty key already threw inside the helper; this
 *      pass enforces the exact shape the factory consumes.
 *   2. Open the substrate-shaped `RepoStore` via
 *      `createAgentRepoStore` against the sidecar's data dir and
 *      Ed25519 keypair.
 *   3. Construct the child-side `WorkflowRunPackClient` against the
 *      bare `RepoStore` and the dep-supplied hub sink, build a
 *      single-entry `DeploymentAddressRegistry`
 *      (`workflowRunRepoId.id -> deploymentMailAddress`), and wrap the
 *      store via `createWorkflowRunPackPushingRepoStore` so a
 *      successful workflow-run write fires the pack push hook.
 *   4. Start the host-process scheduler singleton against the wrapped
 *      substrate, then adapt it to the runtime's `Scheduler` shape.
 *   5. Construct the production `invokeStep` and `spawnChild`
 *      adapters.
 *   6. Return the `RunWorkflowChildBindings` the runtime body
 *      consumes, with the wrapped store in the `substrate` slot.
 */
export function createSidecarSubstrateFactory(
  deps: SidecarSubstrateFactoryDeps = {},
): SubstrateFactory {
  const createHubPackSink =
    deps.createHubPackSink ?? (() => defaultCreateHubPackSink());
  const createBareRepoStore =
    deps.createBareRepoStore ??
    (({ dataDir, signingKey }) =>
      createAgentRepoStore({ dataDir, signingKey }).repoStore);

  return async (env: SubstrateFactoryEnv) => {
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

    const bareStore: RepoStore = createBareRepoStore({
      dataDir: validated.SIDECAR_DATA_DIR,
      signingKey,
    });

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

    // Child-side pack-push wrap. The registry maps the deployment's
    // workflow-run repo id back to the agent address the hub uses to
    // route the outbound pack; the child only ever serves one
    // deployment, so the registry is a single-entry constant.
    const hubPackSink = createHubPackSink({
      hubWsUrl: validated.HUB_WS_URL,
      sidecarId: validated.SIDECAR_ID,
      sidecarToken: validated.SIDECAR_TOKEN,
    });
    const packClient: WorkflowRunPackClient = createWorkflowRunPackClient({
      substrate: bareStore,
      hubLink: { pushWorkflowRunPack: hubPackSink.pushWorkflowRunPack },
    });
    const deploymentAddressRegistry = createDeploymentAddressRegistry();
    deploymentAddressRegistry.record(
      workflowRunRepoId.id,
      env.spawn.mailboxAddress,
    );
    const substrate: RepoStore = createWorkflowRunPackPushingRepoStore({
      underlying: bareStore,
      packClient,
      registry: deploymentAddressRegistry,
    });

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
}

/**
 * Production substrate factory. The sidecar's
 * `bin/workflow-child` binary calls
 * `runWorkflowChildFromProcessEnv(createSubstrate, { substrateConfigKeys: SIDECAR_SUBSTRATE_CONFIG_KEYS })`
 * and the helper invokes this factory with the parsed env. The
 * factory is the default-deps variant of
 * `createSidecarSubstrateFactory`; deployments that need a recording
 * hub sink (tests, alternate hosts) construct their own via
 * `createSidecarSubstrateFactory`.
 */
export const createSubstrate: SubstrateFactory =
  createSidecarSubstrateFactory();
