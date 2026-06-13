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

import type { AuditStore, ContextStore } from "@intx/types/runtime";
import { InferenceSource } from "@intx/types/runtime";
import { evaluateGrants } from "@intx/authz";
import type { GrantRule } from "@intx/authz";
import type { DirectorRegistry } from "@intx/agent";
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
  type StepEnvBase,
  type SubstrateFactory,
  type SubstrateFactoryEnv,
} from "@intx/workflow-host";
import type { StepInvokeRequest } from "@intx/workflow";

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
  "STEP_INFERENCE_SOURCES",
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
  STEP_INFERENCE_SOURCES: "string > 0",
}).onUndeclaredKey("ignore");

/**
 * Per-step `InferenceSource` table parsed from the spawn-time
 * `STEP_INFERENCE_SOURCES` env entry. The deploy router serializes
 * `frame.workflow.sources` (a `Record<stepId, InferenceSource>`) as
 * JSON and threads it through the supervisor's `substrateEnv`; the
 * factory parses and validates the table once at construction time
 * and pins it for `buildEnv` lookups.
 */
export const StepInferenceSourceTable = type({
  "[string]": InferenceSource,
});
export type StepInferenceSourceTable = typeof StepInferenceSourceTable.infer;

/**
 * Parse and validate the JSON-encoded `STEP_INFERENCE_SOURCES` entry
 * the supervisor threaded through `substrateEnv`. A malformed JSON
 * payload, a non-object root, or a value that does not match
 * `Record<string, InferenceSource>` is rejected at the boundary with
 * a structured error rather than being deferred to a deep-stack
 * `buildEnv` failure.
 */
export function parseStepInferenceSources(
  raw: string,
): StepInferenceSourceTable {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (cause) {
    const reason = cause instanceof Error ? cause.message : String(cause);
    throw new Error(
      `sidecar workflow-child substrate config: STEP_INFERENCE_SOURCES is not valid JSON: ${reason}`,
    );
  }
  const validated = StepInferenceSourceTable(parsed);
  if (validated instanceof type.errors) {
    throw new Error(
      `sidecar workflow-child substrate config: STEP_INFERENCE_SOURCES failed validation: ${validated.summary}`,
    );
  }
  return validated;
}

/**
 * Resolve the per-step `InferenceSource` pinned at factory
 * construction. The supervisor's multi-step branch only invokes a
 * step whose `stepId` appears in `frame.workflow.sources`; a lookup
 * miss here is a programmer error in the supervisor, not a wire-side
 * failure, and the resolver surfaces it with the missing `stepId`
 * named.
 */
export function createStepInferenceSourceResolver(
  table: StepInferenceSourceTable,
): (stepId: string) => InferenceSource {
  return (stepId: string): InferenceSource => {
    const source = table[stepId];
    if (source === undefined) {
      throw new Error(
        `sidecar workflow-child step invoker buildEnv: no InferenceSource pinned for stepId ${JSON.stringify(stepId)}; the supervisor must populate frame.workflow.sources for every stepOrder entry`,
      );
    }
    return source;
  };
}

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
 * Object-shaped `StepEnvBase` slot whose every access throws. The
 * sidecar's substrate factory wires `source` from the pinned
 * `STEP_INFERENCE_SOURCES` table; the remaining `StepEnvBase` slots
 * (storage, audit, directors) are not yet populated by the factory.
 * Returning a throwing-getter Proxy keeps the static `StepEnvBase`
 * contract intact while surfacing a precise failure at the first
 * downstream access — a step invocation that actually consumes one
 * of these slots gets a structured "not wired" error naming the
 * slot and the originating `stepId`.
 */
function throwingStepEnvSlot<T extends object>(
  slot: string,
  stepId: string,
): T {
  const trap = (prop: PropertyKey): never => {
    throw new Error(
      `sidecar workflow-child step invoker buildEnv: ${slot} slot is not wired (stepId=${JSON.stringify(stepId)}, access=${String(prop)}); the substrate factory does not yet supply per-step ${slot}`,
    );
  };
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- throwing Proxy stands in for a typed StepEnvBase slot until per-step storage/audit/directors land
  return new Proxy({} as T, {
    get(_target, prop) {
      return trap(prop);
    },
    has(_target, prop) {
      return trap(prop);
    },
    apply() {
      return trap("apply");
    },
  });
}

/**
 * Sentinel `workdir` path the agent's lock boundary uses. The
 * substrate factory does not yet allocate a per-step workdir; a
 * step invocation that reaches `BaseEnv.workdir` surfaces a loud
 * `ENOENT` against this path rather than silently writing into an
 * unrelated directory. The path is intentionally unusable so a
 * silent default is impossible.
 */
function throwingStepEnvWorkdir(stepId: string): string {
  return `/__sidecar_workflow_child_workdir_not_wired__/stepId=${stepId}`;
}

/**
 * Build the step-invoker `buildEnv` callback the workflow-host's
 * adapter consumes. Pulled out of `createSidecarSubstrateFactory` so
 * source-resolution is observable without standing up the full
 * substrate; the closure pins the parsed per-step source table once,
 * derives the `stepId` from the runtime's `AuthorizeContext`, and
 * populates `StepEnvBase.source` from the table. The other
 * `StepEnvBase` slots are not yet supplied by the substrate factory
 * and are filled with throwing-getter stubs so a step invocation
 * that exercises them surfaces a precise failure rather than a
 * silent default.
 */
export function createSidecarStepBuildEnv(
  table: StepInferenceSourceTable,
): (req: StepInvokeRequest) => Promise<StepEnvBase> {
  const resolveStepInferenceSource = createStepInferenceSourceResolver(table);
  return async (req: StepInvokeRequest): Promise<StepEnvBase> => {
    const stepId = req.authzContext.stepId;
    if (stepId === undefined) {
      throw new Error(
        "sidecar workflow-child step invoker buildEnv: AuthorizeContext.stepId is required for per-step InferenceSource resolution; the workflow runtime must populate stepId on every step-originated invocation",
      );
    }
    const source = resolveStepInferenceSource(stepId);
    return {
      source,
      storage: throwingStepEnvSlot<ContextStore>("storage", stepId),
      workdir: throwingStepEnvWorkdir(stepId),
      audit: throwingStepEnvSlot<AuditStore>("audit", stepId),
      directors: throwingStepEnvSlot<DirectorRegistry>("directors", stepId),
    };
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

    const stepInferenceSources = parseStepInferenceSources(
      validated.STEP_INFERENCE_SOURCES,
    );
    const buildStepEnv = createSidecarStepBuildEnv(stepInferenceSources);

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
      buildEnv: buildStepEnv,
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
