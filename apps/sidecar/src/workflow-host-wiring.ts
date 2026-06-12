// Thin wiring module that constructs `createWorkflowSupervisor` with
// this sidecar's host-specific bindings: the existing mail-bus
// instance, the sidecar's Ed25519 signing keypair, the substrate
// RepoStore handle, `Bun.spawn` as the subprocess spawner, and a
// host-injected `trivialLaunch` callback that drives the legacy
// single-agent provisioning surface for trivial (1-step) deploys.
// Any logic that would benefit a future alternative-sidecar
// implementation lives inside `@intx/workflow-host`, not here.

import { sign as nodeSign } from "node:crypto";
import { fileURLToPath } from "node:url";

import { importPrivateKeyBytes } from "@intx/crypto-node";
import { getLogger } from "@intx/log";
import type { HubTransport } from "@intx/mail-memory";
import type { RepoId, RepoStore } from "@intx/hub-sessions";
import type {
  AgentKeyStore,
  DeployRouter,
  DeployRouterResult,
  SessionManager,
} from "@intx/hub-agent";
import {
  createWorkflowSupervisor,
  wrapHubTransportAsMailBus,
  type CredentialsSnapshot,
  type DeriveStepAddress,
  type HubTransportMailBusAdapter,
  type RecordRunEvent,
  type SubprocessSpawner,
  type SupervisorRunEvent,
  type TrivialLaunch,
  type WorkflowSupervisor,
} from "@intx/workflow-host";
import type { InferenceEvent } from "@intx/types/runtime";

const logger = getLogger(["interchange", "sidecar", "workflow-host-wiring"]);

/**
 * Project an agent address into a substrate-safe deployment id for
 * the trivial branch. The workflow-run repo's `repoId.id` must match
 * `/^[a-zA-Z0-9_-]+$/` (see `SAFE_REPO_ID` in
 * `packages/hub-sessions/src/repo-store/types.ts`), and the supervisor
 * principal's `deploymentId` must equal `workflowRunRepoId.id` for
 * the workflow-run kind handler's authz check to pass. The strict
 * regex rejects `@` and `.`, which both appear in every agent
 * address. The substrate test
 * `packages/hub-sessions/src/agent-repo.test.ts` explicitly asserts
 * `agent@domain` is rejected as `repo_id_invalid`, so the regex is
 * the substrate's contract surface for repo-path safety; widening it
 * would mean updating that contract and the test suite that pins it.
 *
 * Substitute disallowed characters with `-`. The mapping is lossy
 * (two distinct addresses can collapse to the same slug) but
 * deterministic; trivial deployments share one workflow-run repo
 * per agent address by design, and a collision implies two
 * deployments are claiming the same trivial workflow surface.
 */
function deriveTrivialDeploymentId(agentAddress: string): string {
  return agentAddress.replaceAll(/[^a-zA-Z0-9_-]/g, "-");
}

// The supervisor's `binaryPath` binding resolves to the sidecar's
// own `bin/workflow-child` script via `import.meta.resolve` against
// the `@intx/sidecar-app` package. The script lives next to this
// wiring module (`../bin/workflow-child`); resolving it statically
// at wiring-module load time keeps the production spawn surface
// independent of any runtime env override. Tests inject a sentinel
// path via the `binaryPath` opts override; production wiring
// closes over this constant.
const SIDECAR_WORKFLOW_CHILD_BINARY: string = (() => {
  const url = import.meta.resolve("../bin/workflow-child");
  return fileURLToPath(url);
})();

const defaultSubprocessSpawner: SubprocessSpawner = () => {
  throw new Error(
    "workflow-host-wiring: Bun.spawn-backed subprocessSpawner is not yet implemented; provide one via createSidecarWorkflowSupervisor's spawner override",
  );
};

export type CreateSidecarWorkflowSupervisorOpts = {
  /** Sidecar's hub mail transport. */
  transport: HubTransport;
  /** Substrate-shaped RepoStore the workflow-host's supervisor reads from. */
  repoStore: RepoStore;
  /** Sidecar's 32-byte Ed25519 private key seed for principal signing. */
  signingKeySeed: Uint8Array;
  /** Workflow-run repo identity for the deployment. */
  workflowRunRepoId: RepoId;
  /** Workflow-run repo ref the supervisor commits events to. */
  workflowRunRef: string;
  /** Deployment id baked into principal claims and address derivation. */
  deploymentId: string;
  /** Deployment's mail address. */
  deploymentMailAddress: string;
  /** Per-step mail-address derivation. */
  deriveStepAddress: DeriveStepAddress;
  /** Substrate-config keys propagated to the child via spawn-time env. */
  substrateEnv: Record<string, string>;
  /**
   * Host-injected callback the supervisor's trivial branch invokes.
   * For the production sidecar this closes over the
   * `SessionManager.provisionAgent` flow plus the hub-pairing-key
   * recording the legacy `agent.deploy` handler performed inline.
   */
  trivialLaunch: TrivialLaunch;
  /**
   * Override the subprocess spawner. Tests inject a deterministic
   * mock; production uses `Bun.spawn` (not yet implemented).
   */
  subprocessSpawner?: SubprocessSpawner;
  /** Override the `bin/workflow-child` path. */
  binaryPath?: string;
};

export type SidecarWorkflowSupervisor = {
  supervisor: WorkflowSupervisor;
  /** Hand a delivered inbound message off to the supervisor's mail subscription. */
  routeInbound(message: Uint8Array): void;
  /** Snapshot accessor that proxies the supervisor's credentials view. */
  getCredentialsSnapshot(): CredentialsSnapshot | null;
};

/**
 * Construct the sidecar's `DeployRouter`. The router holds the
 * `sessions` + `keyStore` closures the workflow-host supervisor's
 * `trivialLaunch` needs and constructs a fresh per-deployment
 * supervisor on every inbound `agent.deploy` frame. The trivial
 * branch then calls back into `sessions.provisionAgent` plus the
 * pre-existing hub-pairing-key recording so the bytes flowing
 * through the deploy-flow gate test path stay bit-identical to the
 * pre-supervisor path.
 *
 * Multi-step deploys (frames carrying a workflow definition with
 * `steps.length >= 2`) route through `supervisor.deploy`'s
 * multi-step branch -- the IPC channel, child spawn, and
 * `credentialsSnapshot` assembly. The routing seam exists here so
 * the frame-format extension that carries the workflow definition is
 * a pure data-shape change.
 */
/**
 * Stable step identifier the trivial workflow uses for its single
 * step. The on-disk `StepStarted` / `StepCompleted` envelopes carry
 * this value verbatim; it is opaque to the supervisor and the
 * substrate, but downstream audit-log consumers join run events on
 * it. The trivial workflow has exactly one step per run, so the id
 * is a constant rather than a per-deploy mint.
 */
const TRIVIAL_STEP_ID = "trivial";

export function createSidecarDeployRouter(deps: {
  sessions: SessionManager;
  keyStore: AgentKeyStore;
  onAgentEvent: SessionManager["onAgentEvent"];
  transport: HubTransport;
  repoStore: RepoStore;
  signingKeySeed: Uint8Array;
}): DeployRouter {
  return {
    async deploy(frame): Promise<DeployRouterResult> {
      let publicKey: string | undefined;
      const deploymentId = deriveTrivialDeploymentId(frame.agentAddress);
      const wired = createSidecarWorkflowSupervisor({
        transport: deps.transport,
        repoStore: deps.repoStore,
        signingKeySeed: deps.signingKeySeed,
        workflowRunRepoId: {
          kind: "workflow-run",
          id: deploymentId,
        },
        workflowRunRef: "refs/heads/main",
        deploymentId,
        deploymentMailAddress: frame.agentAddress,
        deriveStepAddress: ({ deploymentId: dep, stepId }) =>
          `${dep}-${stepId}`,
        substrateEnv: {},
        trivialLaunch: async (bindings) => {
          const result = await deps.sessions.provisionAgent(
            // The trivialLaunch contract treats `config` as opaque
            // bytes the host minted; for the sidecar wiring the
            // bytes are a `HarnessConfig` the frame carried
            // verbatim, and `SessionManager.provisionAgent`
            // expects exactly that.
            // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- frame.config is the validated HarnessConfig the link surfaced
            bindings.config as Parameters<SessionManager["provisionAgent"]>[0],
          );
          deps.keyStore.recordHubKey(
            bindings.agentAddress,
            bindings.hubPublicKey,
          );
          await deps.sessions.persistHubPublicKey(
            bindings.agentAddress,
            bindings.hubPublicKey,
          );
          publicKey = result.publicKey;
          // Subscribe to per-agent InferenceEvents and project the
          // reactor's run-bracket vocabulary onto the workflow-run
          // event chain. The seam is a no-op until the harness
          // dispatches the first `message.run.started`; the
          // disposer is not held here because the trivial branch
          // shares the agent's lifetime with the deployment and
          // SessionManager prunes the listener set on
          // destroySession through the closure's natural unbind.
          // The reactor brackets one workflow-run per inbound
          // mail; each `message.run.started` mints a fresh runId
          // and brackets the chain.
          const cell: TrivialRunCell = {
            runId: null,
            stepStarted: false,
          };
          deps.onAgentEvent(bindings.agentAddress, (event) => {
            driveTrivialRunChain(event, bindings.recordRunEvent, cell).catch(
              (err: unknown) => {
                // Capture rejections inside the listener so a substrate
                // failure (e.g. the trivial-only RepoStore placeholder
                // throwing on `writeTreePreservingPrefix`) does not
                // surface as an unhandled rejection on the host
                // process. The trivial branch's audit chain is
                // best-effort against the deploy path; persistent
                // substrate misconfigurations log loudly here without
                // killing the agent's reactor.
                const msg = err instanceof Error ? err.message : String(err);
                logger.warn`trivial run-event recording failed for ${bindings.agentAddress}: ${msg}`;
              },
            );
          });
        },
      });
      await wired.supervisor.deploy({
        agentAddress: frame.agentAddress,
        agentId: frame.agentId,
        config: frame.config,
        hubPublicKey: frame.hubPublicKey,
      });
      if (publicKey === undefined) {
        throw new Error(
          "sidecar deploy router: trivialLaunch did not surface a public key",
        );
      }
      return { publicKey };
    },
  };
}

/**
 * Per-deployment cell tracking the active workflow-run bracket. The
 * trivial branch holds one cell per agent: each `message.run.started`
 * mints a fresh `runId`, the first `inference.start` after that flips
 * `stepStarted` true, and the matching `message.run.ended` reads both
 * back out before clearing the cell. Two outstanding brackets at once
 * are not possible for the trivial path because the reactor
 * serializes inbound messages.
 */
export interface TrivialRunCell {
  runId: string | null;
  stepStarted: boolean;
}

/**
 * Placeholder definition hash baked into the trivial workflow's
 * `RunStarted` envelopes. The trivial workflow's content-addressed
 * definition lands with the trivial-deploy capability walk; until
 * then the on-disk envelope carries a stable sentinel so audit-log
 * consumers see a consistent value across deployments.
 */
const TRIVIAL_DEFINITION_HASH = "trivial:v1";

/**
 * Map one InferenceEvent into `recordRunEvent` calls when the
 * reactor's run-bracket vocabulary lines up with a workflow-run
 * lifecycle moment. The mapping is:
 *
 *   - `message.run.started` -> RunStarted (mints a new workflow runId)
 *   - first `inference.start` after RunStarted -> StepStarted (attempt 1)
 *   - `message.run.ended` status=completed -> StepCompleted + RunCompleted
 *   - `message.run.ended` status=failed -> StepCompleted
 *
 * The runId is the `messageRunId` the reactor minted. `StepStarted`
 * is suppressed if a second `inference.start` arrives within the
 * same bracket (the trivial workflow's single step does not retry
 * inside one run; the next attempt would be a separate workflow-run
 * instance).
 *
 * The supervisor's `SupervisorRunEvent` union covers `RunCompleted`
 * but not `RunFailed`; the trivial branch surfaces a failed run by
 * recording `StepCompleted` against the step that failed (so the
 * per-step audit trail closes) and letting the absence of a
 * subsequent `RunCompleted` mark the run as unsuccessful for
 * downstream consumers. Widening the supervisor's
 * `SupervisorRunEvent` union to carry `RunFailed` lands when the
 * substrate kind handler grows the matching signature-verification
 * path.
 */
export async function driveTrivialRunChain(
  event: InferenceEvent,
  recordRunEvent: RecordRunEvent,
  cell: TrivialRunCell,
): Promise<void> {
  if (event.type === "message.run.started") {
    cell.runId = event.data.messageRunId;
    cell.stepStarted = false;
    const runStarted: SupervisorRunEvent = {
      kind: "RunStarted",
      runId: event.data.messageRunId,
      at: new Date().toISOString(),
      definitionHash: TRIVIAL_DEFINITION_HASH,
      trigger: {
        type: "mail",
        payload: { messageId: event.data.messageId },
      },
      consumedMessageId: event.data.messageId,
    };
    await recordRunEvent(runStarted);
    return;
  }
  if (event.type === "inference.start") {
    if (cell.runId === null) return;
    if (cell.stepStarted) return;
    cell.stepStarted = true;
    await recordRunEvent({
      kind: "StepStarted",
      runId: cell.runId,
      at: new Date().toISOString(),
      stepId: TRIVIAL_STEP_ID,
      attempt: 1,
      input: { ref: "refs/heads/main" },
    });
    return;
  }
  if (event.type === "message.run.ended") {
    const runId = cell.runId;
    if (runId === null) return;
    cell.runId = null;
    cell.stepStarted = false;
    const at = new Date().toISOString();
    await recordRunEvent({
      kind: "StepCompleted",
      runId,
      at,
      stepId: TRIVIAL_STEP_ID,
      attempt: 1,
      output: { ref: "refs/heads/main" },
    });
    if (event.data.status === "completed") {
      await recordRunEvent({
        kind: "RunCompleted",
        runId,
        at,
      });
    }
  }
}

/**
 * Construct a per-deployment supervisor with the sidecar's bindings
 * pre-wired. The host calls this once per `agent.deploy` frame; the
 * supervisor's trivial branch routes the deploy through the
 * host-supplied `trivialLaunch` callback so the on-wire and
 * on-disk surfaces stay bit-identical to the pre-supervisor path.
 */
export function createSidecarWorkflowSupervisor(
  opts: CreateSidecarWorkflowSupervisorOpts,
): SidecarWorkflowSupervisor {
  const mailBus: HubTransportMailBusAdapter = wrapHubTransportAsMailBus(
    opts.transport,
  );
  const supervisor = createWorkflowSupervisor({
    repoStore: opts.repoStore,
    signAsPrincipal: (kind, payload) => {
      const key = importPrivateKeyBytes(opts.signingKeySeed);
      const sig = nodeSign(null, payload, key);
      return { sig: new Uint8Array(sig), principalKind: kind };
    },
    mailBus,
    subprocessSpawner: opts.subprocessSpawner ?? defaultSubprocessSpawner,
    binaryPath: opts.binaryPath ?? SIDECAR_WORKFLOW_CHILD_BINARY,
    substrateEnv: opts.substrateEnv,
    workflowRunRepoId: opts.workflowRunRepoId,
    workflowRunRef: opts.workflowRunRef,
    deploymentId: opts.deploymentId,
    deploymentMailAddress: opts.deploymentMailAddress,
    readPrincipal: { kind: "supervisor" },
    deriveStepAddress: opts.deriveStepAddress,
    trivialLaunch: opts.trivialLaunch,
  });
  return {
    supervisor,
    routeInbound(message) {
      mailBus.routeInbound(opts.deploymentMailAddress, message);
    },
    getCredentialsSnapshot: () => supervisor.getCredentialsSnapshot(),
  };
}
