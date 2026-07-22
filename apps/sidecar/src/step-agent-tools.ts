// Per-step tool materialization and agent construction for the
// workflow-process child.
//
// The child IS the sidecar binary, so the sidecar's tool runtime
// (`@intx/tool-packaging` loader, posix, the LSP plugin) is present in
// the child's address space. This module is the seam that runs that
// runtime when the real step-invoker builds a step's agent: it reads
// the step's deploy tree off disk, materializes the pinned tool-package
// closure via `materializeToolPackages`, builds the plugin chain,
// attaches the resulting tool factories to the step's
// `AgentDefinition`, and returns an `Agent` whose `close()` tears every
// plugin and tool bundle down.
//
// LSP lifecycle (the riskiest sub-item, design §6): the LSP plugin
// factory's `dispose` terminates the LSP subprocess. The
// `createWorkflowStepInvoker` adapter calls `agent.close()` in a
// `finally` on EVERY exit path (clean completion, abort, rejection),
// so wrapping `close()` to also run the plugin + bundle disposers is
// what guarantees the LSP subprocess dies with the step's agent --
// no leak across steps, on abort, or on child recycle/drain (recycle
// kills the child process, which kills the LSP grandchild regardless).
//
// Layering (design §3d): everything here lives in `apps/sidecar`. The
// portable `@intx/workflow-host` package never gains a dependency on
// the tool runtime; it only sees the `agentFactory` callback this
// module produces.

import path from "node:path";

import {
  createAgent,
  defineAgent,
  defineTool,
  toolApprovalEffect,
  type Agent,
  type AgentDefinition,
  type AnnotatedPluginFactory,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolBundle,
} from "@intx/agent";
import { readDeployTree, sanitizeAddress } from "@intx/hub-agent/paths";
import { getLogger } from "@intx/log";
import type { LoadedToolFactory } from "@intx/tool-packaging";
import { resolveStepAddress } from "@intx/workflow-deploy";
import { parseAgentAddress } from "@intx/types";
import type { GrantRule } from "@intx/types/authz";
import { baseStepId } from "@intx/workflow";

import { materializeToolPackages } from "./tool-materialization";

const logger = getLogger(["sidecar", "workflow-child", "step-tools"]);

const INSTANCE_PREFIX = "ins_";

/**
 * Cache and registry caps the per-step tool loader needs. Resolved at
 * the sidecar boot edge from the existing `SIDECAR_CACHE_*` /
 * `SIDECAR_REGISTRY_*` config keys and threaded into the child through
 * the substrate config, so the child's per-step materialization is
 * bounded by those boot-edge-resolved caps.
 */
export interface StepToolCacheConfig {
  readonly cacheMaxBytes: number;
  readonly registryMaxTarballBytes: number;
}

/**
 * Materialized tool runtime for one step's agent. Carried from
 * `buildEnv` (which knows the step's identity) to the `agentFactory`
 * (which knows the agent definition + env) via a symbol-keyed slot on
 * the per-step env so the two callbacks of `createWorkflowStepInvoker`
 * can cooperate without widening the portable adapter's surface.
 */
export interface StepToolMaterialization {
  readonly factories: readonly LoadedToolFactory[];
  readonly pluginFactories: readonly AnnotatedPluginFactory[];
}

/**
 * Derive the per-step tool-mark floor grants from a step's materialized
 * tool factories. Each loaded factory carries its static
 * `definitions` (name + optional `approval` mark) verbatim from the tool
 * package; every declared tool contributes a `tool:<name>` / `invoke`
 * grant whose effect is the tool's floor (`ask` for an approval-gated
 * tool, `allow` otherwise), computed through the same
 * `toolApprovalEffect` mapping the deploy-time capability walk uses so a
 * pinned tool floors identically hub-side and sidecar-side.
 *
 * The hub's capability walk reads only INLINE `agent.toolFactories`, so a
 * tool that ships as a pinned package loads in the child and never
 * produces a `tool:<name>` grant on the run principal. These derived rows
 * supply that missing floor: they join the per-step grants the authz path
 * evaluates as ADDITIONAL rows, so `evaluateGrants` precedence still
 * resolves an explicit `deny` (priority 2) over the derived `ask`/`allow`
 * -- the floor only raises the minimum authority to the tool's mark, it
 * never overrides a declared denial.
 *
 * The grant `id` is deterministic (`floor:tool:<name>`) rather than
 * random: `evaluateGrants` never dedupes or joins on `id` (it ranks by
 * specificity then effect), so a stable id keeps the rows reproducible
 * without a generator, and a floor row that coincides with a
 * hub-supplied `tool:<name>` row resolves by effect precedence regardless
 * of the ids.
 */
export function deriveToolMarkFloorGrants(
  factories: readonly LoadedToolFactory[],
): GrantRule[] {
  const rows: GrantRule[] = [];
  for (const factory of factories) {
    for (const definition of factory.definitions) {
      rows.push({
        id: `floor:tool:${definition.name}`,
        resource: `tool:${definition.name}`,
        action: "invoke",
        effect: toolApprovalEffect(definition),
        origin: "creator",
        conditions: null,
        expiresAt: null,
        roleId: null,
        principalId: null,
      });
    }
  }
  return rows;
}

/**
 * Symbol-keyed slot the `buildEnv` callback sets on the env it returns
 * and the `agentFactory` reads. Object spread (`{ ...envBase,
 * authorize }`) inside the step-invoker adapter copies own enumerable
 * symbol-keyed properties, so the slot survives the spread that
 * produces the env handed to `agentFactory`.
 */
const STEP_TOOLS = Symbol("intx.sidecar.step-tools");

// The per-step env carries one private symbol-keyed slot the
// buildEnv/agentFactory pair cooperate over. The slot is read/written
// through `Reflect.get`/`Reflect.set` so neither site needs a type
// assertion: `BaseEnv` is an interface without a symbol index
// signature, so a structural cast would otherwise be required.
function setStepToolSlot(
  env: object,
  materialization: StepToolMaterialization,
): void {
  Reflect.set(env, STEP_TOOLS, materialization);
}

function getStepToolSlot(env: object): StepToolMaterialization | undefined {
  const value: unknown = Reflect.get(env, STEP_TOOLS);
  if (value === undefined) return undefined;
  if (!isStepToolMaterialization(value)) {
    throw new Error(
      "sidecar workflow-child step tools: the per-step env's tool slot is not a StepToolMaterialization; the slot is private to this module and must only be set by attachStepTools",
    );
  }
  return value;
}

function isStepToolMaterialization(
  value: unknown,
): value is StepToolMaterialization {
  return (
    typeof value === "object" &&
    value !== null &&
    "factories" in value &&
    "pluginFactories" in value &&
    Array.isArray(value.factories) &&
    Array.isArray(value.pluginFactories)
  );
}

/**
 * Resolve the on-disk directory holding a step's deploy tree.
 *
 * The deploy tree (`deploy/prompt.md`, `deploy/tool-packages-manifest.json`,
 * `deploy/asset-mounts.json`) is shipped to the sidecar per step by the
 * hub's `launchSession` deploy-pack push, which lands it in the LEGACY
 * per-agent directory keyed by the step's sanitized mail address (see
 * `@intx/hub-agent` `agentDir` / `sanitizeAddress`). It is NOT in the
 * substrate's `agent-state/<id>` layout -- the multi-step deploy path
 * never pushes step `agent-state` packs to the child's substrate.
 *
 * The step's mail address is `resolveStepAddress(...)`, the single owner
 * of the head/step collapse: for a single-step deployment the lone step
 * IS the head (the deployment mailbox itself), so the tree is read at the
 * head; for multi-step it is `deriveStepAddress(deploymentId, stepId,
 * deploymentDomain)`. The `deploymentId`/`deploymentDomain` are recovered
 * from the deployment mailbox address the supervisor threaded into the
 * child as `MAILBOX_ADDRESS` (`ins_<deploymentId>@<domain>`): the
 * instance-id local part minus the `ins_` prefix is the deploymentId, and
 * the address domain is the deploymentDomain. `stepCount` is sourced from
 * the host (via `substrateEnv`) so producer and consumer never derive
 * divergent addresses.
 */
export function stepDeployTreeDir(args: {
  dataDir: string;
  mailboxAddress: string;
  stepId: string;
  stepCount: number;
}): string {
  const parsed = parseAgentAddress(args.mailboxAddress);
  if (parsed === null) {
    throw new Error(
      `sidecar workflow-child step tools: deployment mailbox address ${JSON.stringify(args.mailboxAddress)} is not a parseable agent address; cannot locate the step's deploy tree`,
    );
  }
  if (!parsed.instanceId.startsWith(INSTANCE_PREFIX)) {
    throw new Error(
      `sidecar workflow-child step tools: deployment mailbox instance id ${JSON.stringify(parsed.instanceId)} does not carry the ${JSON.stringify(INSTANCE_PREFIX)} prefix; cannot derive the orchestrator deploymentId`,
    );
  }
  const deploymentId = parsed.instanceId.slice(INSTANCE_PREFIX.length);
  // A `map` iteration runs under a scoped step id `<base>[<index>]`, but
  // deploy stages one deploy tree per base step, so the scoped id resolves
  // to its base address -- every iteration reads the base step's tree.
  // `baseStepId` is the identity on an unscoped id, so a plain step is
  // unaffected.
  const stepAddress = resolveStepAddress({
    deploymentId,
    stepId: baseStepId(args.stepId),
    deploymentDomain: parsed.domain,
    stepCount: args.stepCount,
  });
  return path.join(args.dataDir, sanitizeAddress(stepAddress));
}

/**
 * Read a step's deploy tree and materialize its pinned tool-package
 * closure. The tarball cache and the tool instance dir are rooted under
 * the supplied per-step `storeDir` (the Phase-1 per-step state root) so
 * concurrent steps/agents in one child never collide on cache or
 * apply-state paths.
 *
 * A deploy with no tool-package manifest yields empty factories -- the
 * legitimate `rawManifestBytes === undefined` case. A manifest that is
 * present but fails to load surfaces loudly through
 * `materializeToolPackages` (the throw path), never a silent
 * empty-tools fallback that would mask a broken deploy.
 */
export async function materializeStepTools(args: {
  dataDir: string;
  mailboxAddress: string;
  stepId: string;
  stepCount: number;
  /** Per-step state root; cache + instance dir + workspace live under it. */
  storeDir: string;
  cache: StepToolCacheConfig;
}): Promise<StepToolMaterialization> {
  const deployTreeDir = stepDeployTreeDir({
    dataDir: args.dataDir,
    mailboxAddress: args.mailboxAddress,
    stepId: args.stepId,
    stepCount: args.stepCount,
  });
  const deployTree = await readDeployTree(deployTreeDir);

  // Root the tarball cache per step so concurrent steps in one child
  // do not race on the content-addressable cache root. The cache is
  // content-addressed and therefore safe to share globally, but the
  // design (§3d, point 3) calls for a per-step cacheRoot so a wedged
  // or partially-written apply in one step cannot corrupt another's
  // view.
  const cacheRoot = path.join(args.storeDir, "tarball-cache");

  // Asset-mounted tool tarballs are staged by the hub's asset-pack push
  // into the step's LEGACY agent dir workspace (the same dir the deploy
  // tree lives in), not under the per-step store dir. Point the loader's
  // asset resolution there while keeping the apply-state + cache rooted
  // per step under `storeDir`.
  //
  // This `<deployTreeDir>/workspace` (read-only staged assets, keyed by the
  // BASE step) and the agent's read-write workdir `<storeDir>/workspace`
  // (keyed by the SCOPED step) share a leaf name but are deliberately
  // different roots -- a map iteration reads one shared deploy tree while
  // each iteration writes its own scratch. Do not unify them.
  const assetRoot = path.join(deployTreeDir, "workspace");

  const materialized = await materializeToolPackages({
    rawManifestBytes: deployTree.toolPackageManifestRaw,
    assetMounts: deployTree.assetMounts,
    storeDir: args.storeDir,
    assetRoot,
    agentAddress: args.mailboxAddress,
    cacheRoot,
    cacheMaxBytes: args.cache.cacheMaxBytes,
    registryMaxTarballBytes: args.cache.registryMaxTarballBytes,
  });
  return {
    factories: materialized.factories,
    pluginFactories: materialized.pluginFactories,
  };
}

/**
 * Attach a step's materialized tool runtime to the per-step env so the
 * tool-bearing `agentFactory` can consume it. Called by `buildEnv`
 * after `materializeStepTools` resolves.
 *
 * The parameter is `Omit<BaseEnv, "authorize">` because `buildEnv`
 * yields exactly that shape (the step-invoker adapter adds `authorize`
 * before the env reaches `agentFactory`); the symbol slot survives the
 * adapter's `{ ...envBase, authorize }` spread.
 */
export function attachStepTools(
  env: Omit<BaseEnv, "authorize">,
  materialization: StepToolMaterialization,
): void {
  setStepToolSlot(env, materialization);
}

/**
 * Re-wrap a loaded tool factory so its bundle's `dispose` (when present)
 * is captured via `onDispose`, forwarding the loader's `id`, `requires`,
 * and `definitions` so the result is a real `AnnotatedToolFactory`, not a
 * hand-shaped lookalike. The static `definitions` declaration is
 * forwarded verbatim: this wrapper does not rename tools, so the names
 * the deploy-time walk enumerates must survive the re-wrap unchanged.
 *
 * Exported so the definitions-preservation contract is testable in
 * isolation; the production path calls it from
 * `createToolBearingAgentFactory`.
 */
export function rewrapStepToolFactory(
  annotated: AnnotatedToolFactory<BaseEnv>,
  onDispose: (dispose: () => unknown) => void,
): AnnotatedToolFactory<BaseEnv> {
  return defineTool({
    id: annotated.id,
    requires: annotated.requires,
    definitions: annotated.definitions,
    factory: (factoryEnv: BaseEnv): ToolBundle => {
      const bundle = annotated(factoryEnv);
      if (bundle.dispose !== undefined) {
        onDispose(bundle.dispose);
      }
      return bundle;
    },
  });
}

/**
 * Build the `agentFactory` the workflow step-invoker uses. The returned
 * factory reads the materialized tool runtime off the env (set by
 * `buildEnv` via `attachStepTools`), augments the step's
 * `AgentDefinition` with the loaded tool factories (wrapped to capture
 * each bundle's disposer), constructs the plugin chain on `env.plugins`,
 * builds the agent, and wraps
 * `agent.close()` so every plugin instance and tool bundle is disposed
 * when the step's agent closes.
 *
 * When the env carries no materialized tools (the `buildEnv` did not
 * run materialization, e.g. a unit test using the bare factory), the
 * factory falls back to `createAgent(def, env)` unchanged.
 */
export function createToolBearingAgentFactory(): <EnvReq extends BaseEnv>(
  def: AgentDefinition<EnvReq>,
  env: EnvReq,
) => Promise<Agent> {
  return async <EnvReq extends BaseEnv>(
    def: AgentDefinition<EnvReq>,
    env: EnvReq,
  ): Promise<Agent> => {
    const materialization = getStepToolSlot(env);
    if (materialization === undefined) {
      return createAgent(def, env);
    }

    // Wrap each loaded tool factory so its bundle's `dispose` (when
    // present) is captured. Dedupe by closure identity: a factory whose
    // bundle returns the same `dispose` on every invocation must not be
    // torn down once per push.
    const capturedDisposers = new Set<() => unknown>();
    const factoriesWithCapture = materialization.factories.map((annotated) =>
      rewrapStepToolFactory(annotated, (dispose) => {
        capturedDisposers.add(dispose);
      }),
    );

    // Rebuild the def with the materialized tool factories. The
    // serialized `def.toolFactories` carry only `{ id, requires }`
    // metadata (the workflow projection strips closures on the wire),
    // so the runnable factories come from materialization, not the
    // incoming def. `defineAgent` owns the contravariance escape for
    // the `BaseEnv`-typed loader factories (see its `EnvRequiredByAll`
    // machinery).
    const toolDef = defineAgent({
      id: def.id,
      systemPrompt: def.systemPrompt,
      tools: factoriesWithCapture,
      capabilities: [...def.capabilities],
      inference: { sources: [...def.inference.sources] },
      ...(def.description !== undefined
        ? { description: def.description }
        : {}),
      ...(def.director !== undefined ? { director: def.director } : {}),
      ...(def.tags !== undefined ? { tags: def.tags } : {}),
    });

    // Instantiate plugin factories one at a time so each successive
    // factory sees the prior plugins' instances on `env.plugins`:
    // posix's bundle reads `env.plugins` and threads ToolPlugin-shaped
    // values into `createPosixTools`; the LSP plugin factory is what
    // populates them.
    //
    // On a midway factory throw, every plugin instance already
    // constructed releases what it acquired (the LSP plugin starts a
    // subprocess) before the construction error propagates, so a
    // partial-success chain never leaks an LSP subprocess.
    const pluginInstances: unknown[] = [];
    let chainEnv: BaseEnv = env;
    try {
      for (const factory of materialization.pluginFactories) {
        const instance = factory(chainEnv);
        pluginInstances.push(instance);
        chainEnv = {
          ...env,
          plugins: [...pluginInstances],
        };
      }
    } catch (err) {
      await disposeAll(pluginInstances, "plugin construction rollback");
      throw err;
    }

    let agent: Agent;
    try {
      agent = await createAgent(toolDef, chainEnv);
    } catch (err) {
      // `createAgent` disposes the tool bundles it constructed on its
      // own failure path, but the plugin instances are this module's
      // to own -- tear them down so a failed agent build does not leak
      // the LSP subprocess.
      await disposeAll(pluginInstances, "agent construction failure");
      throw err;
    }

    return wrapAgentClose(agent, async () => {
      // Tool bundle disposers first (posix's bundle dispose chains
      // through to the LSP plugin's `dispose`), then the plugin
      // instances directly. Disposing the LSP plugin twice is safe:
      // `lsp.dispose()` clears its client set and the posix bundle's
      // dispose is idempotent. Running both guarantees the LSP
      // subprocess is torn down even for a plugin no tool bundle
      // consumed.
      for (const dispose of capturedDisposers) {
        try {
          await dispose();
        } catch (cause) {
          logger.warn`step tool bundle dispose failed: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
      }
      await disposeAll(pluginInstances, "step teardown");
    });
  };
}

/**
 * Return an `Agent` whose `close()` runs the original close and then
 * the supplied teardown. The teardown runs AFTER the agent's own close
 * so the reactor has stopped issuing tool calls before the tool/plugin
 * resources are released. `close()` is idempotent at the agent layer;
 * this wrapper guards its own teardown so a double `close()` does not
 * double-dispose.
 */
function wrapAgentClose(agent: Agent, teardown: () => Promise<void>): Agent {
  let tornDown = false;
  return {
    ...agent,
    send: (content, opts) => agent.send(content, opts),
    stream: () => agent.stream(),
    deliver: (message) => agent.deliver(message),
    setSource: (source) => agent.setSource(source),
    history: () => agent.history(),
    checkpoints: (limit) => agent.checkpoints(limit),
    readAt: (hash) => agent.readAt(hash),
    blobReader: agent.blobReader,
    async close() {
      await agent.close();
      if (tornDown) return;
      tornDown = true;
      await teardown();
    },
  };
}

async function disposeAll(
  instances: readonly unknown[],
  context: string,
): Promise<void> {
  for (const instance of instances) {
    const dispose = pluginDispose(instance);
    if (dispose === undefined) continue;
    try {
      // `await` accepts non-promise values verbatim, so this works
      // whether the disposer is sync or async.
      await dispose();
    } catch (cause) {
      logger.warn`step plugin dispose failed during ${context}: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  }
}

/**
 * Extract a callable `dispose` from a plugin instance whose static type
 * is `unknown` (plugin factories return host-defined shapes the agent
 * runtime does not interpret). Returns a bound disposer or `undefined`
 * when the instance carries no `dispose` function.
 */
function pluginDispose(value: unknown): (() => unknown) | undefined {
  if (value === null || typeof value !== "object") return undefined;
  if (!("dispose" in value)) return undefined;
  const dispose: unknown = value.dispose;
  if (typeof dispose !== "function") return undefined;
  const fn = dispose;
  return () => fn.call(value);
}
