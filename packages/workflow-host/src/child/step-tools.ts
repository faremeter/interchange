// Per-step tool attachment and agent construction for the workflow child.
//
// This module attaches a step's tool factories to the step env and builds the
// agent whose `close()` tears the plugin chain and tool bundles down.

import {
  createAgent,
  defineAgent,
  defineTool,
  toolFactoryMeta,
  type Agent,
  type AgentDefinition,
  type AnnotatedPluginFactory,
  type AnnotatedToolFactory,
  type BaseEnv,
  type ToolBundle,
} from "@intx/agent";
import {
  buildCredentialCapabilities,
  type StepCredentialWiring,
} from "@intx/harness";
import { getLogger } from "@intx/log";
import type { HostCredentialCapability } from "@intx/types";
import type { ToolCredentialDeclaration } from "@intx/types/package-json";
import {
  layerRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";

const logger = getLogger(["sidecar", "workflow-child", "step-tools"]);

/**
 * Tool runtime for one step's agent. Carried from
 * `buildEnv` (which knows the step's identity) to the `agentFactory`
 * (which knows the agent definition + env) via a symbol-keyed slot on
 * the per-step env so the two callbacks of `createWorkflowStepInvoker`
 * can cooperate without widening the portable adapter's surface.
 */
export interface StepToolRuntime {
  readonly factories: readonly {
    readonly factoryId: string;
    readonly declaredCredentials: readonly ToolCredentialDeclaration[];
    readonly factory: AnnotatedToolFactory<BaseEnv>;
  }[];
  readonly pluginFactories: readonly AnnotatedPluginFactory[];
}

/**
 * The step tool factories of a source-ref step: the agent's own evaluated
 * factories, each keyed by its bundle `id` (the credential Gate-2 consumer)
 * and carrying its `defineTool` credential declaration, so the declared
 * handles are reconciled against the delivered bindings when the agent is
 * built.
 */
export function sourceStepToolFactories(
  toolFactories: readonly AnnotatedToolFactory<BaseEnv>[],
): StepToolRuntime["factories"] {
  return toolFactories.map((factory) => ({
    factoryId: factory.id,
    declaredCredentials: (factory.credentials ?? []).map((declaration) => ({
      handle: declaration.handle,
      ...(declaration.scopes !== undefined
        ? { scopes: [...declaration.scopes] }
        : {}),
    })),
    factory,
  }));
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
function setStepToolSlot(env: object, stepTools: StepToolRuntime): void {
  Reflect.set(env, STEP_TOOLS, stepTools);
}

function getStepToolSlot(env: object): StepToolRuntime | undefined {
  const value: unknown = Reflect.get(env, STEP_TOOLS);
  if (value === undefined) return undefined;
  if (!isStepToolRuntime(value)) {
    throw new Error(
      "sidecar workflow-child step tools: the per-step env's tool slot is not a StepToolRuntime; the slot is private to this module and must only be set by attachStepTools",
    );
  }
  return value;
}

function isStepToolRuntime(value: unknown): value is StepToolRuntime {
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
 * Symbol-keyed slot carrying the per-step credential wiring the
 * `agentFactory` assembles each bundle's consumer-scoped `credentials`
 * capability from: the live material cell, the step's grants, and the
 * provider registry. Set by `buildEnv` alongside the tool slot, read by
 * `createToolBearingAgentFactory`. Absent for a build that threaded no
 * credential context (a unit test using the bare factory) -- then no
 * credentials capability is assembled and bundles keep the base capabilities
 * bag.
 */
const STEP_CREDENTIAL_WIRING = Symbol("intx.sidecar.step-credential-wiring");

function setStepCredentialWiring(
  env: object,
  wiring: StepCredentialWiring,
): void {
  Reflect.set(env, STEP_CREDENTIAL_WIRING, wiring);
}

function getStepCredentialWiring(
  env: object,
): StepCredentialWiring | undefined {
  const value: unknown = Reflect.get(env, STEP_CREDENTIAL_WIRING);
  if (value === undefined) return undefined;
  if (!isStepCredentialWiring(value)) {
    throw new Error(
      "sidecar workflow-child step tools: the per-step env's credential-wiring slot is not a StepCredentialWiring; the slot is private to this module and must only be set by attachStepCredentialWiring",
    );
  }
  return value;
}

function isStepCredentialWiring(value: unknown): value is StepCredentialWiring {
  return (
    typeof value === "object" &&
    value !== null &&
    "materialCell" in value &&
    "resolveGrants" in value &&
    "providers" in value
  );
}

/**
 * Attach a step's credential wiring to the per-step env so the tool-bearing
 * `agentFactory` can assemble each bundle's `credentials` capability. Called
 * by `buildEnv` for a step whose build threaded a credential context; omitted
 * otherwise, which leaves the slot unset and the credentials capability
 * unassembled.
 */
export function attachStepCredentialWiring(
  env: Omit<BaseEnv, "authorize">,
  wiring: StepCredentialWiring,
): void {
  setStepCredentialWiring(env, wiring);
}

/**
 * Read the base `RuntimeCapabilities` bag `buildEnv` set on the per-step env
 * (`env.capabilities`, currently `mail.transport`). `BaseEnv` does not type
 * the key -- it is a runtime-only widening the step env carries -- so it is
 * read reflectively and validated. Throws when a bundle has a credentials
 * capability to layer but the env carries no base bag: that is a wiring
 * inconsistency (the same `buildEnv` sets both), not a condition to paper over.
 */
function requireCapabilitiesBag(env: BaseEnv): RuntimeCapabilities {
  const value: unknown = Reflect.get(env, "capabilities");
  if (
    typeof value !== "object" ||
    value === null ||
    !("resolve" in value) ||
    typeof (value as { resolve: unknown }).resolve !== "function"
  ) {
    throw new Error(
      "sidecar workflow-child step tools: a credentials capability was assembled for this bundle but the per-step env carries no base capabilities bag to layer it onto; buildEnv must set env.capabilities before assembling credentials",
    );
  }
  // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- validated structurally above: an object whose `resolve` is callable is the RuntimeCapabilities resolver buildEnv set
  return value as RuntimeCapabilities;
}

/**
 * Attach a step's tool runtime to the per-step env so the
 * tool-bearing `agentFactory` can consume it. Called by `buildEnv`
 * once it has assembled the step's factories.
 *
 * The parameter is `Omit<BaseEnv, "authorize">` because `buildEnv`
 * yields exactly that shape (the step-invoker adapter adds `authorize`
 * before the env reaches `agentFactory`); the symbol slot survives the
 * adapter's `{ ...envBase, authorize }` spread.
 */
export function attachStepTools(
  env: Omit<BaseEnv, "authorize">,
  stepTools: StepToolRuntime,
): void {
  setStepToolSlot(env, stepTools);
}

/**
 * Re-wrap a tool factory so its bundle's `dispose` (when present) is
 * captured via `onDispose`, forwarding its `id`, `requires`, `definitions`,
 * and `credentials` so the result is a real `AnnotatedToolFactory`, not a
 * hand-shaped lookalike. The static `definitions` declaration is
 * forwarded verbatim: this wrapper does not rename tools, so the names
 * the deploy-time walk enumerates must survive the re-wrap unchanged.
 *
 * When `credentials` is supplied, the bundle's factory sees a per-bundle
 * capabilities bag: the step's base bag layered with THIS factory's
 * consumer-scoped `credentials` capability, so the bundle resolves only the
 * handles its own factory is authorized for. When it is absent (the factory
 * declares and binds no credential), the base bag is passed through unchanged.
 *
 * Exported so the definitions-preservation contract is testable in
 * isolation; the production path calls it from
 * `createToolBearingAgentFactory`.
 */
export function rewrapStepToolFactory(
  annotated: AnnotatedToolFactory<BaseEnv>,
  onDispose: (dispose: () => unknown) => void,
  credentials: HostCredentialCapability | undefined,
): AnnotatedToolFactory<BaseEnv> {
  return defineTool({
    ...toolFactoryMeta(annotated),
    factory: (factoryEnv: BaseEnv): ToolBundle => {
      const env =
        credentials === undefined
          ? factoryEnv
          : layerCredentialsOntoEnv(factoryEnv, credentials);
      const bundle = annotated(env);
      if (bundle.dispose !== undefined) {
        onDispose(bundle.dispose);
      }
      return bundle;
    },
  });
}

/**
 * Produce a copy of `factoryEnv` whose `capabilities` bag is the base bag
 * layered with this bundle's consumer-scoped `credentials` capability. Object
 * spread copies own enumerable properties -- including the runtime-only
 * `capabilities` key `BaseEnv` does not type and the private tool slot -- so
 * every other env surface survives; only `capabilities` is replaced.
 */
function layerCredentialsOntoEnv(
  factoryEnv: BaseEnv,
  credentials: HostCredentialCapability,
): BaseEnv {
  const base = requireCapabilitiesBag(factoryEnv);
  const layered = layerRuntimeCapabilities(base, { credentials });
  // Copy every env surface (spread carries own enumerable string AND symbol
  // keys, so `transport`, `address`, and the private tool/credential slots
  // survive), then replace the runtime-only `capabilities` key via Reflect --
  // a `{ ...factoryEnv, capabilities }` literal trips the excess-property
  // check because BaseEnv does not type the key.
  const layeredEnv: BaseEnv = { ...factoryEnv };
  Reflect.set(layeredEnv, "capabilities", layered);
  return layeredEnv;
}

/**
 * Build the `agentFactory` the workflow step-invoker uses. The returned
 * factory reads the tool runtime off the env (set by
 * `buildEnv` via `attachStepTools`), augments the step's
 * `AgentDefinition` with the tool factories (wrapped to capture
 * each bundle's disposer), constructs the plugin chain on `env.plugins`,
 * builds the agent, and wraps
 * `agent.close()` so every plugin instance and tool bundle is disposed
 * when the step's agent closes.
 *
 * When the env carries no tools (the `buildEnv` attached none, e.g. a
 * unit test using the bare factory), the
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
    const stepTools = getStepToolSlot(env);
    if (stepTools === undefined) {
      return createAgent(def, env);
    }

    // Assemble each factory's consumer-scoped `credentials` capability once,
    // when the step carries credential wiring (a bare test build carries none,
    // yielding an empty map). A factory that declares a handle no binding
    // resolves fails
    // the launch here, loudly, rather than at the tool's first resolve. This
    // can throw (reconcile fail-closed, malformed delivery) before any handle
    // is shaped, so nothing is orphaned on that path.
    const credentialWiring = getStepCredentialWiring(env);
    const credentialCapabilities =
      credentialWiring === undefined
        ? new Map<string, HostCredentialCapability>()
        : buildCredentialCapabilities(stepTools.factories, credentialWiring);

    // Wrap each tool factory so its bundle's `dispose` (when
    // present) is captured. Dedupe by closure identity: a factory whose
    // bundle returns the same `dispose` on every invocation must not be
    // torn down once per push. Each factory's credentials capability joins
    // the same teardown set so its shaped handles are released with the agent.
    const capturedDisposers = new Set<() => unknown>();
    for (const capability of credentialCapabilities.values()) {
      capturedDisposers.add(() => capability.dispose());
    }
    const factoriesWithCapture = stepTools.factories.map((stf) =>
      rewrapStepToolFactory(
        stf.factory,
        (dispose) => {
          capturedDisposers.add(dispose);
        },
        credentialCapabilities.get(stf.factoryId),
      ),
    );

    // Run every captured disposer -- each credentials capability and each tool
    // bundle -- guarding each so one failure does not strand the rest. Used on
    // the success teardown AND on the construction-failure rollbacks below: the
    // credentials capabilities are built before the plugin chain, so a plugin
    // or agent build that throws must still release them (an http handle holds
    // nothing, but a future key-file/socket handle would leak otherwise).
    // Bundle disposers are idempotent, so re-running one `createAgent` already
    // disposed on its own failure path is safe.
    const runCapturedDisposers = async (): Promise<unknown[]> => {
      const failures: unknown[] = [];
      for (const dispose of capturedDisposers) {
        try {
          await dispose();
        } catch (cause) {
          logger.error`step tool bundle dispose failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          failures.push(cause);
        }
      }
      return failures;
    };

    // Rebuild the def with the wrapped factories, which capture disposers
    // and layer credentials. `defineAgent` owns the contravariance escape
    // for the `BaseEnv`-typed factories (see its `EnvRequiredByAll`
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
      for (const factory of stepTools.pluginFactories) {
        const instance = factory(chainEnv);
        pluginInstances.push(instance);
        chainEnv = {
          ...env,
          plugins: [...pluginInstances],
        };
      }
    } catch (err) {
      // Release the credentials capabilities (built above, before the plugin
      // chain) and any bundle disposers captured so far, then the plugin
      // instances this module owns.
      await runCapturedDisposers();
      await disposeAll(pluginInstances, "plugin construction rollback");
      throw err;
    }

    let agent: Agent;
    try {
      agent = await createAgent(toolDef, chainEnv);
    } catch (err) {
      // `createAgent` disposes the tool bundles it constructed on its
      // own failure path, but the credentials capabilities and the plugin
      // instances are this module's to own -- tear them down so a failed
      // agent build does not leak a shaped handle or the LSP subprocess.
      await runCapturedDisposers();
      await disposeAll(pluginInstances, "agent construction failure");
      throw err;
    }

    return wrapAgentClose(agent, async () => {
      // Captured disposers first (each credentials capability, then the tool
      // bundles -- posix's bundle dispose chains through to the LSP plugin's
      // `dispose`), then the plugin instances directly. Disposing the LSP
      // plugin twice is safe: `lsp.dispose()` clears its client set and the
      // posix bundle's dispose is idempotent. Running both guarantees the LSP
      // subprocess is torn down even for a plugin no tool bundle consumed.
      // Both loops run every disposer and collect failures rather than
      // throwing mid-loop, so one failing disposer never strands the rest.
      // A leaked or failing LSP subprocess must surface, not be swallowed:
      // any collected failure fails the close, so the caller sees it.
      const failures = [
        ...(await runCapturedDisposers()),
        ...(await disposeAll(pluginInstances, "step teardown")),
      ];
      if (failures.length > 0) {
        throw new AggregateError(
          failures,
          `step agent close: ${String(failures.length)} disposer(s) failed during teardown; an LSP subprocess may be leaked`,
        );
      }
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
): Promise<unknown[]> {
  const failures: unknown[] = [];
  for (const instance of instances) {
    const dispose = pluginDispose(instance);
    if (dispose === undefined) continue;
    try {
      // `await` accepts non-promise values verbatim, so this works
      // whether the disposer is sync or async.
      await dispose();
    } catch (cause) {
      logger.error`step plugin dispose failed during ${context}: ${cause instanceof Error ? cause.message : String(cause)}`;
      failures.push(cause);
    }
  }
  return failures;
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
