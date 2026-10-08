// Per-step tool attachment and agent construction for the workflow child.
//
// The process that boots the child materializes pinned tool packages and
// passes the result in. This module attaches that result to the step env,
// derives the tool-mark floor from each factory's static declarations, and
// builds the agent whose `close()` tears the plugin chain and tool bundles
// down. It does not read a deploy tree or load a tool package.

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
  type ToolDeclaration,
} from "@intx/agent";
import type { GrantRule } from "@intx/authz";
import {
  buildCredentialCapabilities,
  type HostCredentialCapability,
  type StepCredentialWiring,
} from "@intx/harness";
import { getLogger } from "@intx/log";
import type { ToolCredentialDeclaration } from "@intx/types/package-json";
import {
  layerRuntimeCapabilities,
  type RuntimeCapabilities,
} from "@intx/types/runtime-capabilities";

const logger = getLogger(["sidecar", "workflow-child", "step-tools"]);

/**
 * Cache and registry caps the per-step tool loader needs, resolved at the
 * sidecar boot edge and threaded through the substrate config so the child's
 * per-step materialization is bounded by those caps.
 */
export interface StepToolCacheConfig {
  readonly cacheMaxBytes: number;
  readonly registryMaxTarballBytes: number;
}

/**
 * Materialized tool runtime for one step's agent. Carried from `buildEnv`
 * (which knows the step's identity) to the `agentFactory` (which knows the
 * agent definition + env) via a symbol-keyed slot on the per-step env, so
 * the two step-invoker callbacks cooperate without widening the portable
 * adapter's surface.
 */
export interface StepToolMaterialization {
  readonly factories: readonly {
    readonly packageName: string;
    readonly declaredCredentials: readonly ToolCredentialDeclaration[];
    readonly factory: AnnotatedToolFactory<BaseEnv>;
  }[];
  readonly pluginFactories: readonly AnnotatedPluginFactory[];
}

/**
 * Derive the per-step tool-mark floor grants from a step's materialized
 * tool factories. Each loaded factory carries its static `definitions`
 * (name + optional `approval` mark); every declared tool contributes a
 * `tool:<name>` / `invoke` grant whose effect is the tool's floor (`ask`
 * for an approval-gated tool, `allow` otherwise), computed through the
 * same `toolApprovalEffect` mapping the deploy-time capability walk uses.
 *
 * The hub's capability walk reads only INLINE `agent.toolFactories`, so a
 * pinned package tool loads in the child and never produces a `tool:<name>`
 * grant on the run principal. These derived rows supply that missing floor
 * as ADDITIONAL rows: `evaluateGrants` precedence still resolves an
 * explicit `deny` over the derived `ask`/`allow`.
 *
 * The grant `id` is deterministic (`floor:tool:<name>`): `evaluateGrants`
 * never dedupes or joins on `id`, so a stable id keeps the rows
 * reproducible, and a floor row that coincides with a hub-supplied
 * `tool:<name>` row resolves by effect precedence regardless of the ids.
 */
export function deriveToolMarkFloorGrants(
  factories: readonly {
    readonly packageName: string;
    readonly definitions: readonly ToolDeclaration[];
  }[],
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
 * Symbol-keyed slot the `buildEnv` callback sets on the env it returns and
 * the `agentFactory` reads. Object spread (`{ ...envBase, authorize }`)
 * inside the step-invoker adapter copies own enumerable symbol-keyed
 * properties, so the slot survives the spread into the env handed to
 * `agentFactory`.
 */
const STEP_TOOLS = Symbol("intx.sidecar.step-tools");

// The slot is read/written through `Reflect.get`/`Reflect.set` so neither
// site needs a type assertion: `BaseEnv` is an interface without a symbol
// index signature, so a structural cast would otherwise be required.
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
 * Symbol-keyed slot carrying the per-step credential wiring the
 * `agentFactory` assembles each bundle's consumer-scoped `credentials`
 * capability from: the live material cell, the step's grants, and the
 * provider registry. Set by `buildEnv` alongside the tool slot, read by
 * `createToolBearingAgentFactory`. Absent for a build that threaded no
 * credential context -- then no credentials capability is assembled.
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
 * otherwise, which leaves the slot unset and the capability unassembled.
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
 * the key -- it is a runtime-only widening -- so it is read reflectively and
 * validated. Throws when a bundle has a credentials capability to layer but
 * the env carries no base bag: that is a wiring inconsistency (the same
 * `buildEnv` sets both), not a condition to paper over.
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
 * Attach a step's materialized tool runtime to the per-step env so the
 * tool-bearing `agentFactory` can consume it. Called by `buildEnv` after
 * `materializeStepTools` resolves.
 *
 * The parameter is `Omit<BaseEnv, "authorize">` because `buildEnv` yields
 * exactly that shape (the step-invoker adapter adds `authorize` before the
 * env reaches `agentFactory`); the symbol slot survives the adapter's
 * `{ ...envBase, authorize }` spread.
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
 * and `definitions` so the result is a real `AnnotatedToolFactory`. The
 * static `definitions` declaration is forwarded verbatim: this wrapper
 * does not rename tools, so the names the deploy-time walk enumerates
 * must survive the re-wrap unchanged.
 *
 * When `credentials` is supplied, the bundle's factory sees a per-bundle
 * capabilities bag: the step's base bag layered with THIS package's
 * consumer-scoped `credentials` capability, so the bundle resolves only
 * the handles its own package is authorized for. When absent, the base
 * bag passes through unchanged.
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
    id: annotated.id,
    requires: annotated.requires,
    definitions: annotated.definitions,
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
 * layered with this bundle's consumer-scoped `credentials` capability.
 * Object spread copies own enumerable properties -- including the
 * runtime-only `capabilities` key `BaseEnv` does not type and the private
 * tool slot -- so every other env surface survives; only `capabilities` is
 * replaced.
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
 * factory reads the materialized tool runtime off the env (set by
 * `buildEnv` via `attachStepTools`), augments the step's
 * `AgentDefinition` with the loaded tool factories (wrapped to capture
 * each bundle's disposer), constructs the plugin chain on `env.plugins`,
 * builds the agent, and wraps `agent.close()` so every plugin instance
 * and tool bundle is disposed when the step's agent closes.
 *
 * When the env carries no materialized tools (e.g. a unit test using the
 * bare factory), the factory falls back to `createAgent(def, env)`
 * unchanged.
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

    // Assemble each package's consumer-scoped `credentials` capability once,
    // when the step carries credential wiring (a bare test build carries none,
    // yielding an empty map). Every factory in a package shares the one
    // capability; a package that declares a handle no binding resolves fails
    // the launch here, loudly, rather than at the tool's first resolve.
    const credentialWiring = getStepCredentialWiring(env);
    const credentialCapabilities =
      credentialWiring === undefined
        ? new Map<string, HostCredentialCapability>()
        : buildCredentialCapabilities(
            materialization.factories,
            credentialWiring,
          );

    // Wrap each loaded tool factory so its bundle's `dispose` (when present)
    // is captured. Dedupe by closure identity: a factory whose bundle returns
    // the same `dispose` on every invocation must not be torn down once per
    // push. Each package's credentials capability joins the same teardown set
    // so its shaped handles are released with the agent.
    const capturedDisposers = new Set<() => unknown>();
    for (const capability of credentialCapabilities.values()) {
      capturedDisposers.add(() => capability.dispose());
    }
    const factoriesWithCapture = materialization.factories.map((stf) =>
      rewrapStepToolFactory(
        stf.factory,
        (dispose) => {
          capturedDisposers.add(dispose);
        },
        credentialCapabilities.get(stf.packageName),
      ),
    );

    // Run every captured disposer -- each credentials capability and each
    // tool bundle -- guarding each so one failure does not strand the rest.
    // Used on the success teardown AND on the construction-failure rollbacks
    // below (the credentials capabilities are built before the plugin chain,
    // so a plugin or agent build that throws must still release them).
    // Bundle disposers are idempotent, so re-running one after `createAgent`
    // already disposed on its own failure path is safe.
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

    // Rebuild the def with the materialized tool factories. The serialized
    // `def.toolFactories` carry only `{ id, requires }` metadata (the workflow
    // projection strips closures on the wire), so the runnable factories come
    // from materialization, not the incoming def.
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

    // Instantiate plugin factories one at a time so each successive factory
    // sees the prior plugins' instances on `env.plugins` (posix's bundle reads
    // `env.plugins`; the LSP plugin factory populates them). On a midway
    // factory throw, every plugin instance already constructed releases what
    // it acquired before the construction error propagates, so a
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
      // Release the credentials capabilities (built before the plugin chain)
      // and any bundle disposers captured so far, then the plugin instances
      // this module owns.
      await runCapturedDisposers();
      await disposeAll(pluginInstances, "plugin construction rollback");
      throw err;
    }

    let agent: Agent;
    try {
      agent = await createAgent(toolDef, chainEnv);
    } catch (err) {
      // `createAgent` disposes the tool bundles it constructed on its own
      // failure path, but the credentials capabilities and the plugin
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
      // plugin twice is safe (`lsp.dispose()` clears its client set; the
      // posix bundle's dispose is idempotent), and running both guarantees
      // the LSP subprocess is torn down even for a plugin no tool bundle
      // consumed. Both loops run every disposer and collect failures rather
      // than throwing mid-loop, so one failing disposer never strands the
      // rest, and any failure fails the close so the caller sees it.
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
 * Return an `Agent` whose `close()` runs the original close and then the
 * supplied teardown -- after the reactor has stopped issuing tool calls.
 * `close()` is idempotent at the agent layer; this wrapper guards its own
 * teardown so a double `close()` does not double-dispose.
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
      // `await` accepts non-promise values verbatim, so this works whether
      // the disposer is sync or async.
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
