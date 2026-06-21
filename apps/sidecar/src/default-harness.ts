// Default HarnessBuilder for the sidecar app.
//
// Implements the HarnessBuilder seam declared by @intx/hub-agent using
// the concrete plugins the sidecar app ships with: posix + LSP tools,
// the mail tools package, the isogit-backed context and mail audit
// stores, the authz engine, and the inference runtime. Any host that
// wants a different mix of plugins ships its own builder; the package
// never sees these concrete dependencies.

import fs from "node:fs";
import path from "node:path";
import { evaluateGrants } from "@intx/authz";
import {
  type BaseEnv,
  createDefaultDirectorRegistry,
  defineAgent,
} from "@intx/agent";
import { createHarness, type MailEnv } from "@intx/harness";
import {
  readDeployTree,
  type HarnessBuilder,
  type HarnessBundle,
} from "@intx/hub-agent";
import { hasProvider } from "@intx/inference";
import { getLogger } from "@intx/log";
import { createIsogitStore, createMailAuditStore } from "@intx/storage-isogit";
import type { InferenceSource } from "@intx/types/runtime";

import { materializeToolPackages } from "./tool-materialization";

const logger = getLogger(["sidecar", "harness-builder"]);

export interface DefaultHarnessBuilderConfig {
  /**
   * Root directory of the content-addressable tarball cache. The
   * loader instantiated on each apply opens a fresh `TarballCache`
   * against this path.
   */
  readonly cacheRoot: string;
  /**
   * Cache size cap, resolved at the boot edge so the per-apply
   * `TarballCache` receives a concrete value rather than re-reading
   * env at a non-boundary call site.
   */
  readonly cacheMaxBytes: number;
  /**
   * Per-tarball cap on the loader's HTTP-registry fetcher. Resolved
   * at the boot edge from `SIDECAR_REGISTRY_MAX_TARBALL_BYTES` so the
   * per-apply loader receives a concrete value.
   */
  readonly registryMaxTarballBytes: number;
}

export function createDefaultHarnessBuilder(
  config: DefaultHarnessBuilderConfig,
): HarnessBuilder {
  return {
    canBuildSource(source: InferenceSource): void {
      if (!hasProvider(source.provider)) {
        throw new Error(
          `Source provider "${source.provider}" is not registered`,
        );
      }
    },

    async build({
      agentAddress,
      agentConfig,
      sources,
      defaultSource,
      storeDir,
      agentTransport,
      crypto,
      onEvent,
      onConnectorStateChanged,
      emitDeployApplyError,
    }): Promise<HarnessBundle> {
      const signer = (payload: string) => crypto.signSSH(payload);

      const storage = await createIsogitStore(storeDir, signer);
      const mailStore = await createMailAuditStore(storeDir, signer);

      const deployTree = await readDeployTree(storeDir);
      const systemPrompt = deployTree.systemPrompt ?? agentConfig.systemPrompt;

      // Materialize tool-package manifest (if present) via the loader.
      // Failures emit a deploy.apply.error frame back to the hub and
      // abort harness construction; the prior deploy remains active.
      const { factories: loadedToolFactories, pluginFactories: loadedPlugins } =
        await materializeToolPackages({
          rawManifestBytes: deployTree.toolPackageManifestRaw,
          assetMounts: deployTree.assetMounts,
          storeDir,
          agentAddress,
          cacheRoot: config.cacheRoot,
          cacheMaxBytes: config.cacheMaxBytes,
          registryMaxTarballBytes: config.registryMaxTarballBytes,
          emitDeployApplyError,
        });

      const grantsRef = { current: agentConfig.grants };
      const { principalId, tenantId } = agentConfig;
      const authorize = async (
        resource: string,
        action: string,
        _context: unknown,
      ) =>
        evaluateGrants(grantsRef.current, resource, action, {
          principalId,
          tenantId,
        });

      const workDir = path.join(storeDir, "workspace");
      await fs.promises.mkdir(workDir, { recursive: true });

      // Per the @intx/agent ToolBundle contract, the agent does not
      // invoke `bundle.dispose` on normal shutdown — only on
      // construction rollback. The caller of `createHarness` is
      // responsible for disposing the bundle resources at session
      // end. Wrap each loaded factory so its bundle's `dispose`
      // (when present) lands in `capturedDisposers`; the harness's
      // `disposers` array below replays them on session teardown.
      // Dedupe by disposer-function identity: a factory whose bundle
      // returns the same `dispose` closure on every invocation would
      // otherwise re-append it on agent rebuild / hot-reapply paths
      // and the teardown loop would call it once per push. A `Set`
      // keyed on the closure reference collapses identical pushes
      // while still capturing distinct disposers from genuinely
      // distinct bundles.
      const capturedDisposers = new Set<() => unknown>();
      const factoriesWithCapture = loadedToolFactories.map((f) => {
        const wrapped = (env: BaseEnv) => {
          const bundle = f(env);
          if (bundle.dispose !== undefined)
            capturedDisposers.add(bundle.dispose);
          return bundle;
        };
        // Freeze the harness wrapper so downstream consumers cannot
        // mutate `wrapped.id` / `wrapped.requires`. The loader already
        // froze the source factory's wrapper, but `Object.assign` here
        // produces a fresh outer object whose own `id`/`requires`
        // would be writable without this freeze — defeating the
        // invariant the loader's wrapper documents. Object.freeze on
        // the outer reference is enough; the inner `requires` array
        // is already frozen at the loader layer.
        return Object.freeze(
          Object.assign(wrapped, { id: f.id, requires: f.requires }),
        );
      });

      const def = defineAgent({
        id: agentAddress,
        systemPrompt,
        // Mail, posix, and LSP travel through the loader path as
        // ordinary tool packages — the agent definition pins them and
        // the materialization above resolved their factories. The
        // sidecar harness no longer constructs them directly.
        tools: factoriesWithCapture,
        capabilities: [],
        inference: {
          sources: sources.map((s) => ({
            provider: s.provider,
            model: s.model,
          })),
        },
      });

      const baseEnv: MailEnv = {
        sources,
        defaultSource,
        storage,
        workdir: workDir,
        audit: storage,
        authorize,
        directors: createDefaultDirectorRegistry(),
        transport: agentTransport,
        address: agentAddress,
        onConnectorStateChanged,
      };

      // Instantiate plugin factories one at a time so each successive
      // factory sees the prior plugins' instances on `env.plugins`.
      // The chain is rebuilt incrementally rather than handing every
      // factory the same baseEnv (which would force a plugin that
      // wanted to consume other plugins to receive an undefined slot).
      // Posix's bundle reads `env.plugins` and threads ToolPlugin-
      // shaped values into `createPosixTools`; LSP's plugin factory
      // is what populates them.
      //
      // If a midway factory throws, every plugin instance already
      // constructed has to release whatever it acquired (LSP starts a
      // subprocess) before the construction error propagates.
      // Otherwise a partial-success chain leaks resources the
      // harness never owned.
      const pluginInstances: unknown[] = [];
      let chainEnv: MailEnv = baseEnv;
      try {
        for (const factory of loadedPlugins) {
          const instance = factory(chainEnv);
          pluginInstances.push(instance);
          // Each iteration constructs a fresh `chainEnv` object so a
          // factory observing `env.plugins` sees the prior chain
          // entries. The non-`plugins` fields (storage, transport,
          // address, etc.) are shared by reference across every
          // chainEnv produced here, so factories must not compare
          // envs by identity — equality checks across iterations
          // would always return false even though the underlying
          // substrate is the same object graph.
          chainEnv = {
            ...baseEnv,
            plugins: [...pluginInstances],
          };
        }
      } catch (err) {
        for (const instance of pluginInstances) {
          if (instance === null || typeof instance !== "object") continue;
          if (!("dispose" in instance)) continue;
          const dispose: unknown = (instance as { dispose: unknown }).dispose;
          if (typeof dispose !== "function") continue;
          try {
            // `await` accepts non-promise values verbatim, so this
            // works whether the plugin's disposer is sync or async
            // without the Promise.resolve indirection that would
            // silently flatten a thrown-then-rejected pair.
            const result: unknown = dispose.call(instance);
            await result;
          } catch (disposeErr) {
            // A disposer that throws during harness construction
            // rollback leaks whatever the plugin acquired (LSP starts
            // a subprocess; a leak survives the harness teardown). At
            // this point the tool-package apply itself already
            // succeeded — materializeToolPackages returned — so the
            // failure is not attributable to any specific apply
            // attempt. Emitting a deploy.apply.error frame with a
            // freshly-minted attemptId and a `none` previousDeployId
            // would land in the hub's audit trail uncorrelated with
            // any real apply record, so log the leak and rely on the
            // sidecar log line; the hub's session-launch failure
            // (raised by the outer throw below) already informs the
            // operator that the harness did not come up.
            logger.error`plugin dispose failed during harness construction rollback: ${disposeErr instanceof Error ? disposeErr.message : String(disposeErr)}`;
          }
        }
        throw err;
      }
      const env: MailEnv = chainEnv;

      const harness = await createHarness(def, env);

      // The sidecar's event channel expects onEvent calls. The
      // harness exposes the underlying agent's event stream; forward
      // every event onto the legacy callback EXCEPT `message.received`,
      // which is the single intentional exclusion.
      //
      // Why `message.received` is filtered:
      //  - It is an assembly-internal signal. The reactor emits it
      //    when an inbound mail dequeues, but the hub-facing audit
      //    chain expresses per-message work as the
      //    `message.run.started` / `message.run.ended` bracket pair,
      //    minted around the same dequeue. Forwarding both would
      //    double-count per-message work in downstream consumers
      //    (SessionManager, workflow-runtime translation) and would
      //    leak the dequeue-vs-bracket distinction into a layer that
      //    has no use for it.
      //  - The raw inbound mail bytes are already authoritative in
      //    the mail-audit store; the bracket events carry messageId
      //    plus the reactor-minted messageRunId, which is what the
      //    audit chain needs to correlate.
      //
      // The filter is an allowlist-of-everything-except, so new
      // InferenceEvent members flow through by default. In particular,
      // `message.run.started` and `message.run.ended` are forwarded
      // unchanged; widening this filter to exclude other event types
      // would silently drop hub-facing audit data and must be done
      // deliberately.
      //
      // `harness.stream()` is invoked synchronously here so the
      // underlying StreamConsumer is registered before any microtask
      // runs. The IIFE below iterates the returned AsyncIterable;
      // events emitted in the window between createHarness()
      // resolving and the for-await loop starting are buffered by the
      // consumer and delivered on the first iteration.
      const events = harness.stream();
      let stopForward = false;
      const forwardDone = (async () => {
        try {
          for await (const event of events) {
            if (stopForward) break;
            if (event.type === "message.received") continue;
            onEvent(event);
          }
        } catch (cause) {
          logger.warn`Event forwarder terminated: ${cause}`;
        }
      })();

      return {
        harness,
        mailStore,
        updateGrants(grants) {
          grantsRef.current = grants;
        },
        // The event-forwarder shutdown runs first so no more events
        // reach a downstream that is about to release resources. Then
        // every captured `ToolBundle.dispose` runs in registration
        // order — the agent built the bundles in factory order, so
        // disposing in the same order matches that build sequence.
        // Each dispose is wrapped in its own try so one runner's
        // failure does not stop the others from running.
        disposers: [
          async () => {
            stopForward = true;
            await forwardDone;
          },
          async () => {
            for (const dispose of capturedDisposers) {
              try {
                await dispose();
              } catch (disposeErr) {
                logger.warn`tool bundle dispose failed during session teardown: ${disposeErr instanceof Error ? disposeErr.message : String(disposeErr)}`;
              }
            }
          },
        ],
      };
    },
  };
}
