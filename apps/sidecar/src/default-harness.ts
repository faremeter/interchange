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
  createDefaultDirectorRegistry,
  defineAgent,
  defineTool,
} from "@intx/agent";
import {
  createHarness,
  createHarnessRuntimeCapabilities,
  defineMailTools,
  type MailEnv,
} from "@intx/harness";
import { readDeployTree } from "@intx/hub-agent";
import { hasProvider } from "@intx/inference";
import { getLogger } from "@intx/log";
import { createIsogitStore, createMailAuditStore } from "@intx/storage-isogit";
import { createMailTools } from "@intx/tools-mail";
import { createPosixTools } from "@intx/tools-posix";
import { createLSPPlugin } from "@intx/tools-lsp";
import { createBlobReader } from "@intx/types/runtime";
import type { InferenceSource } from "@intx/types/runtime";
import type { HarnessBuilder, HarnessBundle } from "@intx/hub-agent";

const logger = getLogger(["sidecar", "harness-builder"]);

export function createDefaultHarnessBuilder(): HarnessBuilder {
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
      source,
      storeDir,
      agentTransport,
      crypto,
      onEvent,
      onConnectorStateChanged,
    }): Promise<HarnessBundle> {
      const signer = (payload: string) => crypto.signSSH(payload);

      const storage = await createIsogitStore(storeDir, signer);
      const mailStore = await createMailAuditStore(storeDir, signer);

      const deployTree = await readDeployTree(storeDir);
      const systemPrompt = deployTree.systemPrompt ?? agentConfig.systemPrompt;

      const grantsRef = { current: agentConfig.grants };
      const { principalId, tenantId } = agentConfig;
      const authorize = async (resource: string, action: string) =>
        evaluateGrants(grantsRef.current, resource, action, {
          principalId,
          tenantId,
        });

      const workDir = path.join(storeDir, "workspace");
      await fs.promises.mkdir(workDir, { recursive: true });

      const blobReader = createBlobReader(storage);
      const posixTools = createPosixTools({
        cwd: workDir,
        plugins: [createLSPPlugin({ cwd: workDir })],
        blobReader,
      });

      // Nested try blocks isolate cleanup responsibility per
      // construction step. The outer catch always disposes
      // posixTools; the inner catch disposes mailTools only when it
      // was successfully constructed. Each dispose is itself
      // try-wrapped so a dispose failure does not mask the original
      // construction error.
      const reportDisposeFailure = (runner: string, error: unknown): void => {
        logger.warn`${runner}.dispose failed during harness rollback: ${error}`;
      };

      try {
        const capabilities = createHarnessRuntimeCapabilities({
          transport: agentTransport,
        });
        const mailTools = createMailTools({ capabilities });

        try {
          // Wrap each tool bundle as an AnnotatedToolFactory. The
          // posix bundle is transport-independent (no `requires`); the
          // mail bundle declares requires: ["transport", "address"]
          // via defineMailTools. The intersection EnvRequiredByAll
          // lands on MailEnv because the mail factory drags it up.
          const posixFactory = defineTool({
            id: "@intx/tools-posix/sidecar-bundle",
            factory: () => ({
              definitions: posixTools.definitions,
              run: (call, signal) => posixTools.run(call, signal),
            }),
          });

          const mailFactory = defineMailTools(() => ({
            definitions: mailTools.definitions,
            run: (call, signal) => mailTools.run(call, signal),
          }));

          const def = defineAgent({
            id: agentAddress,
            systemPrompt,
            tools: [mailFactory, posixFactory],
            capabilities: [],
            inference: {
              sources: [{ provider: source.provider, model: source.model }],
            },
          });

          const env: MailEnv = {
            source,
            storage,
            workdir: workDir,
            audit: storage,
            authorize,
            directors: createDefaultDirectorRegistry(),
            transport: agentTransport,
            address: agentAddress,
            onConnectorStateChanged,
          };

          const harness = await createHarness(def, env);

          // The sidecar's event channel expects onEvent calls. The
          // harness exposes the underlying agent's event stream;
          // forward every event (except message.received, an
          // assembly-internal signal) onto the legacy callback.
          //
          // `harness.stream()` is invoked synchronously here so the
          // underlying StreamConsumer is registered before any
          // microtask runs. The IIFE below iterates the returned
          // AsyncIterable; events emitted in the window between
          // createHarness() resolving and the for-await loop starting
          // are buffered by the consumer and delivered on the first
          // iteration.
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
            // LIFO of physical construction order: mail was created
            // after posix, so mail is disposed first. The event
            // forwarder runs in the background; awaiting it during
            // teardown lets the caller observe a settled state.
            disposers: [
              async () => {
                stopForward = true;
                await forwardDone;
              },
              () => mailTools.dispose(),
              () => posixTools.dispose(),
            ],
          };
        } catch (innerErr) {
          try {
            await mailTools.dispose();
          } catch (disposeErr) {
            reportDisposeFailure("mailTools", disposeErr);
          }
          throw innerErr;
        }
      } catch (outerErr) {
        try {
          await posixTools.dispose();
        } catch (disposeErr) {
          reportDisposeFailure("posixTools", disposeErr);
        }
        throw outerErr;
      }
    },
  };
}
