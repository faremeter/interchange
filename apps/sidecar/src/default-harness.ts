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
  createHarness,
  createHarnessRuntimeCapabilities,
  mergeToolRunners,
  readDeployTree,
} from "@intx/harness";
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

      // Construct disk-backed siblings up front. If a later step throws
      // their handles are still safe to drop on the floor — the stores
      // hold no process-level resources that require explicit teardown.
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
        // Same transport reference: mail tools send via it, the harness
        // watches INBOX and sends connector replies on it.
        const capabilities = createHarnessRuntimeCapabilities({
          transport: agentTransport,
        });
        const mailTools = createMailTools({ capabilities });

        try {
          const tools = mergeToolRunners([mailTools, posixTools]);

          const harness = createHarness({
            address: agentAddress,
            systemPrompt,
            source,
            transport: agentTransport,
            crypto,
            storage,
            authorize,
            auditStore: storage,
            tools,
            onEvent,
            onConnectorStateChanged,
          });

          return {
            harness,
            mailStore,
            updateGrants(grants) {
              grantsRef.current = grants;
            },
            // LIFO of physical construction order: mail was created
            // after posix, so mail is disposed first. Both disposers
            // are currently independent and idempotent; LIFO is the
            // safe default for any future addition that introduces a
            // dependency between them.
            disposers: [() => mailTools.dispose(), () => posixTools.dispose()],
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
