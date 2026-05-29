// Default HarnessBuilder for the sidecar app.
//
// Implements the HarnessBuilder seam declared by @intx/hub-agent using
// the concrete plugins the sidecar app ships with: posix + LSP tools,
// the isogit-backed context and mail audit stores, the authz engine,
// and the inference runtime. Any host that wants a different mix of
// plugins ships its own builder; the package never sees these
// concrete dependencies.

import fs from "node:fs";
import path from "node:path";
import { evaluateGrants } from "@intx/authz";
import { createHarness, readDeployTree } from "@intx/harness";
import { hasProvider } from "@intx/inference";
import { getLogger } from "@intx/log";
import { createIsogitStore, createMailAuditStore } from "@intx/storage-isogit";
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

      try {
        const harness = createHarness({
          address: agentAddress,
          systemPrompt,
          source,
          transport: agentTransport,
          crypto,
          storage,
          authorize,
          auditStore: storage,
          deployTools: deployTree.tools,
          tools: posixTools,
          onEvent,
          onConnectorStateChanged,
        });

        return {
          harness,
          mailStore,
          updateGrants(grants) {
            grantsRef.current = grants;
          },
          disposers: [() => posixTools.dispose()],
        };
      } catch (err) {
        // Honor the dispose contract on the failure path. Whatever the
        // tool runner allocated to back its dispose method — child
        // processes, file handles, sockets, nothing — is its concern
        // to release, and the builder's job is to make sure dispose
        // runs whether construction succeeds or fails. A failure in
        // dispose is logged but does not mask the original
        // construction failure that is about to propagate.
        try {
          await posixTools.dispose();
        } catch (disposeErr) {
          const msg =
            disposeErr instanceof Error
              ? disposeErr.message
              : String(disposeErr);
          logger.warn`posixTools.dispose failed during harness rollback: ${msg}`;
        }
        throw err;
      }
    },
  };
}
