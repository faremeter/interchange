import fs from "node:fs/promises";
import path from "node:path";

import type {
  DestroySidecarRequest,
  EnsureSidecarRequest,
  SidecarProvisioner,
} from "@intx/hub-sessions";

const DEFAULT_STOP_TIMEOUT_MS = 1_000;
const REPO_ROOT = path.resolve(import.meta.dirname, "../../..");

export interface LocalSidecarProcess {
  readonly pid: number;
  readonly exited: Promise<number>;
  kill(signal: NodeJS.Signals): void;
}

export type SpawnLocalSidecar = (args: {
  readonly request: EnsureSidecarRequest;
  readonly dataDir: string;
}) => LocalSidecarProcess;

export type CreateLocalProcessSidecarProvisionerOpts = {
  readonly dataRoot: string;
  readonly spawnSidecar?: SpawnLocalSidecar;
  readonly stopTimeoutMs?: number;
};

export interface LocalProcessSidecarProvisioner {
  readonly provisioner: SidecarProvisioner;
  shutdown(): Promise<void>;
}

type ManagedProcess = {
  readonly handle: LocalSidecarProcess;
  exited: boolean;
};

type AllocationState =
  | {
      readonly kind: "live";
      readonly generation: number;
      readonly sidecarId: string;
      readonly dataDir: string;
      readonly process: ManagedProcess;
    }
  | {
      readonly kind: "destroyed";
      readonly generation: number;
      readonly sidecarId: string;
    };

function spawnSidecarProcess({
  request,
  dataDir,
}: Parameters<SpawnLocalSidecar>[0]): LocalSidecarProcess {
  const childProcess = Bun.spawn(
    ["bun", "run", "--conditions=intx-src", "apps/sidecar/src/index.ts"],
    {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        HUB_WS_URL: request.hubWebSocketUrl,
        SIDECAR_ID: request.sidecarId,
        SIDECAR_TOKEN: request.token,
        SIDECAR_DATA_DIR: dataDir,
        SIDECAR_CREDENTIAL_ENCRYPTION_KEY:
          "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
      },
      stdin: "ignore",
      stdout: "inherit",
      stderr: "inherit",
    },
  );
  return {
    pid: childProcess.pid,
    exited: childProcess.exited,
    kill(signal) {
      childProcess.kill(signal);
    },
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function exitedWithin(
  managed: ManagedProcess,
  timeoutMs: number,
): Promise<boolean> {
  if (managed.exited) return Promise.resolve(true);
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    void managed.handle.exited.then(() => {
      clearTimeout(timer);
      resolve(true);
    });
  });
}

export function createLocalProcessSidecarProvisioner({
  dataRoot,
  spawnSidecar = spawnSidecarProcess,
  stopTimeoutMs = DEFAULT_STOP_TIMEOUT_MS,
}: CreateLocalProcessSidecarProvisionerOpts): LocalProcessSidecarProvisioner {
  if (stopTimeoutMs <= 0) {
    throw new Error("Local sidecar stop timeout must be positive");
  }

  const allocations = new Map<string, AllocationState>();
  let shutdownPromise: Promise<void> | null = null;

  async function stopAndRemove(
    state: Extract<AllocationState, { kind: "live" }>,
  ): Promise<void> {
    if (!state.process.exited) {
      try {
        state.process.handle.kill("SIGTERM");
      } catch (error) {
        if (!state.process.exited) throw error;
      }
      if (!(await exitedWithin(state.process, stopTimeoutMs))) {
        state.process.handle.kill("SIGKILL");
        if (!(await exitedWithin(state.process, stopTimeoutMs))) {
          throw new Error(
            `Local sidecar process ${String(state.process.handle.pid)} did not exit`,
          );
        }
      }
    }
    await fs.rm(state.dataDir, { recursive: true, force: true });
  }

  async function ensure(request: EnsureSidecarRequest) {
    const existing = allocations.get(request.allocationId);
    if (existing !== undefined && existing.generation > request.generation) {
      return {
        kind: "rejected" as const,
        code: "stale_generation",
        message: `Generation ${String(request.generation)} is older than ${String(existing.generation)}`,
        retryable: false,
      };
    }
    // Replacement destroys the old identity at the new generation before
    // ensuring a fresh identity at that same generation. Fence only the exact
    // identity that was destroyed.
    if (
      existing?.kind === "destroyed" &&
      existing.generation === request.generation &&
      existing.sidecarId === request.sidecarId
    ) {
      return {
        kind: "rejected" as const,
        code: "generation_destroyed",
        message: `Generation ${String(request.generation)} was already destroyed`,
        retryable: false,
      };
    }
    if (
      existing?.kind === "live" &&
      existing.generation === request.generation &&
      existing.sidecarId !== request.sidecarId
    ) {
      return {
        kind: "rejected" as const,
        code: "sidecar_identity_conflict",
        message: `Generation ${String(request.generation)} already belongs to another sidecar identity`,
        retryable: false,
      };
    }
    if (
      existing?.kind === "live" &&
      existing.generation === request.generation &&
      !existing.process.exited
    ) {
      return {
        kind: "accepted" as const,
        externalRef: String(existing.process.handle.pid),
      };
    }
    if (existing?.kind === "live") {
      await stopAndRemove(existing);
    }

    await fs.mkdir(dataRoot, { recursive: true });
    const dataDir = await fs.mkdtemp(
      path.join(dataRoot, `${request.allocationId}-`),
    );
    try {
      const handle = spawnSidecar({ request, dataDir });
      const managed: ManagedProcess = { handle, exited: false };
      void handle.exited.then(() => {
        managed.exited = true;
      });
      allocations.set(request.allocationId, {
        kind: "live",
        generation: request.generation,
        sidecarId: request.sidecarId,
        dataDir,
        process: managed,
      });
      return { kind: "accepted" as const, externalRef: String(handle.pid) };
    } catch (error) {
      await fs.rm(dataDir, { recursive: true, force: true });
      return {
        kind: "rejected" as const,
        code: "spawn_failed",
        message: errorMessage(error),
        retryable: true,
      };
    }
  }

  async function destroy(request: DestroySidecarRequest) {
    const existing = allocations.get(request.allocationId);
    if (existing !== undefined && existing.generation > request.generation) {
      return { kind: "destroyed" as const };
    }
    // A delayed destroy for the superseded identity must not terminate the
    // replacement that already owns this generation.
    if (existing !== undefined && existing.sidecarId !== request.sidecarId) {
      return { kind: "destroyed" as const };
    }
    if (existing?.kind === "live") {
      await stopAndRemove(existing);
    }
    allocations.set(request.allocationId, {
      kind: "destroyed",
      generation: request.generation,
      sidecarId: request.sidecarId,
    });
    return { kind: "destroyed" as const };
  }

  const provisioner: SidecarProvisioner = {
    id: "local-process",
    apiVersion: 1,
    bindingFingerprint: "local-process:v1",
    capabilities: [],
    ensure,
    destroy,
  };

  return {
    provisioner,
    shutdown() {
      shutdownPromise ??= (async () => {
        const failures: unknown[] = [];
        for (const state of allocations.values()) {
          if (state.kind !== "live") continue;
          try {
            await stopAndRemove(state);
          } catch (error) {
            failures.push(error);
          }
        }
        allocations.clear();
        await fs.rm(dataRoot, { recursive: true, force: true });
        if (failures.length > 0) {
          throw new AggregateError(
            failures,
            "Failed to stop every local sidecar process",
          );
        }
      })();
      return shutdownPromise;
    },
  };
}
