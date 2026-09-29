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

/**
 * Names the sidecar process a probe or allocation joins. Work with the same
 * key shares one process while any of it is live; `null` gives the work a
 * process of its own.
 */
export type ShareLocalSidecarsBy = (
  request: EnsureSidecarRequest,
) => string | null;

export type CreateLocalProcessSidecarProvisionerOpts = {
  readonly dataRoot: string;
  readonly spawnSidecar?: SpawnLocalSidecar;
  readonly shareSidecarsBy?: ShareLocalSidecarsBy;
  readonly stopTimeoutMs?: number;
};

export interface LocalProcessSidecarProvisioner {
  readonly provisioner: SidecarProvisioner;
  /** Process ids of the running sidecars and the work each hosts. */
  sidecars(): { readonly pid: number; readonly hosts: readonly string[] }[];
  shutdown(): Promise<void>;
}

type ManagedProcess = {
  readonly handle: LocalSidecarProcess;
  exited: boolean;
};

type LocalSidecar = {
  readonly sidecarId: string;
  readonly shareKey: string | null;
  readonly dataDir: string;
  readonly process: ManagedProcess;
  readonly hosts: Set<string>;
};

type AllocationState =
  | {
      readonly kind: "live";
      readonly generation: number;
      // The identity this generation's ensure was offered. It differs from
      // the sidecar's own id when the work joined an existing sidecar.
      readonly offeredSidecarId: string;
      readonly sidecar: LocalSidecar;
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

function chain() {
  const tails = new Map<string, Promise<void>>();
  return function run<T>(key: string, task: () => Promise<T>): Promise<T> {
    const previous = tails.get(key) ?? Promise.resolve();
    const pending = previous.then(task);
    const settled = pending.then(
      () => undefined,
      () => undefined,
    );
    tails.set(key, settled);
    void settled.then(() => {
      if (tails.get(key) === settled) tails.delete(key);
    });
    return pending;
  };
}

export function createLocalProcessSidecarProvisioner({
  dataRoot,
  spawnSidecar = spawnSidecarProcess,
  shareSidecarsBy = () => null,
  stopTimeoutMs = DEFAULT_STOP_TIMEOUT_MS,
}: CreateLocalProcessSidecarProvisionerOpts): LocalProcessSidecarProvisioner {
  if (stopTimeoutMs <= 0) {
    throw new Error("Local sidecar stop timeout must be positive");
  }

  const allocations = new Map<string, AllocationState>();
  const sidecars = new Set<LocalSidecar>();
  const operations = new Map<string, Promise<void>>();
  // Placement and release on shared sidecars are ordered per share key, so
  // two allocations cannot both start the process they were meant to share,
  // and none joins a sidecar that is being stopped.
  const placements = chain();
  let shutdownPromise: Promise<void> | null = null;

  function serialize<T>(
    allocationId: string,
    run: () => Promise<T>,
  ): Promise<T> {
    if (shutdownPromise !== null) {
      return Promise.reject(
        new Error("Local sidecar provisioner is shutting down"),
      );
    }
    const previous = operations.get(allocationId) ?? Promise.resolve();
    const pending = previous.then(run);
    const settled = pending.then(
      () => undefined,
      () => undefined,
    );
    operations.set(allocationId, settled);
    void settled.then(() => {
      if (operations.get(allocationId) === settled)
        operations.delete(allocationId);
    });
    return pending;
  }

  function placementKey(shareKey: string | null, allocationId: string) {
    return shareKey === null ? `own:${allocationId}` : `shared:${shareKey}`;
  }

  // A sidecar stays tracked until its process has exited, so a shutdown still
  // finds one whose stop failed.
  async function stopSidecar(sidecar: LocalSidecar): Promise<void> {
    const managed = sidecar.process;
    if (!managed.exited) {
      try {
        managed.handle.kill("SIGTERM");
      } catch (error) {
        if (!managed.exited) throw error;
      }
      if (!(await exitedWithin(managed, stopTimeoutMs))) {
        managed.handle.kill("SIGKILL");
        if (!(await exitedWithin(managed, stopTimeoutMs))) {
          throw new Error(
            `Local sidecar process ${String(managed.handle.pid)} did not exit`,
          );
        }
      }
    }
    sidecars.delete(sidecar);
    await fs.rm(sidecar.dataDir, { recursive: true, force: true });
  }

  function release(
    allocationId: string,
    state: Extract<AllocationState, { kind: "live" }>,
  ): Promise<void> {
    const { sidecar } = state;
    return placements(
      placementKey(sidecar.shareKey, allocationId),
      async () => {
        sidecar.hosts.delete(allocationId);
        if (sidecar.hosts.size === 0) await stopSidecar(sidecar);
      },
    );
  }

  // Joins a live sidecar with the same share key, or starts one with the
  // identity the request offers.
  function place(request: EnsureSidecarRequest): Promise<LocalSidecar> {
    const shareKey = shareSidecarsBy(request);
    return placements(
      placementKey(shareKey, request.allocationId),
      async () => {
        const shared =
          shareKey === null
            ? undefined
            : [...sidecars].find(
                (sidecar) =>
                  sidecar.shareKey === shareKey &&
                  !sidecar.process.exited &&
                  sidecar.hosts.size > 0,
              );
        if (shared !== undefined) {
          shared.hosts.add(request.allocationId);
          return shared;
        }
        await fs.mkdir(dataRoot, { recursive: true });
        const dataDir = await fs.mkdtemp(
          path.join(dataRoot, `${request.sidecarId}-`),
        );
        try {
          request.signal?.throwIfAborted();
          const handle = spawnSidecar({ request, dataDir });
          const managed: ManagedProcess = { handle, exited: false };
          void handle.exited.then(() => {
            managed.exited = true;
          });
          const sidecar: LocalSidecar = {
            sidecarId: request.sidecarId,
            shareKey,
            dataDir,
            process: managed,
            hosts: new Set([request.allocationId]),
          };
          sidecars.add(sidecar);
          return sidecar;
        } catch (error) {
          await fs.rm(dataDir, { recursive: true, force: true });
          throw error;
        }
      },
    );
  }

  function accepted(state: Extract<AllocationState, { kind: "live" }>) {
    const { sidecar } = state;
    return {
      kind: "accepted" as const,
      externalRef: String(sidecar.process.handle.pid),
      ...(sidecar.sidecarId !== state.offeredSidecarId
        ? { sidecarId: sidecar.sidecarId }
        : {}),
    };
  }

  async function ensure(request: EnsureSidecarRequest) {
    request.signal?.throwIfAborted();
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
      existing.offeredSidecarId !== request.sidecarId
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
      !existing.sidecar.process.exited
    ) {
      return accepted(existing);
    }
    if (existing?.kind === "live") {
      await release(request.allocationId, existing);
    }

    let placed: LocalSidecar;
    try {
      placed = await place(request);
    } catch (error) {
      return {
        kind: "rejected" as const,
        code: "spawn_failed",
        message: errorMessage(error),
        retryable: true,
      };
    }
    const state = {
      kind: "live" as const,
      generation: request.generation,
      offeredSidecarId: request.sidecarId,
      sidecar: placed,
    };
    allocations.set(request.allocationId, state);
    return accepted(state);
  }

  async function destroy(request: DestroySidecarRequest) {
    const existing = allocations.get(request.allocationId);
    if (existing !== undefined && existing.generation > request.generation) {
      return { kind: "destroyed" as const };
    }
    // A delayed destroy for the superseded identity must not terminate the
    // replacement that already owns this generation. The Hub names either the
    // identity it offered or the sidecar the work was placed on.
    if (
      existing?.kind === "live" &&
      request.sidecarId !== existing.offeredSidecarId &&
      request.sidecarId !== existing.sidecar.sidecarId
    ) {
      return { kind: "destroyed" as const };
    }
    if (
      existing?.kind === "destroyed" &&
      existing.sidecarId !== request.sidecarId
    ) {
      return { kind: "destroyed" as const };
    }
    if (existing?.kind === "live") {
      await release(request.allocationId, existing);
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
    // Keep cleanup behind any late ensure for the same allocation, including
    // work the Hub stopped awaiting after an operation timeout.
    ensure: (request) => serialize(request.allocationId, () => ensure(request)),
    destroy: (request) =>
      serialize(request.allocationId, () => destroy(request)),
  };

  return {
    provisioner,
    sidecars() {
      return [...sidecars].map((sidecar) => ({
        pid: sidecar.process.handle.pid,
        hosts: [...sidecar.hosts],
      }));
    },
    shutdown() {
      shutdownPromise ??= (async () => {
        await Promise.all(operations.values());
        const failures: unknown[] = [];
        for (const sidecar of [...sidecars]) {
          try {
            await stopSidecar(sidecar);
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
