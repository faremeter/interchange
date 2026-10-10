import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { expect, test } from "@playwright/test";
import type { EnsureSidecarRequest } from "@intx/hub-sessions";

import {
  createLocalProcessSidecarProvisioner,
  type LocalSidecarProcess,
} from "./local-process-sidecar-provisioner";

const tempDirs: string[] = [];

test.afterEach(async () => {
  await Promise.all(
    tempDirs
      .splice(0)
      .map((dir) => fs.rm(dir, { recursive: true, force: true })),
  );
});

function createRequest(
  generation = 0,
  sidecarId = `sc_${String(generation)}`,
): EnsureSidecarRequest {
  return {
    allocationId: "sal_test",
    generation,
    tenantId: "tnt_test",
    anchorRunId: "run_test",
    sidecarId,
    token: `token_${String(generation)}`,
    hubWebSocketUrl: "ws://127.0.0.1:3000/api/sidecars/ws",
  };
}

function createFakeProcess(pid: number) {
  const { promise, resolve } = Promise.withResolvers<number>();
  const signals: NodeJS.Signals[] = [];
  const handle: LocalSidecarProcess = {
    pid,
    exited: promise,
    kill(signal) {
      signals.push(signal);
      resolve(0);
    },
  };
  return { process: handle, signals, exit: () => resolve(0) };
}

async function createHarness(stopTimeoutMs = 10) {
  const dataRoot = await fs.mkdtemp(
    path.join(os.tmpdir(), "intx-local-provisioner-test-"),
  );
  tempDirs.push(dataRoot);
  const spawned: ReturnType<typeof createFakeProcess>[] = [];
  const local = await createLocalProcessSidecarProvisioner({
    dataRoot,
    spawnSidecar() {
      const fake = createFakeProcess(1000 + spawned.length);
      spawned.push(fake);
      return fake.process;
    },
    stopTimeoutMs,
  });
  return { local, spawned, dataRoot };
}

test.describe("createLocalProcessSidecarProvisioner", () => {
  test("a cancelled ensure does not block later cleanup", async () => {
    const { local, spawned, dataRoot } = await createHarness();
    const controller = new AbortController();
    const request = { ...createRequest(), signal: controller.signal };
    const ensuring = local.provisioner.ensure(request);
    controller.abort(new Error("ensure timed out"));
    const destroying = local.provisioner.destroy(request);
    await expect(ensuring).rejects.toThrow("ensure timed out");
    expect(await destroying).toEqual({
      kind: "destroyed",
      cleanup: "confirmed",
    });
    expect(spawned).toEqual([]);
    expect(await fs.readdir(dataRoot)).toEqual([]);
    await local.shutdown();
  });

  test("a clean harness restart confirms old cleanup even after unrelated work starts", async () => {
    const { local, spawned, dataRoot } = await createHarness();
    const request = createRequest(1);
    await local.provisioner.ensure(request);
    await local.shutdown();
    expect(spawned[0]?.signals).toEqual(["SIGTERM"]);
    await expect(fs.stat(dataRoot)).rejects.toMatchObject({ code: "ENOENT" });

    const next = createFakeProcess(2000);
    const restarted = await createLocalProcessSidecarProvisioner({
      dataRoot,
      spawnSidecar: () => next.process,
    });
    try {
      await restarted.provisioner.ensure({
        ...createRequest(1, "sc_new"),
        allocationId: "sal_new",
      });
      const release = { ...request, generation: 2 };
      expect(await restarted.provisioner.destroy(release)).toEqual({
        kind: "destroyed",
        cleanup: "confirmed",
      });
      expect(await restarted.provisioner.destroy(release)).toEqual({
        kind: "destroyed",
        cleanup: "confirmed",
      });
      expect(next.signals).toEqual([]);
      expect(restarted.sidecars()).toEqual([{ pid: 2000, hosts: ["sal_new"] }]);
    } finally {
      await restarted.shutdown();
    }
  });

  test("a restart with old state does not claim an unknown worker is gone or erase its files", async () => {
    const { local, spawned, dataRoot } = await createHarness();
    const request = createRequest(1);
    await local.provisioner.ensure(request);
    const [directory] = await fs.readdir(dataRoot);
    if (directory === undefined) throw new Error("Missing worker directory");
    const record = path.join(dataRoot, directory, "deployment.json");
    await fs.writeFile(record, "preserved worker state");
    const restarted = await createLocalProcessSidecarProvisioner({
      dataRoot,
      spawnSidecar: () => {
        throw new Error("Unexpected spawn");
      },
    });
    try {
      expect(
        await restarted.provisioner.destroy({ ...request, generation: 2 }),
      ).toEqual({ kind: "destroyed", cleanup: "required" });
      expect(spawned[0]?.signals).toEqual([]);
      await expect(restarted.shutdown()).rejects.toMatchObject({
        code: "ENOTEMPTY",
      });
      expect(await fs.readFile(record, "utf8")).toBe("preserved worker state");
    } finally {
      await local.shutdown();
    }
  });

  test("a failed process stop preserves the state that prevents a false cleanup confirmation on restart", async () => {
    const { local, spawned, dataRoot } = await createHarness();
    const request = createRequest(1);
    await local.provisioner.ensure(request);
    const worker = spawned[0];
    if (worker === undefined) throw new Error("Missing worker");
    worker.process.kill = (signal) => {
      worker.signals.push(signal);
    };
    try {
      await expect(local.shutdown()).rejects.toThrow(
        "Failed to stop every local sidecar process",
      );
      expect(await fs.readdir(dataRoot)).toHaveLength(1);
      const restarted = await createLocalProcessSidecarProvisioner({
        dataRoot,
        spawnSidecar: () => {
          throw new Error("Unexpected spawn");
        },
      });
      expect(
        await restarted.provisioner.destroy({ ...request, generation: 2 }),
      ).toEqual({ kind: "destroyed", cleanup: "required" });
      await expect(restarted.shutdown()).rejects.toMatchObject({
        code: "ENOTEMPTY",
      });
    } finally {
      worker.exit();
      await worker.process.exited;
    }
  });

  test("serializes one allocation while another can still start", async () => {
    const { local, spawned } = await createHarness(5_000);
    await local.provisioner.ensure(createRequest());
    const original = spawned[0];
    if (original === undefined) throw new Error("Missing original process");
    const stopping = Promise.withResolvers<boolean>();
    original.process.kill = (signal) => {
      original.signals.push(signal);
      stopping.resolve(true);
    };
    try {
      const destroying = local.provisioner.destroy(createRequest(1, "sc_0"));
      await stopping.promise;
      const replacement = local.provisioner.ensure(createRequest(1, "sc_new"));
      expect(
        await local.provisioner.ensure({
          ...createRequest(),
          allocationId: "sal_independent",
        }),
      ).toMatchObject({ kind: "accepted" });
      expect(spawned).toHaveLength(2);

      original.exit();
      await destroying;
      expect(await replacement).toMatchObject({ kind: "accepted" });
      expect(spawned).toHaveLength(3);
      expect(original.signals).toEqual(["SIGTERM"]);
    } finally {
      original.exit();
      await local.shutdown();
    }
  });

  test("shutdown drains admitted work and refuses new work", async () => {
    const { local, spawned, dataRoot } = await createHarness();
    const ensuring = local.provisioner.ensure(createRequest());
    const stopping = local.shutdown();
    await Promise.all([ensuring, stopping]);
    expect(spawned).toHaveLength(1);
    expect(spawned[0]?.signals).toEqual(["SIGTERM"]);
    await expect(fs.stat(dataRoot)).rejects.toMatchObject({ code: "ENOENT" });
    await expect(local.provisioner.ensure(createRequest(1))).rejects.toThrow(
      "shutting down",
    );
    expect(spawned).toHaveLength(1);
  });

  test("does not leave a worker alive when destroy overlaps ensure", async () => {
    const { local, spawned, dataRoot } = await createHarness();
    const request = createRequest();
    try {
      const ensuring = local.provisioner.ensure(request);
      const destroying = local.provisioner.destroy(request);
      await Promise.all([ensuring, destroying]);

      expect(spawned).toHaveLength(1);
      expect(spawned[0]?.signals).toEqual(["SIGTERM"]);
      expect(await fs.readdir(dataRoot)).toEqual([]);
    } finally {
      await local.shutdown();
    }
  });

  test("reuses the process for repeated ensure calls", async () => {
    const { local, spawned } = await createHarness();

    expect(await local.provisioner.ensure(createRequest())).toEqual({
      kind: "accepted",
      externalRef: "1000",
    });
    expect(await local.provisioner.ensure(createRequest())).toEqual({
      kind: "accepted",
      externalRef: "1000",
    });
    expect(spawned).toHaveLength(1);

    await local.shutdown();
  });

  test("fences destroyed and stale generations", async () => {
    const { local } = await createHarness();

    await local.provisioner.ensure(createRequest(1));
    await local.provisioner.destroy({
      allocationId: "sal_test",
      generation: 1,
      sidecarId: "sc_1",
    });

    expect(await local.provisioner.ensure(createRequest(1))).toMatchObject({
      kind: "rejected",
      code: "generation_destroyed",
      retryable: false,
    });
    expect(await local.provisioner.ensure(createRequest(0))).toMatchObject({
      kind: "rejected",
      code: "stale_generation",
      retryable: false,
    });
  });

  test("stops the previous process before advancing generations", async () => {
    const { local, spawned, dataRoot } = await createHarness();

    await local.provisioner.ensure(createRequest(0));
    await local.provisioner.ensure(createRequest(1));

    expect(spawned).toHaveLength(2);
    expect(spawned[0]?.signals).toEqual(["SIGTERM"]);
    expect(await fs.readdir(dataRoot)).toHaveLength(1);

    await local.shutdown();
    expect(spawned[1]?.signals).toEqual(["SIGTERM"]);
  });

  test("accepts a new identity after destroying the old identity at the replacement generation", async () => {
    const { local, spawned } = await createHarness();

    await local.provisioner.ensure(createRequest(0, "sc_old"));
    await local.provisioner.destroy({
      allocationId: "sal_test",
      generation: 1,
      sidecarId: "sc_old",
    });

    expect(await local.provisioner.ensure(createRequest(1, "sc_new"))).toEqual({
      kind: "accepted",
      externalRef: "1001",
    });
    expect(
      await local.provisioner.ensure(createRequest(1, "sc_old")),
    ).toMatchObject({
      kind: "rejected",
      code: "sidecar_identity_conflict",
      retryable: false,
    });

    await local.provisioner.destroy({
      allocationId: "sal_test",
      generation: 1,
      sidecarId: "sc_old",
    });
    expect(spawned[1]?.signals).toEqual([]);
    expect(await local.provisioner.ensure(createRequest(1, "sc_new"))).toEqual({
      kind: "accepted",
      externalRef: "1001",
    });

    await local.shutdown();
    expect(spawned[1]?.signals).toEqual(["SIGTERM"]);
  });
});

test.describe("createLocalProcessSidecarProvisioner sharing sidecars", () => {
  async function createSharingHarness() {
    const dataRoot = await fs.mkdtemp(
      path.join(os.tmpdir(), "intx-local-provisioner-share-test-"),
    );
    tempDirs.push(dataRoot);
    const spawned: ReturnType<typeof createFakeProcess>[] = [];
    const local = await createLocalProcessSidecarProvisioner({
      dataRoot,
      spawnSidecar() {
        const fake = createFakeProcess(1000 + spawned.length);
        spawned.push(fake);
        return fake.process;
      },
      shareSidecarsBy: (request) => request.tenantId,
      stopTimeoutMs: 10,
    });
    return { local, spawned, dataRoot };
  }

  function requestFor(
    allocationId: string,
    tenantId = "tnt_test",
  ): EnsureSidecarRequest {
    return {
      ...createRequest(0, `sc_${allocationId}`),
      allocationId,
      tenantId,
    };
  }

  test("places a tenant's allocations on one sidecar", async () => {
    const { local, spawned } = await createSharingHarness();

    expect(await local.provisioner.ensure(requestFor("sal_a"))).toEqual({
      kind: "accepted",
      externalRef: "1000",
    });
    expect(await local.provisioner.ensure(requestFor("sal_b"))).toEqual({
      kind: "accepted",
      externalRef: "1000",
      sidecarId: "sc_sal_a",
    });
    expect(await local.provisioner.ensure(requestFor("sal_b"))).toEqual({
      kind: "accepted",
      externalRef: "1000",
      sidecarId: "sc_sal_a",
    });
    expect(spawned).toHaveLength(1);
    expect(local.sidecars()).toEqual([
      { pid: 1000, hosts: ["sal_a", "sal_b"] },
    ]);

    await local.shutdown();
  });

  test("keeps a shared sidecar until its last allocation leaves", async () => {
    const { local, spawned, dataRoot } = await createSharingHarness();
    await local.provisioner.ensure(requestFor("sal_a"));
    await local.provisioner.ensure(requestFor("sal_b"));
    const [directory] = await fs.readdir(dataRoot);
    if (directory === undefined)
      throw new Error("Missing shared worker directory");
    const record = path.join(dataRoot, directory, "deployment.json");
    await fs.writeFile(record, "restorable deployment state");

    const firstRelease = {
      allocationId: "sal_a",
      generation: 0,
      sidecarId: "sc_sal_a",
    };
    expect(await local.provisioner.destroy(firstRelease)).toEqual({
      kind: "destroyed",
      cleanup: "required",
    });
    expect(await local.provisioner.destroy(firstRelease)).toEqual({
      kind: "destroyed",
      cleanup: "required",
    });
    expect(spawned[0]?.signals).toEqual([]);
    expect(await fs.readFile(record, "utf8")).toBe(
      "restorable deployment state",
    );
    expect(local.sidecars()).toEqual([{ pid: 1000, hosts: ["sal_b"] }]);

    expect(
      await local.provisioner.destroy({
        allocationId: "sal_b",
        generation: 0,
        sidecarId: "sc_sal_a",
      }),
    ).toEqual({ kind: "destroyed", cleanup: "confirmed" });
    expect(spawned[0]?.signals).toEqual(["SIGTERM"]);
    expect(await fs.readdir(dataRoot)).toEqual([]);
    expect(local.sidecars()).toEqual([]);
    expect(await local.provisioner.destroy(firstRelease)).toEqual({
      kind: "destroyed",
      cleanup: "confirmed",
    });
  });

  test("releases work named by the identity its ensure was offered", async () => {
    const { local } = await createSharingHarness();
    await local.provisioner.ensure(requestFor("sal_a"));
    await local.provisioner.ensure(requestFor("sal_b"));

    // A Hub that never recorded the placement names the offered identity.
    await local.provisioner.destroy({
      allocationId: "sal_b",
      generation: 0,
      sidecarId: "sc_sal_b",
    });

    expect(local.sidecars()).toEqual([{ pid: 1000, hosts: ["sal_a"] }]);
    await local.shutdown();
  });

  test("starts a new sidecar once the shared one has no work left", async () => {
    const { local, spawned } = await createSharingHarness();
    await local.provisioner.ensure(requestFor("sal_a"));
    await local.provisioner.destroy({
      allocationId: "sal_a",
      generation: 0,
      sidecarId: "sc_sal_a",
    });

    expect(await local.provisioner.ensure(requestFor("sal_b"))).toEqual({
      kind: "accepted",
      externalRef: "1001",
    });
    expect(spawned).toHaveLength(2);
    await local.shutdown();
  });

  test("keeps different tenants on different sidecars", async () => {
    const { local, spawned } = await createSharingHarness();

    await local.provisioner.ensure(requestFor("sal_a", "tnt_a"));
    await local.provisioner.ensure(requestFor("sal_b", "tnt_b"));

    expect(spawned).toHaveLength(2);
    await local.shutdown();
  });
});
