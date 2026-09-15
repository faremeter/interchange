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
  const local = createLocalProcessSidecarProvisioner({
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
    expect(await destroying).toEqual({ kind: "destroyed" });
    expect(spawned).toEqual([]);
    expect(await fs.readdir(dataRoot)).toEqual([]);
    await local.shutdown();
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
