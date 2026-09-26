import { describe, expect, test } from "bun:test";

import { chunkPack } from "@intx/pack-transport";
import type { RepoId } from "@intx/types/sidecar";
import { deriveWorkflowRunRepoId } from "@intx/workflow-deploy";

import {
  connectAllocated,
  createAllocatedRouter,
  createMockWs,
  TEST_CONFIG,
  TEST_IDENTITY,
  TEST_TARGET,
  tick,
} from "./sidecar-handler.test-helpers";
import {
  createSidecarRouter,
  isDeployFrameFailure,
  type SidecarAuthIdentity,
  SidecarIdentityValidationError,
} from "./sidecar-handler";

function lastFrame(ws: { sent: string[] }): Record<string, unknown> {
  const raw = ws.sent.at(-1);
  if (raw === undefined) throw new Error("Expected a frame");
  const parsed: unknown = JSON.parse(raw);
  if (typeof parsed !== "object" || parsed === null) {
    throw new Error("Expected an object frame");
  }
  const frame: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(parsed)) {
    frame[key] = value;
  }
  return frame;
}

describe("SidecarRouter allocation initialization cancellation", () => {
  test.each(["deploy", "restore"] as const)(
    "rechecks connection ownership and abort before admitted %s sends",
    async (operation) => {
      for (const change of ["abort", "disconnect", "fence"] as const) {
        const entered = Promise.withResolvers<undefined>();
        const release = Promise.withResolvers<undefined>();
        const controller = new AbortController();
        const router = createAllocatedRouter({
          withExecutableWorkflowRun: async (_target, send) => {
            entered.resolve(undefined);
            await release.promise;
            return send();
          },
        });
        const ws = await connectAllocated(router);
        const before = [...ws.sent];
        const sending = (
          operation === "deploy"
            ? router.sendAgentDeployToAllocation(
                TEST_TARGET,
                TEST_IDENTITY.workflowRunAddress,
                TEST_CONFIG,
                undefined,
                controller.signal,
              )
            : router.sendWorkflowRunPackToAllocation(
                TEST_TARGET,
                TEST_IDENTITY.workflowRunAddress,
                new Uint8Array([1]),
                "refs/heads/main",
                "a".repeat(40),
                controller.signal,
              )
        ).catch((cause: unknown) => cause);
        try {
          await Promise.race([entered.promise, sending]);
          if (change === "abort") controller.abort(new Error("Lease lost"));
          if (change === "disconnect") router.handleClose(ws);
          if (change === "fence")
            router.fenceAllocation(
              TEST_TARGET.allocationId,
              TEST_TARGET.generation + 1,
            );
          release.resolve(undefined);
          expect(await sending).toBeInstanceOf(Error);
          if (operation === "deploy")
            expect(await sending).toMatchObject({ frameSent: false });
          expect(ws.sent).toEqual(before);
          expect(router.getRoutableAddresses()).toEqual([]);
        } finally {
          release.resolve(undefined);
          router.handleClose(ws);
          await sending;
        }
      }
    },
  );

  test("a transaction failure after the deploy send preserves the possibly-live outcome", async () => {
    const failure = new Error("Admission commit response lost");
    const router = createAllocatedRouter({
      withExecutableWorkflowRun: async (_target, send) => {
        send();
        throw failure;
      },
    });
    const ws = await connectAllocated(router);
    try {
      const error = await router
        .sendAgentDeployToAllocation(
          TEST_TARGET,
          TEST_IDENTITY.workflowRunAddress,
          TEST_CONFIG,
        )
        .catch((cause: unknown) => cause);
      expect(error).toMatchObject({ frameSent: true, cause: failure });
      expect(lastFrame(ws)["type"]).toBe("agent.deploy");
    } finally {
      // Reject the orphaned reply wait too: it must already have an owner.
      router.handleClose(ws);
    }
  });

  test("owns an acknowledgement failure while admission is still finishing", async () => {
    const sent = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const router = createAllocatedRouter({
      withExecutableWorkflowRun: async (_target, send) => {
        const result = send();
        sent.resolve(undefined);
        await release.promise;
        return result;
      },
    });
    const ws = await connectAllocated(router);
    const sending = router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);
    try {
      await Promise.race([sent.promise, sending]);
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "agent.error",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          error: "Worker rejected deployment",
        }),
      );
      await tick();
      expect(router.getRoutableAddresses()).toEqual([]);
      release.resolve(undefined);
      expect(await sending).toMatchObject({
        frameSent: true,
        message: "Worker rejected deployment",
      });
    } finally {
      release.resolve(undefined);
      router.handleClose(ws);
      await sending;
    }
  });

  for (const change of ["cancel", "disconnect", "reject"] as const) {
    test(`does not send after ${change} while persisting the deployment attempt`, async () => {
      const controller = new AbortController();
      const router = createAllocatedRouter();
      const ws = await connectAllocated(router);
      const entered = Promise.withResolvers<boolean>();
      const release = Promise.withResolvers<boolean>();
      const before = [...ws.sent];
      const sent = router
        .sendAgentDeployToAllocation(
          TEST_TARGET,
          TEST_IDENTITY.workflowRunAddress,
          TEST_CONFIG,
          undefined,
          controller.signal,
          async () => {
            entered.resolve(true);
            await release.promise;
          },
        )
        .catch((error: unknown) => error);
      await entered.promise;
      expect(ws.sent).toEqual(before);
      if (change === "cancel") controller.abort(new Error("cancelled"));
      if (change === "disconnect") router.handleClose(ws);
      if (change === "reject")
        release.reject(new Error("commit response lost"));
      else release.resolve(true);
      expect(await sent).toMatchObject({ frameSent: false });
      expect(ws.sent).toEqual(before);
      expect(router.getRoutableAddresses()).toEqual([]);
    });
  }

  for (const operation of ["deploy", "restore"] as const) {
    test(`does not send ${operation} frames after cancellation during identity validation`, async () => {
      const controller = new AbortController();
      const cancelled = new Error("Initialization cancelled");
      let cancelDuringValidation = false;
      const router = createAllocatedRouter({
        validateSidecarIdentity: async () => {
          if (cancelDuringValidation) controller.abort(cancelled);
          return true;
        },
      });
      const ws = await connectAllocated(router);
      const previousFrames = [...ws.sent];
      cancelDuringValidation = true;

      const sending =
        operation === "deploy"
          ? router.sendAgentDeployToAllocation(
              TEST_TARGET,
              TEST_IDENTITY.workflowRunAddress,
              TEST_CONFIG,
              undefined,
              controller.signal,
            )
          : router.sendWorkflowRunPackToAllocation(
              TEST_TARGET,
              TEST_IDENTITY.workflowRunAddress,
              new Uint8Array([1, 2, 3]),
              "refs/heads/events",
              "a".repeat(40),
              controller.signal,
            );
      const error = await sending.catch((cause: unknown) => cause);

      if (operation === "deploy") {
        expect(error).toMatchObject({ frameSent: false, cause: cancelled });
      } else {
        expect(error).toBe(cancelled);
      }
      expect(ws.sent).toEqual(previousFrames);
      expect(router.getRoutableAddresses()).toEqual([]);
    });
  }
});

describe("SidecarRouter dispatch cancellation", () => {
  for (const kind of ["mail", "signal"] as const) {
    test(`does not send ${kind} after cancellation during identity validation`, async () => {
      const controller = new AbortController();
      const cancelled = new Error("Delivery lease expired");
      let cancelDuringValidation = false;
      const router = createAllocatedRouter({
        validateSidecarIdentity: async () => {
          if (cancelDuringValidation) controller.abort(cancelled);
          return true;
        },
      });
      const address = TEST_IDENTITY.workflowRunAddress;
      const ws = await connectAllocated(router, [address]);
      const before = [...ws.sent];
      cancelDuringValidation = true;
      try {
        const sending =
          kind === "mail"
            ? router.sendWorkflowRunDispatchToAllocation(
                TEST_TARGET,
                address,
                TEST_IDENTITY.anchorRunId,
                [],
                "bWFpbA==",
                "sender@tenant.example",
                "message-1",
                controller.signal,
              )
            : router.sendSignalDeliverToAllocation(
                TEST_TARGET,
                {
                  agentAddress: address,
                  runId: TEST_IDENTITY.anchorRunId,
                  signalName: "continue",
                  signalId: "signal-1",
                  payload: {},
                },
                controller.signal,
              );
        expect(await sending.catch((error: unknown) => error)).toBe(cancelled);
        expect(ws.sent).toEqual(before);
      } finally {
        router.handleClose(ws);
      }
    });
  }

  test("does not send grants or mail when a cancelled sender-key lookup returns", async () => {
    const controller = new AbortController();
    const cancelled = new Error("Delivery lease expired");
    const entered = Promise.withResolvers<boolean>();
    const key = Promise.withResolvers<string>();
    const router = createAllocatedRouter({
      lookups: {
        resolveSenderKey: () => {
          entered.resolve(true);
          return key.promise;
        },
      },
    });
    const address = TEST_IDENTITY.workflowRunAddress;
    const ws = await connectAllocated(router, [address]);
    const before = [...ws.sent];
    const sending = router
      .sendWorkflowRunDispatchToAllocation(
        TEST_TARGET,
        address,
        TEST_IDENTITY.anchorRunId,
        [],
        "bWFpbA==",
        "sender@tenant.example",
        "message-1",
        controller.signal,
      )
      .catch((error: unknown) => error);
    try {
      await entered.promise;
      controller.abort(cancelled);
      key.resolve("a".repeat(64));
      expect(await sending).toBe(cancelled);
      expect(ws.sent).toEqual(before);
    } finally {
      key.resolve("a".repeat(64));
      await sending;
      router.handleClose(ws);
    }
  });
});

describe("SidecarRouter allocation deploy transport", () => {
  test("rejects deploys without a Hub signing key before mutating routing", async () => {
    const router = createSidecarRouter({
      withExecutableWorkflowRun: async (_target, send) => send(),
      authenticateSidecar: async () => TEST_IDENTITY,
      validateSidecarIdentity: async () => true,
    });
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    await connectAllocated(router);

    const error = await router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);

    expect(isDeployFrameFailure(error)).toBe(true);
    if (!isDeployFrameFailure(error))
      throw new Error("Expected deploy failure");
    expect(error.frameSent).toBe(false);
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("times out an unacknowledged deploy and removes its route", async () => {
    const router = createAllocatedRouter({ requestTimeoutMs: 20 });
    await connectAllocated(router);

    const error = await router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);

    expect(isDeployFrameFailure(error)).toBe(true);
    if (!isDeployFrameFailure(error))
      throw new Error("Expected deploy failure");
    expect(error.frameSent).toBe(true);
    expect(error.message).toContain("timed out");
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("marks a synchronous socket failure as not sent", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    ws.send = () => {
      throw new Error("socket closed");
    };

    const error = await router
      .sendAgentDeployToAllocation(
        TEST_TARGET,
        TEST_IDENTITY.workflowRunAddress,
        TEST_CONFIG,
      )
      .catch((cause: unknown) => cause);

    expect(isDeployFrameFailure(error)).toBe(true);
    if (!isDeployFrameFailure(error))
      throw new Error("Expected deploy failure");
    expect(error.frameSent).toBe(false);
    expect(error.message).toContain("failed to send");
  });

  test("rejects and rolls back routing on agent.error", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.error",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        error: "worker failed",
      }),
    );

    await expect(deploy).rejects.toThrow("worker failed");
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("rejects a deploy when its acknowledgement subscriber fails", async () => {
    const router = createAllocatedRouter();
    router.events.on("agent.deploy.ack", () => {
      throw new Error("database write failed");
    });
    const ws = await connectAllocated(router);
    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );

    await expect(deploy).rejects.toThrow("Failed to store public key");
    expect(router.getRoutableAddresses()).toEqual([]);
  });

  test("disconnect rejects an in-flight deploy", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router);
    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    await tick();

    router.handleClose(ws);

    await expect(deploy).rejects.toThrow("disconnected");
  });

  test("ignores a deploy acknowledgement from another allocation", async () => {
    const secondary: Extract<SidecarAuthIdentity, { kind: "allocated" }> = {
      ...TEST_IDENTITY,
      sidecarId: "sc-secondary",
      allocationId: "alloc-2",
      anchorRunId: "run-secondary",
      workflowRunAddress: "run-secondary@tenant.example",
    };
    const router = createSidecarRouter({
      withExecutableWorkflowRun: async (_target, send) => send(),
      authenticateSidecar: async ({ sidecarId }) =>
        sidecarId === secondary.sidecarId ? secondary : TEST_IDENTITY,
      validateSidecarIdentity: async () => true,
      hubPublicKey: "a".repeat(64),
      requestTimeoutMs: 500,
    });
    router.fenceAllocation(TEST_TARGET.allocationId, TEST_TARGET.generation);
    router.fenceAllocation(secondary.allocationId, secondary.generation);

    const primaryWs = createMockWs();
    router.handleOpen(primaryWs);
    router.handleMessage(
      primaryWs,
      JSON.stringify({
        type: "register",
        sidecarId: TEST_IDENTITY.sidecarId,
        token: "primary",
        agentAddresses: [],
      }),
    );
    const secondaryWs = createMockWs();
    router.handleOpen(secondaryWs);
    router.handleMessage(
      secondaryWs,
      JSON.stringify({
        type: "register",
        sidecarId: secondary.sidecarId,
        token: "secondary",
        agentAddresses: [],
      }),
    );
    await tick();

    const deploy = router.sendAgentDeployToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      TEST_CONFIG,
    );
    let settled = false;
    void deploy.finally(() => {
      settled = true;
    });
    router.handleMessage(
      secondaryWs,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "c".repeat(64),
      }),
    );
    await tick();
    expect(settled).toBe(false);

    router.handleMessage(
      primaryWs,
      JSON.stringify({
        type: "agent.deploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        publicKey: "b".repeat(64),
      }),
    );
    await expect(deploy).resolves.toEqual({ publicKey: "b".repeat(64) });
  });

  test("undeploy waits for its acknowledgement before removing routing", async () => {
    const router = createAllocatedRouter();
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);

    const undeploy = router.sendAgentUndeploy(
      TEST_IDENTITY.workflowRunAddress,
      "session-ended",
    );
    expect(lastFrame(ws)).toMatchObject({
      type: "agent.undeploy",
      agentAddress: TEST_IDENTITY.workflowRunAddress,
      reason: "session-ended",
    });
    expect(router.getRoutableAddresses()).toContain(
      TEST_IDENTITY.workflowRunAddress,
    );

    router.handleMessage(
      ws,
      JSON.stringify({
        type: "agent.undeploy.ack",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        statePushed: true,
      }),
    );
    await undeploy;
    expect(router.getRoutableAddresses()).toEqual([]);
  });
});

describe("SidecarRouter allocation pack transport", () => {
  test("requires an outbound pack acknowledgement from its receiving socket", async () => {
    const router = createAllocatedRouter();
    const owner = await connectAllocated(router);
    const rogue = createMockWs();
    router.handleOpen(rogue);
    const sending = router.sendWorkflowRunPackToAllocation(
      TEST_TARGET,
      TEST_IDENTITY.workflowRunAddress,
      new Uint8Array([1, 2, 3]),
      "refs/heads/events",
      "a".repeat(40),
    );
    await tick();
    const done = lastFrame(owner);

    router.handleMessage(
      rogue,
      JSON.stringify({
        type: "repo.pack.ack",
        agentAddress: done["agentAddress"],
        repoId: done["repoId"],
        transferId: done["transferId"],
      }),
    );
    let settled = false;
    void sending.finally(() => {
      settled = true;
    });
    await tick();
    expect(settled).toBe(false);

    router.handleMessage(
      owner,
      JSON.stringify({
        type: "repo.pack.ack",
        agentAddress: done["agentAddress"],
        repoId: done["repoId"],
        transferId: done["transferId"],
      }),
    );
    await expect(sending).resolves.toBeUndefined();
  });

  test("accepts an authenticated workflow-run pack and acknowledges it", async () => {
    const received: unknown[] = [];
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack(repoId, pack, ref, commitSha, source) {
          received.push({ repoId, pack: [...pack], ref, commitSha, source });
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(TEST_IDENTITY.workflowRunAddress),
    };
    const pack = new Uint8Array([4, 5, 6]);

    for (const chunk of chunkPack(pack)) {
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          repoId,
          transferId: "transfer-1",
          seq: chunk.seq,
          data: chunk.data,
        }),
      );
    }
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        repoId,
        transferId: "transfer-1",
        ref: "refs/heads/events",
        commitSha: "d".repeat(40),
      }),
    );
    await tick();

    expect(received).toEqual([
      {
        repoId,
        pack: [4, 5, 6],
        ref: "refs/heads/events",
        commitSha: "d".repeat(40),
        source: {
          kind: "allocated",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          allocationId: TEST_TARGET.allocationId,
          anchorRunId: TEST_IDENTITY.anchorRunId,
          generation: TEST_TARGET.generation,
        },
      },
    ]);
    expect(lastFrame(ws)).toMatchObject({
      type: "repo.pack.ack",
      transferId: "transfer-1",
      repoId,
    });
  });

  test("forwards a workflow-run pack rejection to the sidecar", async () => {
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack() {
          return { accepted: false, reason: "path_violation" };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const repoId: RepoId = {
      kind: "workflow-run",
      id: deriveWorkflowRunRepoId(TEST_IDENTITY.workflowRunAddress),
    };
    const pack = new Uint8Array([7]);
    for (const chunk of chunkPack(pack)) {
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          repoId,
          transferId: "transfer-reject",
          seq: chunk.seq,
          data: chunk.data,
        }),
      );
    }
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        repoId,
        transferId: "transfer-reject",
        ref: "refs/heads/events",
        commitSha: "e".repeat(40),
      }),
    );
    await tick();

    expect(lastFrame(ws)).toMatchObject({
      type: "repo.pack.reject",
      transferId: "transfer-reject",
      reason: "path_violation",
    });
  });

  test("rejects a workflow-run pack outside the allocation repository", async () => {
    let received = false;
    const router = createAllocatedRouter({
      lookups: {
        async receiveWorkflowRunPack() {
          received = true;
          return { accepted: true };
        },
      },
    });
    const ws = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    const repoId: RepoId = { kind: "workflow-run", id: "another-workflow" };
    const pack = new Uint8Array([8]);
    for (const chunk of chunkPack(pack)) {
      router.handleMessage(
        ws,
        JSON.stringify({
          type: "repo.pack.push",
          agentAddress: TEST_IDENTITY.workflowRunAddress,
          repoId,
          transferId: "transfer-rogue",
          seq: chunk.seq,
          data: chunk.data,
        }),
      );
    }
    router.handleMessage(
      ws,
      JSON.stringify({
        type: "repo.pack.done",
        agentAddress: TEST_IDENTITY.workflowRunAddress,
        repoId,
        transferId: "transfer-rogue",
        ref: "refs/heads/events",
        commitSha: "f".repeat(40),
      }),
    );
    await tick();

    expect(received).toBe(false);
    expect(lastFrame(ws)).toMatchObject({
      type: "repo.pack.reject",
      transferId: "transfer-rogue",
      reason: "path_violation",
    });
  });
});

describe("SidecarRouter readiness validation failures", () => {
  test("does not mistake a failed identity lookup for an inactive supervisor", async () => {
    const validationError = new Error("statement timeout");
    let failValidation = false;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if (failValidation) throw validationError;
        return true;
      },
    });
    const socket = await connectAllocated(router, [
      TEST_IDENTITY.workflowRunAddress,
    ]);
    try {
      failValidation = true;
      const error = await router
        .isAllocatedWorkflowActive(TEST_TARGET)
        .catch((cause: unknown) => cause);
      expect(error).toBeInstanceOf(SidecarIdentityValidationError);
      expect(error).toMatchObject({ cause: validationError });
      expect(socket.closed).toBe(false);

      failValidation = false;
      expect(await router.isAllocatedWorkflowActive(TEST_TARGET)).toBe(true);
      router.handleClose(socket);
      expect(await router.isAllocatedWorkflowActive(TEST_TARGET)).toBe(false);
    } finally {
      router.handleClose(socket);
    }
  });

  test("reports an identity validation failure distinctly from an absent worker", async () => {
    const validationError = new Error("statement timeout");
    let failValidation = false;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if (failValidation) throw validationError;
        return true;
      },
    });
    await connectAllocated(router);
    failValidation = true;

    const error = await router
      .isAllocatedSidecarReady(TEST_TARGET)
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(SidecarIdentityValidationError);
    expect(error).toMatchObject({ cause: validationError });
    failValidation = false;
    await expect(router.isAllocatedSidecarReady(TEST_TARGET)).resolves.toBe(
      true,
    );
  });

  test("rejects a connection wait without remaining time on validation failure", async () => {
    let failValidation = false;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if (failValidation) throw new Error("statement timeout");
        return true;
      },
    });
    await connectAllocated(router);
    failValidation = true;

    const error = await router
      .waitForAllocatedSidecar(TEST_TARGET, 0)
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(SidecarIdentityValidationError);
  });

  test("waits out a validation failure and reports it instead of a worker timeout", async () => {
    let failValidation = false;
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if (failValidation) throw new Error("statement timeout");
        return true;
      },
    });
    await connectAllocated(router);
    failValidation = true;

    const error = await router
      .waitForAllocatedSidecar(TEST_TARGET, 20)
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(SidecarIdentityValidationError);
    expect(error).toMatchObject({
      message: expect.not.stringContaining("Timed out waiting"),
    });
  });

  test("still reports a worker timeout when the worker is absent", async () => {
    const router = createAllocatedRouter();

    const error = await router
      .waitForAllocatedSidecar(TEST_TARGET, 10)
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(SidecarIdentityValidationError);
    expect(error).toMatchObject({
      message: expect.stringContaining("Timed out waiting"),
    });
  });

  test("resolves a waiting connection once validation recovers", async () => {
    const outcomes: ("ok" | "fail")[] = ["ok", "fail", "ok", "ok"];
    const router = createAllocatedRouter({
      validateSidecarIdentity: async () => {
        if ((outcomes.shift() ?? "ok") === "fail") {
          throw new Error("statement timeout");
        }
        return true;
      },
    });
    const waiting = router.waitForAllocatedSidecar(TEST_TARGET, 500).then(
      () => "resolved",
      (cause: unknown) => cause,
    );
    await tick();
    await connectAllocated(router);
    await tick();
    await connectAllocated(router, [], "reconnect");

    await expect(waiting).resolves.toBe("resolved");
  });
});
