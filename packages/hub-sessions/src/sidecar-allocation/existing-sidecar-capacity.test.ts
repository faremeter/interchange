import { describe, expect, test } from "bun:test";

import type {
  ExecutionHostAssignment,
  ExecutionHostAssignmentStore,
  ExecutionHostSession,
} from "@intx/db";

import type { ExecutionHostControlRouter } from "../ws/execution-host-handler";
import type { EnsureSidecarRequest, ExistingHostCandidate } from "./contracts";
import { createExistingSidecarCapacity } from "./existing-sidecar-capacity";

const chooseFirstHost = {
  chooseHost: (hosts: readonly ExistingHostCandidate[]) =>
    hosts[0]?.hostId ?? null,
};

function session(overrides: Partial<ExecutionHostSession> = {}) {
  return {
    hostId: "host-1",
    principalId: "principal-host-1",
    ownerPrincipalId: "principal-owner-1",
    tenantId: "tenant-1",
    sessionId: "session-1",
    generation: 1,
    hubInstanceId: "hub-1",
    capabilities: [
      { capability: "runtime:browser", state: "available" as const },
    ],
    leaseExpiresAt: new Date(Date.now() + 60_000),
    ...overrides,
  };
}

function request(
  overrides: Partial<EnsureSidecarRequest> = {},
): EnsureSidecarRequest {
  return {
    allocationId: "allocation-1",
    generation: 1,
    tenantId: "tenant-1",
    placementPrincipalId: "principal-owner-1",
    placementPolicy: {
      tenantPolicies: [],
      workflowRules: [{ capability: "runtime:browser", effect: "require" }],
    },
    anchorRunId: "run-1",
    sidecarId: "sidecar-1",
    token: "sidecar-token",
    hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
    ...overrides,
  };
}

function assignment(
  host: ExecutionHostSession,
  status: ExecutionHostAssignment["status"] = "claiming",
): ExecutionHostAssignment {
  return {
    operationId: "allocation-1",
    generation: 1,
    sidecarId: "sidecar-1",
    hostId: host.hostId,
    hostSessionId: host.sessionId,
    hostSessionGeneration: host.generation,
    capabilities: host.capabilities,
    status,
  };
}

function harness(
  hosts: ExecutionHostSession[],
  canUseHost: (hostId: string) => boolean = () => false,
) {
  const assignmentFrames: unknown[] = [];
  const releaseFrames: unknown[] = [];
  const claims: string[] = [];
  let current: ExecutionHostAssignment | null = null;
  const assignments: ExecutionHostAssignmentStore = {
    async beginRelease(args) {
      if (current === null) return null;
      current = {
        ...current,
        hostSessionId: args.candidate.sessionId,
        hostSessionGeneration: args.candidate.sessionGeneration,
        status: "releasing",
        destroyedGeneration: args.generation,
      };
      return current;
    },
    async claim(args) {
      claims.push(args.candidate.hostId);
      if (current !== null) return current;
      current = assignment(
        hosts.find((host) => host.hostId === args.candidate.hostId) ??
          session({ hostId: args.candidate.hostId }),
      );
      return current;
    },
    async findBySidecarId() {
      return current;
    },
    async listAvailableCandidates(candidates) {
      return candidates.filter(
        (host) =>
          current === null ||
          current.status === "destroyed" ||
          current.hostId !== host.hostId,
      );
    },
    async matchesTargetHost() {
      throw new Error("Unexpected target verification in claim service");
    },
    async markAssigned() {
      if (current === null) return null;
      current = { ...current, status: "assigned" };
      return current;
    },
    async markDestroyed() {
      if (current === null) return null;
      current = { ...current, status: "destroyed" };
      return current;
    },
  };
  const router: ExecutionHostControlRouter = {
    getConnectedSession(hostId) {
      return hosts.find((host) => host.hostId === hostId) ?? null;
    },
    handleClose() {
      return undefined;
    },
    handleMessage() {
      return undefined;
    },
    handleOpen() {
      return undefined;
    },
    listConnectedSessions() {
      return hosts;
    },
    async sendAssignment(_session, frame) {
      assignmentFrames.push(frame);
    },
    async sendRelease(_session, frame) {
      releaseFrames.push(frame);
    },
  };
  const capacity = createExistingSidecarCapacity({
    router,
    assignments,
    canUseHost: async ({ hostId }) => canUseHost(hostId),
  });
  return {
    assignments,
    assignmentFrames,
    claims,
    getAssignment: () => current,
    capacity,
    releaseFrames,
    router,
  };
}

describe("createExistingSidecarCapacity", () => {
  test("claims matching principal-owned capacity and delivers the assignment", async () => {
    const wrongOwner = session({
      hostId: "host-wrong-owner",
      principalId: "principal-wrong-owner",
      ownerPrincipalId: "principal-owner-2",
    });
    const blockedRuntime = session({
      hostId: "host-blocked-runtime",
      principalId: "principal-blocked-runtime",
      capabilities: [
        { capability: "runtime:browser", state: "blocked" as const },
      ],
    });
    const matching = session();
    const h = harness([wrongOwner, blockedRuntime, matching]);

    const offered: (readonly ExistingHostCandidate[])[] = [];
    expect(
      await h.capacity.claim(request(), {
        chooseHost(candidates) {
          offered.push(candidates);
          return candidates[0]?.hostId ?? null;
        },
      }),
    ).toEqual({
      kind: "accepted",
      externalRef: "host-1",
    });
    expect(h.claims).toEqual(["host-1"]);
    expect(offered).toEqual([
      [
        {
          hostId: matching.hostId,
          hostPrincipalId: matching.principalId,
          capabilities: matching.capabilities,
        },
      ],
    ]);
    expect(h.assignmentFrames).toEqual([
      {
        allocationId: "allocation-1",
        generation: 1,
        tenantId: "tenant-1",
        anchorRunId: "run-1",
        sidecarId: "sidecar-1",
        sidecarToken: "sidecar-token",
        hubWebSocketUrl: "wss://hub.example/api/sidecars/ws",
      },
    ]);
    expect(h.getAssignment()?.status).toBe("assigned");
  });

  test("honors an exact host principal target", async () => {
    const first = session();
    const targeted = session({
      hostId: "host-2",
      principalId: "principal-host-2",
      sessionId: "session-2",
    });
    const h = harness([first, targeted]);

    await h.capacity.claim(
      request({ targetHostPrincipalId: "principal-host-2" }),
      chooseFirstHost,
    );

    expect(h.claims).toEqual(["host-2"]);
  });

  test("returns null when no current host matches", async () => {
    const h = harness([
      session({
        capabilities: [
          { capability: "runtime:browser", state: "blocked" as const },
        ],
      }),
    ]);

    expect(
      await h.capacity.claim(request(), {
        chooseHost() {
          throw new Error("No eligible hosts should be offered");
        },
      }),
    ).toBeNull();
  });

  test("claims capacity shared with the placement principal", async () => {
    const shared = session({
      ownerPrincipalId: "principal-owner-2",
    });
    const h = harness([shared], (hostId) => hostId === shared.hostId);

    expect(await h.capacity.claim(request(), chooseFirstHost)).toMatchObject({
      kind: "accepted",
      externalRef: shared.hostId,
    });
  });

  test("keeps the host reserved until release acknowledgement", async () => {
    const h = harness([session()]);
    await h.capacity.claim(request(), chooseFirstHost);

    expect(
      await h.capacity.release({
        allocationId: "allocation-1",
        generation: 2,
        sidecarId: "sidecar-1",
      }),
    ).toEqual({ kind: "destroyed" });
    expect(h.releaseFrames).toEqual([
      { allocationId: "allocation-1", generation: 2, sidecarId: "sidecar-1" },
    ]);
    expect(h.getAssignment()).toMatchObject({
      status: "destroyed",
      destroyedGeneration: 2,
    });
  });

  test("uses the provisioner's choice among hosts with identical capabilities", async () => {
    const first = session();
    const second = session({
      hostId: "host-2",
      principalId: "principal-host-2",
    });
    const h = harness([first, second]);

    expect(
      await h.capacity.claim(request(), {
        chooseHost: async (candidates) =>
          candidates.find((host) => host.hostId === second.hostId)?.hostId ??
          null,
      }),
    ).toEqual({ kind: "accepted", externalRef: second.hostId });
    expect(h.claims).toEqual([second.hostId]);
  });

  test("lets the chooser decline existing capacity without reserving a host", async () => {
    const h = harness([session()]);
    expect(
      await h.capacity.claim(request(), { chooseHost: () => null }),
    ).toBeNull();
    expect(h.claims).toEqual([]);
    expect(h.assignmentFrames).toEqual([]);
  });

  test("rejects a choice outside the offered candidates", async () => {
    const h = harness([session()]);
    await expect(
      h.capacity.claim(request(), { chooseHost: () => "unoffered-host" }),
    ).rejects.toThrow(/outside the offered candidates/);
    expect(h.claims).toEqual([]);
  });

  test("offers remaining hosts after losing a reservation race", async () => {
    const h = harness([session(), session({ hostId: "host-2" })]);
    const claim = h.assignments.claim;
    h.assignments.claim = async (args) =>
      args.candidate.hostId === "host-1" ? null : claim(args);
    const offers: string[][] = [];

    expect(
      await h.capacity.claim(request(), {
        chooseHost(candidates) {
          offers.push(candidates.map((host) => host.hostId));
          return candidates[0]?.hostId ?? null;
        },
      }),
    ).toEqual({ kind: "accepted", externalRef: "host-2" });
    expect(offers).toEqual([["host-1", "host-2"], ["host-2"]]);
  });

  test("bounds reservation attempts when every offered host is taken", async () => {
    const h = harness([session(), session({ hostId: "host-2" })]);
    const attempted: string[] = [];
    h.assignments.claim = async (args) => {
      attempted.push(args.candidate.hostId);
      return null;
    };

    expect(await h.capacity.claim(request(), chooseFirstHost)).toBeNull();
    expect(attempted).toEqual(["host-1", "host-2"]);
  });

  test("rechecks access after an asynchronous chooser returns", async () => {
    let allowed = true;
    const h = harness(
      [session({ ownerPrincipalId: "other-owner" })],
      () => allowed,
    );

    expect(
      await h.capacity.claim(request(), {
        async chooseHost(candidates) {
          allowed = false;
          return candidates[0]?.hostId ?? null;
        },
      }),
    ).toBeNull();
    expect(h.claims).toEqual([]);
  });

  test("does not reserve a host whose connection changed during selection", async () => {
    const hosts = [session()];
    const h = harness(hosts);

    expect(
      await h.capacity.claim(request(), {
        chooseHost(candidates) {
          hosts[0] = session({
            sessionId: "replacement-session",
            generation: 2,
          });
          return candidates[0]?.hostId ?? null;
        },
      }),
    ).toBeNull();
    expect(h.claims).toEqual([]);
  });

  test("rejects an unavailable exact target instead of allowing fallback", async () => {
    const h = harness([session()]);
    expect(
      await h.capacity.claim(
        request({ targetHostPrincipalId: "other-host-principal" }),
        chooseFirstHost,
      ),
    ).toMatchObject({
      kind: "rejected",
      code: "target_host_unavailable",
      retryable: true,
    });
    expect(h.claims).toEqual([]);
  });

  test("rejects a declined exact target instead of allowing fallback", async () => {
    const h = harness([session()]);
    expect(
      await h.capacity.claim(
        request({ targetHostPrincipalId: "principal-host-1" }),
        { chooseHost: () => null },
      ),
    ).toMatchObject({
      kind: "rejected",
      code: "target_host_unavailable",
      retryable: true,
    });
  });

  test("preserves an assigned host without invoking the chooser again", async () => {
    const h = harness([session()]);
    await h.capacity.claim(request(), chooseFirstHost);
    expect(
      await h.capacity.claim(request(), {
        chooseHost() {
          throw new Error("An existing assignment must be reused");
        },
      }),
    ).toEqual({ kind: "accepted", externalRef: "host-1" });
    expect(h.assignmentFrames).toHaveLength(1);
  });

  test("resumes an uncertain assignment without selecting another host", async () => {
    const h = harness([session(), session({ hostId: "host-2" })]);
    const sendAssignment = h.router.sendAssignment;
    h.router.sendAssignment = async () => {
      throw new Error("Lost acknowledgement");
    };

    await expect(h.capacity.claim(request(), chooseFirstHost)).rejects.toThrow(
      /Lost acknowledgement/,
    );
    expect(h.getAssignment()?.status).toBe("claiming");
    h.router.sendAssignment = sendAssignment;
    expect(
      await h.capacity.claim(request(), {
        chooseHost() {
          throw new Error("An uncertain assignment must be resumed");
        },
      }),
    ).toEqual({ kind: "accepted", externalRef: "host-1" });
    expect(h.claims).toEqual(["host-1"]);
  });
});
