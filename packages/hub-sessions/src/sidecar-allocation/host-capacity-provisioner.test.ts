import { describe, expect, test } from "bun:test";

import type {
  ExecutionHostAssignment,
  ExecutionHostAssignmentStore,
  ExecutionHostSession,
} from "@intx/db";

import type { ExecutionHostControlRouter } from "../ws/execution-host-handler";
import type { EnsureSidecarRequest } from "./contracts";
import { createHostCapacityProvisioner } from "./host-capacity-provisioner";

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
    allocationId: "allocation-1",
    generation: 1,
    sidecarId: "sidecar-1",
    hostId: host.hostId,
    hostSessionId: host.sessionId,
    hostSessionGeneration: host.generation,
    status,
  };
}

function harness(hosts: ExecutionHostSession[]) {
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
  const provisioner = createHostCapacityProvisioner({
    id: "browser-host",
    bindingFingerprint: "browser-host:v1",
    capabilities: [{ capability: "runtime:browser", state: "available" }],
    router,
    assignments,
  });
  return {
    assignmentFrames,
    claims,
    getAssignment: () => current,
    provisioner,
    releaseFrames,
  };
}

describe("createHostCapacityProvisioner", () => {
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

    expect(await h.provisioner.ensure(request())).toEqual({
      kind: "accepted",
      externalRef: "host-1",
    });
    expect(h.claims).toEqual(["host-1"]);
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

    await h.provisioner.ensure(
      request({ targetHostPrincipalId: "principal-host-2" }),
    );

    expect(h.claims).toEqual(["host-2"]);
  });

  test("rejects retryably when no current host matches", async () => {
    const h = harness([
      session({
        capabilities: [
          { capability: "runtime:browser", state: "blocked" as const },
        ],
      }),
    ]);

    expect(await h.provisioner.ensure(request())).toMatchObject({
      kind: "rejected",
      code: "host_capacity_unavailable",
      retryable: true,
    });
  });

  test("keeps the host reserved until release acknowledgement", async () => {
    const h = harness([session()]);
    await h.provisioner.ensure(request());

    expect(
      await h.provisioner.destroy({
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
});
