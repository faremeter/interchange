import type {
  ExecutionHostAssignment,
  ExecutionHostAssignmentStore,
  ExecutionHostClaimCandidate,
  ExecutionHostSession,
} from "@intx/db";

import type { ExecutionHostControlRouter } from "../ws/execution-host-handler";
import type {
  DestroySidecarRequest,
  DestroySidecarResult,
  EnsureSidecarRequest,
  EnsureSidecarResult,
  SidecarProvisioner,
} from "./contracts";
import { matchSidecarCapabilityPolicy } from "./capability-policy";

export type CreateHostCapacityProvisionerOpts = {
  readonly id: string;
  readonly bindingFingerprint: string;
  readonly capabilities: SidecarProvisioner["capabilities"];
  readonly router: ExecutionHostControlRouter;
  readonly assignments: ExecutionHostAssignmentStore;
  readonly acknowledgementTimeoutMs?: number;
};

const DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_MS = 30_000;

function candidate(session: ExecutionHostSession): ExecutionHostClaimCandidate {
  return {
    hostId: session.hostId,
    principalId: session.principalId,
    ownerPrincipalId: session.ownerPrincipalId,
    tenantId: session.tenantId,
    sessionId: session.sessionId,
    sessionGeneration: session.generation,
    hubInstanceId: session.hubInstanceId,
    capabilities: session.capabilities,
  };
}

function staleOperation(message: string) {
  return {
    kind: "rejected" as const,
    code: "stale_host_assignment",
    message,
    retryable: false,
  };
}

export function createHostCapacityProvisioner({
  id,
  bindingFingerprint,
  capabilities,
  router,
  assignments,
  acknowledgementTimeoutMs = DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_MS,
}: CreateHostCapacityProvisionerOpts): SidecarProvisioner {
  if (acknowledgementTimeoutMs <= 0) {
    throw new Error("Host assignment acknowledgement timeout must be positive");
  }

  async function claim(
    request: EnsureSidecarRequest,
  ): Promise<ExecutionHostAssignment | null> {
    const sessions = router
      .listConnectedSessions()
      .filter(
        (session) =>
          session.tenantId === request.tenantId &&
          session.ownerPrincipalId === request.placementPrincipalId &&
          (request.targetHostPrincipalId === undefined ||
            session.principalId === request.targetHostPrincipalId) &&
          matchSidecarCapabilityPolicy(
            request.placementPolicy,
            session.capabilities,
          ).ok,
      )
      // Claim in host-id order so concurrent reconcilers contend for the same host first.
      .sort((left, right) => left.hostId.localeCompare(right.hostId));

    for (const session of sessions) {
      const assignment = await assignments.claim({
        allocationId: request.allocationId,
        generation: request.generation,
        sidecarId: request.sidecarId,
        tenantId: request.tenantId,
        placementPrincipalId: request.placementPrincipalId,
        ...(request.targetHostPrincipalId !== undefined
          ? { targetHostPrincipalId: request.targetHostPrincipalId }
          : {}),
        candidate: candidate(session),
      });
      if (assignment !== null) return assignment;
    }
    return null;
  }

  // Stale ownership is terminal, but a lost session is transient: throw so reconciliation retries.
  async function ensure(
    request: EnsureSidecarRequest,
  ): Promise<EnsureSidecarResult> {
    const previous = await assignments.findBySidecarId(request.sidecarId);
    if (previous?.status === "assigned") {
      return previous.allocationId === request.allocationId &&
        previous.generation === request.generation
        ? { kind: "accepted", externalRef: previous.hostId }
        : staleOperation("Sidecar identity belongs to another host assignment");
    }
    if (previous?.status === "destroyed" || previous?.status === "releasing") {
      return staleOperation("Sidecar identity has already been released");
    }

    const assignment = previous ?? (await claim(request));
    if (assignment === null) {
      return {
        kind: "rejected",
        code: "host_capacity_unavailable",
        message: "No matching execution host is currently available",
        retryable: true,
      };
    }
    if (
      assignment.allocationId !== request.allocationId ||
      assignment.generation !== request.generation
    ) {
      return staleOperation("Sidecar identity belongs to another allocation");
    }

    const session = router.getConnectedSession(assignment.hostId);
    if (
      session === null ||
      session.sessionId !== assignment.hostSessionId ||
      session.generation !== assignment.hostSessionGeneration
    ) {
      throw new Error(
        `Execution host assignment ${request.sidecarId} lost its claiming session`,
      );
    }
    await router.sendAssignment(
      session,
      {
        allocationId: request.allocationId,
        generation: request.generation,
        tenantId: request.tenantId,
        anchorRunId: request.anchorRunId,
        sidecarId: request.sidecarId,
        sidecarToken: request.token,
        hubWebSocketUrl: request.hubWebSocketUrl,
      },
      acknowledgementTimeoutMs,
    );
    const assigned = await assignments.markAssigned({
      allocationId: assignment.allocationId,
      generation: assignment.generation,
      sidecarId: assignment.sidecarId,
      hostId: assignment.hostId,
      hostSessionId: assignment.hostSessionId,
      hostSessionGeneration: assignment.hostSessionGeneration,
    });
    if (assigned === null) {
      throw new Error(
        `Execution host assignment ${request.sidecarId} changed before acknowledgement`,
      );
    }
    return { kind: "accepted", externalRef: assigned.hostId };
  }

  async function destroy(
    request: DestroySidecarRequest,
  ): Promise<DestroySidecarResult> {
    const existing = await assignments.findBySidecarId(request.sidecarId);
    if (existing === null) return { kind: "destroyed" };
    if (existing.allocationId !== request.allocationId) {
      return staleOperation("Sidecar identity belongs to another allocation");
    }
    if (existing.status === "destroyed") return { kind: "destroyed" };
    if (
      request.generation < existing.generation ||
      (existing.destroyedGeneration !== undefined &&
        request.generation < existing.destroyedGeneration)
    ) {
      return staleOperation("Destroy generation is stale");
    }

    const session = router.getConnectedSession(existing.hostId);
    if (session === null) {
      throw new Error(
        `Execution host ${existing.hostId} is unavailable for release`,
      );
    }
    const releasing = await assignments.beginRelease({
      allocationId: request.allocationId,
      generation: request.generation,
      sidecarId: request.sidecarId,
      candidate: candidate(session),
    });
    if (releasing === null) {
      throw new Error(
        `Execution host assignment ${request.sidecarId} changed before release`,
      );
    }
    if (releasing.status === "destroyed") return { kind: "destroyed" };

    await router.sendRelease(
      session,
      {
        allocationId: request.allocationId,
        generation: request.generation,
        sidecarId: request.sidecarId,
      },
      acknowledgementTimeoutMs,
    );
    const destroyed = await assignments.markDestroyed({
      allocationId: releasing.allocationId,
      generation: releasing.generation,
      destroyedGeneration: request.generation,
      sidecarId: releasing.sidecarId,
      hostId: releasing.hostId,
      hostSessionId: releasing.hostSessionId,
      hostSessionGeneration: releasing.hostSessionGeneration,
    });
    if (destroyed === null) {
      throw new Error(
        `Execution host assignment ${request.sidecarId} changed before release acknowledgement`,
      );
    }
    return { kind: "destroyed" };
  }

  return {
    id,
    apiVersion: 1,
    bindingFingerprint,
    capabilities,
    ensure,
    destroy,
  };
}
