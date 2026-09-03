import { type } from "arktype";

import type {
  ExecutionHostAssignment,
  ExecutionHostAssignmentStore,
  ExecutionHostClaimCandidate,
  ExecutionHostSession,
} from "@intx/db";

import type { ExecutionHostControlRouter } from "../ws/execution-host-handler";
import { matchSidecarCapabilityPolicy } from "./capability-policy";
import type {
  ClaimExistingSidecarOpts,
  DestroySidecarRequest,
  DestroySidecarResult,
  EnsureSidecarRequest,
  EnsureSidecarResult,
  ExistingSidecarCapacity,
} from "./contracts";

export type ExistingSidecarHostAccessRequest = {
  readonly tenantId: string;
  readonly placementPrincipalId: string;
  readonly hostId: string;
  readonly hostPrincipalId: string;
  readonly ownerPrincipalId: string;
};

export type CreateExistingSidecarCapacityOpts = {
  readonly router: ExecutionHostControlRouter;
  readonly assignments: ExecutionHostAssignmentStore;
  readonly canUseHost?: (
    request: ExistingSidecarHostAccessRequest,
  ) => Promise<boolean>;
  readonly acknowledgementTimeoutMs?: number;
};

const DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_MS = 30_000;
const SelectedHostId = type("string | null");

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

// Deny cross-principal host use unless a policy callback allows it; owner use needs no callback.
export function createExistingSidecarCapacity({
  router,
  assignments,
  canUseHost = async () => false,
  acknowledgementTimeoutMs = DEFAULT_ACKNOWLEDGEMENT_TIMEOUT_MS,
}: CreateExistingSidecarCapacityOpts): ExistingSidecarCapacity {
  if (acknowledgementTimeoutMs <= 0) {
    throw new Error("Host assignment acknowledgement timeout must be positive");
  }

  async function canUse(
    request: EnsureSidecarRequest,
    session: ExecutionHostClaimCandidate,
  ): Promise<boolean> {
    return (
      session.ownerPrincipalId === request.placementPrincipalId ||
      (await canUseHost({
        tenantId: request.tenantId,
        placementPrincipalId: request.placementPrincipalId,
        hostId: session.hostId,
        hostPrincipalId: session.principalId,
        ownerPrincipalId: session.ownerPrincipalId,
      }))
    );
  }

  async function eligibleCandidates(
    request: EnsureSidecarRequest,
    hosts: readonly ExecutionHostClaimCandidate[],
  ): Promise<ExecutionHostClaimCandidate[]> {
    const connected = hosts.filter((host) => {
      const current = router.getConnectedSession(host.hostId);
      return (
        current?.sessionId === host.sessionId &&
        current.generation === host.sessionGeneration &&
        current.hubInstanceId === host.hubInstanceId
      );
    });
    const available = await assignments.listAvailableCandidates(connected);
    const permitted = await Promise.all(
      available
        .filter(
          (session) =>
            session.tenantId === request.tenantId &&
            (request.targetHostPrincipalId === undefined ||
              session.principalId === request.targetHostPrincipalId) &&
            matchSidecarCapabilityPolicy(
              request.placementPolicy,
              session.capabilities,
            ).ok,
        )
        .map(async (host) => ((await canUse(request, host)) ? host : null)),
    );
    return permitted.filter((host) => host !== null);
  }

  async function findAndClaim(
    request: EnsureSidecarRequest,
    { chooseHost }: ClaimExistingSidecarOpts,
  ): Promise<ExecutionHostAssignment | null> {
    let candidates = await eligibleCandidates(
      request,
      router.listConnectedSessions().map(candidate),
    );
    while (candidates.length > 0) {
      const hostId = SelectedHostId.assert(
        await chooseHost(
          candidates.map((host) => ({
            hostId: host.hostId,
            hostPrincipalId: host.principalId,
            capabilities: host.capabilities.map((capability) => ({
              ...capability,
            })),
          })),
        ),
      );
      if (hostId === null) return null;
      const selected = candidates.find((host) => host.hostId === hostId);
      if (selected === undefined) {
        throw new Error(
          "Existing host chooser returned a host outside the offered candidates",
        );
      }

      // A chooser can await external policy. Revalidate before reserving, and
      // never retry an already-attempted host within this claim call.
      const remaining = candidates.filter((host) => host.hostId !== hostId);
      const [current] = await eligibleCandidates(request, [selected]);
      if (current === undefined) {
        candidates = await eligibleCandidates(request, remaining);
        continue;
      }
      const assignment = await assignments.claim({
        operationId: request.allocationId,
        generation: request.generation,
        sidecarId: request.sidecarId,
        tenantId: request.tenantId,
        placementPrincipalId: request.placementPrincipalId,
        ...(request.targetHostPrincipalId !== undefined
          ? { targetHostPrincipalId: request.targetHostPrincipalId }
          : {}),
        candidate: current,
      });
      if (assignment !== null) return assignment;
      candidates = await eligibleCandidates(request, remaining);
    }
    return null;
  }

  // Stale ownership is terminal, but a lost session is transient: throw so reconciliation retries.
  async function claim(
    request: EnsureSidecarRequest,
    opts: ClaimExistingSidecarOpts,
  ): Promise<EnsureSidecarResult | null> {
    const assignment =
      (await assignments.findBySidecarId(request.sidecarId)) ??
      (await findAndClaim(request, opts)) ??
      (await assignments.findBySidecarId(request.sidecarId));
    if (assignment === null) {
      return request.targetHostPrincipalId === undefined
        ? null
        : {
            kind: "rejected",
            code: "target_host_unavailable",
            message: "The requested execution host could not be claimed",
            retryable: true,
          };
    }
    if (
      assignment.operationId !== request.allocationId ||
      assignment.generation !== request.generation
    ) {
      return staleOperation("Sidecar identity belongs to another allocation");
    }
    if (
      assignment.status === "destroyed" ||
      assignment.status === "releasing"
    ) {
      return staleOperation("Sidecar identity has already been released");
    }
    if (assignment.status === "assigned") {
      return { kind: "accepted", externalRef: assignment.hostId };
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
      operationId: assignment.operationId,
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

  async function release(
    request: DestroySidecarRequest,
  ): Promise<DestroySidecarResult | null> {
    const existing = await assignments.findBySidecarId(request.sidecarId);
    if (existing === null) return null;
    if (existing.operationId !== request.allocationId) {
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
      operationId: request.allocationId,
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
      operationId: releasing.operationId,
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

  return { claim, release };
}
