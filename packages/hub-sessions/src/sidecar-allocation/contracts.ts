import { type } from "arktype";

import type { SidecarCapabilityDeclaration } from "@intx/types";

export type EnsureSidecarRequest = {
  /** Cancellation is best effort; a cancelled ensure has an uncertain outcome. */
  readonly signal?: AbortSignal;
  readonly allocationId: string;
  readonly generation: number;
  readonly tenantId: string;
  readonly anchorRunId: string;
  /**
   * A new sidecar identity minted for this generation. Capacity the
   * provisioner starts authenticates with it; a provisioner that places the
   * work on a sidecar it already runs returns that sidecar's id instead.
   */
  readonly sidecarId: string;
  readonly token: string;
  readonly hubWebSocketUrl: string;
};

export type DestroySidecarRequest = {
  /** Cancellation never confirms destruction; the Hub may retry cleanup. */
  readonly signal?: AbortSignal;
  readonly allocationId: string;
  readonly generation: number;
  /**
   * The sidecar the Hub last recorded for this generation. When the Hub never
   * learned an ensure's outcome, this is the identity that ensure was offered
   * even if the provisioner placed the work on an existing sidecar, so a
   * provisioner that reuses sidecars must be able to release by allocationId.
   * A destroy releases the allocation's hold at `generation` or any older one:
   * release and replacement advance the generation before destroying. The
   * Hub never re-announces holds, so such a provisioner stores them durably;
   * held only in memory, they are lost on its restart, leaking sidecars or
   * stopping one that other work still uses.
   */
  readonly sidecarId: string;
  /**
   * Optional provider handle recorded after ensure returns. A Hub crash can
   * occur after capacity is created but before this value is persisted, so
   * destroy must always be able to identify the capacity from allocationId,
   * generation, and sidecarId alone.
   */
  readonly externalRef?: string;
};

// Provisioner results cross a plugin boundary, so they are validated at
// runtime rather than trusted from the declared return type.
export const SidecarOperationFailure = type({
  kind: "'rejected'",
  code: "string",
  message: "string",
  retryable: "boolean",
});
export type SidecarOperationFailure = typeof SidecarOperationFailure.infer;

/**
 * Acceptance means the requested infrastructure exists, not that it is ready.
 * `sidecarId` names a sidecar this provisioner started for earlier work that
 * now also hosts this generation; omit it when the request's own identity is
 * used. The Hub accepts only a sidecar that still hosts another probe or
 * allocation of the same provisioner binding, including one whose release or
 * replacement has not yet destroyed it.
 * Rejection means no infrastructure exists for this generation (ensure-only;
 * destroy rejections below carry no such guarantee); a provisioner must throw
 * when it cannot determine whether the request took effect.
 */
export const EnsureSidecarResult = type({
  kind: "'accepted'",
  "externalRef?": "string",
  "sidecarId?": "string",
}).or(SidecarOperationFailure);
export type EnsureSidecarResult = typeof EnsureSidecarResult.infer;

/**
 * Destruction confirms the allocation no longer holds its capacity and older
 * ensure calls are fenced. Whether a sidecar that hosts other work stays up is
 * the provisioner's decision; the Hub has already stopped routing the
 * allocation and undeploys it from a sidecar that is, or next becomes,
 * connected. A non-retryable rejection stops automatic cleanup; capacity may
 * still exist.
 */
export const DestroySidecarResult = type({
  kind: "'destroyed'",
}).or(SidecarOperationFailure);
export type DestroySidecarResult = typeof DestroySidecarResult.infer;

export interface SidecarProvisioner {
  readonly id: string;
  readonly apiVersion: 1;
  /** Stable, non-secret identity for the backend configuration. */
  readonly bindingFingerprint: string;
  readonly capabilities: readonly SidecarCapabilityDeclaration[];
  /**
   * Converges infrastructure for this generation. Implementations must be
   * idempotent and reject generations older than one they have observed.
   * Honour the request signal where possible. A late completion must still
   * respect a concurrent destroy's fence, even if cancellation was ignored.
   */
  ensure(request: EnsureSidecarRequest): Promise<EnsureSidecarResult>;
  /**
   * Idempotently destroys the allocation and fences older ensure calls so a
   * delayed request cannot recreate infrastructure after destruction. It must
   * succeed without an externalRef; that value is only an optional optimization.
   */
  destroy(request: DestroySidecarRequest): Promise<DestroySidecarResult>;
}

/** One probe or allocation generation that a sidecar currently hosts. */
export type SidecarCredentialIdentity =
  | {
      readonly kind: "allocated";
      readonly sidecarId: string;
      readonly allocationId: string;
      readonly tenantId: string;
      readonly anchorRunId: string;
      readonly workflowRunAddress: string;
      readonly generation: number;
    }
  | {
      readonly kind: "probe";
      readonly sidecarId: string;
      readonly allocationId: string;
      readonly tenantId: string;
      readonly generation: number;
    };

/** A verified sidecar and every probe and allocation generation it hosts. */
export type SidecarCredentials = {
  readonly sidecarId: string;
  readonly bindings: readonly SidecarCredentialIdentity[];
};

/**
 * `reclaim` also requires the deployment's first deploy to have completed: its
 * anchor's key is committed and no initialization is in flight. A copy whose
 * deploy is still uncertain is undeployed instead, since releasing it on a
 * sidecar that hosts other work does not stop the sidecar. It further requires
 * the anchor run not to be terminal: a copy of a run that has ended is not
 * routed, even when its own history never recorded the end.
 *
 * `retention` holds for such a copy instead when the run ended through its own
 * history: it requires what `reclaim` does, except that the anchor run has
 * ended without the Hub cancelling it. The copy then stays unrouted, its local
 * state kept until the Hub releases the deployment, so a reconnect does not
 * cut short the retention the deployment's policy sets. A copy of a run the
 * Hub cancelled is undeployed, since a restart that finds its run record still
 * on disk may be running that run again.
 */
export type SidecarIdentityUse =
  | "registration"
  | "readiness"
  | "routing"
  | "reclaim"
  | "retention";

export interface SidecarCredentialResolver {
  /** Resolves a bearer token, or null when it hosts nothing current. */
  resolve(token: string): Promise<SidecarCredentials | null>;
  /** The current bindings of a sidecar, empty when it hosts nothing. */
  resolveBindings(
    sidecarId: string,
  ): Promise<readonly SidecarCredentialIdentity[]>;
  isCurrent(
    identity: SidecarCredentialIdentity,
    use: SidecarIdentityUse,
  ): Promise<boolean>;
}
