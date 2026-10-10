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
 * replacement has not yet destroyed it. A deployment also needs the sidecar to
 * host fewer than `MAX_SIDECAR_INCARNATIONS` deployments, which is checked
 * when an allocation is placed and when a deployment adopts its probe's
 * sidecar, not when the probe is placed.
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
 * Destruction fences older ensure calls and releases the provisioner's hold.
 * `cleanup: confirmed` additionally proves that the deployment's worker and
 * local state are gone and cannot return. A shared worker that remains needs
 * `cleanup: required`; the Hub reserves its slot until the sidecar acknowledges
 * removal. Retries must preserve this distinction, including after restarts.
 * Stopping the last worker must also remove its restorable deployment state
 * and confirm cleanup. A result requiring sidecar cleanup must leave that
 * cleanup path available; absence from a provider's in-memory map is not proof.
 * A non-retryable rejection stops automatic cleanup and leaves the provider
 * obligation and cleanup binding for operator recovery. A copy holds a slot
 * until removal is confirmed.
 */
export const DestroySidecarResult = type({
  kind: "'destroyed'",
  cleanup: "'confirmed' | 'required'",
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

type AllocationCredentialIdentity = {
  readonly sidecarId: string;
  readonly allocationId: string;
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly workflowRunAddress: string;
  readonly generation: number;
};

/** One probe, deployment, or cleanup obligation authenticated on a sidecar. */
export type SidecarCredentialIdentity =
  | (AllocationCredentialIdentity & { readonly kind: "allocated" })
  | (AllocationCredentialIdentity & { readonly kind: "cleanup" })
  | {
      readonly kind: "probe";
      readonly sidecarId: string;
      readonly allocationId: string;
      readonly tenantId: string;
      readonly generation: number;
    };

/** The sidecar a bearer token verifies as. */
export type SidecarCredentials = {
  readonly sidecarId: string;
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
 * ended without the Hub cancelling or failing it. The copy then stays
 * unrouted, its local state kept until the Hub releases the deployment, so a
 * reconnect does not cut short the retention the deployment's policy sets. A
 * copy of a run the Hub ended is undeployed, since a restart that lost its
 * stopped mark may be running that run again. `cleanup` only accepts releasing
 * allocations. Their registration keeps the cleanup connection alive but
 * grants none of the running or retained copy's rights.
 */
export type SidecarIdentityUse =
  | "registration"
  | "cleanup"
  | "readiness"
  | "routing"
  | "reclaim"
  | "retention";

export interface SidecarCredentialResolver {
  /**
   * Resolves a bearer token to its sidecar, or null for an unknown token. A
   * sidecar hosting nothing current still resolves.
   */
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
