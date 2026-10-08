import { type } from "arktype";

import type { SidecarCapabilityDeclaration } from "@intx/types";

export type EnsureSidecarRequest = {
  /** Cancellation is best effort; a cancelled ensure has an uncertain outcome. */
  readonly signal?: AbortSignal;
  readonly allocationId: string;
  readonly generation: number;
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly sidecarId: string;
  readonly token: string;
  readonly hubWebSocketUrl: string;
};

export type DestroySidecarRequest = {
  /** Cancellation never confirms destruction; the Hub may retry cleanup. */
  readonly signal?: AbortSignal;
  readonly allocationId: string;
  readonly generation: number;
  readonly sidecarId: string;
  /**
   * Optional provider handle recorded after ensure returns; destroy must also
   * work from allocationId, generation, and sidecarId alone, since a Hub crash
   * can precede persisting this value.
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
 * Rejection means no infrastructure exists for this generation (destroy
 * rejections carry no such guarantee); a provisioner must throw when it cannot
 * determine whether the request took effect.
 */
export const EnsureSidecarResult = type({
  kind: "'accepted'",
  "externalRef?": "string",
}).or(SidecarOperationFailure);
export type EnsureSidecarResult = typeof EnsureSidecarResult.infer;

/**
 * Destruction confirms the capacity is gone and older ensure calls are fenced.
 * A non-retryable rejection stops automatic cleanup; capacity may still exist.
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
   * Converges infrastructure for this generation: idempotent, rejects
   * generations older than one it has observed, honours the request signal
   * where possible, and a late completion still respects a concurrent destroy's
   * fence even if cancellation was ignored.
   */
  ensure(request: EnsureSidecarRequest): Promise<EnsureSidecarResult>;
  /**
   * Idempotently destroys the allocation and fences older ensure calls so a
   * delayed request cannot recreate infrastructure after destruction; must
   * succeed without an externalRef, which is only an optional optimization.
   */
  destroy(request: DestroySidecarRequest): Promise<DestroySidecarResult>;
}

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

export interface SidecarCredentialResolver {
  resolve(token: string): Promise<SidecarCredentialIdentity | null>;
  isCurrent(
    identity: SidecarCredentialIdentity,
    use: "registration" | "readiness" | "routing",
  ): Promise<boolean>;
}
