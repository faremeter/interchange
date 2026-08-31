import { type } from "arktype";

import type { SidecarCapabilityDeclaration } from "@intx/types";

export type EnsureSidecarRequest = {
  readonly allocationId: string;
  readonly generation: number;
  readonly tenantId: string;
  readonly anchorRunId: string;
  readonly sidecarId: string;
  readonly token: string;
  readonly hubWebSocketUrl: string;
};

export type DestroySidecarRequest = {
  readonly allocationId: string;
  readonly generation: number;
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
 * Rejection means no infrastructure exists for this generation; a provisioner
 * must throw when it cannot determine whether the request took effect.
 */
export const EnsureSidecarResult = type({
  kind: "'accepted'",
  "externalRef?": "string",
}).or(SidecarOperationFailure);
export type EnsureSidecarResult = typeof EnsureSidecarResult.infer;

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
   */
  ensure(request: EnsureSidecarRequest): Promise<EnsureSidecarResult>;
  /**
   * Idempotently destroys the allocation and fences older ensure calls so a
   * delayed request cannot recreate infrastructure after destruction. It must
   * succeed without an externalRef; that value is only an optional optimization.
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
