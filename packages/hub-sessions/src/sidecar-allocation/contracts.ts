import { type } from "arktype";

import type {
  AdapterManifest,
  RetireIdleSidecarResult,
  SidecarCapabilityDeclaration,
  SidecarView,
} from "@intx/types";

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
  /**
   * Operator-declared custom adapter manifest the spawned sidecar must load.
   * Passed through opaquely — the sidecar's adapter registry is the single
   * authority that resolves entries and admits `model_provider.plugin`
   * values at deploy time. Absent means no custom adapters are declared.
   * A sidecar reads its manifest only when it starts, so a provisioner that
   * answers with a sidecar it already runs must return one that booted with
   * this manifest, also when it retries an ensure for the same generation.
   * The Hub refuses reuse across a manifest change, so a changed manifest
   * applies to new placements only.
   */
  readonly adapterManifest?: AdapterManifest;
};

export type DestroySidecarRequest = {
  /** Cancellation never confirms destruction; the Hub may retry cleanup. */
  readonly signal?: AbortSignal;
  readonly allocationId: string;
  readonly generation: number;
  /**
   * The sidecar the Hub last recorded for this generation. When the Hub never
   * learned an ensure's outcome, this is the identity that ensure was offered
   * even if the provisioner placed the work on an existing sidecar. The Hub
   * sent such a placement no deployment, so nothing is left to release for it
   * on that sidecar. A destroy releases the allocation's hold at `generation`
   * or any older one: release and replacement advance the generation before
   * destroying.
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
 * used. The Hub accepts only a sidecar minted for the same provisioner binding
 * and adapter manifest that is not retired, whether or not it still hosts
 * other work. A rejected ensure, or a destroy that confirms cleanup, retires
 * the identity. A deployment also needs the sidecar to have fewer active
 * reservations than the capacity it advertised in hello. Copies
 * with confirmed removal or retained storage do not consume active capacity.
 * This is checked when an allocation is placed and when a deployment adopts
 * its probe's sidecar, not when the probe is placed.
 * Unknown inventory delays placement until hello or the connection deadline.
 * This is not a capacity rejection. The pending acceptance is in memory;
 * after a Hub restart, cleanup precedes any new provisioning attempt.
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
 * local state are gone and cannot return. A worker that remains, shared or
 * kept running idle, needs `cleanup: required`; the Hub reserves its slot
 * until the sidecar acknowledges removal. Retries must preserve this
 * distinction, including after restarts. Stopping a worker in answer to a
 * destroy must also remove its restorable deployment state and confirm cleanup;
 * one that may host other work needs `retireIfIdle` to answer `retired` first.
 * A result requiring sidecar cleanup must leave that cleanup path
 * available; absence from a provider's in-memory map is not proof.
 * A non-retryable rejection of an outstanding hold release stops automatic cleanup
 * and leaves the provider obligation and cleanup binding for operator recovery.
 * A copy holds an active slot until removal or its transfer to retained storage
 * is confirmed.
 */
export const DestroySidecarResult = type({
  kind: "'destroyed'",
  cleanup: "'confirmed' | 'required'",
}).or(SidecarOperationFailure);
export type DestroySidecarResult = typeof DestroySidecarResult.infer;

/**
 * `not_owned` is final: the Hub minted no sidecar with that id for this
 * provisioner's current binding.
 */
export type SidecarStoppedResult =
  | { readonly kind: "retired" }
  | { readonly kind: "not_owned" };

/**
 * `endReconnectWindow` stops the Hub waiting for the sidecar's work to
 * reconnect: its allocations get no reconnect window, now or after a later
 * disconnect, and go to its connection loss handling at once. Without it,
 * that work keeps its reconnect window, in case the provisioner brings the
 * sidecar back elsewhere. Work still connected runs until it disconnects;
 * probes are not touched.
 */
export const SidecarStoppedOptions = type({
  "endReconnectWindow?": "boolean",
});
export type SidecarStoppedOptions = typeof SidecarStoppedOptions.infer;

/** Calls the Hub offers a provisioner through `attach`. */
export interface SidecarProvisionerHost {
  /**
   * The sidecars minted under this provisioner's current binding and what is
   * on them, so a provisioner needs no record of its own: every sidecar not
   * retired, idle ones included, and every retired one with work left. Sidecars
   * of an older binding are not listed, and may still run healthy work.
   *
   * This is a hint for placement and for catching up after a restart, not a
   * reason to stop a sidecar. A worker missing from the list may have started
   * after the read, so stop it only after `retireIfIdle` answers `retired`. The
   * Hub records which sidecar a placement chose only after `ensure` returns;
   * until then the list shows it as `provisioning` on the identity the request
   * offered.
   */
  listSidecars(): Promise<readonly SidecarView[]>;
  /**
   * Retires a sidecar only if nothing is left on it; stop a sidecar that may
   * host other work only on `retired`. Owed cleanup counts as work. A copy the
   * Hub confirmed removed and a probe being released do not, so it works from
   * `destroy`. A placement racing it either commits first, giving `in_use`, or
   * is refused with `sidecar_reuse_rejected`.
   *
   * It cannot see a placement whose `ensure` is still in flight, so never
   * overlap it with your own `ensure` on the same sidecar; see
   * docs/SIDECAR_PLACEMENT.md. Idempotent. `not_owned` is final. A throw is
   * transient; invalid arguments also throw, and retrying them never succeeds.
   */
  retireIfIdle(sidecarId: string): Promise<RetireIdleSidecarResult>;
  /**
   * Reports that a sidecar this provisioner started under its current binding
   * is already gone for good, such as one that died. The Hub retires the
   * identity whatever is on it. To stop an idle sidecar, call `retireIfIdle`
   * first instead. Idempotent. A throw is transient: retry it. Work on a
   * sidecar nobody reports waits for its disconnect deadline. Invalid arguments
   * also throw, and retrying them never succeeds.
   */
  sidecarStopped(
    sidecarId: string,
    options?: SidecarStoppedOptions,
  ): Promise<SidecarStoppedResult>;
}

export interface SidecarProvisioner {
  readonly id: string;
  readonly apiVersion: 2;
  /** Stable, non-secret identity for the backend configuration. */
  readonly bindingFingerprint: string;
  readonly capabilities: readonly SidecarCapabilityDeclaration[];
  /** Called once when the Hub starts, with the calls it offers. */
  attach?(host: SidecarProvisionerHost): void;
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
  /**
   * The identity is retired with neither current work nor cleanup left. Its
   * handshake undeploys what the sidecar reports and closes.
   */
  readonly retired?: true;
};

/**
 * `reclaim` also requires the deployment's first deploy to have completed: its
 * run principal holds the deployment's key and no initialization is in flight.
 * A copy whose deploy is still uncertain is undeployed instead, since
 * releasing it on a sidecar that hosts other work does not stop the sidecar. It
 * further requires the anchor run not to be terminal: a copy of a run that has
 * ended is not routed, even when its own history never recorded the end.
 *
 * `retention` permits retaining an initialized terminal copy while its capacity
 * retention period remains open. `retention-transition` validates terminal
 * copies independently of that deadline, including stop acknowledgements and
 * acknowledgements for retains issued before expiry. Copies already due for
 * release are stopped without consuming kept capacity. `cleanup` accepts
 * releasing or replacing allocations and every failed cleanup, whatever
 * stopped it. Their connection grants no workflow routes and preserves
 * inventory. A reconnect can resume a disconnect timeout's cleanup, preserving
 * its attempts and terminal workflow outcome. Other failed cleanups do not
 * reopen. Replacing a parked deployment must confirm removal of its previous
 * copy before binding replacement capacity.
 */
export type SidecarIdentityUse =
  | "registration"
  | "cleanup"
  | "readiness"
  | "routing"
  | "reclaim"
  | "retention"
  | "retention-transition";

export interface SidecarCredentialResolver {
  /**
   * Resolves a bearer token to its sidecar, or null for an unknown token. A
   * retired one with neither current work nor cleanup left resolves marked
   * `retired`. A sidecar hosting nothing current still resolves.
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
