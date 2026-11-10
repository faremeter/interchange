import { type } from "arktype";

import { SidecarCapabilityDeclaration } from "@intx/types";

import type { SidecarProvisioner } from "./contracts";
import {
  matchSidecarCapabilityPolicy,
  type EffectiveSidecarCapabilityPolicy,
  type SidecarCapabilityMismatch,
} from "./capability-policy";

export type SidecarProvisionerSelection =
  | { readonly ok: true; readonly provisioner: SidecarProvisioner }
  | {
      readonly ok: false;
      readonly reason: "no_match";
      readonly mismatches: Readonly<
        Record<string, readonly SidecarCapabilityMismatch[]>
      >;
    }
  | { readonly ok: false; readonly reason: "declined" };

/** An ensure that failed for the probe being placed. */
export type SidecarProvisionerAttempt = {
  readonly provisionerId: string;
  /**
   * The provisioner's rejection code, or the Hub's own: `ensure_failed` when
   * ensure threw and `sidecar_reuse_rejected` when the Hub refused the sidecar
   * it named.
   */
  readonly code: string;
  readonly message: string;
  readonly retryable: boolean;
};

export type SidecarProvisionerChoiceContext = {
  readonly purpose: "probe" | "deployment";
  readonly tenantId: string;
  /** The deployment's anchor run, also when its probe is being placed. */
  readonly anchorRunId: string;
  /** Aborts when the Hub stops waiting for the choice. */
  readonly signal: AbortSignal;
  /** Oldest first. Empty on the first choice, and always for a deployment. */
  readonly attempts: readonly SidecarProvisionerAttempt[];
};

/**
 * Selects one provisioner from the capability-matched candidates. The Hub
 * asks again after a failed probe ensure, with that failure added to
 * `attempts`. Returning a provisioner that already failed retries it;
 * returning null fails the deploy request. State a chooser keeps belongs to
 * one Hub process.
 */
export type SidecarProvisionerChooser = (
  candidates: readonly SidecarProvisioner[],
  context: SidecarProvisionerChoiceContext,
) => SidecarProvisioner | null | Promise<SidecarProvisioner | null>;

export type SidecarPluginRegistry = {
  /** Missing plugins return null so reconciliation can stop fail-closed. */
  getProvisioner(id: string): SidecarProvisioner | null;
  selectProvisioner(
    policy: EffectiveSidecarCapabilityPolicy,
    context: SidecarProvisionerChoiceContext,
  ): Promise<SidecarProvisionerSelection>;
};

export type CreateSidecarPluginRegistryOpts = {
  /** Ordered candidates; the default chooser selects the first match. */
  readonly provisioners: readonly SidecarProvisioner[];
  readonly chooser?: SidecarProvisionerChooser;
};

/**
 * The first match in registration order. After a failed ensure it selects
 * nothing, so a probe is never moved to another provisioner.
 */
export function chooseFirstSidecarProvisioner(
  candidates: readonly SidecarProvisioner[],
  { attempts }: SidecarProvisionerChoiceContext,
): SidecarProvisioner | null {
  return attempts.length === 0 ? (candidates[0] ?? null) : null;
}

export function createSidecarPluginRegistry({
  provisioners,
  chooser = chooseFirstSidecarProvisioner,
}: CreateSidecarPluginRegistryOpts): SidecarPluginRegistry {
  const provisionersById = new Map<string, SidecarProvisioner>();
  for (const provisioner of provisioners) {
    validateId(provisioner.id);
    if (provisioner.apiVersion !== 2) {
      throw new Error(
        `Unsupported sidecar provisioner API version for ${provisioner.id}: ${String(provisioner.apiVersion)}`,
      );
    }
    if (provisioner.bindingFingerprint.trim() === "") {
      throw new Error(
        `Sidecar provisioner ${provisioner.id} requires a binding fingerprint`,
      );
    }
    const capabilities = SidecarCapabilityDeclaration.array()(
      provisioner.capabilities,
    );
    if (capabilities instanceof type.errors) {
      throw new Error(
        `Invalid capability declarations on sidecar provisioner ${provisioner.id}: ${capabilities.summary}`,
      );
    }
    if (provisionersById.has(provisioner.id)) {
      throw new Error(`Duplicate sidecar provisioner id: ${provisioner.id}`);
    }
    provisionersById.set(provisioner.id, provisioner);
  }

  return {
    getProvisioner(id) {
      return provisionersById.get(id) ?? null;
    },
    async selectProvisioner(policy, context) {
      const mismatches: Record<string, readonly SidecarCapabilityMismatch[]> =
        {};
      const candidates: SidecarProvisioner[] = [];
      for (const provisioner of provisionersById.values()) {
        const match = matchSidecarCapabilityPolicy(
          policy,
          provisioner.capabilities,
        );
        if (match.ok) {
          candidates.push(provisioner);
        } else {
          mismatches[provisioner.id] = match.mismatches;
        }
      }
      if (candidates.length === 0) {
        return { ok: false, reason: "no_match", mismatches };
      }
      const provisioner = await chooser(candidates, context);
      if (provisioner === null) return { ok: false, reason: "declined" };
      if (!candidates.includes(provisioner)) {
        throw new Error(
          "Sidecar provisioner chooser returned a provisioner outside the matching candidates",
        );
      }
      return { ok: true, provisioner };
    },
  };
}

export function formatProvisionerMismatches(
  mismatches: Readonly<Record<string, readonly SidecarCapabilityMismatch[]>>,
): string {
  const details = Object.entries(mismatches)
    .sort(([left], [right]) => left.localeCompare(right))
    .flatMap(([provisionerId, failures]) =>
      failures.map(
        ({ capability, expected, actual }) =>
          `${provisionerId}: ${capability} must be ${expected}, was ${actual}`,
      ),
    );
  return details.length === 0
    ? "No sidecar provisioners are registered"
    : `No sidecar provisioner satisfies the deployment (${details.join("; ")})`;
}

function validateId(id: string): void {
  if (id.trim() === "") {
    throw new Error("Sidecar provisioner id must be non-empty");
  }
}
