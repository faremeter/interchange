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
    };

export type SidecarPluginRegistry = {
  /** Missing plugins return null so reconciliation can stop fail-closed. */
  getProvisioner(id: string): SidecarProvisioner | null;
  selectProvisioner(
    policy: EffectiveSidecarCapabilityPolicy,
  ): SidecarProvisionerSelection;
};

export type CreateSidecarPluginRegistryOpts = {
  /** Ordered by selection priority; the first matching provisioner wins. */
  readonly provisioners: readonly SidecarProvisioner[];
};

export function createSidecarPluginRegistry({
  provisioners,
}: CreateSidecarPluginRegistryOpts): SidecarPluginRegistry {
  const provisionersById = new Map<string, SidecarProvisioner>();
  for (const provisioner of provisioners) {
    validateId(provisioner.id);
    if (provisioner.apiVersion !== 1) {
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
    selectProvisioner(policy) {
      const mismatches: Record<string, readonly SidecarCapabilityMismatch[]> =
        {};
      for (const provisioner of provisionersById.values()) {
        const match = matchSidecarCapabilityPolicy(
          policy,
          provisioner.capabilities,
        );
        if (match.ok) {
          return { ok: true, provisioner };
        } else {
          mismatches[provisioner.id] = match.mismatches;
        }
      }
      return { ok: false, reason: "no_match", mismatches };
    },
  };
}

function validateId(id: string): void {
  if (id.trim() === "") {
    throw new Error("Sidecar provisioner id must be non-empty");
  }
}
