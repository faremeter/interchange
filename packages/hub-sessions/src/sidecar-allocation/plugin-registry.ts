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
  | {
      readonly ok: false;
      readonly reason: "ambiguous";
      readonly provisionerIds: readonly string[];
    };

export type SidecarPluginRegistry = {
  getDefaultProvisioner(): SidecarProvisioner | null;
  /** Missing plugins return null so reconciliation can stop fail-closed. */
  getProvisioner(id: string): SidecarProvisioner | null;
  selectProvisioner(
    policy: EffectiveSidecarCapabilityPolicy,
  ): SidecarProvisionerSelection;
};

export type CreateSidecarPluginRegistryOpts = {
  readonly provisioners: readonly SidecarProvisioner[];
  readonly defaultProvisionerId?: string;
};

export function createSidecarPluginRegistry({
  provisioners,
  defaultProvisionerId,
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

  if (defaultProvisionerId !== undefined) {
    validateId(defaultProvisionerId);
    if (!provisionersById.has(defaultProvisionerId)) {
      throw new Error(
        `Default sidecar provisioner ${defaultProvisionerId} is not registered`,
      );
    }
  }

  return {
    getDefaultProvisioner() {
      return defaultProvisionerId === undefined
        ? null
        : (provisionersById.get(defaultProvisionerId) ?? null);
    },
    getProvisioner(id) {
      return provisionersById.get(id) ?? null;
    },
    selectProvisioner(policy) {
      const eligible: SidecarProvisioner[] = [];
      const mismatches: Record<string, readonly SidecarCapabilityMismatch[]> =
        {};
      for (const provisioner of provisionersById.values()) {
        const match = matchSidecarCapabilityPolicy(
          policy,
          provisioner.capabilities,
        );
        if (match.ok) {
          eligible.push(provisioner);
        } else {
          mismatches[provisioner.id] = match.mismatches;
        }
      }

      const preferred =
        defaultProvisionerId === undefined
          ? null
          : (provisionersById.get(defaultProvisionerId) ?? null);
      if (preferred !== null && eligible.includes(preferred)) {
        return { ok: true, provisioner: preferred };
      }
      if (eligible.length === 1) {
        const provisioner = eligible[0];
        if (provisioner === undefined) {
          throw new Error("Eligible provisioner disappeared during selection");
        }
        return { ok: true, provisioner };
      }
      if (eligible.length === 0) {
        return { ok: false, reason: "no_match", mismatches };
      }
      return {
        ok: false,
        reason: "ambiguous",
        provisionerIds: eligible.map(({ id }) => id).sort(),
      };
    },
  };
}

function validateId(id: string): void {
  if (id.trim() === "") {
    throw new Error("Sidecar provisioner id must be non-empty");
  }
}
