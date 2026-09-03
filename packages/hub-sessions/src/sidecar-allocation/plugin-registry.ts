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

export type SidecarProvisionerSelectionContext = {
  readonly tenantId: string;
  /** Authenticated tenant principal whose request owns placement. */
  readonly placementPrincipalId: string;
  readonly targetHostPrincipalId?: string;
  /** Policy already used to filter the candidate list. */
  readonly capabilityPolicy: EffectiveSidecarCapabilityPolicy;
};

/** Selects one provisioner from the capability-matched candidates. */
export type SidecarProvisionerChooser = (
  candidates: readonly SidecarProvisioner[],
  context: SidecarProvisionerSelectionContext,
) => SidecarProvisioner | Promise<SidecarProvisioner>;

export type SidecarPluginRegistry = {
  /** Missing plugins return null so reconciliation can stop fail-closed. */
  getProvisioner(id: string): SidecarProvisioner | null;
  selectProvisioner(
    context: SidecarProvisionerSelectionContext,
  ): Promise<SidecarProvisionerSelection>;
};

export type CreateSidecarPluginRegistryOpts = {
  /** Ordered candidates; the default chooser selects the first match. */
  readonly provisioners: readonly SidecarProvisioner[];
  readonly chooser?: SidecarProvisionerChooser;
};

export function chooseFirstSidecarProvisioner(
  candidates: readonly SidecarProvisioner[],
): SidecarProvisioner {
  const first = candidates[0];
  if (first === undefined) {
    throw new Error("No matching sidecar provisioners are available");
  }
  return first;
}

export function createSidecarPluginRegistry({
  provisioners,
  chooser = chooseFirstSidecarProvisioner,
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
    async selectProvisioner(context) {
      const mismatches: Record<string, readonly SidecarCapabilityMismatch[]> =
        {};
      const candidates: SidecarProvisioner[] = [];
      for (const provisioner of provisionersById.values()) {
        const match = matchSidecarCapabilityPolicy(
          context.capabilityPolicy,
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
      if (!candidates.includes(provisioner)) {
        throw new Error(
          "Sidecar provisioner chooser returned a provisioner outside the matching candidates",
        );
      }
      return { ok: true, provisioner };
    },
  };
}

function validateId(id: string): void {
  if (id.trim() === "") {
    throw new Error("Sidecar provisioner id must be non-empty");
  }
}
