import type { SidecarProvisioner } from "./contracts";

export type SidecarPluginRegistry = {
  getDefaultProvisioner(): SidecarProvisioner | null;
  /** Missing plugins return null so reconciliation can stop fail-closed. */
  getProvisioner(id: string): SidecarProvisioner | null;
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
  };
}

function validateId(id: string): void {
  if (id.trim() === "") {
    throw new Error("Sidecar provisioner id must be non-empty");
  }
}
