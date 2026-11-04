import { type } from "arktype";

import type {
  AllocationConnectionCheck,
  ProvisionerSidecarRetirement,
  SidecarProvisionerBinding,
} from "@intx/db";
import { getLogger } from "@intx/log";
import type { RetireIdleSidecarResult } from "@intx/types";

import { SidecarStoppedOptions, type SidecarProvisioner } from "./contracts";

const logger = getLogger(["hub", "sidecar-allocation"]);

const SidecarId = type("string > 0");

const SidecarStoppedArgs = type({
  sidecarId: SidecarId,
  options: SidecarStoppedOptions.or("undefined"),
});

export type AttachSidecarProvisionersDeps = {
  readonly provisioners: readonly SidecarProvisioner[];
  readonly retireProvisionerSidecar: (args: {
    readonly sidecarId: string;
    readonly binding: SidecarProvisionerBinding;
    readonly endReconnectWindow: boolean;
    readonly isConnected: AllocationConnectionCheck;
  }) => Promise<ProvisionerSidecarRetirement | null>;
  readonly retireIdleProvisionerSidecar: (args: {
    readonly sidecarId: string;
    readonly binding: SidecarProvisionerBinding;
  }) => Promise<RetireIdleSidecarResult>;
  readonly isConnected: AllocationConnectionCheck;
};

// A provisioner can sit in both the deployment and the probe registry, so
// each object is attached once.
export function attachSidecarProvisioners({
  provisioners,
  retireProvisionerSidecar,
  retireIdleProvisionerSidecar,
  isConnected,
}: AttachSidecarProvisionersDeps): void {
  for (const provisioner of new Set(provisioners)) {
    const binding: SidecarProvisionerBinding = {
      provisionerId: provisioner.id,
      provisionerApiVersion: provisioner.apiVersion,
      provisionerBindingFingerprint: provisioner.bindingFingerprint,
    };
    provisioner.attach?.({
      async retireIfIdle(rawSidecarId) {
        const sidecarId = SidecarId(rawSidecarId);
        if (sidecarId instanceof type.errors)
          throw new Error(
            `Provisioner ${provisioner.id} sent an invalid retireIfIdle call: ${sidecarId.summary}`,
          );
        return retireIdleProvisionerSidecar({ sidecarId, binding });
      },
      async sidecarStopped(rawSidecarId, rawOptions) {
        // A provisioner is outside the Hub's type checks; its report is input.
        const args = SidecarStoppedArgs({
          sidecarId: rawSidecarId,
          options: rawOptions,
        });
        if (args instanceof type.errors)
          throw new Error(
            `Provisioner ${provisioner.id} sent an invalid sidecarStopped report: ${args.summary}`,
          );
        const { sidecarId, options } = args;
        const retirement = await retireProvisionerSidecar({
          sidecarId,
          binding,
          endReconnectWindow: options?.endReconnectWindow ?? false,
          isConnected,
        });
        if (retirement === null) return { kind: "not_owned" };
        if (retirement.connectedAllocationIds.length > 0)
          logger.warn`Provisioner ${provisioner.id} reported sidecar ${sidecarId} stopped while allocations ${retirement.connectedAllocationIds.join(", ")} are still connected to it; leaving them running`;
        return { kind: "retired" };
      },
    });
  }
}
