import { getLogger } from "@intx/log";
import { createHubServer } from "@intx/hub-app/server";

import { createLocalProcessSidecarProvisioner } from "./local-process-sidecar-provisioner";

const hubDataDir = process.env["HUB_DATA_DIR"];
if (hubDataDir === undefined || hubDataDir.trim() === "") {
  throw new Error("HUB_DATA_DIR environment variable is required");
}

const logger = getLogger(["admin-ui-e2e", "hub"]);
const localProcessProvisioner = createLocalProcessSidecarProvisioner({
  dataRoot: `${hubDataDir}/local-sidecars`,
  // E2E_SHARE_SIDECARS=tenant runs each tenant's probes and deployments on one
  // sidecar process instead of a process per allocation.
  ...(process.env["E2E_SHARE_SIDECARS"] === "tenant"
    ? { shareSidecarsBy: (request) => request.tenantId }
    : {}),
});

let shuttingDown = false;
function shutdown(): void {
  if (shuttingDown) return;
  shuttingDown = true;
  void localProcessProvisioner
    .shutdown()
    .catch((error: unknown) => {
      logger.error`Failed to stop local sidecars: ${error instanceof Error ? error.message : String(error)}`;
    })
    .finally(() => process.exit(0));
}

process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);

export default await createHubServer({
  sidecarProvisioners: [localProcessProvisioner.provisioner],
  probeSidecarProvisioners: [localProcessProvisioner.provisioner],
});
