// The mail-server provisioner the suites hand to the fixture hub.
//
// Production supplies an operator-configured argv; here that argv is the
// container's own account tool. The point of using the real
// `createCommandMailboxProvisioner` rather than a stub is that the suites then
// exercise what production runs: the derivation, the already-exists tolerance,
// and the credential the deploy frame carries.

import {
  createCommandMailboxProvisioner,
  type MailboxProvisioner,
} from "@intx/mail-imap";

import { MAIL_SERVER, TEST_MAIL_ROOT_SECRET } from "./server";

export function createTestMailboxProvisioner(): MailboxProvisioner {
  return createCommandMailboxProvisioner({
    createArgv: [
      "docker",
      "exec",
      MAIL_SERVER.container,
      "setup",
      "email",
      "add",
      "{address}",
      "{password}",
    ],
    updateArgv: [
      "docker",
      "exec",
      MAIL_SERVER.container,
      "setup",
      "email",
      "update",
      "{address}",
      "{password}",
    ],
    removeArgv: [
      "docker",
      "exec",
      MAIL_SERVER.container,
      "setup",
      "email",
      "del",
      "-y",
      "{address}",
    ],
    rootSecret: TEST_MAIL_ROOT_SECRET,
  });
}

/**
 * The fixture hook shape: provision and hand back the credential the deploy
 * frame carries.
 */
export function testProvisionMailbox(): (
  address: string,
) => Promise<{ user: string; password: string }> {
  const provisioner = createTestMailboxProvisioner();
  return async (address) => {
    const { user, pass } = await provisioner.ensure(address);
    return { user, password: pass };
  };
}

/** The fixture hook shape for the undeploy end of the mailbox's life. */
export function testDeprovisionMailbox(): (address: string) => Promise<void> {
  const provisioner = createTestMailboxProvisioner();
  return (address) => provisioner.remove(address);
}
