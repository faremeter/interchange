// A deployment's mailbox does not outlive the deployment.
//
// The router's ordering rules are unit-tested in `@intx/hub-sessions` against a
// mock socket. What that cannot show is whether the account is really gone
// afterwards: the hook is handed an operator-configured argv, and whether the
// command it runs removes anything is a property of the server, not of the
// router. So this drives the real `createCommandMailboxProvisioner` against the
// real Dovecot and asks the server.
//
// Why it matters: a deployment has exactly one run and is never redeployed to
// the same address, so a mailbox left behind is read by nothing ever again. It
// is pure accumulation -- the container this suite runs against had grown past
// a hundred accounts before the undeploy end was wired.
//
// One property is deliberately NOT asserted here. Removing an account does not
// reliably invalidate its credential: `setup email del` does not reload
// Dovecot, so the running auth process keeps serving the removed account until
// some later reload lands -- and whether one lands before the next login is a
// race against other deployments' provisioning. The behaviour is recorded on
// `MailboxProvisioner.remove`, where a caller will read it; asserting a race
// here would only produce a test that fails one run in three.

import { afterAll, describe, expect, test } from "bun:test";
import { ImapFlow } from "imapflow";

import {
  accountExists,
  mailServerReachable,
  MAIL_SERVER,
  waitForMailboxLogin,
} from "./server";
import { createTestMailboxProvisioner } from "./provisioner";

const reachable = await mailServerReachable();

/** Unique per run so a failed run's leftovers cannot mask a later one. */
const runTag = String(Date.now());
const ADDRESS = `run_undeploy-removal-${runTag}@${MAIL_SERVER.domain}`;

const provisioner = createTestMailboxProvisioner();

// A failed assertion between provision and remove would leak the account this
// test created, which is the exact condition the test exists to prevent.
afterAll(async () => {
  if (!reachable) return;
  await provisioner.remove(ADDRESS);
});

describe.skipIf(!reachable)("a deployment's mailbox is removed with it", () => {
  test("the provisioned account exists and then does not", async () => {
    await provisioner.ensure(ADDRESS);
    // Provisioning returns once the account is written to the user database,
    // which is before Dovecot has reloaded it. Waiting for a login means the
    // removal below acts on an account the server has actually published --
    // otherwise "gone" could just mean "not there yet".
    await waitForMailboxLogin(ADDRESS);
    expect(await accountExists(ADDRESS)).toBe(true);

    await provisioner.remove(ADDRESS);

    expect(await accountExists(ADDRESS)).toBe(false);
  });

  test("removing a mailbox that is already gone is not an error", async () => {
    // The undeploy path can run twice for one address -- a retried teardown, a
    // hub that restarts mid-undeploy -- and the second call must not report a
    // failure for an address whose mailbox is correctly absent.
    const absent = `run_undeploy-never-existed-${runTag}@${MAIL_SERVER.domain}`;
    expect(await accountExists(absent)).toBe(false);

    await provisioner.remove(absent);

    expect(await accountExists(absent)).toBe(false);
  });

  test("a mailbox with mail still in it is removed anyway", async () => {
    // The deployment is gone, so nothing will ever read this mail again, and
    // keeping the mailbox to preserve it would keep it forever. Asserted
    // because discarding mail is the kind of thing that should be a decision
    // on the record rather than a side effect nobody chose.
    const address = `run_undeploy-nonempty-${runTag}@${MAIL_SERVER.domain}`;
    const { pass } = await provisioner.ensure(address);
    await waitForMailboxLogin(address);

    const client = new ImapFlow({
      host: MAIL_SERVER.host,
      port: MAIL_SERVER.imapPort,
      secure: false,
      auth: { user: address, pass },
      logger: false,
    });
    await client.connect();
    await client.append(
      "INBOX",
      Buffer.from(
        `From: sender@${MAIL_SERVER.domain}\r\nTo: ${address}\r\nSubject: unread\r\n\r\nstill here\r\n`,
      ),
    );
    const box = await client.mailboxOpen("INBOX");
    expect(box.exists).toBe(1);
    await client.logout();

    await provisioner.remove(address);

    expect(await accountExists(address)).toBe(false);
  });
});
