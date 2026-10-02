// Harness for the containerized Postfix + Dovecot server the IMAP transport
// tests run against.
//
// The container is managed OUTSIDE the test run, by `bin/mail-server`. A test
// run neither starts nor stops it: start-up takes long enough that paying it per
// run would dominate the suite, and the container holds no per-run state that
// needs resetting -- each test provisions its own addresses, so two runs do not
// collide.
//
// A run with no reachable server SKIPS rather than fails. The server is an
// external dependency this spike does not yet wire into `make test`, so a
// developer without it should not see red.

import { connect } from "node:net";

import { ImapFlow } from "imapflow";

import { deriveMailboxPassword } from "@intx/mail-imap";

/** Ports `bin/mail-server` publishes on the host. */
export const MAIL_SERVER = {
  host: "127.0.0.1",
  imapPort: 3143,
  smtpPort: 3587,
  domain: "test.interchange",
  container: "intx-mail",
} as const;

/**
 * Root secret these suites derive mailbox passwords from.
 *
 * Fixed rather than random because the derivation's whole purpose is stability:
 * the hub provisions an account inside one process and a test logs into it from
 * another, and both must arrive at the same password without passing it
 * between them.
 */
export const TEST_MAIL_ROOT_SECRET =
  "interchange-spike-mailbox-root-secret-do-not-use-in-production";

/**
 * The password the hub would derive for `address`. Every login in these suites
 * goes through this, so there is no shared account secret anywhere in them --
 * the same property production gets.
 */
export function testMailboxPassword(address: string): Promise<string> {
  return deriveMailboxPassword(TEST_MAIL_ROOT_SECRET, address);
}

/** Resolve once a TCP connect to `port` succeeds, or reject when it does not. */
function probePort(port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = connect({ host: MAIL_SERVER.host, port });
    socket.once("connect", () => {
      socket.destroy();
      resolve();
    });
    socket.once("error", (cause) => {
      socket.destroy();
      reject(cause);
    });
  });
}

/**
 * Whether both server ports accept a connection. Used to skip the suite when
 * the container is not running.
 */
export async function mailServerReachable(): Promise<boolean> {
  try {
    await Promise.all([
      probePort(MAIL_SERVER.imapPort),
      probePort(MAIL_SERVER.smtpPort),
    ]);
    return true;
  } catch {
    return false;
  }
}

async function runInContainer(
  args: string[],
): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  const proc = Bun.spawn(["docker", "exec", MAIL_SERVER.container, ...args], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const exitCode = await proc.exited;
  const stdout = await new Response(proc.stdout).text();
  const stderr = await new Response(proc.stderr).text();
  return { exitCode, stdout, stderr };
}

/**
 * Whether the server holds a mail account for `address`.
 *
 * Asks the account tool rather than attempting a login: an account that exists
 * but whose Maildir Dovecot has not reloaded yet refuses a login, which would
 * read as absent.
 */
export async function accountExists(address: string): Promise<boolean> {
  const { exitCode, stdout, stderr } = await runInContainer([
    "setup",
    "email",
    "list",
  ]);
  if (exitCode !== 0) {
    throw new Error(`could not list mail accounts: ${stderr.trim()}`);
  }
  // Matched on a word boundary: `a@x` must not be found inside `ba@x`.
  return new RegExp(
    `(^|\\s)${address.replace(/[.+]/g, "\\$&")}(\\s|$)`,
    "m",
  ).test(stdout);
}

/**
 * Wait until the server reports an open IMAP session for `address`.
 *
 * A test that forces a server reload to drop a deployment's connections needs
 * those connections to EXIST first, and no client-side signal says they do: a
 * deploy resolves when the sidecar has applied the deployment, which is before
 * its mailbox login has necessarily finished -- the login retries a refusal, so
 * it can still be in flight. Forcing the reload in that window drops nothing,
 * the login absorbs both reloads, and the test then waits forever for a loss
 * that never happened.
 *
 * `doveadm who` answers it from the server's side, which is the only place that
 * knows a session is established.
 *
 * Polls because the server emits nothing a client can await; carries no
 * deadline of its own, so the test runner's budget is what turns a hang into a
 * failure.
 */
export async function waitForOpenSession(address: string): Promise<void> {
  for (;;) {
    const { exitCode, stdout, stderr } = await runInContainer([
      "doveadm",
      "who",
      address,
    ]);
    // An account `doveadm` cannot resolve yet is not a failure to report: it
    // means the reload that publishes it has not landed, which is the same
    // "not yet" this function polls through.
    if (exitCode !== 0 && !stderr.includes("User doesn't exist")) {
      throw new Error(
        `could not ask the server who is connected as ${address}: ${stderr.trim()}`,
      );
    }
    // The header line is always printed, so a listed session is any line
    // naming the address.
    if (stdout.includes(address)) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/**
 * Ensure a mail account exists and return its address.
 *
 * Idempotent, because accounts outlive a test run: a suite whose addresses are
 * derived from fixed identifiers (a deployment address comes from its anchor run
 * id) cannot pick a fresh local part per run, so re-provisioning an existing
 * account has to succeed rather than fail the second run.
 */
export async function provisionAddress(localPart: string): Promise<string> {
  const address = `${localPart}@${MAIL_SERVER.domain}`;
  const password = await testMailboxPassword(address);
  const created = await runInContainer([
    "setup",
    "email",
    "add",
    address,
    password,
  ]);
  if (created.exitCode === 0) {
    // Settle before returning: the reload this create triggered refuses logins
    // while it runs, so a caller that connects straight away races it.
    await waitForMailboxLogin(address);
    return address;
  }
  if (!created.stderr.includes("already exists")) {
    throw new Error(
      `could not provision ${address} on container ${MAIL_SERVER.container}: ${created.stderr.trim()}`,
    );
  }
  // Converge an account that outlived an earlier run: accounts persist on the
  // container, so one created before the derivation existed still holds its old
  // password and no login would match it.
  const updated = await runInContainer([
    "setup",
    "email",
    "update",
    address,
    password,
  ]);
  if (updated.exitCode !== 0) {
    throw new Error(
      `could not set ${address}'s password on container ${MAIL_SERVER.container}: ${updated.stderr.trim()}`,
    );
  }
  await waitForMailboxLogin(address);
  return address;
}

/**
 * Wait until `address` answers an authenticated IMAP login.
 *
 * Creating an account reloads Dovecot, and the reload terminates live sessions
 * and refuses logins while it runs -- so a caller that provisions and
 * immediately connects races it. Worse, every suite sharing this container
 * races it: one suite's provisioning breaks another's connections, which is the
 * same hazard the transport's reconnect exists for, reaching the harness.
 *
 * Polls because the server emits no readiness signal a client can await, and
 * carries no deadline of its own: the test runner's budget is what turns a
 * server that never comes back into a failure.
 */
export async function waitForMailboxLogin(address: string): Promise<void> {
  const pass = await testMailboxPassword(address);
  for (;;) {
    const client = new ImapFlow({
      host: MAIL_SERVER.host,
      port: MAIL_SERVER.imapPort,
      secure: false,
      auth: { user: address, pass },
      logger: false,
    });
    try {
      await client.connect();
      await client.logout();
      return;
    } catch {
      await client.close?.();
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
}

/**
 * Empty an account's INBOX, so a suite reusing a fixed address does not observe
 * a previous run's mail. Uses the server's own admin tool rather than IMAP, so
 * it does not depend on the transport under test.
 */
export async function purgeInbox(address: string): Promise<void> {
  const { exitCode, stderr } = await runInContainer([
    "doveadm",
    "expunge",
    "-u",
    address,
    "mailbox",
    "INBOX",
    "all",
  ]);
  if (exitCode === 0) return;
  // Two non-faults. An empty mailbox makes some versions exit non-zero with
  // nothing to say. And an account created moments ago is not yet visible to
  // `doveadm`, which reads the user database Dovecot reloads asynchronously --
  // a mailbox that does not exist yet is already as empty as a purge can make
  // it, so there is nothing to report.
  const message = stderr.trim();
  if (message === "" || message.includes("User doesn't exist")) return;
  throw new Error(`could not purge ${address}'s INBOX: ${message}`);
}

/**
 * Wait until the account's mailbox answers an authenticated IMAP login.
 *
 * `setup email add` returns once the account is written to the user database,
 * which is before Dovecot has reloaded it and before the Maildir exists. The
 * readiness signal is therefore a successful login, not the provisioning
 * command's exit. Polls because the server emits nothing a client can await;
 * carries no deadline of its own, so the test runner's budget is what turns a
 * hang into a failure.
 */
export async function waitForAccount(
  address: string,
  login: (address: string) => Promise<void>,
): Promise<void> {
  for (;;) {
    try {
      await login(address);
      return;
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }
  }
}
