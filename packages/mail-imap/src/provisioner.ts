// Mailbox provisioning for a deployment address.
//
// A deployment cannot read its mail until an account exists for its address, and
// the address is only known once the deployment is created -- so provisioning
// belongs in the deploy path. The hub owns it, because the hub is what knows a
// deployment is about to exist and can create the mailbox BEFORE the sidecar is
// told to connect to it.
//
// What a mailbox is created with differs per server -- `setup email add` on one,
// a SQL insert on another, a provider's HTTP API on a third -- so this takes an
// operator-supplied command rather than knowing any of them. The hub learns only
// that an address now has a mailbox.
//
// WARNING: provisioning is disruptive on at least one real server. Creating an
// account on Dovecot reloads it, and the reload terminates every live IMAP
// session on the server -- so creating one deployment's mailbox drops every
// other deployment's connection. Recipients must therefore survive a dropped
// connection; `createImapHubTransport` rebuilds and re-sweeps for exactly this
// reason. Do not treat provisioning as a quiet operation.

import { getLogger } from "@intx/log";

const logger = getLogger(["interchange", "mail-imap", "provisioner"]);

/** A mailbox's login, as both the provisioner and the reader need it. */
export type MailAccountCredentials = { user: string; pass: string };

/**
 * Derive a mailbox password from an operator root secret and the address.
 *
 * Derived rather than random so it needs no storage and no rotation protocol: a
 * deployment's address is fixed for its whole life, so the same address always
 * yields the same password. A redeploy or a replacement worker therefore
 * provisions the account it already had, with the credential it already had,
 * and a live connection is not invalidated underneath itself.
 *
 * HMAC-SHA256 over a domain-separated input, base64url of the full 32-byte tag.
 * One address's password reveals nothing about another's, which is the whole
 * point of not sharing one secret: a compromised deployment reads its own mail
 * and no one else's. The root secret must be high-entropy -- it is the only
 * thing standing between one leaked password and all of them.
 *
 * If a second consumer ever needs this, it belongs in `@intx/crypto`; it lives
 * here while the provisioner is the only caller.
 */
export async function deriveMailboxPassword(
  rootSecret: string,
  address: string,
): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(rootSecret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const tag = await crypto.subtle.sign(
    "HMAC",
    key,
    // Domain-separated so this derivation can never collide with another use of
    // the same root secret, and length-prefixed so no two addresses can produce
    // the same input by concatenation.
    new TextEncoder().encode(
      `interchange-mailbox-password:${String(address.length)}:${address}`,
    ),
  );
  return Buffer.from(tag).toString("base64url");
}

export type MailboxProvisioner = {
  /**
   * Ensure a mailbox exists for `address`, and return its credential.
   *
   * Returning the credential rather than only creating the account is what lets
   * the caller hand it to the one recipient entitled to it, on the same frame
   * that tells that recipient to connect.
   *
   * MUST be idempotent, but not because an address is ever reused: a deployment
   * owns one address and one run for its whole life, and no later deployment
   * claims that address. The repeats all belong to the SAME deployment -- a
   * retried deploy, and the allocation reconciler redeploying it onto a
   * restarted worker, which is a continuation of that deployment rather than a
   * new one. So a second `ensure` is always for the mailbox its first call
   * created, and finding it already there is success.
   */
  ensure(address: string): Promise<MailAccountCredentials>;
  /**
   * Remove the mailbox for `address`. Tolerates an address with no mailbox.
   *
   * Safe to call on teardown precisely because addresses are not recycled:
   * nothing will later deploy to this address and find its mail missing.
   *
   * Removal takes the account out of the server's user database. Whether it
   * also invalidates the credential right away is the SERVER's behaviour, not
   * this function's, and at least one does not: `docker-mailserver`'s
   * `setup email del` does not reload Dovecot, so the removed account's
   * password keeps authenticating until some later reload -- in practice the
   * next deployment, since provisioning one always reloads. A caller that needs
   * the credential dead at a known instant has to reload the server itself.
   */
  remove(address: string): Promise<void>;
};

/**
 * The address shapes this provisioner will substitute into a command.
 *
 * Deployment addresses are derived (`run_<id>@<domain>`), never author-supplied,
 * so this is a guard against a future caller rather than against a current one.
 * It is a whitelist because the value reaches an external command: anything
 * outside it is refused rather than escaped.
 */
const SAFE_ADDRESS = /^[A-Za-z0-9._+-]+@[A-Za-z0-9.-]+$/;

export type CommandMailboxProvisionerConfig = {
  /**
   * Argv for creating a mailbox. The literal tokens `{address}` and
   * `{password}` are replaced, each as a WHOLE argv element.
   *
   * Argv, not a shell string: the command runs without a shell, so no quoting
   * rule governs how an address is interpreted and there is no injection to
   * reason about.
   */
  createArgv: readonly string[];
  /**
   * Argv for setting an EXISTING mailbox's password. Same substitution.
   *
   * Used when the create reports the account already there. Without it `ensure`
   * is only create-or-ignore, which leaves an account that predates the current
   * root secret holding a password nobody derives any more -- permanently
   * unopenable, and silently so, because creating it "succeeded". Converging
   * the password is what makes `ensure` mean what it says.
   */
  updateArgv?: readonly string[];
  /** Argv for removing a mailbox. Same substitution. */
  removeArgv?: readonly string[];
  /**
   * Root secret the per-address password is derived from. Never leaves the hub:
   * each recipient is given only its own derived password.
   */
  rootSecret: string;
  /**
   * Treat a non-zero exit whose stderr contains any of these as success.
   *
   * Idempotence has no standard spelling: one tool exits zero for an existing
   * account, another fails and says so. Naming the strings that mean "already
   * there" keeps a retried deploy from failing on an account it wanted anyway.
   */
  alreadyExistsMarkers?: readonly string[];
  /** Runs the command. Injected so a test observes calls without spawning. */
  run?: (
    argv: readonly string[],
  ) => Promise<{ exitCode: number; stderr: string }>;
};

async function spawnArgv(
  argv: readonly string[],
): Promise<{ exitCode: number; stderr: string }> {
  const proc = Bun.spawn([...argv], { stdout: "pipe", stderr: "pipe" });
  const exitCode = await proc.exited;
  const stderr = await new Response(proc.stderr).text();
  return { exitCode, stderr };
}

/**
 * A provisioner that runs an operator-supplied command per address, with a
 * password derived per address.
 */
export function createCommandMailboxProvisioner(
  config: CommandMailboxProvisionerConfig,
): MailboxProvisioner {
  const run = config.run ?? spawnArgv;
  const markers = config.alreadyExistsMarkers ?? ["already exists"];

  function render(
    argv: readonly string[],
    address: string,
    password: string,
  ): readonly string[] {
    return argv.map((token) =>
      token === "{address}"
        ? address
        : token === "{password}"
          ? password
          : token,
    );
  }

  function requireSafeAddress(address: string, what: string): void {
    if (!SAFE_ADDRESS.test(address)) {
      throw new Error(
        `mailbox provisioner: refusing to ${what} ${JSON.stringify(address)}; it is not a plain addr-spec`,
      );
    }
  }

  return {
    async ensure(address) {
      requireSafeAddress(address, "provision");
      const pass = await deriveMailboxPassword(config.rootSecret, address);
      const credentials: MailAccountCredentials = { user: address, pass };
      const { exitCode, stderr } = await run(
        render(config.createArgv, address, pass),
      );
      if (exitCode === 0) {
        logger.info`provisioned a mailbox for ${address}`;
        return credentials;
      }
      if (markers.some((marker) => stderr.includes(marker))) {
        // The account is already there. Its password is NOT assumed to match:
        // it may predate the current root secret, in which case leaving it
        // alone yields an account the hub provisioned "successfully" and no one
        // can open. Converge it when a command to do so was configured.
        if (config.updateArgv === undefined) {
          logger.warn`mailbox for ${address} already existed and no update command is configured; its password is whatever it already was, which may not be the derived one`;
          return credentials;
        }
        const updated = await run(render(config.updateArgv, address, pass));
        if (updated.exitCode !== 0) {
          throw new Error(
            `mailbox provisioner: ${address} already exists and its password could not be set (exit ${String(updated.exitCode)}): ${updated.stderr.trim()}`,
          );
        }
        logger.debug`converged the existing mailbox password for ${address}`;
        return credentials;
      }
      throw new Error(
        `mailbox provisioner: could not create a mailbox for ${address} (exit ${String(exitCode)}): ${stderr.trim()}`,
      );
    },

    async remove(address) {
      requireSafeAddress(address, "remove the mailbox of");
      if (config.removeArgv === undefined) {
        logger.warn`no mailbox-removal command configured; the mailbox for ${address} is left in place`;
        return;
      }
      const { exitCode, stderr } = await run(
        render(
          config.removeArgv,
          address,
          await deriveMailboxPassword(config.rootSecret, address),
        ),
      );
      if (exitCode !== 0) {
        // Removal is cleanup, not a correctness step: a mailbox left behind
        // costs disk, whereas a throw here would fail an undeploy that has
        // already torn the deployment down.
        logger.warn`could not remove the mailbox for ${address} (exit ${String(exitCode)}): ${stderr.trim()}`;
        return;
      }
      logger.info`removed the mailbox for ${address}`;
    },
  };
}
