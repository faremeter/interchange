// Per-deployment mailbox credentials, sealed at rest.
//
// The hub derives a distinct password per deployment address and delivers it on
// that deployment's `agent.deploy` frame, so a compromised deployment can read
// its own mail and nothing else. This holds what arrived.
//
// It has to be durable, not just in memory: a sidecar that restarts restores
// its deployments from disk WITHOUT a new deploy frame, so a credential kept
// only in memory would leave every restored deployment unable to open its own
// mailbox. The one thing the restore cannot recover is the thing it most needs.
//
// Sealed through the same `CredentialCipher` the sidecar already uses for its
// at-rest credential material, with the aad bound to the address -- so a sealed
// blob cannot be moved between addresses and still decrypt. The file name is a
// hex encoding of the address rather than the address itself, matching the
// sender-key cache: an address is not a safe path component.

import { readFile, readdir, mkdir } from "node:fs/promises";
import path from "node:path";

import { getLogger } from "@intx/log";
import { credentialAad, hexDecode, hexEncode } from "@intx/types";
import type { CredentialCipher } from "@intx/types";

const logger = getLogger(["sidecar", "mailbox-credentials"]);

/** Subdirectory of the sidecar data dir holding one file per address. */
const DIR_NAME = "mailbox-credentials";

/** The aad column tag, so this seal cannot be confused with another. */
const AAD_COLUMN = "mailbox-password";

export type MailboxCredential = { user: string; pass: string };

export type MailboxCredentialStore = {
  /**
   * Record a deployment's mailbox credential durably. Overwrites, because the
   * hub's derivation is stable and a redeploy of the same deployment delivers
   * the same value -- so a write is either the first one or a no-op in content.
   */
  put(address: string, credential: MailboxCredential): Promise<void>;
  /**
   * The credential held for `address`, or `undefined` when none is.
   *
   * Synchronous because the transport's `credentialsFor` is: the whole keyring
   * is loaded once at boot, so a read after that is a map lookup.
   */
  get(address: string): MailboxCredential | undefined;
  /** Durably forget an address's credential, on undeploy. */
  evict(address: string): Promise<void>;
};

export type MailboxCredentialStoreDeps = {
  dataDir: string;
  cipher: CredentialCipher;
  /** Atomic, fsynced write. Injected so the seal is at least as durable as the
   * deployment state it gates. */
  writeFileDurable: (filePath: string, contents: string) => Promise<void>;
  removeFileDurable: (filePath: string) => Promise<void>;
};

/**
 * Load the keyring from disk and return a store over it.
 *
 * A file that will not decrypt is logged at ERROR and SKIPPED rather than
 * failing the boot: one unreadable credential must not stop every other
 * deployment from starting, and the affected deployment surfaces its own
 * failure when it cannot open its mailbox. The operator condition is named in
 * the log either way.
 */
export async function createMailboxCredentialStore(
  deps: MailboxCredentialStoreDeps,
): Promise<MailboxCredentialStore> {
  const dir = path.join(deps.dataDir, DIR_NAME);
  await mkdir(dir, { recursive: true });

  const fileFor = (address: string): string =>
    path.join(dir, hexEncode(new TextEncoder().encode(address)));

  const held = new Map<string, MailboxCredential>();

  let names: string[];
  try {
    names = await readdir(dir);
  } catch (cause) {
    throw new Error(`mailbox credential store: could not read ${dir}`, {
      cause,
    });
  }

  for (const name of names) {
    let address: string;
    try {
      address = new TextDecoder().decode(hexDecode(name));
    } catch {
      logger.error`skipping mailbox-credential file ${name}: its name is not a hex-encoded address`;
      continue;
    }
    try {
      const sealed = await readFile(path.join(dir, name), "utf8");
      const pass = await deps.cipher.decrypt(
        sealed,
        credentialAad(address, AAD_COLUMN),
      );
      held.set(address, { user: address, pass });
    } catch (cause) {
      logger.error`mailbox credential for ${address} did not load, so this deployment cannot open its mailbox until the hub redelivers it: ${cause instanceof Error ? cause.message : String(cause)}`;
    }
  }

  if (held.size > 0) {
    logger.info`loaded ${String(held.size)} mailbox credential(s)`;
  }

  return {
    async put(address, credential) {
      const sealed = await deps.cipher.encrypt(
        credential.pass,
        credentialAad(address, AAD_COLUMN),
      );
      // Durable BEFORE the in-memory map, so a crash cannot leave a credential
      // that this process serves and a restart does not.
      await deps.writeFileDurable(fileFor(address), sealed);
      held.set(address, credential);
    },

    get(address) {
      return held.get(address);
    },

    async evict(address) {
      held.delete(address);
      await deps.removeFileDurable(fileFor(address));
    },
  };
}
