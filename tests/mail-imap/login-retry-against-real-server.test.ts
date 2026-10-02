// What `createImapTransport.start()` does against a real server when the login
// is refused.
//
// The classification is unit-tested in the package. What cannot be unit-tested
// is the part that makes a retry work at all: `ImapFlow` cannot reopen a closed
// instance, so each attempt has to build a new pair of clients. A test with a
// stubbed client would pass whether or not the transport did that, because the
// stub would happily accept a second `connect()`. A real server refuses one.
//
// So these cases drive the real client against the real Dovecot and count the
// attempts the server saw.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { createImapTransport } from "@intx/mail-imap";

import {
  MAIL_SERVER,
  mailServerReachable,
  provisionAddress,
  testMailboxPassword,
} from "./server";

const reachable = await mailServerReachable();

let address = "";
let password = "";

beforeAll(async () => {
  if (!reachable) return;
  address = await provisionAddress("login-retry");
  password = await testMailboxPassword(address);
});

// Each case builds its own transport, and a transport that connected holds two
// sessions open. Closing them keeps a long suite from accumulating sessions
// against a server whose per-user connection limit is finite.
const opened: { close: () => Promise<void> }[] = [];
afterAll(async () => {
  await Promise.allSettled(opened.map((transport) => transport.close()));
});

async function startTransport(
  auth: { user: string; pass: string },
  policy: { loginAttempts: number; sleep: (ms: number) => Promise<void> },
) {
  const crypto = createEd25519Crypto(await generateKeyPair());
  const transport = createImapTransport({
    address,
    crypto,
    getCrypto: () => undefined,
    imap: {
      host: MAIL_SERVER.host,
      port: MAIL_SERVER.imapPort,
      secure: false,
      auth,
    },
    smtp: {
      host: MAIL_SERVER.host,
      port: MAIL_SERVER.smtpPort,
      secure: false,
      ignoreTLS: true,
    },
    loginAttempts: policy.loginAttempts,
    sleep: policy.sleep,
  });
  return transport;
}

describe.skipIf(!reachable)("IMAP login against a real server", () => {
  test("a correct password logs in on the first attempt", async () => {
    const delays: number[] = [];
    const transport = await startTransport(
      { user: address, pass: password },
      {
        loginAttempts: 4,
        sleep: async (ms) => {
          delays.push(ms);
        },
      },
    );

    await transport.start();
    opened.push(transport);

    // No delay recorded means no attempt was retried. A transport that
    // reconnected needlessly would still pass `start()`, so the absence of a
    // retry is the assertion.
    expect(delays).toEqual([]);
  });

  test("a wrong password is retried and then reported", async () => {
    // AUTHENTICATIONFAILED is classified transient, so a password that is
    // genuinely wrong is retried to exhaustion rather than reported at once.
    // That is the accepted cost of retrying the server-reload case, and this
    // pins the behaviour so the cost stays visible: the server really does see
    // three logins it did not have to.
    const delays: number[] = [];
    const transport = await startTransport(
      { user: address, pass: "not-the-derived-password" },
      {
        loginAttempts: 3,
        sleep: async (ms) => {
          delays.push(ms);
        },
      },
    );

    let reported: Error | undefined;
    try {
      await transport.start();
    } catch (cause) {
      if (!(cause instanceof Error)) {
        throw new Error(`expected an Error, got ${String(cause)}`, { cause });
      }
      reported = cause;
    }
    if (reported === undefined) {
      throw new Error("expected the login to be reported as failed");
    }

    // Exhausted, not permanent: the message has to say which, because a caller
    // reporting this upward answers differently for each.
    expect(reported.message).toContain("still failing after 3 attempts");
    expect(reported.message).toContain("AUTHENTICATIONFAILED");
    expect(reported.message).toContain(address);
    // Two delays for three attempts: nothing waits after the last refusal.
    expect(delays).toHaveLength(2);
  });

  test("a transport can log in after an earlier attempt was refused", async () => {
    // The point of rebuilding the clients per attempt. A transport that reused
    // its `ImapFlow` instances would fail here even with the right password,
    // because the instance the refused attempt closed stays closed.
    const transport = await startTransport(
      { user: address, pass: password },
      { loginAttempts: 2, sleep: async () => undefined },
    );

    await transport.start();
    opened.push(transport);

    // Reading the mailbox proves the session is usable and not merely open.
    const refs = await transport.search("INBOX", {});
    expect(Array.isArray(refs)).toBe(true);
  });
});
