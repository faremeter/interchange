// Which refused IMAP logins the transport reissues.
//
// The companion to relay-retry.test.ts, and it exists for the same reason: the
// interesting case is an authentication failure, which the protocol reports as
// final and which this deployment produces transiently. Creating a mailbox
// reloads the mail server, a reload refuses logins for about a second, and the
// hub creates a mailbox inside the deploy whose sidecar then logs in. The
// reasoning is written out at `isTransientLoginFailure`.
//
// The retry loop itself is covered through the relay, which drives the same
// `retrying` helper. These assertions are about classification only, and the
// fields they build are the ones imapflow really sets -- see the
// AUTHENTICATIONFAILED case, which reproduces an observed failure verbatim.

import { describe, expect, test } from "bun:test";

import { describeLoginFailure, isTransientLoginFailure } from "./transport";

describe("isTransientLoginFailure retries what clears on its own", () => {
  test("the authentication failure a server reload produces is transient", () => {
    // Taken from an observed failure: a sidecar logging in while another
    // deployment's mailbox creation reloaded Dovecot. The password was correct.
    const observed = Object.assign(new Error("Command failed"), {
      response: "2 NO [AUTHENTICATIONFAILED] Authentication failed.",
      responseStatus: "NO",
      executedCommand: "2 AUTHENTICATE PLAIN",
      responseText: "Authentication failed.",
      serverResponseCode: "AUTHENTICATIONFAILED",
      authenticationFailed: true,
    });

    expect(isTransientLoginFailure(observed)).toBe(true);
  });

  test("a server that says it is temporarily unavailable is transient", () => {
    for (const serverResponseCode of ["UNAVAILABLE", "INUSE", "SERVERBUG"]) {
      expect(isTransientLoginFailure({ serverResponseCode })).toBe(true);
    }
  });

  test("a socket that never came up is transient", () => {
    for (const code of [
      "ECONNREFUSED",
      "ECONNRESET",
      "ETIMEDOUT",
      "EPIPE",
      "EHOSTUNREACH",
      "ENOTFOUND",
    ]) {
      expect(
        isTransientLoginFailure(Object.assign(new Error("x"), { code })),
      ).toBe(true);
    }
  });
});

describe("isTransientLoginFailure leaves the rest alone", () => {
  test("a refusal naming no transient condition is permanent", () => {
    // A tagged NO on something other than authentication. Retrying it would
    // issue the same rejected command three more times.
    expect(
      isTransientLoginFailure({
        responseStatus: "NO",
        serverResponseCode: "ALREADYEXISTS",
        responseText: "Mailbox exists.",
      }),
    ).toBe(false);
  });

  test("a certificate the client will not accept is permanent", () => {
    expect(
      isTransientLoginFailure(
        Object.assign(new Error("self-signed certificate"), {
          code: "DEPTH_ZERO_SELF_SIGNED_CERT",
        }),
      ),
    ).toBe(false);
  });

  test("a failure carrying no classification is permanent", () => {
    // A defect in our own connect path arrives as a bare Error. Retrying it
    // would turn one visible bug into four invisible ones.
    expect(isTransientLoginFailure(new TypeError("auth is undefined"))).toBe(
      false,
    );
    expect(isTransientLoginFailure(undefined)).toBe(false);
    expect(isTransientLoginFailure("Authentication failed.")).toBe(false);
  });
});

describe("describeLoginFailure", () => {
  test("it reports what the server said", () => {
    expect(
      describeLoginFailure({
        serverResponseCode: "AUTHENTICATIONFAILED",
        responseText: "Authentication failed.",
      }),
    ).toBe("AUTHENTICATIONFAILED Authentication failed.");
  });

  test("a failure it cannot read is named rather than reported as empty", () => {
    // An empty parenthetical in the reported message reads as "no reason
    // given", which is indistinguishable from a bug in this function.
    expect(describeLoginFailure(new Error("boom"))).toBe("unclassified");
  });
});
