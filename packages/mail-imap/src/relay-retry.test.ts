// Which refused SMTP submissions the relay reissues, and what it does when
// reissuing does not help.
//
// The classification is the part worth pinning down. SMTP's own reply classes
// are not a reliable guide here: the relay retries a 535, which RFC 5321
// section 4.2.1 calls permanent, because the hub provisions a mailbox inside
// the deploy it is serving and the resulting server reload answers 535 for
// about a second. The reasoning is in `isTransientSubmitFailure`; these
// assertions hold the line on both halves of it, because a rule that retried
// every 5xx would reissue an unknown-recipient refusal three times for nothing.
//
// The retry loop runs against an injected sequence of failures rather than a
// server coaxed into producing them, so a case names exactly one condition and
// the pass costs no wall-clock.

import { describe, expect, test } from "bun:test";

import { isTransientSubmitFailure, submitWithRetry } from "./relay";
import type { RetryPolicy } from "./retry";

/** A nodemailer failure, as the fields `relay.ts` classifies on. */
function smtpError(
  fields: { code?: string; responseCode?: number; command?: string },
  message = "submission refused",
): Error {
  return Object.assign(new Error(message), fields);
}

/**
 * The error a submission rejected with. A submission that resolved is a
 * different failure from the one under test, so it reports rather than
 * answering undefined and letting the assertions below read as a miss.
 */
async function rejection(run: () => Promise<void>): Promise<Error> {
  try {
    await run();
  } catch (cause) {
    if (!(cause instanceof Error)) {
      throw new Error(`expected an Error, got ${String(cause)}`, { cause });
    }
    return cause;
  }
  throw new Error("expected the submission to be reported as failed");
}

// Records the delays rather than waiting them out, after the captured-scheduler
// seam the mail-wait suites use. A case that retried more than it should shows
// up as an extra recorded delay, which is a clearer failure than a slow pass.
function capturingPolicy(attempts: number): RetryPolicy & { delays: number[] } {
  const delays: number[] = [];
  return {
    attempts,
    baseMs: 100,
    delays,
    sleep: async (ms) => {
      delays.push(ms);
    },
  };
}

describe("isTransientSubmitFailure retries what clears on its own", () => {
  test("an authentication failure is transient despite its 5xx code", () => {
    // The reload case. 535 is permanent by the letter of RFC 5321, and
    // treating it that way is what made every deploy a coin flip.
    expect(
      isTransientSubmitFailure(
        smtpError({ code: "EAUTH", responseCode: 535, command: "AUTH PLAIN" }),
      ),
    ).toBe(true);
  });

  test("a 4xx reply is transient", () => {
    expect(
      isTransientSubmitFailure(
        smtpError({ code: "EENVELOPE", responseCode: 451, command: "RCPT TO" }),
      ),
    ).toBe(true);
  });

  test("a connection that never came up is transient", () => {
    for (const code of ["ECONNECTION", "ESOCKET", "ETIMEDOUT", "EDNS"]) {
      expect(isTransientSubmitFailure(smtpError({ code }))).toBe(true);
    }
  });
});

describe("isTransientSubmitFailure leaves permanent refusals alone", () => {
  test("an unknown recipient is permanent", () => {
    expect(
      isTransientSubmitFailure(
        smtpError({ code: "EENVELOPE", responseCode: 550, command: "RCPT TO" }),
      ),
    ).toBe(false);
  });

  test("a message the server will never accept is permanent", () => {
    // 552 is the size refusal. Our 44MB cap exceeds the 10MB a stock Postfix
    // advertises, so this is a reachable case and not a hypothetical.
    expect(
      isTransientSubmitFailure(
        smtpError({ code: "EMESSAGE", responseCode: 552, command: "DATA" }),
      ),
    ).toBe(false);
  });

  test("a failure carrying no classification is permanent", () => {
    // A defect in our own submission path arrives as a bare Error. Retrying it
    // would turn one visible bug into three invisible ones.
    expect(isTransientSubmitFailure(new TypeError("raw is not a Buffer"))).toBe(
      false,
    );
    expect(isTransientSubmitFailure(undefined)).toBe(false);
    expect(isTransientSubmitFailure("refused")).toBe(false);
  });
});

describe("submitWithRetry", () => {
  test("a transient failure that clears is not reported to the caller", async () => {
    const policy = capturingPolicy(4);
    let calls = 0;

    await submitWithRetry(
      async () => {
        calls += 1;
        if (calls === 1) throw smtpError({ code: "EAUTH", responseCode: 535 });
      },
      "hub@test.interchange",
      policy,
    );

    expect(calls).toBe(2);
    expect(policy.delays).toEqual([100]);
  });

  test("the delay doubles between attempts", async () => {
    const policy = capturingPolicy(4);

    const thrown = await rejection(() =>
      submitWithRetry(
        async () => {
          throw smtpError({ code: "ECONNECTION" });
        },
        "hub@test.interchange",
        policy,
      ),
    );
    expect(thrown.message).toMatch(/still failing after 4 attempts/);

    // Three delays for four attempts: nothing waits after the last refusal.
    expect(policy.delays).toEqual([100, 200, 400]);
  });

  test("a permanent refusal is submitted exactly once", async () => {
    const policy = capturingPolicy(4);
    let calls = 0;

    const thrown = await rejection(() =>
      submitWithRetry(
        async () => {
          calls += 1;
          throw smtpError({ code: "EENVELOPE", responseCode: 550 });
        },
        "hub@test.interchange",
        policy,
      ),
    );
    expect(thrown.message).toMatch(/refused permanently/);

    expect(calls).toBe(1);
    expect(policy.delays).toEqual([]);
  });

  test("the reported failure says whether retrying was tried", async () => {
    // The two outcomes warrant different responses from a caller -- a
    // deployment whose relay credentials are wrong is not a deployment whose
    // mail server is briefly down -- so the distinction has to survive to it.
    const exhausted = await rejection(() =>
      submitWithRetry(
        async () => {
          throw smtpError({ code: "EAUTH", responseCode: 535 });
        },
        "hub@test.interchange",
        capturingPolicy(2),
      ),
    );

    const permanent = await rejection(() =>
      submitWithRetry(
        async () => {
          throw smtpError({ code: "EMESSAGE", responseCode: 552 });
        },
        "hub@test.interchange",
        capturingPolicy(2),
      ),
    );

    expect(exhausted.message).toContain("still failing after 2 attempts");
    expect(exhausted.message).toContain("EAUTH 535");
    expect(permanent.message).toContain("refused permanently");
    expect(permanent.message).toContain("EMESSAGE 552");
  });

  test("the original failure stays reachable as the cause", async () => {
    // The wrapper names what the relay decided; the diagnosis needs what the
    // server actually said.
    const original = smtpError(
      { code: "EAUTH", responseCode: 535, command: "AUTH PLAIN" },
      "535 5.7.8 Error: authentication failed",
    );

    const thrown = await rejection(() =>
      submitWithRetry(
        async () => {
          throw original;
        },
        "hub@test.interchange",
        capturingPolicy(1),
      ),
    );

    expect(thrown.cause).toBe(original);
  });

  test("a policy that permits no attempt is a configuration defect", async () => {
    // Zero attempts would silently never submit. It reports rather than
    // resolving as though the message went out.
    const thrown = await rejection(() =>
      submitWithRetry(
        async () => undefined,
        "hub@test.interchange",
        capturingPolicy(0),
      ),
    );
    expect(thrown.message).toMatch(/attempts must be at least 1/);
  });
});
