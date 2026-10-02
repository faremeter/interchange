// Drives `@intx/tools-mail` -- unchanged -- against a real Postfix + Dovecot
// server through `@intx/mail-imap`.
//
// The question this answers is the one the research could not: does the shipped
// mail tool set work over a real SMTP submission relay and a real IMAP mailbox,
// with no change to the tools, the tool error vocabulary, or the agent-facing
// contract? Every assertion below is made through a tool call, never against the
// transport directly, so a pass means the AGENT's surface works, not merely that
// the client library does.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { createImapTransport, type ImapTransport } from "@intx/mail-imap";
import { createMailTools, type MailTools } from "@intx/tools-mail";
import type { CryptoProvider, ToolResult } from "@intx/types/runtime";
import { createRuntimeCapabilities } from "@intx/types/runtime-capabilities";

import {
  MAIL_SERVER,
  mailServerReachable,
  provisionAddress,
  testMailboxPassword,
  waitForAccount,
} from "./server";

const reachable = await mailServerReachable();

/** Unique per run so two runs against one long-lived container do not collide. */
const runTag = Bun.hash(
  `${String(process.pid)}:${String(performance.now())}`,
).toString(36);

type Party = {
  address: string;
  crypto: CryptoProvider;
  transport: ImapTransport;
  tools: MailTools;
};

const parties: Party[] = [];
const keysByAddress = new Map<string, CryptoProvider>();

async function createParty(address: string): Promise<Party> {
  const crypto = createEd25519Crypto(await generateKeyPair());
  keysByAddress.set(address, crypto);
  // Derived per address, exactly as the hub derives a deployment's. No shared
  // account secret exists in these suites.
  const pass = await testMailboxPassword(address);

  const transport = createImapTransport({
    address,
    crypto,
    // Both parties resolve each other's key, so a verified signature is the
    // expected outcome rather than `unknown`.
    getCrypto: (from) => keysByAddress.get(from),
    imap: {
      host: MAIL_SERVER.host,
      port: MAIL_SERVER.imapPort,
      secure: false,
      auth: { user: address, pass },
    },
    smtp: {
      host: MAIL_SERVER.host,
      port: MAIL_SERVER.smtpPort,
      secure: false,
      auth: { user: address, pass },
      ignoreTLS: true,
    },
  });

  await waitForAccount(address, async () => {
    const probe = createImapTransport({
      address,
      crypto,
      getCrypto: () => undefined,
      imap: {
        host: MAIL_SERVER.host,
        port: MAIL_SERVER.imapPort,
        secure: false,
        auth: { user: address, pass },
      },
      smtp: {
        host: MAIL_SERVER.host,
        port: MAIL_SERVER.smtpPort,
        secure: false,
        ignoreTLS: true,
      },
    });
    await probe.start();
    await probe.close();
  });

  await transport.start();
  const tools = createMailTools({
    capabilities: createRuntimeCapabilities({ "mail.transport": transport }),
  });
  const party: Party = { address, crypto, transport, tools };
  parties.push(party);
  return party;
}

/** Call a mail tool and fail the test with the tool's own error text on failure. */
async function call(
  party: Party,
  name: string,
  args: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const result: ToolResult = await party.tools.run(
    { id: `${name}-${String(parties.length)}`, name, arguments: args },
    new AbortController().signal,
  );
  if (typeof result.content === "string") {
    throw new Error(`${name} returned text, not a record: ${result.content}`);
  }
  if (result.isError === true) {
    throw new Error(
      `${name} failed: ${JSON.stringify(result.content)} args=${JSON.stringify(args)}`,
    );
  }
  return result.content;
}

let alpha: Party;
let beta: Party;

beforeAll(async () => {
  if (!reachable) return;
  // Provision EVERY account before opening any connection. `setup email add`
  // reloads Dovecot, and the reload SIGTERMs live IMAP sessions -- so
  // provisioning a second address while the first address is connected kills
  // that connection. Ordering the two phases keeps that effect out of the
  // tests below; it is recorded as its own finding rather than worked around
  // silently.
  const addresses = await Promise.all([
    provisionAddress(`run_alpha${runTag}`),
    provisionAddress(`run_beta${runTag}`),
  ]);
  const [alphaAddress, betaAddress] = addresses;
  if (alphaAddress === undefined || betaAddress === undefined) {
    throw new Error("provisioning did not return both addresses");
  }
  alpha = await createParty(alphaAddress);
  beta = await createParty(betaAddress);
});

afterAll(async () => {
  await Promise.allSettled(parties.map((p) => p.transport.close()));
});

describe.skipIf(!reachable)("mail tools over real SMTP and IMAP", () => {
  test("a sent message arrives, verifies, and reads back through the tools", async () => {
    const body = "The quarterly review is ready for your sign-off.";

    // mail_wait installs the watch BEFORE the send, which is the ordering a
    // real agent uses and the one that exercises IMAP IDLE rather than the
    // opening search.
    const waiting = call(beta, "mail_wait", {
      query: { from: alpha.address },
      timeout: 60,
    });

    const sent = await call(alpha, "mail_send", {
      to: beta.address,
      subject: "Quarterly review",
      content: body,
    });
    expect(typeof sent["messageId"]).toBe("string");

    const arrived = await waiting;
    expect(arrived["from"]).toContain(alpha.address);
    expect(arrived["subject"]).toBe("Quarterly review");
    expect(arrived["content"]).toBe(body);

    const ref = arrived["ref"];
    const full = await call(beta, "mail_read", { ref, parts: "full" });
    // The whole point of the round trip: the PGP/MIME signature computed
    // before SMTP submission still verifies over the bytes IMAP served back.
    expect(full["signatureStatus"]).toBe("valid");
    expect(full["content"]).toBe(body);
  });

  test("non-ASCII content survives the round trip and keeps its signature", async () => {
    // A body outside US-ASCII is the case the `7bit` label used to misdescribe.
    const body = "Café — naïve façade 日本語 🙂";

    const waiting = call(beta, "mail_wait", {
      query: { header: { field: "subject", contains: "Unicode" } },
      timeout: 60,
    });

    await call(alpha, "mail_send", {
      to: beta.address,
      subject: "Unicode",
      content: body,
    });

    const arrived = await waiting;
    const full = await call(beta, "mail_read", {
      ref: arrived["ref"],
      parts: "full",
    });
    expect(full["content"]).toBe(body);
    expect(full["signatureStatus"]).toBe("valid");
  });

  test("a custom keyword set through mail_flag is searchable", async () => {
    const waiting = call(beta, "mail_wait", {
      query: { header: { field: "subject", contains: "Keyword" } },
      timeout: 60,
    });
    await call(alpha, "mail_send", {
      to: beta.address,
      subject: "Keyword",
      content: "flag me",
    });
    const arrived = await waiting;
    const ref = arrived["ref"];

    await call(beta, "mail_flag", { ref, set: ["$Processed"] });

    const found = await call(beta, "mail_search", {
      query: { hasFlags: ["$Processed"] },
      limit: 20,
    });
    const results = found["results"];
    if (!Array.isArray(results)) throw new Error("results is not an array");
    expect(results.length).toBeGreaterThan(0);

    await call(beta, "mail_flag", { ref, clear: ["$Processed"] });
    const afterClear = await call(beta, "mail_search", {
      query: { hasFlags: ["$Processed"] },
      limit: 20,
    });
    const clearedResults = afterClear["results"];
    if (!Array.isArray(clearedResults)) {
      throw new Error("results is not an array");
    }
    expect(clearedResults).toHaveLength(0);
  });

  test("a message part reads back by its MIME path", async () => {
    const waiting = call(beta, "mail_wait", {
      query: { header: { field: "subject", contains: "Parts" } },
      timeout: 60,
    });
    await call(alpha, "mail_send", {
      to: beta.address,
      subject: "Parts",
      content: "part addressed body",
    });
    const arrived = await waiting;

    // Part 1.1 is the text/plain body under the assembler's conversation
    // shape, per MESSAGE.md's part addressing table.
    const part = await call(beta, "mail_read", {
      ref: arrived["ref"],
      parts: "1.1",
    });
    expect(JSON.stringify(part)).toContain("part addressed body");
  });

  test("mail_expunge removes a message flagged for deletion", async () => {
    const waiting = call(beta, "mail_wait", {
      query: { header: { field: "subject", contains: "Expunge" } },
      timeout: 60,
    });
    await call(alpha, "mail_send", {
      to: beta.address,
      subject: "Expunge",
      content: "delete me",
    });
    const arrived = await waiting;
    const ref = arrived["ref"];

    await call(beta, "mail_flag", { ref, set: ["\\Deleted"] });
    const swept = await call(beta, "mail_expunge", {});
    const expunged = swept["expungedUids"];
    if (!Array.isArray(expunged)) {
      throw new Error("expungedUids is not an array");
    }
    expect(expunged.length).toBeGreaterThan(0);

    // The reference now names nothing, which the tool reports as not_found.
    const after = await beta.tools.run(
      { id: "read-gone", name: "mail_read", arguments: { ref } },
      new AbortController().signal,
    );
    expect(after.isError).toBe(true);
  });
});
