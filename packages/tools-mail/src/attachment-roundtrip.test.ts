import { describe, test, expect } from "bun:test";

import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { createInMemoryTransport } from "@intx/mail-memory";
import { base64Encode } from "@intx/types";

import { makeMailReadHandler, makeMailSendHandler } from "./handlers";

const signal = new AbortController().signal;

// The mock-transport tests stop at the OutboundMessage. These run the tools
// against a real signing transport, so the part paths mail_read advertises
// are checked against what the MIME writer actually emits.
describe("attachments through a real transport", () => {
  async function createMailboxes() {
    const transport = createInMemoryTransport();
    for (const address of ["alpha@test.interchange", "beta@test.interchange"]) {
      transport.register(address, createEd25519Crypto(await generateKeyPair()));
    }
    return {
      alpha: transport.getTransportFor("alpha@test.interchange"),
      beta: transport.getTransportFor("beta@test.interchange"),
    };
  }

  async function exchange(attachments: Record<string, string>[]) {
    const { alpha, beta } = await createMailboxes();
    const send = makeMailSendHandler(alpha);
    const read = makeMailReadHandler(beta);

    const sent = await send(
      {
        id: "send",
        name: "mail_send",
        arguments: {
          to: "beta@test.interchange",
          content: "see attached",
          attachments,
        },
      },
      signal,
    );
    expect(sent.isError).toBeUndefined();

    const [ref] = await beta.search("INBOX", {});
    if (ref === undefined) throw new Error("expected a delivered message");
    const readPart = async (parts: string) => {
      const result = await read(
        { id: parts, name: "mail_read", arguments: { ref, parts } },
        signal,
      );
      expect(result.isError).toBeUndefined();
      return result.content;
    };
    return { readPart };
  }

  test("each advertised part path returns the attachment that was sent", async () => {
    const png = base64Encode(new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x00]));
    const { readPart } = await exchange([
      { name: "ascii.txt", contentType: "text/plain", content: "plain\r\n" },
      { name: "notes.md", contentType: "text/markdown", content: "café\n" },
      { name: "shot.png", contentType: "image/png", content: png },
    ]);

    expect(await readPart("full")).toMatchObject({
      attachments: [
        { name: "ascii.txt", contentType: "text/plain", size: 7, part: "1.2" },
        {
          name: "notes.md",
          contentType: "text/markdown",
          size: 6,
          part: "1.3",
        },
        { name: "shot.png", contentType: "image/png", size: 5, part: "1.4" },
      ],
    });
    expect(await readPart("1.2")).toMatchObject({
      encoding: "utf-8",
      content: "plain\r\n",
    });
    expect(await readPart("1.3")).toMatchObject({
      encoding: "utf-8",
      content: "café\n",
    });
    expect(await readPart("1.4")).toMatchObject({
      encoding: "base64",
      content: png,
    });
  });

  test("mail_read refuses the signed mixed wrapper and still fetches 1.1", async () => {
    const { alpha, beta } = await createMailboxes();
    const send = makeMailSendHandler(alpha);
    const read = makeMailReadHandler(beta);
    const sent = await send(
      {
        id: "send",
        name: "mail_send",
        arguments: {
          to: "beta@test.interchange",
          content: "the body",
          attachments: [
            { name: "notes.txt", contentType: "text/plain", content: "file" },
          ],
        },
      },
      signal,
    );
    expect(sent.isError).toBeUndefined();
    const [ref] = await beta.search("INBOX", {});
    if (ref === undefined) throw new Error("expected a delivered message");

    const mixed = await read(
      { id: "mixed", name: "mail_read", arguments: { ref, parts: "1" } },
      signal,
    );
    expect(mixed.isError).toBe(true);
    if (typeof mixed.content === "string") {
      throw new Error("expected object content");
    }
    expect(mixed.content["code"]).toBe("invalid_part");

    const body = await read(
      { id: "body", name: "mail_read", arguments: { ref, parts: "1.1" } },
      signal,
    );
    expect(body.isError).toBeUndefined();
    expect(body.content).toMatchObject({
      encoding: "utf-8",
      content: "the body",
    });
  });

  test("read-then-send round-trips advertised attachment bytes", async () => {
    const { alpha, beta } = await createMailboxes();
    const sendAlpha = makeMailSendHandler(alpha);
    const readBeta = makeMailReadHandler(beta);
    const first = await sendAlpha(
      {
        id: "send1",
        name: "mail_send",
        arguments: {
          to: "beta@test.interchange",
          content: "see attached",
          attachments: [
            {
              name: "notes.txt",
              contentType: "text/plain; charset=utf-8",
              content: "café",
            },
          ],
        },
      },
      signal,
    );
    expect(first.isError).toBeUndefined();
    const [ref] = await beta.search("INBOX", {});
    if (ref === undefined) throw new Error("expected a delivered message");

    const listed = await readBeta(
      { id: "full", name: "mail_read", arguments: { ref, parts: "full" } },
      signal,
    );
    expect(listed.isError).toBeUndefined();
    if (typeof listed.content === "string") {
      throw new Error("expected object content");
    }
    const attachments = listed.content["attachments"];
    if (!Array.isArray(attachments) || attachments[0] === undefined) {
      throw new Error("expected one listed attachment");
    }
    const advertised = attachments[0];
    if (
      typeof advertised !== "object" ||
      advertised === null ||
      !("part" in advertised) ||
      typeof advertised.part !== "string" ||
      !("name" in advertised) ||
      typeof advertised.name !== "string" ||
      !("contentType" in advertised) ||
      typeof advertised.contentType !== "string"
    ) {
      throw new Error("expected listed attachment metadata");
    }

    const fetched = await readBeta(
      {
        id: "part",
        name: "mail_read",
        arguments: { ref, parts: advertised.part },
      },
      signal,
    );
    expect(fetched.isError).toBeUndefined();
    if (typeof fetched.content === "string") {
      throw new Error("expected object content");
    }
    const encoding = fetched.content["encoding"];
    const content = fetched.content["content"];
    if (typeof encoding !== "string" || typeof content !== "string") {
      throw new Error("expected fetched part content");
    }

    const sendBeta = makeMailSendHandler(beta);
    const resent = await sendBeta(
      {
        id: "send2",
        name: "mail_send",
        arguments: {
          to: "alpha@test.interchange",
          content: "forwarded",
          attachments: [
            {
              name: advertised.name,
              contentType: advertised.contentType,
              content,
              encoding,
            },
          ],
        },
      },
      signal,
    );
    expect(resent.isError).toBeUndefined();

    const readAlpha = makeMailReadHandler(alpha);
    const [back] = await alpha.search("INBOX", {});
    if (back === undefined) throw new Error("expected a resent message");
    const backListed = await readAlpha(
      {
        id: "back",
        name: "mail_read",
        arguments: { ref: back, parts: "full" },
      },
      signal,
    );
    expect(backListed.isError).toBeUndefined();
    if (typeof backListed.content === "string") {
      throw new Error("expected object content");
    }
    const backAtts = backListed.content["attachments"];
    if (!Array.isArray(backAtts) || backAtts[0] === undefined) {
      throw new Error("expected resent attachment");
    }
    const backAtt = backAtts[0];
    if (
      typeof backAtt !== "object" ||
      backAtt === null ||
      !("part" in backAtt) ||
      typeof backAtt.part !== "string"
    ) {
      throw new Error("expected resent part path");
    }
    const backPart = await readAlpha(
      {
        id: "back-part",
        name: "mail_read",
        arguments: { ref: back, parts: backAtt.part },
      },
      signal,
    );
    expect(backPart.content).toMatchObject({
      encoding: "utf-8",
      content: "café",
    });
  });

  test("empty attachments round-trip at size zero", async () => {
    const { readPart } = await exchange([
      { name: "empty.txt", contentType: "text/plain", content: "" },
      { name: "empty.png", contentType: "image/png", content: "" },
    ]);

    expect(await readPart("full")).toMatchObject({
      attachments: [
        { name: "empty.txt", size: 0, part: "1.2" },
        { name: "empty.png", size: 0, part: "1.3" },
      ],
    });
    expect(await readPart("1.2")).toMatchObject({
      encoding: "utf-8",
      content: "",
    });
    expect(await readPart("1.3")).toMatchObject({
      encoding: "base64",
      content: "",
    });
  });

  test("a structured mail with attachments fails the send and delivers nothing", async () => {
    const { alpha, beta } = await createMailboxes();
    const send = makeMailSendHandler(alpha);

    const result = await send(
      {
        id: "structured",
        name: "mail_send",
        arguments: {
          to: "beta@test.interchange",
          type: "offering.request",
          payload: { offeringId: "code-review" },
          attachments: [
            { name: "notes.txt", contentType: "text/plain", content: "hi" },
          ],
        },
      },
      signal,
    );

    expect(result.isError).toBe(true);
    if (typeof result.content === "string")
      throw new Error("expected object content");
    expect(result.content["code"]).toBe("send_failed");
    expect(await beta.search("INBOX", {})).toHaveLength(0);
  });

  test("text that is not valid UTF-8 comes back as base64, not mangled", async () => {
    const latin1 = base64Encode(new Uint8Array([0x63, 0x61, 0x66, 0xe9]));
    const { readPart } = await exchange([
      {
        name: "latin1.txt",
        contentType: "text/plain",
        content: latin1,
        encoding: "base64",
      },
    ]);

    expect(await readPart("1.2")).toMatchObject({
      encoding: "base64",
      content: latin1,
    });
  });
});

function htmlSiblingPdfMessage(): Uint8Array {
  // Conversation shape plus an extra inline text/html sibling. The PDF is
  // IMAP part 1.3; advertising it as 1.2 (attachment index + 2) fetches HTML.
  const body = [
    "From: alpha@test.interchange",
    "To: beta@test.interchange",
    "Subject: html sibling",
    "Message-ID: <html-sib@test.interchange>",
    "Date: Thu, 01 Jan 2026 00:00:00 +0000",
    "MIME-Version: 1.0",
    "Interchange-Type: conversation.message",
    `Content-Type: multipart/signed; protocol="application/pgp-signature"; micalg=pgp-sha512; boundary="outer"`,
    "",
    "--outer",
    `Content-Type: multipart/mixed; boundary="inner"`,
    "",
    "--inner",
    "Content-Type: text/plain; charset=utf-8",
    "Content-Transfer-Encoding: 7bit",
    "",
    "hello",
    "--inner",
    "Content-Type: text/html; charset=utf-8",
    "Content-Disposition: inline",
    "",
    "<p>hello</p>",
    "--inner",
    "Content-Type: application/pdf",
    "Content-Transfer-Encoding: base64",
    `Content-Disposition: attachment; filename="report.pdf"`,
    "",
    "JVBERi0=",
    "--inner--",
    "--outer",
    "Content-Type: application/pgp-signature",
    "",
    "FAKE-SIGNATURE",
    "--outer--",
    "",
  ].join("\r\n");
  return new TextEncoder().encode(body);
}

describe("mail_read attachment paths on inbound MIME", () => {
  test("an extra html sibling does not advertise the PDF as 1.2", async () => {
    const hub = createInMemoryTransport();
    hub.register(
      "beta@test.interchange",
      createEd25519Crypto(await generateKeyPair()),
    );
    hub.deliver("beta@test.interchange", htmlSiblingPdfMessage());
    const beta = hub.getTransportFor("beta@test.interchange");
    const read = makeMailReadHandler(beta);
    const [ref] = await beta.search("INBOX", {});
    if (ref === undefined) throw new Error("expected a delivered message");

    const listed = await read(
      { id: "full", name: "mail_read", arguments: { ref, parts: "full" } },
      signal,
    );
    expect(listed.isError).toBeUndefined();
    if (typeof listed.content === "string") {
      throw new Error("expected object content");
    }
    const attachments = listed.content["attachments"];
    if (!Array.isArray(attachments) || attachments[0] === undefined) {
      throw new Error("expected one listed attachment");
    }
    const advertised = attachments[0];
    if (
      typeof advertised !== "object" ||
      advertised === null ||
      !("part" in advertised) ||
      typeof advertised.part !== "string"
    ) {
      throw new Error("expected listed attachment with a part path");
    }
    expect(advertised).toEqual({
      name: "report.pdf",
      contentType: "application/pdf",
      size: 5,
      part: "1.3",
    });

    const fetched = await read(
      {
        id: "pdf",
        name: "mail_read",
        arguments: { ref, parts: advertised.part },
      },
      signal,
    );
    expect(fetched.isError).toBeUndefined();
    expect(fetched.content).toMatchObject({
      encoding: "base64",
      content: "JVBERi0=",
    });
  });
});
