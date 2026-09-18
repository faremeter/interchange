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
