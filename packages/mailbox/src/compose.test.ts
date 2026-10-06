import { describe, expect, test } from "bun:test";
import { createEd25519Crypto, generateKeyPair } from "@intx/crypto";
import { extractAttachments } from "@intx/mime";

import { composeOutbound } from "./compose";
import { verifyMimeSignature } from "./verify-signature";

async function senderCrypto() {
  return createEd25519Crypto(await generateKeyPair());
}

describe("composeOutbound", () => {
  test("a conversation attachment is base64 inside the signed part", async () => {
    const crypto = await senderCrypto();
    const data = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0xff]);
    const composed = await composeOutbound(
      "alpha@test.interchange",
      {
        to: "beta@test.interchange",
        type: "conversation.message",
        content: "with image",
        attachments: [{ name: "shot.png", contentType: "image/png", data }],
      },
      crypto,
    );

    const wire = new TextDecoder().decode(composed.rawBytes);
    const encoded = Buffer.from(data).toString("base64");
    const at = wire.indexOf(encoded);
    expect(at).toBeGreaterThan(0);
    expect(wire).toContain("Content-Transfer-Encoding: base64");

    expect(extractAttachments(composed.rawBytes)).toEqual([
      { name: "shot.png", contentType: "image/png", data },
    ]);
    expect(
      await verifyMimeSignature(composed.rawBytes, crypto.getPublicKey()),
    ).toBe("valid");

    // The signature covers the encoded attachment. One flipped octet in that
    // base64 is a different signed part.
    const tampered = new Uint8Array(composed.rawBytes);
    const octet = tampered[at];
    if (octet === undefined) throw new Error("attachment bytes missing");
    tampered[at] = octet ^ 0x01;
    expect(await verifyMimeSignature(tampered, crypto.getPublicKey())).toBe(
      "invalid",
    );
  });

  test("a structured message cannot carry attachments", async () => {
    const crypto = await senderCrypto();
    await expect(
      composeOutbound(
        "alpha@test.interchange",
        {
          to: "beta@test.interchange",
          type: "offering.request",
          payload: { offeringId: "code-review" },
          attachments: [
            {
              name: "shot.png",
              contentType: "image/png",
              data: new Uint8Array([1]),
            },
          ],
        },
        crypto,
      ),
    ).rejects.toThrow("Structured messages must not carry attachments");
  });
});
