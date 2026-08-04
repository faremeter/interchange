import { describe, test, expect } from "bun:test";
import { base64Encode, base64Decode } from "@intx/types";

import { aeadEncrypt, aeadDecrypt } from "./aead";

const KEY_A = new Uint8Array(32).fill(1);
const KEY_B = new Uint8Array(32).fill(2);

describe("aeadEncrypt / aeadDecrypt", () => {
  test("round-trips a value under the same key and aad", async () => {
    const blob = await aeadEncrypt(KEY_A, "sk-secret-123", "cred_x:secret");
    expect(await aeadDecrypt(KEY_A, blob, "cred_x:secret")).toBe(
      "sk-secret-123",
    );
  });

  test("emits the enc:aead:<keyid> scheme format", async () => {
    const blob = await aeadEncrypt(KEY_A, "x", "aad");
    expect(blob).toStartWith("enc:aead:");
    const parts = blob.split(":");
    expect(parts[0]).toBe("enc");
    expect(parts[1]).toBe("aead");
    expect(parts[2]).toMatch(/^[0-9a-f]{8}$/); // 4-byte key id, hex
    expect((parts[3] ?? "").length).toBeGreaterThan(0);
  });

  test("a fresh iv makes two encryptions of the same input differ", async () => {
    const a = await aeadEncrypt(KEY_A, "same", "aad");
    const b = await aeadEncrypt(KEY_A, "same", "aad");
    expect(a).not.toBe(b);
    expect(await aeadDecrypt(KEY_A, a, "aad")).toBe("same");
    expect(await aeadDecrypt(KEY_A, b, "aad")).toBe("same");
  });

  test("refuses to treat a non-ciphertext value as plaintext", async () => {
    await expect(aeadDecrypt(KEY_A, "sk-plain-text", "aad")).rejects.toThrow(
      /not an enc:aead/,
    );
  });

  test("rejects a ciphertext produced under a different key (key-id mismatch)", async () => {
    // A different key yields a different key id, so this is caught by the key-id
    // check before AES-GCM; GCM's own wrong-key rejection is exercised by the
    // tamper and aad-mismatch cases below (same key id, GCM rejects).
    const blob = await aeadEncrypt(KEY_A, "x", "aad");
    await expect(aeadDecrypt(KEY_B, blob, "aad")).rejects.toThrow(
      /key id .* does not match/,
    );
  });

  test("round-trips an empty plaintext (the iv+tag length boundary)", async () => {
    const blob = await aeadEncrypt(KEY_A, "", "aad");
    expect(await aeadDecrypt(KEY_A, blob, "aad")).toBe("");
  });

  test("rejects malformed ciphertexts cleanly", async () => {
    // Right prefix, no key-id separator.
    await expect(
      aeadDecrypt(KEY_A, "enc:aead:deadbeef", "aad"),
    ).rejects.toThrow();
    // Correct key id but empty / too-short body.
    const good = await aeadEncrypt(KEY_A, "x", "aad");
    const keyid = good.split(":")[2];
    await expect(
      aeadDecrypt(KEY_A, `enc:aead:${keyid}:`, "aad"),
    ).rejects.toThrow();
    await expect(
      aeadDecrypt(KEY_A, `enc:aead:${keyid}:QQ`, "aad"),
    ).rejects.toThrow(/too short/);
  });

  test("rejects an aad that differs from the one used to encrypt", async () => {
    const blob = await aeadEncrypt(KEY_A, "x", "cred_a:secret");
    await expect(aeadDecrypt(KEY_A, blob, "cred_b:secret")).rejects.toThrow();
  });

  test("rejects a tampered ciphertext", async () => {
    const blob = await aeadEncrypt(KEY_A, "x", "aad");
    // Flip a character in the base64 body (well past the enc:aead:<keyid>: header).
    const idx = blob.length - 2;
    const flipped =
      blob.slice(0, idx) +
      (blob[idx] === "A" ? "B" : "A") +
      blob.slice(idx + 1);
    await expect(aeadDecrypt(KEY_A, flipped, "aad")).rejects.toThrow();
  });

  test("rejects a wrong-length key on encrypt and decrypt", async () => {
    await expect(aeadEncrypt(new Uint8Array(16), "x", "aad")).rejects.toThrow(
      /32 bytes/,
    );
    const blob = await aeadEncrypt(KEY_A, "x", "aad");
    await expect(aeadDecrypt(new Uint8Array(16), blob, "aad")).rejects.toThrow(
      /32 bytes/,
    );
  });

  test("round-trips an empty aad, and a non-empty aad then rejects", async () => {
    const blob = await aeadEncrypt(KEY_A, "v", "");
    expect(await aeadDecrypt(KEY_A, blob, "")).toBe("v");
    await expect(aeadDecrypt(KEY_A, blob, "x")).rejects.toThrow();
  });

  test("a forged minimum-length body under the real key id still fails GCM", async () => {
    // 28 zero bytes (a 12-byte iv + 16-byte tag, empty ciphertext) clears the
    // length guard and carries the configured key's own key id, so nothing but
    // GCM authentication is left to reject it -- and it must.
    const real = await aeadEncrypt(KEY_A, "x", "aad");
    const keyid = real.split(":")[2];
    const forged = `enc:aead:${keyid}:${base64Encode(new Uint8Array(28))}`;
    await expect(aeadDecrypt(KEY_A, forged, "aad")).rejects.toThrow();
  });

  test("splicing one ciphertext's iv onto another's body breaks authentication", async () => {
    // Same key and key id, so the graft clears the prefix and key-id checks;
    // GCM binds the iv to the tag, so a swapped iv fails authentication rather
    // than yielding a wrong plaintext.
    const a = await aeadEncrypt(KEY_A, "AAAA", "aad");
    const b = await aeadEncrypt(KEY_A, "BBBB", "aad");
    const bodyA = base64Decode(a.slice(a.lastIndexOf(":") + 1));
    const bodyB = base64Decode(b.slice(b.lastIndexOf(":") + 1));
    const spliced = new Uint8Array(bodyB.length);
    spliced.set(bodyA.subarray(0, 12), 0); // iv from A
    spliced.set(bodyB.subarray(12), 12); // ciphertext + tag from B
    const keyid = a.split(":")[2];
    await expect(
      aeadDecrypt(KEY_A, `enc:aead:${keyid}:${base64Encode(spliced)}`, "aad"),
    ).rejects.toThrow();
  });
});
