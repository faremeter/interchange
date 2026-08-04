import { describe, test, expect } from "bun:test";
import { credentialAad } from "@intx/types";

import { createEnvKeyCredentialCipher } from "./credential-cipher";

const KEY = new Uint8Array(32).fill(7);

describe("createEnvKeyCredentialCipher", () => {
  test("round-trips through the CredentialCipher interface", async () => {
    const cipher = createEnvKeyCredentialCipher(KEY);
    const blob = await cipher.encrypt("sk-abc", "cred_x:secret");
    expect(blob).toStartWith("enc:aead:");
    expect(await cipher.decrypt(blob, "cred_x:secret")).toBe("sk-abc");
  });

  test("throws at construction on a wrong-length key", () => {
    expect(() => createEnvKeyCredentialCipher(new Uint8Array(16))).toThrow(
      /32 bytes/,
    );
  });

  test("enforces the aad binding (a blob cannot be transplanted)", async () => {
    const cipher = createEnvKeyCredentialCipher(KEY);
    const blob = await cipher.encrypt(
      "sk-abc",
      credentialAad("cred_a", "secret"),
    );
    // Same key, different row.
    await expect(
      cipher.decrypt(blob, credentialAad("cred_b", "secret")),
    ).rejects.toThrow();
    // Same key + row, different column.
    await expect(
      cipher.decrypt(blob, credentialAad("cred_a", "refreshSecret")),
    ).rejects.toThrow();
  });

  test("defensively copies the key so a later buffer mutation does not change it", async () => {
    const mutable = new Uint8Array(32).fill(7);
    const cipher = createEnvKeyCredentialCipher(mutable);
    const blob = await cipher.encrypt("x", "aad");
    mutable.fill(9);
    expect(await cipher.decrypt(blob, "aad")).toBe("x");
  });
});

describe("credentialAad injectivity", () => {
  // Injectivity is the property that makes ciphertext transplant impossible:
  // distinct (id, column) pairs must never share an aad. Fuzz it across ids and
  // columns packed with JSON-structural characters -- quotes, backslashes,
  // brackets, commas -- and unicode edge cases. A collision here would mean the
  // binding is broken; this would catch a regression that "simplified" the
  // builder to a naive id:column join or any other non-injective encoding.
  test("no collision across crafted structural and unicode inputs", () => {
    const ids = [
      "a",
      "a,b",
      '","',
      '"]',
      '["credential-secret","a","b"]',
      "cred\\",
      'cred\\"',
      "a b",
      'a","b',
      "]",
      '","secret"]',
      "caf\u00e9", // precomposed e-acute (U+00E9)
      "cafe\u0301", // decomposed e + combining acute -- a distinct string
      "😀", // grinning face
      "\ud83d", // lone high surrogate
      "\ude00", // lone low surrogate
    ];
    const cols = [
      "secret",
      "refreshSecret",
      "clientSecret",
      "s,x",
      '"',
      "]",
      "",
    ];
    const seen = new Map<string, string>();
    for (const id of ids) {
      for (const col of cols) {
        const aad = credentialAad(id, col);
        const prior = seen.get(aad);
        if (prior !== undefined) {
          throw new Error(
            `collision: [${id}|${col}] and [${prior}] both map to ${aad}`,
          );
        }
        seen.set(aad, `${id}|${col}`);
      }
    }
    expect(seen.size).toBe(ids.length * cols.length);
  });

  test("delimiter-ambiguous pairs stay distinct", () => {
    // Each of these collides under a naive id:column join.
    expect(credentialAad("cred:a", "secret")).not.toBe(
      credentialAad("cred", "a:secret"),
    );
    expect(credentialAad("a", "")).not.toBe(credentialAad("", "a"));
    expect(credentialAad('a","b', "c")).not.toBe(credentialAad("a", 'b","c'));
  });

  test("output is deterministic and matches the documented shape", () => {
    expect(credentialAad("cred_x", "secret")).toBe(
      credentialAad("cred_x", "secret"),
    );
    expect(credentialAad("cred_x", "secret")).toBe(
      '["credential-secret","cred_x","secret"]',
    );
  });
});
