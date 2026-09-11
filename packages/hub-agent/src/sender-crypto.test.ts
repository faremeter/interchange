import { describe, test, expect } from "bun:test";
import { isRunAddress } from "@intx/types";

import {
  createPublicKeyCrypto,
  createSenderCryptoResolver,
} from "./sender-crypto";
import type { SenderKeyCache } from "./sender-key-cache";

function makeKey(seed: number): Uint8Array {
  const key = new Uint8Array(32);
  for (let i = 0; i < 32; i++) key[i] = (seed + i) & 0xff;
  return key;
}

function stubCache(entries: Record<string, Uint8Array>): SenderKeyCache {
  const map = new Map(Object.entries(entries));
  return {
    get: (address) => map.get(address),
    put: async () => undefined,
    evict: async () => undefined,
    addresses: () => [...map.keys()],
    rotatableAddresses: () => [...map.keys()].filter((a) => !isRunAddress(a)),
  };
}

describe("createPublicKeyCrypto", () => {
  test("reports the wrapped public key", () => {
    const key = makeKey(1);
    expect(createPublicKeyCrypto(key).getPublicKey()).toBe(key);
  });

  test("refuses to sign without a private key", async () => {
    const crypto = createPublicKeyCrypto(makeKey(2));
    await expect(crypto.sign(new Uint8Array([1]))).rejects.toThrow(
      /public-key-only/,
    );
    await expect(crypto.signSSH("payload")).rejects.toThrow(/public-key-only/);
  });

  test("refuses to verify (callers use verifyMimeSignature over the key)", async () => {
    const crypto = createPublicKeyCrypto(makeKey(3));
    await expect(
      crypto.verify(new Uint8Array([1]), new Uint8Array([2]), makeKey(3)),
    ).rejects.toThrow(/public-key-only/);
  });
});

describe("createSenderCryptoResolver", () => {
  test("wraps the cached key for a known sender", () => {
    const key = makeKey(4);
    const resolve = createSenderCryptoResolver(
      stubCache({ "sender@peer.example": key }),
    );
    const crypto = resolve("sender@peer.example");
    expect(crypto).toBeDefined();
    expect(crypto?.getPublicKey()).toBe(key);
  });

  test("returns undefined for a sender with no cached key", () => {
    const resolve = createSenderCryptoResolver(stubCache({}));
    expect(resolve("unknown@peer.example")).toBeUndefined();
  });
});
