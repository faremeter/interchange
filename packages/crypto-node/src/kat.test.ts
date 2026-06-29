import { describe, test, expect } from "bun:test";
import { hexDecode, hexEncode } from "@intx/types";

import { asArrayBuffer, signEd25519, verifyEd25519 } from "./keys";
import {
  buildSignatureHashInput,
  buildSignaturePacket,
  armorEncode,
} from "./pgp";
import { createDetachedSignature } from "./sign";
import { verifyDetachedSignature } from "./verify";
import { createSSHSignature, verifySSHSignature } from "./sshsig";
import * as kat from "./kat-vectors";

// These vectors were captured from the pre-port node:crypto implementation
// (see kat-vectors.ts header). Reproducing them byte-for-byte with the Web
// Crypto code is the wire-compatibility guarantee for the migration.

const seed = hexDecode(kat.SEED_HEX);
const publicKey = hexDecode(kat.PUBLIC_KEY_HEX);

describe("known-answer vectors (wire compatibility)", () => {
  test("raw Ed25519 signature matches the fixture", async () => {
    const sig = await signEd25519(
      seed,
      new TextEncoder().encode(kat.RAW_MESSAGE),
    );
    expect(hexEncode(sig)).toBe(kat.RAW_SIGNATURE_HEX);
  });

  test("verify accepts the committed pre-port raw signature", async () => {
    const ok = await verifyEd25519(
      new TextEncoder().encode(kat.RAW_MESSAGE),
      hexDecode(kat.RAW_SIGNATURE_HEX),
      publicKey,
    );
    expect(ok).toBe(true);
  });

  test("assembled PGP packet matches the fixture", async () => {
    const content = new TextEncoder().encode(kat.PGP_CONTENT);
    const { hashInput, header } = buildSignatureHashInput(
      content,
      kat.CREATION_TIME,
    );
    const rawSig = await signEd25519(seed, hashInput);
    const digest = new Uint8Array(
      await crypto.subtle.digest("SHA-512", asArrayBuffer(hashInput)),
    );
    const armored = armorEncode(
      buildSignaturePacket(header, rawSig, digest.subarray(0, 2)),
    );
    expect(armored).toBe(kat.PGP_ARMORED);
  });

  test("verifyDetachedSignature accepts the committed PGP fixture", async () => {
    const ok = await verifyDetachedSignature(
      new TextEncoder().encode(kat.PGP_CONTENT),
      new TextEncoder().encode(kat.PGP_ARMORED),
      publicKey,
    );
    expect(ok).toBe(true);
  });

  test("SSHSIG armor matches the fixture", async () => {
    const armored = await createSSHSignature(kat.SSH_PAYLOAD, seed, publicKey);
    expect(armored).toBe(kat.SSH_ARMORED);
  });

  test("verifySSHSignature accepts the committed SSHSIG fixture", async () => {
    const ok = await verifySSHSignature(
      kat.SSH_PAYLOAD,
      kat.SSH_ARMORED,
      publicKey,
    );
    expect(ok).toBe(true);
  });

  test("createDetachedSignature round-trips without asserting bytes", async () => {
    const content = new TextEncoder().encode(kat.PGP_CONTENT);
    const sig = await createDetachedSignature(content, seed);
    const ok = await verifyDetachedSignature(content, sig, publicKey);
    expect(ok).toBe(true);
  });
});
