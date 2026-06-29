// Generator for kat-vectors.ts.
//
// Run with: bun run packages/crypto/src/kat-vectors.gen.ts
//
// Recomputes the known-answer vectors from this package's own Web Crypto
// implementation and rewrites kat-vectors.ts. The committed vectors were
// first captured from the pre-port node:crypto implementation at commit
// 76f4c96e; because Ed25519 is deterministic (RFC 8032) and the PKCS#8 /
// SPKI DER framing and OpenPGP / SSHSIG assembly are unchanged across the
// port, the bytes this generator emits are byte-identical to that capture.
//
// The fixed public key below was derived once from the fixed seed via the
// pre-port code (Web Crypto cannot export the public half of an imported
// private key); it is carried as a fixed input so the generator stays
// self-contained.

import { hexDecode, hexEncode } from "@intx/types";
import { asArrayBuffer, signEd25519 } from "./keys";
import {
  buildSignatureHashInput,
  buildSignaturePacket,
  armorEncode,
} from "./pgp";
import { createSSHSignature } from "./sshsig";

const SEED_HEX =
  "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
const PUBLIC_KEY_HEX =
  "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8";
const RAW_MESSAGE = "interchange ed25519 KAT message";
const CREATION_TIME = 1700000000;
const PGP_CONTENT = "KAT PGP content\r\n";
const SSH_PAYLOAD = "tree 0123\nauthor KAT <k@k> 1700000000 +0000\n\nkat ssh\n";

const seed = hexDecode(SEED_HEX);
const publicKey = hexDecode(PUBLIC_KEY_HEX);

const rawSignatureHex = hexEncode(
  await signEd25519(seed, new TextEncoder().encode(RAW_MESSAGE)),
);

const { hashInput, header } = buildSignatureHashInput(
  new TextEncoder().encode(PGP_CONTENT),
  CREATION_TIME,
);
const pgpRawSig = await signEd25519(seed, hashInput);
const pgpDigest = new Uint8Array(
  await crypto.subtle.digest("SHA-512", asArrayBuffer(hashInput)),
);
const pgpArmored = armorEncode(
  buildSignaturePacket(header, pgpRawSig, pgpDigest.subarray(0, 2)),
);

const sshArmored = await createSSHSignature(SSH_PAYLOAD, seed, publicKey);

const lines = [
  "// Known-answer test vectors for the Ed25519 / OpenPGP / SSHSIG path.",
  "//",
  "// GENERATED FILE — do not edit by hand. Regenerate with:",
  "//   bun run packages/crypto/src/kat-vectors.gen.ts",
  "//",
  "// The expected outputs were first captured from the pre-port node:crypto",
  "// implementation at commit 76f4c96e and are reproduced by the Web Crypto",
  "// port. Ed25519 is deterministic (RFC 8032) and the DER framing and",
  "// OpenPGP / SSHSIG assembly are unchanged across the port, so the bytes",
  "// are identical either way. kat.test.ts asserts the package reproduces",
  "// these vectors — the wire-compatibility guarantee.",
  "",
  `export const SEED_HEX = ${JSON.stringify(SEED_HEX)};`,
  `export const PUBLIC_KEY_HEX = ${JSON.stringify(PUBLIC_KEY_HEX)};`,
  `export const RAW_MESSAGE = ${JSON.stringify(RAW_MESSAGE)};`,
  `export const RAW_SIGNATURE_HEX = ${JSON.stringify(rawSignatureHex)};`,
  `export const CREATION_TIME = ${JSON.stringify(CREATION_TIME)};`,
  `export const PGP_CONTENT = ${JSON.stringify(PGP_CONTENT)};`,
  `export const PGP_ARMORED = ${JSON.stringify(pgpArmored)};`,
  `export const SSH_PAYLOAD = ${JSON.stringify(SSH_PAYLOAD)};`,
  `export const SSH_ARMORED = ${JSON.stringify(sshArmored)};`,
  "",
];

await Bun.write(new URL("./kat-vectors.ts", import.meta.url), lines.join("\n"));
