import {
  sign as nodeSign,
  verify as nodeVerify,
  createHash,
} from "node:crypto";
import { importPrivateKeyBytes, importPublicKeyBytes } from "./keys";

const MAGIC_PREAMBLE = new TextEncoder().encode("SSHSIG");
const SIG_VERSION = 1;
const NAMESPACE = "git";
const HASH_ALGORITHM = "sha512";

function writeUint32(value: number): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer);
  view.setUint32(0, value);
  return buf;
}

function writeString(data: Uint8Array): Uint8Array {
  const len = writeUint32(data.length);
  const result = new Uint8Array(4 + data.length);
  result.set(len, 0);
  result.set(data, 4);
  return result;
}

function writeStringUtf8(text: string): Uint8Array {
  return writeString(new TextEncoder().encode(text));
}

function readUint32(buf: Uint8Array, offset: number): [number, number] {
  if (offset + 4 > buf.length) {
    throw new Error("SSHSIG: truncated uint32");
  }
  const view = new DataView(buf.buffer, buf.byteOffset + offset, 4);
  return [view.getUint32(0), offset + 4];
}

function readString(buf: Uint8Array, offset: number): [Uint8Array, number] {
  const [len, afterLen] = readUint32(buf, offset);
  if (afterLen + len > buf.length) {
    throw new Error("SSHSIG: truncated string field");
  }
  return [buf.slice(afterLen, afterLen + len), afterLen + len];
}

function buildPublicKeyBlob(publicKeyBytes: Uint8Array): Uint8Array {
  const keyType = writeStringUtf8("ssh-ed25519");
  const keyData = writeString(publicKeyBytes);
  const blob = new Uint8Array(keyType.length + keyData.length);
  blob.set(keyType, 0);
  blob.set(keyData, keyType.length);
  return blob;
}

function buildSignatureBlob(rawSignature: Uint8Array): Uint8Array {
  const sigType = writeStringUtf8("ssh-ed25519");
  const sigData = writeString(rawSignature);
  const blob = new Uint8Array(sigType.length + sigData.length);
  blob.set(sigType, 0);
  blob.set(sigData, sigType.length);
  return blob;
}

function buildSignedData(messageHash: Uint8Array): Uint8Array {
  // The signed data does NOT include the version or public key — those are
  // envelope-only fields. Only magic + namespace + reserved + hash_alg + H(m).
  const parts = [
    MAGIC_PREAMBLE,
    writeStringUtf8(NAMESPACE),
    writeStringUtf8(""),
    writeStringUtf8(HASH_ALGORITHM),
    writeString(messageHash),
  ];
  const totalLen = parts.reduce((sum, p) => sum + p.length, 0);
  const result = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.length;
  }
  return result;
}

function armorSshSig(binaryBlob: Uint8Array): string {
  const b64 = Buffer.from(binaryBlob).toString("base64");
  const lines: string[] = ["-----BEGIN SSH SIGNATURE-----"];
  for (let i = 0; i < b64.length; i += 70) {
    lines.push(b64.slice(i, i + 70));
  }
  lines.push("-----END SSH SIGNATURE-----");
  return lines.join("\n");
}

function dearmorSshSig(armored: string): Uint8Array {
  const beginMarker = "-----BEGIN SSH SIGNATURE-----";
  const endMarker = "-----END SSH SIGNATURE-----";
  const beginIdx = armored.indexOf(beginMarker);
  const endIdx = armored.indexOf(endMarker);
  if (beginIdx === -1 || endIdx === -1) {
    throw new Error("SSHSIG: missing armor markers");
  }
  const body = armored.slice(beginIdx + beginMarker.length, endIdx).trim();
  const b64 = body.replace(/\s+/g, "");
  return new Uint8Array(Buffer.from(b64, "base64"));
}

/**
 * Produce an SSH signature (SSHSIG format) over a commit payload.
 *
 * The returned string is the PEM-armored signature suitable for embedding
 * in a git commit's gpgsig header. Compatible with `git verify-commit`
 * when the allowed_signers file lists the corresponding public key.
 */
export function createSshSignature(
  payload: string,
  privateKeyBytes: Uint8Array,
  publicKeyBytes: Uint8Array,
): string {
  const payloadBytes = new TextEncoder().encode(payload);
  const messageHash = createHash("sha512").update(payloadBytes).digest();

  const publicKeyBlob = buildPublicKeyBlob(publicKeyBytes);
  const signedData = buildSignedData(new Uint8Array(messageHash));

  const privateKey = importPrivateKeyBytes(privateKeyBytes);
  const rawSignature = nodeSign(null, signedData, privateKey);

  const signatureBlob = buildSignatureBlob(new Uint8Array(rawSignature));

  const outputParts = [
    MAGIC_PREAMBLE,
    writeUint32(SIG_VERSION),
    writeString(publicKeyBlob),
    writeStringUtf8(NAMESPACE),
    writeStringUtf8(""),
    writeStringUtf8(HASH_ALGORITHM),
    writeString(signatureBlob),
  ];
  const totalLen = outputParts.reduce((sum, p) => sum + p.length, 0);
  const output = new Uint8Array(totalLen);
  let offset = 0;
  for (const part of outputParts) {
    output.set(part, offset);
    offset += part.length;
  }

  return armorSshSig(output);
}

/**
 * Verify an SSH signature (SSHSIG format) over a commit payload.
 *
 * Returns true if the signature is valid for the given public key.
 * Throws on malformed input (truncated data, wrong magic, unsupported
 * version or algorithm). Returns false only when the signature is
 * structurally valid but cryptographically incorrect.
 */
export function verifySshSignature(
  payload: string,
  signature: string,
  publicKeyBytes: Uint8Array,
): boolean {
  const blob = dearmorSshSig(signature);

  let offset = 0;

  if (blob.length < 6) {
    throw new Error("SSHSIG: blob too short for magic");
  }
  const magic = new TextDecoder().decode(blob.slice(0, 6));
  if (magic !== "SSHSIG") {
    throw new Error("SSHSIG: invalid magic");
  }
  offset = 6;

  const [version, afterVersion] = readUint32(blob, offset);
  if (version !== 1) {
    throw new Error(`SSHSIG: unsupported version ${version}`);
  }
  offset = afterVersion;

  const [embeddedPubKey, afterPubKey] = readString(blob, offset);
  offset = afterPubKey;

  const [namespaceBytes, afterNamespace] = readString(blob, offset);
  const namespace = new TextDecoder().decode(namespaceBytes);
  if (namespace !== NAMESPACE) {
    throw new Error(
      `SSHSIG: namespace mismatch: expected "git", got ${JSON.stringify(namespace)}`,
    );
  }
  offset = afterNamespace;

  // reserved
  const [, afterReserved] = readString(blob, offset);
  offset = afterReserved;

  const [hashAlgBytes, afterHashAlg] = readString(blob, offset);
  const hashAlg = new TextDecoder().decode(hashAlgBytes);
  if (hashAlg !== HASH_ALGORITHM) {
    throw new Error(
      `SSHSIG: unsupported hash algorithm ${JSON.stringify(hashAlg)}`,
    );
  }
  offset = afterHashAlg;

  const [sigBlob, afterSig] = readString(blob, offset);
  if (afterSig !== blob.length) {
    throw new Error("SSHSIG: trailing data after signature blob");
  }

  // Parse the signature blob: string("ssh-ed25519") || string(raw_sig)
  let sigOffset = 0;
  const [sigTypeBytes, afterSigType] = readString(sigBlob, sigOffset);
  const sigType = new TextDecoder().decode(sigTypeBytes);
  if (sigType !== "ssh-ed25519") {
    throw new Error(
      `SSHSIG: unsupported signature type ${JSON.stringify(sigType)}`,
    );
  }
  sigOffset = afterSigType;

  const [rawSig, afterRawSig] = readString(sigBlob, sigOffset);
  if (afterRawSig !== sigBlob.length) {
    throw new Error("SSHSIG: trailing data in signature blob");
  }
  if (rawSig.length !== 64) {
    throw new Error(`SSHSIG: expected 64-byte signature, got ${rawSig.length}`);
  }

  // Extract the public key from the embedded blob for verification
  // Parse: string("ssh-ed25519") || string(32-byte-key)
  let pkOffset = 0;
  const [pkTypeBytes, afterPkType] = readString(embeddedPubKey, pkOffset);
  const pkType = new TextDecoder().decode(pkTypeBytes);
  if (pkType !== "ssh-ed25519") {
    throw new Error(`SSHSIG: unsupported key type ${JSON.stringify(pkType)}`);
  }
  pkOffset = afterPkType;

  const [embeddedKey, afterEmbeddedKey] = readString(embeddedPubKey, pkOffset);
  if (afterEmbeddedKey !== embeddedPubKey.length) {
    throw new Error("SSHSIG: trailing data in public key blob");
  }
  if (embeddedKey.length !== 32) {
    throw new Error(
      `SSHSIG: expected 32-byte public key, got ${embeddedKey.length}`,
    );
  }

  // Verify the embedded key matches the expected key
  if (publicKeyBytes.length !== 32) {
    throw new Error(
      `Ed25519 public key must be 32 bytes, got ${publicKeyBytes.length}`,
    );
  }
  for (let i = 0; i < 32; i++) {
    if (embeddedKey[i] !== publicKeyBytes[i]) {
      return false;
    }
  }

  // Reconstruct signed data and verify
  const payloadBytes = new TextEncoder().encode(payload);
  const messageHash = createHash("sha512").update(payloadBytes).digest();
  const signedData = buildSignedData(new Uint8Array(messageHash));

  const pubKey = importPublicKeyBytes(publicKeyBytes);
  return nodeVerify(null, signedData, pubKey, rawSig);
}
