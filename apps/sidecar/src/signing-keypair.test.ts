import { describe, test, expect } from "bun:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { loadOrMintSidecarKeypair } from "./signing-keypair";

async function makeTempDir(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "sidecar-signing-keypair-"));
}

const PRIVATE_KEY_PATH = (dir: string) => path.join(dir, "ed25519.private");
const PUBLIC_KEY_PATH = (dir: string) => path.join(dir, "ed25519.public");

describe("loadOrMintSidecarKeypair", () => {
  test("mints a fresh keypair on first boot and persists both files", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    const minted = await loadOrMintSidecarKeypair(signingDir);

    expect(minted.privateKey).toHaveLength(32);
    expect(minted.publicKey).toHaveLength(32);
    expect(Array.from(await fs.readFile(PRIVATE_KEY_PATH(signingDir)))).toEqual(
      Array.from(minted.privateKey),
    );
    expect(Array.from(await fs.readFile(PUBLIC_KEY_PATH(signingDir)))).toEqual(
      Array.from(minted.publicKey),
    );
  });

  test("reloads a consistent keypair and derives the public key from the seed", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    const minted = await loadOrMintSidecarKeypair(signingDir);
    const reloaded = await loadOrMintSidecarKeypair(signingDir);

    expect(reloaded.privateKey).toEqual(minted.privateKey);
    expect(reloaded.publicKey).toEqual(minted.publicKey);
  });

  test("rejects a public key file that does not match the seed", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    const minted = await loadOrMintSidecarKeypair(signingDir);

    const tampered = new Uint8Array(minted.publicKey);
    const firstByte = tampered[0];
    if (firstByte === undefined) {
      throw new Error("expected a 32-byte public key");
    }
    tampered[0] = firstByte ^ 0xff;
    await fs.writeFile(PUBLIC_KEY_PATH(signingDir), tampered);

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /does not match the key derived from the seed/,
    );
  });

  test("rejects a truncated public key file", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    await loadOrMintSidecarKeypair(signingDir);
    await fs.writeFile(PUBLIC_KEY_PATH(signingDir), new Uint8Array(5));

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /does not match the key derived from the seed/,
    );
  });

  test("rejects an empty public key file", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    await loadOrMintSidecarKeypair(signingDir);
    await fs.writeFile(PUBLIC_KEY_PATH(signingDir), new Uint8Array(0));

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /does not match the key derived from the seed/,
    );
  });

  test("rejects a seed that is not a valid Ed25519 seed", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    await loadOrMintSidecarKeypair(signingDir);

    await fs.writeFile(PRIVATE_KEY_PATH(signingDir), new Uint8Array(16));

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /is not a valid Ed25519 seed/,
    );
  });

  test("rejects a length-valid seed that derives to a different public key", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    await loadOrMintSidecarKeypair(signingDir);

    const otherDir = path.join(await makeTempDir(), ".sidecar-signing");
    const other = await loadOrMintSidecarKeypair(otherDir);
    await fs.writeFile(PRIVATE_KEY_PATH(signingDir), other.privateKey);

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /does not match the key derived from the seed/,
    );
  });

  test("rejects a partial keypair where only the private key is present", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    await loadOrMintSidecarKeypair(signingDir);
    await fs.rm(PUBLIC_KEY_PATH(signingDir));

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /is partial/,
    );
  });

  test("rejects a partial keypair where only the public key is present", async () => {
    const dataDir = await makeTempDir();
    const signingDir = path.join(dataDir, ".sidecar-signing");

    await loadOrMintSidecarKeypair(signingDir);
    await fs.rm(PRIVATE_KEY_PATH(signingDir));

    await expect(loadOrMintSidecarKeypair(signingDir)).rejects.toThrow(
      /is partial/,
    );
  });
});
