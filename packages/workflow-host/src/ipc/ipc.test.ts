import { describe, test, expect } from "bun:test";
import { type } from "arktype";
import fs from "node:fs";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto";
import { hexDecode, hexEncode } from "@intx/types";
import { APPROVAL_SNAPSHOT_MAX_BYTES } from "@intx/types/runtime";

import {
  ControlPayload,
  createControlChannelSender,
  createEventChannelSender,
  decodeEnvelope,
  encodeEnvelope,
  EventPayload,
  FrameEnvelope,
  generateChannelId,
  generateHmacKey,
  MacedEnvelope,
  receiveControlChannel,
  receiveEventChannel,
  signEd25519,
  signHmac,
  SignedEnvelope,
  verifyEd25519,
  verifyHmac,
} from "./index";
import type {
  FrameReader,
  FrameWriter,
  NdjsonReader,
  NdjsonWriter,
} from "./index";

/**
 * Synthetic `childPublicKey` hex used to populate `ready` payloads
 * in tests whose receiver pins a fixed `publicKey` Uint8Array
 * (non-bootstrap mode). The receiver verifies signatures against the
 * key it was constructed with; the `childPublicKey` field is just
 * payload content here and never gets read out as a key. Tests that
 * exercise bootstrap mode supply a real keypair's public half.
 */
const TEST_CHILD_PUBKEY_HEX = "ab".repeat(32);

function createMemoryNdjsonStream(): {
  writer: NdjsonWriter;
  reader: NdjsonReader;
  close: () => void;
  inject: (line: string) => void;
} {
  const buffer: string[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: NdjsonReader = {
    read(): AsyncIterableIterator<string> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  return {
    writer: {
      write(line: string) {
        buffer.push(line.replace(/\n$/, ""));
        wake();
      },
    },
    reader,
    inject(line: string) {
      buffer.push(line);
      wake();
    },
    close() {
      done = true;
      wake();
    },
  };
}

function createMemoryFrameStream(): {
  writer: FrameWriter;
  reader: FrameReader;
  close: () => void;
  inject: (bytes: Uint8Array) => void;
  injectRaw: (bytes: Uint8Array) => void;
} {
  const buffer: Uint8Array[] = [];
  let waiter: (() => void) | null = null;
  let done = false;
  function wake() {
    const w = waiter;
    waiter = null;
    if (w) w();
  }
  const reader: FrameReader = {
    read(): AsyncIterableIterator<Uint8Array> {
      return (async function* () {
        while (true) {
          if (buffer.length > 0) {
            const next = buffer.shift();
            if (next === undefined) {
              throw new Error("buffer shift returned undefined");
            }
            yield next;
            continue;
          }
          if (done) return;
          await new Promise<void>((resolve) => {
            waiter = resolve;
          });
        }
      })();
    },
  };
  return {
    writer: {
      write(bytes: Uint8Array) {
        buffer.push(bytes);
        wake();
      },
    },
    reader,
    inject(bytes: Uint8Array) {
      // Simulate one complete frame arriving on the wire. The event
      // channel is newline-delimited (`createEventChannelSender`
      // terminates every frame with `\n`); the receiver splits on that
      // terminator, so an injected raw frame must carry it too. Callers
      // pass the JSON envelope bytes; the terminator is appended here so
      // the injected bytes form exactly one wire frame.
      const framed = new Uint8Array(bytes.length + 1);
      framed.set(bytes, 0);
      framed[bytes.length] = 0x0a;
      buffer.push(framed);
      wake();
    },
    injectRaw(bytes: Uint8Array) {
      // Push bytes onto the wire verbatim, with no frame terminator
      // appended. Used to simulate the kernel coalescing several frames
      // into one read chunk or splitting one frame across chunks -- the
      // exact byte-stream behaviour the newline framing must survive.
      buffer.push(bytes);
      wake();
    },
    close() {
      done = true;
      wake();
    },
  };
}

describe("FrameEnvelope schema", () => {
  test("round-trips a structured payload", () => {
    const envelope: FrameEnvelope = {
      seq: 1,
      channelId: "abcd",
      payload: {
        type: "ready",
        data: { childPid: 42, childPublicKey: TEST_CHILD_PUBKEY_HEX },
      },
    };
    const bytes = encodeEnvelope(envelope);
    const decoded = decodeEnvelope(bytes);
    expect(decoded).toEqual(envelope);
  });

  test("rejects bytes that are not JSON", () => {
    expect(() => decodeEnvelope(new TextEncoder().encode("not json"))).toThrow(
      /not valid JSON/,
    );
  });

  test("rejects an envelope missing required fields", () => {
    const bytes = new TextEncoder().encode(JSON.stringify({ seq: 1 }));
    expect(() => decodeEnvelope(bytes)).toThrow(/failed validation/);
  });
});

describe("Ed25519 primitives", () => {
  test("sign + verify round-trip", async () => {
    const kp = await generateKeyPair();
    const bytes = new TextEncoder().encode("hello");
    const sig = await signEd25519(bytes, kp.privateKey);
    expect(await verifyEd25519(bytes, sig, kp.publicKey)).toBe(true);
  });

  test("rejects a tampered payload", async () => {
    const kp = await generateKeyPair();
    const bytes = new TextEncoder().encode("hello");
    const sig = await signEd25519(bytes, kp.privateKey);
    const tampered = new TextEncoder().encode("hellO");
    expect(await verifyEd25519(tampered, sig, kp.publicKey)).toBe(false);
  });

  test("rejects an Ed25519 signature of the wrong length", async () => {
    const kp = await generateKeyPair();
    await expect(
      verifyEd25519(new Uint8Array(4), new Uint8Array(63), kp.publicKey),
    ).rejects.toThrow(/signature must be 64 bytes/);
  });
});

describe("HMAC primitives", () => {
  test("sign + verify round-trip", async () => {
    const key = generateHmacKey();
    const bytes = new TextEncoder().encode("hello");
    const tag = await signHmac(bytes, key);
    expect(await verifyHmac(bytes, tag, key)).toBe(true);
  });

  test("rejects a tampered tag", async () => {
    const key = generateHmacKey();
    const bytes = new TextEncoder().encode("hello");
    const tag = await signHmac(bytes, key);
    const first = tag[0];
    if (first === undefined) throw new Error("tag empty");
    tag[0] = first ^ 0x01;
    expect(await verifyHmac(bytes, tag, key)).toBe(false);
  });

  test("rejects an HMAC key of the wrong length", async () => {
    await expect(
      signHmac(new Uint8Array(4), new Uint8Array(16)),
    ).rejects.toThrow(/HMAC key must be 32 bytes/);
  });

  test("rejects every single-bit flip of a valid tag", async () => {
    // The constant-time compare must reject a mismatch in any bit of any
    // byte, not just the first. Sweep all 256 single-bit flips.
    const key = generateHmacKey();
    const bytes = new TextEncoder().encode("hello");
    const tag = await signHmac(bytes, key);
    expect(await verifyHmac(bytes, tag, key)).toBe(true);
    for (let byteIdx = 0; byteIdx < tag.length; byteIdx++) {
      for (let bit = 0; bit < 8; bit++) {
        const flipped = new Uint8Array(tag);
        const original = flipped[byteIdx];
        if (original === undefined) throw new Error("tag too short");
        flipped[byteIdx] = original ^ (1 << bit);
        expect(await verifyHmac(bytes, flipped, key)).toBe(false);
      }
    }
  });
});

describe("channelId minting", () => {
  test("produces 32 hex characters (16 bytes)", () => {
    const id = generateChannelId();
    expect(id).toMatch(/^[0-9a-f]{32}$/);
  });

  test("does not repeat across mintings", () => {
    const ids = new Set<string>();
    for (let i = 0; i < 16; i++) {
      ids.add(generateChannelId());
    }
    expect(ids.size).toBe(16);
  });
});

describe("Control channel", () => {
  test("round-trips a sequence of payloads through Ed25519", async () => {
    const kp = await generateKeyPair();
    const channelId = generateChannelId();
    const stream = createMemoryNdjsonStream();
    const sender = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId,
      writer: stream.writer,
    });
    const crashes: string[] = [];
    const received: ControlPayload[] = [];

    const consumer = (async () => {
      for await (const payload of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        received.push(payload);
        if (received.length === 3) return;
      }
    })();

    await sender.send({
      type: "ready",
      data: { childPid: 42, childPublicKey: TEST_CHILD_PUBKEY_HEX },
    });
    await sender.send({ type: "drain", data: { deadlineMs: 5_000 } });
    await sender.send({
      type: "trigger.fire",
      data: { runId: "r1", messageId: "m1", receivedAt: 100 },
    });

    await consumer;
    stream.close();

    expect(crashes).toEqual([]);
    expect(received).toEqual([
      {
        type: "ready",
        data: { childPid: 42, childPublicKey: TEST_CHILD_PUBKEY_HEX },
      },
      { type: "drain", data: { deadlineMs: 5_000 } },
      {
        type: "trigger.fire",
        data: { runId: "r1", messageId: "m1", receivedAt: 100 },
      },
    ]);
  });

  test("serializes concurrent sends so frames keep seq order", async () => {
    // Signing is async; without the sender's internal lock two concurrent
    // sends could assign seq, suspend on the signer, and write in
    // signature-resolution order. A deferred writer proves the lock: the
    // second send must not reach the writer until the first send's write
    // resolves, and the wire order must be seq 1 then seq 2.
    const kp = await generateKeyPair();
    const channelId = generateChannelId();
    const writes: string[] = [];
    const gates: (() => void)[] = [];
    const writer = {
      write(line: string): Promise<void> {
        writes.push(line);
        return new Promise<void>((resolve) => {
          gates.push(resolve);
        });
      },
    };
    const sender = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId,
      writer,
    });

    const waitForWrites = async (n: number) => {
      for (let i = 0; i < 200; i++) {
        if (writes.length >= n) return;
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      throw new Error(
        `timed out waiting for ${n} writes, got ${writes.length}`,
      );
    };

    // Fire both sends without awaiting either.
    const first = sender.send({ type: "drain", data: { deadlineMs: 1 } });
    const second = sender.send({ type: "drain", data: { deadlineMs: 2 } });

    // After signing settles, only the first send has written; the second
    // is held at the lock behind the first send's unresolved write.
    await waitForWrites(1);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(writes.length).toBe(1);

    // Releasing the first write lets the second send proceed.
    gates[0]?.();
    await waitForWrites(2);
    expect(writes.length).toBe(2);
    gates[1]?.();
    await Promise.all([first, second]);

    const seqs = writes.map((line) => {
      const parsed: unknown = JSON.parse(line);
      const validated = SignedEnvelope(parsed);
      if (validated instanceof type.errors) {
        throw new Error(`unexpected wire shape: ${validated.summary}`);
      }
      return validated.envelope.seq;
    });
    expect(seqs).toEqual([1, 2]);
  });

  test("a rejecting write does not wedge subsequent sends", async () => {
    // The sender releases its serialization lock in a `finally`, so a
    // frame whose write rejects must not stall the frames behind it. The
    // failed send still rejects to its caller; the next send proceeds and
    // the wire order is preserved.
    const kp = await generateKeyPair();
    const channelId = generateChannelId();
    const writes: string[] = [];
    let failNext = true;
    const writer = {
      write(line: string): Promise<void> {
        writes.push(line);
        if (failNext) {
          failNext = false;
          return Promise.reject(new Error("write failed"));
        }
        return Promise.resolve();
      },
    };
    const sender = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId,
      writer,
    });

    const first = sender.send({ type: "drain", data: { deadlineMs: 1 } });
    const second = sender.send({ type: "drain", data: { deadlineMs: 2 } });

    await expect(first).rejects.toThrow(/write failed/);
    await second;
    expect(writes.length).toBe(2);

    const seqs = writes.map((line) => {
      const parsed: unknown = JSON.parse(line);
      const validated = SignedEnvelope(parsed);
      if (validated instanceof type.errors) {
        throw new Error(`unexpected wire shape: ${validated.summary}`);
      }
      return validated.envelope.seq;
    });
    expect(seqs).toEqual([1, 2]);
  });

  test("crashes on a forged frame whose signature does not verify", async () => {
    const kp = await generateKeyPair();
    const stream = createMemoryNdjsonStream();
    const channelId = generateChannelId();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        // no-op; we expect zero successful frames
      }
    })();

    const envelope: FrameEnvelope = {
      seq: 1,
      channelId,
      payload: {
        type: "ready",
        data: { childPid: 1, childPublicKey: TEST_CHILD_PUBKEY_HEX },
      },
    };
    const forged: SignedEnvelope = {
      envelope,
      sig: "00".repeat(64),
    };
    stream.inject(JSON.stringify(forged));
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/signature did not verify/);
  });

  test("crashes on a frame with a non-current channelId", async () => {
    const kp = await generateKeyPair();
    const stream = createMemoryNdjsonStream();
    const currentChannel = generateChannelId();
    const staleChannel = generateChannelId();
    const sender = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId: staleChannel,
      writer: stream.writer,
    });
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId: currentChannel,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        // no-op
      }
    })();

    await sender.send({
      type: "ready",
      data: { childPid: 1, childPublicKey: TEST_CHILD_PUBKEY_HEX },
    });
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/channelId mismatch/);
  });

  test("crashes on an out-of-order seq (replay)", async () => {
    const kp = await generateKeyPair();
    const stream = createMemoryNdjsonStream();
    const channelId = generateChannelId();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        // no-op
      }
    })();

    async function emitSignedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: {
          type: "ready",
          data: { childPid: seq, childPublicKey: TEST_CHILD_PUBKEY_HEX },
        },
      };
      const sig = await signEd25519(encodeEnvelope(envelope), kp.privateKey);
      const signed: SignedEnvelope = { envelope, sig: hexEncode(sig) };
      stream.inject(JSON.stringify(signed));
    }

    await emitSignedFrame(1);
    await emitSignedFrame(2);
    await emitSignedFrame(2);
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/out-of-order seq/);
  });

  test("crashes on a seq gap", async () => {
    const kp = await generateKeyPair();
    const stream = createMemoryNdjsonStream();
    const channelId = generateChannelId();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        // no-op
      }
    })();

    async function emitSignedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: {
          type: "ready",
          data: { childPid: seq, childPublicKey: TEST_CHILD_PUBKEY_HEX },
        },
      };
      const sig = await signEd25519(encodeEnvelope(envelope), kp.privateKey);
      const signed: SignedEnvelope = { envelope, sig: hexEncode(sig) };
      stream.inject(JSON.stringify(signed));
    }

    await emitSignedFrame(1);
    await emitSignedFrame(3);
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/seq gap/);
  });

  test("survives channelId rotation when senders rotate in lockstep", async () => {
    const kp = await generateKeyPair();
    const channelIdA = generateChannelId();
    const channelIdB = generateChannelId();
    expect(channelIdA).not.toBe(channelIdB);

    const streamA = createMemoryNdjsonStream();
    const senderA = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId: channelIdA,
      writer: streamA.writer,
    });
    const receivedA: ControlPayload[] = [];
    const consumerA = (async () => {
      for await (const p of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId: channelIdA,
        reader: streamA.reader,
        onCrash: (r) => {
          throw new Error(r);
        },
      })) {
        receivedA.push(p);
        if (receivedA.length === 1) return;
      }
    })();
    await senderA.send({
      type: "ready",
      data: { childPid: 1, childPublicKey: TEST_CHILD_PUBKEY_HEX },
    });
    await consumerA;
    streamA.close();

    const streamB = createMemoryNdjsonStream();
    const senderB = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId: channelIdB,
      writer: streamB.writer,
    });
    const receivedB: ControlPayload[] = [];
    const consumerB = (async () => {
      for await (const p of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId: channelIdB,
        reader: streamB.reader,
        onCrash: (r) => {
          throw new Error(r);
        },
      })) {
        receivedB.push(p);
        if (receivedB.length === 1) return;
      }
    })();
    await senderB.send({
      type: "ready",
      data: { childPid: 2, childPublicKey: TEST_CHILD_PUBKEY_HEX },
    });
    await consumerB;
    streamB.close();

    expect(receivedA).toEqual([
      {
        type: "ready",
        data: { childPid: 1, childPublicKey: TEST_CHILD_PUBKEY_HEX },
      },
    ]);
    expect(receivedB).toEqual([
      {
        type: "ready",
        data: { childPid: 2, childPublicKey: TEST_CHILD_PUBKEY_HEX },
      },
    ]);
  });

  describe("bootstrap mode (bootstrapFromReady)", () => {
    test("crashes when the first frame is not a ready frame", async () => {
      // The supervisor's upstream receiver opens with no pinned key and
      // must see `ready` first so it can extract the child's public key.
      // A non-`ready` first frame is rejected before any signature check.
      const kp = await generateKeyPair();
      const channelId = generateChannelId();
      const stream = createMemoryNdjsonStream();
      const sender = createControlChannelSender({
        privateKeySeed: kp.privateKey,
        channelId,
        writer: stream.writer,
      });
      const crashes: string[] = [];
      const received: ControlPayload[] = [];

      const consumer = (async () => {
        for await (const payload of receiveControlChannel({
          publicKey: { bootstrapFromReady: true },
          channelId,
          reader: stream.reader,
          onCrash: (reason) => crashes.push(reason),
        })) {
          received.push(payload);
        }
      })();

      await sender.send({ type: "drain", data: { deadlineMs: 5_000 } });
      stream.close();
      await consumer;

      expect(received).toEqual([]);
      expect(crashes.length).toBe(1);
      expect(crashes[0]).toMatch(/expected a ready frame/);
    });

    test("crashes when the ready frame's childPublicKey is malformed hex", async () => {
      // Bootstrap extracts and hex-decodes `childPublicKey` from the
      // ready payload before verifying the frame. A non-hex value crashes
      // the receiver at the decode step.
      const kp = await generateKeyPair();
      const channelId = generateChannelId();
      const stream = createMemoryNdjsonStream();
      const sender = createControlChannelSender({
        privateKeySeed: kp.privateKey,
        channelId,
        writer: stream.writer,
      });
      const crashes: string[] = [];
      const received: ControlPayload[] = [];

      const consumer = (async () => {
        for await (const payload of receiveControlChannel({
          publicKey: { bootstrapFromReady: true },
          channelId,
          reader: stream.reader,
          onCrash: (reason) => crashes.push(reason),
        })) {
          received.push(payload);
        }
      })();

      await sender.send({
        type: "ready",
        data: { childPid: 1, childPublicKey: "zz" },
      });
      stream.close();
      await consumer;

      expect(received).toEqual([]);
      expect(crashes.length).toBe(1);
      expect(crashes[0]).toMatch(/bootstrap childPublicKey decode failed/);
    });

    test("pins the child key so a later frame signed by a different key is rejected", async () => {
      // Bootstrap is not TOFU-per-frame: the key carried on the ready
      // frame pins every subsequent frame. The first frame bootstraps the
      // receiver onto childA's key; a second well-formed, in-order frame
      // signed by a DIFFERENT key (childB) must fail to verify. The
      // signature check runs before the seq checks, so the rejection
      // proves the pin rather than an ordering artifact.
      const childA = await generateKeyPair();
      const childB = await generateKeyPair();
      const channelId = generateChannelId();
      const stream = createMemoryNdjsonStream();
      const senderA = createControlChannelSender({
        privateKeySeed: childA.privateKey,
        channelId,
        writer: stream.writer,
      });
      const crashes: string[] = [];
      const received: ControlPayload[] = [];

      const consumer = (async () => {
        for await (const payload of receiveControlChannel({
          publicKey: { bootstrapFromReady: true },
          channelId,
          reader: stream.reader,
          onCrash: (reason) => crashes.push(reason),
        })) {
          received.push(payload);
        }
      })();

      await senderA.send({
        type: "ready",
        data: { childPid: 1, childPublicKey: hexEncode(childA.publicKey) },
      });

      const envelope: FrameEnvelope = {
        seq: 2,
        channelId,
        payload: { type: "drain", data: { deadlineMs: 1 } },
      };
      const sig = await signEd25519(
        encodeEnvelope(envelope),
        childB.privateKey,
      );
      const signed: SignedEnvelope = { envelope, sig: hexEncode(sig) };
      stream.inject(JSON.stringify(signed));
      stream.close();
      await consumer;

      expect(received).toEqual([
        {
          type: "ready",
          data: { childPid: 1, childPublicKey: hexEncode(childA.publicKey) },
        },
      ]);
      expect(crashes.length).toBe(1);
      expect(crashes[0]).toMatch(/signature did not verify/);
    });
  });
});

describe("Event channel", () => {
  test("round-trips inference events under HMAC", async () => {
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const sender = createEventChannelSender({
      hmacKey: key,
      channelId,
      writer: stream.writer,
    });
    const crashes: string[] = [];
    const received: EventPayload[] = [];

    const consumer = (async () => {
      for await (const payload of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        received.push(payload);
        if (received.length === 2) return;
      }
    })();

    await sender.send({
      type: "inference.start",
      seq: 1,
      data: { model: "test" },
    });
    await sender.send({
      type: "message.run.started",
      seq: 2,
      data: { messageId: "m1", messageRunId: "mr1", receivedAt: 100 },
    });
    stream.close();
    await consumer;

    expect(crashes).toEqual([]);
    expect(received.length).toBe(2);
  });

  test("serializes concurrent sends so frames keep seq order", async () => {
    // signHmac is async, so without the sender's internal lock two
    // concurrent fire-and-forget sends could assign seq, suspend on the
    // HMAC, and write in resolution order. A deferred writer proves the
    // lock: the second send must not reach the writer until the first
    // send's write resolves, and the wire order must be seq 1 then seq 2.
    const hmacKey = generateHmacKey();
    const channelId = generateChannelId();
    const writes: Uint8Array[] = [];
    const gates: (() => void)[] = [];
    const writer = {
      write(bytes: Uint8Array): Promise<void> {
        writes.push(bytes);
        return new Promise<void>((resolve) => {
          gates.push(resolve);
        });
      },
    };
    const sender = createEventChannelSender({ hmacKey, channelId, writer });

    const waitForWrites = async (n: number) => {
      for (let i = 0; i < 200; i++) {
        if (writes.length >= n) return;
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
      throw new Error(
        `timed out waiting for ${n} writes, got ${writes.length}`,
      );
    };

    // Fire both sends without awaiting either.
    const first = sender.send({
      type: "inference.start",
      seq: 1,
      data: { model: "x" },
    });
    const second = sender.send({
      type: "inference.start",
      seq: 2,
      data: { model: "y" },
    });

    // After signing settles, only the first send has written; the second
    // is held at the lock behind the first send's unresolved write.
    await waitForWrites(1);
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(writes.length).toBe(1);

    // Releasing the first write lets the second send proceed.
    gates[0]?.();
    await waitForWrites(2);
    expect(writes.length).toBe(2);
    gates[1]?.();
    await Promise.all([first, second]);

    const seqs = writes.map((bytes) => {
      const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
      const validated = MacedEnvelope(parsed);
      if (validated instanceof type.errors) {
        throw new Error(`unexpected wire shape: ${validated.summary}`);
      }
      return validated.envelope.seq;
    });
    expect(seqs).toEqual([1, 2]);
  });

  test("a rejecting write does not wedge subsequent sends", async () => {
    // The sender releases its serialization lock in a `finally`, so a
    // frame whose write rejects must not stall the frames behind it. The
    // failed send still rejects to its caller; the next send proceeds and
    // the wire order is preserved.
    const hmacKey = generateHmacKey();
    const channelId = generateChannelId();
    const writes: Uint8Array[] = [];
    let failNext = true;
    const writer = {
      write(bytes: Uint8Array): Promise<void> {
        writes.push(bytes);
        if (failNext) {
          failNext = false;
          return Promise.reject(new Error("write failed"));
        }
        return Promise.resolve();
      },
    };
    const sender = createEventChannelSender({ hmacKey, channelId, writer });

    const first = sender.send({
      type: "inference.start",
      seq: 1,
      data: { model: "x" },
    });
    const second = sender.send({
      type: "inference.start",
      seq: 2,
      data: { model: "y" },
    });

    await expect(first).rejects.toThrow(/write failed/);
    await second;
    expect(writes.length).toBe(2);

    const seqs = writes.map((bytes) => {
      const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
      const validated = MacedEnvelope(parsed);
      if (validated instanceof type.errors) {
        throw new Error(`unexpected wire shape: ${validated.summary}`);
      }
      return validated.envelope.seq;
    });
    expect(seqs).toEqual([1, 2]);
  });

  test("crashes on a forged HMAC", async () => {
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        // no-op
      }
    })();

    const envelope: FrameEnvelope = {
      seq: 1,
      channelId,
      payload: { type: "inference.start", seq: 1, data: { model: "x" } },
    };
    const forged: MacedEnvelope = {
      envelope,
      mac: "00".repeat(32),
    };
    stream.inject(new TextEncoder().encode(JSON.stringify(forged)));
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/HMAC did not verify/);
  });

  test("crashes on a non-current channelId", async () => {
    const key = generateHmacKey();
    const currentChannel = generateChannelId();
    const staleChannel = generateChannelId();
    const stream = createMemoryFrameStream();
    const sender = createEventChannelSender({
      hmacKey: key,
      channelId: staleChannel,
      writer: stream.writer,
    });
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId: currentChannel,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        // no-op
      }
    })();

    await sender.send({
      type: "inference.start",
      seq: 1,
      data: { model: "x" },
    });
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/channelId mismatch/);
  });

  test("crashes on out-of-order seq (replay)", async () => {
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        // no-op
      }
    })();

    async function emitMacedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: { type: "inference.start", seq, data: { model: "x" } },
      };
      const tag = await signHmac(encodeEnvelope(envelope), key);
      const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
      stream.inject(new TextEncoder().encode(JSON.stringify(maced)));
    }

    await emitMacedFrame(1);
    await emitMacedFrame(2);
    await emitMacedFrame(2);
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/out-of-order seq/);
  });

  test("crashes on buffer overrun", async () => {
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];

    const limit = 4;
    let consumerStarted = false;
    const _consumer = (async () => {
      consumerStarted = true;
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        bufferLimit: limit,
        onCrash: (r) => crashes.push(r),
      })) {
        // do not pump; let the buffer fill
        await new Promise<void>((r) => setTimeout(r, 1_000_000));
      }
    })();
    expect(consumerStarted).toBe(true);

    async function emitMacedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: { type: "inference.start", seq, data: { model: "x" } },
      };
      const tag = await signHmac(encodeEnvelope(envelope), key);
      const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
      stream.inject(new TextEncoder().encode(JSON.stringify(maced)));
    }

    for (let i = 1; i <= limit + 2; i++) {
      await emitMacedFrame(i);
    }

    // Give the pump a chance to read and crash.
    await new Promise<void>((r) => setTimeout(r, 50));
    stream.close();

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/buffer overrun/);
  });

  test("rejects a control-shaped payload over the event channel", async () => {
    // The two channels carry disjoint payload unions. A "control"
    // payload sneaked over the event wire fails event validation;
    // no event-channel consumer can act on it.
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        // no-op
      }
    })();

    const envelope: FrameEnvelope = {
      seq: 1,
      channelId,
      payload: { type: "drain", data: { deadlineMs: 5_000 } },
    };
    const tag = await signHmac(encodeEnvelope(envelope), key);
    const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
    stream.inject(new TextEncoder().encode(JSON.stringify(maced)));
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/payload failed validation/);
  });

  test("reassembles multiple frames coalesced into one read chunk", async () => {
    // A real step emits a burst of events; the kernel can deliver
    // several newline-delimited frames in a single pipe read. The
    // receiver must split them on the terminator, not parse the whole
    // chunk as one JSON value.
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];
    const received: EventPayload[] = [];

    const consumer = (async () => {
      for await (const payload of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        received.push(payload);
        if (received.length === 3) return;
      }
    })();

    async function macedLine(seq: number): Promise<string> {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: { type: "inference.start", seq, data: { model: "x" } },
      };
      const tag = await signHmac(encodeEnvelope(envelope), key);
      const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
      return `${JSON.stringify(maced)}\n`;
    }

    // Three frames in a single chunk.
    stream.injectRaw(
      new TextEncoder().encode(
        (await macedLine(1)) + (await macedLine(2)) + (await macedLine(3)),
      ),
    );
    stream.close();
    await consumer;

    expect(crashes).toEqual([]);
    expect(received.length).toBe(3);
  });

  test("reassembles one frame split across two read chunks", async () => {
    // The kernel can also split a single frame across reads. The
    // receiver buffers the partial line until its terminator arrives.
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];
    const received: EventPayload[] = [];

    const consumer = (async () => {
      for await (const payload of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        received.push(payload);
        if (received.length === 1) return;
      }
    })();

    const envelope: FrameEnvelope = {
      seq: 1,
      channelId,
      payload: { type: "inference.start", seq: 1, data: { model: "x" } },
    };
    const tag = await signHmac(encodeEnvelope(envelope), key);
    const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
    const wire = new TextEncoder().encode(`${JSON.stringify(maced)}\n`);
    const cut = Math.floor(wire.length / 2);

    stream.injectRaw(wire.subarray(0, cut));
    // The first chunk has no terminator; nothing is delivered yet.
    await new Promise<void>((r) => setTimeout(r, 10));
    expect(received.length).toBe(0);
    stream.injectRaw(wire.subarray(cut));
    stream.close();
    await consumer;

    expect(crashes).toEqual([]);
    expect(received.length).toBe(1);
  });

  test("crashes on a non-JSON line", async () => {
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        // no-op
      }
    })();

    stream.injectRaw(new TextEncoder().encode("not json at all\n"));
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/non-JSON frame/);
  });

  test("crashes on a truncated final frame at EOF", async () => {
    // The sender always terminates a frame with `\n`; unterminated bytes
    // at EOF mean the writer died mid-frame, which must surface rather
    // than silently drop the partial envelope.
    const key = generateHmacKey();
    const channelId = generateChannelId();
    const stream = createMemoryFrameStream();
    const crashes: string[] = [];

    const consumer = (async () => {
      for await (const _ of receiveEventChannel({
        hmacKey: key,
        channelId,
        reader: stream.reader,
        onCrash: (r) => crashes.push(r),
      })) {
        // no-op
      }
    })();

    const envelope: FrameEnvelope = {
      seq: 1,
      channelId,
      payload: { type: "inference.start", seq: 1, data: { model: "x" } },
    };
    const tag = await signHmac(encodeEnvelope(envelope), key);
    const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
    // No trailing newline: a truncated frame.
    stream.injectRaw(new TextEncoder().encode(JSON.stringify(maced)));
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/truncated frame at EOF/);
  });
});

describe("Spawn-time trust-anchor bootstrap", () => {
  // Documents the exact env contract the supervisor's spawn-time
  // construction code path must satisfy. The keys here are the only
  // values that should appear in the env handed to the child; the
  // supervisor's Ed25519 private key MUST NOT appear.
  const REQUIRED_ENV_KEYS = [
    "HOST_PUBKEY",
    "IPC_HMAC_KEY",
    "IPC_CHANNEL_ID",
  ] as const;
  const BANNED_ENV_KEYS = ["HOST_PRIVATE_KEY", "SUPERVISOR_PRIVATE_KEY"];

  test("env carries pubkey + HMAC key + channelId, never the supervisor private key", async () => {
    const kp = await generateKeyPair();
    const hmacKey = generateHmacKey();
    const channelId = generateChannelId();

    const env: Record<string, string> = {
      HOST_PUBKEY: hexEncode(kp.publicKey),
      IPC_HMAC_KEY: hexEncode(hmacKey),
      IPC_CHANNEL_ID: channelId,
    };

    for (const key of REQUIRED_ENV_KEYS) {
      expect(env[key]).toBeTruthy();
    }
    for (const banned of BANNED_ENV_KEYS) {
      expect(env[banned]).toBeUndefined();
    }
    const privateKeyHex = hexEncode(kp.privateKey);
    for (const value of Object.values(env)) {
      expect(value).not.toBe(privateKeyHex);
    }
  });

  test("the child reconstructs working trust anchors from env", async () => {
    const kp = await generateKeyPair();
    const hmacKey = generateHmacKey();
    const channelId = generateChannelId();
    const env: Record<string, string> = {
      HOST_PUBKEY: hexEncode(kp.publicKey),
      IPC_HMAC_KEY: hexEncode(hmacKey),
      IPC_CHANNEL_ID: channelId,
    };

    const hostPubKeyHex = env["HOST_PUBKEY"];
    const childHmacKeyHex = env["IPC_HMAC_KEY"];
    const childChannelId = env["IPC_CHANNEL_ID"];
    if (
      hostPubKeyHex === undefined ||
      childHmacKeyHex === undefined ||
      childChannelId === undefined
    ) {
      throw new Error("env missing required keys");
    }
    const hostPubKey = hexDecode(hostPubKeyHex);
    const childHmacKey = hexDecode(childHmacKeyHex);

    const controlBytes = new TextEncoder().encode("control");
    const sig = await signEd25519(controlBytes, kp.privateKey);
    expect(await verifyEd25519(controlBytes, sig, hostPubKey)).toBe(true);

    const eventBytes = new TextEncoder().encode("event");
    const tag = await signHmac(eventBytes, hmacKey);
    expect(await verifyHmac(eventBytes, tag, childHmacKey)).toBe(true);

    expect(childChannelId).toBe(channelId);
  });
});

describe("Threat-model comment block", () => {
  test("is present at the top of the IPC index module", () => {
    const indexPath = path.join(__dirname, "index.ts");
    const text = fs.readFileSync(indexPath, "utf8");
    const head = text.slice(0, 8_000);
    expect(head).toMatch(/THREAT MODEL/);
    expect(head).toMatch(/Asymmetric crypto/i);
    expect(head).toMatch(/HMAC-SHA256/);
    expect(head).toMatch(/ChannelId rotation/i);
    expect(head).toMatch(/Crash-on-overrun/i);
    expect(head).toMatch(/Supervisor-minted channelId/i);
    // The phrase wraps across a comment-line boundary in index.ts; the
    // regex tolerates the line-leading `// ` continuation.
    expect(head).toMatch(/PRIVATE(\s|\/\/)+KEY NEVER LEAVES/);
  });
});

describe("Payload union disjointness", () => {
  test("an InferenceEvent shape does not satisfy ControlPayload", () => {
    const inferenceEventShape = {
      type: "inference.start",
      seq: 1,
      data: { model: "test" },
    };
    const validated = ControlPayload(inferenceEventShape);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("a ControlPayload shape does not satisfy EventPayload", () => {
    const controlShape = {
      type: "drain",
      data: { deadlineMs: 1000 },
    };
    const validated = EventPayload(controlShape);
    expect(validated instanceof type.errors).toBe(true);
  });
});

describe("park.notify snapshot validation", () => {
  const validSnapshot = {
    name: "charge_card",
    description: "Charge the customer's card",
    inputSchema: { type: "object" },
    arguments: { amount: 100 },
  };

  test("accepts a park.notify carrying an approval snapshot", () => {
    const validated = ControlPayload({
      type: "park.notify",
      data: {
        runId: "run-1",
        correlationId: "corr-1",
        kind: "approval",
        snapshot: validSnapshot,
      },
    });
    expect(validated instanceof type.errors).toBe(false);
  });

  test("accepts a park.notify with no snapshot", () => {
    const validated = ControlPayload({
      type: "park.notify",
      data: { runId: "run-1", correlationId: "corr-1", kind: "approval" },
    });
    expect(validated instanceof type.errors).toBe(false);
  });

  test("rejects a park.notify whose snapshot exceeds the size cap", () => {
    const validated = ControlPayload({
      type: "park.notify",
      data: {
        runId: "run-1",
        correlationId: "corr-1",
        kind: "approval",
        snapshot: {
          ...validSnapshot,
          inputSchema: { pad: "a".repeat(APPROVAL_SNAPSHOT_MAX_BYTES) },
        },
      },
    });
    expect(validated instanceof type.errors).toBe(true);
  });
});

describe("substrate.write.request payload validation", () => {
  test("accepts a well-formed substrate.write.request", () => {
    const payload = {
      type: "substrate.write.request",
      data: {
        requestId: "sw-1-abc",
        repoId: { kind: "workflow-run", id: "dep-1" },
        ref: "refs/heads/main",
        preservePrefix: "runs/run-1/events/",
        message: "append event",
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("rejects a substrate.write.request missing requestId", () => {
    const payload = {
      type: "substrate.write.request",
      data: {
        repoId: { kind: "workflow-run", id: "dep-1" },
        ref: "refs/heads/main",
        preservePrefix: "runs/run-1/events/",
        message: "append event",
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects a substrate.write.request with a malformed repoId", () => {
    const payload = {
      type: "substrate.write.request",
      data: {
        requestId: "sw-1",
        repoId: { kind: "workflow-run" },
        ref: "refs/heads/main",
        preservePrefix: "runs/run-1/events/",
        message: "append event",
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });
});

describe("sources-updated payload validation", () => {
  const source = {
    id: "primary",
    provider: "anthropic",
    baseURL: "https://api.anthropic.com",
    apiKey: "sk-x",
    model: "claude-3-5",
  };

  test("accepts a well-formed sources-updated frame", () => {
    const payload = {
      type: "sources-updated",
      data: { sources: [source], defaultSource: "primary" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("rejects an empty sources list", () => {
    const payload = {
      type: "sources-updated",
      data: { sources: [], defaultSource: "primary" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects a missing sources list", () => {
    const payload = {
      type: "sources-updated",
      data: { defaultSource: "primary" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects a missing defaultSource", () => {
    const payload = {
      type: "sources-updated",
      data: { sources: [source] },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects an empty-string defaultSource", () => {
    const payload = {
      type: "sources-updated",
      data: { sources: [source], defaultSource: "" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects a defaultSource that is not the head source", () => {
    // The wire boundary owns the head-is-default invariant: the first
    // element must be the default source, so the warm-swap and cold-build
    // rotation paths agree on the active source. A default that is present
    // but not first is still rejected here.
    const second = { ...source, id: "secondary" };
    const payload = {
      type: "sources-updated",
      data: { sources: [source, second], defaultSource: "secondary" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects duplicate source ids", () => {
    const payload = {
      type: "sources-updated",
      data: { sources: [source, { ...source }], defaultSource: "primary" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });

  test("rejects a source element missing a required field", () => {
    const { apiKey: _apiKey, ...sourceWithoutApiKey } = source;
    const payload = {
      type: "sources-updated",
      data: { sources: [sourceWithoutApiKey], defaultSource: "primary" },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });
});

describe("substrate.merge.request payload validation", () => {
  test("accepts a well-formed substrate.merge.request", () => {
    const payload = {
      type: "substrate.merge.request",
      data: {
        requestId: "sw-1-abc",
        existing: [
          {
            path: "runs/run-1/events/1.json",
            contentBase64: "eyJzZXEiOjF9",
          },
        ],
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("accepts an empty existing array", () => {
    const payload = {
      type: "substrate.merge.request",
      data: { requestId: "sw-1-abc", existing: [] },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });
});

describe("substrate.merge.response payload validation", () => {
  test("accepts ok=true response with files", () => {
    const payload = {
      type: "substrate.merge.response",
      data: {
        requestId: "sw-1-abc",
        result: {
          ok: true,
          files: [
            {
              path: "runs/run-1/events/1.json",
              contentBase64: "eyJzZXEiOjF9",
            },
          ],
        },
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("accepts ok=false response with reason", () => {
    const payload = {
      type: "substrate.merge.response",
      data: {
        requestId: "sw-1-abc",
        result: { ok: false, reason: "merge failed" },
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });
});

describe("substrate.write.response payload validation", () => {
  test("accepts ok=true response with commitSha", () => {
    const payload = {
      type: "substrate.write.response",
      data: {
        requestId: "sw-1-abc",
        result: { ok: true, commitSha: "deadbeef" },
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("accepts ok=false response with reason", () => {
    const payload = {
      type: "substrate.write.response",
      data: {
        requestId: "sw-1-abc",
        result: { ok: false, reason: "substrate rejected" },
      },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(false);
  });

  test("rejects ok=true without a commitSha", () => {
    const payload = {
      type: "substrate.write.response",
      data: { requestId: "sw-1-abc", result: { ok: true } },
    };
    const validated = ControlPayload(payload);
    expect(validated instanceof type.errors).toBe(true);
  });
});
