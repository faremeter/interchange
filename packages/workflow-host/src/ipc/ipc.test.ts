import { describe, test, expect } from "bun:test";
import fs from "node:fs";
import path from "node:path";

import { generateKeyPair } from "@intx/crypto-node";

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
  hexDecode,
  hexEncode,
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
import { type } from "arktype";

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
    const sig = signEd25519(bytes, kp.privateKey);
    expect(verifyEd25519(bytes, sig, kp.publicKey)).toBe(true);
  });

  test("rejects a tampered payload", async () => {
    const kp = await generateKeyPair();
    const bytes = new TextEncoder().encode("hello");
    const sig = signEd25519(bytes, kp.privateKey);
    const tampered = new TextEncoder().encode("hellO");
    expect(verifyEd25519(tampered, sig, kp.publicKey)).toBe(false);
  });

  test("rejects an Ed25519 signature of the wrong length", async () => {
    const kp = await generateKeyPair();
    expect(() =>
      verifyEd25519(new Uint8Array(4), new Uint8Array(63), kp.publicKey),
    ).toThrow(/signature must be 64 bytes/);
  });
});

describe("HMAC primitives", () => {
  test("sign + verify round-trip", () => {
    const key = generateHmacKey();
    const bytes = new TextEncoder().encode("hello");
    const tag = signHmac(bytes, key);
    expect(verifyHmac(bytes, tag, key)).toBe(true);
  });

  test("rejects a tampered tag", () => {
    const key = generateHmacKey();
    const bytes = new TextEncoder().encode("hello");
    const tag = signHmac(bytes, key);
    const first = tag[0];
    if (first === undefined) throw new Error("tag empty");
    tag[0] = first ^ 0x01;
    expect(verifyHmac(bytes, tag, key)).toBe(false);
  });

  test("rejects an HMAC key of the wrong length", () => {
    expect(() => signHmac(new Uint8Array(4), new Uint8Array(16))).toThrow(
      /HMAC key must be 32 bytes/,
    );
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

    function emitSignedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: {
          type: "ready",
          data: { childPid: seq, childPublicKey: TEST_CHILD_PUBKEY_HEX },
        },
      };
      const sig = signEd25519(encodeEnvelope(envelope), kp.privateKey);
      const signed: SignedEnvelope = { envelope, sig: hexEncode(sig) };
      stream.inject(JSON.stringify(signed));
    }

    emitSignedFrame(1);
    emitSignedFrame(2);
    emitSignedFrame(2);
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

    function emitSignedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: {
          type: "ready",
          data: { childPid: seq, childPublicKey: TEST_CHILD_PUBKEY_HEX },
        },
      };
      const sig = signEd25519(encodeEnvelope(envelope), kp.privateKey);
      const signed: SignedEnvelope = { envelope, sig: hexEncode(sig) };
      stream.inject(JSON.stringify(signed));
    }

    emitSignedFrame(1);
    emitSignedFrame(3);
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

    function emitMacedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: { type: "inference.start", seq, data: { model: "x" } },
      };
      const tag = signHmac(encodeEnvelope(envelope), key);
      const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
      stream.inject(new TextEncoder().encode(JSON.stringify(maced)));
    }

    emitMacedFrame(1);
    emitMacedFrame(2);
    emitMacedFrame(2);
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

    function emitMacedFrame(seq: number) {
      const envelope: FrameEnvelope = {
        seq,
        channelId,
        payload: { type: "inference.start", seq, data: { model: "x" } },
      };
      const tag = signHmac(encodeEnvelope(envelope), key);
      const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
      stream.inject(new TextEncoder().encode(JSON.stringify(maced)));
    }

    for (let i = 1; i <= limit + 2; i++) {
      emitMacedFrame(i);
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
    const tag = signHmac(encodeEnvelope(envelope), key);
    const maced: MacedEnvelope = { envelope, mac: hexEncode(tag) };
    stream.inject(new TextEncoder().encode(JSON.stringify(maced)));
    stream.close();
    await consumer;

    expect(crashes.length).toBe(1);
    expect(crashes[0]).toMatch(/payload failed validation/);
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
    const sig = signEd25519(controlBytes, kp.privateKey);
    expect(verifyEd25519(controlBytes, sig, hostPubKey)).toBe(true);

    const eventBytes = new TextEncoder().encode("event");
    const tag = signHmac(eventBytes, hmacKey);
    expect(verifyHmac(eventBytes, tag, childHmacKey)).toBe(true);

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
