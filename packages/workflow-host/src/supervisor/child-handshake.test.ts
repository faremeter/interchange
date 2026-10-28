import { describe, expect, test } from "bun:test";

import { generateKeyPair } from "@intx/crypto";
import { hexEncode } from "@intx/types";
import {
  createMemoryNdjsonStream,
  sendDefinition,
  TEST_DEFINITION,
  TEST_DEFINITION_HASH,
} from "@intx/workflow-host/testing";

import {
  createControlChannelSender,
  generateChannelId,
  receiveControlChannel,
} from "../ipc/index";
import { waitForDefinition } from "./child-handshake";

type ChildSender = ReturnType<typeof createControlChannelSender>;

// Run `report` as the child's first frame and start the supervisor's check
// of it through the real control receiver.
async function checkReport(
  report: (
    sender: ChildSender,
    child: { childPid: number; childPublicKey: string },
  ) => Promise<void>,
) {
  const channelId = generateChannelId();
  const stream = createMemoryNdjsonStream();
  const keyPair = await generateKeyPair();
  const iter = receiveControlChannel({
    publicKey: { bootstrapFromDefinition: true },
    channelId,
    reader: stream.reader,
    onCrash: () => undefined,
  });
  const sender = createControlChannelSender({
    privateKeySeed: keyPair.privateKey,
    channelId,
    writer: {
      write(line: string) {
        stream.inject(line);
      },
    },
  });
  await report(sender, {
    childPid: 77,
    childPublicKey: hexEncode(keyPair.publicKey),
  });
  return waitForDefinition(iter, TEST_DEFINITION_HASH);
}

describe("waitForDefinition", () => {
  test("accepts a projection that hashes to the approved hash", async () => {
    const report = await checkReport((sender, child) =>
      sendDefinition(sender, child),
    );
    expect(report).toEqual({ childPid: 77, projection: TEST_DEFINITION });
  });

  test("rejects a well-formed projection that does not hash to the approved hash", async () => {
    await expect(
      checkReport((sender, child) =>
        sendDefinition(sender, {
          ...child,
          projection: { ...TEST_DEFINITION, id: "a-different-workflow" },
        }),
      ),
    ).rejects.toThrow(
      /reported a definition hashing to [0-9a-f]+, not the approved/,
    );
  });

  test("rejects a projection that fails the wire schema", async () => {
    await expect(
      checkReport((sender, child) =>
        sender.send({
          type: "definition",
          data: { ...child, result: { ok: true, projection: { id: 42 } } },
        }),
      ),
    ).rejects.toThrow(/reported a malformed definition/);
  });

  test("rejects with the child's own reason when it could not load", async () => {
    await expect(
      checkReport((sender, child) =>
        sender.send({
          type: "definition",
          data: { ...child, result: { ok: false, error: "closure blew up" } },
        }),
      ),
    ).rejects.toThrow(/could not load its definition: closure blew up/);
  });

  test("rejects when the control channel ends before any report", async () => {
    const stream = createMemoryNdjsonStream();
    const iter = receiveControlChannel({
      publicKey: { bootstrapFromDefinition: true },
      channelId: generateChannelId(),
      reader: stream.reader,
      onCrash: () => undefined,
    });
    const outcome = waitForDefinition(iter, TEST_DEFINITION_HASH);
    stream.close();
    await expect(outcome).rejects.toThrow(
      /control channel ended before the child reported its definition/,
    );
  });
});
