// A mailbox operand the mail tools accept must survive the control channel.
// The receiver crashes the child when a payload fails `ControlPayload`, so
// the transport forwards the operand and the supervisor refuses it.

import { describe, expect, test } from "bun:test";

import { generateKeyPair } from "@intx/crypto";
import { deriveWorkflowRunId } from "@intx/types";
import type { MessageTransport } from "@intx/types/runtime";
import { createMemoryNdjsonStream } from "@intx/workflow-host/testing";

import { generateChannelId } from "../ipc/crypto";
import {
  createControlChannelSender,
  receiveControlChannel,
  type ControlPayload,
} from "../ipc/control-channel";
import { createChildMailboxCallBridge } from "./mailbox-call-bridge";
import { createChildMailboxMutationBridge } from "./mailbox-mutation-bridge";
import { createMailboxWatchRegistry } from "./mailbox-watch-registry";
import { createChildOutboundMailBridge } from "./outbound-mail-bridge";
import { createSupervisorBackedTransport } from "./supervisor-backed-transport";

const ADDRESS = "run_agent@example.com";

async function until(predicate: () => boolean, label: string): Promise<void> {
  const deadline = Date.now() + 1000;
  while (!predicate()) {
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${label}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

describe("supervisor-backed transport frames", () => {
  async function drive(
    run: (transport: MessageTransport) => Promise<unknown>,
  ): Promise<{ crashes: string[]; received: ControlPayload[] }> {
    const kp = await generateKeyPair();
    const channelId = generateChannelId();
    const stream = createMemoryNdjsonStream();
    const crashes: string[] = [];
    const received: ControlPayload[] = [];
    const sender = createControlChannelSender({
      privateKeySeed: kp.privateKey,
      channelId,
      writer: stream.writer,
    });
    const consumer = (async () => {
      for await (const payload of receiveControlChannel({
        publicKey: kp.publicKey,
        channelId,
        reader: stream.reader,
        onCrash: (reason) => crashes.push(reason),
      })) {
        received.push(payload);
      }
    })();
    const callBridge = createChildMailboxCallBridge({ upstreamSender: sender });
    const mutationBridge = createChildMailboxMutationBridge({
      upstreamSender: sender,
    });
    const transport = createSupervisorBackedTransport(
      createChildOutboundMailBridge({ upstreamSender: sender }),
      ADDRESS,
      {
        watchRegistry: createMailboxWatchRegistry(),
        callBridge,
        mutationBridge,
      },
    );

    const pending = run(transport);
    try {
      await until(
        () => crashes.length > 0 || received.length > 0,
        "a frame to be accepted or the channel to crash",
      );
    } finally {
      callBridge.cancelAll("test done");
      mutationBridge.cancelAll("test done");
      stream.close();
      await consumer;
      await pending.catch(() => undefined);
    }
    return { crashes, received };
  }

  function forwarded(received: ControlPayload[]): ControlPayload {
    expect(received).toHaveLength(1);
    const payload = received[0];
    if (payload === undefined) throw new Error("expected a frame");
    return payload;
  }

  test("a non-INBOX name is forwarded", async () => {
    const { crashes, received } = await drive((transport) =>
      transport.search("Drafts", {}),
    );

    expect(crashes).toEqual([]);
    const payload = forwarded(received);
    if (payload.type !== "mailbox.call.request") {
      throw new Error(payload.type);
    }
    if (payload.data.op !== "search") throw new Error(payload.data.op);
    expect(payload.data.mailbox).toBe("Drafts");
    expect(payload.data.runId).toBe(deriveWorkflowRunId(ADDRESS));
  });

  test("an empty mailbox search is forwarded", async () => {
    const { crashes, received } = await drive((transport) =>
      transport.search("", {}),
    );

    expect(crashes).toEqual([]);
    const payload = forwarded(received);
    if (payload.type !== "mailbox.call.request") {
      throw new Error(payload.type);
    }
    if (payload.data.op !== "search") throw new Error(payload.data.op);
    expect(payload.data.mailbox).toBe("");
  });

  test("a non-positive uid fetch is forwarded", async () => {
    const { crashes, received } = await drive((transport) =>
      transport.fetchFull({ uid: 0, mailbox: "INBOX" }),
    );

    expect(crashes).toEqual([]);
    const payload = forwarded(received);
    if (payload.type !== "mailbox.call.request") {
      throw new Error(payload.type);
    }
    if (payload.data.op !== "fetchFull") throw new Error(payload.data.op);
    expect(payload.data.ref).toEqual({ uid: 0, mailbox: "INBOX" });
  });

  test("an empty part path is forwarded", async () => {
    const { crashes, received } = await drive((transport) =>
      transport.fetchPart({ uid: 1, mailbox: "INBOX" }, ""),
    );

    expect(crashes).toEqual([]);
    const payload = forwarded(received);
    if (payload.type !== "mailbox.call.request") {
      throw new Error(payload.type);
    }
    if (payload.data.op !== "fetchPart") throw new Error(payload.data.op);
    expect(payload.data.partPath).toBe("");
  });

  test("an empty mailbox flag write is forwarded", async () => {
    const { crashes, received } = await drive((transport) =>
      transport.setFlags({ uid: 1, mailbox: "" }, ["\\Seen"]),
    );

    expect(crashes).toEqual([]);
    const payload = forwarded(received);
    if (payload.type !== "mailbox.mutate.request") {
      throw new Error(payload.type);
    }
    expect(payload.data.mailbox).toBe("");
    expect(payload.data.op).toBe("addFlags");
  });

  test("a non-positive uid flag write is forwarded", async () => {
    const { crashes, received } = await drive((transport) =>
      transport.setFlags({ uid: 0, mailbox: "INBOX" }, ["\\Seen"]),
    );

    expect(crashes).toEqual([]);
    const payload = forwarded(received);
    if (payload.type !== "mailbox.mutate.request") {
      throw new Error(payload.type);
    }
    if (payload.data.op !== "addFlags") throw new Error(payload.data.op);
    expect(payload.data.uid).toBe(0);
    expect(payload.data.mailbox).toBe("INBOX");
  });
});
