// What the mail tools report for a handle the transport refuses outright.
//
// A scoped handle whose address registration is gone is refused before any
// mailbox is touched. It raises RFC 5530's `CANNOT`, and reissuing the same call
// does not address it -- which is what separates it from the `*_failed` codes:
// those say the transport was reached and the outcome is unknown, and MESSAGE.md
// tells a caller as much.
//
// These assertions run against a real in-memory transport rather than a mock,
// because the value under test is what a caller actually receives from the pair
// -- the condition the transport raises and the code the classifier derives from
// it. A mock could assert the classifier alone and would keep passing if the
// transport stopped naming the condition.

import { describe, expect, test } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import { createInMemoryTransport } from "@intx/mail-memory";
import type { MessageTransport, ToolResult } from "@intx/types/runtime";

import {
  makeMailExpungeHandler,
  makeMailFlagHandler,
  makeMailReadHandler,
  makeMailSearchHandler,
} from "./handlers";

const ADDRESS = "alpha@test.interchange";
const signal = new AbortController().signal;

async function deregisteredTransport(): Promise<MessageTransport> {
  const transport = createInMemoryTransport();
  const keys = await generateKeyPair();
  transport.register(ADDRESS, createEd25519Crypto(keys));
  const scoped = transport.getTransportFor(ADDRESS);
  transport.unregister(ADDRESS);
  return scoped;
}

describe("a handle the transport refuses outright", () => {
  test("reports not_available rather than an outcome-unknown code", async () => {
    const transport = await deregisteredTransport();

    const calls: [label: string, run: () => Promise<ToolResult>][] = [
      [
        "mail_search",
        () =>
          makeMailSearchHandler(transport)(
            { id: "d1", name: "mail_search", arguments: {} },
            signal,
          ),
      ],
      [
        "mail_expunge",
        () =>
          makeMailExpungeHandler(transport)(
            { id: "d2", name: "mail_expunge", arguments: {} },
            signal,
          ),
      ],
      [
        "mail_flag",
        () =>
          makeMailFlagHandler(transport)(
            {
              id: "d3",
              name: "mail_flag",
              arguments: {
                ref: { uid: 1, mailbox: "INBOX" },
                set: ["\\Seen"],
              },
            },
            signal,
          ),
      ],
      [
        "mail_read",
        () =>
          makeMailReadHandler(transport)(
            {
              id: "d4",
              name: "mail_read",
              arguments: { ref: { uid: 1, mailbox: "INBOX" } },
            },
            signal,
          ),
      ],
    ];

    for (const [label, run] of calls) {
      const result = await run();
      expect(result.isError).toBe(true);
      if (typeof result.content === "string") {
        throw new Error(`${label}: expected object content`);
      }
      // Labelled so a failure names the tool that regressed rather than only
      // the code it answered with.
      expect(`${label}: ${String(result.content["code"])}`).toBe(
        `${label}: not_available`,
      );
      // The transport's own words survive the classification, so the caller
      // learns the registration is what is gone rather than only that the
      // operation was refused.
      expect(String(result.content["error"])).toContain("deregistered");
    }
  });

  test("does not report the mailbox as the thing to change", async () => {
    // `invalid_mailbox` is the other code raised before the operation, and it
    // is the wrong answer here: it tells a caller to name a different mailbox,
    // and every mailbox name fails the same way while the registration is gone.
    const transport = await deregisteredTransport();

    const result = await makeMailSearchHandler(transport)(
      { id: "d5", name: "mail_search", arguments: { mailbox: "Archive" } },
      signal,
    );

    if (typeof result.content === "string") {
      throw new Error("expected object content");
    }
    expect(result.content["code"]).not.toBe("invalid_mailbox");
    expect(result.content["code"]).toBe("not_available");
  });
});
