// Settlement guarantees for mail_wait -- that the call answers whatever the
// transport does, and that the deadline it declares is the deadline it keeps --
// and the recursive reach of the search query's date coercion.

import { describe, expect, test } from "bun:test";
import { type } from "arktype";
import type {
  BodyStructure,
  InboundMessage,
  ListInfo,
  Mailbox,
  MailboxEvent,
  MailboxStatus,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageTransport,
  SendReceipt,
  SyncResult,
  Thread,
  ToolResult,
  Unsubscribe,
} from "@intx/types/runtime";

import { SearchQueryArgs, makeMailWaitHandler } from "./handlers";

// A method no test in this file drives. Reaching one is a test that is not
// asserting what it says it asserts, so it fails loudly rather than answering.
function notDriven(method: string): never {
  throw new Error(`the mail_wait stub transport does not implement ${method}`);
}

// The full interface, so a stub is a MessageTransport rather than a cast of a
// partial one. Only `search`, `fetchFull` and `watch` answer; a test overrides
// the ones it drives.
function createStubTransport(
  overrides: Partial<MessageTransport>,
): MessageTransport {
  const base: MessageTransport = {
    async send(): Promise<SendReceipt> {
      return notDriven("send");
    },
    async append(): Promise<MessageRef> {
      return notDriven("append");
    },
    async listMailboxes(): Promise<Mailbox[]> {
      return notDriven("listMailboxes");
    },
    async createMailbox(): Promise<Mailbox> {
      return notDriven("createMailbox");
    },
    async deleteMailbox(): Promise<void> {
      return notDriven("deleteMailbox");
    },
    async getMailboxStatus(): Promise<MailboxStatus> {
      return notDriven("getMailboxStatus");
    },
    async search(): Promise<MessageRef[]> {
      return [];
    },
    async thread(): Promise<Thread[]> {
      return notDriven("thread");
    },
    async fetchHeaders(): Promise<MessageHeaders> {
      return notDriven("fetchHeaders");
    },
    async fetchStructure(): Promise<BodyStructure> {
      return notDriven("fetchStructure");
    },
    async fetchPart(): Promise<MessagePart> {
      return notDriven("fetchPart");
    },
    async fetchFull(ref: MessageRef): Promise<InboundMessage> {
      return {
        ref,
        headers: {
          from: "alice@test",
          to: ["agent@test"],
          date: new Date().toISOString(),
          subject: "a match",
        },
        flags: [],
        content: "hi",
        signatureStatus: "missing",
      };
    },
    async setFlags(): Promise<void> {
      return notDriven("setFlags");
    },
    async clearFlags(): Promise<void> {
      return notDriven("clearFlags");
    },
    async move(): Promise<void> {
      return notDriven("move");
    },
    async copy(): Promise<void> {
      return notDriven("copy");
    },
    async expunge(): Promise<{ expungedUids: number[] }> {
      return notDriven("expunge");
    },
    watch(): Unsubscribe {
      return () => {
        /* nothing to unsubscribe */
      };
    },
    async sync(): Promise<SyncResult> {
      return notDriven("sync");
    },
    async createList(): Promise<ListInfo> {
      return notDriven("createList");
    },
    async listMembers(): Promise<string[]> {
      return notDriven("listMembers");
    },
    async subscribe(): Promise<void> {
      return notDriven("subscribe");
    },
    async unsubscribe(): Promise<void> {
      return notDriven("unsubscribe");
    },
  };
  return { ...base, ...overrides };
}

function arrival(uid: number): MailboxEvent {
  return {
    type: "exists",
    uid,
    headers: {
      from: "alice@test",
      to: ["agent@test"],
      date: new Date().toISOString(),
    },
  };
}

// A search that never answers, for the cases whose subject is what the handler
// does while a read is outstanding.
function neverAnswers(): Promise<MessageRef[]> {
  return new Promise<MessageRef[]>(() => {
    /* the transport never answers this read */
  });
}

function errorContent(result: ToolResult): Record<string, unknown> {
  expect(result.isError).toBe(true);
  if (typeof result.content === "string") {
    throw new Error("expected object content");
  }
  return result.content;
}

describe("mail_wait always settles", () => {
  test("a throwing watch unsubscribe does not strand the call", async () => {
    const watching = Promise.withResolvers<(event: MailboxEvent) => void>();
    let watched = false;
    const transport = createStubTransport({
      watch(_mailbox: string, callback: (event: MailboxEvent) => void) {
        watched = true;
        watching.resolve(callback);
        return () => {
          throw new Error("unsubscribe blew up");
        };
      },
      async search(): Promise<MessageRef[]> {
        return watched ? [{ uid: 1, mailbox: "INBOX" }] : [];
      },
    });

    const running = makeMailWaitHandler(transport)(
      { id: "w1", name: "mail_wait", arguments: { timeout: 30 } },
      new AbortController().signal,
    );
    const deliver = await watching.promise;
    deliver(arrival(1));

    const result = await running;
    expect(result.isError).toBeUndefined();
    if (typeof result.content === "string") {
      throw new Error("expected object content");
    }
    expect(result.content["ref"]).toEqual({ uid: 1, mailbox: "INBOX" });
  });

  test("an abort during the initial read settles the call", async () => {
    const controller = new AbortController();
    const running = makeMailWaitHandler(
      createStubTransport({ search: neverAnswers }),
    )(
      { id: "w2", name: "mail_wait", arguments: { timeout: 30 } },
      controller.signal,
    );

    controller.abort();

    expect(errorContent(await running)["code"]).toBe("aborted");
  });

  test("the declared timeout covers a read that never answers", async () => {
    // The deadline is the subject, so it is driven rather than waited out. The
    // seam also puts the declared seconds under assertion, which waiting out a
    // real timer cannot do.
    const armed = Promise.withResolvers<() => void>();
    const scheduler = {
      setTimeout(callback: () => void, delayMs: number) {
        expect(delayMs).toBe(1000);
        armed.resolve(callback);
        return () => {
          /* the test fires the deadline instead of cancelling it */
        };
      },
    };

    const running = makeMailWaitHandler(
      createStubTransport({ search: neverAnswers }),
      scheduler,
    )(
      { id: "w3", name: "mail_wait", arguments: { timeout: 1 } },
      new AbortController().signal,
    );

    (await armed.promise)();

    expect(errorContent(await running)["code"]).toBe("timeout");
  });
});

describe("mail_wait bounds its timeout", () => {
  // Out of range means out of setTimeout's signed 32-bit delay: the runtime
  // coerces such a delay to 1ms, so an unbounded timeout returns at once with a
  // `timeout` error for a wait that never happened. Nothing may reach the
  // transport either -- a refused argument costs no round trip.
  test.each([
    ["past the 32-bit setTimeout range", 3_000_000],
    ["one second past the ceiling", 1741],
    ["zero", 0],
    ["negative", -5],
    ["fractional", 1.5],
  ])("refuses a timeout %s", async (_case: string, timeout: number) => {
    let searched = false;
    const transport = createStubTransport({
      async search(): Promise<MessageRef[]> {
        searched = true;
        return [];
      },
    });

    const result = await makeMailWaitHandler(transport)(
      { id: "w4", name: "mail_wait", arguments: { timeout } },
      new AbortController().signal,
    );

    expect(errorContent(result)["code"]).toBe("invalid_arguments");
    expect(searched).toBe(false);
  });

  test("accepts the RFC 2177 ceiling of 1740 seconds", async () => {
    const transport = createStubTransport({
      async search(): Promise<MessageRef[]> {
        return [{ uid: 4, mailbox: "INBOX" }];
      },
    });

    const result = await makeMailWaitHandler(transport)(
      { id: "w5", name: "mail_wait", arguments: { timeout: 1740 } },
      new AbortController().signal,
    );

    expect(result.isError).toBeUndefined();
  });
});

describe("the search query coerces dates at every depth", () => {
  test("a date filter inside and, or and not becomes a Date", () => {
    const out = SearchQueryArgs({
      and: [{ before: "2026-03-01" }],
      or: [{ after: "2026-03-01" }],
      not: { on: "2026-03-01" },
    });
    if (out instanceof type.errors) throw new Error(out.summary);
    expect(out.and?.[0]?.before).toBeInstanceOf(Date);
    expect(out.or?.[0]?.after).toBeInstanceOf(Date);
    expect(out.not?.on).toBeInstanceOf(Date);
  });

  test("an unparseable date inside a nested branch is rejected", () => {
    expect(
      SearchQueryArgs({ and: [{ before: "yesterday" }] }) instanceof
        type.errors,
    ).toBe(true);
  });
});
