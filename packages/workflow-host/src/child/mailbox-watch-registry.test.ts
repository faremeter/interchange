import { describe, test, expect } from "bun:test";

import type { MailboxEvent, MessageHeaders } from "@intx/types/runtime";

import { createMailboxWatchRegistry } from "./mailbox-watch-registry";

function headersFor(messageId: string): MessageHeaders {
  return {
    from: "user@integration",
    to: ["run@integration"],
    date: "",
    messageId,
  };
}

function existsEvent(uid: number, messageId: string): MailboxEvent {
  return { type: "exists", uid, headers: headersFor(messageId) };
}

// Yield to the microtask queue so a deferred `fire` delivery runs before the
// assertion. `queueMicrotask` callbacks drain ahead of a resolved promise
// continuation, so a single `await` is enough.
function flushMicrotasks(): Promise<void> {
  return Promise.resolve();
}

describe("createMailboxWatchRegistry", () => {
  test("fires an exists event to a registered callback on notify", async () => {
    const registry = createMailboxWatchRegistry();
    const received: MailboxEvent[] = [];
    registry.watch("INBOX", (event) => received.push(event));

    const event = existsEvent(1, "<m1@integration>");
    registry.fire("INBOX", event);

    // Delivery is asynchronous: nothing runs on the delivering call stack.
    expect(received).toEqual([]);

    await flushMicrotasks();
    expect(received).toEqual([event]);
  });

  test("delivers only to callbacks registered for the fired mailbox", async () => {
    const registry = createMailboxWatchRegistry();
    const inbox: MailboxEvent[] = [];
    const archive: MailboxEvent[] = [];
    registry.watch("INBOX", (event) => inbox.push(event));
    registry.watch("Archive", (event) => archive.push(event));

    const event = existsEvent(2, "<m2@integration>");
    registry.fire("INBOX", event);
    await flushMicrotasks();

    expect(inbox).toEqual([event]);
    expect(archive).toEqual([]);
  });

  test("fans a notify out to every callback on the mailbox", async () => {
    const registry = createMailboxWatchRegistry();
    const first: MailboxEvent[] = [];
    const second: MailboxEvent[] = [];
    registry.watch("INBOX", (event) => first.push(event));
    registry.watch("INBOX", (event) => second.push(event));

    const event = existsEvent(3, "<m3@integration>");
    registry.fire("INBOX", event);
    await flushMicrotasks();

    expect(first).toEqual([event]);
    expect(second).toEqual([event]);
  });

  test("stops firing to a callback after unsubscribe", async () => {
    const registry = createMailboxWatchRegistry();
    const received: MailboxEvent[] = [];
    const unsubscribe = registry.watch("INBOX", (event) =>
      received.push(event),
    );

    const first = existsEvent(4, "<m4@integration>");
    registry.fire("INBOX", first);
    await flushMicrotasks();
    expect(received).toEqual([first]);

    unsubscribe();
    registry.fire("INBOX", existsEvent(5, "<m5@integration>"));
    await flushMicrotasks();

    // No new event lands after unsubscribe.
    expect(received).toEqual([first]);
  });

  test("does not deliver when unsubscribe races a pending delivery", async () => {
    const registry = createMailboxWatchRegistry();
    const received: MailboxEvent[] = [];
    const unsubscribe = registry.watch("INBOX", (event) =>
      received.push(event),
    );

    // Fire, then unsubscribe synchronously before the delivery microtask runs.
    // Delivery re-checks registration, so the event never lands.
    registry.fire("INBOX", existsEvent(6, "<m6@integration>"));
    unsubscribe();
    await flushMicrotasks();

    expect(received).toEqual([]);
  });

  test("a double-unsubscribe does not remove a re-registered callback", async () => {
    const registry = createMailboxWatchRegistry();
    const received: MailboxEvent[] = [];
    const callback = (event: MailboxEvent) => received.push(event);

    const unsubscribe = registry.watch("INBOX", callback);
    unsubscribe();
    unsubscribe();

    // Re-register the same callback identity; the stale unsubscribe must not
    // affect the fresh registration.
    registry.watch("INBOX", callback);
    const event = existsEvent(7, "<m7@integration>");
    registry.fire("INBOX", event);
    await flushMicrotasks();

    expect(received).toEqual([event]);
  });

  test("fire is a no-op when no callback is registered for the mailbox", async () => {
    const registry = createMailboxWatchRegistry();
    // No throw, no delivery.
    registry.fire("INBOX", existsEvent(8, "<m8@integration>"));
    await flushMicrotasks();
  });
});
