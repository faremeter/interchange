// What mail_wait answers when the transport refuses the watch it installs
// after the opening search has already answered.
//
// The opening search and the watch install are two separate calls, so a removal
// that lands between them is refused by the second one and not by the first.
// The refusal carries the same RFC 5530 condition the opening search would have
// carried, and the caller is owed the same code for it: a mailbox that is gone
// is `invalid_mailbox`, and a handle whose registration is gone is
// `not_available`. Neither is `internal_error`, which errors.ts reserves for a
// defect in this package.
//
// These assertions run against the real in-memory transport rather than a mock,
// so the condition under test is the one a transport actually raises. Only the
// interleaving is injected: a delegating handle performs the removal as the last
// act of `search`, which makes the window between the two calls deterministic
// rather than a race against the microtask queue.

import { describe, expect, test } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import { createInMemoryTransport } from "@intx/mail-memory";
import type {
  MessageRef,
  MessageTransport,
  SearchQuery,
  ToolResult,
} from "@intx/types/runtime";
import {
  MessageTransportError,
  isMessageTransportError,
} from "@intx/types/runtime";

import { makeMailWaitHandler, type WaitScheduler } from "./handlers";

const ADDRESS = "alpha@test.interchange";
const signal = new AbortController().signal;

// Delegate every MessageTransport method to `real`, with `overrides` taking
// precedence. A Proxy rather than a spread: the in-memory transport's scoped
// handle is a class instance, whose methods live on the prototype and so are
// absent from `{ ...handle }`.
function delegate(
  real: MessageTransport,
  overrides: Partial<MessageTransport>,
): MessageTransport {
  return new Proxy(real, {
    get(target, prop) {
      if (prop in overrides) {
        return (overrides as Record<string | symbol, unknown>)[prop];
      }
      const value = Reflect.get(target, prop, target) as unknown;
      if (typeof value === "function") return value.bind(target);
      return value;
    },
  });
}

async function liveHandle() {
  const root = createInMemoryTransport();
  root.register(ADDRESS, createEd25519Crypto(await generateKeyPair()));
  return { root, scoped: root.getTransportFor(ADDRESS) };
}

// A transport whose search answers normally and whose watch install then
// refuses with `condition`. The sequence is what the handler sees when a mailbox
// goes away between its opening search and its watch: the search succeeded, so
// the refusal has to be classified from the install alone.
//
// The condition is injected rather than produced by removing a mailbox. There is
// no mailbox-removal method to produce it with any more, and the transport's own
// raising of NONEXISTENT is asserted directly in the suite above -- so splitting
// the two leaves each assertion about one thing.
function withActionAfterFirstSearch(
  real: MessageTransport,
  afterSearch: () => Promise<void>,
): MessageTransport {
  let fired = false;
  return delegate(real, {
    async search(
      mailbox: string,
      query: SearchQuery,
      sig?: AbortSignal,
    ): Promise<MessageRef[]> {
      const refs = await real.search(mailbox, query, sig);
      if (!fired) {
        fired = true;
        await afterSearch();
      }
      return refs;
    },
  });
}

function withRefusingWatch(
  real: MessageTransport,
  condition: "NONEXISTENT" | "CANNOT" | "SERVERBUG",
): MessageTransport {
  return delegate(real, {
    watch(): never {
      throw new MessageTransportError(
        condition,
        `watch install refused with ${condition}`,
      );
    },
  });
}

// The deadline is captured rather than waited out, after the seam in
// mail-wait-settlement.test.ts. Nothing in this file fires it, so no case can
// answer `timeout`: every case is expected to settle on the transport's
// refusal, and one whose refusal never reaches the caller hangs instead, which
// the test runner's own budget charges as a failure.
function capturedScheduler(): WaitScheduler {
  return {
    setTimeout() {
      return () => undefined;
    },
  };
}

function errorContent(result: ToolResult): Record<string, unknown> {
  expect(result.isError).toBe(true);
  if (typeof result.content === "string") {
    throw new Error("expected object content");
  }
  return result.content;
}

// The RFC 5530 condition a synchronous refusal named. A refusal naming none is
// a different failure from the one under test, so it throws rather than
// answering undefined and letting the comparison below read as a miss.
function thrownCondition(run: () => void): string {
  try {
    run();
  } catch (cause) {
    if (!isMessageTransportError(cause)) {
      throw new Error(`expected a condition, got ${String(cause)}`, { cause });
    }
    return cause.condition;
  }
  throw new Error("expected the call to refuse");
}

describe("the in-memory transport's watch refuses with a condition", () => {
  // The outer assertions are only about classification if the transport really
  // does name a condition here, and `watch` is synchronous, so mail-memory's own
  // rejection sweeps do not cover it. Assert it rather than assume it.
  test("a mailbox that is not there is NONEXISTENT", async () => {
    const { scoped } = await liveHandle();

    // A name the address never had. The transport has no method to delete a
    // mailbox -- an agent owns exactly one -- so "not there" is expressed by
    // naming one that was never created, which reaches the same guard.
    const condition = thrownCondition(() =>
      scoped.watch("NoSuchMailbox", () => undefined),
    );

    expect(condition).toBe("NONEXISTENT");
  });

  test("a handle whose registration is gone is CANNOT", async () => {
    const { root, scoped } = await liveHandle();
    root.unregister(ADDRESS);

    const condition = thrownCondition(() =>
      scoped.watch("INBOX", () => undefined),
    );

    expect(condition).toBe("CANNOT");
  });
});

describe("mail_wait classifies a refused watch install", () => {
  test("a mailbox removed after the opening search is invalid_mailbox", async () => {
    const { scoped } = await liveHandle();
    const transport = withRefusingWatch(scoped, "NONEXISTENT");

    const result = await makeMailWaitHandler(transport, capturedScheduler())(
      { id: "b1", name: "mail_wait", arguments: {} },
      signal,
    );

    expect(errorContent(result)["code"]).toBe("invalid_mailbox");
  });

  test("a handle deregistered after the opening search is not_available", async () => {
    const { root, scoped } = await liveHandle();
    const transport = withActionAfterFirstSearch(scoped, async () => {
      root.unregister(ADDRESS);
    });

    const result = await makeMailWaitHandler(transport, capturedScheduler())(
      { id: "b2", name: "mail_wait", arguments: {} },
      signal,
    );

    expect(errorContent(result)["code"]).toBe("not_available");
  });

  // The same two removals, performed before the opening search rather than
  // after it, are the contrast that makes the pair above the interesting case:
  // the code a caller receives must not depend on which of the two calls saw
  // the removal first. deregistered.test.ts covers the deregistered handle
  // across the whole toolset; the vanished mailbox is covered here.
  test("a mailbox not there at the opening search is invalid_mailbox", async () => {
    const { scoped } = await liveHandle();

    const result = await makeMailWaitHandler(scoped, capturedScheduler())(
      {
        id: "b3",
        name: "mail_wait",
        arguments: { mailbox: "NoSuchMailbox" },
      },
      signal,
    );

    expect(errorContent(result)["code"]).toBe("invalid_mailbox");
  });
});

describe("mail_wait keeps internal_error for a defect in this package", () => {
  // The classification above must not swallow the case the code exists for. A
  // cause naming no condition is not an operational outcome the caller
  // provoked, and reporting it as one would launder a bug in this package into
  // an ordinary transport failure.
  test("a watch install that throws a bare error is internal_error", async () => {
    const { scoped } = await liveHandle();
    const transport = delegate(scoped, {
      watch(): never {
        throw new TypeError("undefined is not a function");
      },
    });

    const result = await makeMailWaitHandler(transport, capturedScheduler())(
      { id: "b5", name: "mail_wait", arguments: {} },
      signal,
    );

    const content = errorContent(result);
    expect(content["code"]).toBe("internal_error");
    expect(String(content["error"])).toContain("undefined is not a function");
  });
});
