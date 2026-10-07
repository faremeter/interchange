// What mail_wait answers when the transport refuses the watch it installs
// after the opening search has already answered.
//
// The opening search and the watch install are two separate calls, so a
// removal that lands between them is refused by the second one and not by
// the first. The refusal carries the same RFC 5530 condition the opening
// search would have carried, and the caller is owed the same code for it:
// a mailbox that is gone is `invalid_mailbox`, a handle whose registration
// is gone is `not_available`; neither is `internal_error`.
//
// These assertions run against the real in-memory transport. Only the
// interleaving is injected: a delegating handle performs the removal as the
// last act of `search`, which makes the window between the two calls
// deterministic rather than a race against the microtask queue.

import { describe, expect, test } from "bun:test";
import { generateKeyPair, createEd25519Crypto } from "@intx/crypto";
import { createInMemoryTransport } from "@intx/mail-memory";
import type {
  MessageRef,
  MessageTransport,
  SearchQuery,
  ToolResult,
} from "@intx/types/runtime";
import { isMessageTransportError } from "@intx/types/runtime";

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

// Runs `afterSearch` once, after the real search has answered, so the next call
// the handler makes -- the watch install -- is the first to see the removal.
function withRemovalAfterFirstSearch(
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

// The deadline is captured rather than waited out, after the seam in
// mail-wait-settlement.test.ts. Nothing in this file fires it, so no case can
// answer `timeout`: every case is expected to settle on the transport's
// refusal, and one whose refusal never reaches the caller hangs instead,
// which the test runner's own budget charges as a failure.
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

// The RFC 5530 condition a refused watch names. A refusal naming none is a
// different failure from the one under test, so it throws rather than
// answering undefined and letting the comparison below read as a miss.
async function refusedCondition(run: () => Promise<unknown>): Promise<string> {
  try {
    await run();
  } catch (cause) {
    if (!isMessageTransportError(cause)) {
      throw new Error(`expected a condition, got ${String(cause)}`, { cause });
    }
    return cause.condition;
  }
  throw new Error("expected the call to refuse");
}

describe("the in-memory transport's watch refuses with a condition", () => {
  // These two refusals are what mail_wait classifies below. Assert the
  // condition on the rejection rather than assume the transport names one.
  test("a mailbox that is gone is NONEXISTENT", async () => {
    const { scoped } = await liveHandle();
    await scoped.deleteMailbox("INBOX");

    const condition = await refusedCondition(() =>
      scoped.watch("INBOX", () => undefined),
    );

    expect(condition).toBe("NONEXISTENT");
  });

  test("a handle whose registration is gone is CANNOT", async () => {
    const { root, scoped } = await liveHandle();
    root.unregister(ADDRESS);

    const condition = await refusedCondition(() =>
      scoped.watch("INBOX", () => undefined),
    );

    expect(condition).toBe("CANNOT");
  });
});

describe("mail_wait classifies a refused watch install", () => {
  test("a mailbox removed after the opening search is invalid_mailbox", async () => {
    const { scoped } = await liveHandle();
    const transport = withRemovalAfterFirstSearch(scoped, () =>
      scoped.deleteMailbox("INBOX"),
    );

    const result = await makeMailWaitHandler(transport, capturedScheduler())(
      { id: "b1", name: "mail_wait", arguments: {} },
      signal,
    );

    expect(errorContent(result)["code"]).toBe("invalid_mailbox");
  });

  test("a handle deregistered after the opening search is not_available", async () => {
    const { root, scoped } = await liveHandle();
    const transport = withRemovalAfterFirstSearch(scoped, async () => {
      root.unregister(ADDRESS);
    });

    const result = await makeMailWaitHandler(transport, capturedScheduler())(
      { id: "b2", name: "mail_wait", arguments: {} },
      signal,
    );

    expect(errorContent(result)["code"]).toBe("not_available");
  });

  // The same two removals, performed before the opening search rather than
  // after it, are the contrast that makes the pair above the interesting
  // case: the code a caller receives must not depend on which of the two
  // calls saw the removal first. deregistered.test.ts covers the
  // deregistered handle across the whole toolset; the vanished mailbox is
  // covered here.
  test("a mailbox already gone at the opening search is invalid_mailbox", async () => {
    const { scoped } = await liveHandle();
    await scoped.deleteMailbox("INBOX");

    const result = await makeMailWaitHandler(scoped, capturedScheduler())(
      { id: "b3", name: "mail_wait", arguments: {} },
      signal,
    );

    expect(errorContent(result)["code"]).toBe("invalid_mailbox");
  });
});

describe("mail_wait keeps internal_error for a defect in this package", () => {
  // The classification above must not swallow the case the code exists for.
  // A cause naming no condition is not an operational outcome the caller
  // provoked, and reporting it as one would launder a bug in this package
  // into an ordinary transport failure.
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
