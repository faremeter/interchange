import { describe, test, expect } from "bun:test";
import { type } from "arktype";

import { createInboundMessage } from "@intx/mime";
import {
  ConnectorThreadState as ConnectorThreadStateSchema,
  type InboundMessage,
} from "@intx/types/runtime";

type ConnectorThreadState = typeof ConnectorThreadStateSchema.infer;

import {
  createConnectorRouter,
  NoActiveConnectorThreadError,
  type ConnectorReplyParts,
  type ConnectorRouter,
} from "./connector-router";

const AGENT = "agent@example.com";

function requireReplyParts(router: ConnectorRouter): ConnectorReplyParts {
  const parts = router.composeReply();
  if (parts === null) {
    throw new Error("composeReply() returned no reply address");
  }
  return parts;
}

// `cc` names the recipients the starting sender addresses besides the
// agent, which makes them participants that can join the thread.
function startMessage(opts?: {
  subject?: string;
  cc?: string[];
}): InboundMessage {
  return createInboundMessage({
    from: "user@example.com",
    to: [AGENT],
    content: "hello",
    messageId: "<root@example.com>",
    ...(opts?.cc !== undefined ? { cc: opts.cc } : {}),
    ...(opts?.subject !== undefined ? { subject: opts.subject } : {}),
  });
}

function continuationByReferences(
  threadRoot: string,
  opts?: { from?: string; messageId?: string },
): InboundMessage {
  return createInboundMessage({
    from: opts?.from ?? "user@example.com",
    to: ["agent@example.com"],
    content: "more",
    messageId: opts?.messageId ?? "<reply-1@example.com>",
    references: [threadRoot],
  });
}

function continuationByInReplyTo(
  lastMessageId: string,
  opts?: { from?: string; messageId?: string },
): InboundMessage {
  return createInboundMessage({
    from: opts?.from ?? "user@example.com",
    to: ["agent@example.com"],
    content: "more",
    messageId: opts?.messageId ?? "<reply-2@example.com>",
    inReplyTo: lastMessageId,
  });
}

function unrelatedMessage(): InboundMessage {
  return createInboundMessage({
    from: "stranger@example.com",
    to: ["agent@example.com"],
    content: "unrelated",
    messageId: "<other@example.com>",
  });
}

describe("createConnectorRouter", () => {
  describe("route + commit (inbound)", () => {
    test("no active thread: start initializes with empty cc", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });

      router.commit(router.route(startMessage({ subject: "Hello" })));

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<root@example.com>",
        replyTo: "user@example.com",
        cc: [],
        subject: "Hello",
      });
    });

    test("active thread + references includes threadRoot: continue from the same sender keeps cc empty", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));

      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "user@example.com",
            messageId: "<follow-1@example.com>",
          }),
        ),
      );

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<follow-1@example.com>",
        lastReferences: ["<root@example.com>"],
        replyTo: "user@example.com",
        cc: [],
        subject: "Hello",
      });
    });

    test("active thread + inReplyTo equals lastMessageId: continue from the same sender keeps cc empty", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage()));

      router.commit(
        router.route(
          continuationByInReplyTo("<root@example.com>", {
            messageId: "<follow-2@example.com>",
          }),
        ),
      );

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<follow-2@example.com>",
        lastReferences: ["<root@example.com>"],
        replyTo: "user@example.com",
        cc: [],
      });
    });

    test("continue from a different sender moves the prior replyTo into cc", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          startMessage({ subject: "Important", cc: ["other@example.com"] }),
        ),
      );

      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "other@example.com",
            messageId: "<follow-3@example.com>",
          }),
        ),
      );

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<follow-3@example.com>",
        lastReferences: ["<root@example.com>"],
        replyTo: "other@example.com",
        cc: ["user@example.com"],
        addressed: ["other@example.com"],
        subject: "Important",
      });
    });

    test("continue accumulates participants across multiple distinct senders", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          startMessage({
            subject: "Important",
            cc: ["second@example.com", "third@example.com"],
          }),
        ),
      );

      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "third@example.com",
            messageId: "<follow-b@example.com>",
          }),
        ),
      );

      const snap = router.snapshot();
      expect(snap?.replyTo).toBe("third@example.com");
      expect(snap?.cc).toEqual(["user@example.com", "second@example.com"]);
    });

    test("continue from a sender already in cc does not duplicate them", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ cc: ["second@example.com"] })));
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );
      // Now: replyTo=second, cc=[user]. The original user returns.
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "user@example.com",
            messageId: "<follow-b@example.com>",
          }),
        ),
      );

      const snap = router.snapshot();
      expect(snap?.replyTo).toBe("user@example.com");
      // user is now the most recent speaker; second is the only other
      // participant. user must not appear in cc.
      expect(snap?.cc).toEqual(["second@example.com"]);
    });

    test("subject is preserved across many continues from different senders", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const others = ["b@example.com", "c@example.com", "d@example.com"];
      router.commit(
        router.route(startMessage({ subject: "Important", cc: others })),
      );

      for (const from of others) {
        router.commit(
          router.route(
            continuationByReferences("<root@example.com>", {
              from,
              messageId: `<follow-${from}>`,
            }),
          ),
        );
      }

      expect(router.snapshot()?.replyTo).toBe("d@example.com");
      expect(router.snapshot()?.subject).toBe("Important");
    });

    test("active thread + neither header rule matches: passthrough leaves state unchanged", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));
      const before = router.snapshot();

      const decision = router.route(unrelatedMessage());
      expect(decision.kind).toBe("passthrough");

      router.commit(decision);

      expect(router.snapshot()).toEqual(before);
    });

    test("commit on a foreign decision throws", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      expect(() => {
        router.commit({ kind: "start" });
      }).toThrow();
    });
  });

  describe("composeReply (outbound)", () => {
    test("single-participant thread: cc is empty", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));

      const parts = router.composeReply();
      expect(parts).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        references: ["<root@example.com>"],
        subject: "Hello",
      });
    });

    test("multi-participant thread: cc contains all prior speakers", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          startMessage({
            subject: "Important",
            cc: ["second@example.com", "third@example.com"],
          }),
        ),
      );
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "third@example.com",
            messageId: "<follow-b@example.com>",
          }),
        ),
      );

      const parts = router.composeReply();
      expect(parts).toEqual({
        to: "third@example.com",
        cc: ["user@example.com", "second@example.com"],
        inReplyTo: "<follow-b@example.com>",
        references: ["<root@example.com>", "<follow-b@example.com>"],
        subject: "Important",
      });
    });

    test("active thread without subject: subject key absent (not undefined)", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage()));

      const parts = requireReplyParts(router);
      expect(parts.to).toBe("user@example.com");
      expect(parts.cc).toEqual([]);
      expect(parts.inReplyTo).toBe("<root@example.com>");
      expect("subject" in parts).toBe(false);
    });

    test("composeReply returns a copy of cc, not the live array", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ cc: ["second@example.com"] })));
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );

      const parts = requireReplyParts(router);
      parts.cc.push("injected@example.com");

      // Subsequent state must not include the injected value.
      expect(router.snapshot()?.cc).toEqual(["user@example.com"]);
    });

    test("no active thread: throws NoActiveConnectorThreadError", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      expect(() => {
        router.composeReply();
      }).toThrow(NoActiveConnectorThreadError);
    });

    test("onReplySent advances lastMessageId and preserves cc", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          startMessage({ subject: "Hello", cc: ["second@example.com"] }),
        ),
      );
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );

      router.onReplySent(
        {
          messageId: "<sent-1@example.com>",
          status: "delivered",
        },
        [],
      );

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<sent-1@example.com>",
        replyTo: "second@example.com",
        cc: ["user@example.com"],
        addressed: ["second@example.com"],
        subject: "Hello",
      });

      // A subsequent inbound whose inReplyTo matches the new
      // lastMessageId must route as continue.
      const followup = continuationByInReplyTo("<sent-1@example.com>", {
        from: "second@example.com",
        messageId: "<follow-after-send@example.com>",
      });
      expect(router.route(followup).kind).toBe("continue");
    });

    test("onReplySent with no active thread throws", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      expect(() => {
        router.onReplySent(
          {
            messageId: "<sent@example.com>",
            status: "delivered",
          },
          [],
        );
      }).toThrow(NoActiveConnectorThreadError);
    });
  });

  describe("snapshot/restore round-trip", () => {
    test("a router restored from a snapshot decides identically", () => {
      const a = createConnectorRouter({ ownAddress: AGENT });
      a.commit(
        a.route(startMessage({ subject: "Hello", cc: ["other@example.com"] })),
      );
      a.commit(
        a.route(
          continuationByReferences("<root@example.com>", {
            from: "other@example.com",
            messageId: "<follow-A@example.com>",
          }),
        ),
      );

      const snap = a.snapshot();

      const b = createConnectorRouter({ ownAddress: AGENT });
      b.restore(snap);

      expect(b.snapshot()).toEqual(snap);

      const probe = continuationByInReplyTo("<follow-A@example.com>", {
        messageId: "<probe@example.com>",
      });
      expect(b.route(probe).kind).toBe(a.route(probe).kind);

      // Outbound parts also match (including cc).
      expect(b.composeReply()).toEqual(a.composeReply());
    });

    test("restore(null) clears active thread", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage()));
      expect(router.snapshot()).not.toBeNull();

      router.restore(null);
      expect(router.snapshot()).toBeNull();

      expect(router.route(unrelatedMessage()).kind).toBe("start");
    });

    test("snapshot is a copy, not the live state", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));

      const snap = router.snapshot();
      if (snap === null) throw new Error("expected non-null snapshot");

      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            messageId: "<follow-snap@example.com>",
          }),
        ),
      );

      expect(snap.lastMessageId).toBe("<root@example.com>");
      expect(router.snapshot()?.lastMessageId).toBe(
        "<follow-snap@example.com>",
      );
    });

    test("restore takes a defensive copy of the input state and its cc", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const input: ConnectorThreadState = {
        threadRoot: "<x@example.com>",
        lastMessageId: "<x@example.com>",
        replyTo: "x@example.com",
        cc: ["a@example.com"],
        subject: "S",
      };
      router.restore(input);

      input.lastMessageId = "<mutated@example.com>";
      input.cc.push("injected@example.com");

      const snap = router.snapshot();
      expect(snap?.lastMessageId).toBe("<x@example.com>");
      expect(snap?.cc).toEqual(["a@example.com"]);
    });
  });

  describe("replyTo normalization", () => {
    // These tests bypass createInboundMessage because its `from` validator
    // requires a bare addr-spec, but on the production fetch path
    // (mail-memory's buildMessageHeaders) the `from` header is copied
    // verbatim from the wire and may contain a display name. The router
    // is the layer that has to handle that, so the test exercises it
    // with wire-shaped values.
    function inboundWith(
      fromHeader: string,
      messageId: string,
    ): InboundMessage {
      return {
        ref: { uid: 1, mailbox: "INBOX" },
        headers: {
          from: fromHeader,
          to: ["agent@example.com"],
          date: new Date().toISOString(),
          messageId,
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
    }

    test("strips display name and lowercases when storing replyTo on start", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });

      router.commit(
        router.route(
          inboundWith('"Alice Doe" <Alice@Example.COM>', "<root@example.com>"),
        ),
      );

      expect(router.snapshot()?.replyTo).toBe("alice@example.com");
    });

    test("strips display name when advancing replyTo on continue", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          startMessage({ subject: "Hello", cc: ["other@example.com"] }),
        ),
      );

      const followup: InboundMessage = {
        ref: { uid: 2, mailbox: "INBOX" },
        headers: {
          from: '"Other User" <Other@Example.com>',
          to: ["agent@example.com"],
          date: new Date().toISOString(),
          messageId: "<follow-norm@example.com>",
          references: ["<root@example.com>"],
        },
        flags: [],
        content: "more",
        signatureStatus: "missing",
      };
      router.commit(router.route(followup));

      expect(router.snapshot()?.replyTo).toBe("other@example.com");
    });

    test("route() throws when the from header is unparseable on start", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const message = inboundWith("not-an-address", "<bad@example.com>");

      expect(() => router.route(message)).toThrow();
    });

    test("route() throws when the from header is unparseable on continue", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage()));

      const malformedContinuation: InboundMessage = {
        ref: { uid: 99, mailbox: "INBOX" },
        headers: {
          from: "not-an-address",
          to: ["agent@example.com"],
          date: new Date().toISOString(),
          messageId: "<follow-bad@example.com>",
          references: ["<root@example.com>"],
        },
        flags: [],
        content: "more",
        signatureStatus: "missing",
      };

      expect(() => router.route(malformedContinuation)).toThrow();
    });
  });

  describe("passthrough reply target", () => {
    const THREAD = {
      threadRoot: "<root@example.com>",
      lastMessageId: "<root@example.com>",
      replyTo: "user@example.com",
      cc: [],
      subject: "Hello",
    };

    function passthroughMessage(opts?: {
      references?: string[];
    }): InboundMessage {
      return createInboundMessage({
        from: "carol@example.com",
        to: ["agent@example.com"],
        content: "not on the thread",
        messageId: "<carol-1@example.com>",
        subject: "Side question",
        ...(opts?.references !== undefined
          ? { references: opts.references }
          : {}),
      });
    }

    function routerWithPassthrough(opts?: {
      onStateChanged?: (state: ConnectorThreadState | null) => void;
    }): ConnectorRouter {
      const router = createConnectorRouter({ ownAddress: AGENT, ...opts });
      router.commit(router.route(startMessage({ subject: "Hello" })));
      const decision = router.route(
        passthroughMessage({ references: ["<earlier@example.com>"] }),
      );
      expect(decision.kind).toBe("passthrough");
      router.commit(decision);
      return router;
    }

    const CAROL_REPLY = {
      to: "carol@example.com",
      cc: [],
      inReplyTo: "<carol-1@example.com>",
      references: ["<earlier@example.com>", "<carol-1@example.com>"],
      subject: "Side question",
    };

    test("a passthrough commit points the reply at its sender and leaves the thread unchanged", () => {
      const router = routerWithPassthrough();

      expect(router.composeReply()).toEqual(CAROL_REPLY);
      expect(router.snapshot()).toEqual(THREAD);
    });

    test("onReplySent after a passthrough reply leaves the thread unchanged", () => {
      const changes: (ConnectorThreadState | null)[] = [];
      const router = routerWithPassthrough({
        onStateChanged: (state) => changes.push(state),
      });
      const changesBefore = changes.length;

      router.onReplySent(
        {
          messageId: "<sent-to-carol@example.com>",
          status: "delivered",
        },
        [],
      );

      expect(router.snapshot()).toEqual(THREAD);
      expect(changes.length).toBe(changesBefore);
    });

    test("two replies in one passthrough turn go to the same sender under the same parent", () => {
      const router = routerWithPassthrough();

      const first = router.composeReply();
      router.onReplySent(
        {
          messageId: "<sent-1@example.com>",
          status: "delivered",
        },
        [],
      );
      const second = router.composeReply();

      expect(first).toEqual(CAROL_REPLY);
      expect(second).toEqual(CAROL_REPLY);
    });

    test("a continue commit clears the target and replies on the thread", () => {
      const router = routerWithPassthrough();

      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            messageId: "<follow-1@example.com>",
          }),
        ),
      );

      expect(router.composeReply()).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<follow-1@example.com>",
        references: ["<root@example.com>", "<follow-1@example.com>"],
        subject: "Hello",
      });
    });

    test("a start commit clears the target", () => {
      const noFrom: InboundMessage = {
        ref: { uid: 9, mailbox: "INBOX" },
        headers: {
          to: ["agent@example.com"],
          date: new Date().toISOString(),
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(noFrom));
      expect(router.composeReply()).toBeNull();

      router.commit(router.route(startMessage({ subject: "Hello" })));

      expect(router.composeReply()).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        references: ["<root@example.com>"],
        subject: "Hello",
      });
    });

    test("beginTurn() clears the target, so a later turn replies on the thread", () => {
      const router = routerWithPassthrough();

      router.beginTurn();

      expect(router.composeReply()).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        references: ["<root@example.com>"],
        subject: "Hello",
      });
    });

    test("an approval resume takes the reply target its parked turn filed", () => {
      const router = routerWithPassthrough();

      expect(router.parkTurn("c1")).toBe(true);
      expect(router.parkedReplyTargets()).toEqual({
        c1: { parts: CAROL_REPLY },
      });
      expect(router.beginTurn({ resume: "c1" })).toBe(true);

      expect(router.composeReply()).toEqual(CAROL_REPLY);
      expect(router.parkedReplyTargets()).toEqual({});
    });

    test("a resume of an id with no filed target replies on the thread", () => {
      const router = routerWithPassthrough();

      expect(router.beginTurn({ resume: "unknown" })).toBe(false);

      expect(router.composeReply()).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        references: ["<root@example.com>"],
        subject: "Hello",
      });
    });

    test("a turn with no reply target files nothing when it parks", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));

      expect(router.parkTurn("c1")).toBe(false);
      expect(router.parkedReplyTargets()).toEqual({});
    });

    test("a parked target with no address resumes with no reply address", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));
      router.commit({ kind: "passthrough" });

      router.parkTurn("c1");
      expect(router.parkedReplyTargets()).toEqual({ c1: { parts: null } });
      router.beginTurn();
      router.beginTurn({ resume: "c1" });

      expect(router.composeReply()).toBeNull();
    });

    test("restored parked targets resume like filed ones", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));

      router.restoreParkedReplyTargets({ c1: { parts: CAROL_REPLY } });
      router.beginTurn({ resume: "c1" });

      expect(router.composeReply()).toEqual(CAROL_REPLY);
    });

    test("a passthrough decision the caller made itself yields no reply address", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));

      router.commit({ kind: "passthrough" });

      expect(router.composeReply()).toBeNull();
      expect(router.snapshot()).toEqual(THREAD);
    });
  });

  describe("message carrying no originator", () => {
    // A message with no `From` at all reaches the router on the production
    // fetch path: buildMessageHeaders omits the field when the wire message
    // carried none. Such a message names no address a reply could go to, so
    // the router must derive none. Naming a stand-in instead would record it
    // as a thread participant and address the thread's replies to it. The
    // decision is the router's own -- `passthrough`, not a throw a caller has
    // to translate -- so each test below pins the returned kind and drives the
    // harness's own route-then-commit sequence to pin the resulting state.
    function inboundWithNoFrom(
      messageId: string,
      opts?: { references?: string[] },
    ): InboundMessage {
      return {
        ref: { uid: 7, mailbox: "INBOX" },
        headers: {
          to: ["agent@example.com"],
          date: new Date().toISOString(),
          messageId,
          ...(opts?.references !== undefined
            ? { references: opts.references }
            : {}),
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
    }

    test("route() passes through on start and opens no thread", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const message = inboundWithNoFrom("<orphan@example.com>");

      const decision = router.route(message);
      expect(decision.kind).toBe("passthrough");

      router.commit(decision);

      // No thread exists, and the turn has no reply address, rather than one
      // the router chose.
      expect(router.snapshot()).toBeNull();
      expect(router.composeReply()).toBeNull();
    });

    test("route() passes through on continue and leaves the thread untouched", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(startMessage({ subject: "Hello" })));
      const before = router.snapshot();

      const followup = inboundWithNoFrom("<follow-orphan@example.com>", {
        references: ["<root@example.com>"],
      });
      const decision = router.route(followup);
      expect(decision.kind).toBe("passthrough");

      router.commit(decision);

      // The thread still names its real speaker: the message that could not
      // be attributed neither became replyTo nor joined cc. The reply to it
      // has no address, so it does not go to the thread's speaker either.
      expect(router.snapshot()).toEqual(before);
      expect(router.composeReply()).toBeNull();
    });

    test("an absent originator passes through where an unparseable one throws", () => {
      // The two share an outcome at the caller -- the harness delivers the
      // message and leaves the thread unadvanced either way -- but they reach
      // it differently, and only the throw is a failure. Pinning all three
      // shapes in one closure keeps a change that collapses them together
      // from passing.
      const absent = createConnectorRouter({ ownAddress: AGENT });
      expect(absent.route(inboundWithNoFrom("<orphan@example.com>")).kind).toBe(
        "passthrough",
      );

      const unparseable = createConnectorRouter({ ownAddress: AGENT });
      const malformed: InboundMessage = {
        ref: { uid: 8, mailbox: "INBOX" },
        headers: {
          from: "not-an-address",
          to: ["agent@example.com"],
          date: new Date().toISOString(),
          messageId: "<bad@example.com>",
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
      expect(() => unparseable.route(malformed)).toThrow();

      const named = createConnectorRouter({ ownAddress: AGENT });
      const decision = named.route(startMessage({ subject: "Hello" }));
      expect(decision.kind).toBe("start");
      named.commit(decision);
      expect(named.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<root@example.com>",
        replyTo: "user@example.com",
        cc: [],
        subject: "Hello",
      });
    });
  });

  describe("message carrying no message id", () => {
    // A message with no `Message-ID` still names an address to reply to, so
    // the thread opens. What it denies is an anchor: no later message can
    // match it as a continuation, so the next one re-anchors the thread.
    function inboundWithNoMessageId(opts?: {
      from?: string;
      inReplyTo?: string;
      references?: string[];
    }): InboundMessage {
      return {
        ref: { uid: 11, mailbox: "INBOX" },
        headers: {
          from: opts?.from ?? "user@example.com",
          to: ["agent@example.com"],
          date: new Date().toISOString(),
          ...(opts?.inReplyTo !== undefined
            ? { inReplyTo: opts.inReplyTo }
            : {}),
          ...(opts?.references !== undefined
            ? { references: opts.references }
            : {}),
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
    }

    test("route() opens a thread so the reply is not dropped", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const decision = router.route(inboundWithNoMessageId());

      expect(decision.kind).toBe("start");
      router.commit(decision);

      const state = router.snapshot();
      expect(state).not.toBeNull();
      expect(state?.replyTo).toBe("user@example.com");
      expect(state?.threadRoot).toBeUndefined();
      expect(state?.lastMessageId).toBeUndefined();
    });

    test("composeReply() omits In-Reply-To rather than emitting an empty one", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(inboundWithNoMessageId()));

      const parts = requireReplyParts(router);
      expect(parts.to).toBe("user@example.com");
      expect(parts.inReplyTo).toBeUndefined();
      expect("inReplyTo" in parts).toBe(false);
    });

    test("a continuation naming no id is answered with its References alone", () => {
      // RFC 5322 3.6.4: a parent with no Message-ID gives no In-Reply-To,
      // and the reply's References is the parent's References alone. The
      // root is not a fallback parent.
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          startMessage({ subject: "Invoice", cc: ["bob@example.com"] }),
        ),
      );

      const decision = router.route(
        inboundWithNoMessageId({
          from: "bob@example.com",
          references: ["<root@example.com>"],
        }),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);
      expect(router.snapshot()?.lastMessageId).toBeUndefined();
      expect(router.snapshot()?.lastReferences).toEqual(["<root@example.com>"]);

      expect(router.composeReply()).toEqual({
        to: "bob@example.com",
        cc: ["user@example.com"],
        references: ["<root@example.com>"],
        subject: "Invoice",
      });
    });

    test("an arriving message re-anchors the thread rather than continuing it", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(inboundWithNoMessageId()));

      const next = continuationByInReplyTo("<whatever@example.com>", {
        from: "other@example.com",
      });
      const decision = router.route(next);

      expect(decision.kind).toBe("start");
      router.commit(decision);
      expect(router.snapshot()?.replyTo).toBe("other@example.com");
      expect(router.snapshot()?.cc).toEqual([]);
      expect(router.snapshot()?.threadRoot).toBe("<reply-2@example.com>");
    });

    test("a blank In-Reply-To does not continue an anchorless thread", () => {
      // The empty id is the case that has to hold, not merely a non-matching
      // one: an anchorless thread holds no `lastMessageId`, and a message
      // whose `In-Reply-To` header was written blank carries an empty id.
      // Compared as values the two are equal, so a continue would push the
      // real correspondent into `cc`. The value reaches the router through
      // the wire, not through `createInboundMessage`, which rejects an empty
      // `inReplyTo`.
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(inboundWithNoMessageId()));

      const stranger = inboundWithNoMessageId({
        from: "stranger@example.com",
        inReplyTo: "",
      });
      const decision = router.route(stranger);

      expect(decision.kind).toBe("start");

      router.commit(decision);
      expect(router.snapshot()?.replyTo).toBe("stranger@example.com");
      expect(router.snapshot()?.cc).toEqual([]);
    });

    test("an anchorless thread is not an absorbing state", () => {
      // Before the re-anchor rule an anchorless thread could never match
      // `isContinuation`, so every later message routed `passthrough` -- the
      // INBOX never drained and the first sender kept the reply address for
      // the life of the agent.
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(inboundWithNoMessageId()));

      const anchored = startMessage({ cc: ["other@example.com"] });
      const decision = router.route(anchored);
      expect(decision.kind).toBe("start");
      router.commit(decision);

      expect(router.snapshot()?.threadRoot).toBe("<root@example.com>");
      expect(router.snapshot()?.lastMessageId).toBe("<root@example.com>");

      // The re-anchored thread continues normally from here.
      const follow = router.route(
        continuationByReferences("<root@example.com>", {
          from: "other@example.com",
        }),
      );
      expect(follow.kind).toBe("continue");
      router.commit(follow);
      expect(router.snapshot()?.replyTo).toBe("other@example.com");
      expect(router.snapshot()?.cc).toEqual(["user@example.com"]);
    });

    test("onReplySent anchors the thread so later replies continue it", () => {
      // The agent replying is the ordinary way an anchorless thread acquires
      // an anchor: the reply's own id becomes `lastMessageId`.
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(inboundWithNoMessageId()));
      router.onReplySent(
        {
          messageId: "<agent-reply@example.com>",
          status: "delivered",
        },
        [],
      );

      const decision = router.route(
        continuationByInReplyTo("<agent-reply@example.com>"),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);
      expect(router.snapshot()?.replyTo).toBe("user@example.com");
    });
  });

  describe("onStateChanged callback", () => {
    test("fires after a start decision commits", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });

      router.commit(router.route(startMessage({ subject: "Hello" })));

      expect(events).toHaveLength(1);
      expect(events[0]).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<root@example.com>",
        replyTo: "user@example.com",
        cc: [],
        subject: "Hello",
      });
    });

    test("fires after continue advances the thread", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });
      router.commit(router.route(startMessage({ cc: ["second@example.com"] })));
      events.length = 0;

      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-cb@example.com>",
          }),
        ),
      );

      expect(events).toHaveLength(1);
      expect(events[0]?.lastMessageId).toBe("<follow-cb@example.com>");
      expect(events[0]?.replyTo).toBe("second@example.com");
      expect(events[0]?.cc).toEqual(["user@example.com"]);
    });

    test("does not fire for passthrough commits", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });
      router.commit(router.route(startMessage()));
      events.length = 0;

      router.commit(router.route(unrelatedMessage()));

      expect(events).toHaveLength(0);
    });

    test("fires after onReplySent advances lastMessageId", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });
      router.commit(router.route(startMessage()));
      events.length = 0;

      router.onReplySent(
        {
          messageId: "<sent-cb@example.com>",
          status: "delivered",
        },
        [],
      );

      expect(events).toHaveLength(1);
      expect(events[0]?.lastMessageId).toBe("<sent-cb@example.com>");
    });

    test("fires on restore() when state changes from null", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });

      router.restore({
        threadRoot: "<r@example.com>",
        lastMessageId: "<r@example.com>",
        replyTo: "r@example.com",
        cc: [],
      });

      expect(events).toHaveLength(1);
      expect(events[0]?.threadRoot).toBe("<r@example.com>");
    });

    test("does not fire on restore(null) when already null (cold start)", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });

      router.restore(null);

      expect(events).toHaveLength(0);
    });

    test("does not fire on restore() into an equal state", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });
      const snap: ConnectorThreadState = {
        threadRoot: "<r@example.com>",
        lastMessageId: "<r@example.com>",
        replyTo: "r@example.com",
        cc: ["a@example.com"],
        subject: "S",
      };
      router.restore(snap);
      events.length = 0;

      router.restore({ ...snap, cc: [...snap.cc] });

      expect(events).toHaveLength(0);
    });

    test("fires when cc changes even if replyTo and lastMessageId do not", () => {
      // Defensive: any field of the state changing is a state change.
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });
      const snap: ConnectorThreadState = {
        threadRoot: "<r@example.com>",
        lastMessageId: "<r@example.com>",
        replyTo: "r@example.com",
        cc: [],
      };
      router.restore(snap);
      events.length = 0;

      router.restore({ ...snap, cc: ["a@example.com"] });

      expect(events).toHaveLength(1);
      expect(events[0]?.cc).toEqual(["a@example.com"]);
    });

    test("fires with a snapshot copy, not the live state", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (s) => events.push(s),
      });

      router.commit(router.route(startMessage()));
      const captured = events[0];
      if (captured === null || captured === undefined) {
        throw new Error("expected non-null captured state");
      }

      router.onReplySent(
        {
          messageId: "<later@example.com>",
          status: "delivered",
        },
        [],
      );

      expect(captured.lastMessageId).toBe("<root@example.com>");
    });

    test("a throwing subscriber does not propagate out of commit()", () => {
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: () => {
          throw new Error("subscriber boom");
        },
      });

      const decision = router.route(startMessage());
      expect(() => router.commit(decision)).not.toThrow();
      expect(router.snapshot()).not.toBeNull();
    });

    test("a throwing subscriber does not propagate out of onReplySent()", () => {
      let firstCall = true;
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: () => {
          if (firstCall) {
            firstCall = false;
            return;
          }
          throw new Error("subscriber boom");
        },
      });
      router.commit(router.route(startMessage()));

      expect(() =>
        router.onReplySent(
          { messageId: "<sent-throw@example.com>", status: "delivered" },
          [],
        ),
      ).not.toThrow();
      expect(router.snapshot()?.lastMessageId).toBe("<sent-throw@example.com>");
    });
  });

  describe("participant check", () => {
    // `In-Reply-To` and `References` are the sender's own claims, so naming
    // the thread is not enough to join it: the speaker must be on the
    // thread or addressed by a participant.
    function threadMessage(opts: {
      from: string;
      messageId: string;
      authenticatedSender?: string;
      cc?: string[];
    }): InboundMessage {
      const message = createInboundMessage({
        from: opts.from,
        to: [AGENT],
        content: "more",
        messageId: opts.messageId,
        references: ["<root@example.com>"],
        ...(opts.cc !== undefined ? { cc: opts.cc } : {}),
      });
      return opts.authenticatedSender !== undefined
        ? { ...message, authenticatedSender: opts.authenticatedSender }
        : message;
    }

    const THREAD: ConnectorThreadState = {
      threadRoot: "<root@example.com>",
      lastMessageId: "<root@example.com>",
      replyTo: "user@example.com",
      cc: [],
      subject: "Hello",
    };

    test("a participant that names the thread continues it", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore(THREAD);

      const decision = router.route(
        threadMessage({
          from: "user@example.com",
          messageId: "<user-2@example.com>",
        }),
      );

      expect(decision.kind).toBe("continue");
    });

    test("a non-participant that names the thread is passthrough and gets its own reply", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore(THREAD);

      const decision = router.route(
        threadMessage({
          from: "mallory@example.com",
          messageId: "<mallory-1@example.com>",
        }),
      );
      expect(decision.kind).toBe("passthrough");
      router.commit(decision);

      expect(router.snapshot()).toEqual(THREAD);
      expect(router.composeReply()).toEqual({
        to: "mallory@example.com",
        cc: [],
        inReplyTo: "<mallory-1@example.com>",
        references: ["<root@example.com>", "<mallory-1@example.com>"],
      });
    });

    test("a recipient a participant cc'd joins the thread by speaking", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore(THREAD);
      router.commit(
        router.route(
          threadMessage({
            from: "user@example.com",
            messageId: "<user-2@example.com>",
            cc: ["Bob@Example.com"],
          }),
        ),
      );
      expect(router.snapshot()?.addressed).toEqual(["bob@example.com"]);

      const decision = router.route(
        threadMessage({
          from: "bob@example.com",
          messageId: "<bob-1@example.com>",
        }),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);

      expect(router.composeReply()).toEqual({
        to: "bob@example.com",
        cc: ["user@example.com"],
        inReplyTo: "<bob-1@example.com>",
        references: ["<root@example.com>", "<bob-1@example.com>"],
        subject: "Hello",
      });
    });

    test("an addressed recipient is not on outbound mail until it speaks", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(startMessage({ cc: ["bob@example.com", AGENT] })),
      );

      expect(router.snapshot()?.addressed).toEqual(["bob@example.com"]);
      expect(router.composeReply()).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        references: ["<root@example.com>"],
      });
    });

    test("the speaker is the authenticated sender, not the From", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore(THREAD);

      // The From claims the thread owner; the hub verified someone else.
      const forged = threadMessage({
        from: "user@example.com",
        messageId: "<forged@example.com>",
        authenticatedSender: "mallory@example.com",
      });
      const decision = router.route(forged);
      expect(decision.kind).toBe("passthrough");
      router.commit(decision);
      expect(router.composeReply()?.to).toBe("mallory@example.com");

      const genuine = threadMessage({
        from: "someone-else@example.com",
        messageId: "<genuine@example.com>",
        authenticatedSender: "user@example.com",
      });
      expect(router.route(genuine).kind).toBe("continue");
    });

    test("the speaker is the From when the message has no authenticated sender", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore(THREAD);

      const decision = router.route(
        threadMessage({
          from: "User@Example.com",
          messageId: "<user-2@example.com>",
        }),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);
      expect(router.snapshot()?.replyTo).toBe("user@example.com");
    });

    test("a thread owner stored with another letter case still continues", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore({
        ...THREAD,
        replyTo: "Alice@Example.com",
        cc: ["Carol@Example.com"],
      });

      const decision = router.route(
        threadMessage({
          from: "alice@example.com",
          messageId: "<alice-2@example.com>",
        }),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);

      // The returning owner is not also carried on cc under the old spelling.
      expect(router.snapshot()?.replyTo).toBe("alice@example.com");
      expect(router.snapshot()?.cc).toEqual(["Carol@Example.com"]);
    });

    test("a cc'd participant stored with another letter case still continues", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.restore({ ...THREAD, addressed: ["Bob@Example.com"] });

      const decision = router.route(
        threadMessage({
          from: "bob@example.com",
          messageId: "<bob-1@example.com>",
        }),
      );

      expect(decision.kind).toBe("continue");
    });

    test("a state persisted without addressed restores and keeps its participants", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const persisted = ConnectorThreadStateSchema({
        threadRoot: "<root@example.com>",
        lastMessageId: "<root@example.com>",
        replyTo: "user@example.com",
        cc: ["second@example.com"],
      });
      if (persisted instanceof type.errors) {
        throw new Error(persisted.summary);
      }
      router.restore(persisted);

      expect(router.snapshot()).toEqual(persisted);
      expect(
        router.route(
          threadMessage({
            from: "second@example.com",
            messageId: "<second-1@example.com>",
          }),
        ).kind,
      ).toBe("continue");
      expect(
        router.route(
          threadMessage({
            from: "stranger@example.com",
            messageId: "<stranger-1@example.com>",
          }),
        ).kind,
      ).toBe("passthrough");
    });
  });

  // RFC 5322 3.6.4: a reply's References is the parent's References (or,
  // when it has none, its single In-Reply-To id) followed by the parent's
  // Message-ID. The parent is the exact message the reply answers.
  describe("References chain", () => {
    function inbound(opts: {
      from?: string;
      messageId: string;
      inReplyTo?: string;
      references?: string[];
    }): InboundMessage {
      return createInboundMessage({
        from: opts.from ?? "user@example.com",
        to: [AGENT],
        content: "x",
        messageId: opts.messageId,
        ...(opts.inReplyTo !== undefined ? { inReplyTo: opts.inReplyTo } : {}),
        ...(opts.references !== undefined
          ? { references: opts.references }
          : {}),
      });
    }

    // Built by hand because `createInboundMessage` always names a
    // Message-ID.
    function inboundWithoutId(opts: {
      inReplyTo?: string;
      references?: string[];
    }): InboundMessage {
      return {
        ref: { uid: 1, mailbox: "INBOX" },
        headers: {
          from: "user@example.com",
          to: [AGENT],
          date: new Date().toISOString(),
          ...(opts.inReplyTo !== undefined
            ? { inReplyTo: opts.inReplyTo }
            : {}),
          ...(opts.references !== undefined
            ? { references: opts.references }
            : {}),
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
    }

    function sent(messageId: string): {
      messageId: string;
      status: "delivered";
    } {
      return { messageId, status: "delivered" };
    }

    test("a start stores the inbound message's References as the parent's chain", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          inbound({
            messageId: "<m3@example.com>",
            inReplyTo: "<m2@example.com>",
            references: ["<m1@example.com>", "<m2@example.com>"],
          }),
        ),
      );

      expect(router.snapshot()?.lastMessageId).toBe("<m3@example.com>");
      expect(router.snapshot()?.lastReferences).toEqual([
        "<m1@example.com>",
        "<m2@example.com>",
      ]);
      expect(requireReplyParts(router).references).toEqual([
        "<m1@example.com>",
        "<m2@example.com>",
        "<m3@example.com>",
      ]);
      expect(requireReplyParts(router).inReplyTo).toBe("<m3@example.com>");
    });

    test("a start with no References takes a single-id In-Reply-To as the chain", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          inbound({
            messageId: "<m2@example.com>",
            inReplyTo: "<m1@example.com>",
          }),
        ),
      );

      expect(router.snapshot()?.lastReferences).toEqual(["<m1@example.com>"]);
      expect(requireReplyParts(router).references).toEqual([
        "<m1@example.com>",
        "<m2@example.com>",
      ]);
    });

    test("an In-Reply-To that names several ids gives no chain", () => {
      // Built by hand because `createInboundMessage` accepts only a single
      // id, while a parsed header keeps whatever the sender wrote.
      const message: InboundMessage = {
        ref: { uid: 1, mailbox: "INBOX" },
        headers: {
          from: "user@example.com",
          to: [AGENT],
          date: new Date().toISOString(),
          messageId: "<m3@example.com>",
          inReplyTo: "<m1@example.com> <m2@example.com>",
        },
        flags: [],
        content: "x",
        signatureStatus: "missing",
      };
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(message));

      expect(router.snapshot()?.lastReferences).toBeUndefined();
      expect(requireReplyParts(router).references).toEqual([
        "<m3@example.com>",
      ]);
    });

    test("a continue makes the continuing message the parent", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          inbound({
            messageId: "<m2@example.com>",
            references: ["<m1@example.com>"],
          }),
        ),
      );
      router.commit(
        router.route(
          inbound({
            messageId: "<m4@example.com>",
            inReplyTo: "<m2@example.com>",
            references: [
              "<m1@example.com>",
              "<m2@example.com>",
              "<m3@example.com>",
            ],
          }),
        ),
      );

      expect(router.snapshot()?.lastMessageId).toBe("<m4@example.com>");
      expect(requireReplyParts(router)).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<m4@example.com>",
        references: [
          "<m1@example.com>",
          "<m2@example.com>",
          "<m3@example.com>",
          "<m4@example.com>",
        ],
      });
    });

    test("a sent thread reply becomes the parent of the next reply", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          inbound({
            messageId: "<m2@example.com>",
            references: ["<m1@example.com>"],
          }),
        ),
      );

      const first = requireReplyParts(router);
      router.onReplySent(sent("<r1@example.com>"), first.references ?? []);

      expect(router.snapshot()?.lastMessageId).toBe("<r1@example.com>");
      expect(router.snapshot()?.lastReferences).toEqual([
        "<m1@example.com>",
        "<m2@example.com>",
      ]);
      const second = requireReplyParts(router);
      expect(second.inReplyTo).toBe("<r1@example.com>");
      expect(second.references).toEqual([
        "<m1@example.com>",
        "<m2@example.com>",
        "<r1@example.com>",
      ]);
    });

    test("the next reply carries the References the sent reply carried", () => {
      // The chain comes from the send, not from the state at send time.
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(router.route(inbound({ messageId: "<m1@example.com>" })));

      router.onReplySent(sent("<r1@example.com>"), [
        "<other@example.com>",
        "<m1@example.com>",
      ]);

      expect(requireReplyParts(router).references).toEqual([
        "<other@example.com>",
        "<m1@example.com>",
        "<r1@example.com>",
      ]);
    });

    test("a continue that names no id is answered with its References and no In-Reply-To", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      router.commit(
        router.route(
          inbound({
            messageId: "<m1@example.com>",
            references: ["<r0@example.com>"],
          }),
        ),
      );
      const first = requireReplyParts(router);
      expect(first.references).toEqual([
        "<r0@example.com>",
        "<m1@example.com>",
      ]);
      router.onReplySent(sent("<s1@example.com>"), first.references ?? []);

      const decision = router.route(
        inboundWithoutId({
          inReplyTo: "<s1@example.com>",
          references: [
            "<r0@example.com>",
            "<m1@example.com>",
            "<s1@example.com>",
          ],
        }),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);

      expect(router.snapshot()?.lastMessageId).toBeUndefined();
      expect(router.snapshot()?.lastReferences).toEqual([
        "<r0@example.com>",
        "<m1@example.com>",
        "<s1@example.com>",
      ]);
      const parts = requireReplyParts(router);
      expect(parts).toEqual({
        to: "user@example.com",
        cc: [],
        references: [
          "<r0@example.com>",
          "<m1@example.com>",
          "<s1@example.com>",
        ],
      });
      expect("inReplyTo" in parts).toBe(false);
    });

    test("a start that names no id is answered with its References and no In-Reply-To", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const decision = router.route(
        inboundWithoutId({ references: ["<x@example.com>"] }),
      );
      expect(decision.kind).toBe("start");
      router.commit(decision);

      const parts = requireReplyParts(router);
      expect(parts).toEqual({
        to: "user@example.com",
        cc: [],
        references: ["<x@example.com>"],
      });
      expect("inReplyTo" in parts).toBe(false);
    });

    test("a state persisted without lastReferences threads under the parent id alone", () => {
      const router = createConnectorRouter({ ownAddress: AGENT });
      const persisted = ConnectorThreadStateSchema({
        threadRoot: "<m1@example.com>",
        lastMessageId: "<m5@example.com>",
        replyTo: "user@example.com",
        cc: [],
      });
      if (persisted instanceof type.errors) {
        throw new Error(persisted.summary);
      }
      router.restore(persisted);

      expect(router.snapshot()).toEqual(persisted);
      expect(requireReplyParts(router)).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<m5@example.com>",
        references: ["<m5@example.com>"],
      });
    });

    test("snapshot and restore carry lastReferences", () => {
      const a = createConnectorRouter({ ownAddress: AGENT });
      a.commit(
        a.route(
          inbound({
            messageId: "<m2@example.com>",
            references: ["<m1@example.com>"],
          }),
        ),
      );
      a.onReplySent(sent("<r1@example.com>"), [
        "<m1@example.com>",
        "<m2@example.com>",
      ]);
      const snap = a.snapshot();
      const parsed = ConnectorThreadStateSchema(snap);
      if (parsed instanceof type.errors) throw new Error(parsed.summary);

      const b = createConnectorRouter({ ownAddress: AGENT });
      b.restore(parsed);

      expect(b.snapshot()).toEqual(snap);
      expect(b.composeReply()).toEqual(a.composeReply());

      // The restored state is a copy of the input's chain.
      parsed.lastReferences?.push("<injected@example.com>");
      expect(b.snapshot()?.lastReferences).toEqual([
        "<m1@example.com>",
        "<m2@example.com>",
      ]);
    });

    test("a change to lastReferences alone notifies the subscriber", () => {
      const changes: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
        ownAddress: AGENT,
        onStateChanged: (state) => changes.push(state),
      });
      router.restore({
        lastMessageId: "<m2@example.com>",
        replyTo: "user@example.com",
        cc: [],
      });
      router.restore({
        lastMessageId: "<m2@example.com>",
        lastReferences: ["<m1@example.com>"],
        replyTo: "user@example.com",
        cc: [],
      });

      expect(changes.map((state) => state?.lastReferences)).toEqual([
        undefined,
        ["<m1@example.com>"],
      ]);
    });
  });
});
