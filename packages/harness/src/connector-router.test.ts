import { describe, test, expect } from "bun:test";

import { createInboundMessage } from "@intx/mime";
import type { ConnectorThreadState, InboundMessage } from "@intx/types/runtime";

import {
  createConnectorRouter,
  NoActiveConnectorThreadError,
} from "./connector-router";

function startMessage(opts?: { subject?: string }): InboundMessage {
  return createInboundMessage({
    from: "user@example.com",
    to: ["agent@example.com"],
    content: "hello",
    messageId: "<root@example.com>",
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
      const router = createConnectorRouter();

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
      const router = createConnectorRouter();
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
        replyTo: "user@example.com",
        cc: [],
        subject: "Hello",
      });
    });

    test("active thread + inReplyTo equals lastMessageId: continue from the same sender keeps cc empty", () => {
      const router = createConnectorRouter();
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
        replyTo: "user@example.com",
        cc: [],
      });
    });

    test("continue from a different sender moves the prior replyTo into cc", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Important" })));

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
        replyTo: "other@example.com",
        cc: ["user@example.com"],
        subject: "Important",
      });
    });

    test("continue accumulates participants across multiple distinct senders", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Important" })));

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
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));
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
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Important" })));

      for (const from of ["b@example.com", "c@example.com", "d@example.com"]) {
        router.commit(
          router.route(
            continuationByReferences("<root@example.com>", {
              from,
              messageId: `<follow-${from}>`,
            }),
          ),
        );
      }

      expect(router.snapshot()?.subject).toBe("Important");
    });

    test("active thread + neither header rule matches: passthrough leaves state unchanged", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));
      const before = router.snapshot();

      const decision = router.route(unrelatedMessage());
      expect(decision.kind).toBe("passthrough");

      router.commit(decision);

      expect(router.snapshot()).toEqual(before);
    });

    test("commit on a foreign decision throws", () => {
      const router = createConnectorRouter();
      expect(() => {
        router.commit({ kind: "start" });
      }).toThrow();
    });
  });

  describe("composeReply (outbound)", () => {
    test("single-participant thread: cc is empty", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));

      const parts = router.composeReply();
      expect(parts).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        subject: "Hello",
      });
    });

    test("multi-participant thread: cc contains all prior speakers", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Important" })));
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
        subject: "Important",
      });
    });

    test("active thread without subject: subject key absent (not undefined)", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));

      const parts = router.composeReply();
      expect(parts.to).toBe("user@example.com");
      expect(parts.cc).toEqual([]);
      expect(parts.inReplyTo).toBe("<root@example.com>");
      expect("subject" in parts).toBe(false);
    });

    test("composeReply returns a copy of cc, not the live array", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );

      const parts = router.composeReply();
      parts.cc.push("injected@example.com");

      // Subsequent state must not include the injected value.
      expect(router.snapshot()?.cc).toEqual(["user@example.com"]);
    });

    test("no active thread: throws NoActiveConnectorThreadError", () => {
      const router = createConnectorRouter();
      expect(() => {
        router.composeReply();
      }).toThrow(NoActiveConnectorThreadError);
    });

    test("onReplySent advances lastMessageId and preserves cc", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));
      router.commit(
        router.route(
          continuationByReferences("<root@example.com>", {
            from: "second@example.com",
            messageId: "<follow-a@example.com>",
          }),
        ),
      );

      router.onReplySent({
        messageId: "<sent-1@example.com>",
        status: "delivered",
      });

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<sent-1@example.com>",
        replyTo: "second@example.com",
        cc: ["user@example.com"],
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
      const router = createConnectorRouter();
      expect(() => {
        router.onReplySent({
          messageId: "<sent@example.com>",
          status: "delivered",
        });
      }).toThrow(NoActiveConnectorThreadError);
    });
  });

  describe("snapshot/restore round-trip", () => {
    test("a router restored from a snapshot decides identically", () => {
      const a = createConnectorRouter();
      a.commit(a.route(startMessage({ subject: "Hello" })));
      a.commit(
        a.route(
          continuationByReferences("<root@example.com>", {
            from: "other@example.com",
            messageId: "<follow-A@example.com>",
          }),
        ),
      );

      const snap = a.snapshot();

      const b = createConnectorRouter();
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
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));
      expect(router.snapshot()).not.toBeNull();

      router.restore(null);
      expect(router.snapshot()).toBeNull();

      expect(router.route(unrelatedMessage()).kind).toBe("start");
    });

    test("snapshot is a copy, not the live state", () => {
      const router = createConnectorRouter();
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
      const router = createConnectorRouter();
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
      const router = createConnectorRouter();

      router.commit(
        router.route(
          inboundWith('"Alice Doe" <Alice@Example.COM>', "<root@example.com>"),
        ),
      );

      expect(router.snapshot()?.replyTo).toBe("alice@example.com");
    });

    test("strips display name when advancing replyTo on continue", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));

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
      const router = createConnectorRouter();
      const message = inboundWith("not-an-address", "<bad@example.com>");

      expect(() => router.route(message)).toThrow();
    });

    test("route() throws when the from header is unparseable on continue", () => {
      const router = createConnectorRouter();
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
      const router = createConnectorRouter();
      const message = inboundWithNoFrom("<orphan@example.com>");

      const decision = router.route(message);
      expect(decision.kind).toBe("passthrough");

      router.commit(decision);

      // No thread exists to reply on, rather than one whose reply address
      // the router chose.
      expect(router.snapshot()).toBeNull();
      expect(() => router.composeReply()).toThrow(NoActiveConnectorThreadError);
    });

    test("route() passes through on continue and leaves the thread untouched", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));
      const before = router.snapshot();

      const followup = inboundWithNoFrom("<follow-orphan@example.com>", {
        references: ["<root@example.com>"],
      });
      const decision = router.route(followup);
      expect(decision.kind).toBe("passthrough");

      router.commit(decision);

      // The thread still names its real speaker: the message that could not
      // be attributed neither became replyTo nor joined cc.
      expect(router.snapshot()).toEqual(before);
      expect(router.composeReply()).toEqual({
        to: "user@example.com",
        cc: [],
        inReplyTo: "<root@example.com>",
        subject: "Hello",
      });
    });

    test("an absent originator passes through where an unparseable one throws", () => {
      // The two share an outcome at the caller -- the harness delivers the
      // message and leaves the thread unadvanced either way -- but they reach
      // it differently, and only the throw is a failure. Pinning all three
      // shapes in one closure keeps a change that collapses them together
      // from passing.
      const absent = createConnectorRouter();
      expect(absent.route(inboundWithNoFrom("<orphan@example.com>")).kind).toBe(
        "passthrough",
      );

      const unparseable = createConnectorRouter();
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

      const named = createConnectorRouter();
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
      const router = createConnectorRouter();
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
      const router = createConnectorRouter();
      router.commit(router.route(inboundWithNoMessageId()));

      const parts = router.composeReply();
      expect(parts.to).toBe("user@example.com");
      expect(parts.inReplyTo).toBeUndefined();
      expect("inReplyTo" in parts).toBe(false);
    });

    test("a continuation naming no id still threads against the root", () => {
      // The reply drain only resolves a References chain when `inReplyTo` is
      // present, so dropping it here costs the outbound reply both threading
      // headers and starts a new thread in every participant's client.
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Invoice" })));

      const decision = router.route(
        inboundWithNoMessageId({
          from: "bob@example.com",
          references: ["<root@example.com>"],
        }),
      );
      expect(decision.kind).toBe("continue");
      router.commit(decision);
      expect(router.snapshot()?.lastMessageId).toBeUndefined();

      expect(router.composeReply()).toEqual({
        to: "bob@example.com",
        cc: ["user@example.com"],
        inReplyTo: "<root@example.com>",
        subject: "Invoice",
      });
    });

    test("an arriving message re-anchors the thread rather than continuing it", () => {
      const router = createConnectorRouter();
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
      const router = createConnectorRouter();
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
      const router = createConnectorRouter();
      router.commit(router.route(inboundWithNoMessageId()));

      const anchored = startMessage();
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
      const router = createConnectorRouter();
      router.commit(router.route(inboundWithNoMessageId()));
      router.onReplySent({
        messageId: "<agent-reply@example.com>",
        status: "delivered",
      });

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
        onStateChanged: (s) => events.push(s),
      });
      router.commit(router.route(startMessage()));
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
        onStateChanged: (s) => events.push(s),
      });
      router.commit(router.route(startMessage()));
      events.length = 0;

      router.onReplySent({
        messageId: "<sent-cb@example.com>",
        status: "delivered",
      });

      expect(events).toHaveLength(1);
      expect(events[0]?.lastMessageId).toBe("<sent-cb@example.com>");
    });

    test("fires on restore() when state changes from null", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
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
        onStateChanged: (s) => events.push(s),
      });

      router.restore(null);

      expect(events).toHaveLength(0);
    });

    test("does not fire on restore() into an equal state", () => {
      const events: (ConnectorThreadState | null)[] = [];
      const router = createConnectorRouter({
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
        onStateChanged: (s) => events.push(s),
      });

      router.commit(router.route(startMessage()));
      const captured = events[0];
      if (captured === null || captured === undefined) {
        throw new Error("expected non-null captured state");
      }

      router.onReplySent({
        messageId: "<later@example.com>",
        status: "delivered",
      });

      expect(captured.lastMessageId).toBe("<root@example.com>");
    });

    test("a throwing subscriber does not propagate out of commit()", () => {
      const router = createConnectorRouter({
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
        router.onReplySent({
          messageId: "<sent-throw@example.com>",
          status: "delivered",
        }),
      ).not.toThrow();
      expect(router.snapshot()?.lastMessageId).toBe("<sent-throw@example.com>");
    });
  });
});
