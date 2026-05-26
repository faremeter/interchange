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
    test("no active thread: any message starts a new thread", () => {
      const router = createConnectorRouter();
      const message = startMessage({ subject: "Hello" });

      const decision = router.route(message);
      expect(decision.kind).toBe("start");

      router.commit(decision);

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<root@example.com>",
        replyTo: "user@example.com",
        subject: "Hello",
      });
    });

    test("active thread + references includes threadRoot: continue", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));

      const followup = continuationByReferences("<root@example.com>", {
        from: "user@example.com",
        messageId: "<follow-1@example.com>",
      });
      const decision = router.route(followup);
      expect(decision.kind).toBe("continue");

      router.commit(decision);

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<follow-1@example.com>",
        replyTo: "user@example.com",
        subject: "Hello",
      });
    });

    test("active thread + inReplyTo equals lastMessageId: continue", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));

      const followup = continuationByInReplyTo("<root@example.com>", {
        messageId: "<follow-2@example.com>",
      });
      const decision = router.route(followup);
      expect(decision.kind).toBe("continue");

      router.commit(decision);

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<follow-2@example.com>",
        replyTo: "user@example.com",
      });
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

    test("continue preserves threadRoot and subject from initial state", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Important" })));

      // Continuation from a different sender — replyTo should advance, but
      // threadRoot and subject must carry through unchanged.
      const followup = continuationByReferences("<root@example.com>", {
        from: "other@example.com",
        messageId: "<follow-3@example.com>",
      });
      router.commit(router.route(followup));

      const snap = router.snapshot();
      expect(snap?.threadRoot).toBe("<root@example.com>");
      expect(snap?.subject).toBe("Important");
      expect(snap?.replyTo).toBe("other@example.com");
      expect(snap?.lastMessageId).toBe("<follow-3@example.com>");
    });

    test("commit on a foreign decision throws", () => {
      const router = createConnectorRouter();
      // A `start` decision the router did not produce.
      expect(() => {
        router.commit({ kind: "start" });
      }).toThrow();
    });
  });

  describe("composeReply (outbound)", () => {
    test("active thread with subject: emits to, inReplyTo, subject", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));

      const parts = router.composeReply();
      expect(parts).toEqual({
        to: "user@example.com",
        inReplyTo: "<root@example.com>",
        subject: "Hello",
      });
    });

    test("active thread without subject: subject key absent (not undefined)", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));

      const parts = router.composeReply();
      expect(parts.to).toBe("user@example.com");
      expect(parts.inReplyTo).toBe("<root@example.com>");
      expect("subject" in parts).toBe(false);
    });

    test("no active thread: throws NoActiveConnectorThreadError", () => {
      const router = createConnectorRouter();
      expect(() => {
        router.composeReply();
      }).toThrow(NoActiveConnectorThreadError);
    });

    test("onReplySent advances lastMessageId", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));

      router.onReplySent({
        messageId: "<sent-1@example.com>",
        status: "delivered",
      });

      expect(router.snapshot()).toEqual({
        threadRoot: "<root@example.com>",
        lastMessageId: "<sent-1@example.com>",
        replyTo: "user@example.com",
        subject: "Hello",
      });

      // A subsequent inbound message whose inReplyTo matches the new
      // lastMessageId must route as continue.
      const followup = continuationByInReplyTo("<sent-1@example.com>", {
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
            messageId: "<follow-A@example.com>",
          }),
        ),
      );

      const snap = a.snapshot();

      const b = createConnectorRouter();
      b.restore(snap);

      // Snapshots match.
      expect(b.snapshot()).toEqual(snap);

      // Same message → same decision kind from both routers.
      const probe = continuationByInReplyTo("<follow-A@example.com>", {
        messageId: "<probe@example.com>",
      });
      expect(b.route(probe).kind).toBe(a.route(probe).kind);

      // Outbound parts also match.
      expect(b.composeReply()).toEqual(a.composeReply());
    });

    test("restore(null) clears active thread", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage()));
      expect(router.snapshot()).not.toBeNull();

      router.restore(null);
      expect(router.snapshot()).toBeNull();

      // After clear, the next inbound starts a new thread.
      expect(router.route(unrelatedMessage()).kind).toBe("start");
    });

    test("snapshot is a copy, not the live state", () => {
      const router = createConnectorRouter();
      router.commit(router.route(startMessage({ subject: "Hello" })));

      const snap = router.snapshot();
      if (snap === null) throw new Error("expected non-null snapshot");

      // Advance router state, then verify the prior snapshot is unaffected.
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

    test("restore takes a defensive copy of the input state", () => {
      const router = createConnectorRouter();
      const input: ConnectorThreadState = {
        threadRoot: "<x@example.com>",
        lastMessageId: "<x@example.com>",
        replyTo: "x@example.com",
        subject: "S",
      };
      router.restore(input);

      // Mutating the input after restore must not affect router state.
      input.lastMessageId = "<mutated@example.com>";

      expect(router.snapshot()?.lastMessageId).toBe("<x@example.com>");
    });
  });
});
