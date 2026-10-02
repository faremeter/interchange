// `HubTransport` backed by real SMTP submission and real IMAP mailboxes.
//
// This is the object the sidecar composition injects in place of
// `InMemoryTransport`. It satisfies the same four-method host surface, so the
// deploy router, the hub link, and the supervisor's mail bus are untouched --
// but the mail leaves over SMTP and arrives over IMAP IDLE instead of riding the
// hub control socket.
//
// Three places where the host surface and a network transport disagree, each
// resolved here rather than papered over:
//
//   1. `register` is SYNCHRONOUS and a connection is not. Registering starts the
//      connect and returns; `getTransportFor` hands back a proxy whose methods
//      await it. A connect that fails surfaces on the first method call, which is
//      the first moment a caller is in a position to hear about it.
//   2. `setRemoteSendHandler` has nothing to do. It exists so the in-memory
//      transport can hand a recipient it does not host to the hub for routing;
//      an SMTP relay routes every recipient itself, local or not. The handler is
//      accepted and never called, and that is the whole point: the websocket's
//      mail-routing role is what this transport removes.
//   3. `deliver` injects hub-pushed bytes straight into the inbound sink rather
//      than IMAP-APPENDing them. The sink is the same one IDLE feeds, so a
//      deployment behaves identically whichever path a message took, and the
//      socket path keeps working while the migration is partial.

import { getLogger } from "@intx/log";
import type {
  CryptoProvider,
  HubTransport,
  MessageRef,
  MessageSentHandler,
  MessageTransport,
} from "@intx/types/runtime";
import { MessageTransportError } from "@intx/types/runtime";

import { createImapTransport, type ImapTransport } from "./transport";
import type { MailAccountCredentials } from "./provisioner";

const logger = getLogger(["interchange", "mail-imap", "hub"]);

export type ImapHubTransportConfig = {
  imap: { host: string; port: number; secure: boolean };
  smtp: {
    host: string;
    port: number;
    secure: boolean;
    ignoreTLS?: boolean;
  };
  /**
   * Resolve an address's mailbox credentials. A deployment's account must
   * already exist on the server: this transport authenticates, it does not
   * provision. Creating and removing accounts alongside deploy and undeploy is
   * the operator's half of the contract and has no home in this interface.
   */
  credentialsFor: (address: string) => MailAccountCredentials;
  /** Resolves the key that verifies a sender's mail, for `fetchFull`. */
  getCrypto: (fromAddress: string) => CryptoProvider | undefined;
  /**
   * Receives the verbatim bytes of every message that reaches a registered
   * address, whether IMAP IDLE observed it or the hub pushed it through
   * `deliver`. The host decides admission and routing; this transport takes no
   * stance on either, and acts only on the verdict it returns.
   *
   * Three outcomes, because two would conflate the ones that need different
   * handling. A host that signalled only success/failure would leave a message
   * it deliberately refused looking exactly like one it accepted.
   */
  onInbound: (address: string, raw: Uint8Array) => Promise<IngestOutcome>;
  /**
   * Fired after a successful SMTP submission from a registered address, with
   * the bytes that went out. The host transport fans it out to every handler
   * registered through `addMessageSentHandler`.
   */
  onSent?: MessageSentHandler;
};

/**
 * What the host did with a message, and therefore what becomes of the copy the
 * mail server still holds.
 *
 * - `accepted` -- durably stored by the deployment. The server's copy is
 *   redundant (the committed `<uid>.eml` is byte-identical and replicates to
 *   the hub) so it is EXPUNGED. Keeping it would grow the mailbox without
 *   bound for no reader.
 * - `refused` -- the admission policy rejected it. Final, so retrying would
 *   re-reject forever; it is flagged and left in place as the only remaining
 *   evidence of what arrived.
 * - `retry` -- nothing durable happened. Left untouched so the next sweep
 *   re-offers it.
 */
export type IngestOutcome = "accepted" | "refused" | "retry";

export type ImapHubTransport = HubTransport & {
  /** Close every per-address connection. */
  close(): Promise<void>;
};

/**
 * One connected lifetime of an address's transport. Replaced wholesale on
 * reconnect, because `ImapFlow` cannot reopen a closed connection -- recovery
 * means a new transport, not a revived one.
 */
type Generation = {
  transport: ImapTransport;
  /** Resolves once `start()`, the watch arm, and the backlog sweep completed. */
  ready: Promise<void>;
  unwatch?: () => void;
};

type Entry = {
  crypto: CryptoProvider;
  /** The live generation. Readers resolve through the entry, never capture it. */
  current: Generation;
  /** Set by `unregister`/`close` so a deliberate teardown does not reconnect. */
  retired: boolean;
  /** Consecutive failed reconnect attempts, for the backoff. */
  attempts: number;
};

export function createImapHubTransport(
  config: ImapHubTransportConfig,
): ImapHubTransport {
  const entries = new Map<string, Entry>();
  const messageSentHandlers = new Set<MessageSentHandler>();

  /**
   * Keyword marking a message the host REFUSED.
   *
   * There is deliberately no marker for an ACCEPTED message: an accepted
   * message is expunged, so anything still in the mailbox is by definition not
   * yet accepted. That keeps the sweep's question trivial and removes a flag
   * that would otherwise have to stay in agreement with the git copy.
   *
   * A refused message is kept rather than expunged because it is the only
   * remaining record of what arrived -- nothing committed it anywhere -- and
   * the decision is final, so re-offering it on every restart would only
   * re-refuse it. The flag is what stops that loop.
   */
  const REFUSED_KEYWORD = "$Refused";

  /**
   * Reconnect backoff. The common cause of a drop here is the mail server
   * reloading after an account was created, which resolves in under a second,
   * so the first retry is quick; the ceiling keeps a server that is genuinely
   * down from being hammered.
   */
  const RECONNECT_BASE_MS = 250;
  const RECONNECT_MAX_MS = 30_000;

  function startGeneration(
    address: string,
    crypto: CryptoProvider,
    onClose: (reason: string) => void,
  ): Generation {
    const transport = createImapTransport({
      address,
      crypto,
      getCrypto: config.getCrypto,
      imap: { ...config.imap, auth: config.credentialsFor(address) },
      smtp: {
        ...config.smtp,
        auth: config.credentialsFor(address),
      },
      // Restores outbound audit parity with the control-socket path: the
      // sidecar registers a handler that forwards each sent message to the hub,
      // and this is what fires it. Handlers run under `allSettled` -- the
      // message is already submitted, so one that throws must not be reported
      // as a send failure.
      onClose,
      onSent: async (ctx) => {
        const results = await Promise.allSettled(
          [...messageSentHandlers].map((handler) => handler(ctx)),
        );
        for (const result of results) {
          if (result.status === "rejected") {
            logger.error`a message-sent handler failed for ${address} after submission: ${result.reason instanceof Error ? result.reason.message : String(result.reason)}`;
          }
        }
      },
    });

    // Offer one message to the host and act on its verdict. Never throws: the
    // ingress runs detached from any caller, so there is nowhere for a
    // rejection to go but a log.
    const ingest = async (ref: MessageRef): Promise<IngestOutcome> => {
      let raw: Uint8Array;
      try {
        raw = await transport.readRaw(ref);
      } catch (cause) {
        logger.error`ingress for ${address} could not read uid ${String(ref.uid)}: ${cause instanceof Error ? cause.message : String(cause)}`;
        return "retry";
      }

      let outcome: IngestOutcome;
      try {
        outcome = await config.onInbound(address, raw);
      } catch (cause) {
        // A throw is not a verdict. Treat it as `retry` and leave the message
        // untouched, so a host defect costs a redelivery rather than a
        // silently discarded message.
        logger.error`inbound mail for ${address} uid ${String(ref.uid)} threw instead of returning a verdict; leaving it for the next sweep: ${cause instanceof Error ? cause.message : String(cause)}`;
        return "retry";
      }

      if (outcome === "retry") return "retry";

      if (outcome === "refused") {
        try {
          await transport.setFlags(ref, [REFUSED_KEYWORD]);
        } catch (cause) {
          // The flag only stops the sweep re-offering it, so losing it costs a
          // repeated refusal, not a lost message.
          logger.warn`could not mark ${address} uid ${String(ref.uid)} refused; the next sweep will re-offer it: ${cause instanceof Error ? cause.message : String(cause)}`;
        }
        return "refused";
      }

      // Accepted. The deployment committed the verbatim bytes to its
      // workflow-run repo, which replicates to the hub and is validated there,
      // so this copy carries nothing the git copy does not. Removing it is what
      // keeps the mailbox from growing without bound behind a reader that never
      // comes back for it.
      //
      // A crash between the host's commit and this expunge leaves the message
      // present, so the next sweep re-offers it. That is at-least-once, which
      // the deployment's own arrival dedup makes effectively-once -- the same
      // contract the control socket carries.
      try {
        await transport.setFlags(ref, ["\\Deleted"]);
        await transport.expunge("INBOX");
      } catch (cause) {
        logger.warn`could not expunge ${address} uid ${String(ref.uid)} after acceptance; a restart will re-offer it: ${cause instanceof Error ? cause.message : String(cause)}`;
      }
      return "accepted";
    };

    // Declared ahead of the entry so the ready promise can publish the
    // unsubscribe it installs without reading a binding it is inside.
    let unwatch: (() => void) | undefined;
    const ready = (async () => {
      await transport.start();
      // Arm the watch BEFORE the catch-up sweep, so a message arriving during
      // the sweep is not missed by both. The overlap can offer one message
      // twice, which the deployment's arrival dedup makes harmless; a gap
      // between the two would lose one outright.
      unwatch = transport.watch("INBOX", (event) => {
        if (event.type !== "exists") return;
        void ingest({ uid: event.uid, mailbox: "INBOX" });
      });

      // The backlog is everything present that was not refused -- after a
      // restart, every message that arrived while this process was gone, since
      // an accepted one would have been expunged.
      const backlog = await transport.search("INBOX", {
        missingFlags: [REFUSED_KEYWORD],
      });
      if (backlog.length > 0) {
        logger.info`offering ${String(backlog.length)} unprocessed message(s) from ${address}`;
      }
      for (const ref of backlog) await ingest(ref);
    })();

    const generation: Generation = { transport, ready };
    // `unwatch` is published onto the generation once the arm lands, so a
    // teardown that happens before `ready` settles still finds it afterwards.
    void ready
      .then(() => {
        if (unwatch !== undefined) generation.unwatch = unwatch;
      })
      .catch(() => {
        // A generation that never became ready has no watch to release, and its
        // failure is reported by whoever awaited `ready`.
      });
    return generation;
  }

  /**
   * Replace a dead generation with a fresh one, retrying until it comes up.
   *
   * Recovery is not just a reconnect: the new generation re-arms the watch and
   * re-sweeps the mailbox, so every message that arrived while the connection
   * was down is offered to the host. That is the same sweep a cold start runs,
   * which is why a drop costs latency rather than mail.
   */
  function scheduleReconnect(address: string): void {
    const entry = entries.get(address);
    if (entry === undefined || entry.retired) return;
    const delay = Math.min(
      RECONNECT_BASE_MS * 2 ** entry.attempts,
      RECONNECT_MAX_MS,
    );
    entry.attempts += 1;
    setTimeout(() => {
      const live = entries.get(address);
      if (live === undefined || live.retired) return;
      logger.info`reconnecting the mailbox for ${address} (attempt ${String(live.attempts)})`;
      const generation = startGeneration(address, live.crypto, (reason) => {
        onGenerationClosed(address, generation, reason);
      });
      live.current = generation;
      void generation.ready.then(
        () => {
          live.attempts = 0;
          logger.info`mailbox for ${address} is connected again`;
        },
        (cause: unknown) => {
          logger.warn`reconnect for ${address} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
          scheduleReconnect(address);
        },
      );
    }, delay);
  }

  /**
   * Handle a generation's unexpected close. Ignored unless the generation is
   * still the live one: a stale generation's close is the expected consequence
   * of having already been replaced.
   */
  function onGenerationClosed(
    address: string,
    generation: Generation,
    reason: string,
  ): void {
    const entry = entries.get(address);
    if (entry === undefined || entry.retired) return;
    // A stale generation's close is the expected consequence of having been
    // replaced already, so only the live one triggers a rebuild.
    if (entry.current !== generation) return;
    generation.unwatch?.();
    logger.warn`mailbox for ${address} lost its connection (${reason}); rebuilding`;
    scheduleReconnect(address);
  }

  function requireEntry(address: string, what: string): Entry {
    const entry = entries.get(address);
    if (entry === undefined) {
      throw new MessageTransportError(
        "NONEXISTENT",
        `imap hub transport: ${what} for "${address}", which is not registered`,
      );
    }
    return entry;
  }

  /**
   * A `MessageTransport` whose every method awaits the address's connection
   * first. `watch` is the exception the interface forces: it returns
   * synchronously, so it registers against the live transport once ready and
   * hands back an unsubscribe that is honoured whenever the install lands.
   */
  function deferredTransport(address: string): MessageTransport {
    const entry = requireEntry(address, "getTransportFor");
    // Resolved through `entry.current` on every call, so an operation issued
    // after a reconnect reaches the LIVE transport rather than a dead one a
    // closure captured.
    const ready = async (): Promise<MessageTransport> => {
      const generation = entry.current;
      await generation.ready;
      return generation.transport;
    };
    return {
      send: async (message, signal) => (await ready()).send(message, signal),
      search: async (mailbox, query, signal) =>
        (await ready()).search(mailbox, query, signal),
      fetchHeaders: async (ref, signal) =>
        (await ready()).fetchHeaders(ref, signal),
      fetchPart: async (ref, path, signal) =>
        (await ready()).fetchPart(ref, path, signal),
      fetchFull: async (ref, signal) => (await ready()).fetchFull(ref, signal),
      readRaw: async (ref, signal) => (await ready()).readRaw(ref, signal),
      setFlags: async (ref, flags, signal) =>
        (await ready()).setFlags(ref, flags, signal),
      clearFlags: async (ref, flags, signal) =>
        (await ready()).clearFlags(ref, flags, signal),
      expunge: async (mailbox, signal) =>
        (await ready()).expunge(mailbox, signal),
      watch: (mailbox, callback) => {
        // Armed against whichever generation is live when the install lands. A
        // caller that needs a watch to survive a reconnect has to re-install
        // it; the ingress does, because each generation arms its own.
        let installed: (() => void) | undefined;
        let cancelled = false;
        void (async () => {
          const transport = await ready();
          if (cancelled) return;
          installed = transport.watch(mailbox, callback);
        })();
        return () => {
          cancelled = true;
          installed?.();
        };
      },
    };
  }

  return {
    register(address, crypto) {
      if (entries.has(address)) {
        throw new Error(`Address "${address}" is already registered`);
      }
      // The close handler resolves the entry from the registry rather than
      // closing over it, so the generation can be built before the entry exists
      // and neither needs a placeholder the other has to narrow away.
      const generation = startGeneration(address, crypto, (reason) => {
        onGenerationClosed(address, generation, reason);
      });
      entries.set(address, {
        crypto,
        current: generation,
        retired: false,
        attempts: 0,
      });
      // Surface a connect failure here as well as on first use, and retry it:
      // an address whose mailbox is not up yet (its account was created moments
      // ago) must not be left permanently unconnected.
      generation.ready.catch((cause: unknown) => {
        logger.error`mailbox for ${address} did not come up: ${cause instanceof Error ? cause.message : String(cause)}`;
        scheduleReconnect(address);
      });
    },

    unregister(address) {
      const entry = entries.get(address);
      if (entry === undefined) return;
      entries.delete(address);
      entry.retired = true;
      entry.current.unwatch?.();
      void entry.current.transport.close().catch((cause: unknown) => {
        logger.warn`closing the mailbox for ${address} failed: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    },

    getTransportFor(address) {
      return deferredTransport(address);
    },

    setRemoteSendHandler() {
      // Accepted and never called. See the note at the top of this file: an SMTP
      // relay owns delivery for every recipient, so there is no "remote" leg for
      // the host to take over.
      logger.debug`remote-send handler registered and ignored; SMTP submission routes every recipient`;
    },

    addMessageSentHandler(handler) {
      messageSentHandlers.add(handler);
    },

    deliver(address, message) {
      // Hub-pushed bytes take the same sink as an IMAP arrival, so a deployment
      // behaves identically whichever path a message took. The verdict is
      // discarded: there is no server-side copy to expunge or flag, and the
      // hub's own retry owns redelivery of what it pushed.
      void config.onInbound(address, message).catch((cause: unknown) => {
        logger.error`hub-pushed mail for ${address} was not accepted: ${cause instanceof Error ? cause.message : String(cause)}`;
      });
    },

    async close() {
      const open = [...entries.values()];
      entries.clear();
      for (const entry of open) {
        entry.retired = true;
        entry.current.unwatch?.();
      }
      await Promise.allSettled(
        open.map((entry) => entry.current.transport.close()),
      );
    },
  };
}
