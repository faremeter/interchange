// Supervisor-backed `MessageTransport` for a unified-host step agent.
//
// The supervisor owns the durable inbox and the signing key. This transport
// does not open the mailbox and does not decide which mailbox exists. Every
// method other than `send` asks the supervisor and returns that answer,
// including a refusal. `send` still goes through the outbound-mail bridge.
// Flag writes and `expunge` still go through the mailbox-mutation bridge,
// because those frames already exist. The rest go through the mailbox-call
// bridge.
//
// `watch` registers its callback before the round trip. `mailbox.notify` is
// not queued: a callback installed only after the supervisor accepts would
// miss mail that lands while the call is in flight, and `mail_wait` would
// then wait until its timeout. A refusal unregisters the callback and
// rejects, so nothing stays armed for a mailbox the supervisor refused.
//
// When the sidecar constructs the transport without `inbound`, every inbound
// method fails with `SERVERBUG` "not wired". A spawned child has no inbox
// surface, and that failure is the wiring error, not a mailbox policy.

import { base64Decode } from "@intx/types/base64";
import { deriveWorkflowRunId } from "@intx/types/workflow-run-id";
import type {
  BodyStructure,
  InboundMessage,
  ListInfo,
  Mailbox,
  MailboxEvent,
  MailboxStatus,
  MessageAttachment,
  MessageHeaders,
  MessagePart,
  MessageRef,
  MessageTransport,
  OutboundMessage,
  SearchQuery,
  SendReceipt,
  SyncResult,
  SyncState,
  Thread,
  Unsubscribe,
} from "@intx/types/runtime-core";
import { MessageTransportError } from "@intx/types/runtime-core";

import type { ChildMailboxCallBridge } from "./mailbox-call-bridge";
import type { MailboxCallSuccess } from "./mailbox-call-bridge";
import type { ChildMailboxMutationBridge } from "./mailbox-mutation-bridge";
import type { MailboxWatchRegistry } from "./mailbox-watch-registry";
import type { ChildOutboundMailBridge } from "./outbound-mail-bridge";

/**
 * The inbox surface a warm agent owns. `watchRegistry` is the same registry
 * the control loop fires `mailbox.notify` into. `mutationBridge` carries flag
 * writes and expunge. `callBridge` carries every other mailbox method. A
 * build without this surface has no inbox.
 */
export interface SupervisorBackedTransportInbound {
  watchRegistry: MailboxWatchRegistry;
  mutationBridge: ChildMailboxMutationBridge;
  callBridge: ChildMailboxCallBridge;
}

type MailboxCall = Parameters<ChildMailboxCallBridge["submit"]>[0];

type DistributiveOmit<T, K extends PropertyKey> = T extends unknown
  ? Omit<T, K>
  : never;

type WireSearchQuery = Extract<MailboxCall, { op: "search" }>["query"];

/**
 * Search dates are `Date` on the transport and strings on the frame. Convert
 * the tree here. `toISOString` throws on an invalid date, so a bad one fails
 * the call instead of becoming JSON `null` and crashing the control channel.
 */
function searchQueryToWire(query: SearchQuery): WireSearchQuery {
  const wire: WireSearchQuery = {};
  if (query.from !== undefined) wire.from = query.from;
  if (query.to !== undefined) wire.to = query.to;
  if (query.cc !== undefined) wire.cc = query.cc;
  if (query.bcc !== undefined) wire.bcc = query.bcc;
  if (query.header !== undefined) {
    wire.header = {
      field: query.header.field,
      contains: query.header.contains,
    };
  }
  if (query.before !== undefined) wire.before = query.before.toISOString();
  if (query.after !== undefined) wire.after = query.after.toISOString();
  if (query.on !== undefined) wire.on = query.on.toISOString();
  if (query.sentBefore !== undefined) {
    wire.sentBefore = query.sentBefore.toISOString();
  }
  if (query.sentAfter !== undefined) {
    wire.sentAfter = query.sentAfter.toISOString();
  }
  if (query.sentOn !== undefined) wire.sentOn = query.sentOn.toISOString();
  if (query.hasFlags !== undefined) wire.hasFlags = query.hasFlags;
  if (query.missingFlags !== undefined) wire.missingFlags = query.missingFlags;
  if (query.body !== undefined) wire.body = query.body;
  if (query.text !== undefined) wire.text = query.text;
  if (query.largerThan !== undefined) wire.largerThan = query.largerThan;
  if (query.smallerThan !== undefined) wire.smallerThan = query.smallerThan;
  if (query.and !== undefined) wire.and = query.and.map(searchQueryToWire);
  if (query.or !== undefined) wire.or = query.or.map(searchQueryToWire);
  if (query.not !== undefined) wire.not = searchQueryToWire(query.not);
  return wire;
}

function projectPart(
  value: Extract<MailboxCallSuccess, { op: "fetchPart" }>["value"],
): MessagePart {
  const part: MessagePart = {
    contentType: value.contentType,
    content: base64Decode(value.contentBase64),
  };
  if (value.encoding !== undefined) part.encoding = value.encoding;
  if (value.filename !== undefined) part.filename = value.filename;
  if (value.disposition !== undefined) part.disposition = value.disposition;
  return part;
}

function projectFetched(
  value: Extract<MailboxCallSuccess, { op: "fetchFull" }>["value"],
): InboundMessage {
  const message: InboundMessage = {
    ref: value.ref,
    headers: value.headers,
    flags: [...value.flags],
    signatureStatus: value.signatureStatus,
  };
  if (value.content !== undefined) message.content = value.content;
  if (value.payload !== undefined) message.payload = value.payload;
  if (value.attachments !== undefined) {
    message.attachments = value.attachments.map((attachment) => {
      const projected: MessageAttachment = {
        name: attachment.name,
        contentType: attachment.contentType,
        data: base64Decode(attachment.dataBase64),
      };
      if (attachment.part !== undefined) projected.part = attachment.part;
      return projected;
    });
  }
  return message;
}

/**
 * Construct a `MessageTransport` whose mail methods are answered by the
 * supervisor. `address` is the agent's mail address; the supervisor signs
 * outbound mail as this address. When `inbound` is omitted, inbound methods
 * throw `SERVERBUG`.
 */
export function createSupervisorBackedTransport(
  bridge: ChildOutboundMailBridge,
  address: string,
  inbound?: SupervisorBackedTransportInbound,
): MessageTransport {
  function requireInbound(method: string): SupervisorBackedTransportInbound {
    if (inbound === undefined) {
      throw new MessageTransportError(
        "SERVERBUG",
        `supervisor-backed transport: ${method} needs the inbound surface, but it is not wired for unified-host step agent ${address}`,
      );
    }
    return inbound;
  }

  // The request itself is the type parameter. Inferring `op` and then
  // extracting the arm collapses ops that share a shape (`move`/`copy`,
  // the fetches) to `never`.
  async function ask<
    const Request extends DistributiveOmit<MailboxCall, "runId">,
  >(
    request: Request,
  ): Promise<Extract<MailboxCallSuccess, { op: Request["op"] }>> {
    const { callBridge } = requireInbound(request.op);
    const response = await callBridge.submit({
      ...request,
      runId: deriveWorkflowRunId(address),
    });
    if (response.op !== request.op) {
      throw new Error(
        `supervisor-backed transport: mailbox.call.response op ${JSON.stringify(response.op)} does not match ${JSON.stringify(request.op)}`,
      );
    }
    // The comparison above is the narrow. The success union does not
    // distribute over a generic `op`, so the assertion follows the check.
    // eslint-disable-next-line @typescript-eslint/no-unsafe-type-assertion -- response.op === request.op is the narrow; Extract does not distribute over a generic op
    return response as Extract<MailboxCallSuccess, { op: Request["op"] }>;
  }

  return {
    async send(
      message: OutboundMessage,
      _signal?: AbortSignal,
    ): Promise<SendReceipt> {
      return bridge.submit(address, message);
    },

    async append(
      mailbox: string,
      message: InboundMessage,
      flags?: string[],
      _signal?: AbortSignal,
    ): Promise<MessageRef> {
      if (message.attachments !== undefined && message.attachments.length > 0) {
        throw new Error(
          "Cannot append message: attachments are not carried on a mailbox append",
        );
      }
      const answer = await ask({
        op: "append",
        mailbox,
        headers: message.headers,
        ...(message.content !== undefined ? { content: message.content } : {}),
        ...(message.payload !== undefined ? { payload: message.payload } : {}),
        ...(flags !== undefined ? { flags } : {}),
      });
      return answer.value;
    },

    async listMailboxes(_signal?: AbortSignal): Promise<Mailbox[]> {
      const answer = await ask({ op: "listMailboxes" });
      return answer.value.map((mailbox) => {
        const projected: Mailbox = { name: mailbox.name };
        if (mailbox.role !== undefined) projected.role = mailbox.role;
        if (mailbox.delimiter !== undefined) {
          projected.delimiter = mailbox.delimiter;
        }
        return projected;
      });
    },

    async createMailbox(name: string, _signal?: AbortSignal): Promise<Mailbox> {
      const answer = await ask({ op: "createMailbox", name });
      const mailbox: Mailbox = { name: answer.value.name };
      if (answer.value.role !== undefined) mailbox.role = answer.value.role;
      if (answer.value.delimiter !== undefined) {
        mailbox.delimiter = answer.value.delimiter;
      }
      return mailbox;
    },

    async deleteMailbox(name: string, _signal?: AbortSignal): Promise<void> {
      await ask({ op: "deleteMailbox", name });
    },

    async getMailboxStatus(
      name: string,
      _signal?: AbortSignal,
    ): Promise<MailboxStatus> {
      const answer = await ask({
        op: "getMailboxStatus",
        mailbox: name,
      });
      return answer.value;
    },

    async search(
      mailbox: string,
      query: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<MessageRef[]> {
      const answer = await ask({
        op: "search",
        mailbox,
        query: searchQueryToWire(query),
      });
      return answer.value;
    },

    async thread(
      mailbox: string,
      algorithm: "references" | "orderedsubject",
      query?: SearchQuery,
      _signal?: AbortSignal,
    ): Promise<Thread[]> {
      const answer = await ask({
        op: "thread",
        mailbox,
        algorithm,
        ...(query !== undefined ? { query: searchQueryToWire(query) } : {}),
      });
      return answer.value;
    },

    async fetchHeaders(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<MessageHeaders> {
      const answer = await ask({ op: "fetchHeaders", ref });
      return answer.value;
    },

    async fetchStructure(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<BodyStructure> {
      const answer = await ask({ op: "fetchStructure", ref });
      return answer.value;
    },

    async fetchPart(
      ref: MessageRef,
      partPath: string,
      _signal?: AbortSignal,
    ): Promise<MessagePart> {
      const answer = await ask({ op: "fetchPart", ref, partPath });
      return projectPart(answer.value);
    },

    async fetchFull(
      ref: MessageRef,
      _signal?: AbortSignal,
    ): Promise<InboundMessage> {
      const answer = await ask({ op: "fetchFull", ref });
      return projectFetched(answer.value);
    },

    async setFlags(
      ref: MessageRef,
      flags: string[],
      _signal?: AbortSignal,
    ): Promise<void> {
      const { mutationBridge } = requireInbound("setFlags");
      await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox: ref.mailbox,
        op: "addFlags",
        uid: ref.uid,
        flags,
      });
    },

    async clearFlags(
      ref: MessageRef,
      flags: string[],
      _signal?: AbortSignal,
    ): Promise<void> {
      const { mutationBridge } = requireInbound("clearFlags");
      await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox: ref.mailbox,
        op: "removeFlags",
        uid: ref.uid,
        flags,
      });
    },

    async move(
      ref: MessageRef,
      toMailbox: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      await ask({ op: "move", ref, toMailbox });
    },

    async copy(
      ref: MessageRef,
      toMailbox: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      await ask({ op: "copy", ref, toMailbox });
    },

    async expunge(
      mailbox: string,
      _signal?: AbortSignal,
    ): Promise<{ expungedUids: number[] }> {
      const { mutationBridge } = requireInbound("expunge");
      // The supervisor applies mutations in arrival order. A caller that
      // flags `\Deleted` and then expunges must await the two in sequence,
      // or the sweep can run before the flag and miss the message.
      const result = await mutationBridge.submit({
        runId: deriveWorkflowRunId(address),
        mailbox,
        op: "expunge",
      });
      return { expungedUids: result.expungedUids ?? [] };
    },

    async watch(
      mailbox: string,
      callback: (event: MailboxEvent) => void,
    ): Promise<Unsubscribe> {
      const { watchRegistry, callBridge } = requireInbound("watch");
      // Register before the first await. A notify that arrives while the
      // acceptance is in flight is delivered only to callbacks already in
      // the registry; the supervisor does not replay it.
      const unsubscribe = watchRegistry.watch(mailbox, callback);
      try {
        await callBridge.submit({
          runId: deriveWorkflowRunId(address),
          op: "watch",
          mailbox,
        });
      } catch (cause) {
        unsubscribe();
        throw cause;
      }
      return unsubscribe;
    },

    async sync(
      mailbox: string,
      knownState: SyncState,
      _signal?: AbortSignal,
    ): Promise<SyncResult> {
      // `knownUids` is not on the wire. The supervisor splits new messages
      // from flag changes using `uidNext` and returns the finished result.
      const answer = await ask({
        op: "sync",
        mailbox,
        uidNext: knownState.uidNext,
        uidValidity: knownState.uidValidity,
        highestModSeq: knownState.highestModSeq,
      });
      return answer.value;
    },

    async createList(
      listAddress: string,
      name: string,
      _signal?: AbortSignal,
    ): Promise<ListInfo> {
      const answer = await ask({
        op: "createList",
        address: listAddress,
        name,
      });
      return answer.value;
    },

    async listMembers(
      listAddress: string,
      _signal?: AbortSignal,
    ): Promise<string[]> {
      const answer = await ask({
        op: "listMembers",
        address: listAddress,
      });
      return answer.value;
    },

    async subscribe(
      listAddress: string,
      subscriberAddress: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      await ask({ op: "subscribe", listAddress, subscriberAddress });
    },

    async unsubscribe(
      listAddress: string,
      subscriberAddress: string,
      _signal?: AbortSignal,
    ): Promise<void> {
      await ask({ op: "unsubscribe", listAddress, subscriberAddress });
    },
  };
}
