export {
  InMemoryTransport,
  inboundMessageToRaw,
  type HubTransport,
} from "./transport";
export type {
  RemoteSendHandler,
  MessageSentHandler,
  MessageSentContext,
} from "./send";

/**
 * Create a fresh in-memory transport instance. The returned transport is
 * shared across all addresses in a single process; register addresses
 * before sending messages.
 */
import { InMemoryTransport } from "./transport";

export function createInMemoryTransport(): InMemoryTransport {
  return new InMemoryTransport();
}
