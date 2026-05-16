export { InMemoryTransport } from "./transport";
export type {
  RemoteSendHandler,
  MessageSentHandler,
  MessageSentContext,
} from "./send";

/**
 * Create a fresh in-memory transport instance.
 *
 * The returned transport is shared across all addresses in a single
 * process. Register addresses before sending messages:
 *
 *   const transport = createInMemoryTransport();
 *   transport.register("alpha@local.interchange", cryptoProviderA);
 *   transport.register("beta@local.interchange", cryptoProviderB);
 *
 *   const alphaTransport = transport.getTransportFor("alpha@local.interchange");
 *   await alphaTransport.send({ to: "beta@local.interchange", ... });
 */
import { InMemoryTransport } from "./transport";

export function createInMemoryTransport(): InMemoryTransport {
  return new InMemoryTransport();
}
