export { InMemoryTransport } from "./transport";
export type {
  RemoteSendHandler,
  MessageSentHandler,
  MessageSentContext,
} from "./send";

/**
 * Create a fresh in-memory transport instance.
 *
 * The returned transport is shared across all agents in a single process.
 * Register agents before sending messages:
 *
 *   const transport = createInMemoryTransport();
 *   transport.registerAgent("alpha@local.interchange", cryptoProviderA);
 *   transport.registerAgent("beta@local.interchange", cryptoProviderB);
 *
 *   const alphaTransport = transport.getTransportForAgent("alpha@local.interchange");
 *   await alphaTransport.send({ to: "beta@local.interchange", ... });
 */
import { InMemoryTransport } from "./transport";

export function createInMemoryTransport(): InMemoryTransport {
  return new InMemoryTransport();
}
