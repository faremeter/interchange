// @intx/workflow/testing -- waiters for tests that need a run to have reached
// a state before acting on it.
//
// Two observables, because one does not cover every park. The log waiters
// read a run's committed events through the ordinary `RepoStore` surface and
// hold no state of their own, so nothing about them is specific to the
// in-memory store; a production caller wanting to react to committed events
// should subscribe directly rather than through a helper written for test
// ergonomics. They cannot see a RE-PARK, which commits nothing, so the
// observed channel reports that one from the `SignalChannel` seam instead.
export { waitForEvent, waitForNthEvent } from "./log-waiters";
export {
  createObservedSignalChannel,
  type ObservedSignalChannel,
} from "./signal-channel-observer";
