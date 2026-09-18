// @intx/types/testing -- waiting helpers for tests.
//
// These exist so a test can wait for a condition without choosing an interval
// to wait for it in. Nothing here belongs in production: a production caller
// that needs to know when something happened should be given the event, not a
// loop that re-reads until it sees it.
export { waitUntil } from "./wait-until";
