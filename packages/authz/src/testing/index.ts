// @intx/authz/testing -- permissive implementations of the authz contracts,
// for tests.
//
// The exports here allow every decision. They exist so a test that is not
// about authorization can satisfy a required authorizer without a database
// or real grants. Importing from this subpath in production allows every
// request it guards.

export { permitAllMailSend } from "./mail-send-allow";
