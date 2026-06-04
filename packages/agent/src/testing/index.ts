// @intx/agent/testing -- no-op implementations of the env contract's
// required fields, for tests and examples.
//
// The exports here silently permit every authz decision and discard
// every audit record. They exist so test fixtures, the in-tree
// examples, and short-lived demos can satisfy `BaseEnv` without
// bringing in a real audit store or policy engine. Production
// deployments replace these with a real `AuditStore` (durably
// recording audit and error events) and a real `authorize` callback
// (gating tool calls per the deployment's policy). Importing from
// this subpath in production silently disables auditing and allows
// every tool call, which is almost never what a production caller
// actually wants -- treat the subpath the same way you would treat a
// hard-coded `() => true` permission check elsewhere in the
// codebase.

export { noopAuditStore } from "./audit-noop";
export { permissiveAuthorize } from "./authorize-allow";
