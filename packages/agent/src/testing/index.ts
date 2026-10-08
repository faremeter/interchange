// @intx/agent/testing -- no-op implementations of the env contract's
// required fields, for tests and examples. The exports here permit
// every authz decision and discard every audit record, so test
// fixtures, in-tree examples, and short-lived demos can satisfy
// `BaseEnv` without a real audit store or policy engine. Production
// deployments must replace these with real implementations; importing
// this subpath in production silently disables auditing and allows
// every tool call.

export { noopAuditStore } from "./audit-noop";
export { permissiveAuthorize } from "./authorize-allow";
export { waitForReactorDone } from "./reactor-waiters";
