// Workflow trigger configurations.
//
// A workflow declares one or more triggers; each trigger type carries
// its own run-firing semantics. The workflow runtime materializes the
// trigger at run construction time and observes incoming events.

/**
 * Mail trigger. The first inbound mail at `to` fires the deployment's stable
 * top-level run. While that run is live, later mail may resume an `onTrigger`
 * section through its current input correlation. Once the top-level run is
 * terminal, the deployment cannot be fired again.
 */
export interface MailTrigger {
  type: "mail";
  to: string;
}

/**
 * Cron-shaped schedule trigger. RESERVED, NOT IMPLEMENTED: no cron parser and
 * no scheduler exist, so nothing ever fires a schedule trigger. A workflow that
 * declared one would hash, deploy, and then simply never run -- no error, no
 * log, no failed run. Two layers reject it instead: `defineWorkflow` refuses to
 * normalize a definition carrying one, and the hub's probe gate refuses an
 * inert projection carrying one (the gate is what reaches an already-deployed
 * closure, which bundles its own copy of `defineWorkflow`).
 *
 * The intended semantics, once a scheduler exists: missed ticks during outages
 * are skipped; the next future tick fires normally.
 */
export interface ScheduleTrigger {
  type: "schedule";
  cron: string;
}

/**
 * Manual trigger. The workflow runtime exposes an explicit
 * invocation entry point that fires a single run; nothing fires
 * automatically.
 */
export interface ManualTrigger {
  type: "manual";
}

export type Trigger = MailTrigger | ScheduleTrigger | ManualTrigger;
