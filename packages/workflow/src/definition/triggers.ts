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
 * Cron-shaped schedule trigger. Missed ticks during outages are
 * skipped; the next future tick fires normally.
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
