// Workflow trigger configurations.
//
// A workflow declares one or more triggers; each trigger type carries
// its own run-firing semantics. The workflow runtime materializes the
// trigger at run construction time and observes incoming events.

/**
 * Mail trigger. Each inbound mail at `to` fires a new run; the runtime
 * serializes runs per address so two mails to the same address run
 * sequentially.
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
