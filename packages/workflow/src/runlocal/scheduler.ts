// In-process setTimeout scheduler.
//
// Not durable across restart: stranded `TimerSet` events from prior
// runs are not auto-serviced. The spec calls this out explicitly in
// §8 -- `runLocal` is normative modulo durability.

import type { Scheduler } from "../runtime/env";

export function createInMemoryScheduler(): Scheduler {
  return {
    scheduleIn(delayMs, fire) {
      const handle = setTimeout(fire, delayMs);
      return () => {
        clearTimeout(handle);
      };
    },
  };
}
