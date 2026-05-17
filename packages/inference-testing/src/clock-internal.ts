/**
 * @internal TEST-ONLY: do not import from production code.
 *
 * This module exists to keep test-only seams off the public surface of the
 * package. It is intentionally not re-exported from `index.ts`. Production
 * code must not import from this module; only co-located tests should.
 */

import { createClockInternal, type Clock } from "./clock";

/**
 * @internal TEST-ONLY
 *
 * Constructs a Clock with the monotonic sequence counter seeded to the
 * given value. Used to drive the overflow guard without scheduling
 * Number.MAX_SAFE_INTEGER entries.
 */
export function createClockWithSeq(initialSeq: number): Clock {
  return createClockInternal({ initialSeq });
}
