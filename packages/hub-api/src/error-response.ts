import type { Context } from "hono";
import type { ContentfulStatusCode } from "hono/utils/http-status";

// The hub's canonical error envelope: `{ error: { code, message } }`, with
// the HTTP status paired to the machine-readable code. Every route returns
// this shape for non-2xx responses so clients can switch on `code` without
// parsing free-form text; `STATUS_BY_CODE` is the single source of truth
// for the status each code implies.
const STATUS_BY_CODE: Readonly<Record<string, ContentfulStatusCode>> = {
  not_found: 404,
  conflict: 409,
  not_implemented: 501,
  bad_request: 400,
  forbidden: 403,
  payload_too_large: 413,
  invalid_workflow: 409,
  sidecar_unavailable: 502,
  workflow_run_not_running: 409,
  deployment_unreachable: 409,
  workflow_dispatch_unavailable: 503,
  workflow_provisioning_unavailable: 409,
  invalid_request: 400,
  unavailable: 503,
  unsupported_source: 400,
  anchor_run_missing: 500,
  reserved_signal_name: 400,
  unaddressable_run: 400,
  signal_id_conflict: 409,
  path_violation: 400,
  gone: 410,
  already_resolved: 409,
  unauthorized: 401,
};

/** A JSON error response in the canonical `{ error: { code, message } }`
 *  envelope, with the status `code` implies. Throws when `code` has no
 *  mapping so a newly introduced code fails loudly instead of silently
 *  returning the wrong status. */
export function errorResponse(
  c: Context,
  code: string,
  message: string,
): Response {
  const status = STATUS_BY_CODE[code];
  if (status === undefined) {
    throw new Error(
      `errorResponse: no status mapping for error code "${code}"`,
    );
  }
  return c.json({ error: { code, message } }, status);
}
