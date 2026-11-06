import { and, eq, sql } from "drizzle-orm";

import { parseAddress } from "@intx/types";
import { deriveRunPrincipalId } from "@intx/hub-common";

import type { DBExecutor } from "./client";
import { principal } from "./schema/principals";
import { tenant } from "./schema/tenants";
import { workflowDefinition } from "./schema/workflow-definitions";
import { workflowRun } from "./schema/workflow-run";

const RUN_PREFIX = "run_";

/**
 * The local party a mail address names: a deployment's run, or a user
 * principal.
 *
 * A run carries its deployment anchor's id and tenant, the workflow asset that
 * backs its definition (`null` when the definition has no asset), the run
 * principal its first trigger linked (`linkedPrincipalId`, `null` before that
 * trigger), and the run principal's id (`principalId`). Deploy creates the run
 * principal with the id `deriveRunPrincipalId(tenantId, anchorRunId)` and the
 * first trigger links that same id, so `principalId` names the run principal
 * before and after the first trigger.
 */
export type LocalMailParty =
  | {
      kind: "run";
      anchorRunId: string;
      tenantId: string;
      assetId: string | null;
      linkedPrincipalId: string | null;
      principalId: string;
    }
  | { kind: "user"; principalId: string; tenantId: string };

/**
 * Resolve the local run or user that a mail address names, or `null` when it
 * names neither. The address is matched in lower case and without a trailing
 * root dot on its domain, so every spelling of one address names one party.
 *
 * This reads no signing key and no run state: a run that has not fired, has no
 * active key, or has ended still names the same party. A caller that needs a
 * key or a live run checks that itself.
 *
 * A `run_` local part names a deployment anchor: the run whose id is the local
 * part, which is its own anchor, and whose stored address normalizes to the
 * same address. Any other local part names a user principal (see
 * `resolveUserPrincipal`), which throws when the address is ambiguous.
 */
export async function resolveLocalMailParty(
  db: DBExecutor,
  address: string,
): Promise<LocalMailParty | null> {
  const normalized = normalizeAddress(address);
  if (normalized === null) return null;
  const { localPart, domain } = normalized;

  if (!localPart.startsWith(RUN_PREFIX)) {
    const user = await resolveUserPrincipal(db, domain, localPart);
    return user === null ? null : { kind: "user", ...user };
  }

  // Run ids are lower case, so the local part is the anchor's primary key.
  const [anchor] = await db
    .select({
      id: workflowRun.id,
      anchorRunId: workflowRun.anchorRunId,
      address: workflowRun.address,
      tenantId: workflowRun.tenantId,
      linkedPrincipalId: workflowRun.principalId,
      assetId: workflowDefinition.assetId,
    })
    .from(workflowRun)
    .innerJoin(
      workflowDefinition,
      eq(workflowRun.definitionId, workflowDefinition.id),
    )
    .where(eq(workflowRun.id, localPart))
    .limit(1);
  const stored =
    anchor?.address == null ? null : normalizeAddress(anchor.address);
  if (
    anchor === undefined ||
    anchor.anchorRunId !== anchor.id ||
    stored?.localPart !== localPart ||
    stored.domain !== domain
  ) {
    return null;
  }
  return {
    kind: "run",
    anchorRunId: anchor.id,
    tenantId: anchor.tenantId,
    assetId: anchor.assetId,
    linkedPrincipalId: anchor.linkedPrincipalId,
    principalId:
      anchor.linkedPrincipalId ??
      (await deriveRunPrincipalId(anchor.tenantId, anchor.id)),
  };
}

// An address in lower case, split at its `@`, without a trailing root dot on
// its domain.
function normalizeAddress(
  address: string,
): { localPart: string; domain: string } | null {
  const parsed = parseAddress(address.toLowerCase());
  if (parsed === null) return null;
  const domain = parsed.domain.endsWith(".")
    ? parsed.domain.slice(0, -1)
    : parsed.domain;
  if (domain.length === 0) return null;
  return { localPart: parsed.localPart, domain };
}

/**
 * Resolve the user principal a normalized `<localPart>@<domain>` address names,
 * or `null` when none matches. `lower(domain)` is unique, so the domain selects
 * at most one tenant; within it, prefer the principal whose `refId` is exactly
 * the normalized localPart (the canonical lowercase refId). The domain is
 * matched case-insensitively in both queries because an existing tenant's stored
 * domain may be mixed-case (creation lowercases new ones, but legacy rows are
 * left as stored). Only when no exact refId exists does it fall back to a
 * case-insensitive refId match, which finds a mixed-case-stored refId. A
 * non-unique case-insensitive match with no exact row means two principals in
 * the tenant hold case-variant refIds; that is genuinely ambiguous, so it throws
 * rather than return an arbitrary one and attribute the address to the wrong
 * principal.
 */
async function resolveUserPrincipal(
  db: DBExecutor,
  domain: string,
  localPart: string,
): Promise<{ principalId: string; tenantId: string } | null> {
  const columns = { principalId: principal.id, tenantId: principal.tenantId };

  const [exact] = await db
    .select(columns)
    .from(principal)
    .innerJoin(tenant, eq(principal.tenantId, tenant.id))
    .where(
      and(
        eq(sql`lower(${tenant.domain})`, domain),
        eq(principal.kind, "user"),
        eq(principal.refId, localPart),
      ),
    )
    .limit(1);
  if (exact !== undefined) return exact;

  const caseInsensitive = await db
    .select(columns)
    .from(principal)
    .innerJoin(tenant, eq(principal.tenantId, tenant.id))
    .where(
      and(
        eq(sql`lower(${tenant.domain})`, domain),
        eq(principal.kind, "user"),
        eq(sql`lower(${principal.refId})`, localPart),
      ),
    )
    .limit(2);
  if (caseInsensitive.length === 0) return null;
  if (caseInsensitive.length > 1) {
    throw new Error(
      `user address ${localPart}@${domain} is ambiguous; it matches ` +
        `multiple principals with case-variant refIds and no canonical ` +
        `row -- the colliding principal refIds must be reconciled`,
    );
  }
  const [only] = caseInsensitive;
  return only === undefined ? null : only;
}
